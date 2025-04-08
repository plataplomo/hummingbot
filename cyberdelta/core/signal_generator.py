import asyncio
import logging
import time
from typing import Dict, List, Optional, AsyncGenerator

# Assuming structure: strategy_math/coding_strategy/src/
from ..config.settings import settings
from ..core.models import Ticker, FundingRate, ArbitrageOpportunity, OrderSide
from ..core.data_handler import DataHandler
from ..apis.base import ExchangeAPI, APIError

logger = logging.getLogger(__name__)

class SignalGenerator:
    """Generates arbitrage opportunities based on market data."""

    def __init__(self, data_handler: DataHandler, api_clients: Dict[str, ExchangeAPI]):
        self.data_handler = data_handler
        self.api_clients = api_clients
        self.strategy_params = settings.get_strategy_params()
        self.funding_rate_params = settings.get_funding_rate_params()
        self._stop_event = asyncio.Event()
        self._latest_opportunities: List[ArbitrageOpportunity] = []

    async def _calculate_opportunities(self) -> List[ArbitrageOpportunity]:
        """Core logic to calculate arbitrage opportunities."""
        opportunities = []
        active_exchanges = settings.active_exchanges
        symbols = settings.symbols

        # Fetch necessary data (use data_handler cache or direct API calls for now)
        # In a real implementation, data_handler would provide efficient access
        funding_rates: Dict[str, FundingRate] = {}
        tickers: Dict[str, Ticker] = {}

        # --- Fetch Data (Example using direct calls - suboptimal) ---
        # Ideally, DataHandler provides cached/streamed data
        fetch_tasks = []
        for exchange in active_exchanges:
            client = self.api_clients.get(exchange)
            if not client:
                continue
            for symbol in symbols:
                # Fetch funding rate
                fetch_tasks.append(self._fetch_funding_rate_safe(client, symbol))
                # Fetch ticker (example)
                # fetch_tasks.append(self._fetch_ticker_safe(client, symbol))

        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        # Process results (populate funding_rates and tickers dictionaries)
        for result in results:
            if isinstance(result, FundingRate):
                key = f"{result.symbol}@{client.exchange_name}" # Need to associate client back
                funding_rates[key] = result
            # elif isinstance(result, Ticker): # Process tickers if fetched
            #     key = f"{result.symbol}@{client.exchange_name}"
            #     tickers[key] = result
            elif isinstance(result, Exception):
                logger.warning(f"Error during data fetch: {result}")
            # Handle None results if fetch_safe returns None on error

        # --- Calculate Cross-Exchange Opportunities --- # 
        utility_lambda = self.funding_rate_params.get("utility_lambda", 0.5)
        min_profit = self.funding_rate_params.get("min_expected_profit", 0.0)

        for symbol in symbols:
            for i in range(len(active_exchanges)):
                for j in range(i + 1, len(active_exchanges)):
                    ex1 = active_exchanges[i]
                    ex2 = active_exchanges[j]
                    key1 = f"{symbol}@{ex1}"
                    key2 = f"{symbol}@{ex2}"

                    fr1 = funding_rates.get(key1)
                    fr2 = funding_rates.get(key2)

                    if fr1 and fr1.predicted_rate is not None and fr2 and fr2.predicted_rate is not None:
                        # Placeholder calculations (replace with actual formulas)
                        nfd = fr1.predicted_rate - fr2.predicted_rate
                        # TODO: Incorporate estimated costs (fees, slippage)
                        estimated_costs = 0.0005 # Example static cost
                        profit_adj_1_vs_2 = nfd - estimated_costs
                        profit_adj_2_vs_1 = -nfd - estimated_costs

                        # TODO: Calculate relevant basis volatility (σ_B)
                        basis_volatility = 0.001 # Example static volatility

                        # Opportunity 1: Long ex1, Short ex2
                        if profit_adj_1_vs_2 > min_profit:
                            utility_score = profit_adj_1_vs_2 - utility_lambda * (basis_volatility ** 2)
                            opp_id = f"{ex1}_vs_{ex2}_{symbol}_L1S2"
                            opportunity = ArbitrageOpportunity(
                                opportunity_id=opp_id,
                                opportunity_type='cross',
                                legs=[key1, key2],
                                expected_profit_adj=profit_adj_1_vs_2,
                                basis_volatility=basis_volatility,
                                utility_score=utility_score,
                                recommended_size=0.0, # RiskManager should determine this
                                execution_details={'side1': OrderSide.BUY, 'side2': OrderSide.SELL}
                            )
                            opportunities.append(opportunity)
                            logger.debug(f"Potential Opportunity: {opp_id}, Profit: {profit_adj_1_vs_2:.6f}, Utility: {utility_score:.6f}")

                        # Opportunity 2: Short ex1, Long ex2
                        if profit_adj_2_vs_1 > min_profit:
                            utility_score = profit_adj_2_vs_1 - utility_lambda * (basis_volatility ** 2)
                            opp_id = f"{ex1}_vs_{ex2}_S1L2"
                            opportunity = ArbitrageOpportunity(
                                opportunity_id=opp_id,
                                opportunity_type='cross',
                                legs=[key1, key2],
                                expected_profit_adj=profit_adj_2_vs_1,
                                basis_volatility=basis_volatility,
                                utility_score=utility_score,
                                recommended_size=0.0, # RiskManager should determine this
                                execution_details={'side1': OrderSide.SELL, 'side2': OrderSide.BUY}
                            )
                            opportunities.append(opportunity)
                            logger.debug(f"Potential Opportunity: {opp_id}, Profit: {profit_adj_2_vs_1:.6f}, Utility: {utility_score:.6f}")

        # Sort opportunities by utility score (descending)
        opportunities.sort(key=lambda o: o.utility_score, reverse=True)
        return opportunities

    async def _fetch_funding_rate_safe(self, client: ExchangeAPI, symbol: str) -> Optional[FundingRate]:
        """Safely fetch funding rate, returning None on error."""
        try:
            return await client.fetch_funding_rate(symbol)
        except APIError as e:
            logger.warning(f"[{client.exchange_name}] API Error fetching funding rate for {symbol}: {e}")
        except Exception as e:
             logger.error(f"[{client.exchange_name}] Unexpected error fetching funding rate for {symbol}: {e}", exc_info=True)
        return None

    # Add _fetch_ticker_safe if needed

    async def run(self) -> AsyncGenerator[List[ArbitrageOpportunity], None]:
        """Periodically calculates and yields arbitrage opportunities."""
        logger.info("Signal Generator starting run loop.")
        # TODO: Determine appropriate calculation frequency
        calculation_interval = 10 # seconds (adjust as needed)

        while not self._stop_event.is_set():
            start_time = time.monotonic()
            try:
                logger.debug("Calculating opportunities...")
                opportunities = await self._calculate_opportunities()
                if opportunities:
                    self._latest_opportunities = opportunities
                    logger.info(f"Generated {len(opportunities)} potential opportunities. Best utility: {opportunities[0].utility_score:.6f}")
                    yield opportunities # Yield the list for other components
                else:
                    logger.info("No new opportunities found in this cycle.")
                    yield [] # Yield empty list if none found

            except asyncio.CancelledError:
                logger.info("Signal Generator run loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in Signal Generator loop: {e}", exc_info=True)
                yield [] # Yield empty list on error

            # Wait for the next interval
            elapsed = time.monotonic() - start_time
            sleep_time = max(0, calculation_interval - elapsed)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=sleep_time)
                break # Stop event was set
            except asyncio.TimeoutError:
                continue # Timeout reached, continue loop

        logger.info("Signal Generator run loop finished.")

    def stop(self):
        """Signals the signal generator to stop."""
        logger.info("Signal Generator received stop signal.")
        self._stop_event.set()

    def get_latest_opportunities(self) -> List[ArbitrageOpportunity]:
        """Returns the most recently calculated opportunities."""
        return self._latest_opportunities 
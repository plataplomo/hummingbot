from __future__ import annotations  # Enable postponed evaluation

from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, getcontext  # Import Decimal and InvalidOperation
from typing import (  # Added Callable, Coroutine, Dict, Optional, List
    TYPE_CHECKING,
)

import numpy as np

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import (  # Import MarketData, OrderBook
    ArbitrageOpportunity,
    FundingRate,
    MarketData,
    Ticker,
)
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.utils.config import Config
from cyberdelta.utils.logging_config import get_logger  # <--- Use get_logger

if TYPE_CHECKING:
    from cyberdelta.core.models import ArbitrageOpportunity


logger = get_logger(__name__)  # <--- Use configured logger

# Set precision for Decimal
getcontext().prec = 28


class SignalGenerator:
    """
    Identifies funding rate arbitrage opportunities between exchanges.

    Responsible for:
    - Monitoring funding rates across exchanges
    - Calculating Net Funding Differential (NFD)
    - Computing expected profit metrics including costs
    - Generating and ranking arbitrage opportunities
    """

    def __init__(
        self, config: Config, data_handler: DataHandler, symbol_mapper: SymbolMapper
    ) -> None:
        """
        Initialize the signal generator.

        Args:
            config: Application configuration
            data_handler: DataHandler for market data
            symbol_mapper: SymbolMapper for translating symbols
        """
        self.config = config
        self.data_handler = data_handler
        self.symbol_mapper = symbol_mapper

        # Parameters from config - ensure Decimal where appropriate
        self.min_funding_differential = Decimal(
            str(config.get("strategy.funding_rate.min_funding_differential", 0.0001))
        )
        self.min_profit_threshold = Decimal(
            str(config.get("strategy.funding_rate.min_profit_threshold", 5.0))
        )
        self.default_slippage = Decimal(
            str(config.get("strategy.funding_rate.default_slippage", 0.001))  # 0.1%
        )
        self.slippage_sensitivity = Decimal(
            str(config.get("strategy.funding_rate.slippage_sensitivity", 0.5))
        )
        self.liquidity_threshold_usd = Decimal(
            str(config.get("strategy.funding_rate.liquidity_threshold_usd", 10000))
        )
        self.max_slippage_percent = Decimal(
            str(config.get("strategy.funding_rate.max_slippage_percent", 0.01))  # 1%
        )

        # Historical funding rates for volatility calculation
        # exchange -> internal_symbol -> deque[(timestamp, rate: Decimal)]
        self.historical_funding_rates: dict[str, dict[str, deque[tuple[datetime, Decimal]]]] = {}

        # Historical basis data for volatility calculation
        # internal_symbol -> deque[(timestamp, basis: Decimal)]
        self.historical_basis: dict[str, deque[tuple[datetime, Decimal]]] = {}

        # Sample period and count for funding rate history
        self.funding_sample_period = config.get(
            "strategy.funding_rate.funding_sample_period", 3600
        )  # seconds
        self.funding_sample_count = config.get("strategy.funding_rate.funding_sample_count", 24)

        # Risk aversion parameter (λ) for utility function (float is fine here)
        self.risk_aversion = config.get("strategy.funding_rate.risk_aversion", 1.0)

        # Initialize data structures
        self._initialize_data_structures()
        logger.info("SignalGenerator initialized.")

    def _initialize_data_structures(self) -> None:
        """Initialize data structures for historical data using SymbolMapper."""
        all_internal_symbols = self.symbol_mapper.get_all_internal_symbols()
        configured_exchanges = self.config.get("exchanges", {}).keys()

        enabled_exchanges = [
            ex_id
            for ex_id in configured_exchanges
            if self.config.get(f"exchanges.{ex_id}.enabled", False)
        ]

        logger.debug(
            f"Initializing data structures for enabled exchanges: {enabled_exchanges} and internal symbols: {all_internal_symbols}"
        )

        # Initialize historical funding rate storage
        for exchange_id in enabled_exchanges:
            self.historical_funding_rates[exchange_id] = {}
            for internal_symbol in all_internal_symbols:
                # Check if this exchange has a mapping for the internal symbol
                exchange_symbol = self.symbol_mapper.get_exchange_symbol(
                    internal_symbol, exchange_id
                )
                if exchange_symbol:
                    self.historical_funding_rates[exchange_id][internal_symbol] = deque()
                    logger.debug(
                        f"  Initialized funding deque for {exchange_id} / {internal_symbol} (maps to {exchange_symbol})"
                    )
                # else: # No need to log missing mappings, it's expected
                #    logger.debug(f"  No mapping found for {internal_symbol} on {exchange_id}, skipping funding deque.")

        # Initialize basis history using all known internal symbols
        for internal_symbol in all_internal_symbols:
            self.historical_basis[internal_symbol] = deque()
            logger.debug(f"  Initialized basis deque for {internal_symbol}")

        # Log the final structure for verification
        # logger.debug(f"Final historical_funding_rates structure: {self.historical_funding_rates}")
        # logger.debug(f"Final historical_basis structure: {self.historical_basis}")

        logger.info(
            f"Initialized historical data structures for {len(enabled_exchanges)} enabled exchanges and {len(all_internal_symbols)} internal symbols."
        )

    def update_historical_data(self) -> None:
        """
        Update historical funding rate and basis data with latest information.
        Should be called regularly to maintain up-to-date volatility calculations.
        Uses SymbolMapper for translation.
        """
        now = datetime.now(UTC)
        all_internal_symbols = self.symbol_mapper.get_all_internal_symbols()

        # Determine enabled exchanges directly from config
        configured_exchanges = self.config.get("exchanges", {}).keys()
        enabled_exchanges = [
            ex_id
            for ex_id in configured_exchanges
            if self.config.get(f"exchanges.{ex_id}.enabled", False)
        ]

        # --- Update funding rate history ---
        for exchange_id in enabled_exchanges:
            # Iterate through the internal symbols expected for this exchange based on initialization
            expected_internal_symbols = self.historical_funding_rates.get(exchange_id, {}).keys()

            for internal_symbol in expected_internal_symbols:
                # Get the corresponding exchange symbol using the mapper
                exchange_symbol = self.symbol_mapper.get_exchange_symbol(
                    internal_symbol, exchange_id
                )

                if not exchange_symbol:
                    # This indicates a possible inconsistency if the symbol was present during init
                    logger.error(
                        f"Symbol mapping inconsistency: Cannot find exchange symbol for "
                        f"internal symbol '{internal_symbol}' on '{exchange_id}', "
                        f"though it was expected during initialization. Skipping update."
                    )
                    continue

                # Fetch data using the exchange-specific symbol
                funding_data: FundingRate | None = self.data_handler.get_funding_rate(
                    exchange_id, exchange_symbol
                )

                if funding_data:
                    rate = funding_data.funding_rate
                    timestamp = now  # Use current time as timestamp

                    # Ensure rate is Decimal
                    if not isinstance(rate, Decimal):
                        try:
                            rate = Decimal(str(rate))
                        except Exception as e:
                            logger.warning(
                                f"Could not convert funding rate '{rate}' to Decimal for "
                                f"{exchange_id}/{exchange_symbol} (Internal: {internal_symbol}). "
                                f"Error: {e}"
                            )
                            continue  # Skip this data point

                    # Add to history using internal_symbol key
                    # Ensure the structure exists (might be overly cautious if init is correct)
                    if (
                        exchange_id in self.historical_funding_rates
                        and internal_symbol in self.historical_funding_rates[exchange_id]
                    ):
                        history_deque = self.historical_funding_rates[exchange_id][internal_symbol]
                        history_deque.append((timestamp, rate))

                        # Trim history using deque's efficient popleft
                        cutoff_time = now - timedelta(
                            seconds=self.funding_sample_period * self.funding_sample_count
                        )
                        while history_deque and history_deque[0][0] < cutoff_time:
                            history_deque.popleft()
                    else:
                        logger.error(
                            f"Historical funding rate deque not found for {exchange_id}/{internal_symbol} during update."
                        )

        # --- Update basis history (price difference between exchanges) ---
        for internal_symbol in all_internal_symbols:
            # Find exchanges that map this internal symbol
            valid_exchanges_for_symbol = []
            exchange_tickers: dict[str, Ticker] = {}  # Store valid tickers

            # Check all enabled exchanges
            for exchange_id in enabled_exchanges:
                # Get exchange symbol using mapper
                exchange_symbol = self.symbol_mapper.get_exchange_symbol(
                    internal_symbol, exchange_id
                )

                if exchange_symbol:  # Check if mapping exists
                    ticker: Ticker | None = self.data_handler.get_ticker(
                        exchange_id, exchange_symbol
                    )
                    # Ensure ticker and price are valid
                    if ticker and ticker.price is not None:
                        # Attempt to convert price to Decimal immediately for validation
                        try:
                            price_decimal = Decimal(str(ticker.price))
                            valid_exchanges_for_symbol.append(exchange_id)
                            # Store the ticker *after* ensuring its price is valid and convertible
                            exchange_tickers[exchange_id] = ticker
                            # Overwrite price with validated Decimal for consistency downstream
                            ticker.price = price_decimal
                        except (InvalidOperation, TypeError) as conversion_error:
                            logger.warning(
                                f"Could not convert ticker price '{ticker.price}' to Decimal for "
                                f"{exchange_id}/{exchange_symbol} (Internal: {internal_symbol}): {conversion_error}"
                            )
                            # Do not add this exchange/ticker if price is invalid
                # No else needed, if no exchange_symbol, we skip this exchange for this internal_symbol

            # Compute basis if enough valid tickers were found
            if len(valid_exchanges_for_symbol) >= 2:
                # Example: Basis between first two valid exchanges
                ex1 = valid_exchanges_for_symbol[0]
                ex2 = valid_exchanges_for_symbol[1]
                ticker1 = exchange_tickers.get(ex1)
                ticker2 = exchange_tickers.get(ex2)

                # Check if tickers and their prices (already validated as Decimal) exist
                if ticker1 and ticker1.price is not None and ticker2 and ticker2.price is not None:
                    # We know prices are Decimal here due to earlier checks
                    basis = ticker1.price - ticker2.price
                    timestamp = now  # Use current time for basis update

                    # Add to basis history for the internal symbol
                    if internal_symbol in self.historical_basis:
                        basis_deque = self.historical_basis[internal_symbol]
                        basis_deque.append((timestamp, basis))
                        # Trim history
                        cutoff_time = now - timedelta(
                            seconds=self.funding_sample_period * self.funding_sample_count
                        )
                        while basis_deque and basis_deque[0][0] < cutoff_time:
                            basis_deque.popleft()
                    else:
                        logger.error(
                            f"Historical basis deque not found for {internal_symbol} during update."
                        )
                # else: logger.debug(f"Skipping basis calculation for {internal_symbol}: Not enough valid ticker prices.")
            # else: logger.debug(f"Skipping basis calculation for {internal_symbol}: Need at least 2 exchanges with valid tickers.")

    def calculate_funding_rate_volatility(self, exchange: str, internal_symbol: str) -> Decimal:
        """Calculate the volatility (std dev) of historical funding rates."""
        if (
            exchange not in self.historical_funding_rates
            or internal_symbol not in self.historical_funding_rates[exchange]
        ):
            # logger.debug(f"No historical funding rate data for {exchange}/{internal_symbol}. Returning default volatility.")
            return Decimal("0.0001")  # Default low volatility if no data

        history_deque = self.historical_funding_rates[exchange][internal_symbol]
        if len(history_deque) < 2:
            # logger.debug(f"Insufficient historical funding rate data for {exchange}/{internal_symbol} (need >= 2). Returning default.")
            return Decimal("0.0001")  # Default low volatility

        # Extract rates (they should be Decimal)
        rates = [rate for _, rate in history_deque]

        # Use numpy for standard deviation, converting Decimals to floats for calculation
        try:
            # Convert Decimal list to list of floats for numpy
            rates_float = [float(r) for r in rates]
            std_dev = np.std(rates_float)
            # Convert result back to Decimal
            std_dev_decimal = Decimal(str(std_dev))
            # Return a minimum non-zero volatility
            return max(Decimal("1e-8"), std_dev_decimal)  # Ensure non-zero return
        except (InvalidOperation, TypeError, ValueError) as e:
            logger.error(
                f"Error calculating funding rate volatility for {exchange}/{internal_symbol}: {e}"
            )
            return Decimal("0.0001")  # Default on calculation error

    def calculate_basis_volatility(self, symbol: str) -> Decimal:
        """Calculate the volatility (std dev) of the historical price basis."""
        if symbol not in self.historical_basis:
            logger.debug(f"No historical basis data for {symbol}. Returning default volatility.")
            return Decimal("0.01")  # Default basis volatility

        history_deque = self.historical_basis[symbol]
        if len(history_deque) < 2:
            # logger.debug(f"Insufficient historical basis data for {symbol} (need >= 2). Returning default.")
            return Decimal("0.01")  # Default volatility

        basis_values = [basis for _, basis in history_deque]
        try:
            basis_float = [float(b) for b in basis_values]
            std_dev = np.std(basis_float)
            std_dev_decimal = Decimal(str(std_dev))
            return max(Decimal("1e-8"), std_dev_decimal)  # Ensure non-zero return
        except (InvalidOperation, TypeError, ValueError) as e:
            logger.error(f"Error calculating basis volatility for {symbol}: {e}")
            return Decimal("0.01")  # Default on calculation error

    def estimate_slippage(self, exchange: str, symbol: str) -> Decimal:
        """
        Estimate the slippage cost for trading on a given exchange and symbol.

        Args:
            exchange: Exchange name
            symbol: Trading symbol

        Returns:
            Estimated slippage as a Decimal
        """
        # Use historical data if available, otherwise use default
        if exchange in self.historical_slippage and symbol in self.historical_slippage[exchange]:
            slippage_data = self.historical_slippage[exchange][symbol]
            if slippage_data and len(slippage_data) > 0:
                # Calculate average slippage from historical data
                avg_slippage = sum(slippage_data) / len(slippage_data)
                if not isinstance(avg_slippage, Decimal):
                    avg_slippage = Decimal(str(avg_slippage))
                return avg_slippage

        # Fallback to configured value for the exchange or default
        base_slippage = self.config.get(
            f"exchanges.{exchange}.expected_slippage", self.default_slippage
        )

        # Ensure base_slippage is a Decimal
        if not isinstance(base_slippage, Decimal):
            base_slippage = Decimal(str(base_slippage))

        # Apply slippage sensitivity multiplier
        if not isinstance(self.slippage_sensitivity, Decimal):
            sensitivity = Decimal(str(self.slippage_sensitivity))
        else:
            sensitivity = self.slippage_sensitivity

        return base_slippage * sensitivity

    def generate_arbitrage_opportunities(
        self, funding_data: dict[str, dict[str, FundingRate | None]], market_data: MarketData
    ) -> list[ArbitrageOpportunity]:
        """
        Generate arbitrage opportunities based on funding rate differentials and market data.

        Args:
            funding_data: Dictionary mapping symbols to dictionaries mapping exchange names to FundingRate objects
            market_data: MarketData object with current ticker information

        Returns:
            List of arbitrage opportunities
        """
        if market_data is None or market_data.ticker_data is None:
            self.logger.warning(
                "Market data or ticker data is None - cannot generate arbitrage opportunities"
            )
            return []

        opportunities = []

        for symbol in self.tracked_symbols:
            # Ensure we have funding data for this symbol
            if symbol not in funding_data:
                continue

            exchanges_with_data = {
                exchange: data
                for exchange, data in funding_data[symbol].items()
                if data is not None
            }

            # We need at least two exchanges with data to create an arbitrage opportunity
            if len(exchanges_with_data) < 2:
                continue

            # Get ticker data for all exchanges for this symbol
            tickers = {}
            if symbol in market_data.ticker_data:
                tickers = market_data.ticker_data[symbol]

            # Check for funding rate opportunities
            funding_opportunities = self._check_funding_rate_opportunities(
                symbol, exchanges_with_data, tickers
            )

            # Add valid opportunities to the list
            opportunities.extend(funding_opportunities)

        # Sort opportunities by net funding differential in descending order
        opportunities.sort(key=lambda x: float(x.net_funding_differential), reverse=True)
        return opportunities

    def _check_funding_rate_opportunities(
        self, symbol: str, exchanges_with_data: dict[str, FundingRate], tickers: dict[str, Ticker]
    ) -> list[ArbitrageOpportunity]:
        """
        Check for funding rate arbitrage opportunities between exchanges for a given symbol.

        Args:
            symbol: The trading symbol to check
            exchanges_with_data: Dictionary of exchange names to funding rate data
            tickers: Dictionary of exchange names to ticker data

        Returns:
            List of arbitrage opportunities
        """
        opportunities = []

        # Check each pair of exchanges
        exchanges = list(exchanges_with_data.keys())
        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                exchange_a = exchanges[i]
                exchange_b = exchanges[j]

                funding_a = exchanges_with_data[exchange_a]
                funding_b = exchanges_with_data[exchange_b]

                # Ensure we have valid funding rates
                if funding_a is None or funding_b is None:
                    continue

                try:
                    rate_a = funding_a.funding_rate
                    rate_b = funding_b.funding_rate

                    # Ensure rates are not None and convert to Decimal if needed
                    if rate_a is None or rate_b is None:
                        continue

                    if not isinstance(rate_a, Decimal):
                        rate_a = Decimal(str(rate_a))
                    if not isinstance(rate_b, Decimal):
                        rate_b = Decimal(str(rate_b))

                    # Calculate the funding rate differential
                    funding_differential = rate_b - rate_a

                    # Check if the differential exceeds our threshold
                    if abs(funding_differential) < self.min_funding_differential:
                        continue

                    # Check if we have ticker data for both exchanges
                    ticker_a = tickers.get(exchange_a)
                    ticker_b = tickers.get(exchange_b)

                    if ticker_a is None or ticker_b is None:
                        continue

                    # Ensure we have valid prices
                    if ticker_a.price is None or ticker_b.price is None:
                        continue

                    # Convert prices to Decimal if needed
                    price_a = ticker_a.price
                    price_b = ticker_b.price

                    if not isinstance(price_a, Decimal):
                        price_a = Decimal(str(price_a))
                    if not isinstance(price_b, Decimal):
                        price_b = Decimal(str(price_b))

                    # Calculate the funding payment in USD terms
                    funding_payment_a = price_a * rate_a
                    funding_payment_b = price_b * rate_b

                    # Calculate net funding differential
                    net_funding_differential = funding_payment_b - funding_payment_a

                    # Calculate expected profit after slippage
                    slippage_a = self.estimate_slippage(exchange_a, symbol)
                    slippage_b = self.estimate_slippage(exchange_b, symbol)

                    # Convert slippage to Decimal if needed
                    if not isinstance(slippage_a, Decimal):
                        slippage_a = Decimal(str(slippage_a))
                    if not isinstance(slippage_b, Decimal):
                        slippage_b = Decimal(str(slippage_b))

                    total_slippage = slippage_a + slippage_b
                    expected_profit = abs(net_funding_differential) - total_slippage

                    # Check if expected profit meets our threshold
                    if expected_profit < self.min_profit_threshold:
                        continue

                    # Create an arbitrage opportunity
                    if funding_differential > Decimal("0"):
                        # Short exchange_b, long exchange_a
                        opportunity = ArbitrageOpportunity(
                            symbol=symbol,
                            long_exchange=exchange_a,
                            short_exchange=exchange_b,
                            funding_rate_long=rate_a,
                            funding_rate_short=rate_b,
                            funding_differential=funding_differential,
                            net_funding_differential=net_funding_differential,
                            expected_profit=expected_profit,
                            timestamp=datetime.now(UTC),
                        )
                    else:
                        # Short exchange_a, long exchange_b
                        opportunity = ArbitrageOpportunity(
                            symbol=symbol,
                            long_exchange=exchange_b,
                            short_exchange=exchange_a,
                            funding_rate_long=rate_b,
                            funding_rate_short=rate_a,
                            funding_differential=abs(funding_differential),
                            net_funding_differential=abs(net_funding_differential),
                            expected_profit=expected_profit,
                            timestamp=datetime.now(UTC),
                        )

                    opportunities.append(opportunity)

                except (InvalidOperation, TypeError, ValueError) as e:
                    self.logger.warning(
                        f"Error calculating funding arbitrage for {symbol} between {exchange_a} and {exchange_b}: {e}"
                    )
                    continue

        return opportunities

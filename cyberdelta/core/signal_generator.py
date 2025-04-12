from __future__ import annotations  # Enable postponed evaluation

import decimal
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, getcontext  # Import Decimal and InvalidOperation
from typing import TYPE_CHECKING, Any, Callable, Coroutine  # Added Callable, Coroutine

import numpy as np
import pandas as pd

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import ArbitrageOpportunity, FundingRate, Ticker, MarketData, OrderBook  # Import MarketData, OrderBook
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
                if (
                    ticker1 and ticker1.price is not None and
                    ticker2 and ticker2.price is not None
                ):
                    # We know prices are Decimal here due to earlier checks
                    basis = ticker1.price - ticker2.price
                    timestamp = now # Use current time for basis update

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
                         logger.error(f"Historical basis deque not found for {internal_symbol} during update.")
                # else: logger.debug(f"Skipping basis calculation for {internal_symbol}: Not enough valid ticker prices.")
            # else: logger.debug(f"Skipping basis calculation for {internal_symbol}: Need at least 2 exchanges with valid tickers.")

    def calculate_funding_rate_volatility(self, exchange: str, internal_symbol: str) -> Decimal:
        """Calculate the volatility (std dev) of historical funding rates."""
        if exchange not in self.historical_funding_rates or internal_symbol not in self.historical_funding_rates[exchange]:
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
            return max(Decimal("1e-8"), std_dev_decimal) # Ensure non-zero return
        except (InvalidOperation, TypeError, ValueError) as e:
            logger.error(f"Error calculating funding rate volatility for {exchange}/{internal_symbol}: {e}")
            return Decimal("0.0001") # Default on calculation error

    def calculate_basis_volatility(self, symbol: str) -> Decimal:
        """Calculate the volatility (std dev) of the historical price basis."""
        if symbol not in self.historical_basis:
            logger.debug(f"No historical basis data for {symbol}. Returning default volatility.")
            return Decimal("0.01") # Default basis volatility

        history_deque = self.historical_basis[symbol]
        if len(history_deque) < 2:
            # logger.debug(f"Insufficient historical basis data for {symbol} (need >= 2). Returning default.")
            return Decimal("0.01") # Default volatility

        basis_values = [basis for _, basis in history_deque]
        try:
            basis_float = [float(b) for b in basis_values]
            std_dev = np.std(basis_float)
            std_dev_decimal = Decimal(str(std_dev))
            return max(Decimal("1e-8"), std_dev_decimal) # Ensure non-zero return
        except (InvalidOperation, TypeError, ValueError) as e:
             logger.error(f"Error calculating basis volatility for {symbol}: {e}")
             return Decimal("0.01") # Default on calculation error

    def estimate_slippage(self, symbol: str, order_size: Decimal, exchange: str) -> Decimal:
        """
        Estimate slippage based on order size and market liquidity (simplified).
        Uses internal_symbol implicitly via the provided symbol argument.

        Args:
            symbol: The internal symbol for the asset.
            order_size: Proposed order size in USD (Decimal).
            exchange: The exchange where the order would be placed.

        Returns:
            Estimated slippage cost as a fraction of price (Decimal).
        """
        # Get exchange-specific symbol
        exchange_symbol = self.symbol_mapper.get_exchange_symbol(symbol, exchange)
        if not exchange_symbol:
            logger.warning(f"Cannot estimate slippage for {symbol} on {exchange}: No mapping found.")
            return self.default_slippage # Return default if no mapping

        try:
            # --- Fetch Order Book --- #
            order_book: OrderBook | None = self.data_handler.get_order_book(exchange, exchange_symbol, depth=5) # Get shallow depth

            if not order_book:
                logger.debug(f"No order book data for {exchange}/{exchange_symbol} to estimate slippage. Using default.")
                return self.default_slippage

            # --- Calculate Liquidity --- #
            # Check liquidity on both sides (simplified: sum top 5 levels in USD)
            ask_liquidity = sum(Decimal(str(p)) * Decimal(str(q)) for p, q in order_book.asks[:5]) if order_book.asks else Decimal("0")
            bid_liquidity = sum(Decimal(str(p)) * Decimal(str(q)) for p, q in order_book.bids[:5]) if order_book.bids else Decimal("0")
            total_top5_liquidity = ask_liquidity + bid_liquidity

            if total_top5_liquidity <= Decimal("0"):
                 logger.debug(f"Zero liquidity in top 5 levels for {exchange}/{exchange_symbol}. Using default slippage.")
                 return self.default_slippage

            # --- Estimate Slippage --- #
            # Simple model: slippage increases with size relative to liquidity
            # Ensure order_size is Decimal
            size_ratio = float(order_size / total_top5_liquidity)
            if size_ratio < 0:
                 logger.warning(f"Calculated negative size ratio ({size_ratio}) for slippage of {symbol}. Using base.")
                 size_ratio = 0.0 # Correct negative ratio

            # Slippage = BaseSlippage + Sensitivity * (Size / Liquidity)^2
            # Perform calculation using float for simplicity here, then convert back
            slippage_estimate_float = float(self.default_slippage) + float(self.slippage_sensitivity) * (size_ratio ** 2)

            # Convert final estimate back to Decimal
            slippage_estimate = Decimal(str(slippage_estimate_float))

            # Cap slippage at max allowed
            final_slippage = min(slippage_estimate, self.max_slippage_percent)

            # logger.debug(f"Slippage estimation for {exchange}/{exchange_symbol}: Size=${order_size:.2f}, Liq=${total_top5_liquidity:.2f}, Ratio={size_ratio:.4f}, Est={slippage_estimate:.6f}, Final={final_slippage:.6f}")

            return final_slippage

        except (InvalidOperation, TypeError, ValueError) as e:
            logger.error(f"Error estimating slippage for {exchange}/{exchange_symbol}: {e}")
            return self.default_slippage # Return default on any calculation error
        except Exception as e:
             logger.error(f"Unexpected error estimating slippage for {exchange}/{exchange_symbol}: {e}", exc_info=True)
             return self.default_slippage

    def generate_opportunities(self) -> list[ArbitrageOpportunity]:
        """
        Scan exchanges and symbols to find potential arbitrage opportunities.
        Uses SymbolMapper to iterate through internal symbols and map to exchanges.
        """
        logger.info("Generating arbitrage opportunities...")
        opportunities = []
        all_internal_symbols = self.symbol_mapper.get_all_internal_symbols()

        configured_exchanges = list(self.config.get("exchanges", {}).keys())
        enabled_exchanges = [
            ex_id for ex_id in configured_exchanges
            if self.config.get(f"exchanges.{ex_id}.enabled", False)
        ]

        if len(enabled_exchanges) < 2:
             logger.warning("Need at least two enabled exchanges to find arbitrage opportunities.")
             return []

        # Iterate through each internal symbol
        for internal_symbol in all_internal_symbols:
            logger.debug(f"Checking opportunities for internal symbol: {internal_symbol}")
            exchange_data: dict[str, tuple[Decimal, Decimal | None]] = {}

            # Collect price and rate data for this symbol from all enabled exchanges
            for exchange_id in enabled_exchanges:
                exchange_symbol = self.symbol_mapper.get_exchange_symbol(
                    internal_symbol, exchange_id
                )
                if not exchange_symbol:
                    # logger.debug(f"  Skipping {exchange_id}: No mapping for {internal_symbol}")
                    continue

                # Get data from DataHandler
                ticker = self.data_handler.get_ticker(exchange_id, exchange_symbol)
                funding_rate = self.data_handler.get_funding_rate(exchange_id, exchange_symbol)

                # Log retrieved data for debugging
                logger.debug(f"    {exchange_id} Ticker ({exchange_symbol}): {ticker}")
                logger.debug(f"    {exchange_id} Funding ({exchange_symbol}): {funding_rate}")

                # --- Data Validation ---
                if not all([ticker, funding_rate]):
                    logger.debug(
                        f"  Missing required data for {internal_symbol} (mapped: {exchange_symbol}) "
                        f"on {exchange_id}. Skipping opportunity check."
                    )
                    continue

                if ticker.price is None or funding_rate.funding_rate is None:
                    logger.debug(
                        f"  Missing ticker price or funding rate for {internal_symbol} (mapped: {exchange_symbol}) "
                        f"on {exchange_id}. Skipping opportunity check."
                    )
                    continue

                # Ensure necessary components for calculation are Decimal
                try:
                    rate = funding_rate.funding_rate
                    price = ticker.price
                    if not isinstance(rate, Decimal):
                        rate = Decimal(str(rate))
                    if not isinstance(price, Decimal):
                        price = Decimal(str(price))
                except (InvalidOperation, TypeError, ValueError) as e:
                    logger.warning(
                        f"Failed to convert funding/ticker data to Decimal for {internal_symbol} "
                        f"(mapped: {exchange_symbol}) on {exchange_id}: {e}. Skipping."
                    )
                    continue
                # --- End Data Validation ---

                # Store data in exchange_data
                exchange_data[exchange_id] = (rate, price)

            # Rank opportunities based on utility score
            # Ensure utility_score exists and handle None for sorting
            def sort_key(op: ArbitrageOpportunity) -> float:
                # Return negative infinity for None scores so they rank last
                return op.utility_score if op.utility_score is not None else -float('inf')

            opportunities.sort(key=sort_key, reverse=True)

            return opportunities

    def _check_funding_rate_opportunity(
        self,
        exchange1: str,
        exchange2: str,
        internal_symbol: str,
    ) -> ArbitrageOpportunity | None:
        """Check for funding rate arbitrage between two exchanges."""
        now = datetime.now(UTC)
        logger.debug(
            f"Checking opportunity for {internal_symbol} between {exchange1} and {exchange2}"
        )

        # Get exchange-specific symbols
        symbol1 = self.config.get(f"exchanges.{exchange1}.symbols.{internal_symbol}")
        symbol2 = self.config.get(f"exchanges.{exchange2}.symbols.{internal_symbol}")
        if not symbol1 or not symbol2:
            logger.warning(
                f"Symbol mapping missing for {internal_symbol} on {exchange1} or {exchange2}"
            )
            return None

        # Get data from DataHandler
        ticker1 = self.data_handler.get_ticker(exchange1, symbol1)
        ticker2 = self.data_handler.get_ticker(exchange2, symbol2)
        funding_rate1 = self.data_handler.get_funding_rate(exchange1, symbol1)
        funding_rate2 = self.data_handler.get_funding_rate(exchange2, symbol2)

        # --- Start Fix 36 Logging ---
        logger.debug(f"  {exchange1} Ticker: {ticker1}")
        logger.debug(f"  {exchange2} Ticker: {ticker2}")
        logger.debug(f"  {exchange1} FundingRate: {funding_rate1}")
        logger.debug(f"  {exchange2} FundingRate: {funding_rate2}")

        # Check if data is available and valid
        if not all([ticker1, ticker2, funding_rate1, funding_rate2]):
            logger.debug("  Missing ticker or funding rate data, cannot calculate opportunity.")
            return None

        if ticker1.price is None or ticker2.price is None:
            logger.debug("  Ticker price is None, cannot calculate opportunity.")
            return None
        # --- End Fix 36 Logging ---

        # Perform checks (ensure Decimal)
        try:
            # Convert rates to Decimal, default to 0 if None/invalid
            rate1 = funding_rate1.funding_rate if funding_rate1 else Decimal("0")
            rate2 = funding_rate2.funding_rate if funding_rate2 else Decimal("0")
            if not isinstance(rate1, Decimal):
                rate1 = Decimal(str(rate1))
            if not isinstance(rate2, Decimal):
                rate2 = Decimal(str(rate2))

            # Convert prices to Decimal, default to 0 if None/invalid
            price1 = ticker1.price
            price2 = ticker2.price
            if not isinstance(price1, Decimal):
                price1 = Decimal(str(price1))
            if not isinstance(price2, Decimal):
                price2 = Decimal(str(price2))

        except Exception as e:
            logger.error(f"Error converting data for {internal_symbol}: {e}")
            return None

        # Calculate Net Funding Differential (NFD)
        net_funding_differential = rate1 - rate2
        logger.debug(f"  NFD: {net_funding_differential:.8f}")  # Fix 36 Logging

        # Check minimum differential threshold
        if abs(net_funding_differential) < self.min_funding_differential:
            logger.debug(
                f"  NFD below threshold ({self.min_funding_differential:.8f})"
            )  # Fix 36 Logging
            return None

        # Determine long/short exchanges
        if net_funding_differential > 0:  # rate1 > rate2 -> Short exchange1, Long exchange2
            short_exchange, long_exchange = exchange1, exchange2
            short_rate, long_rate = rate1, rate2
            short_ticker, long_ticker = ticker1, ticker2
        else:  # rate1 < rate2 -> Long exchange1, Short exchange2
            long_exchange, short_exchange = exchange1, exchange2
            long_rate, short_rate = rate1, rate2
            long_ticker, short_ticker = ticker1, ticker2

        # Prices for entry
        long_entry_price = long_ticker.ask
        short_entry_price = short_ticker.bid

        # Calculate Basis and Volatility
        basis = price1 - price2
        basis_volatility = self.calculate_basis_volatility(internal_symbol)
        funding_rate_volatility = (
            self.calculate_funding_rate_volatility(exchange1, internal_symbol)
            + self.calculate_funding_rate_volatility(exchange2, internal_symbol)
        ) / 2
        logger.debug(
            f"  Basis Vol: {basis_volatility:.8f}, Funding Vol: {funding_rate_volatility:.8f}"
        )  # Fix 36 Logging

        # Estimate Costs (Assuming $1000 notional for cost estimation)
        estimated_trade_size = Decimal("1000.0")
        long_slippage = self.estimate_slippage(long_exchange, estimated_trade_size, long_exchange)
        short_slippage = self.estimate_slippage(
            short_exchange, estimated_trade_size, short_exchange
        )
        long_fee_rate = Decimal(str(self.config.get(f"exchanges.{long_exchange}.fee_rate", 0.001)))
        short_fee_rate = Decimal(
            str(self.config.get(f"exchanges.{short_exchange}.fee_rate", 0.001))
        )
        total_costs = estimated_trade_size * (
            long_slippage + short_slippage + long_fee_rate + short_fee_rate
        )
        logger.debug(f"  Estimated Costs (for $1000): {total_costs:.4f}")  # Fix 36 Logging

        # Calculate Expected Profit (adjust based on $1000 size)
        expected_profit = (estimated_trade_size * abs(net_funding_differential)) - total_costs
        logger.debug(f"  Expected Profit (for $1000): {expected_profit:.4f}")  # Fix 36 Logging

        # Check minimum profit threshold
        if expected_profit < self.min_profit_threshold:
            logger.debug(
                f"  Expected Profit below threshold (${self.min_profit_threshold})"
            )  # Fix 36 Logging
            return None

        # Calculate Utility Score
        combined_volatility = basis_volatility + funding_rate_volatility  # Simple sum for now
        utility_score = float(expected_profit - (self.risk_aversion * (combined_volatility**2)))
        logger.debug(f"  Utility Score: {utility_score:.4f}")  # Fix 36 Logging

        # Create opportunity
        opportunity = ArbitrageOpportunity(
            symbol=internal_symbol,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            long_price=long_entry_price,
            short_price=short_entry_price,
            long_funding_rate=long_rate,
            short_funding_rate=short_rate,
            net_funding_differential=abs(net_funding_differential),
            timestamp=now,
            expected_profit=expected_profit,  # Store profit based on $1000 estimate
            utility_score=utility_score,
            basis_volatility=float(basis_volatility),
            confidence=None,  # Confidence could be derived later
            optimal_size=None,  # Optimal size determined by RiskManager
        )

        logger.info(f"Generated opportunity: {opportunity}")
        return opportunity

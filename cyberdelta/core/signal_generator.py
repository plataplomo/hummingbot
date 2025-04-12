from __future__ import annotations  # Enable postponed evaluation

import decimal
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, getcontext  # Import Decimal and InvalidOperation
from typing import TYPE_CHECKING  # Added TYPE_CHECKING and Optional

import numpy as np

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import ArbitrageOpportunity, FundingRate, Ticker
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
                        except decimal.InvalidOperation:
                            logger.warning(
                                f"Could not convert ticker price '{ticker.price}' to Decimal for "
                                f"{exchange_id}/{exchange_symbol} (Internal: {internal_symbol})"
                            )
                            # Do not add this exchange/ticker if price is invalid
                # No else needed, if no exchange_symbol, we skip this exchange for this internal_symbol

            # Need at least two valid prices to calculate basis
            if len(valid_exchanges_for_symbol) >= 2:
                # Use the first two valid exchanges found (simple approach)
                # A more robust approach might average prices or use specific pairs
                exchange1, exchange2 = (
                    valid_exchanges_for_symbol[0],
                    valid_exchanges_for_symbol[1],
                )
                ticker1 = exchange_tickers[exchange1]
                ticker2 = exchange_tickers[exchange2]

                # Prices should already be Decimal due to checks above
                price1 = ticker1.price
                price2 = ticker2.price

                # Double-check types just in case (defensive programming)
                if isinstance(price1, Decimal) and isinstance(price2, Decimal):
                    basis = price1 - price2

                    # Add to historical data using internal symbol key
                    # Ensure the deque exists
                    if internal_symbol in self.historical_basis:
                        basis_deque = self.historical_basis[internal_symbol]
                        basis_deque.append((now, basis))

                        # Trim to keep only recent samples using popleft
                        cutoff_time = now - timedelta(
                            seconds=self.funding_sample_period * self.funding_sample_count
                        )
                        while basis_deque and basis_deque[0][0] < cutoff_time:
                            basis_deque.popleft()
                    else:
                        logger.error(
                            f"Historical basis deque not found for {internal_symbol} during update."
                        )

                else:
                    # This case should ideally not be reached due to earlier checks
                    logger.error(
                        f"Unexpected non-Decimal price found when calculating basis for "
                        f"'{internal_symbol}' between {exchange1} and {exchange2}. "
                        f"Price1: {price1} ({type(price1)}), Price2: {price2} ({type(price2)})"
                    )
            # else: No basis calculation possible if fewer than 2 prices found

    def calculate_funding_rate_volatility(self, exchange: str, internal_symbol: str) -> Decimal:
        """Calculates the volatility of historical funding rates."""
        # Access history using internal_symbol
        history = self.historical_funding_rates.get(exchange, {}).get(internal_symbol, deque())
        if len(history) < 2:
            return Decimal("0.0")

        # Extract Decimal rates for calculation (deque supports iteration)
        rates = [rate for _, rate in history]
        # Ensure rates are float for numpy calculation, then convert result back to Decimal
        if not rates:
            return Decimal("0.0")
        # Convert list of Decimals to numpy array of floats for std calculation
        rates_float = np.array([float(r) for r in rates])
        volatility_float = np.std(rates_float)
        return Decimal(str(volatility_float))

    def calculate_basis_volatility(self, symbol: str) -> Decimal:
        """
        Calculate the standard deviation of the basis for a symbol.
        Returns Decimal.
        """
        if symbol not in self.historical_basis or len(self.historical_basis[symbol]) < 2:
            return Decimal("0.0")

        # Basis values are already stored as Decimal
        basis_values = [b[1] for b in self.historical_basis[symbol]]

        # Calculate standard deviation (convert to float for numpy, back to Decimal)
        basis_values_float = np.array([float(b) for b in basis_values])
        volatility_float = np.std(basis_values_float)
        return Decimal(str(volatility_float))

    def estimate_slippage(self, symbol: str, order_size: Decimal, exchange: str) -> Decimal:
        """
        Estimate slippage based on order size and liquidity.
        Returns Decimal percentage.

        Args:
            symbol: Trading symbol (exchange-specific)
            order_size: Size of the order in USD (Decimal)
            exchange: Exchange identifier

        Returns:
            Estimated slippage as a Decimal percentage (e.g., 0.001 for 0.1%)
        """
        # Get orderbook from data handler
        orderbook = self.data_handler.get_orderbook(exchange, symbol)
        if (
            orderbook is None
            or not hasattr(orderbook, "bids")
            or not hasattr(orderbook, "asks")
            or not orderbook.bids
            or not orderbook.asks
        ):
            logger.warning(
                f"Orderbook data incomplete or missing for {symbol} on {exchange}. "
                f"Using default slippage: {self.default_slippage}"
            )
            return self.default_slippage  # Return default if no depth info

        # Find relevant price level based on size
        cumulative_size = Decimal("0")
        impacted_price = None

        # Determine if it's likely a buy (hitting asks) or sell (hitting bids)
        # This is an estimation; actual order type might vary
        # Assume order_size > 0 means buying, < 0 means selling (convention)
        if order_size > 0:  # Estimate buy slippage
            ref_price = orderbook.asks[0][0]  # Best ask
            for price, size in orderbook.asks:
                cumulative_size += price * size  # Value at this level
                if cumulative_size >= order_size:
                    impacted_price = price
                    break
            if impacted_price is None:  # Order larger than visible book depth
                impacted_price = orderbook.asks[-1][0]  # Use worst visible ask
                logger.warning(
                    f"Order size {order_size} exceeds visible ask depth for {symbol}. "
                    f"Using worst ask price {impacted_price} for slippage estimate."
                )

        elif order_size < 0:  # Estimate sell slippage
            ref_price = orderbook.bids[0][0]  # Best bid
            abs_order_size = abs(order_size)
            for price, size in orderbook.bids:
                cumulative_size += price * size
                if cumulative_size >= abs_order_size:
                    impacted_price = price
                    break
            if impacted_price is None:
                impacted_price = orderbook.bids[-1][0]  # Use worst visible bid
                logger.warning(
                    f"Order size {order_size} exceeds visible bid depth for {symbol}. "
                    f"Using worst bid price {impacted_price} for slippage estimate."
                )
        else:
            return Decimal("0")  # No slippage for zero size order

        if impacted_price is None or ref_price is None or ref_price == Decimal("0"):
            logger.warning(
                f"Could not determine valid reference or impacted price for {symbol}. "
                f"Using default slippage."
            )
            return self.default_slippage

        # Calculate slippage percentage
        slippage = abs(impacted_price - ref_price) / ref_price

        # Ensure a tiny minimum slippage for non-zero orders if calculation yields zero
        # to account for potential execution nuances not captured by simple LOB scan.
        MIN_SLIPPAGE = Decimal("1e-9")
        if order_size != Decimal("0") and slippage == Decimal("0"):
            slippage = MIN_SLIPPAGE

        # Apply sensitivity/scaling if needed (optional)
        # slippage *= (order_size / self.liquidity_threshold_usd) ** self.slippage_sensitivity

        # Cap slippage
        slippage = min(slippage, self.max_slippage_percent)

        return slippage

    def generate_opportunities(self) -> list[ArbitrageOpportunity]:
        """
        Generate a list of arbitrage opportunities sorted by utility score.
        Uses SymbolMapper for translation.
        """
        opportunities = []
        processed_pairs = (
            set()
        )  # Keep track of processed pairs (exchange1, exchange2, internal_symbol)
        now = datetime.now(UTC)

        # Determine enabled exchanges from config
        configured_exchanges = self.config.get("exchanges", {}).keys()
        enabled_exchanges = [
            ex_id
            for ex_id in configured_exchanges
            if self.config.get(f"exchanges.{ex_id}.enabled", False)
        ]

        if len(enabled_exchanges) < 2:
            logger.info(
                "Need at least two enabled exchanges to generate funding rate opportunities."
            )
            return []  # Return early

        logger.debug(f"Generating opportunities across enabled exchanges: {enabled_exchanges}")

        # Get all unique internal symbols known to the mapper
        all_internal_symbols = self.symbol_mapper.get_all_internal_symbols()

        # Iterate through all pairs of enabled exchanges
        for i in range(len(enabled_exchanges)):
            for j in range(i + 1, len(enabled_exchanges)):
                exchange1 = enabled_exchanges[i]
                exchange2 = enabled_exchanges[j]

                # Find common *internal* symbols supported by *both* exchanges using the mapper
                symbols1 = set(
                    self.symbol_mapper.get_internal_symbols_for_exchange(exchange1).values()
                )
                symbols2 = set(
                    self.symbol_mapper.get_internal_symbols_for_exchange(exchange2).values()
                )
                common_internal_symbols = list(symbols1.intersection(symbols2))

                logger.debug(
                    f"Checking pair: {exchange1} vs {exchange2}. Common internal symbols: {common_internal_symbols}"
                )

                if not common_internal_symbols:
                    continue  # Skip if no common symbols

                for internal_symbol in common_internal_symbols:
                    # Avoid processing the same pair twice (e.g., A-B vs B-A)
                    pair_key = tuple(sorted((exchange1, exchange2))) + (internal_symbol,)
                    if pair_key in processed_pairs:
                        continue
                    processed_pairs.add(pair_key)  # Mark as processed

                    logger.debug(
                        f"Processing internal symbol: '{internal_symbol}' for pair {exchange1}/{exchange2}"
                    )

                    # Get exchange-specific symbols using the mapper
                    symbol1 = self.symbol_mapper.get_exchange_symbol(internal_symbol, exchange1)
                    symbol2 = self.symbol_mapper.get_exchange_symbol(internal_symbol, exchange2)

                    # --- CRITICAL: Check if mapping was successful ---
                    if not symbol1 or not symbol2:
                        logger.error(  # Use ERROR level as this indicates a config/logic issue
                            f"Symbol mapping failed for internal symbol '{internal_symbol}' on {exchange1} ('{symbol1}') "
                            f"or {exchange2} ('{symbol2}'). Skipping opportunity check. Verify configuration."
                        )
                        continue  # Skip this symbol for this pair
                    # --- End Critical Check ---

                    logger.debug(
                        f"  Mapped symbols: {exchange1}='{symbol1}', {exchange2}='{symbol2}'"
                    )

                    # Get latest data using the retrieved *exchange-specific* symbols
                    funding1 = self.data_handler.get_funding_rate(exchange1, symbol1)
                    funding2 = self.data_handler.get_funding_rate(exchange2, symbol2)
                    ticker1 = self.data_handler.get_ticker(exchange1, symbol1)
                    ticker2 = self.data_handler.get_ticker(exchange2, symbol2)

                    # Log retrieved data for debugging
                    logger.debug(f"    {exchange1} Funding ({symbol1}): {funding1}")
                    logger.debug(f"    {exchange2} Funding ({symbol2}): {funding2}")
                    logger.debug(f"    {exchange1} Ticker ({symbol1}): {ticker1}")
                    logger.debug(f"    {exchange2} Ticker ({symbol2}): {ticker2}")

                    # --- Data Validation ---
                    if not all([funding1, funding2, ticker1, ticker2]):
                        logger.debug(
                            f"  Missing required data for {internal_symbol} (mapped: {symbol1}/{symbol2}) "
                            f"on {exchange1}/{exchange2}. Skipping opportunity check."
                        )
                        continue

                    if ticker1.price is None or ticker2.price is None:
                        logger.debug(
                            f"  Missing ticker price for {internal_symbol} (mapped: {symbol1}/{symbol2}) "
                            f"on {exchange1}/{exchange2}. Skipping opportunity check."
                        )
                        continue

                    # Ensure necessary components for calculation are Decimal
                    try:
                        rate1 = Decimal(str(funding1.funding_rate))
                        rate2 = Decimal(str(funding2.funding_rate))
                        # Use ask/bid for potential trade prices
                        ask1 = Decimal(str(ticker1.ask)) if ticker1.ask is not None else None
                        bid1 = Decimal(str(ticker1.bid)) if ticker1.bid is not None else None
                        ask2 = Decimal(str(ticker2.ask)) if ticker2.ask is not None else None
                        bid2 = Decimal(str(ticker2.bid)) if ticker2.bid is not None else None

                        # Check if all required prices for opportunity checks are available
                        if None in [ask1, bid1, ask2, bid2]:
                            logger.debug(
                                f" Missing ask/bid price for {internal_symbol} ({symbol1}/{symbol2}). Skipping."
                            )
                            continue

                    except (InvalidOperation, TypeError, ValueError) as e:  # Added ValueError
                        logger.warning(
                            f"Failed to convert funding/ticker data to Decimal for {internal_symbol} "
                            f"(mapped: {symbol1}/{symbol2}) on {exchange1}/{exchange2}: {e}. Skipping."
                        )
                        continue
                    # --- End Data Validation ---

                    # Calculate basis volatility using the *internal* symbol
                    basis_volatility = self.calculate_basis_volatility(internal_symbol)

                    # Opportunity Check 1: rate1 < rate2 => Long exchange2, Short exchange1
                    if rate1 < rate2:
                        net_differential = rate2 - rate1
                        logger.debug(
                            f"  Potential NFD (rate2-rate1): {rate2} - {rate1} = {net_differential}"
                        )
                        if (
                            net_differential > self.min_funding_differential
                        ):  # NFD is already positive
                            long_exchange_actual, short_exchange_actual = exchange2, exchange1
                            long_rate_actual, short_rate_actual = rate2, rate1
                            # Buy at ask2 (higher price), Sell at bid1 (lower price)
                            long_price_actual, short_price_actual = ask2, bid1

                            logger.debug(
                                f"    NFD {net_differential} > threshold {self.min_funding_differential}. "
                                f"Opportunity: Long {long_exchange_actual}/{symbol2}, Short {short_exchange_actual}/{symbol1}"
                            )
                            try:
                                # Simplified utility calculation (replace with more sophisticated model later)
                                optimal_size_placeholder = Decimal("1.0")  # Placeholder size
                                expected_profit = (
                                    net_differential * optimal_size_placeholder
                                )  # Per unit size
                                utility_score = float(expected_profit) - self.risk_aversion * (
                                    float(basis_volatility) ** 2
                                )

                                logger.debug(
                                    f"      Expected Profit/Unit: {expected_profit}, Basis Vol: {basis_volatility}, Utility: {utility_score}"
                                )

                                opportunity = ArbitrageOpportunity(
                                    symbol=internal_symbol,  # Use internal symbol
                                    long_exchange=long_exchange_actual,
                                    short_exchange=short_exchange_actual,
                                    long_price=long_price_actual,
                                    short_price=short_price_actual,
                                    long_funding_rate=long_rate_actual,
                                    short_funding_rate=short_rate_actual,
                                    net_funding_differential=net_differential,
                                    timestamp=now,
                                    optimal_size=None,  # To be determined later
                                    expected_profit=expected_profit,  # Per unit
                                    confidence=None,
                                    basis_volatility=float(basis_volatility),
                                    utility_score=utility_score,
                                )
                                opportunities.append(opportunity)
                                logger.info(
                                    f"  Appended Opportunity: Long {long_exchange_actual}/{symbol2} vs "
                                    f"Short {short_exchange_actual}/{symbol1} for '{internal_symbol}'"
                                )
                            except Exception as e:
                                logger.error(
                                    f"Error creating opportunity (Long {long_exchange_actual}/Short {short_exchange_actual}) "
                                    f"for symbol '{internal_symbol}' (mapped: {symbol1}/{symbol2}): {e}",
                                    exc_info=True,
                                )
                        else:
                            logger.debug(
                                f"    NFD {net_differential} <= threshold {self.min_funding_differential}"
                            )

                    # Opportunity Check 2: rate2 < rate1 => Long exchange1, Short exchange2
                    elif rate2 < rate1:
                        net_differential = rate1 - rate2  # Keep NFD positive
                        logger.debug(
                            f"  Potential NFD (rate1-rate2): {rate1} - {rate2} = {net_differential}"
                        )
                        if net_differential > self.min_funding_differential:
                            long_exchange_actual, short_exchange_actual = exchange1, exchange2
                            long_rate_actual, short_rate_actual = rate1, rate2
                            # Buy at ask1 (higher price), Sell at bid2 (lower price)
                            long_price_actual, short_price_actual = ask1, bid2

                            logger.debug(
                                f"    NFD {net_differential} > threshold {self.min_funding_differential}. "
                                f"Opportunity: Long {long_exchange_actual}/{symbol1}, Short {short_exchange_actual}/{symbol2}"
                            )
                            try:
                                # Simplified utility calculation
                                optimal_size_placeholder = Decimal("1.0")  # Placeholder size
                                expected_profit = net_differential * optimal_size_placeholder
                                utility_score = float(expected_profit) - self.risk_aversion * (
                                    float(basis_volatility) ** 2
                                )

                                logger.debug(
                                    f"      Expected Profit/Unit: {expected_profit}, Basis Vol: {basis_volatility}, Utility: {utility_score}"
                                )

                                opportunity = ArbitrageOpportunity(
                                    symbol=internal_symbol,  # Use internal symbol
                                    long_exchange=long_exchange_actual,
                                    short_exchange=short_exchange_actual,
                                    long_price=long_price_actual,
                                    short_price=short_price_actual,
                                    long_funding_rate=long_rate_actual,
                                    short_funding_rate=short_rate_actual,
                                    net_funding_differential=net_differential,
                                    timestamp=now,
                                    optimal_size=None,
                                    expected_profit=expected_profit,
                                    confidence=None,
                                    basis_volatility=float(basis_volatility),
                                    utility_score=utility_score,
                                )
                                opportunities.append(opportunity)
                                logger.info(
                                    f"  Appended Opportunity: Long {long_exchange_actual}/{symbol1} vs "
                                    f"Short {short_exchange_actual}/{symbol2} for '{internal_symbol}'"
                                )
                            except Exception as e:
                                logger.error(
                                    f"Error creating opportunity (Long {long_exchange_actual}/Short {short_exchange_actual}) "
                                    f"for symbol '{internal_symbol}' (mapped: {symbol1}/{symbol2}): {e}",
                                    exc_info=True,
                                )
                        else:
                            logger.debug(
                                f"    NFD {net_differential} <= threshold {self.min_funding_differential}"
                            )
                    else:  # Rates are equal
                        logger.debug(
                            f"  Funding rates are equal for {internal_symbol} ({symbol1}/{symbol2}). No NFD."
                        )

        # Sort opportunities by utility score (descending)
        opportunities.sort(key=lambda x: x.utility_score, reverse=True)

        logger.info(f"Generated {len(opportunities)} potential arbitrage opportunities.")
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

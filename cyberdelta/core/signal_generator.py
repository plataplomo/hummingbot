from __future__ import annotations  # Enable postponed evaluation

from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, getcontext  # Import Decimal and InvalidOperation

import numpy as np

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import (  # Import MarketData, OrderBook
    FundingRate,
    MarketData,
    Ticker,
)
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.utils.config import Config
from cyberdelta.utils.logging_config import get_logger  # <--- Use get_logger
from cyberdelta.validation.funding_data import ArbitrageOpportunity

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

        # Historical slippage data for estimation
        # exchange -> symbol -> list[Decimal]
        self.historical_slippage: dict[str, dict[str, list[Decimal]]] = {}

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
            f"Initializing data structures for enabled exchanges: {enabled_exchanges} "
            f"and internal symbols: {all_internal_symbols}"
        )

        # Track which symbols we're monitoring
        self.tracked_symbols = all_internal_symbols

        # Initialize historical funding rate storage
        for exchange_id in enabled_exchanges:
            self.historical_funding_rates[exchange_id] = {}
            self.historical_slippage[exchange_id] = {}  # Initialize historical slippage structure
            for internal_symbol in all_internal_symbols:
                # Check if this exchange has a mapping for the internal symbol
                exchange_symbol = self.symbol_mapper.get_exchange_symbol(
                    internal_symbol, exchange_id
                )
                if exchange_symbol:
                    self.historical_funding_rates[exchange_id][internal_symbol] = deque()
                    self.historical_slippage[exchange_id][
                        internal_symbol
                    ] = []  # Initialize empty list
                    logger.debug(
                        f"  Initialized funding deque for {exchange_id} / {internal_symbol} "
                        f"(maps to {exchange_symbol})"
                    )
                # else: # No need to log missing mappings, it's expected
                #    logger.debug(
                #        f"  No mapping found for {internal_symbol} on {exchange_id}, "
                #        f"skipping funding deque."
                #    )

        # Initialize basis history using all known internal symbols
        for internal_symbol in all_internal_symbols:
            self.historical_basis[internal_symbol] = deque()
            logger.debug(f"  Initialized basis deque for {internal_symbol}")

        # Log the final structure for verification
        # logger.debug(f"Final historical_funding_rates structure: {self.historical_funding_rates}")
        # logger.debug(f"Final historical_basis structure: {self.historical_basis}")

        logger.info(
            f"Initialized historical data structures for {len(enabled_exchanges)} enabled "
            f"exchanges and {len(all_internal_symbols)} internal symbols."
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
            # Iterate through internal symbols expected for this exchange based on init
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
                funding_data = self.data_handler.get_funding_rate(exchange_id, exchange_symbol)
                if not isinstance(funding_data, FundingRate):
                    continue
                rate = funding_data.funding_rate
                timestamp = now  # Use current time as timestamp
                if rate is None:
                    continue  # Skip this data point
                # Add to history using internal_symbol key
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
                        f"Historical funding rate deque not found for "
                        f"{exchange_id}/{internal_symbol} during update."
                    )

        # --- Update basis history (price difference between exchanges) ---
        for internal_symbol in all_internal_symbols:
            # Find exchanges that map this internal symbol
            valid_exchanges_for_symbol: list[str] = []
            exchange_tickers: dict[str, Ticker] = {}  # Store valid tickers

            # Check all enabled exchanges
            for exchange_id in enabled_exchanges:
                # Get exchange symbol using mapper
                exchange_symbol = self.symbol_mapper.get_exchange_symbol(
                    internal_symbol, exchange_id
                )

                if exchange_symbol:  # Check if mapping exists
                    market_data: MarketData | None = self.data_handler.get_ticker(
                        exchange_id, exchange_symbol
                    )
                    # Ensure market data and price are valid
                    if market_data is not None and hasattr(market_data, "close"):
                        # Attempt to convert price to Decimal immediately for validation
                        try:
                            price_decimal = (
                                market_data.close
                            )  # Already Decimal from MarketData.__post_init__
                            valid_exchanges_for_symbol.append(exchange_id)
                            # Create a Ticker object from MarketData for consistency
                            ticker = Ticker(
                                symbol=market_data.symbol,
                                price=price_decimal,
                                timestamp=int(market_data.timestamp.timestamp() * 1000)
                                if market_data.timestamp
                                else None,
                            )
                            # Store the ticker
                            exchange_tickers[exchange_id] = ticker
                        except (InvalidOperation, TypeError, AttributeError) as conversion_error:
                            logger.warning(
                                f"Could not process market data for "
                                f"{exchange_id}/{exchange_symbol} "
                                f"(Internal: {internal_symbol}): {conversion_error}"
                            )
                            # Do not add this exchange/ticker if price is invalid
                # No else needed, if no exchange_symbol, we skip this exchange

            # Compute basis if enough valid tickers were found
            if len(valid_exchanges_for_symbol) >= 2:
                ex1: str = valid_exchanges_for_symbol[0]
                ex2: str = valid_exchanges_for_symbol[1]
                ticker1: Ticker | None = exchange_tickers.get(ex1)
                ticker2: Ticker | None = exchange_tickers.get(ex2)

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
                # else: logger.debug(
                #    f"Skipping basis calc for {internal_symbol}: Not enough valid tickers."
                # )
            # else: logger.debug(
            #    f"Skipping basis calc for {internal_symbol}: Need >= 2 valid tickers."
            # )

    def calculate_funding_rate_volatility(self, exchange: str, internal_symbol: str) -> Decimal:
        """Calculate the volatility (std dev) of the historical funding rates."""
        if exchange not in self.historical_funding_rates:
            logger.debug(f"No funding data for {exchange}. Returning default volatility.")
            return Decimal("0.0001")  # Default funding volatility

        if internal_symbol not in self.historical_funding_rates[exchange]:
            # logger.debug(
            #    f"No funding data for {internal_symbol} on {exchange}. Returning default."
            # )
            return Decimal("0.0001")  # Default funding volatility

        history_deque = self.historical_funding_rates[exchange][internal_symbol]
        if len(history_deque) < 2:
            # Need at least 2 points to calculate volatility
            return Decimal("0.0001")  # Default volatility

        # Extract just the rates
        rates: list[Decimal] = [rate for _, rate in history_deque]

        try:
            # Calculate standard deviation using Decimal arithmetic
            # Convert any non-Decimal values to Decimal
            decimal_rates = [r for r in rates]  # All rates are Decimal by construction

            # Calculate mean
            n = len(decimal_rates)
            mean = sum(decimal_rates) / Decimal(n)

            # Calculate variance (sum of squared differences from mean, divided by n-1)
            variance = sum((r - mean) ** 2 for r in decimal_rates) / Decimal(n - 1)

            # Take square root for standard deviation
            std_dev_decimal = variance.sqrt()

            # Return a minimum non-zero volatility
            return max(Decimal("1e-8"), std_dev_decimal)  # Ensure non-zero return
        except (InvalidOperation, TypeError, ValueError) as e:
            logger.warning(
                f"Decimal calculation failed for {exchange}/{internal_symbol}: {e}. "
                f"Falling back to numpy (with potential precision loss)."
            )
            try:
                # Convert Decimal list to list of floats for numpy
                rates_float = [float(str(r)) for r in rates]  # Convert via string to minimize loss
                std_dev = np.std(rates_float)
                # Convert result back to Decimal
                std_dev_decimal = Decimal(str(std_dev))
                return max(Decimal("1e-8"), std_dev_decimal)
            except Exception as e2:
                logger.error(
                    f"Error calculating funding rate volatility for "
                    f"{exchange}/{internal_symbol}: {e2}"
                )
                return Decimal("0.0001")  # Default on calculation error

    def calculate_basis_volatility(self, symbol: str) -> Decimal:
        """Calculate the volatility (std dev) of the historical price basis."""
        if symbol not in self.historical_basis:
            logger.debug(f"No historical basis data for {symbol}. Returning default volatility.")
            return Decimal("0.01")  # Default basis volatility

        history_deque = self.historical_basis[symbol]
        if len(history_deque) < 2:
            # logger.debug(
            #    f"Insufficient historical basis data for {symbol} (need >= 2). "
            #    f"Returning default."
            # )
            return Decimal("0.01")  # Default volatility

        basis_values = [basis for _, basis in history_deque]

        try:
            # Calculate standard deviation using Decimal arithmetic
            # Convert any non-Decimal values to Decimal
            decimal_basis = [
                b for b in basis_values
            ]  # All basis values are Decimal by construction

            # Calculate mean
            n = len(decimal_basis)
            mean = sum(decimal_basis) / Decimal(n)

            # Calculate variance (sum of squared differences from mean, divided by n-1)
            variance = sum((b - mean) ** 2 for b in decimal_basis) / Decimal(n - 1)

            # Take square root for standard deviation
            std_dev_decimal = variance.sqrt()

            # Return a minimum non-zero volatility
            return max(Decimal("1e-8"), std_dev_decimal)  # Ensure non-zero return
        except (InvalidOperation, TypeError, ValueError) as e:
            # Fallback to numpy if Decimal calculation fails
            logger.warning(
                f"Decimal calculation failed for basis volatility {symbol}: {e}. "
                f"Falling back to numpy (with potential precision loss)."
            )
            try:
                basis_float = [
                    float(str(b)) for b in basis_values
                ]  # Convert via string to minimize loss
                std_dev = np.std(basis_float)
                std_dev_decimal = Decimal(str(std_dev))
                return max(Decimal("1e-8"), std_dev_decimal)
            except Exception as e2:
                logger.error(f"Error calculating basis volatility for {symbol}: {e2}")
                return Decimal("0.01")  # Default on calculation error

    def estimate_slippage(self, exchange: str, symbol: str, size: Decimal | None = None) -> Decimal:
        """
        Estimate the slippage cost for trading on a given exchange and symbol,
        potentially considering the trade size.

        Args:
            exchange: Exchange name
            symbol: Trading symbol
            size: Optional trade size (Decimal) to potentially adjust slippage estimate.

        Returns:
            Estimated slippage as a Decimal
        """
        # TODO: Implement logic to use 'size' and potentially order book depth
        # from data_handler to provide a more accurate slippage estimate.
        # Use historical data if available, otherwise use default
        if exchange in self.historical_slippage and symbol in self.historical_slippage[exchange]:
            slippage_data = self.historical_slippage[exchange][symbol]
            if slippage_data and len(slippage_data) > 0:
                # Calculate average slippage from historical data (result is Decimal)
                avg_slippage = sum(slippage_data) / Decimal(len(slippage_data))
                # Removed redundant check: if not isinstance(avg_slippage, Decimal):
                # avg_slippage = Decimal(str(avg_slippage))
                return avg_slippage

        # Fallback to configured value for the exchange or default
        base_slippage = self.config.get(
            f"exchanges.{exchange}.expected_slippage", self.default_slippage
        )

        # Ensure base_slippage is a Decimal
        if not isinstance(base_slippage, Decimal):
            base_slippage = Decimal(str(base_slippage))

        # Apply slippage sensitivity multiplier (self.slippage_sensitivity is guaranteed Decimal)
        sensitivity = self.slippage_sensitivity
        # Removed redundant check: if not isinstance(sensitivity, Decimal):
        # Removed unreachable code: sensitivity = Decimal(str(sensitivity))
        # Return the final calculated slippage
        # Mypy incorrectly reports [no-any-return] here sometimes.
        # Both base_slippage and sensitivity are guaranteed Decimal by this point.
        # type: ignore[no-any-return]  # Mypy false positive: runtime checks guarantee Decimal
        return base_slippage * sensitivity

    def generate_arbitrage_opportunities(
        self, funding_data: dict[str, dict[str, FundingRate | None]], market_data: MarketData
    ) -> list[ArbitrageOpportunity]:
        """
        Generate arbitrage opportunities based on funding rate differentials and market data.

        Args:
            funding_data: Dict mapping symbols to dicts mapping exchange names
                          to FundingRate objects
            market_data: MarketData object with current ticker information

        Returns:
            List of arbitrage opportunities
        """
        if market_data is None or market_data.ticker_data is None:
            logger.warning(
                "Market data or ticker data is None - cannot generate arbitrage opportunities"
            )
            return []

        opportunities: list[ArbitrageOpportunity] = []

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
        # Use a lambda that explicitly returns a float for sorting compatibility
        opportunities.sort(
            key=lambda x: float(x.net_funding_differential)
            if x.net_funding_differential is not None
            else 0.0,  # type: ignore
            reverse=True,
        )
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
        opportunities: list[ArbitrageOpportunity] = []

        # Check each pair of exchanges
        exchanges: list[str] = list(exchanges_with_data.keys())
        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                exchange_a = exchanges[i]
                exchange_b = exchanges[j]

                funding_a = exchanges_with_data[exchange_a]
                funding_b = exchanges_with_data[exchange_b]

                # Ensure we have valid funding rates.
                # Mypy incorrectly flags as unreachable, but the input type hint
                # funding_data: dict[str, dict[str, FundingRate | None]]
                # explicitly allows None values in the inner dict.
                # funding_a and funding_b are always FundingRate (not None) by type
                if funding_a is None or funding_b is None:  # mypy: [unreachable]
                    continue

                try:
                    rate_a = funding_a.funding_rate
                    rate_b = funding_b.funding_rate

                    # Ensure rates are not None and convert to Decimal if needed.
                    # Mypy incorrectly flags the following 'if' and subsequent lines
                    # as unreachable, likely due to its earlier incorrect assessment
                    # of the check at line 556. This check is necessary.
                    if rate_a is None or rate_b is None:  # mypy: [unreachable]
                        continue

                    # Mypy flags this block as unreachable due to the above.
                    # These conversions are necessary if rates might not be Decimal.
                    if not isinstance(rate_a, Decimal):  # mypy: [unreachable]
                        rate_a = Decimal(str(rate_a))
                    if not isinstance(rate_b, Decimal):  # mypy: [unreachable]
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

                    # Ensure we have valid prices.
                    # Mypy incorrectly flags as unreachable, but Ticker.price
                    # is defined as Decimal | None.
                    if ticker_a.price is None or ticker_b.price is None:  # mypy: [unreachable]
                        continue  # mypy: [unreachable]

                    # Convert prices to Decimal if needed
                    price_a = ticker_a.price
                    price_b = ticker_b.price

                    # Calculate the funding payment in USD terms
                    # Mypy incorrectly flags the following lines as unreachable,
                    # likely due to its earlier incorrect assessments.
                    funding_payment_a = price_a * rate_a  # mypy: [unreachable]
                    funding_payment_b = price_b * rate_b  # mypy: [unreachable]

                    # Calculate net funding differential
                    net_funding_differential = (
                        funding_payment_b - funding_payment_a
                    )  # mypy: [unreachable]

                    # Calculate expected profit after slippage
                    slippage_a = self.estimate_slippage(exchange_a, symbol)
                    slippage_b = self.estimate_slippage(exchange_b, symbol)

                    # Calculate total slippage
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
                            long_price=price_a,
                            short_price=price_b,
                            long_funding_rate=rate_a,
                            short_funding_rate=rate_b,
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
                            long_price=price_b,
                            short_price=price_a,
                            long_funding_rate=rate_b,
                            short_funding_rate=rate_a,
                            net_funding_differential=abs(net_funding_differential),
                            expected_profit=expected_profit,
                            timestamp=datetime.now(UTC),
                        )

                    opportunities.append(opportunity)

                except (InvalidOperation, TypeError, ValueError) as e:
                    logger.warning(
                        f"Error calculating funding arbitrage for {symbol} between "
                        f"{exchange_a} and {exchange_b}: {e}"
                    )
                    continue

        return opportunities

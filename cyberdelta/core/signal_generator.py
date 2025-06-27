"""Signal Generator for CyberDeltaEngine.

This module contains the SignalGenerator class, which is responsible for identifying
funding rate arbitrage opportunities between exchanges. It monitors funding rates,
calculates net funding differentials, computes expected profit metrics including costs,
and generates ranked arbitrage opportunities for the trading strategy.

The SignalGenerator serves as the core component for opportunity detection in the
delta-neutral arbitrage strategy implementation.
"""

from __future__ import annotations  # Enable postponed evaluation

from collections import deque
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, getcontext  # Import Decimal and InvalidOperation

import numpy as np

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.structlog_config import get_logger  # <--- Use get_logger
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import (  # Import MarketData, OrderBook
    FundingRate,
    Ticker,
)
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.validation.funding_data import ArbitrageOpportunity


logger = get_logger(__name__)  # <--- Use configured logger

# Set precision for Decimal
getcontext().prec = 28


class SignalGenerator:
    """Identifies funding rate arbitrage opportunities between exchanges.

    Responsible for:
    - Monitoring funding rates across exchanges
    - Calculating Net Funding Differential (NFD)
    - Computing expected profit metrics including costs
    - Generating and ranking arbitrage opportunities
    """

    def __init__(
        self,
        app_settings: AppSettings,
        data_handler: DataHandler,
        symbol_mapper: SymbolMapper,
    ) -> None:
        """Initialize the signal generator.

        Args:
            app_settings: Application configuration object containing strategy parameters.
            data_handler: DataHandler for market data.
            symbol_mapper: SymbolMapper for translating symbols.

        """
        logger.debug("SIGNAL_GENERATOR_TEST_LOG: Initializing SignalGenerator instance.")
        self.app_settings = app_settings
        self.data_handler = data_handler
        self.symbol_mapper = symbol_mapper

        # Configuration values from AppSettings
        # Use strategy configuration from the hl_perp_bp_spot strategy
        strategy_config = self.app_settings.strategies.hl_perp_bp_spot
        self.min_funding_differential = strategy_config.params.funding_threshold
        self.min_profit_threshold = strategy_config.params.min_profit_usd
        self.max_slippage_percent = strategy_config.params.max_price_spread_pct

        # TODO: Add more comprehensive strategy configuration to AppSettings when needed
        # For now, use reasonable defaults for parameters not in current config
        self.slippage_sensitivity = Decimal("0.5")  # Default 0.5
        self.liquidity_threshold_usd = Decimal("10000.0")  # Default $10,000
        self.default_slippage = Decimal("0.001")  # Default 0.1%
        self.funding_sample_period: int = 3600  # Default 1 hour
        self.funding_sample_count: int = 24  # Default 24 samples
        self.risk_aversion = 1.0  # Default risk aversion

        # Historical funding rates for volatility calculation
        # exchange -> internal_symbol -> deque[(timestamp, rate: Decimal)]
        self.historical_funding_rates: dict[str, dict[str, deque[tuple[datetime, Decimal]]]] = {}

        # Historical basis data for volatility calculation
        # internal_symbol -> deque[(timestamp, basis: Decimal)]
        self.historical_basis: dict[str, deque[tuple[datetime, Decimal]]] = {}

        # Historical slippage data for estimation
        # exchange -> symbol -> list[Decimal]
        self.historical_slippage: dict[str, dict[str, list[Decimal]]] = {}

        # Initialize data structures
        self._initialize_data_structures()
        logger.info("SignalGenerator initialized.")

    def _initialize_data_structures(self) -> None:
        """Initialize data structures for historical data using SymbolMapper."""
        all_internal_symbols = self.symbol_mapper.get_all_internal_symbols()
        # Use the exchanges from AppSettings structure
        configured_exchanges: list[str] = list(self.app_settings.exchanges.keys())

        enabled_exchanges = [
            ex_id for ex_id in configured_exchanges if self.app_settings.exchanges[ex_id].enabled
        ]

        logger.debug(
            "initializing_data_structures",
            action="init",
            enabled_exchanges=enabled_exchanges,
            internal_symbols=all_internal_symbols,
            message=(
                f"Initializing data structures for enabled exchanges: {enabled_exchanges} "
                f"and internal symbols: {all_internal_symbols}"
            ),
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
                    internal_symbol,
                    exchange_id,
                )
                if exchange_symbol:
                    self.historical_funding_rates[exchange_id][internal_symbol] = deque(
                        maxlen=self.funding_sample_count,  # Use maxlen
                    )
                    self.historical_slippage[exchange_id][
                        internal_symbol
                    ] = []  # Initialize empty list
                    logger.debug(
                        "funding_deque_initialized",
                        action="init",
                        exchange_id=exchange_id,
                        internal_symbol=internal_symbol,
                        exchange_symbol=exchange_symbol,
                        message=(
                            f"  Initialized funding deque for {exchange_id} / {internal_symbol} "
                            f"(maps to {exchange_symbol})"
                        ),
                    )
                #    logger.debug(
                #        f"skipping funding deque."

        # Initialize basis history using all known internal symbols
        for internal_symbol in all_internal_symbols:
            self.historical_basis[internal_symbol] = deque(
                maxlen=self.funding_sample_count,
            )  # Use maxlen
            logger.debug(
                "basis_deque_initialized",
                action="init",
                internal_symbol=internal_symbol,
                message=f"  Initialized basis deque for {internal_symbol}",
            )

        # Log the final structure for verification

        logger.info(
            "data_structures_initialized",
            action="init",
            num_exchanges=len(enabled_exchanges),
            num_symbols=len(all_internal_symbols),
            message=(
                f"Initialized historical data structures for {len(enabled_exchanges)} "
                f"enabled exchanges and {len(all_internal_symbols)} internal symbols."
            ),
        )

    def update_historical_data(self) -> None:
        """Update historical funding rate and basis data with latest information.

        Should be called regularly to maintain up-to-date volatility calculations.
        Uses SymbolMapper for translation.
        """
        now = datetime.now(UTC)
        all_internal_symbols = self.symbol_mapper.get_all_internal_symbols()

        # Determine enabled exchanges directly from config
        enabled_exchanges = self._get_enabled_exchanges()

        # Update funding rate history
        self._update_funding_rate_history(enabled_exchanges, now)

        # Update basis history (price difference between exchanges)
        self._update_basis_history(all_internal_symbols, enabled_exchanges, now)

    def _get_enabled_exchanges(self) -> list[str]:
        """Get list of enabled exchanges from configuration."""
        configured_exchanges: list[str] = list(self.app_settings.exchanges.keys())
        return [
            ex_id for ex_id in configured_exchanges if self.app_settings.exchanges[ex_id].enabled
        ]

    def _update_funding_rate_history(self, enabled_exchanges: list[str], now: datetime) -> None:
        """Update funding rate history for all exchanges and symbols."""
        for exchange_id in enabled_exchanges:
            # Iterate through internal symbols expected for this exchange based on init
            expected_internal_symbols = self.historical_funding_rates.get(exchange_id, {}).keys()

            for internal_symbol in expected_internal_symbols:
                self._update_single_funding_rate(exchange_id, internal_symbol, now)

    def _update_single_funding_rate(
        self,
        exchange_id: str,
        internal_symbol: str,
        now: datetime,
    ) -> None:
        """Update funding rate for a single exchange/symbol combination."""
        # Get the corresponding exchange symbol using the mapper
        exchange_symbol = self.symbol_mapper.get_exchange_symbol(
            internal_symbol,
            exchange_id,
        )

        if not exchange_symbol:
            # This indicates a possible inconsistency if the symbol was present during init
            logger.error(
                "symbol_mapping_inconsistency",
                action="update",
                internal_symbol=internal_symbol,
                exchange_id=exchange_id,
                message=(
                    f"Symbol mapping inconsistency: Cannot find exchange symbol for "
                    f"internal symbol '{internal_symbol}' on '{exchange_id}', though it was "
                    f"expected during initialization. Skipping update."
                ),
            )
            return

        # Fetch data using the exchange-specific symbol
        funding_data = self.data_handler.get_latest_funding_rate(
            exchange_id,
            exchange_symbol,
        )
        if not isinstance(funding_data, FundingRate):
            return

        rate = funding_data.funding_rate
        timestamp = now  # Use current time as timestamp
        if rate is None:
            return  # Skip this data point

        # Add to history using internal_symbol key
        if (
            exchange_id in self.historical_funding_rates
            and internal_symbol in self.historical_funding_rates[exchange_id]
        ):
            history_deque = self.historical_funding_rates[exchange_id][internal_symbol]
            history_deque.append((timestamp, rate))
        else:
            logger.error(
                "funding_deque_not_found",
                action="update",
                exchange_id=exchange_id,
                internal_symbol=internal_symbol,
                message=(
                    f"Historical funding rate deque not found for "
                    f"{exchange_id}/{internal_symbol} during update."
                ),
            )

    def _update_basis_history(
        self,
        all_internal_symbols: list[str],
        enabled_exchanges: list[str],
        now: datetime,
    ) -> None:
        """Update basis history for all symbols."""
        for internal_symbol in all_internal_symbols:
            self._update_single_basis(internal_symbol, enabled_exchanges, now)

    def _update_single_basis(
        self,
        internal_symbol: str,
        enabled_exchanges: list[str],
        now: datetime,
    ) -> None:
        """Update basis for a single symbol."""
        # Find exchanges that map this internal symbol and get valid tickers
        valid_exchanges_for_symbol, exchange_tickers = self._get_valid_tickers_for_symbol(
            internal_symbol,
            enabled_exchanges,
        )

        # Compute basis if enough valid tickers were found
        if len(valid_exchanges_for_symbol) >= 2:
            self._compute_and_store_basis(
                internal_symbol,
                valid_exchanges_for_symbol,
                exchange_tickers,
                now,
            )

    def _get_valid_tickers_for_symbol(
        self,
        internal_symbol: str,
        enabled_exchanges: list[str],
    ) -> tuple[list[str], dict[str, Ticker]]:
        """Get valid tickers for a symbol across exchanges."""
        valid_exchanges_for_symbol: list[str] = []
        exchange_tickers: dict[str, Ticker] = {}

        # Check all enabled exchanges
        for exchange_id in enabled_exchanges:
            # Get exchange symbol using mapper
            exchange_symbol = self.symbol_mapper.get_exchange_symbol(
                internal_symbol,
                exchange_id,
            )

            if exchange_symbol:  # Check if mapping exists
                ticker_data: Ticker | None = self.data_handler.get_latest_ticker(
                    exchange_id,
                    exchange_symbol,
                )

                # Ensure market data and price are valid
                if ticker_data is not None and ticker_data.price is not None:
                    # Attempt to convert price to Decimal immediately for validation
                    try:
                        # Ticker.price is already Decimal | None, checked above
                        valid_exchanges_for_symbol.append(exchange_id)
                        # Store the ticker
                        exchange_tickers[exchange_id] = ticker_data
                    except (InvalidOperation, TypeError, AttributeError) as conversion_error:
                        logger.warning(
                            "market_data_processing_failed",
                            action="update",
                            exchange_id=exchange_id,
                            exchange_symbol=exchange_symbol,
                            internal_symbol=internal_symbol,
                            error=str(conversion_error),
                            message=(
                                f"Could not process market data for "
                                f"{exchange_id}/{exchange_symbol} "
                                f"(Internal: {internal_symbol}): {conversion_error}"
                            ),
                        )
                        # Do not add this exchange/ticker if price is invalid

        return valid_exchanges_for_symbol, exchange_tickers

    def _compute_and_store_basis(
        self,
        internal_symbol: str,
        valid_exchanges_for_symbol: list[str],
        exchange_tickers: dict[str, Ticker],
        now: datetime,
    ) -> None:
        """Compute and store basis for a symbol."""
        ex1: str = valid_exchanges_for_symbol[0]
        ex2: str = valid_exchanges_for_symbol[1]
        ticker1: Ticker | None = exchange_tickers.get(ex1)
        ticker2: Ticker | None = exchange_tickers.get(ex2)

        # Check if tickers and their prices (already validated as Decimal) exist
        if ticker1 and ticker1.price is not None and ticker2 and ticker2.price is not None:
            # We know prices are Decimal here due to earlier checks
            basis = ticker1.price - ticker2.price
            timestamp = now  # Use current time for basis update

            # Add to basis history
            if internal_symbol in self.historical_basis:
                basis_history_deque = self.historical_basis[internal_symbol]
                basis_history_deque.append((timestamp, basis))
            else:
                logger.warning(
                    "basis_deque_not_found",
                    action="update",
                    internal_symbol=internal_symbol,
                    message=(
                        f"Historical basis deque not found for {internal_symbol} during update."
                    ),
                )

    def calculate_funding_rate_volatility(self, exchange: str, internal_symbol: str) -> Decimal:
        """Calculate the volatility (std dev) of the historical funding rates."""
        if exchange not in self.historical_funding_rates:
            logger.debug(
                "no_funding_data",
                action="calculate_volatility",
                exchange=exchange,
                default_volatility="0.0001",
                message=f"No funding data for {exchange}. Returning default volatility.",
            )
            return Decimal("0.0001")  # Default funding volatility

        if internal_symbol not in self.historical_funding_rates[exchange]:
            # logger.debug(
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
            decimal_rates = list(rates)  # All rates are Decimal by construction

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
                f"Falling back to numpy (with potential precision loss).",
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
                    f"{exchange}/{internal_symbol}: {e2}",
                )
                return Decimal("0.0001")  # Default on calculation error

    def calculate_basis_volatility(self, symbol: str) -> Decimal:
        """Calculate the volatility (std dev) of the historical price basis."""
        if symbol not in self.historical_basis:
            logger.debug(
                "no_historical_basis_data",
                action="calculate_volatility",
                symbol=symbol,
                default_volatility="0.01",
                message=f"No historical basis data for {symbol}. Returning default volatility.",
            )
            return Decimal("0.01")  # Default basis volatility

        history_deque = self.historical_basis[symbol]
        if len(history_deque) < 2:
            # logger.debug(
            #    f"Returning default."
            return Decimal("0.01")  # Default volatility

        basis_values = [basis for _, basis in history_deque]

        try:
            # Calculate standard deviation using Decimal arithmetic
            # Convert any non-Decimal values to Decimal
            decimal_basis = list(basis_values)  # All basis values are Decimal by construction

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
                f"Falling back to numpy (with potential precision loss).",
            )
            try:
                basis_float = [
                    float(str(b)) for b in basis_values
                ]  # Convert via string to minimize loss
                std_dev = np.std(basis_float)
                std_dev_decimal = Decimal(str(std_dev))
                return max(Decimal("1e-8"), std_dev_decimal)
            except Exception as e2:
                logger.error(
                    "basis_volatility_calculation_error",
                    action="calculate_volatility",
                    symbol=symbol,
                    error=str(e2),
                    message=f"Error calculating basis volatility for {symbol}: {e2}",
                )
                return Decimal("0.01")  # Default on calculation error

    def estimate_slippage(self, exchange: str, symbol: str, size: Decimal | None = None) -> Decimal:
        """Estimate the slippage cost for trading on a given exchange and symbol.

        Potentially considers the trade size to provide more accurate slippage estimates.

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
                return sum(slippage_data) / Decimal(len(slippage_data))
                # Removed redundant check: if not isinstance(avg_slippage, Decimal):

        # TODO: Add exchange-specific slippage configuration to AppSettings when needed
        base_slippage = self.default_slippage  # Use default slippage for all exchanges

        # Apply slippage sensitivity multiplier (self.slippage_sensitivity is guaranteed Decimal)
        sensitivity = self.slippage_sensitivity
        # Removed redundant check: if not isinstance(sensitivity, Decimal):
        # Removed unreachable code: sensitivity = Decimal(str(sensitivity))
        # Return the final calculated slippage
        # Mypy incorrectly reports [no-any-return] here sometimes.
        # Both base_slippage and sensitivity are guaranteed Decimal by this point.
        return base_slippage * sensitivity

    async def generate_arbitrage_opportunities(
        self,
        funding_data: dict[str, dict[str, FundingRate | None]],
    ) -> list[ArbitrageOpportunity]:
        """Check for arbitrage opportunities based on the latest funding rates and market data.

        Args:
            funding_data: A dictionary where keys are internal symbols, and values are
                          dictionaries mapping exchange_ids to FundingRate objects or None.
                          Example: {"BTC": {"hyperliquid": FundingRate(...),
                                    "backpack": FundingRate(...)}, ...}

        Returns:
            A list of ArbitrageOpportunity objects, sorted by expected profit.

        """
        opportunities: list[ArbitrageOpportunity] = []
        all_internal_symbols = list(
            funding_data.keys(),
        )  # Process only symbols present in funding_data

        if not all_internal_symbols:
            logger.debug(
                "SG_GEN_OPPS: No internal symbols found in provided funding_data. "
                "Cannot generate opportunities.",
            )
            return []

        logger.debug(
            f"SG_GEN_OPPS: Checking for opportunities across internal symbols: "
            f"{all_internal_symbols}",
        )

        # --- Main Loop to Generate Opportunities ---
        for internal_symbol in all_internal_symbols:
            # For each internal symbol, gather necessary data across all relevant exchanges
            relevant_exchanges = list(funding_data.get(internal_symbol, {}).keys())
            if not relevant_exchanges or len(relevant_exchanges) < 2:
                logger.debug(
                    f"SG_GEN_OPPS: Not enough exchange data for {internal_symbol} "
                    f"to find arbitrage. Need >= 2, Got: {len(relevant_exchanges)}",
                )
                continue  # Need at least two exchanges for an arbitrage

            # --- Gather Tickers ---
            # Correctly fetch tickers using exchange-specific symbols
            current_tickers: dict[str, Ticker | None] = {}
            for exchange_id in relevant_exchanges:
                exchange_specific_symbol = self.symbol_mapper.get_exchange_symbol(
                    internal_symbol,
                    exchange_id,
                )
                if not exchange_specific_symbol:
                    logger.warning(
                        f"SG_GEN_OPPS: Could not map internal symbol {internal_symbol} "
                        f"to an exchange-specific symbol for {exchange_id}. "
                        f"Skipping ticker fetch for this pair.",
                    )
                    current_tickers[exchange_id] = None  # Store None if mapping fails
                    continue

                ticker = self.data_handler.get_latest_ticker(exchange_id, exchange_specific_symbol)
                if ticker is None:
                    logger.debug(
                        f"SG_TICKER_FETCH_FAIL: No ticker available from DataHandler "
                        f"for {exchange_id} / {exchange_specific_symbol} "
                        f"(internal: {internal_symbol}).",
                    )
                current_tickers[exchange_id] = ticker

            # Log if any exchange has no ticker for this internal_symbol AFTER attempting
            # all relevant exchanges
            # This check is better placed after the loop to give a complete picture
            # for the internal_symbol
            if not any(
                ticker is not None for ticker in current_tickers.values()
            ):  # Check if all are None for relevant exchanges
                logger.debug(
                    f"SG_GEN_OPPS: No valid tickers found via DataHandler for internal "
                    f"symbol {internal_symbol} across relevant exchanges {relevant_exchanges}, "
                    f"skipping opportunity check for this symbol.",
                )
                continue

            # Filter funding_data for the current internal_symbol for clarity
            current_funding_on_exchanges_nullable = funding_data.get(internal_symbol)
            # This variable is now current_funding_on_exchanges_nullable to reflect
            # it might have Nones

            if not current_funding_on_exchanges_nullable:
                logger.debug(
                    f"SG_GEN_OPPS: No funding data entries at all for {internal_symbol}, skipping.",
                )
                continue

            # Filter out None values to satisfy _check_funding_rate_opportunities type hint
            current_funding_on_exchanges: dict[str, FundingRate] = {
                ex_id: fr
                for ex_id, fr in current_funding_on_exchanges_nullable.items()
                if isinstance(fr, FundingRate)
            }

            # Ensure we still have enough data points after filtering Nones
            if len(current_funding_on_exchanges) < 2:
                logger.debug(
                    f"SG_GEN_OPPS: Not enough valid (non-None) funding rate data for "
                    f"{internal_symbol} to find arbitrage. Need >= 2, "
                    f"Got: {len(current_funding_on_exchanges)} after filtering Nones.",
                )
                continue

            # Now call _check_funding_rate_opportunities with the correctly gathered tickers
            # and the correctly typed funding data
            symbol_opportunities = self._check_funding_rate_opportunities(
                internal_symbol,
                current_funding_on_exchanges,
                current_tickers,
            )
            opportunities.extend(symbol_opportunities)

        # Sort opportunities by expected profit (descending)
        opportunities.sort(
            key=lambda x: float(
                x.expected_profit if x.expected_profit is not None else Decimal("0.0"),
            ),  # Handle None for float conversion
            reverse=True,
        )

        # --- Logging and Return ---
        logger.info(
            "arbitrage_opportunities_generated",
            action="generate",
            opportunity_count=len(opportunities),
            message=f"Generated {len(opportunities)} arbitrage opportunities.",
        )
        return opportunities

    def _check_funding_rate_opportunities(
        self,
        symbol: str,
        exchanges_with_data: dict[str, FundingRate],
        tickers: dict[str, Ticker | None],
    ) -> list[ArbitrageOpportunity]:
        """Check for funding rate arbitrage opportunities between exchanges for a given symbol.

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
        logger.debug(
            "checking_exchange_pairs",
            action="check_opportunities",
            symbol=symbol,
            exchanges=exchanges,
            message=f"NFD_CHECK_OPPS: Symbol={symbol}, Checking pairs from exchanges: {exchanges}",
        )

        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                exchange_a = exchanges[i]
                exchange_b = exchanges[j]

                opportunity = self._check_exchange_pair_opportunity(
                    symbol,
                    exchange_a,
                    exchange_b,
                    exchanges_with_data,
                    tickers,
                )
                if opportunity:
                    opportunities.append(opportunity)

        return opportunities

    def _check_exchange_pair_opportunity(
        self,
        symbol: str,
        exchange_a: str,
        exchange_b: str,
        exchanges_with_data: dict[str, FundingRate],
        tickers: dict[str, Ticker | None],
    ) -> ArbitrageOpportunity | None:
        """Check for arbitrage opportunity between a specific pair of exchanges."""
        funding_a = exchanges_with_data[exchange_a]
        funding_b = exchanges_with_data[exchange_b]

        # Validate funding rates
        rate_a = funding_a.funding_rate
        rate_b = funding_b.funding_rate

        logger.debug(
            f"NFD_CHECK_PAIR: {symbol} - {exchange_a} (Rate: {rate_a}) "
            f"vs {exchange_b} (Rate: {rate_b})",
        )

        if rate_a is None or rate_b is None:
            logger.debug(
                f"NFD_SKIP_PAIR: {symbol} - Missing rate for {exchange_a} "
                f"or {exchange_b}. Skipping.",
            )
            return None

        # Calculate the funding rate differential
        funding_differential = rate_b - rate_a

        # Check if the differential exceeds our threshold
        if abs(funding_differential) < self.min_funding_differential:
            logger.debug(
                f"NFD_SKIP_THRESHOLD: {symbol} - Pair {exchange_a}/{exchange_b}. "
                f"NFD {funding_differential:.8f} abs({abs(funding_differential):.8f}) "
                f"< Min Diff {self.min_funding_differential:.8f}. Skipping.",
            )
            return None

        # Validate ticker data
        ticker_validation = self._validate_ticker_data(symbol, exchange_a, exchange_b, tickers)
        if ticker_validation is None:
            return None

        ticker_a, ticker_b = ticker_validation

        # Calculate expected profit
        expected_profit = self._calculate_expected_profit(
            symbol,
            exchange_a,
            exchange_b,
            rate_a,
            rate_b,
            ticker_a,
            ticker_b,
        )
        if expected_profit is None:
            return None

        # Create opportunity based on funding differential direction
        return self._create_opportunity_from_differential(
            symbol,
            exchange_a,
            exchange_b,
            funding_differential,
            rate_a,
            rate_b,
            ticker_a,
            ticker_b,
            expected_profit,
        )

    def _validate_ticker_data(
        self,
        symbol: str,
        exchange_a: str,
        exchange_b: str,
        tickers: dict[str, Ticker | None],
    ) -> tuple[Ticker, Ticker] | None:
        """Validate ticker data for both exchanges."""
        ticker_a = tickers.get(exchange_a)
        ticker_b = tickers.get(exchange_b)

        if ticker_a is None or ticker_b is None:
            logger.debug(
                f"NFD_SKIP_TICKER: {symbol} - Missing ticker for {exchange_a} "
                f"or {exchange_b}. Skipping.",
            )
            return None

        # Validate bid/ask prices
        if (
            ticker_a.ask is None
            or ticker_a.bid is None
            or ticker_b.ask is None
            or ticker_b.bid is None
        ):
            logger.debug(
                f"NFD_SKIP_BID_ASK: {symbol} - Missing bid/ask for {symbol} on "
                f"{exchange_a} (A:{ticker_a.ask}, B:{ticker_a.bid}) or "
                f"{exchange_b} (A:{ticker_b.ask}, B:{ticker_b.bid}). Skipping.",
            )
            return None

        # Validate mid prices
        if ticker_a.price is None or ticker_b.price is None:
            logger.debug(
                f"NFD_SKIP_MID_PRICE: {symbol} - Missing .price (mid) for "
                f"{exchange_a} or {exchange_b}. Skipping.",
            )
            return None

        return ticker_a, ticker_b

    def _calculate_expected_profit(
        self,
        symbol: str,
        exchange_a: str,
        exchange_b: str,
        rate_a: Decimal,
        rate_b: Decimal,
        ticker_a: Ticker,
        ticker_b: Ticker,
    ) -> Decimal | None:
        """Calculate expected profit after slippage."""
        # Calculate price-weighted funding payments for profit estimation
        # We know prices are not None due to validation in _validate_ticker_data
        price_a = ticker_a.price
        price_b = ticker_b.price
        if price_a is None or price_b is None:
            # This should not happen due to prior validation, but defensive check
            return None

        funding_payment_a = price_a * rate_a
        funding_payment_b = price_b * rate_b
        # This is the net difference in *value* based on current ticker.price
        net_funding_value_differential: Decimal = funding_payment_b - funding_payment_a

        # Calculate expected profit after slippage
        slippage_a = self.estimate_slippage(exchange_a, symbol)
        slippage_b = self.estimate_slippage(exchange_b, symbol)
        total_slippage = slippage_a + slippage_b
        # Expected profit uses the *value* differential
        expected_profit = abs(net_funding_value_differential) - total_slippage

        if expected_profit < self.min_profit_threshold:
            logger.debug(
                f"NFD_SKIP_PROFIT: {symbol} - Pair {exchange_a}/{exchange_b}. "
                f"ExpProfit {expected_profit:.4f} < MinProfit "
                f"{self.min_profit_threshold:.4f}. "
                f"ValueDiff: {net_funding_value_differential:.4f}, "
                f"Slippage: {total_slippage:.4f}. Skipping.",
            )
            return None

        return expected_profit

    def _create_opportunity_from_differential(
        self,
        symbol: str,
        exchange_a: str,
        exchange_b: str,
        funding_differential: Decimal,
        rate_a: Decimal,
        rate_b: Decimal,
        ticker_a: Ticker,
        ticker_b: Ticker,
        expected_profit: Decimal,
    ) -> ArbitrageOpportunity:
        """Create arbitrage opportunity based on funding differential direction."""
        if funding_differential > Decimal(0):  # rate_b > rate_a: Long B, Short A
            return self._create_long_b_short_a_opportunity(
                symbol,
                exchange_a,
                exchange_b,
                rate_a,
                rate_b,
                ticker_a,
                ticker_b,
                expected_profit,
            )
        # rate_a > rate_b: Long A, Short B
        return self._create_long_a_short_b_opportunity(
            symbol,
            exchange_a,
            exchange_b,
            rate_a,
            rate_b,
            ticker_a,
            ticker_b,
            expected_profit,
        )

    def _create_long_b_short_a_opportunity(
        self,
        symbol: str,
        exchange_a: str,
        exchange_b: str,
        rate_a: Decimal,
        rate_b: Decimal,
        ticker_a: Ticker,
        ticker_b: Ticker,
        expected_profit: Decimal,
    ) -> ArbitrageOpportunity:
        """Create opportunity where we long B and short A."""
        actual_long_rate = rate_b
        actual_short_rate = rate_a

        # We know these are not None due to validation in _validate_ticker_data
        current_long_price = ticker_b.ask
        current_short_price = ticker_a.bid
        if current_long_price is None or current_short_price is None:
            # This should not happen due to prior validation, but defensive check
            raise ValueError(f"Missing bid/ask prices for {symbol} opportunity creation")

        opportunity_nfd_calculated = actual_long_rate - actual_short_rate

        logger.debug(
            f"NFD_OPP_DEBUG: Symbol={symbol}, Case 1 (Long B, Short A) "
            f"before ArbitrageOpportunity creation. "
            f"LongEx: {exchange_b}, ShortEx: {exchange_a}, "
            f"LongPx: {current_long_price}, ShortPx: {current_short_price}, "
            f"LongRate: {actual_long_rate}, ShortRate: {actual_short_rate}, "
            f"NFD: {opportunity_nfd_calculated}",
        )

        opportunity = ArbitrageOpportunity(
            symbol=symbol,
            long_exchange=exchange_b,
            short_exchange=exchange_a,
            long_price=current_long_price,
            short_price=current_short_price,
            long_funding_rate=actual_long_rate,
            short_funding_rate=actual_short_rate,
            net_funding_differential=opportunity_nfd_calculated,
            expected_profit=expected_profit,
            timestamp=datetime.now(UTC),
        )

        logger.info(
            f"NFD_OPP_CREATED: {symbol} - Long {exchange_b} @ {actual_long_rate:.8f}, "
            f"Short {exchange_a} @ {actual_short_rate:.8f}, "
            f"NFD: {opportunity_nfd_calculated:.8f}",
        )

        return opportunity

    def _create_long_a_short_b_opportunity(
        self,
        symbol: str,
        exchange_a: str,
        exchange_b: str,
        rate_a: Decimal,
        rate_b: Decimal,
        ticker_a: Ticker,
        ticker_b: Ticker,
        expected_profit: Decimal,
    ) -> ArbitrageOpportunity:
        """Create opportunity where we long A and short B."""
        actual_long_rate = rate_a
        actual_short_rate = rate_b

        # We know these are not None due to validation in _validate_ticker_data
        current_long_price = ticker_a.ask
        current_short_price = ticker_b.bid
        if current_long_price is None or current_short_price is None:
            # This should not happen due to prior validation, but defensive check
            raise ValueError(f"Missing bid/ask prices for {symbol} opportunity creation")

        opportunity_nfd_calculated = actual_long_rate - actual_short_rate

        logger.debug(
            f"NFD_OPP_DEBUG: Symbol={symbol}, Case 2 (Long A, Short B) "
            f"before ArbitrageOpportunity creation. "
            f"LongEx: {exchange_a}, ShortEx: {exchange_b}, "
            f"LongPx: {current_long_price}, ShortPx: {current_short_price}, "
            f"LongRate: {actual_long_rate}, ShortRate: {actual_short_rate}, "
            f"NFD: {opportunity_nfd_calculated}",
        )

        opportunity = ArbitrageOpportunity(
            symbol=symbol,
            long_exchange=exchange_a,
            short_exchange=exchange_b,
            long_price=current_long_price,
            short_price=current_short_price,
            long_funding_rate=actual_long_rate,
            short_funding_rate=actual_short_rate,
            net_funding_differential=opportunity_nfd_calculated,
            expected_profit=expected_profit,
            timestamp=datetime.now(UTC),
        )

        logger.info(
            f"NFD_OPP_CREATED: {symbol} - Long {exchange_a} @ {actual_long_rate:.8f}, "
            f"Short {exchange_b} @ {actual_short_rate:.8f}, "
            f"NFD: {opportunity_nfd_calculated:.8f}",
        )

        return opportunity

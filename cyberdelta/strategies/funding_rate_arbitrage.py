"""Funding Rate Arbitrage Strategy for CyberDeltaEngine.

This module implements the funding rate arbitrage strategy that identifies
and executes arbitrage opportunities between different exchanges based on
funding rate differentials.

Primary approach for v0.0.1: Hyperliquid-Perp vs Backpack-Spot strategy
This strategy:
- Takes a position on Hyperliquid perpetual contracts
- Hedges with opposite position in Backpack spot markets
- Profits from funding rate payments while maintaining delta neutrality
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal, getcontext
from typing import Any, cast

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import (
    # MarketData, # Removed
    FundingRate,
    OrderSide,
    SignalType,
    Ticker,
    TradeSignal,
)
from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.market import Candle  # Import Candle
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.core.strategy import Strategy
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Set precision for Decimal
getcontext().prec = 28

# Instantiate module-level logger
logger = get_logger(__name__)


class FundingRateArbitrageStrategy(Strategy):
    """Implementation of a funding rate arbitrage strategy between exchanges.

    Primary approach for v0.0.1: Hyperliquid-Perp vs Backpack-Spot strategy
    This strategy:
    - Takes a position on Hyperliquid perpetual contracts
    - Hedges with opposite position in Backpack spot markets
    - Profits from funding rate payments while maintaining delta neutrality
    """

    def __init__(
        self,
        name: str,
        symbol: str,
        data_handler: DataHandler,
        portfolio_tracker: PortfolioTracker,
        risk_manager: RiskManager | None = None,
        params: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the funding rate arbitrage strategy.

        Args:
            name: Unique name for the strategy
            symbol: Trading symbol this strategy operates on (e.g., "BTC-PERP")
            data_handler: Data handler for market data access
            portfolio_tracker: Portfolio tracker for position management
            risk_manager: Risk manager for position sizing and risk controls
            params: Dictionary of strategy parameters

        """
        super().__init__(name, symbol, params)
        self.data_handler = data_handler
        self.portfolio_tracker = portfolio_tracker
        self.risk_manager = risk_manager

        # Use helpers for config-derived parameters
        self.min_funding_differential: Decimal = self._get_decimal_param(
            "min_funding_differential",
            Decimal("0.0001"),
        )
        self.min_profit_threshold: Decimal = self._get_decimal_param(
            "min_profit_usd",
            Decimal("0.1"),
        )
        self.risk_aversion: Decimal = self._get_decimal_param("risk_aversion", Decimal("1.0"))
        self.rebalance_threshold: Decimal = self._get_decimal_param(
            "rebalance_threshold",
            Decimal("0.05"),
        )
        self.check_interval: int = self._get_int_param("check_interval", 10)

        # Strategy state
        self.last_opportunity_check: datetime | None = None
        self.active_opportunities: list[ArbitrageOpportunity] = []
        self.historical_basis: dict[str, list[tuple[datetime, Decimal]]] = {}
        self._consecutive_failures = 0  # Track consecutive funding rate failures
        # Defensive: ensure check_interval is always int

        # Exchange mapping
        self.perp_exchange: str = str(self.get_param("perp_exchange", "hyperliquid"))
        self.spot_exchange: str = str(self.get_param("spot_exchange", "backpack"))

        # Market mapping (perp to spot)
        symbol_mapping_param = self.get_param("symbol_mapping", None)
        if symbol_mapping_param is not None and isinstance(symbol_mapping_param, dict):
            self.symbol_mapping: dict[str, str] = symbol_mapping_param
        else:
            # Default mapping if not provided
            base = symbol.split("-")[0] if "-" in symbol else symbol.split("_")[0]
            self.symbol_mapping = {symbol: f"{base}_USDC"}

        # Position sizing info storage
        self.sized_opportunities: dict[str, SizedOpportunity] = {}

        logger.info(
            "funding_arbitrage_strategy_initialized",
            strategy_name=self.name,
            symbol=self.symbol,
            perp_exchange=self.perp_exchange,
            spot_exchange=self.spot_exchange,
            min_funding_differential=float(self.min_funding_differential),
            min_profit_threshold=float(self.min_profit_threshold),
            risk_aversion=float(self.risk_aversion),
            rebalance_threshold=float(self.rebalance_threshold),
            check_interval=self.check_interval,
            symbol_mapping=self.symbol_mapping,
            message=(
                f"Initialized {self.name} strategy for {self.symbol} between "
                f"{self.perp_exchange} and {self.spot_exchange}"
            ),
        )

    def _get_decimal_param(self, key: str, default: Decimal) -> Decimal:
        value = self.get_param(key, default)
        try:
            return Decimal(str(value))
        except Exception:
            return default

    def _get_int_param(self, key: str, default: int) -> int:
        value = self.get_param(key, default)
        if value is None:
            return default
        if isinstance(value, int | float | str):
            try:
                return int(value)
            except Exception:
                return default
        return default

    async def _get_funding_rate_with_retry(
        self,
        exchange_id: str,
        symbol: str,
        max_retries: int = 3,
    ) -> FundingRate | None:
        """Get funding rate with exponential backoff retry."""
        for attempt in range(max_retries):
            try:
                rate = self.data_handler.get_latest_funding_rate(exchange_id, symbol)
                if rate is not None:
                    return rate

                # If no data and not last attempt, wait before retry
                if attempt < max_retries - 1:
                    wait_time = (2**attempt) * 0.5  # 0.5s, 1s, 2s
                    logger.debug(
                        f"Retrying funding rate fetch for {exchange_id}:{symbol} "
                        f"(attempt {attempt + 1}/{max_retries}) after {wait_time}s",
                    )
                    await asyncio.sleep(wait_time)

            except Exception as e:
                logger.error(
                    f"Error fetching funding rate for {exchange_id}:{symbol}: {e}",
                    exc_info=True,
                )

        return None

    async def _ensure_fresh_hyperliquid_funding(self) -> None:
        """Ensure Hyperliquid funding rates are fresh by fetching if needed."""
        if self.perp_exchange != "hyperliquid":
            return

        current_rate = self.data_handler.get_latest_funding_rate(
            self.perp_exchange,
            self.symbol,
        )

        if self._should_fetch_hyperliquid_funding(current_rate):
            await self.data_handler.fetch_funding_rates(self.perp_exchange, [self.symbol])

    def _should_fetch_hyperliquid_funding(self, current_rate: FundingRate | None) -> bool:
        """Check if Hyperliquid funding rates need to be fetched."""
        if not current_rate:
            return True

        # Hyperliquid funding rates update hourly at the top of the hour
        now = datetime.now(UTC)
        age = now - current_rate.timestamp

        # Fetch if data is older than 55 minutes
        if age > timedelta(minutes=55):
            return True

        # Also fetch if we're within 5 minutes after the hour
        # and data is from before the hour
        minutes_past_hour = now.minute
        if minutes_past_hour <= 5:
            # Check if the cached data is from before this hour
            current_hour_start = now.replace(minute=0, second=0, microsecond=0)
            if current_rate.timestamp < current_hour_start:
                logger.debug(
                    f"Fetching funding rate as we're {minutes_past_hour} minutes "
                    f"past the hour and cached data is from before "
                    f"{current_hour_start}",
                )
                return True

        return False

    def _handle_funding_rate_failure(self) -> None:
        """Handle funding rate retrieval failure and log context."""
        self._consecutive_failures += 1

        # Log with context
        perp_ticker = self.data_handler.get_latest_ticker(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol, "")
        spot_ticker = (
            self.data_handler.get_latest_ticker(self.spot_exchange, spot_symbol)
            if spot_symbol
            else None
        )

        logger.error(
            "funding_rate_unavailable_with_context",
            strategy=self.name,
            symbol=self.symbol,
            perp_exchange=self.perp_exchange,
            consecutive_failures=self._consecutive_failures,
            perp_price=float(perp_ticker.price) if perp_ticker and perp_ticker.price else None,
            spot_price=float(spot_ticker.price) if spot_ticker and spot_ticker.price else None,
            has_perp_connection=self.perp_exchange in self.data_handler.api_clients,
            action="skipping_opportunity_check",
        )

        # Trigger alert if too many consecutive failures
        if self._consecutive_failures >= 10:
            logger.critical(
                "funding_rate_critical_failure",
                strategy=self.name,
                consecutive_failures=self._consecutive_failures,
                message="Funding rate data unavailable for extended period",
            )

    async def _process_arbitrage_opportunity(
        self,
        funding_rate: FundingRate,
        max_history: int,
    ) -> ArbitrageOpportunity | None:
        """Process the arbitrage opportunity with valid funding rate."""
        # Get current time
        now = datetime.now(UTC)

        # Get prices and validate
        price_data = self._get_and_validate_prices()
        if price_data is None:
            return None

        perp_price, spot_price, perp_ticker, spot_ticker = price_data

        # Update historical basis data
        basis = perp_price - spot_price
        self._update_historical_basis(now, basis, max_history)

        # Calculate basis volatility
        basis_volatility = self._calculate_basis_volatility(self.symbol)

        # Validate funding rate and check threshold
        nfd = funding_rate.funding_rate
        if not self._is_valid_funding_rate(nfd):
            return None

        # DEFENSIVE CHECK: Type narrowing for mypy. Mypy=[unreachable] Ruff=[]
        if nfd is None:
            return None

        # Calculate costs and profit
        profit_data = self._calculate_profit_and_costs(nfd)
        if profit_data is None:
            return None

        expected_profit, utility_score = profit_data

        # Determine trading sides and entry prices
        perp_side = "SHORT" if nfd > 0 else "LONG"
        entry_prices = self._get_entry_prices(perp_ticker, spot_ticker, perp_side)
        if entry_prices is None:
            return None

        long_price, short_price = entry_prices

        # Create opportunity object
        return self._create_opportunity_object(
            now,
            nfd,
            expected_profit,
            utility_score,
            basis_volatility,
            perp_side,
            long_price,
            short_price,
        )

    async def _check_opportunity(self) -> ArbitrageOpportunity | None:
        """Check for funding rate arbitrage opportunity between perp and spot markets.

        Returns:
            ArbitrageOpportunity if found, None otherwise

        """
        # Get max_history at the start so it's always defined
        max_history = self._get_int_param("history_length", 24)

        # For Hyperliquid, check if we need to fetch fresh funding rates
        await self._ensure_fresh_hyperliquid_funding()

        # Get funding rate with retries
        funding_rate = await self._get_funding_rate_with_retry(
            self.perp_exchange,
            self.symbol,
            max_retries=3,
        )

        if funding_rate is None:
            self._handle_funding_rate_failure()
            return None

        # Reset failure counter on success
        self._consecutive_failures = 0

        # Process the opportunity
        return await self._process_arbitrage_opportunity(funding_rate, max_history)

    def _get_and_validate_prices(self) -> tuple[Decimal, Decimal, Ticker, Ticker] | None:
        """Get and validate prices for perp and spot markets."""
        # Get prices for basis calculation
        perp_ticker = self.data_handler.get_latest_ticker(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol)
        if spot_symbol is None:
            logger.warning(
                "spot_symbol_mapping_missing",
                strategy=self.name,
                symbol=self.symbol,
                available_mappings=list(self.symbol_mapping.keys()),
                action="skipping_opportunity_check",
                message=f"No spot symbol mapping for {self.symbol}",
            )
            return None
        spot_ticker = self.data_handler.get_latest_ticker(self.spot_exchange, spot_symbol)

        if perp_ticker is None or spot_ticker is None:
            logger.warning(
                "ticker_data_unavailable",
                strategy=self.name,
                symbol=self.symbol,
                spot_symbol=spot_symbol,
                perp_exchange=self.perp_exchange,
                spot_exchange=self.spot_exchange,
                perp_ticker_exists=perp_ticker is not None,
                spot_ticker_exists=spot_ticker is not None,
                action="skipping_opportunity_check",
                message=f"Could not get prices for {self.symbol} or {spot_symbol}",
            )
            return None

        # Ensure .close is accessed only if tickers are not None (already checked)
        current_perp_price = perp_ticker.price
        current_spot_price = spot_ticker.price

        if current_perp_price is None or current_spot_price is None:
            logger.warning(
                "ticker_price_data_none",
                strategy=self.name,
                symbol=self.symbol,
                spot_symbol=spot_symbol,
                perp_price=float(current_perp_price) if current_perp_price else None,
                spot_price=float(current_spot_price) if current_spot_price else None,
                action="skipping_opportunity_check",
                message=(
                    f"Ticker price is None for {self.symbol} or {spot_symbol}. "
                    f"Perp: {current_perp_price}, Spot: {current_spot_price}"
                ),
            )
            return None

        return current_perp_price, current_spot_price, perp_ticker, spot_ticker

    def _update_historical_basis(self, now: datetime, basis: Decimal, max_history: int) -> None:
        """Update historical basis data."""
        if self.symbol not in self.historical_basis:
            self.historical_basis[self.symbol] = []
        self.historical_basis[self.symbol].append((now, basis))

        # Keep only recent data points
        if len(self.historical_basis[self.symbol]) > max_history:
            self.historical_basis[self.symbol] = self.historical_basis[self.symbol][-max_history:]

    def _is_valid_funding_rate(self, nfd: Decimal | None) -> bool:
        """Validate funding rate and check if it meets threshold."""
        if nfd is None:
            logger.warning(
                "funding_rate_value_none",
                strategy=self.name,
                symbol=self.symbol,
                perp_exchange=self.perp_exchange,
                action="skipping_opportunity_check",
                message=f"Funding rate value is None for {self.symbol} on {self.perp_exchange}",
            )
            return False

        # Skip if NFD is below threshold
        if abs(nfd) < self.min_funding_differential:
            logger.debug(
                "funding_differential_below_threshold",
                strategy=self.name,
                symbol=self.symbol,
                net_funding_differential=float(nfd),
                min_threshold=float(self.min_funding_differential),
                nfd_abs=float(abs(nfd)),
                action="skipping_opportunity",
                message=f"NFD ({nfd:.6f}%) below threshold ({self.min_funding_differential:.6f}%)",
            )
            return False

        return True

    def _calculate_profit_and_costs(self, nfd: Decimal) -> tuple[Decimal, float] | None:
        """Calculate expected profit and utility score."""
        # Estimate position size (will be refined by risk manager)
        position_size = Decimal("1000.0")

        # Get spot symbol for cost calculation
        spot_symbol = self.symbol_mapping.get(self.symbol)
        if spot_symbol is None:
            return None

        # Estimate trading costs
        perp_slippage = self._estimate_slippage(self.symbol, position_size, self.perp_exchange)
        spot_slippage = self._estimate_slippage(spot_symbol, position_size, self.spot_exchange)

        perp_fee_rate = self._get_decimal_param(f"{self.perp_exchange}_fee_rate", Decimal("0.0005"))
        spot_fee_rate = self._get_decimal_param(f"{self.spot_exchange}_fee_rate", Decimal("0.0005"))

        # Calculate total costs (Decimal math)
        total_costs = position_size * (
            perp_slippage + spot_slippage + perp_fee_rate + spot_fee_rate
        )

        # Calculate expected profit (Decimal math)
        expected_profit = (position_size * nfd) - total_costs

        # Skip if expected profit is below threshold
        if expected_profit < self.min_profit_threshold:
            logger.debug(
                "expected_profit_below_threshold",
                strategy=self.name,
                symbol=self.symbol,
                expected_profit=float(expected_profit),
                min_profit_threshold=float(self.min_profit_threshold),
                net_funding_differential=float(nfd),
                position_size=float(position_size),
                total_costs=float(total_costs),
                action="skipping_opportunity",
                message=(
                    f"Expected profit (${expected_profit:.2f}) below threshold "
                    f"(${self.min_profit_threshold:.2f})"
                ),
            )
            return None

        # Calculate utility score
        basis_volatility = self._calculate_basis_volatility(self.symbol)
        utility_score = expected_profit - (self.risk_aversion * (basis_volatility**2))

        return expected_profit, float(utility_score)

    def _get_entry_prices(
        self,
        perp_ticker: Ticker,
        spot_ticker: Ticker,
        perp_side: str,
    ) -> tuple[Decimal, Decimal] | None:
        """Get entry prices for long and short positions."""
        # Ensure prices are not None before using them
        entry_perp_price = perp_ticker.price
        entry_spot_price = spot_ticker.price

        if entry_perp_price is None or entry_spot_price is None:
            spot_symbol = self.symbol_mapping.get(self.symbol)
            logger.warning(
                "entry_prices_unavailable_none_ticker_price",
                strategy=self.name,
                symbol=self.symbol,
                spot_symbol=spot_symbol,
                perp_exchange=self.perp_exchange,
                spot_exchange=self.spot_exchange,
                perp_price=float(entry_perp_price) if entry_perp_price else None,
                spot_price=float(entry_spot_price) if entry_spot_price else None,
                action="skipping_opportunity",
                message=(
                    f"Cannot determine entry prices due to None ticker price for "
                    f"{self.symbol} or {spot_symbol}"
                ),
            )
            return None

        long_price = entry_perp_price if perp_side == "LONG" else entry_spot_price
        short_price = entry_spot_price if perp_side == "LONG" else entry_perp_price

        return long_price, short_price

    def _create_opportunity_object(
        self,
        now: datetime,
        nfd: Decimal,
        expected_profit: Decimal,
        utility_score: float,
        basis_volatility: Decimal,
        perp_side: str,
        long_price: Decimal,
        short_price: Decimal,
    ) -> ArbitrageOpportunity:
        """Create the arbitrage opportunity object."""
        opportunity = ArbitrageOpportunity(
            symbol=self.symbol,
            long_exchange=self.perp_exchange if perp_side == "LONG" else self.spot_exchange,
            short_exchange=self.spot_exchange if perp_side == "LONG" else self.perp_exchange,
            long_price=long_price,
            short_price=short_price,
            long_funding_rate=nfd if perp_side == "LONG" else Decimal(0),
            short_funding_rate=nfd if perp_side == "SHORT" else Decimal(0),
            net_funding_differential=nfd,
            timestamp=now,
            expected_profit=expected_profit,
            utility_score=utility_score,
            basis_volatility=float(basis_volatility),
            optimal_size=None,
        )

        logger.info(
            "arbitrage_opportunity_found",
            strategy=self.name,
            opportunity_id=opportunity.id,
            symbol=opportunity.symbol,
            long_exchange=opportunity.long_exchange,
            short_exchange=opportunity.short_exchange,
            net_funding_differential=float(opportunity.net_funding_differential),
            expected_profit=float(opportunity.expected_profit)
            if opportunity.expected_profit is not None
            else 0.0,
            utility_score=opportunity.utility_score,
            basis_volatility=opportunity.basis_volatility,
            long_price=float(opportunity.long_price),
            short_price=float(opportunity.short_price),
            message=(
                f"Found arbitrage opportunity {opportunity.id} for {opportunity.symbol}: "
                f"NFD {opportunity.net_funding_differential:.6f}%, "
                f"Expected profit ${opportunity.expected_profit:.2f}"
            ),
        )
        return opportunity

    def _calculate_basis_volatility(self, symbol: str) -> Decimal:
        """Calculate the volatility of the basis between perp and spot markets.

        Args:
            symbol: Trading symbol

        Returns:
            Basis volatility as standard deviation

        """
        if symbol not in self.historical_basis or len(self.historical_basis[symbol]) < 2:
            return Decimal("0.01")
        basis_values = [Decimal(str(b)) for _, b in self.historical_basis[symbol]]
        mean = sum(basis_values) / Decimal(len(basis_values))
        variance = sum((x - mean) ** 2 for x in basis_values) / Decimal(len(basis_values))
        try:
            if variance < 0:
                logger.warning(
                    "negative_variance_calculated",
                    strategy=self.name,
                    symbol=symbol,
                    variance=float(variance),
                    basis_values_count=len(basis_values),
                    default_volatility=0.01,
                    action="using_default_volatility",
                    message=(
                        f"Calculated negative variance ({variance}) for basis volatility of "
                        f"{symbol}. Returning default."
                    ),
                )
                return Decimal("0.01")
            return variance.sqrt()
        except Exception as e:
            logger.error(
                "variance_sqrt_calculation_error",
                strategy=self.name,
                symbol=symbol,
                variance=float(variance),
                error=str(e),
                default_volatility=0.01,
                action="using_default_volatility",
                message=f"Error calculating sqrt of variance {variance} for {symbol}: {e}",
                exc_info=True,
            )
            return Decimal("0.01")

    def _estimate_slippage(self, symbol: str, size: Decimal, exchange: str) -> Decimal:
        """Estimate slippage for a given symbol, size, and exchange.

        Args:
            symbol: Trading symbol
            size: Position size in USD (Decimal)
            exchange: Exchange name

        Returns:
            Estimated slippage as a percentage (Decimal)

        """
        base_slippage = Decimal("0.0001")
        ref_size = Decimal(10000)
        if size <= Decimal(0) or ref_size <= Decimal(0):
            return base_slippage
        try:
            size_ratio = size / ref_size
            if size_ratio < 0:
                logger.warning(
                    "negative_size_ratio_calculated",
                    strategy=self.name,
                    symbol=symbol,
                    exchange=exchange,
                    size=float(size),
                    ref_size=float(ref_size),
                    size_ratio=float(size_ratio),
                    base_slippage=float(base_slippage),
                    action="using_base_slippage",
                    message=(
                        f"Calculated negative size ratio ({size_ratio}) for slippage of "
                        f"{symbol}. Using base."
                    ),
                )
                return base_slippage
            slippage_scaling = size_ratio.sqrt()
        except Exception as e:
            logger.error(
                "size_ratio_sqrt_calculation_error",
                strategy=self.name,
                symbol=symbol,
                exchange=exchange,
                size=float(size),
                ref_size=float(ref_size),
                error=str(e),
                base_slippage=float(base_slippage),
                action="using_base_slippage",
                message=f"Error calculating sqrt of size_ratio for {symbol} slippage: {e}",
                exc_info=True,
            )
            return base_slippage
        return base_slippage * slippage_scaling

    async def process_data(self, data: Candle) -> list[TradeSignal]:
        """Process incoming market data (Candle).

        Args:
            data: Market data to process

        Returns:
            List of TradeSignals (multi-leg, e.g., for both perp and spot) if trades should be
            executed, or an empty list if no opportunity is found.
            The returned list may be empty if no valid signals are generated.

        """
        self.update_historical_data(data)
        now = datetime.now(UTC)
        signals: list[TradeSignal] = []

        # Time-based check for funding opportunities
        if (
            self.last_opportunity_check is None
            or (now - self.last_opportunity_check).total_seconds() >= self.check_interval
        ):
            opportunity_signals = await self.evaluate_entry_opportunity()
            if opportunity_signals:
                signals.extend(opportunity_signals)
            # self.last_opportunity_check is updated within evaluate_entry_opportunity

        # Rebalance check (can be independent or related to opportunity checks)
        # Fetch live tickers for rebalance check
        # data_handler is guaranteed by __init__ to be non-None
        perp_ticker_live = self.data_handler.get_latest_ticker(self.perp_exchange, self.symbol)

        spot_symbol_mapped = self.symbol_mapping.get(self.symbol)
        spot_ticker_live: Ticker | None = None
        if spot_symbol_mapped:
            spot_ticker_live = self.data_handler.get_latest_ticker(
                self.spot_exchange,
                spot_symbol_mapped,
            )

        if self._should_rebalance(perp_ticker_live, spot_ticker_live):
            rebalance_signals = self._generate_rebalance_signal(perp_ticker_live, spot_ticker_live)
            if rebalance_signals:
                signals.extend(rebalance_signals)

        return signals

    async def evaluate_entry_opportunity(self) -> list[TradeSignal] | None:
        """Core logic to check for opportunities and generate trading signals."""
        self.last_opportunity_check = datetime.now(UTC)
        signals: list[TradeSignal] = []

        if self.risk_manager is None:
            logger.error(
                "risk_manager_not_initialized",
                strategy=self.name,
                symbol=self.symbol,
                method="evaluate_entry_opportunity",
                action="returning_none",
                message=(
                    f"RiskManager not initialized in {self.name} for evaluate_entry_opportunity"
                ),
            )
            return None

        # Fetch current tickers needed by helper methods first
        # data_handler is guaranteed by __init__ to be non-None
        perp_ticker_live = self.data_handler.get_latest_ticker(self.perp_exchange, self.symbol)

        spot_symbol_mapped = self.symbol_mapping.get(self.symbol)
        spot_ticker_live: Ticker | None = None
        if spot_symbol_mapped:
            spot_ticker_live = self.data_handler.get_latest_ticker(
                self.spot_exchange,
                spot_symbol_mapped,
            )

        # Check for rebalancing first
        if self._should_rebalance(perp_ticker_live, spot_ticker_live):
            rebalance_signals = self._generate_rebalance_signal(perp_ticker_live, spot_ticker_live)
            if rebalance_signals:
                signals.extend(rebalance_signals)
                # Potentially return early or manage state to avoid conflicting entry signals
                logger.info(
                    "rebalance_signals_generated",
                    strategy=self.name,
                    symbol=self.symbol,
                    signals_count=len(rebalance_signals),
                    action="returning_early_to_avoid_conflicts",
                    message=(
                        f"Generated {len(rebalance_signals)} rebalance signals for {self.symbol}"
                    ),
                )
                return signals

        opportunity = await self._check_opportunity()
        if opportunity:
            logger.info(
                "arbitrage_opportunity_detected",
                strategy=self.name,
                opportunity_id=opportunity.id,
                symbol=opportunity.symbol,
                net_funding_differential=float(opportunity.net_funding_differential),
                expected_profit=float(opportunity.expected_profit)
                if opportunity.expected_profit is not None
                else 0.0,
                utility_score=opportunity.utility_score,
                action="adding_to_active_opportunities",
                message=f"Found opportunity: {opportunity.id} for {opportunity.symbol}",
            )
            self.active_opportunities.append(opportunity)

            sized_opportunity_raw = self.risk_manager.size_opportunity(opportunity)
            sized_opportunity = cast("SizedOpportunity | None", sized_opportunity_raw)

            if (
                sized_opportunity
                and sized_opportunity.long_size > Decimal(0)
                and sized_opportunity.short_size > Decimal(0)
            ):
                self.sized_opportunities[opportunity.id] = sized_opportunity
                entry_signals = self._generate_entry_signal(
                    opportunity,
                    sized_opportunity,
                    perp_ticker_live,
                    spot_ticker_live,
                )
                if entry_signals:
                    signals.extend(entry_signals)
                    logger.info(
                        "entry_signals_generated",
                        strategy=self.name,
                        opportunity_id=opportunity.id,
                        symbol=opportunity.symbol,
                        signals_count=len(entry_signals),
                        long_size=float(sized_opportunity.long_size),
                        short_size=float(sized_opportunity.short_size),
                        action="signals_added_to_queue",
                        message=(
                            f"Generated {len(entry_signals)} entry signals for {opportunity.id}"
                        ),
                    )
            else:
                logger.info(
                    "opportunity_not_sized_or_zero_size",
                    strategy=self.name,
                    opportunity_id=opportunity.id,
                    symbol=opportunity.symbol,
                    sized_opportunity_exists=sized_opportunity is not None,
                    long_size=float(sized_opportunity.long_size) if sized_opportunity else None,
                    short_size=float(sized_opportunity.short_size) if sized_opportunity else None,
                    action="no_entry_signals_generated",
                    message=(
                        f"Opportunity {opportunity.id} not sized or size is zero, "
                        f"no entry signals generated"
                    ),
                )
        return signals or None

    def _should_rebalance(
        self,
        perp_ticker_live: Ticker | None,
        spot_ticker_live: Ticker | None,
    ) -> bool:
        """Determine if rebalancing is needed based on current positions and market prices."""
        # The portfolio_tracker is guaranteed non-None by __init__.
        # The check for has_active_positions was problematic, subsequent logic
        # handles position existence.
        # if not self.portfolio_tracker: # REMOVED
        #    or not self.portfolio_tracker.has_active_positions(self.name): # REMOVED

        # Get current positions
        perp_position = self.portfolio_tracker.get_position(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol)
        if not spot_symbol:
            logger.error(
                "spot_symbol_not_mapped",
                strategy=self.name,
                symbol=self.symbol,
                method="_should_rebalance",
                available_mappings=list(self.symbol_mapping.keys()),
                action="returning_false",
                message=f"Spot symbol not mapped for {self.symbol} in _should_rebalance",
            )
            return False
        spot_position = self.portfolio_tracker.get_position(self.spot_exchange, spot_symbol)

        # Ensure positions are not None before accessing .size or using in calculations
        if perp_position is None or spot_position is None:
            logger.warning(
                "positions_none_cannot_evaluate_rebalance",
                strategy=self.name,
                symbol=self.symbol,
                spot_symbol=spot_symbol,
                perp_exchange=self.perp_exchange,
                spot_exchange=self.spot_exchange,
                perp_position_exists=perp_position is not None,
                spot_position_exists=spot_position is not None,
                action="returning_false",
                message="Perp or spot position is None, cannot evaluate rebalance",
            )
            return False
        # Assuming DerivativePosition.size is Decimal (not Decimal | None)
        # If .size itself can be None, further checks are needed here.

        if (
            perp_ticker_live is None or spot_ticker_live is None
        ):  # Short-circuit before accessing .price
            logger.warning(
                "live_ticker_data_unavailable_ticker_none",
                strategy=self.name,
                symbol=self.symbol,
                perp_exchange=self.perp_exchange,
                spot_exchange=self.spot_exchange,
                perp_ticker_exists=perp_ticker_live is not None,
                spot_ticker_exists=spot_ticker_live is not None,
                action="returning_false",
                message="Live ticker data unavailable for rebalance check (ticker object None)",
            )
            return False

        current_perp_live_price = perp_ticker_live.price
        current_spot_live_price = spot_ticker_live.price

        if current_perp_live_price is None or current_spot_live_price is None:
            logger.warning(
                "live_ticker_price_unavailable_price_none",
                strategy=self.name,
                symbol=self.symbol,
                perp_exchange=self.perp_exchange,
                spot_exchange=self.spot_exchange,
                perp_price=float(current_perp_live_price) if current_perp_live_price else None,
                spot_price=float(current_spot_live_price) if current_spot_live_price else None,
                action="returning_false",
                message="Live ticker data unavailable for rebalance check (price is None)",
            )
            return False

        perp_price_val = current_perp_live_price
        spot_price_val = current_spot_live_price

        # Values can now be calculated as positions are confirmed to be not None
        perp_value = perp_position.size * perp_price_val
        spot_value = spot_position.size * spot_price_val

        # Check imbalance (simplified: absolute difference in values)
        imbalance = abs(perp_value + spot_value)  # Assuming one is short, one is long
        total_value = abs(perp_value) + abs(spot_value)

        if total_value == Decimal(0):
            return False  # Avoid division by zero if no value

        imbalance_ratio = imbalance / total_value
        logger.debug(
            "rebalance_check_imbalance_ratio",
            strategy=self.name,
            symbol=self.symbol,
            imbalance_ratio=float(imbalance_ratio),
            rebalance_threshold=float(self.rebalance_threshold),
            imbalance=float(imbalance),
            total_value=float(total_value),
            perp_value=float(perp_value),
            spot_value=float(spot_value),
            needs_rebalance=imbalance_ratio > self.rebalance_threshold,
            message=f"Rebalance check: Imbalance ratio {imbalance_ratio:.4f} for {self.symbol}",
        )
        return imbalance_ratio > self.rebalance_threshold

    def _generate_entry_signal(
        self,
        opportunity: ArbitrageOpportunity,
        sized_opportunity: SizedOpportunity | None = None,
        perp_ticker_entry: Ticker | None = None,  # ADDED param
        spot_ticker_entry: Ticker | None = None,  # ADDED param
    ) -> list[TradeSignal]:
        """Generate entry signals for a given opportunity."""
        signals: list[TradeSignal] = []
        default_size = Decimal(0)  # Default for quantity if price is zero

        perp_side = OrderSide.BUY if opportunity.net_funding_differential > 0 else OrderSide.SELL
        spot_side = OrderSide.SELL if opportunity.net_funding_differential > 0 else OrderSide.BUY

        perp_exchange_name = (
            opportunity.long_exchange if perp_side == OrderSide.BUY else opportunity.short_exchange
        )
        spot_exchange_name = (
            opportunity.short_exchange if perp_side == OrderSide.BUY else opportunity.long_exchange
        )

        # Use live tickers passed as arguments
        # perp_ticker_entry was passed
        # spot_ticker_entry was passed

        perp_price_entry = (
            perp_ticker_entry.price
            if perp_ticker_entry is not None and perp_ticker_entry.price is not None
            else default_size
        )
        spot_price_entry = (
            spot_ticker_entry.price
            if spot_ticker_entry is not None and spot_ticker_entry.price is not None
            else default_size
        )

        # Determine quantities
        perp_quantity = default_size
        spot_quantity = default_size

        if sized_opportunity:
            # Use sized opportunity details if available
            perp_size = (
                sized_opportunity.long_size
                if perp_side == OrderSide.BUY
                else sized_opportunity.short_size
            )
            spot_size = (
                sized_opportunity.short_size
                if perp_side == OrderSide.BUY
                else sized_opportunity.long_size
            )

            perp_quantity = (
                perp_size / perp_price_entry if perp_price_entry > Decimal(0) else default_size
            )
            spot_quantity = (
                spot_size / spot_price_entry if spot_price_entry > Decimal(0) else default_size
            )
        else:
            # Fallback if no sized_opportunity (should ideally not happen
            # if risk_manager is used properly)
            # For non-sized opportunity, perhaps use a default quantity or skip
            # if prices are zero
            if perp_price_entry <= Decimal(0) or spot_price_entry <= Decimal(0):
                logger.warning(
                    "prices_zero_or_invalid_non_sized_opportunity",
                    strategy=self.name,
                    opportunity_id=opportunity.id,
                    symbol=opportunity.symbol,
                    perp_price=float(perp_price_entry),
                    spot_price=float(spot_price_entry),
                    action="returning_empty_signals",
                    message=(
                        "Prices for non-sized opportunity are zero/None or invalid, "
                        "cannot determine quantity"
                    ),
                )
                return []  # For safety, do not proceed if not properly sized.
            # Simplified: use a nominal quantity based on opportunity target if not sized
            # This part needs careful consideration for a real strategy.
            # For now, we assume sizing is always done by risk manager if opportunity is pursued.
            logger.warning(
                "no_sized_opportunity_fallback_risky",
                strategy=self.name,
                opportunity_id=opportunity.id,
                symbol=opportunity.symbol,
                expected_profit=float(opportunity.expected_profit)
                if opportunity.expected_profit is not None
                else 0.0,
                action="returning_empty_signals_for_safety",
                message=(
                    f"No sized_opportunity for {opportunity.id}, using fallback "
                    f"quantities might be risky"
                ),
            )
            # Example fallback (not recommended for live trading without proper sizing logic):
            #     nominal_trade_value / perp_price_entry # MODIFIED
            #     if perp_price_entry > Decimal(0) else default_size
            #     nominal_trade_value / spot_price_entry # MODIFIED
            #     if spot_price_entry > Decimal(0) else default_size
            return []  # For safety, do not proceed if not properly sized.

        if perp_quantity > Decimal(0):
            signals.append(
                TradeSignal(
                    source_strategy=self.name,
                    symbol=opportunity.symbol,  # Exchange-specific symbol for perp_exchange_name
                    exchange=perp_exchange_name,  # Use the correct exchange for this leg
                    side=perp_side,
                    price=opportunity.long_price
                    if perp_side == OrderSide.BUY
                    else opportunity.short_price,
                    quantity=perp_quantity,
                    signal_type=SignalType.ENTER_LONG
                    if perp_side == OrderSide.BUY
                    else SignalType.ENTER_SHORT,
                    timestamp=datetime.now(UTC),
                    confidence=opportunity.confidence_score,  # Pass confidence
                    metadata={
                        "strategy_name": self.name,
                        "opportunity_id": opportunity.id,
                    },
                ),
            )
        if spot_quantity > Decimal(0):
            # Map to spot symbol for the spot exchange signal
            spot_symbol_for_signal = self.symbol_mapping.get(opportunity.symbol, opportunity.symbol)
            signals.append(
                TradeSignal(
                    source_strategy=self.name,
                    symbol=spot_symbol_for_signal,  # Use the mapped spot symbol
                    exchange=spot_exchange_name,  # Use the correct exchange for this leg
                    side=spot_side,
                    price=opportunity.short_price
                    if perp_side == OrderSide.BUY
                    else opportunity.long_price,
                    quantity=spot_quantity,
                    signal_type=SignalType.ENTER_SHORT
                    if spot_side == OrderSide.SELL
                    else SignalType.ENTER_LONG,
                    timestamp=datetime.now(UTC),
                    confidence=opportunity.confidence_score,  # Pass confidence
                    metadata={
                        "strategy_name": self.name,
                        "opportunity_id": opportunity.id,
                    },
                ),
            )
        self.signals_generated += len(signals)
        return signals

    def _generate_rebalance_signal(
        self,
        perp_ticker_live: Ticker | None,
        spot_ticker_live: Ticker | None,
    ) -> list[TradeSignal]:
        """Generate rebalancing signals based on current positions and target delta."""
        signals: list[TradeSignal] = []
        if not self.portfolio_tracker:
            logger.error(
                "portfolio_tracker_not_set",
                strategy=self.name,
                symbol=self.symbol,
                method="_generate_rebalance_signal",
                action="returning_empty_signals",
                message=f"PortfolioTracker not set for {self.name}",
            )
            return signals

        # Get positions and validate
        position_data = self._get_and_validate_positions()
        if position_data is None:
            return signals

        perp_position, spot_position, spot_symbol = position_data

        # Get and validate prices
        price_data = self._get_and_validate_rebalance_prices(perp_ticker_live, spot_ticker_live)
        if price_data is None:
            return signals

        perp_price_rebal, spot_price_rebal = price_data

        # Calculate net exposure and check if rebalancing is needed
        net_exposure = self._calculate_net_exposure(
            perp_position,
            spot_position,
            perp_price_rebal,
            spot_price_rebal,
        )

        if not self._should_rebalance_based_on_exposure(
            net_exposure,
            perp_position,
            spot_position,
            perp_price_rebal,
            spot_price_rebal,
        ):
            return signals

        # Generate rebalance signal
        rebalance_signal = self._create_rebalance_signal(
            net_exposure,
            perp_position,
            spot_position,
            perp_price_rebal,
            spot_price_rebal,
            spot_symbol,
        )

        if rebalance_signal:
            signals.append(rebalance_signal)

        return signals

    def _get_and_validate_positions(
        self,
    ) -> tuple[DerivativePosition, DerivativePosition, str] | None:
        """Get and validate perp and spot positions."""
        perp_position = self.portfolio_tracker.get_position(self.perp_exchange, self.symbol)
        spot_symbol = self.symbol_mapping.get(self.symbol)
        if not spot_symbol:
            logger.error(
                "spot_symbol_not_mapped_rebalance",
                strategy=self.name,
                symbol=self.symbol,
                method="_get_and_validate_positions",
                available_mappings=list(self.symbol_mapping.keys()),
                action="returning_none",
                message=f"Spot symbol not mapped for {self.symbol} in _generate_rebalance_signal",
            )
            return None
        spot_position = self.portfolio_tracker.get_position(self.spot_exchange, spot_symbol)

        # Check if positions are None before accessing .size
        if perp_position is None or spot_position is None:
            logger.warning(
                "positions_none_cannot_rebalance",
                strategy=self.name,
                symbol=self.symbol,
                spot_symbol=spot_symbol,
                perp_exchange=self.perp_exchange,
                spot_exchange=self.spot_exchange,
                perp_position_exists=perp_position is not None,
                spot_position_exists=spot_position is not None,
                action="returning_none",
                message="Perp or spot position is None, cannot rebalance",
            )
            return None

        return perp_position, spot_position, spot_symbol

    def _get_and_validate_rebalance_prices(
        self,
        perp_ticker_live: Ticker | None,
        spot_ticker_live: Ticker | None,
    ) -> tuple[Decimal, Decimal] | None:
        """Get and validate live prices for rebalancing."""
        if perp_ticker_live is None or spot_ticker_live is None:
            logger.warning(
                "live_ticker_unavailable_rebalance_ticker_none",
                strategy=self.name,
                symbol=self.symbol,
                perp_exchange=self.perp_exchange,
                spot_exchange=self.spot_exchange,
                perp_ticker_exists=perp_ticker_live is not None,
                spot_ticker_exists=spot_ticker_live is not None,
                action="returning_none",
                message="Live ticker data unavailable for rebalance check (ticker object None)",
            )
            return None

        current_perp_live_price = perp_ticker_live.price
        current_spot_live_price = spot_ticker_live.price

        if current_perp_live_price is None or current_spot_live_price is None:
            logger.warning(
                "live_ticker_price_unavailable_rebalance_price_none",
                strategy=self.name,
                symbol=self.symbol,
                perp_exchange=self.perp_exchange,
                spot_exchange=self.spot_exchange,
                perp_price=float(current_perp_live_price) if current_perp_live_price else None,
                spot_price=float(current_spot_live_price) if current_spot_live_price else None,
                action="returning_none",
                message="Live ticker data unavailable for rebalance check (price is None)",
            )
            return None

        return current_perp_live_price, current_spot_live_price

    def _calculate_net_exposure(
        self,
        perp_position: DerivativePosition,
        spot_position: DerivativePosition,
        perp_price: Decimal,
        spot_price: Decimal,
    ) -> Decimal:
        """Calculate net exposure across perp and spot positions."""
        # Simplified rebalancing: aim for market-neutral by value
        # Positive size = long, negative size = short
        perp_value = perp_position.size * perp_price
        spot_value = spot_position.size * spot_price
        return perp_value + spot_value

    def _should_rebalance_based_on_exposure(
        self,
        net_exposure: Decimal,
        perp_position: DerivativePosition,
        spot_position: DerivativePosition,
        perp_price: Decimal,
        spot_price: Decimal,
    ) -> bool:
        """Determine if rebalancing is needed based on net exposure."""
        # If net exposure is significant, rebalance
        # Example: rebalance if exposure is > 1% of the smaller leg's absolute value
        perp_value = perp_position.size * perp_price
        spot_value = spot_position.size * spot_price
        threshold_value = min(abs(perp_value), abs(spot_value)) * Decimal("0.01")

        if threshold_value == Decimal(0) and net_exposure != Decimal(0):
            logger.info(
                "rebalance_needed_zero_threshold_nonzero_exposure",
                strategy=self.name,
                symbol=self.symbol,
                net_exposure=float(net_exposure),
                threshold_value=float(threshold_value),
                perp_value=float(perp_value),
                spot_value=float(spot_value),
                action="triggering_rebalance",
                message=(
                    f"Rebalance needed for {self.symbol} due to zero threshold and "
                    f"non-zero exposure: {net_exposure}"
                ),
            )
            return True
        if abs(net_exposure) > threshold_value > Decimal(0):
            logger.info(
                "rebalance_triggered_exposure_exceeds_threshold",
                strategy=self.name,
                symbol=self.symbol,
                net_exposure=float(net_exposure),
                net_exposure_abs=float(abs(net_exposure)),
                threshold_value=float(threshold_value),
                perp_value=float(perp_value),
                spot_value=float(spot_value),
                action="triggering_rebalance",
                message=(
                    f"Rebalance triggered for {self.symbol}. Net exposure: {net_exposure}, "
                    f"Threshold: {threshold_value}"
                ),
            )
            return True

        return False

    def _create_rebalance_signal(
        self,
        net_exposure: Decimal,
        perp_position: DerivativePosition,
        spot_position: DerivativePosition,
        perp_price: Decimal,
        spot_price: Decimal,
        spot_symbol: str,
    ) -> TradeSignal | None:
        """Create a rebalance signal based on net exposure."""
        if abs(net_exposure) < Decimal("0.01"):  # Effectively zero, no rebalance needed
            return None

        # Determine which leg to adjust and by how much
        if net_exposure > Decimal(0):  # Net long, need to sell something
            if perp_position.size > 0:  # If perp is long, sell some
                side = OrderSide.SELL
                exchange_to_adjust = self.perp_exchange
                symbol_to_adjust = self.symbol
                price_to_use = perp_price
            else:  # Perp is short, sell more of spot (if spot is long)
                side = OrderSide.SELL
                exchange_to_adjust = self.spot_exchange
                symbol_to_adjust = spot_symbol
                price_to_use = spot_price
        elif perp_position.size < 0:  # If perp is short, buy some back
            side = OrderSide.BUY
            exchange_to_adjust = self.perp_exchange
            symbol_to_adjust = self.symbol
            price_to_use = perp_price
        else:  # Perp is long, buy more of spot (if spot is short)
            side = OrderSide.BUY
            exchange_to_adjust = self.spot_exchange
            symbol_to_adjust = spot_symbol
            price_to_use = spot_price

        if price_to_use <= Decimal(0):
            logger.warning(
                "rebalance_price_zero_or_none",
                strategy=self.name,
                symbol=self.symbol,
                symbol_to_adjust=symbol_to_adjust,
                exchange_to_adjust=exchange_to_adjust,
                price_to_use=float(price_to_use),
                net_exposure=float(net_exposure),
                action="returning_none",
                message=(
                    f"Price for rebalancing {symbol_to_adjust} is zero or None, "
                    f"cannot calculate quantity"
                ),
            )
            return None

        quantity_to_rebalance = abs(net_exposure) / price_to_use

        if quantity_to_rebalance <= Decimal(0):
            return None

        signal = TradeSignal(
            source_strategy=self.name,
            symbol=symbol_to_adjust,
            exchange=exchange_to_adjust,
            side=side,
            price=price_to_use,  # Use current market price for rebalance
            quantity=quantity_to_rebalance,
            signal_type=SignalType.REBALANCE,
            timestamp=datetime.now(UTC),
            # Confidence for rebalance might be different or not applicable
            confidence=1.0,  # MODIFIED: Decimal to float
            metadata={
                "strategy_name": self.name,
                "opportunity_id": "REBALANCE-" + datetime.now(UTC).isoformat(),
            },
        )

        logger.info(
            "rebalance_signal_generated",
            strategy=self.name,
            symbol=self.symbol,
            signal_side=side.value,
            quantity=float(quantity_to_rebalance),
            symbol_to_adjust=symbol_to_adjust,
            exchange_to_adjust=exchange_to_adjust,
            price=float(price_to_use),
            net_exposure=float(net_exposure),
            action="signal_created",
            message=(
                f"Generated rebalance signal: {side} {quantity_to_rebalance} "
                f"{symbol_to_adjust} on {exchange_to_adjust}"
            ),
        )

        return signal

    def on_start(self) -> None:
        """Start the strategy and initialize any required state."""
        logger.info(
            "strategy_started",
            strategy=self.name,
            symbol=self.symbol,
            perp_exchange=self.perp_exchange,
            spot_exchange=self.spot_exchange,
            check_interval=self.check_interval,
            action="strategy_initialization_complete",
            message=f"Strategy {self.name} started for symbol {self.symbol}",
        )
        # Potentially load historical data or prime initial state

    def on_stop(self) -> None:
        """Stop the strategy and perform cleanup operations."""
        logger.info(
            "strategy_stopped",
            strategy=self.name,
            symbol=self.symbol,
            signals_generated_total=self.signals_generated,
            active_opportunities_count=len(self.active_opportunities),
            sized_opportunities_count=len(self.sized_opportunities),
            action="strategy_cleanup_initiated",
            message=f"Strategy {self.name} stopped for symbol {self.symbol}",
        )
        # Perform any cleanup, like cancelling open orders (if strategy manages them directly)

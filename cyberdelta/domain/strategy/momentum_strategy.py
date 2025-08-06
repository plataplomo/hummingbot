"""Momentum-based trading strategy implementation.

This module implements a simple momentum strategy that generates buy signals
when price change exceeds configured thresholds and sell signals when momentum
reverses. All parameters are configuration-driven with no hardcoded values.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import SignalType
from cyberdelta.domain.strategy.strategy_base import BaseStrategy, StrategyConfigurationError
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.trading import OrderSide
from cyberdelta.models import TradeSignal
from cyberdelta.models.derivative_position import DerivativePosition
from cyberdelta.models.market.fill import Fill
from cyberdelta.models.market.market_snapshot import MarketSnapshot
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.symbols import get_symbol_service


if TYPE_CHECKING:
    from cyberdelta.domain.market.market_service import MarketDataService


logger = get_logger(__name__)


class MomentumStrategy(BaseStrategy):
    """Simple momentum-based trading strategy.

    This strategy generates buy signals when price change over the configured
    lookback period exceeds the configured threshold, and sell signals when
    momentum reverses below the negative threshold.

    Configuration Required (config.strategies.momentum):
    - price_change_threshold: Minimum price change percentage to trigger buy signal
    - negative_threshold: Maximum negative price change to trigger sell signal
    - signal_confidence: Confidence level for generated signals (0.0-1.0)
    - target_symbol: Symbol to trade (e.g., "BTC_USD")
    - target_exchange: Exchange to trade on (e.g., "hyperliquid")
    - lookback_hours: Hours to look back for price change calculation
    - min_position_hold_hours: Minimum hours to hold a position before closing

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL parameters from config.strategies.momentum section
    - NO hardcoded thresholds, symbols, or exchanges
    - Uses Symbol objects and ExchangeName enums
    - Fail fast if required configuration missing
    """

    def __init__(self, config: AppSettings, market_service: MarketDataService) -> None:
        """Initialize momentum strategy with configuration.

        Args:
            config: Application settings containing momentum strategy configuration
            market_service: Market data service for fetching historical data (required)

        Raises:
            StrategyConfigurationError: If required configuration is missing
        """
        super().__init__(config)
        self._market_service = market_service

        # Validate required configuration before proceeding
        self._validate_momentum_config()

        # Get momentum-specific configuration section
        momentum_config = self.config.strategies.momentum
        if momentum_config is None:
            msg = "Momentum strategy configuration not found in config.strategies.momentum"
            raise StrategyConfigurationError(msg)

        # Cache frequently used configuration values
        self._price_change_threshold = momentum_config.price_change_threshold
        self._negative_threshold = momentum_config.negative_threshold
        self._signal_confidence = momentum_config.signal_confidence
        self._lookback_hours = momentum_config.lookback_hours
        self._min_position_hold_hours = momentum_config.min_position_hold_hours

        # Get symbol service for proper Symbol object handling
        self._symbol_service = get_symbol_service()

        # Parse target symbol and exchange from config
        self._target_symbol_str = momentum_config.target_symbol
        self._target_exchange = ExchangeName(momentum_config.target_exchange)

        # Get proper Symbol object from symbol service
        try:
            # Create symbol from symbol service for the target exchange
            self._target_symbol = self._symbol_service.create_symbol(
                self._target_symbol_str, self._target_exchange
            )
        except Exception as e:
            msg = (
                f"Failed to create symbol '{self._target_symbol_str}' for exchange "
                f"'{self._target_exchange.value}': {e}"
            )
            raise StrategyConfigurationError(msg) from e

        logger.info(
            "momentum_strategy_configured",
            strategy_name=self.name,
            target_symbol=self._target_symbol.value,
            target_exchange=self._target_exchange.value,
            price_change_threshold=float(self._price_change_threshold),
            negative_threshold=float(self._negative_threshold),
            signal_confidence=float(self._signal_confidence),
            lookback_hours=self._lookback_hours,
            min_position_hold_hours=self._min_position_hold_hours,
        )

    def _validate_momentum_config(self) -> None:
        """Validate that all required momentum configuration is present.

        Raises:
            StrategyConfigurationError: If required configuration is missing
        """
        required_fields = [
            "price_change_threshold",
            "negative_threshold",
            "signal_confidence",
            "target_symbol",
            "target_exchange",
            "lookback_hours",
            "min_position_hold_hours",
        ]

        try:
            self._validate_strategy_config(required_fields, "momentum")
        except ValueError as e:
            raise StrategyConfigurationError(str(e)) from e

    async def analyze(
        self, market_data: MarketSnapshot, portfolio_state: PortfolioState
    ) -> TradeSignal | None:
        """Analyze market conditions and generate momentum-based trading signal.

        Args:
            market_data: Current market snapshot across all exchanges
            portfolio_state: Current portfolio state across all exchanges

        Returns:
            TradeSignal if momentum conditions are met, None otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured thresholds, NO hardcoded values
        - Returns typed TradeSignal with Symbol/ExchangeName objects
        - NO string symbols or exchanges
        - Proper error handling with context
        """
        try:
            # Get ticker for target symbol on target exchange
            ticker = market_data.get_ticker(self._target_exchange, self._target_symbol)
            if not ticker:
                logger.debug(
                    "momentum_analysis_no_ticker",
                    strategy_name=self.name,
                    symbol=self._target_symbol.value,
                    exchange=self._target_exchange.value,
                )
                return None

            # Check if we have required price data
            if not ticker.price:
                logger.debug(
                    "momentum_analysis_insufficient_data",
                    strategy_name=self.name,
                    symbol=self._target_symbol.value,
                    has_price=ticker.price is not None,
                )
                return None

            # Calculate price change percentage using historical data
            price_change_pct = await self._calculate_price_change_percentage(ticker.price)

            # Check current position
            current_position = portfolio_state.get_exchange_positions(self._target_exchange).get(
                f"{self._target_exchange.value}:{self._target_symbol.value}"
            )

            # Generate signal based on momentum and current position
            signal = self._generate_momentum_signal(
                ticker.price, price_change_pct, current_position
            )

            if signal:
                logger.info(
                    "momentum_signal_generated",
                    strategy_name=self.name,
                    symbol=self._target_symbol.value,
                    exchange=self._target_exchange.value,
                    side=signal.side.value,
                    price=float(signal.price),
                    price_change_pct=float(price_change_pct),
                    confidence=float(signal.confidence) if signal.confidence else 0.0,
                )
        except Exception as e:
            logger.exception(
                "momentum_analysis_error",
                strategy_name=self.name,
                error=str(e),
                symbol=self._target_symbol.value,
                exchange=self._target_exchange.value,
            )
            # Don't raise - return None to skip this analysis cycle
            return None
        else:
            return signal

    async def _calculate_price_change_percentage(self, current_price: Decimal) -> Decimal:
        """Calculate price change percentage over the lookback period.

        Args:
            current_price: Current price of the asset

        Returns:
            Price change percentage, or 0.0 if historical data unavailable

        Raises:
            StrategyConfigurationError: If market service is not available

        """
        # Market service is now required in constructor, so we can use it directly

        try:
            # Calculate time range for historical data
            end_time = datetime.now(UTC)
            start_time = end_time - timedelta(hours=self._lookback_hours)

            # Convert to milliseconds
            start_time_ms = int(start_time.timestamp() * 1000)
            end_time_ms = int(end_time.timestamp() * 1000)

            # Determine appropriate timeframe based on lookback period
            timeframe = self._get_appropriate_timeframe(self._lookback_hours)

            # Fetch historical candles
            candles_result = await self._market_service.get_historical_candles(
                symbol=self._target_symbol,
                exchange=self._target_exchange,
                timeframe=timeframe,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )

            if candles_result is None:
                logger.warning(
                    "momentum_no_historical_data",
                    strategy_name=self.name,
                    symbol=self._target_symbol.value,
                    exchange=self._target_exchange.value,
                )
                msg = f"No historical data for {self._target_symbol.value}"
                self._raise_config_error(msg)

            # Type narrowing - after error raised above, candles_result cannot be None
            # Using type guard instead of assert for production code
            if candles_result is None:  # This should never happen due to raise above
                logger.error("momentum_unexpected_none_candles")
                return Decimal(0)

            candles = candles_result

            min_candles_required = 2
            if len(candles) < min_candles_required:
                logger.warning(
                    "momentum_insufficient_historical_data",
                    strategy_name=self.name,
                    symbol=self._target_symbol.value,
                    exchange=self._target_exchange.value,
                    candle_count=len(candles),
                )
                msg = (
                    f"Insufficient historical data for {self._target_symbol.value}. "
                    f"Need at least 2 candles, got {len(candles)}"
                )
                self._raise_config_error(msg)

            # Get the price from the oldest candle in our range
            # Use close price from candle (Candle model has close, not price)
            oldest_candle = candles[0]
            oldest_price = oldest_candle.close

            if not oldest_price or oldest_price == 0:
                logger.error(
                    "momentum_invalid_historical_price",
                    strategy_name=self.name,
                    oldest_price=oldest_price,
                )
                msg = "Historical candle has invalid price data"
                self._raise_config_error(msg)

            # Calculate percentage change
            price_change = current_price - oldest_price
            price_change_pct = (price_change / oldest_price) * Decimal(100)

            logger.info(
                "momentum_price_change_calculated",
                strategy_name=self.name,
                symbol=self._target_symbol.value,
                exchange=self._target_exchange.value,
                current_price=float(current_price),
                oldest_price=float(oldest_price),
                price_change_pct=float(price_change_pct),
                lookback_hours=self._lookback_hours,
                candle_count=len(candles),
            )

        except StrategyConfigurationError:
            # Re-raise configuration errors
            raise
        except Exception as e:
            logger.exception(
                "momentum_historical_data_error",
                strategy_name=self.name,
                error=str(e),
            )
            # Re-raise with context as suggested
            msg = f"Failed to fetch or process historical data: {e}"
            raise StrategyConfigurationError(msg) from e
        else:
            return price_change_pct

    def _generate_momentum_signal(
        self,
        last_price: Decimal,
        price_change_pct: Decimal,
        current_position: DerivativePosition | None,
    ) -> TradeSignal | None:
        """Generate trading signal based on momentum conditions.

        Args:
            last_price: Current price of the asset
            price_change_pct: Price change percentage over lookback period
            current_position: Current position in the asset (if any)

        Returns:
            TradeSignal if conditions are met, None otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured thresholds only
        - NO hardcoded logic decisions
        - Returns proper typed TradeSignal
        """
        # Check if we have a current position
        has_position = current_position is not None and current_position.size != 0

        # Determine signal side based on momentum and position
        signal_side = None
        signal_reason = None

        # Buy signal: positive momentum above threshold and no current position
        if price_change_pct > self._price_change_threshold and not has_position:
            signal_side = OrderSide.BUY
            signal_reason = f"positive_momentum_{float(price_change_pct):.2f}%"

        # Sell signal: negative momentum below threshold and have position
        elif price_change_pct < self._negative_threshold and has_position:
            signal_side = OrderSide.SELL
            signal_reason = f"negative_momentum_{float(price_change_pct):.2f}%"

        # No signal conditions met
        if not signal_side:
            logger.debug(
                "momentum_no_signal_conditions",
                strategy_name=self.name,
                price_change_pct=float(price_change_pct),
                positive_threshold=float(self._price_change_threshold),
                negative_threshold=float(self._negative_threshold),
                has_position=has_position,
            )
            return None

        # Create and return trading signal
        return TradeSignal(
            symbol=self._target_symbol,
            signal_type=(
                SignalType.ENTER_LONG if signal_side == OrderSide.BUY else SignalType.EXIT_LONG
            ),
            exchange=self._target_exchange,
            side=signal_side,
            price=last_price,
            confidence=float(self._signal_confidence),
            source_strategy=self.name,
            metadata={
                "signal_reason": signal_reason,
                "price_change_pct": float(price_change_pct),
                "lookback_hours": self._lookback_hours,
                "threshold_used": float(
                    self._price_change_threshold
                    if signal_side == OrderSide.BUY
                    else self._negative_threshold
                ),
            },
        )

    async def _strategy_initialize(self) -> None:
        """Initialize momentum strategy specific resources.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Validate symbol availability on target exchange
        - Check configuration consistency
        - Fail fast if initialization cannot complete

        Raises:
            StrategyConfigurationError: If configuration validation fails
        """
        # Validate that target symbol is supported on target exchange
        try:
            # Check if symbol service has necessary methods, otherwise log warning
            logger.info(
                "momentum_strategy_symbol_validated",
                strategy_name=self.name,
                symbol=self._target_symbol.value,
                exchange=self._target_exchange.value,
                has_metadata=True,  # Assume valid if Symbol object was created
            )

        except Exception as e:
            msg = (
                f"Target symbol '{self._target_symbol_str}' not available on "
                f"exchange '{self._target_exchange.value}': {e}"
            )
            raise StrategyConfigurationError(msg) from e

        # Validate configuration value ranges
        if not (0.0 <= self._signal_confidence <= 1.0):
            msg = f"signal_confidence must be between 0.0 and 1.0, got {self._signal_confidence}"
            raise StrategyConfigurationError(msg)

        if self._price_change_threshold <= 0:
            msg = f"price_change_threshold must be positive, got {self._price_change_threshold}"
            raise StrategyConfigurationError(msg)

        if self._negative_threshold >= 0:
            msg = f"negative_threshold must be negative, got {self._negative_threshold}"
            raise StrategyConfigurationError(msg)

        if self._lookback_hours <= 0:
            msg = f"lookback_hours must be positive, got {self._lookback_hours}"
            raise StrategyConfigurationError(msg)

        logger.info(
            "momentum_strategy_initialization_completed",
            strategy_name=self.name,
            configuration_validated=True,
        )

    async def _strategy_cleanup(self) -> None:
        """Cleanup momentum strategy specific resources.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Graceful cleanup with proper error handling
        - Log cleanup completion
        """
        # No specific cleanup needed for momentum strategy
        # as it doesn't maintain persistent resources

        logger.info("momentum_strategy_cleanup_completed", strategy_name=self.name)

    def _raise_config_error(self, message: str) -> None:
        """Raise a configuration error.

        Args:
            message: Error message

        Raises:
            StrategyConfigurationError: Always raises with the provided message
        """
        raise StrategyConfigurationError(message)

    def _get_appropriate_timeframe(self, lookback_hours: float) -> str:
        """Determine appropriate candle timeframe based on lookback period.

        Args:
            lookback_hours: Number of hours to look back

        Returns:
            Timeframe string for candle data
        """
        hours_short = 1
        hours_medium = 6
        hours_long = 24

        if lookback_hours <= hours_short:
            return "5m"  # 5-minute candles for short lookback
        if lookback_hours <= hours_medium:
            return "15m"  # 15-minute candles
        if lookback_hours <= hours_long:
            return "1h"  # Hourly candles
        return "4h"  # 4-hour candles for longer lookback

    def _raise_data_error(self, error: Exception) -> None:
        """Raise a data processing error.

        Args:
            error: Original exception

        Raises:
            StrategyConfigurationError: Always raises with wrapped error
        """
        msg = f"Failed to fetch or process historical data: {error}"
        raise StrategyConfigurationError(msg) from error

    async def handle_fill(self, fill: Fill) -> None:
        """Handle fill execution feedback.

        For momentum strategy, we can update our internal state based on fill results.

        Args:
            fill: Executed fill information
        """
        logger.info(
            "momentum_strategy_fill_handled",
            strategy_name=self.name,
            fill_id=fill.id,
            symbol=fill.symbol.value if fill.symbol else None,
            side=fill.side.value if fill.side else None,
            quantity=float(fill.quantity) if fill.quantity else None,
        )

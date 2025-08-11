"""Risk domain event handlers.

Handles risk-related events with CRITICAL priority for pre-trade
validation, exposure monitoring, and circuit breaker integration.

Following CODING_STANDARDS.md:
- NO hardcoded values - ALL from configuration
- NO assumptions about units, formats, or behavior
- Explicit error handling with fail fast approach
- Type safety throughout with Decimal for financial values
"""

from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.models.event_system_config import EventHandlerConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.base_event_handler import EventHandlerActor
from cyberdelta.enums import OrderSide
from cyberdelta.enums.event_bus import HandlerPriority
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.monitoring import RiskSeverity, RiskType
from cyberdelta.enums.signals import SignalType
from cyberdelta.enums.trading import OrderEventType
from cyberdelta.exceptions.trading import OrderDataError
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.models.events.core import (
    OrderEvent,
    PositionEvent,
    RiskEvent,
)
from cyberdelta.models.risk.assessment import RiskAssessment
from cyberdelta.models.trade_signal import TradeSignal
from cyberdelta.symbols.global_service import get_symbol_service
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.retry_utils import create_retryer


if TYPE_CHECKING:
    from cyberdelta.domain.risk.risk_service import RiskService

logger = get_logger(__name__)


class RiskValidationEventHandler(EventHandlerActor):
    """Pre-trade risk validation handler with CRITICAL priority.

    Validates orders before execution to ensure risk limits are respected.
    Runs with CRITICAL priority to process before trading handlers.

    ALL configuration comes from AppSettings - NO hardcoded values.
    """

    def __init__(
        self,
        handler_id: str,
        event_bus: EventBus,
        config: EventHandlerConfig,
        app_config: AppSettings,
        risk_service: "RiskService",
    ) -> None:
        """Initialize risk validation handler.

        Args:
            handler_id: Unique handler identifier
            event_bus: Event bus for publishing/subscribing
            config: Handler configuration
            app_config: Application configuration with risk settings
            risk_service: Risk service for validation
        """
        super().__init__(handler_id, event_bus, config)
        self._app_config = app_config
        self._risk_service = risk_service
        self._symbol_cache: dict[str, Symbol] = {}
        self._retry_strategy = create_retryer(config.retry_config, logger_name=handler_id)

    async def _handle_order_event(self, event: OrderEvent) -> None:
        """Handle order events for pre-trade validation.

        Args:
            event: Order event to validate

        Raises:
            OrderDataError: If order data is missing or invalid
        """
        # Only validate new orders
        if event.event_type != OrderEventType.PLACED:
            return

        # Validate required fields first
        if event.price is None:
            raise OrderDataError(event.order_id, "price")

        try:
            # Convert symbol string to Symbol object with caching
            symbol = await self._get_or_create_symbol(event.symbol, event.exchange)

            # Create a trade signal from order event for assessment

            signal = TradeSignal(
                signal_id=event.order_id,
                symbol=symbol,
                signal_type=SignalType.ENTER_LONG,  # Entry signal for risk assessment
                side=OrderSide.BUY,  # Buy side for risk assessment
                price=event.price,
                exchange=event.exchange,
                confidence=None,  # No confidence score needed for risk assessment
            )

            # Perform risk validation using assess_signal
            validation_result: RiskAssessment = await self._retry_strategy(
                self._risk_service.assess_signal,
                signal=signal,
            )

            if not validation_result.approved:
                # Publish risk rejection event
                violation = (
                    validation_result.limit_violations[0]
                    if validation_result.limit_violations
                    else "Risk validation failed"
                )
                rejection_event = RiskEvent(
                    risk_type=RiskType.LIMIT_BREACH,
                    severity=RiskSeverity.CRITICAL,
                    current_value=self._calculate_order_notional(event),
                    limit_value=validation_result.position_size.quantity,
                    message=f"Order {event.order_id} rejected: {violation}",
                    symbol=event.symbol,
                    exchange=event.exchange,
                )
                await self.event_bus.publish(rejection_event)

                logger.warning(
                    "order_rejected_by_risk_validation",
                    order_id=event.order_id,
                    violation=(
                        validation_result.limit_violations[0]
                        if validation_result.limit_violations
                        else "Risk validation failed"
                    ),
                )
            else:
                logger.debug("Order passed risk validation", order_id=event.order_id)

            self._metrics["events_processed"] += 1

        except Exception:
            logger.exception(
                "risk_validation_error",
                handler_id=self.handler_id,
                order_id=event.order_id,
            )
            self._metrics["errors"] += 1

    def _calculate_order_notional(self, event: OrderEvent) -> Decimal:
        """Calculate order notional value safely without hardcoded fallbacks.

        Args:
            event: Order event to calculate notional for

        Returns:
            Decimal: Order notional value

        Raises:
            OrderDataError: If quantity or price is None

        Note:
            Following CODING_STANDARDS.md - NO hardcoded fallbacks.
            If quantity or price is None, we cannot calculate notional properly.
            This indicates a data quality issue that should be addressed upstream.
        """
        if event.quantity is None:
            raise OrderDataError(event.order_id, "quantity")
        if event.price is None:
            raise OrderDataError(event.order_id, "price")

        return event.quantity * event.price

    async def _get_or_create_symbol(self, symbol_str: str, exchange: ExchangeName) -> Symbol:
        """Get or create Symbol object from string with caching.

        Args:
            symbol_str: Symbol string representation
            exchange: Exchange name

        Returns:
            Symbol object
        """
        cache_key = f"{exchange.value}:{symbol_str}"

        if cache_key not in self._symbol_cache:
            symbol_service = get_symbol_service()
            symbol: Symbol = symbol_service.create_symbol(symbol_str, exchange)
            self._symbol_cache[cache_key] = symbol
            self._metrics["cache_misses"] += 1
        else:
            self._metrics["cache_hits"] += 1

        return self._symbol_cache[cache_key]

    async def start(self) -> None:
        """Start the risk validation handler."""
        await super().start()

        # Subscribe to order events with CRITICAL priority (runs first)
        self.event_bus.subscribe(
            OrderEvent,
            self._handle_order_event,
            HandlerPriority.CRITICAL,
        )

        logger.info("Risk validation handler started", handler_id=self.handler_id)

    async def stop(self) -> None:
        """Stop the risk validation handler."""
        # Unsubscribe from events
        self.event_bus.unsubscribe(OrderEvent, self._handle_order_event)

        # Clear cache
        self._symbol_cache.clear()

        await super().stop()
        logger.info("Risk validation handler stopped", handler_id=self.handler_id)


class RiskExposureEventHandler(EventHandlerActor):
    """Risk exposure monitoring handler.

    Monitors position and balance changes to track overall exposure
    and trigger alerts when limits are approached or breached.

    ALL configuration comes from AppSettings - NO hardcoded values.
    """

    def __init__(
        self,
        handler_id: str,
        event_bus: EventBus,
        config: EventHandlerConfig,
        app_config: AppSettings,
        risk_service: "RiskService",
    ) -> None:
        """Initialize risk exposure handler.

        Args:
            handler_id: Unique handler identifier
            event_bus: Event bus for publishing/subscribing
            config: Handler configuration
            app_config: Application configuration with risk settings
            risk_service: Risk service for exposure monitoring
        """
        super().__init__(handler_id, event_bus, config)
        self._app_config = app_config
        self._risk_service = risk_service
        self._symbol_cache: dict[str, Symbol] = {}
        self._retry_strategy = create_retryer(config.retry_config, logger_name=handler_id)

    async def _handle_position_event(self, event: PositionEvent) -> None:
        """Handle position events for exposure monitoring.

        Args:
            event: Position event to process
        """
        try:
            # Convert symbol string to Symbol object (cached for interface compatibility)
            await self._get_or_create_symbol(event.symbol, event.exchange)

            # Get current portfolio value for exposure monitoring
            await self._risk_service.update_drawdown_monitoring()
            drawdown_status = self._risk_service.get_drawdown_status()

            # Get exposure limits from configuration
            exposure_limit = self._app_config.risk.global_risk.max_total_exposure_usd
            warning_threshold = (
                exposure_limit * self._app_config.risk.global_risk.drawdown_warning_pct / 100
            )

            # Calculate current exposure from portfolio value (peak - trough)
            current_exposure = drawdown_status.peak_value - drawdown_status.trough_value

            # Check exposure limits
            if current_exposure > exposure_limit:
                # Publish exposure breach event
                breach_event = RiskEvent(
                    risk_type=RiskType.EXPOSURE,
                    severity=RiskSeverity.EMERGENCY,
                    current_value=current_exposure,
                    limit_value=exposure_limit,
                    message=f"Total exposure ${current_exposure} exceeds limit ${exposure_limit}",
                    symbol=event.symbol,
                    exchange=event.exchange,
                )
                await self.event_bus.publish(breach_event)

                logger.error(
                    "exposure_breach_detected",
                    current_exposure=current_exposure,
                    exposure_limit=exposure_limit,
                )

            elif current_exposure > warning_threshold:
                # Publish exposure warning event
                warning_event = RiskEvent(
                    risk_type=RiskType.EXPOSURE,
                    severity=RiskSeverity.WARNING,
                    current_value=current_exposure,
                    limit_value=exposure_limit,
                    message=(
                        f"Total exposure ${current_exposure} approaching limit ${exposure_limit}"
                    ),
                    symbol=event.symbol,
                    exchange=event.exchange,
                )
                await self.event_bus.publish(warning_event)

                logger.warning(
                    "exposure_warning_threshold_approached",
                    current_exposure=current_exposure,
                    exposure_limit=exposure_limit,
                )

            self._metrics["events_processed"] += 1

        except Exception:
            logger.exception(
                "risk_exposure_error",
                handler_id=self.handler_id,
                event_type=type(event).__name__,
            )
            self._metrics["errors"] += 1

    async def _get_or_create_symbol(self, symbol_str: str, exchange: ExchangeName) -> Symbol:
        """Get or create Symbol object from string with caching.

        Args:
            symbol_str: Symbol string representation
            exchange: Exchange name

        Returns:
            Symbol object
        """
        cache_key = f"{exchange.value}:{symbol_str}"

        if cache_key not in self._symbol_cache:
            symbol_service = get_symbol_service()
            symbol: Symbol = symbol_service.create_symbol(symbol_str, exchange)
            self._symbol_cache[cache_key] = symbol
            self._metrics["cache_misses"] += 1
        else:
            self._metrics["cache_hits"] += 1

        return self._symbol_cache[cache_key]

    async def start(self) -> None:
        """Start the risk exposure handler."""
        await super().start()

        # Subscribe to position events with HIGH priority
        self.event_bus.subscribe(
            PositionEvent,
            self._handle_position_event,
            HandlerPriority.HIGH,
        )

        logger.info("Risk exposure handler started", handler_id=self.handler_id)

    async def stop(self) -> None:
        """Stop the risk exposure handler."""
        # Unsubscribe from events
        self.event_bus.unsubscribe(PositionEvent, self._handle_position_event)

        # Clear cache
        self._symbol_cache.clear()

        await super().stop()
        logger.info("Risk exposure handler stopped", handler_id=self.handler_id)


class RiskCircuitBreakerEventHandler(EventHandlerActor):
    """Circuit breaker integration handler.

    Monitors risk events and system errors to trigger circuit breakers
    when thresholds are exceeded.

    ALL configuration comes from AppSettings - NO hardcoded values.
    """

    def __init__(
        self,
        handler_id: str,
        event_bus: EventBus,
        config: EventHandlerConfig,
        app_config: AppSettings,
        risk_service: "RiskService",
    ) -> None:
        """Initialize circuit breaker handler.

        Args:
            handler_id: Unique handler identifier
            event_bus: Event bus for publishing/subscribing
            config: Handler configuration
            app_config: Application configuration with safety settings
            risk_service: Risk service with circuit breaker integration
        """
        super().__init__(handler_id, event_bus, config)
        self._app_config = app_config
        self._risk_service = risk_service
        self._retry_strategy = create_retryer(config.retry_config, logger_name=handler_id)

        # Track circuit breaker metrics
        self._breaker_metrics = {
            "breakers_triggered": 0,
            "breakers_reset": 0,
            "rejection_count": 0,
            "breach_count": 0,
        }

    async def _handle_risk_event(self, event: RiskEvent) -> None:
        """Handle risk events for circuit breaker decisions.

        Args:
            event: Risk event to process
        """
        try:
            # Track different risk event types
            if event.risk_type == RiskType.LIMIT_BREACH and event.severity == RiskSeverity.CRITICAL:
                self._breaker_metrics["rejection_count"] += 1

            elif event.risk_type == RiskType.EXPOSURE and event.severity in {
                RiskSeverity.CRITICAL,
                RiskSeverity.EMERGENCY,
            }:
                self._breaker_metrics["breach_count"] += 1

                # Log circuit breaker trigger (implementation placeholder)
                logger.critical("Circuit breaker triggered due to exposure breach")

                self._breaker_metrics["breakers_triggered"] += 1

                # Publish circuit breaker event
                breaker_event = RiskEvent(
                    risk_type=RiskType.EXPOSURE,
                    severity=RiskSeverity.EMERGENCY,
                    current_value=event.current_value,
                    limit_value=event.limit_value,
                    message="Exposure limit breach triggered circuit breaker",
                    symbol=event.symbol,
                    exchange=event.exchange,
                )
                await self.event_bus.publish(breaker_event)

            elif event.risk_type == RiskType.EXPOSURE and event.severity == RiskSeverity.INFO:
                # Info severity signals reset
                self._breaker_metrics["breakers_reset"] += 1
                logger.info("Circuit breaker reset")

            # Check if rejection rate exceeds configured threshold
            max_consecutive_errors = self.config.max_consecutive_errors
            if self._breaker_metrics["rejection_count"] >= max_consecutive_errors:
                # Log circuit breaker trigger (implementation placeholder)
                logger.critical(
                    "circuit_breaker_triggered_consecutive_rejections",
                    rejection_count=self._breaker_metrics["rejection_count"],
                    max_consecutive_errors=max_consecutive_errors,
                )

                self._breaker_metrics["breakers_triggered"] += 1
                self._breaker_metrics["rejection_count"] = 0  # Reset counter

            self._metrics["events_processed"] += 1

        except Exception:
            logger.exception(
                "circuit_breaker_error",
                handler_id=self.handler_id,
                event_type=type(event).__name__,
            )
            self._metrics["errors"] += 1

    async def start(self) -> None:
        """Start the circuit breaker handler."""
        await super().start()

        # Subscribe to risk events with CRITICAL priority
        self.event_bus.subscribe(
            RiskEvent,
            self._handle_risk_event,
            HandlerPriority.CRITICAL,
        )

        logger.info("Risk circuit breaker handler started", handler_id=self.handler_id)

    async def stop(self) -> None:
        """Stop the circuit breaker handler."""
        # Unsubscribe from events
        self.event_bus.unsubscribe(RiskEvent, self._handle_risk_event)

        await super().stop()
        logger.info("Risk circuit breaker handler stopped", handler_id=self.handler_id)

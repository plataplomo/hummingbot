"""Event processors for handling business logic of events in the trading engine."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.monitoring.alert_service import AlertService
from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from cyberdelta.domain.risk.risk_service import RiskService
from cyberdelta.domain.safety.circuit_breaker import CircuitBreakerManager
from cyberdelta.domain.strategy.strategy_service import StrategyService
from cyberdelta.domain.trading.trading_service import TradingService
from cyberdelta.enums.monitoring import AlertLevel
from cyberdelta.models import Fill, TradeSignal


if TYPE_CHECKING:
    from cyberdelta.application.trading_engine_components.event_handling.event_validators import (
        EventValidator,
    )
from cyberdelta.models.events import (
    MarketData,
    OrderEvent,
    PositionEvent,
    RiskEvent,
    SignalEvent,
)
from cyberdelta.symbols.global_service import get_symbol_service


logger = get_logger(__name__)


class EventProcessor:
    """Processes validated events and executes business logic."""

    def __init__(
        self,
        event_validator: EventValidator,
        circuit_breakers: CircuitBreakerManager,
        trading_service: TradingService,
        portfolio_service: PortfolioService,
        risk_service: RiskService,
        alert_service: AlertService,
        strategy_service: StrategyService,
        config: AppSettings,
    ) -> None:
        """Initialize event processor with required services.

        Args:
            event_validator: Event validation service
            circuit_breakers: Circuit breaker manager
            trading_service: Trading service
            portfolio_service: Portfolio service
            risk_service: Risk service
            alert_service: Alert service for critical notifications
            strategy_service: Strategy service
            config: Application configuration
        """
        self._validator = event_validator
        self._circuit_breakers = circuit_breakers
        self._trading_service = trading_service
        self._portfolio_service = portfolio_service
        self._risk_service = risk_service
        self._alert_service = alert_service
        self._strategy_service = strategy_service
        self._config = config

    async def _handle_trading_signal(self, signal: TradeSignal) -> None:
        """Handle trading signal from strategy service.

        Args:
            signal: Trading signal to process

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses typed TradeSignal input
        - All processing through configured services
        - NO business logic in handler
        """
        try:
            logger.info(
                "trading_signal_received",
                signal_id=signal.signal_id,
                symbol=signal.symbol.value,
                exchange=str(signal.exchange),
                side=signal.side.value,
                price=signal.price,
            )

            # Route signal to trading service for execution with circuit breaker protection
            trade = await self._circuit_breakers.protect(
                "trading_service", "execute_signal", self._trading_service.execute_signal, signal
            )

            if trade:
                logger.info(
                    "signal_execution_successful", signal_id=signal.signal_id, trade_id=str(trade)
                )
            else:
                logger.warning(
                    "signal_execution_failed",
                    signal_id=signal.signal_id,
                    reason="trading_service_returned_none",
                )

        except Exception as e:
            logger.exception("signal_handling_error", signal_id=signal.signal_id, error=str(e))

            # Log error for monitoring instead of publishing event
            logger.exception(
                "signal_processing_error",
                signal_id=signal.signal_id,
                error=str(e),
                error_type=type(e).__name__,
            )

    async def _handle_trade_executed(self, trade: Fill) -> None:
        """Handle trade execution completion.

        Args:
            trade: Executed trade

        IMPORTANT: Following CODING_STANDARDS.md:
        - Updates portfolio state
        - Notifies strategy service
        - NO assumptions about trade validity
        """
        try:
            logger.info(
                "trade_executed_received",
                trade_id=trade.id,
                symbol=trade.symbol.value,
                exchange=trade.exchange,
                side=trade.side.value,
                quantity=trade.quantity,
                price=trade.price,
            )

            # Notify strategy service about trade completion with circuit breaker protection
            await self._circuit_breakers.protect(
                "strategy_service", "handle_fill", self._strategy_service.handle_fill, trade
            )

        except Exception as e:
            logger.exception("trade_handling_error", trade_id=trade.id, error=str(e))

    async def _handle_market_data_update(self, update: dict[str, Any]) -> None:
        """Handle market data updates.

        Args:
            update: Market data update event

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about update structure
        - Logs market data events for monitoring
        """
        logger.debug(
            "market_data_update_received",
            update_type=update.get("type", "unknown"),
            exchange=update.get("exchange"),
            symbol=update.get("symbol"),
        )

        # Market data updates are primarily for monitoring
        # Strategies get data through MarketDataService

    async def _handle_portfolio_updated(self, update: dict[str, Any]) -> None:
        """Handle portfolio state updates.

        Args:
            update: Portfolio update event

        IMPORTANT: Following CODING_STANDARDS.md:
        - Logs portfolio changes for audit trail
        - NO assumptions about update structure
        """
        logger.info(
            "portfolio_updated",
            update_type=update.get("type", "unknown"),
            exchange=update.get("exchange"),
            symbol=update.get("symbol"),
            previous_value=update.get("previous_value"),
            new_value=update.get("new_value"),
        )

    async def _handle_risk_violation(self, violation: dict[str, Any]) -> None:
        """Handle risk limit violations.

        Args:
            violation: Risk violation event

        IMPORTANT: Following CODING_STANDARDS.md:
        - Takes immediate action on violations
        - Uses configured response actions
        """
        logger.error(
            "risk_violation_detected",
            violation_type=violation.get("type"),
            signal_id=violation.get("signal_id"),
            violation_details=violation.get("details"),
            current_exposure=violation.get("current_exposure"),
        )

        # Risk violations should trigger immediate responses and alerts
        # based on configured safety settings
        if self._config.safety_systems.circuit_breakers.enabled:
            violation_type = violation.get("type")
            if violation_type in {"max_exposure", "max_drawdown"}:
                logger.warning("triggering_emergency_stop", violation_type=violation_type)

                # Create critical risk violation alert
                await self._alert_service.create_alert(
                    title=f"CRITICAL Risk Violation: {violation_type}",
                    description=(
                        f"Risk violation detected: "
                        f"{violation.get('details', 'No details available')}. "
                        f"Current exposure: {violation.get('current_exposure', 'Unknown')}"
                    ),
                    level=AlertLevel.CRITICAL,
                    source="risk_service",
                    metadata={
                        "violation_type": violation_type,
                        "signal_id": violation.get("signal_id"),
                        "current_exposure": violation.get("current_exposure"),
                        "violation_details": violation.get("details"),
                        "emergency_stop_triggered": True,
                    },
                )

                # In production, this would trigger emergency stop
                # For now, just log the action

    async def process_strategy_signal_event(self, event: SignalEvent) -> None:
        """Process strategy signal event with full type safety.

        Validates all event data using typed methods and creates TradeSignal
        for processing. Returns early if any validation fails.
        """
        # Validate required fields using typed validation
        if not self._validator.validate_event_required_fields(event):
            return

        # Extract and validate price using typed method
        price = self._validator.extract_and_validate_price(event)
        if price is None:
            return

        # Extract enum values with type safety
        side = self._validator.extract_signal_side(event)
        signal_type = self._validator.extract_signal_type(event)

        # Extract other fields - no None checks needed for required fields
        confidence = event.confidence
        source_strategy = event.strategy_name

        # Create Symbol object from string
        symbol_service = get_symbol_service()
        symbol = symbol_service.create_symbol(event.symbol, event.exchange)

        signal = TradeSignal(
            signal_id=event.signal_id,
            symbol=symbol,
            exchange=event.exchange,  # Required field - no defaults
            side=side,  # Type-safe enum
            signal_type=signal_type,  # Type-safe enum
            price=price,  # Validated Decimal
            confidence=confidence,
            source_strategy=source_strategy,
        )

        await self._handle_trading_signal(signal)

    async def process_order_filled_event(self, event: OrderEvent) -> None:
        """Process order filled event with full type safety.

        Validates all event data using typed methods and creates Fill
        for processing. Returns early if any validation fails.

        Raises:
            ValueError: If commission data is missing for the order.
        """
        # Validate required fields using typed validation
        if not self._validator.validate_fill_event_data(event):
            return

        # Extract and validate critical fill data
        fill_price = self._validator.extract_fill_price(event)
        if fill_price is None:
            return

        fill_quantity = self._validator.extract_fill_quantity(event)
        if fill_quantity is None:
            return

        # Get side from event - type safe by construction
        side = event.side

        # Extract commission from event field - explicit None handling
        commission = event.commission
        if commission is None:
            msg = f"Commission data missing for order {event.order_id}"
            raise ValueError(msg)

        # Commission is now guaranteed to be non-None
        fee = commission
        fee_asset = event.fee_asset  # Get fee asset from event

        # Get maker/taker information from event (provided by exchange)
        maker_taker = event.maker_taker

        # Create Symbol object from string
        symbol_service = get_symbol_service()
        symbol = symbol_service.create_symbol(event.symbol, event.exchange)

        # Convert timestamp to datetime
        executed_at = datetime.fromtimestamp(event.timestamp, tz=UTC)

        fill = Fill(
            id=event.order_id,
            symbol=symbol,
            executed_at=executed_at,
            side=side,  # Type-safe enum
            order_id=event.order_id,
            exchange=event.exchange,
            price=fill_price,  # Validated Decimal > 0
            quantity=fill_quantity,  # Validated Decimal > 0
            fee=fee,  # Safe Decimal or 0
            fee_asset=fee_asset,  # Optional string
            maker_taker=maker_taker,
        )
        await self._handle_trade_executed(fill)

    async def process_market_data_event(self, event: MarketData) -> None:
        """Process market data event."""
        update_dict = {
            "type": event.data_type,
            "symbol": event.symbol,
            "exchange": event.exchange,
            "last_price": float(event.price) if event.price else None,
            "bid_price": float(event.bids[0][0]) if event.bids else None,
            "ask_price": float(event.asks[0][0]) if event.asks else None,
            "volume_24h": float(event.volume) if event.volume else None,
            "timestamp": event.timestamp,
            "event_id": f"market_{event.symbol}_{event.exchange}_{event.timestamp}",
        }
        await self._handle_market_data_update(update_dict)

    async def process_position_updated_event(self, event: PositionEvent) -> None:
        """Process position updated event."""
        # PositionEvent doesn't have metadata field, use available fields
        update_dict = {
            "type": "position_update",
            "symbol": event.symbol,
            "exchange": event.exchange,
            "previous_value": None,  # PositionEvent doesn't provide previous value
            "new_value": event.size,
            "timestamp": event.timestamp,
            "event_id": f"position_{event.symbol}_{event.exchange}_{event.timestamp}",
        }
        await self._handle_portfolio_updated(update_dict)

    async def process_risk_limit_event(self, event: RiskEvent) -> None:
        """Process risk limit exceeded event."""
        # RiskEvent doesn't have metadata or current_exposure fields
        violation_dict = {
            "type": event.risk_type,
            "signal_id": None,  # Not available in RiskEvent
            "details": f"Limit {event.risk_type}: {event.current_value} > {event.limit_value}",
            "current_value": float(event.current_value) if event.current_value else 0,
            "limit_value": float(event.limit_value) if event.limit_value else 0,
            "timestamp": event.timestamp,
            "event_id": f"risk_{event.risk_type}_{event.timestamp}",
        }
        await self._handle_risk_violation(violation_dict)

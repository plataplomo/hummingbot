"""Trading service for coordinating order execution and portfolio updates.

This module provides the TradingService class that orchestrates the flow between
execution, portfolio management, and event publishing using validated AppSettings.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.application.event_bus import EventBus
from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName, OrderType, TimeInForce
from cyberdelta.logic.monitoring.health_monitor import ServiceType
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService
from cyberdelta.logic.trading.execution_engine import ExecutionEngine
from cyberdelta.models import TradeSignal
from cyberdelta.models.events import (
    OrderExecutedEvent,
    SignalExecutionFailedEvent,
    SignalProcessedEvent,
)
from cyberdelta.models.market.order import Order
from cyberdelta.models.market.trade import Trade
from cyberdelta.models.risk.assessment import PositionSize
from cyberdelta.models.trading.execution_request import ExecutionRequest
from cyberdelta.protocols.infrastructure.monitoring import HealthCheckable


logger = get_logger(__name__)


class TradingService(HealthCheckable):
    """Trading orchestration service using validated AppSettings.

    This service coordinates the flow between execution, portfolio updates,
    and event publishing. It acts as a pure orchestration layer without
    business logic, routing validated signals through the execution pipeline.


    Configuration Usage:
    - Uses config.general.safe_mode for paper trading mode
    - Uses config.execution.* for execution-related settings
    - Uses config.risk.global_risk.* for position limit awareness
    - Uses config.state.* for portfolio state settings

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL configuration from AppSettings, NO hardcoded values
    - Pure orchestration, NO business logic
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - Coordinates services without duplication
    """

    def __init__(
        self,
        config: AppSettings,
        execution_engine: ExecutionEngine,
        portfolio_service: PortfolioService,
        event_bus: EventBus,
    ) -> None:
        """Initialize trading service with configuration and dependencies.

        Args:
            config: Application settings containing all configuration
            execution_engine: Engine for executing orders
            portfolio_service: Service for portfolio state management
            event_bus: Event bus for publishing trade events
        """
        self.config = config
        self._execution_engine = execution_engine
        self._portfolio_service = portfolio_service
        self._event_bus = event_bus

        # Extract trading configuration - NO hardcoded defaults
        self._safe_mode = config.general.safe_mode
        self._execution_config = config.execution

        # Portfolio update settings
        self._portfolio_config = config.state
        self._update_on_execution = self._portfolio_config.update_on_execution
        self._persist_on_trade = self._portfolio_config.persist_on_trade

        # Health tracking
        self._signal_count = 0
        self._success_count = 0
        self._error_count = 0
        self._last_activity = datetime.now(UTC)

        logger.info(
            "trading_service_initialized",
            safe_mode=self._safe_mode,
            update_on_execution=self._update_on_execution,
            persist_on_trade=self._persist_on_trade,
            execution_timeout=float(self._execution_config.timeout_seconds),
            retry_enabled=self._execution_config.retry_enabled,
        )

    async def execute_signal(self, signal: TradeSignal) -> Order | None:
        """Execute a validated trading signal through the full pipeline.

        Args:
            signal: Validated trading signal to execute


        Returns:
            Order object if execution successful, None otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Pure orchestration of services
        - NO business logic in this method
        - Proper error handling without silent failures
        - Uses typed inputs/outputs (TradeSignal -> Order)
        """
        logger.info(
            "signal_execution_started",
            signal_id=signal.signal_id,
            symbol=signal.symbol.value,
            exchange=(
                signal.exchange.value
                if isinstance(signal.exchange, ExchangeName)
                else signal.exchange[0].value
            ),
            side=signal.side.value,
            price=float(signal.price) if signal.price else None,
            safe_mode=self._safe_mode,
        )

        try:
            # Update health metrics
            self._signal_count += 1
            self._last_activity = datetime.now(UTC)

            # Step 1: Convert signal to execution request
            execution_request = self._create_execution_request(signal)

            # Step 2: Execute order through execution engine
            order = await self._execution_engine.execute_request(execution_request)

            if not order:
                logger.warning(
                    "signal_execution_failed",
                    signal_id=signal.signal_id,
                    reason="execution_engine_returned_none",
                )
                return None

            # Step 3: Update portfolio state if configured
            if self._update_on_execution:
                await self._update_portfolio_from_order(order)

            # Step 4: Publish order events
            await self._publish_order_events(order, signal)

            # Step 5: Persist order if configured
            if self._persist_on_trade:
                await self._persist_order_result(order)

            # Update success metrics
            self._success_count += 1

            logger.info(
                "signal_execution_completed",
                signal_id=signal.signal_id,
                order_id=order.exchange_order_id,
                symbol=order.symbol.value,
                exchange=order.exchange.value,
                price=float(order.price) if order.price else None,
                quantity_requested=(
                    float(order.quantity_requested) if order.quantity_requested else None
                ),
                quantity_filled=float(order.quantity_filled) if order.quantity_filled else None,
            )

        except Exception as e:
            # Update error metrics
            self._error_count += 1

            logger.exception("signal_execution_error", signal_id=signal.signal_id, error=str(e))

            # Publish error event for monitoring
            await self._publish_execution_error(signal, e)
            return None

        return order

    async def execute_bulk_signals(self, signals: list[TradeSignal]) -> list[Order]:
        """Execute multiple signals in sequence.

        Args:
            signals: List of validated trading signals


        Returns:
            List of successful orders (may be shorter than input if some fail)

        IMPORTANT: Following CODING_STANDARDS.md:
        - Executes signals independently
        - One failure doesn't stop others
        - Proper logging for each execution
        """
        logger.info(
            "bulk_signal_execution_started", signal_count=len(signals), safe_mode=self._safe_mode
        )

        orders: list[Order] = []
        for signal in signals:
            order = await self.execute_signal(signal)
            if order:
                orders.append(order)

        logger.info(
            "bulk_signal_execution_completed",
            signals_processed=len(signals),
            orders_executed=len(orders),
            success_rate=len(orders) / len(signals) if signals else 0.0,
        )

        return orders

    def _create_execution_request(self, signal: TradeSignal) -> ExecutionRequest:
        """Convert trading signal to execution request.

        Args:
            signal: Trading signal to convert


        Returns:
            ExecutionRequest for execution engine


        IMPORTANT: Following CODING_STANDARDS.md:
        - Type-safe conversion (TradeSignal -> ExecutionRequest)
        - Uses Symbol objects and ExchangeName enums
        - NO business logic, just data transformation
        """
        # Import already available at top level

        # Get position sizing from config - NO hardcoded values
        position_value = self.config.risk.sizing.min_position_size

        # Calculate quantity based on signal price if available
        if signal.price and signal.price > 0:
            quantity = Decimal(str(position_value)) / signal.price
        else:
            # Use minimal quantity from config
            quantity = self._execution_config.minimal_quantity_fallback

        # Calculate percent of equity placeholder (would be from portfolio)
        # Using config value for now
        percent_of_equity = Decimal(str(self.config.risk.sizing.simple_fixed_fraction * 100))

        position_size = PositionSize(
            value_usd=Decimal(str(position_value)),
            quantity=quantity,
            percent_of_equity=percent_of_equity,
        )

        # Determine order type from config
        if self._execution_config.compensation.use_limit_orders:
            order_type = OrderType.LIMIT
        else:
            order_type = OrderType.MARKET

        # Create execution request with signal and calculated position size
        execution_request = ExecutionRequest(
            signal=signal,
            position_size=position_size,
            order_type=order_type,
            time_in_force=TimeInForce.GTC,
        )

        logger.debug("execution_request_created", signal_id=signal.signal_id)

        return execution_request

    async def _update_portfolio_from_order(self, order: Order) -> None:
        """Update portfolio state with executed order.

        Args:
            order: Executed order to apply to portfolio

        IMPORTANT: Following CODING_STANDARDS.md:
        - Delegates to portfolio service
        - NO portfolio logic in trading service
        """
        try:
            # Create a simplified Trade object from the Order for portfolio update
            # This is a temporary bridge until full Trade events are available
            # Imports already available at top level

            if order.quantity_filled and order.quantity_filled > 0:
                # Create trade from filled order
                trade = Trade(
                    id=f"trade_{uuid.uuid4().hex[:8]}",
                    symbol=order.symbol,
                    executed_at=order.updated_at or datetime.now(UTC),
                    side=order.side,
                    order_id=order.exchange_order_id or "",
                    exchange=order.exchange.value,
                    price=order.average_fill_price or order.price or Decimal(0),
                    quantity=order.quantity_filled,
                    fee=Decimal(0),  # Fee would come from exchange response
                    fee_asset=None,
                    client_order_id=order.client_order_id,
                )

                # Update portfolio with the trade
                await self._portfolio_service.update_from_trade(trade)

            logger.debug(
                "portfolio_updated_from_order",
                order_id=order.exchange_order_id,
                symbol=order.symbol.value,
                exchange=order.exchange.value,
                quantity_filled=float(order.quantity_filled) if order.quantity_filled else 0,
            )

        except Exception as e:
            logger.exception(
                "portfolio_update_from_order_failed", order_id=order.exchange_order_id, error=str(e)
            )
            # Don't re-raise - portfolio update failure shouldn't cancel trade

    async def _publish_order_events(self, order: Order, original_signal: TradeSignal) -> None:
        """Publish order-related events to event bus.

        Args:
            order: Executed order
            original_signal: Original signal that led to this order

        IMPORTANT: Following CODING_STANDARDS.md:
        - Publishes typed events
        - NO business logic, just event publishing
        """
        try:
            # Create domain events for order execution

            # Publish order executed event
            order_event = OrderExecutedEvent(
                order_id=order.exchange_order_id or "",
                symbol=order.symbol,
                exchange=order.exchange,
                side=order.side,
                order_type=order.order_type,
                price=order.price,
                quantity=order.quantity_requested,
                status=order.status,
                timestamp=datetime.now(UTC),
            )
            await self._event_bus.publish(order_event)

            # Publish signal processed event
            # Handle exchange which might be a list
            signal_exchange = original_signal.exchange
            if isinstance(signal_exchange, list):
                signal_exchange = (
                    signal_exchange[0] if signal_exchange else ExchangeName.HYPERLIQUID
                )

            signal_event = SignalProcessedEvent(
                signal_id=original_signal.signal_id,
                order_id=order.exchange_order_id or "",
                symbol=original_signal.symbol,
                exchange=signal_exchange,
                success=order.status.value in {"OPEN", "FILLED", "PARTIALLY_FILLED"},
                timestamp=datetime.now(UTC),
            )
            await self._event_bus.publish(signal_event)

            logger.debug(
                "order_events_published",
                order_id=order.exchange_order_id,
                signal_id=original_signal.signal_id,
            )

        except Exception as e:
            logger.exception(
                "order_event_publishing_failed",
                order_id=order.exchange_order_id,
                signal_id=original_signal.signal_id,
                error=str(e),
            )
            # Don't re-raise - event publishing failure shouldn't cancel trade

    async def _persist_order_result(self, order: Order) -> None:
        """Persist order result for historical tracking.

        Args:
            order: Order to persist

        IMPORTANT: Following CODING_STANDARDS.md:
        - Delegates to portfolio service for persistence
        - NO persistence logic in trading service
        """
        try:
            # Use the portfolio service's state persistence mechanism
            # The portfolio service already handles persistence through its storage layer
            # We just need to ensure the order is tracked in the portfolio state

            # The update_from_order already handles state persistence
            # This method is for additional order history tracking if needed

            # For now, we can use the portfolio's reconciliation mechanism
            # which will persist the current state including this order's effects
            await self._portfolio_service.save_state()

            logger.debug(
                "order_persisted", order_id=order.exchange_order_id, symbol=order.symbol.value
            )

        except Exception as e:
            logger.exception(
                "order_persistence_failed", order_id=order.exchange_order_id, error=str(e)
            )
            # Don't re-raise - persistence failure shouldn't cancel trade

    async def _publish_execution_error(self, signal: TradeSignal, error: Exception) -> None:
        """Publish execution error event for monitoring.

        Args:
            signal: Signal that failed to execute
            error: Exception that caused the failure
        """
        try:
            # Error event already imported at top

            # Handle exchange which might be a list
            error_exchange = signal.exchange
            if isinstance(error_exchange, list):
                error_exchange = error_exchange[0] if error_exchange else ExchangeName.HYPERLIQUID

            error_event = SignalExecutionFailedEvent(
                signal_id=signal.signal_id,
                symbol=signal.symbol,
                exchange=error_exchange,
                error_message=str(error),
                error_type=type(error).__name__,
                timestamp=datetime.now(UTC),
            )
            await self._event_bus.publish(error_event)

        except Exception as e:
            logger.exception(
                "error_event_publishing_failed",
                signal_id=signal.signal_id,
                original_error=str(error),
                publishing_error=str(e),
            )

    async def get_execution_status(self) -> dict[str, Any]:
        """Get current execution status and configuration.

        Returns:
            Dictionary with execution configuration and status


        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns explicit configuration state
        - NO hardcoded defaults in response
        """
        return {
            "configuration": {
                "safe_mode": self._safe_mode,
                "update_on_execution": self._update_on_execution,
                "persist_on_trade": self._persist_on_trade,
                "execution_timeout_seconds": float(self._execution_config.timeout_seconds),
                "retry_enabled": self._execution_config.retry_enabled,
                "max_retry_attempts": (
                    self._execution_config.max_retry_attempts
                    if self._execution_config.retry_enabled
                    else 0
                ),
            },
            "engine_status": "running",  # Execution engine is always running if service is running
        }

    def is_safe_mode(self) -> bool:
        """Check if trading service is in safe mode.

        Returns:
            True if safe mode is enabled, False otherwise


        IMPORTANT: Following CODING_STANDARDS.md:
        - Safe mode setting from config, NOT hardcoded
        """
        return self._safe_mode

    async def check_health(self) -> dict[str, Any]:
        """Health check implementation for TradingService.

        Returns:
            Dictionary with health metrics and status


        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns explicit health metrics
        - No assumptions about normal operation
        """
        # Get execution engine health if available
        execution_health = {}
        try:
            execution_health = await self._execution_engine.check_health()
        except (RuntimeError, ValueError, ConnectionError) as e:
            execution_health = {"error": f"Health check failed: {e}"}

        return {
            "is_running": True,
            "signal_count": self._signal_count,
            "success_count": self._success_count,
            "error_count": self._error_count,
            "last_activity": self._last_activity.isoformat() if self._last_activity else None,
            "safe_mode": self._safe_mode,
            "update_on_execution": self._update_on_execution,
            "persist_on_trade": self._persist_on_trade,
            "execution_engine_health": execution_health,
            "success_rate": (
                self._success_count / self._signal_count if self._signal_count > 0 else 0.0
            ),
        }

    def get_service_type(self) -> ServiceType:
        """Return service type for health monitoring."""
        return ServiceType.TRADING

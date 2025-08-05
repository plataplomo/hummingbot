"""Trading service for coordinating order execution and portfolio updates.

This module provides the TradingService class that orchestrates the flow between
execution, portfolio management, and event publishing using validated AppSettings.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Optional, Dict, Any

from cyberdelta.config.structlog_config import get_logger

from cyberdelta.application.event_bus import EventBus
from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.logic.monitoring.health_monitor import HealthCheckable, ServiceType
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService
from cyberdelta.logic.trading.execution_engine import ExecutionEngine
from cyberdelta.models import TradeSignal, Trade, ExecutionRequest

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
    - Uses config.portfolio.* for portfolio update settings

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
    ):
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
        self._portfolio_config = config.portfolio
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

    async def execute_signal(self, signal: TradeSignal) -> Optional[Trade]:
        """Execute a validated trading signal through the full pipeline.

        Args:
            signal: Validated trading signal to execute

        Returns:
            Trade object if execution successful, None otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Pure orchestration of services
        - NO business logic in this method
        - Proper error handling without silent failures
        - Uses typed inputs/outputs (TradeSignal -> Trade)
        """
        logger.info(
            "signal_execution_started",
            signal_id=signal.signal_id,
            symbol=signal.symbol.value,
            exchange=signal.exchange.value
            if hasattr(signal.exchange, "value")
            else str(signal.exchange),
            side=signal.side.value if hasattr(signal.side, "value") else str(signal.side),
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
            trade = await self._execution_engine.execute_order(execution_request)

            if not trade:
                logger.warning(
                    "signal_execution_failed",
                    signal_id=signal.signal_id,
                    reason="execution_engine_returned_none",
                )
                return None

            # Step 3: Update portfolio state if configured
            if self._update_on_execution:
                await self._update_portfolio_from_trade(trade)

            # Step 4: Publish trade events
            await self._publish_trade_events(trade, signal)

            # Step 5: Persist trade if configured
            if self._persist_on_trade:
                await self._persist_trade_result(trade)

            # Update success metrics
            self._success_count += 1

            logger.info(
                "signal_execution_completed",
                signal_id=signal.signal_id,
                trade_id=trade.id,
                symbol=trade.symbol.value,
                exchange=trade.exchange,
                executed_price=float(trade.executed_price) if trade.executed_price else None,
                executed_quantity=float(trade.executed_quantity)
                if trade.executed_quantity
                else None,
            )

            return trade

        except Exception as e:
            # Update error metrics
            self._error_count += 1

            logger.error(
                "signal_execution_error", signal_id=signal.signal_id, error=str(e), exc_info=True
            )

            # Publish error event for monitoring
            await self._publish_execution_error(signal, e)
            return None

    async def execute_bulk_signals(self, signals: list[TradeSignal]) -> list[Trade]:
        """Execute multiple signals in sequence.

        Args:
            signals: List of validated trading signals

        Returns:
            List of successful trades (may be shorter than input if some fail)

        IMPORTANT: Following CODING_STANDARDS.md:
        - Executes signals independently
        - One failure doesn't stop others
        - Proper logging for each execution
        """
        logger.info(
            "bulk_signal_execution_started", signal_count=len(signals), safe_mode=self._safe_mode
        )

        trades = []
        for signal in signals:
            trade = await self.execute_signal(signal)
            if trade:
                trades.append(trade)

        logger.info(
            "bulk_signal_execution_completed",
            signals_processed=len(signals),
            trades_executed=len(trades),
            success_rate=len(trades) / len(signals) if signals else 0.0,
        )

        return trades

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
        # Create execution request with same data as signal
        # The execution engine will handle quantity calculation, slippage, etc.
        execution_request = ExecutionRequest(
            signal_id=signal.signal_id,
            symbol=signal.symbol,
            exchange=signal.exchange,
            side=signal.side,
            price=signal.price,
            timestamp=signal.timestamp,
            metadata={
                "source_signal_id": signal.signal_id,
                "strategy": getattr(signal, "strategy", None),
                "confidence": str(signal.confidence) if hasattr(signal, "confidence") else None,
            },
        )

        logger.debug(
            "execution_request_created",
            signal_id=signal.signal_id,
            execution_request_id=execution_request.request_id,
        )

        return execution_request

    async def _update_portfolio_from_trade(self, trade: Trade) -> None:
        """Update portfolio state with executed trade.

        Args:
            trade: Executed trade to apply to portfolio

        IMPORTANT: Following CODING_STANDARDS.md:
        - Delegates to portfolio service
        - NO portfolio logic in trading service
        """
        try:
            await self._portfolio_service.apply_trade(trade)

            logger.debug(
                "portfolio_updated_from_trade",
                trade_id=trade.id,
                symbol=trade.symbol.value,
                exchange=trade.exchange,
            )

        except Exception as e:
            logger.error(
                "portfolio_update_from_trade_failed", trade_id=trade.id, error=str(e), exc_info=True
            )
            # Don't re-raise - portfolio update failure shouldn't cancel trade

    async def _publish_trade_events(self, trade: Trade, original_signal: TradeSignal) -> None:
        """Publish trade-related events to event bus.

        Args:
            trade: Executed trade
            original_signal: Original signal that led to this trade

        IMPORTANT: Following CODING_STANDARDS.md:
        - Publishes typed events
        - NO business logic, just event publishing
        """
        try:
            # Publish trade executed event
            await self._event_bus.publish("trade_executed", trade)

            # Publish signal completion event
            await self._event_bus.publish(
                "signal_completed",
                {
                    "signal_id": original_signal.signal_id,
                    "trade_id": trade.id,
                    "execution_status": "completed",
                },
            )

            logger.debug(
                "trade_events_published", trade_id=trade.id, signal_id=original_signal.signal_id
            )

        except Exception as e:
            logger.error(
                "trade_event_publishing_failed",
                trade_id=trade.id,
                signal_id=original_signal.signal_id,
                error=str(e),
                exc_info=True,
            )
            # Don't re-raise - event publishing failure shouldn't cancel trade

    async def _persist_trade_result(self, trade: Trade) -> None:
        """Persist trade result for historical tracking.

        Args:
            trade: Trade to persist

        IMPORTANT: Following CODING_STANDARDS.md:
        - Delegates to portfolio service for persistence
        - NO persistence logic in trading service
        """
        try:
            await self._portfolio_service.persist_trade(trade)

            logger.debug("trade_persisted", trade_id=trade.id, symbol=trade.symbol.value)

        except Exception as e:
            logger.error("trade_persistence_failed", trade_id=trade.id, error=str(e), exc_info=True)
            # Don't re-raise - persistence failure shouldn't cancel trade

    async def _publish_execution_error(self, signal: TradeSignal, error: Exception) -> None:
        """Publish execution error event for monitoring.

        Args:
            signal: Signal that failed to execute
            error: Exception that caused the failure
        """
        try:
            await self._event_bus.publish(
                "signal_execution_failed",
                {
                    "signal_id": signal.signal_id,
                    "symbol": signal.symbol.value,
                    "exchange": signal.exchange.value
                    if hasattr(signal.exchange, "value")
                    else str(signal.exchange),
                    "error": str(error),
                    "error_type": type(error).__name__,
                },
            )

        except Exception as e:
            logger.error(
                "error_event_publishing_failed",
                signal_id=signal.signal_id,
                original_error=str(error),
                publishing_error=str(e),
                exc_info=True,
            )

    async def get_execution_status(self) -> dict:
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
                "max_retry_attempts": self._execution_config.max_retry_attempts
                if self._execution_config.retry_enabled
                else 0,
            },
            "engine_status": await self._execution_engine.get_status()
            if hasattr(self._execution_engine, "get_status")
            else "unknown",
        }

    def is_safe_mode(self) -> bool:
        """Check if trading service is in safe mode.

        Returns:
            True if safe mode is enabled, False otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Safe mode setting from config, NOT hardcoded
        """
        return self._safe_mode

    async def check_health(self) -> Dict[str, Any]:
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
        except Exception as e:
            execution_health = {"error": str(e)}

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
            "success_rate": self._success_count / self._signal_count
            if self._signal_count > 0
            else 0.0,
        }

    def get_service_type(self) -> ServiceType:
        """Return service type for health monitoring."""
        return ServiceType.TRADING

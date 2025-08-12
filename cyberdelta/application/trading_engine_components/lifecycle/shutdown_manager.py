"""Shutdown manager for trading engine graceful termination."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.market.market_service import MarketDataService
from cyberdelta.domain.monitoring.alert_service import AlertService
from cyberdelta.domain.monitoring.metrics_collector import MetricsCollector
from cyberdelta.domain.monitoring.service_health_monitor import ServiceHealthMonitor
from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from cyberdelta.domain.safety.circuit_breaker import CircuitBreakerManager
from cyberdelta.domain.strategy.strategy_service import StrategyService
from cyberdelta.domain.trading.execution.execution_engine import ExecutionEngine


logger = get_logger(__name__)


class ShutdownManager:
    """Manages trading engine shutdown sequence and cleanup."""

    def __init__(
        self,
        config: AppSettings,
        circuit_breakers: CircuitBreakerManager,
        portfolio_service: PortfolioService,
        strategy_service: StrategyService,
        market_data_service: MarketDataService,
        execution_engine: ExecutionEngine,
        health_monitor: ServiceHealthMonitor,
        alert_service: AlertService,
        metrics_collector: MetricsCollector,
    ) -> None:
        """Initialize shutdown manager with required services.

        Args:
            config: Application settings
            circuit_breakers: Circuit breaker manager
            portfolio_service: Portfolio service
            strategy_service: Strategy service
            market_data_service: Market data service
            execution_engine: Execution engine for order management
            health_monitor: Health monitor
            alert_service: Alert service
            metrics_collector: Metrics collector
        """
        self.config = config
        self._circuit_breakers = circuit_breakers
        self._portfolio_service = portfolio_service
        self._strategy_service = strategy_service
        self._market_data_service = market_data_service
        self._execution_engine = execution_engine
        self._health_monitor = health_monitor
        self._alert_service = alert_service
        self._metrics_collector = metrics_collector

    async def stop(
        self,
        is_running: bool,
        tasks: list[Any],
        strategy_execution_enabled: bool,
        monitoring_enabled: bool,
        set_running_callback: Callable[[bool], None],
    ) -> None:
        """Stop the trading engine and all services.

        Args:
            is_running: Whether engine is currently running
            tasks: List of active background tasks
            strategy_execution_enabled: Whether strategy execution is enabled
            monitoring_enabled: Whether monitoring is enabled
            set_running_callback: Callback to set running state

        IMPORTANT: Following CODING_STANDARDS.md:
        - Graceful shutdown with proper task cancellation
        - Service cleanup in reverse order
        - NO assumptions about task state
        """
        if not is_running:
            logger.warning("trading_engine_not_running")
            return

        logger.info("trading_engine_stopping")

        set_running_callback(False)

        try:
            # Step 1: Cancel active orders if configured
            await self._cancel_orders_on_shutdown()

            # Step 2: Save final portfolio state
            await self._save_final_state()

            # Step 3: Cancel all background tasks
            logger.info("cancelling_background_tasks", task_count=len(tasks))
            for task in tasks:
                if not task.done():
                    task.cancel()

            # Wait for tasks to complete with timeout
            if tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True),
                        timeout=float(self.config.general.shutdown_grace_period),
                    )
                except TimeoutError:
                    logger.warning(
                        "shutdown_timeout",
                        grace_period=float(self.config.general.shutdown_grace_period),
                    )

            # Step 4: Stop services in reverse order
            await self._cleanup_services(strategy_execution_enabled, monitoring_enabled)

            logger.info("trading_engine_stopped")

        except Exception as e:
            logger.exception("trading_engine_stop_error", error=str(e))
            raise

    async def cleanup_services(
        self, strategy_execution_enabled: bool, monitoring_enabled: bool
    ) -> None:
        """Public method for cleaning up services during startup failure.

        Args:
            strategy_execution_enabled: Whether strategy execution is enabled
            monitoring_enabled: Whether monitoring is enabled
        """
        await self._cleanup_services(strategy_execution_enabled, monitoring_enabled)

    async def _cancel_orders_on_shutdown(self) -> None:
        """Cancel active orders on shutdown if configured.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Order cancellation based on config setting
        - NO assumptions about order state
        - Proper error handling per exchange
        """
        # Check if order cancellation is enabled in config
        cancel_on_shutdown = self.config.execution.cancel_on_shutdown

        if not cancel_on_shutdown:
            logger.info("order_cancellation_disabled_on_shutdown")
            return

        logger.info("cancelling_active_orders_on_shutdown")

        try:
            # Get all active orders from execution engine
            active_orders = self._execution_engine.get_active_orders()

            if not active_orders:
                logger.info("no_active_orders_to_cancel")
                return

            logger.info("cancelling_orders", order_count=len(active_orders))

            # Cancel each active order
            cancellation_tasks: list[asyncio.Task[bool]] = []
            for order_id, order in active_orders.items():
                logger.debug(
                    "submitting_order_cancellation",
                    order_id=order_id,
                    symbol=str(order.symbol),
                    exchange=order.exchange.value,
                )
                task = asyncio.create_task(self._execution_engine.cancel_order(order_id))
                cancellation_tasks.append(task)

            # Execute all cancellations concurrently with timeout from config
            cancellation_timeout = float(self.config.execution.timeout_seconds)
            results: list[bool | BaseException] = await asyncio.wait_for(
                asyncio.gather(*cancellation_tasks, return_exceptions=True),
                timeout=cancellation_timeout,
            )

            # Count successful cancellations
            successful_cancellations = sum(1 for result in results if result is True)
            failed_cancellations = len(results) - successful_cancellations

            logger.info(
                "active_orders_cancellation_completed",
                total_orders=len(active_orders),
                successful=successful_cancellations,
                failed=failed_cancellations,
            )

        except TimeoutError:
            logger.warning(
                "order_cancellation_timeout",
                timeout_sec=float(self.config.execution.timeout_seconds),
            )
            # Don't raise - timeout shouldn't prevent shutdown
        except Exception as e:
            logger.exception("order_cancellation_failed_on_shutdown", error=str(e))
            # Don't raise - order cancellation failure shouldn't prevent shutdown

    async def _save_final_state(self) -> None:
        """Save final portfolio state before shutdown.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Forced state save regardless of intervals
        - Creates final snapshot for audit trail
        - Handles save failures gracefully
        """
        logger.info("saving_final_state_on_shutdown")

        try:
            # Force save current portfolio state
            await self._circuit_breakers.protect(
                "portfolio_service", "save_state", self._portfolio_service.save_state
            )

            # Create final snapshot with special naming
            final_snapshot_name = f"shutdown_snapshot_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"

            await self._circuit_breakers.protect(
                "portfolio_service",
                "create_snapshot",
                self._portfolio_service.create_snapshot,
            )

            logger.info("final_state_saved_successfully", snapshot_name=final_snapshot_name)

        except Exception as e:
            logger.exception("final_state_save_failed", error=str(e))
            # Don't raise - state save failure shouldn't prevent shutdown

    async def _cleanup_services(
        self, strategy_execution_enabled: bool, monitoring_enabled: bool
    ) -> None:
        """Clean up all services in proper order.

        Args:
            strategy_execution_enabled: Whether strategy execution is enabled
            monitoring_enabled: Whether monitoring is enabled

        IMPORTANT: Following CODING_STANDARDS.md:
        - Services stopped in reverse dependency order
        - Continues cleanup even if individual services fail
        """
        logger.info("cleaning_up_services")

        # Stop strategy service first
        try:
            if strategy_execution_enabled:
                await self._circuit_breakers.protect(
                    "strategy_service", "stop", self._strategy_service.stop
                )
        except Exception as e:
            logger.exception("strategy_service_cleanup_error", error=str(e))

        # Stop market data service
        try:
            await self._circuit_breakers.protect(
                "market_data_service", "stop", self._market_data_service.stop
            )
        except Exception as e:
            logger.exception("market_data_service_cleanup_error", error=str(e))

        # Stop monitoring systems
        try:
            if monitoring_enabled:
                # Stop metrics collection
                await self._metrics_collector.stop()

                # Stop health monitoring
                await self._health_monitor.stop()

                # Stop alert service
                await self._alert_service.stop()
        except Exception as e:
            logger.exception("monitoring_systems_cleanup_error", error=str(e))

        # Portfolio service cleanup is handled by its own shutdown logic
        logger.info("service_cleanup_completed")

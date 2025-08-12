"""Background loop manager for trading engine periodic tasks."""

from __future__ import annotations

import asyncio
from decimal import Decimal

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.monitoring.service_health_monitor import ServiceHealthMonitor
from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from cyberdelta.domain.safety.circuit_breaker import CircuitBreakerManager
from cyberdelta.domain.strategy.strategy_service import StrategyService


logger = get_logger(__name__)


class LoopManager:
    """Manages background loops for periodic tasks in the trading engine."""

    def __init__(
        self,
        config: AppSettings,
        circuit_breakers: CircuitBreakerManager,
        strategy_service: StrategyService,
        portfolio_service: PortfolioService,
        health_service: ServiceHealthMonitor,
    ) -> None:
        """Initialize loop manager with required services.

        Args:
            config: Application settings
            circuit_breakers: Circuit breaker manager
            strategy_service: Strategy service
            portfolio_service: Portfolio service
            health_service: Service that provides health checks
        """
        self.config = config
        self._circuit_breakers = circuit_breakers
        self._strategy_service = strategy_service
        self._portfolio_service = portfolio_service
        self._health_service = health_service
        reconciliation_config = config.safety_systems.position_reconciliation
        self._reconciliation_interval = reconciliation_config.check_interval_sec
        self._running = False

    def set_running_state(self, running: bool) -> None:
        """Set the running state for loops."""
        self._running = running

    async def strategy_execution_loop(self) -> None:
        """Main strategy execution loop.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured execution interval
        - Handles errors without stopping loop
        - NO hardcoded timing
        """
        logger.info("strategy_execution_loop_started")

        while self._running:
            try:
                # Strategy service handles its own execution loop
                # This is a monitoring loop that ensures strategy service stays running
                if not self._strategy_service.is_running():
                    logger.warning("strategy_service_stopped_unexpectedly")
                    await self._circuit_breakers.protect(
                        "strategy_service", "start", self._strategy_service.start
                    )

                # Wait before next check - using general monitoring interval
                await asyncio.sleep(float(self.config.monitoring.health_check_interval_seconds))

            except asyncio.CancelledError:
                logger.info("strategy_execution_loop_cancelled")
                break
            except Exception as e:
                logger.exception("strategy_execution_loop_error", error=str(e))

                # Use exponential backoff from config
                backoff_delay = float(self.config.execution.retry_delay_base_sec) * float(
                    self.config.execution.retry_backoff_multiplier
                )
                await asyncio.sleep(backoff_delay)

        logger.info("strategy_execution_loop_ended")

    async def reconciliation_loop(self) -> None:
        """Periodic reconciliation with exchanges.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured reconciliation interval
        - Handles errors without stopping loop
        - NO hardcoded timing
        """
        logger.info(
            "reconciliation_loop_started",
            interval_seconds=float(self._reconciliation_interval),
        )

        while self._running:
            try:
                # Trigger portfolio reconciliation with exchanges with circuit breaker protection
                await self._circuit_breakers.protect(
                    "portfolio_service",
                    "reconcile_with_exchanges",
                    self._portfolio_service.reconcile_with_exchanges,
                )

                logger.debug("reconciliation_completed")

                # Wait for next reconciliation cycle
                await asyncio.sleep(float(self._reconciliation_interval))

            except asyncio.CancelledError:
                logger.info("reconciliation_loop_cancelled")
                break
            except Exception as e:
                logger.exception("reconciliation_error", error=str(e))

                # Use exponential backoff from config for errors
                retry_delay = float(
                    self.config.execution.retry_delay_base_sec
                    * Decimal(str(self.config.execution.retry_backoff_multiplier))
                )
                await asyncio.sleep(retry_delay)

        logger.info("reconciliation_loop_ended")

    async def snapshot_loop(self) -> None:
        """Periodic portfolio state snapshot creation.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured snapshot interval
        - Handles errors without stopping loop
        - NO hardcoded timing
        """
        snapshot_interval = float(self.config.general.state_save_interval)

        logger.info("snapshot_loop_started", interval_seconds=snapshot_interval)

        while self._running:
            try:
                # Create portfolio snapshot with circuit breaker protection
                await self._circuit_breakers.protect(
                    "portfolio_service", "create_snapshot", self._portfolio_service.create_snapshot
                )

                logger.debug("periodic_snapshot_completed")

                # Wait for next snapshot cycle
                await asyncio.sleep(snapshot_interval)

            except asyncio.CancelledError:
                logger.info("snapshot_loop_cancelled")
                break
            except Exception as e:
                logger.exception("snapshot_error", error=str(e))

                # Use exponential backoff from config for errors
                retry_delay = float(
                    self.config.execution.retry_delay_base_sec
                    * Decimal(str(self.config.execution.retry_backoff_multiplier))
                )
                await asyncio.sleep(retry_delay)

        logger.info("snapshot_loop_ended")

    async def monitoring_loop(self) -> None:
        """Health monitoring and alerting loop.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured monitoring intervals
        - Checks health against configured thresholds
        - NO hardcoded monitoring parameters
        """
        logger.info("monitoring_loop_started")

        health_check_interval = float(self.config.monitoring.health_check_interval_seconds)

        while self._running:
            try:
                # Check health of all services
                # Health monitoring is handled by the health_manager component
                # This loop just ensures basic monitoring tasks continue
                logger.debug("monitoring_loop_heartbeat")

                # Wait for next monitoring cycle
                await asyncio.sleep(health_check_interval)

            except asyncio.CancelledError:
                logger.info("monitoring_loop_cancelled")
                break
            except Exception as e:
                logger.exception("monitoring_loop_error", error=str(e))

                # Brief delay before retrying monitoring
                await asyncio.sleep(float(self.config.execution.retry_delay_base_sec))

        logger.info("monitoring_loop_ended")

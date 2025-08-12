"""Status reporter for trading engine status and configuration reporting."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.domain.monitoring.alert_service import AlertService
from cyberdelta.domain.monitoring.metrics_collector import MetricsCollector
from cyberdelta.domain.monitoring.service_health_monitor import ServiceHealthMonitor
from cyberdelta.domain.safety.circuit_breaker import CircuitBreakerManager


if TYPE_CHECKING:
    from cyberdelta.application.trading_engine import TradingEngine
    from cyberdelta.application.trading_engine_components.snapshot.snapshot_manager import (
        SnapshotManager,
    )


class StatusReporter:
    """Reports trading engine status and configuration."""

    def __init__(
        self,
        config: AppSettings,
        circuit_breakers: CircuitBreakerManager,
        health_monitor: ServiceHealthMonitor,
        alert_service: AlertService,
        metrics_collector: MetricsCollector,
        snapshot_manager: SnapshotManager,
    ) -> None:
        """Initialize status reporter with required services.

        Args:
            config: Application settings
            circuit_breakers: Circuit breaker manager
            health_monitor: Health monitoring service
            alert_service: Alert service
            metrics_collector: Metrics collector
            snapshot_manager: Snapshot manager
        """
        self.config = config
        self._circuit_breakers = circuit_breakers
        self._health_monitor = health_monitor
        self._alert_service = alert_service
        self._metrics_collector = metrics_collector
        self._snapshot_manager = snapshot_manager

    async def get_status(self, trading_engine: TradingEngine) -> dict[str, Any]:
        """Get current trading engine status.

        Args:
            trading_engine: The trading engine instance to query status from

        Returns:
            Dictionary with engine status and configuration

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns explicit configuration state from actual sources
        - NO hardcoded defaults in response
        """
        # Query actual state from trading engine and services
        return {
            "running": trading_engine.is_running,
            "safe_mode": trading_engine.safe_mode,
            "configuration": {
                "circuit_breakers_enabled": trading_engine.circuit_breakers_enabled,
                "monitoring_enabled": trading_engine.monitoring_enabled,
                "reconciliation_interval_sec": trading_engine.reconciliation_interval,
                "strategy_execution_enabled": trading_engine.strategy_execution_enabled,
                "enabled_strategies": self.config.strategies.enabled_strategies,
            },
            "active_tasks": trading_engine.active_tasks_count,
            "services": {
                # Query actual service states dynamically from services
                "portfolio_initialized": await self._check_portfolio_initialized(trading_engine),
                "market_data_connected": await self._check_market_data_connected(trading_engine),
                "strategy_service_running": trading_engine.strategy_service.is_running(),
            },
            "circuit_breakers": self._circuit_breakers.get_system_health(),
            "circuit_breaker_stats": self._circuit_breakers.get_all_stats(),
            "health_monitoring": {
                "enabled": trading_engine.monitoring_enabled,
                "running": self._health_monitor.is_running(),
                "registered_services": self._health_monitor.get_registered_services(),
                "last_checks": {
                    name: {
                        "status": check.health_status.value,
                        "timestamp": check.last_check_timestamp.isoformat(),
                        "response_time_ms": check.response_time_ms,
                    }
                    for name, check in self._health_monitor.get_all_last_checks().items()
                },
            },
            "alert_service": self._alert_service.get_alert_stats(),
            "metrics_collection": self._metrics_collector.get_metrics_summary(),
            "snapshot_system": self._snapshot_manager.get_snapshot_status(),
            "shutdown_configuration": self.get_shutdown_configuration(),
        }

    async def _check_portfolio_initialized(self, trading_engine: TradingEngine) -> bool:
        """Check if portfolio service is initialized.

        Args:
            trading_engine: Trading engine instance

        Returns:
            True if portfolio is initialized
        """
        # Check if portfolio can provide its state
        try:
            await trading_engine.portfolio_service.get_state()
        except (AttributeError, RuntimeError):
            # Service not ready or method not available
            return False
        else:
            return True

    async def _check_market_data_connected(self, trading_engine: TradingEngine) -> bool:
        """Check if market data is connected.

        Args:
            trading_engine: Trading engine instance

        Returns:
            True if connected, False if not connected
        """
        return trading_engine.market_data_service.is_connected()

    def get_shutdown_configuration(self) -> dict[str, Any]:
        """Get shutdown configuration and settings.

        Returns:
            Dictionary with shutdown-related configuration

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns ALL shutdown settings from config
        - NO hardcoded defaults
        """
        return {
            "grace_period_seconds": float(self.config.general.shutdown_grace_period),
            "cancel_orders_on_shutdown": self.config.execution.cancel_on_shutdown,
            "cleanup_order": self.config.general.shutdown_cleanup_order,
        }

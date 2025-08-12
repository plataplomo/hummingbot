"""Metrics manager for trading engine metrics collection."""

from __future__ import annotations

from typing import Any

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.domain.monitoring.metrics_collector import MetricsCollector


class MetricsManager:
    """Manages metrics collection for the trading engine."""

    def __init__(
        self,
        config: AppSettings,
        metrics_collector: MetricsCollector,
        is_running: bool = False,
        safe_mode: bool = False,
        active_tasks_count: int = 0,
        circuit_breakers_enabled: bool = False,
        monitoring_enabled: bool = False,
        strategy_execution_enabled: bool = False,
        reconciliation_interval: float = 0.0,
    ) -> None:
        """Initialize metrics manager with state and services.

        Args:
            config: Application settings
            metrics_collector: Metrics collector service
            is_running: Engine running state
            safe_mode: Safe mode state
            active_tasks_count: Active tasks count
            circuit_breakers_enabled: Circuit breakers enabled state
            monitoring_enabled: Monitoring enabled state
            strategy_execution_enabled: Strategy execution enabled state
            reconciliation_interval: Reconciliation interval
        """
        self.config = config
        self._metrics_collector = metrics_collector
        self._is_running = is_running
        self._safe_mode = safe_mode
        self._active_tasks_count = active_tasks_count
        self._circuit_breakers_enabled = circuit_breakers_enabled
        self._monitoring_enabled = monitoring_enabled
        self._strategy_execution_enabled = strategy_execution_enabled
        self._reconciliation_interval = reconciliation_interval

    async def get_metrics(self) -> dict[str, Any]:
        """Get trading engine metrics for collection.

        Returns:
            Dictionary with trading engine metrics

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns explicit metrics
        - All values from current state, no assumptions
        """
        return {
            "running": 1 if self._is_running else 0,
            "safe_mode": 1 if self._safe_mode else 0,
            "active_tasks": self._active_tasks_count,
            "circuit_breakers_enabled": 1 if self._circuit_breakers_enabled else 0,
            "monitoring_enabled": 1 if self._monitoring_enabled else 0,
            "strategy_execution_enabled": 1 if self._strategy_execution_enabled else 0,
            "reconciliation_interval_sec": float(self._reconciliation_interval),
            "enabled_strategies_count": len(self.config.strategies.enabled_strategies),
        }

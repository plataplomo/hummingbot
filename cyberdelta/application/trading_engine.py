"""Main trading engine orchestrator with modular component architecture.

This module provides the TradingEngine class that coordinates all services
and manages the complete trading flow using validated AppSettings configuration.
The engine has been decomposed into specialized components following DDD principles.
"""

from __future__ import annotations

import asyncio
from typing import Any

from cyberdelta.application.trading_engine_components import (
    AlertManager,
    BreakerManager,
    EventProcessor,
    EventRouter,
    EventValidator,
    HealthManager,
    LoopManager,
    MetricsManager,
    ShutdownManager,
    SnapshotManager,
    StartupManager,
    StatusReporter,
)
from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.market.market_service import MarketDataService
from cyberdelta.domain.monitoring.alert_service import AlertService
from cyberdelta.domain.monitoring.metrics_collector import MetricsCollector
from cyberdelta.domain.monitoring.service_health_monitor import ServiceHealthMonitor
from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from cyberdelta.domain.risk.risk_service import RiskService
from cyberdelta.domain.safety.circuit_breaker import CircuitBreakerManager
from cyberdelta.domain.signal.signal_service import SignalService
from cyberdelta.domain.strategy.strategy_service import StrategyService
from cyberdelta.domain.trading.execution import ExecutionEngine
from cyberdelta.domain.trading.trading_service import TradingService
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.models.monitoring.system_health_models import CircuitBreakerSystemHealth


logger = get_logger(__name__)


class TradingEngine:
    """Main trading engine orchestrator with comprehensive config integration.

    This engine coordinates all services and manages the complete trading flow
    from strategy execution through signal validation, risk assessment, and
    order execution.

    Configuration Usage:
    - Uses config.general.safe_mode to determine operational mode
    - Uses config.safety_systems for circuit breakers and monitoring
    - Uses config.strategies to determine which strategies to run
    - Uses config.monitoring for alerts and notifications
    - Uses config.safety_systems.position_reconciliation for reconciliation intervals

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
        event_bus: EventBus,
        market_data_service: MarketDataService,
        trading_service: TradingService,
        portfolio_service: PortfolioService,
        risk_service: RiskService,
        signal_service: SignalService,
        strategy_service: StrategyService,
        execution_engine: ExecutionEngine,
        circuit_breaker_manager: CircuitBreakerManager | None = None,
        health_monitor: ServiceHealthMonitor | None = None,
        alert_service: AlertService | None = None,
        metrics_collector: MetricsCollector | None = None,
    ) -> None:
        """Initialize trading engine with configuration and all services.

        Args:
            config: Application settings containing all configuration
            event_bus: Event bus for service coordination
            market_data_service: Market data aggregation service
            trading_service: Trading coordination service
            portfolio_service: Portfolio state management service
            risk_service: Risk assessment service
            signal_service: Signal validation service
            strategy_service: Strategy execution service
            execution_engine: Order execution engine
            circuit_breaker_manager: Circuit breaker manager for safety systems
            health_monitor: Health monitoring system for all services
            alert_service: Alert service for notifications and escalation
            metrics_collector: Metrics collection service for system monitoring
        """
        self.config = config
        self._event_bus = event_bus
        self._market_data = market_data_service
        self._trading = trading_service
        self._portfolio = portfolio_service
        self._risk = risk_service
        self._signal = signal_service
        self._strategy = strategy_service
        self._execution = execution_engine
        self._circuit_breakers = circuit_breaker_manager or CircuitBreakerManager(config)
        self._health_monitor = health_monitor or ServiceHealthMonitor(config)
        self._alert_service = alert_service or AlertService(config)
        self._metrics_collector = metrics_collector or MetricsCollector(config)
        self._running = False
        self._tasks: list[asyncio.Task[None]] = []

        # Extract operational settings from config - NO hardcoded defaults
        self._safe_mode = config.general.safe_mode
        self._circuit_breakers_enabled = config.safety_systems.circuit_breakers.enabled
        self._reconciliation_interval = (
            config.safety_systems.position_reconciliation.check_interval_sec
        )
        self._monitoring_enabled = config.monitoring.notifications_enabled

        # Strategy execution settings
        self._strategy_execution_enabled = len(config.strategies.enabled_strategies) > 0

        # Initialize component managers with proper dependency injection
        self._startup_manager = StartupManager(
            config,
            event_bus,
            self._circuit_breakers,
            self._health_monitor,
            self._alert_service,
            self._metrics_collector,
            self._portfolio,
            self._market_data,
            self._strategy,
            self._trading,
            self._execution,
        )
        self._shutdown_manager = ShutdownManager(
            config,
            self._circuit_breakers,
            self._portfolio,
            self._strategy,
            self._market_data,
            self._execution,
            self._health_monitor,
            self._alert_service,
            self._metrics_collector,
        )
        self._event_validator = EventValidator()
        self._event_processor = EventProcessor(
            self._event_validator,
            self._circuit_breakers,
            self._trading,
            self._portfolio,
            self._risk,
            self._alert_service,
            self._strategy,
            config,
        )
        self._event_router = EventRouter(self._event_processor)
        self._health_manager = HealthManager(config, self._health_monitor, self._alert_service)
        self._loop_manager = LoopManager(
            config,
            self._circuit_breakers,
            self._strategy,
            self._portfolio,
            self._health_monitor,
        )
        self._alert_manager = AlertManager(self._alert_service)
        self._metrics_manager = MetricsManager(
            config,
            self._metrics_collector,
            is_running=self._running,
            safe_mode=self._safe_mode,
            active_tasks_count=len(self._tasks),
            circuit_breakers_enabled=self._circuit_breakers_enabled,
            monitoring_enabled=self._monitoring_enabled,
            strategy_execution_enabled=self._strategy_execution_enabled,
            reconciliation_interval=float(self._reconciliation_interval),
        )
        self._snapshot_manager = SnapshotManager(config, self._circuit_breakers, self._portfolio)
        self._breaker_manager = BreakerManager(self._circuit_breakers)
        self._status_reporter = StatusReporter(
            config,
            self._circuit_breakers,
            self._health_monitor,
            self._alert_service,
            self._metrics_collector,
            self._snapshot_manager,
        )

        logger.info(
            "trading_engine_initialized",
            safe_mode=self._safe_mode,
            circuit_breakers_enabled=self._circuit_breakers_enabled,
            monitoring_enabled=self._monitoring_enabled,
            reconciliation_interval_sec=float(self._reconciliation_interval),
            strategy_execution_enabled=self._strategy_execution_enabled,
            enabled_strategies=config.strategies.enabled_strategies,
        )

    # Public API for components to access engine state
    @property
    def is_running(self) -> bool:
        """Check if the trading engine is running."""
        return self._running

    @property
    def safe_mode(self) -> bool:
        """Check if safe mode is enabled."""
        return self._safe_mode

    @property
    def strategy_execution_enabled(self) -> bool:
        """Check if strategy execution is enabled."""
        return self._strategy_execution_enabled

    @property
    def monitoring_enabled(self) -> bool:
        """Check if monitoring is enabled."""
        return self._monitoring_enabled

    @property
    def circuit_breakers_enabled(self) -> bool:
        """Check if circuit breakers are enabled."""
        return self._circuit_breakers_enabled

    @property
    def reconciliation_interval(self) -> float:
        """Get reconciliation interval in seconds."""
        return float(self._reconciliation_interval)

    @property
    def active_tasks_count(self) -> int:
        """Get count of active background tasks."""
        return len(self._tasks)

    # Public API for component access to services
    @property
    def circuit_breakers(self) -> CircuitBreakerManager:
        """Get circuit breaker manager."""
        return self._circuit_breakers

    @property
    def strategy_service(self) -> StrategyService:
        """Get strategy service."""
        return self._strategy

    @property
    def market_data_service(self) -> MarketDataService:
        """Get market data service."""
        return self._market_data

    @property
    def portfolio_service(self) -> PortfolioService:
        """Get portfolio service."""
        return self._portfolio

    @property
    def trading_service(self) -> TradingService:
        """Get trading service."""
        return self._trading

    @property
    def risk_service(self) -> RiskService:
        """Get risk service."""
        return self._risk

    @property
    def health_monitor(self) -> ServiceHealthMonitor:
        """Get health monitor."""
        return self._health_monitor

    @property
    def alert_service(self) -> AlertService:
        """Get alert service."""
        return self._alert_service

    @property
    def metrics_collector(self) -> MetricsCollector:
        """Get metrics collector."""
        return self._metrics_collector

    @property
    def snapshot_manager(self) -> SnapshotManager:
        """Get snapshot manager."""
        return self._snapshot_manager

    def _set_running_state(self, running: bool) -> None:
        """Set the running state of the trading engine."""
        self._running = running

    def _add_task(self, task: asyncio.Task[Any]) -> None:
        """Add a task to the task list."""
        self._tasks.append(task)

    async def _cleanup_on_startup_failure(self) -> None:
        """Cleanup services on startup failure."""
        await self._shutdown_manager.cleanup_services(
            self._strategy_execution_enabled, self._monitoring_enabled
        )

    async def start(self) -> None:
        """Start the trading engine and all services."""
        await self._startup_manager.start(
            self._running,
            self._strategy_execution_enabled,
            self._monitoring_enabled,
            self._safe_mode,
            float(self._reconciliation_interval),
            self._tasks,
            self._loop_manager,
            self._event_router,
            self._set_running_state,
            self._add_task,
            self._cleanup_on_startup_failure,
            self,
        )

    async def stop(self) -> None:
        """Stop the trading engine and all services."""
        await self._shutdown_manager.stop(
            self._running,
            self._tasks,
            self._strategy_execution_enabled,
            self._monitoring_enabled,
            self._set_running_state,
        )

    def is_safe_mode(self) -> bool:
        """Check if trading engine is in safe mode.

        Returns:
            True if safe mode is enabled, False otherwise.
        """
        return self._safe_mode

    async def get_status(self) -> dict[str, Any]:
        """Get current trading engine status.

        Returns:
            Dictionary containing engine status information.
        """
        return await self._status_reporter.get_status(self)

    def reset_circuit_breaker(self, service_name: str) -> bool:
        """Reset a specific circuit breaker.

        Args:
            service_name: Name of the service whose circuit breaker to reset.

        Returns:
            True if circuit breaker was reset, False otherwise.
        """
        return self._breaker_manager.reset_circuit_breaker(service_name)

    def reset_all_circuit_breakers(self) -> None:
        """Reset all circuit breakers."""
        self._breaker_manager.reset_all_circuit_breakers()

    def get_circuit_breaker_health(self) -> CircuitBreakerSystemHealth:
        """Get circuit breaker system health.

        Returns:
            CircuitBreakerSystemHealth object with system health data.
        """
        return self._breaker_manager.get_circuit_breaker_health()

    async def get_system_health_report(self) -> dict[str, Any]:
        """Get comprehensive system health report from health monitor.

        Returns:
            Dictionary containing system health report data.
        """
        return await self._health_manager.get_system_health_report()

    def get_health_monitor_status(self) -> dict[str, Any]:
        """Get health monitor status and configuration.

        Returns:
            Dictionary containing health monitor status.
        """
        return self._health_manager.get_health_monitor_status()

    async def create_alert(
        self,
        title: str,
        description: str,
        level: str,
        source: str = "trading_engine",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Create an alert through the alert service.

        Args:
            title: Alert title.
            description: Alert description.
            level: Alert severity level.
            source: Source of the alert.
            metadata: Optional metadata dictionary.

        Returns:
            Alert information dictionary or None if creation failed.
        """
        return await self._alert_manager.create_alert(title, description, level, source, metadata)

    async def acknowledge_alert(
        self, alert_id: str, acknowledged_by: str = "trading_engine"
    ) -> bool:
        """Acknowledge an active alert.

        Args:
            alert_id: ID of the alert to acknowledge.
            acknowledged_by: Entity acknowledging the alert.

        Returns:
            True if alert was acknowledged, False otherwise.
        """
        return await self._alert_manager.acknowledge_alert(alert_id, acknowledged_by)

    async def resolve_alert(self, alert_id: str, resolved_by: str = "trading_engine") -> bool:
        """Resolve an active alert.

        Args:
            alert_id: ID of the alert to resolve.
            resolved_by: Entity resolving the alert.

        Returns:
            True if alert was resolved, False otherwise.
        """
        return await self._alert_manager.resolve_alert(alert_id, resolved_by)

    def get_active_alerts(self) -> list[dict[str, Any]]:
        """Get list of currently active alerts.

        Returns:
            List of dictionaries containing active alert information.
        """
        return self._alert_manager.get_active_alerts()

    def get_alert_service_status(self) -> dict[str, Any]:
        """Get alert service status and statistics.

        Returns:
            Dictionary containing alert service status.
        """
        return self._alert_manager.get_alert_service_status()

    async def create_manual_snapshot(self, snapshot_name: str | None = None) -> str:
        """Create a manual portfolio snapshot.

        Args:
            snapshot_name: Optional name for the snapshot.

        Returns:
            Name of the created snapshot.
        """
        return await self._snapshot_manager.create_manual_snapshot(snapshot_name)

    async def list_snapshots(self) -> list[str]:
        """List all available portfolio snapshots.

        Returns:
            List of snapshot names.
        """
        return await self._snapshot_manager.list_snapshots()

    async def delete_snapshot(self, snapshot_name: str) -> bool:
        """Delete a specific snapshot.

        Args:
            snapshot_name: Name of the snapshot to delete.

        Returns:
            True if snapshot was deleted, False otherwise.
        """
        return await self._snapshot_manager.delete_snapshot(snapshot_name)

    def get_snapshot_status(self) -> dict[str, Any]:
        """Get snapshot system status.

        Returns:
            Dictionary containing snapshot system status.
        """
        return self._snapshot_manager.get_snapshot_status()

    def get_shutdown_configuration(self) -> dict[str, Any]:
        """Get shutdown configuration and settings.

        Returns:
            Dictionary containing shutdown configuration.
        """
        return self._status_reporter.get_shutdown_configuration()

    async def get_metrics(self) -> dict[str, Any]:
        """Get trading engine metrics for collection.

        Returns:
            Dictionary containing trading engine metrics.
        """
        return await self._metrics_manager.get_metrics()

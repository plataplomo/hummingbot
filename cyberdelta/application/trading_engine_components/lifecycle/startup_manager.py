"""Startup manager for trading engine initialization."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.market.market_service import MarketDataService
from cyberdelta.domain.monitoring.alert_service import AlertService
from cyberdelta.domain.monitoring.metrics_collector import MetricsCollector
from cyberdelta.domain.monitoring.service_health_monitor import ServiceHealthMonitor
from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from cyberdelta.domain.safety.circuit_breaker import CircuitBreakerManager
from cyberdelta.domain.strategy.strategy_service import StrategyService
from cyberdelta.domain.trading.execution import ExecutionEngine
from cyberdelta.domain.trading.trading_service import TradingService
from cyberdelta.infrastructure.event_bus import EventBus


if TYPE_CHECKING:
    from cyberdelta.application.trading_engine_components.background_loops.loop_manager import (
        LoopManager,
    )
    from cyberdelta.application.trading_engine_components.event_handling.event_router import (
        EventRouter,
    )
    from cyberdelta.protocols.infrastructure.monitoring import MetricsProvider
from cyberdelta.models.events import (
    MarketData,
    OrderEvent,
    PositionEvent,
    RiskEvent,
    SignalEvent,
)


logger = get_logger(__name__)


class StartupManager:
    """Manages trading engine startup sequence and initialization."""

    def __init__(
        self,
        config: AppSettings,
        event_bus: EventBus,
        circuit_breakers: CircuitBreakerManager,
        health_monitor: ServiceHealthMonitor,
        alert_service: AlertService,
        metrics_collector: MetricsCollector,
        portfolio_service: PortfolioService,
        market_data_service: MarketDataService,
        strategy_service: StrategyService,
        trading_service: TradingService,
        execution_service: ExecutionEngine,
    ) -> None:
        """Initialize startup manager with required services.

        Args:
            config: Application settings
            event_bus: Event bus for service coordination
            circuit_breakers: Circuit breaker manager
            health_monitor: Health monitoring system
            alert_service: Alert service
            metrics_collector: Metrics collection service
            portfolio_service: Portfolio service
            market_data_service: Market data service
            strategy_service: Strategy service
            trading_service: Trading service
            execution_service: Execution service
        """
        self.config = config
        self._event_bus = event_bus
        self._circuit_breakers = circuit_breakers
        self._health_monitor = health_monitor
        self._alert_service = alert_service
        self._metrics_collector = metrics_collector
        self._portfolio_service = portfolio_service
        self._market_data_service = market_data_service
        self._strategy_service = strategy_service
        self._trading_service = trading_service
        self._execution_service = execution_service

    async def start(
        self,
        is_running: bool,
        strategy_execution_enabled: bool,
        monitoring_enabled: bool,
        safe_mode: bool,
        reconciliation_interval: float,
        tasks: list[Any],
        loop_manager: LoopManager,
        event_router: EventRouter,
        set_running_callback: Callable[[bool], None],
        add_task_callback: Callable[[Any], None],
        cleanup_callback: Callable[[], Any],
        engine_as_metrics_provider: MetricsProvider,
    ) -> None:
        """Start the trading engine and all services.

        Args:
            is_running: Current running state
            strategy_execution_enabled: Whether strategy execution is enabled
            monitoring_enabled: Whether monitoring is enabled
            safe_mode: Whether safe mode is enabled
            reconciliation_interval: Reconciliation interval
            tasks: List to add tasks to
            loop_manager: Loop manager instance
            event_router: Event router instance
            set_running_callback: Callback to set running state
            add_task_callback: Callback to add tasks
            cleanup_callback: Cleanup callback for failures
            engine_as_metrics_provider: Engine instance for metrics

        IMPORTANT: Following CODING_STANDARDS.md:
        - Initialization sequence follows dependencies
        - All timeouts and intervals from config
        - Fail fast if any service fails to start
        """
        if is_running:
            logger.warning("trading_engine_already_running")
            return

        logger.info("trading_engine_starting")

        try:
            # Step 1: Initialize storage and portfolio service
            logger.info("initializing_portfolio_service")
            await self._circuit_breakers.protect(
                "portfolio_service", "initialize", self._portfolio_service.initialize
            )

            # Step 2: Start market data service
            logger.info("starting_market_data_service")
            await self._circuit_breakers.protect(
                "market_data_service", "start", self._market_data_service.start
            )

            # Step 3: Set up event subscriptions
            logger.info("setting_up_event_handlers")
            await self._setup_event_handlers(event_router)

            # Step 4: Start strategy service if enabled
            if strategy_execution_enabled:
                logger.info("starting_strategy_service")
                await self._circuit_breakers.protect(
                    "strategy_service", "start", self._strategy_service.start
                )

                # Set loop manager running state and start strategy execution loop
                loop_manager.set_running_state(True)
                add_task_callback(asyncio.create_task(loop_manager.strategy_execution_loop()))
            else:
                logger.warning("strategy_execution_disabled", reason="no_enabled_strategies")

            # Step 5: Start reconciliation loop if enabled
            if reconciliation_interval > 0:
                logger.info("starting_reconciliation_loop")
                add_task_callback(asyncio.create_task(loop_manager.reconciliation_loop()))

            # Step 5.5: Start snapshot loop if enabled
            snapshot_interval = float(self.config.general.state_save_interval)
            if snapshot_interval > 0:
                logger.info("starting_snapshot_loop")
                add_task_callback(asyncio.create_task(loop_manager.snapshot_loop()))

            # Step 6: Start monitoring systems if enabled
            if monitoring_enabled:
                logger.info("starting_monitoring_systems")

                # Start alert service with circuit breaker protection
                await self._circuit_breakers.protect(
                    "alert_service", "start", self._alert_service.start
                )

                # Setup and start health monitoring with circuit breaker protection
                await self._setup_health_monitoring()
                await self._circuit_breakers.protect(
                    "health_monitor", "start", self._health_monitor.start
                )

                # Start metrics collection with circuit breaker protection
                await self._circuit_breakers.protect(
                    "metrics_collector", "start", self._metrics_collector.start
                )

                # Register services as metrics providers
                await self._setup_metrics_providers(engine_as_metrics_provider)

                # Start monitoring loop
                add_task_callback(asyncio.create_task(loop_manager.monitoring_loop()))

            set_running_callback(True)

            logger.info(
                "trading_engine_started",
                active_tasks=len(tasks),
                safe_mode=safe_mode,
            )

        except Exception as e:
            logger.exception("trading_engine_start_failed", error=str(e))
            # Cleanup any started services
            await cleanup_callback()
            raise

    async def _setup_event_handlers(self, event_router: EventRouter) -> None:
        """Set up event subscriptions for service coordination.

        Args:
            event_router: Event router instance

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit event handler registration
        - NO assumptions about event structure
        - Proper error handling in handlers
        """
        # Subscribe to strategy signal events
        self._event_bus.subscribe(SignalEvent, event_router.handle_strategy_signal_event)

        # Subscribe to order execution events
        self._event_bus.subscribe(OrderEvent, event_router.handle_order_filled_event)

        # Subscribe to market data events
        self._event_bus.subscribe(MarketData, event_router.handle_market_data_event)

        # Subscribe to position events
        self._event_bus.subscribe(PositionEvent, event_router.handle_position_updated_event)

        # Subscribe to risk events
        self._event_bus.subscribe(RiskEvent, event_router.handle_risk_limit_event)

        logger.info("event_handlers_configured", handler_count=5)

    async def _setup_health_monitoring(self) -> None:
        """Set up health monitoring for all services.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Register only services that implement HealthCheckable
        - Use explicit service names from config
        - No auto-discovery of services
        """
        logger.info(
            "health_monitoring_setup_initiated",
            health_check_interval=float(self.config.monitoring.health_check_interval_seconds),
        )

        # Register services that implement HealthCheckable protocol
        self._health_monitor.register_service("trading_service", self._trading_service)
        self._health_monitor.register_service("execution_engine", self._execution_service)
        self._health_monitor.register_service("portfolio_service", self._portfolio_service)

        logger.info(
            "health_monitoring_setup_completed",
            registered_services=len(self._health_monitor.get_registered_services()),
        )

    async def _setup_metrics_providers(self, engine_as_metrics_provider: MetricsProvider) -> None:
        """Set up metrics providers for all services.

        Args:
            engine_as_metrics_provider: Engine instance as metrics provider

        IMPORTANT: Following CODING_STANDARDS.md:
        - Register only services that implement get_metrics()
        - No auto-discovery of providers
        - Explicit provider registration
        """
        logger.info("metrics_providers_setup_initiated")

        # Register trading engine itself as a metrics provider
        self._metrics_collector.register_provider(engine_as_metrics_provider)

        # Log completion with actual count from metrics collector
        logger.info(
            "metrics_providers_setup_completed",
            providers_registered=self._metrics_collector.get_registered_provider_count(),
        )

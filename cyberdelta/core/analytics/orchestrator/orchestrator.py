"""Portfolio analytics orchestrator with complete modular integration."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.analytics.models.performance import (
    AnalyticsState,
    PerformanceSnapshot,
    AttributionResult,
)
from cyberdelta.core.analytics.components import (
    AnalyticsFactory,
    PerformanceCalculator,
    AttributionAnalyzer,
    ReportGenerator,
    AlertManager,
    HealthMonitor,
    MetricsAggregator,
    SnapshotManager,
)
from cyberdelta.core.portfolio.services import PortfolioServiceFactory
from cyberdelta.core.infrastructure.events import EventType, BaseEvent
from cyberdelta.core.portfolio.events.error_events import ErrorOccurredEvent, ErrorData
from typing import Any

# Type alias for portfolio events
PortfolioEvent = BaseEvent[Any]
from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)


class AnalyticsOrchestrator(BaseModel):
    """Advanced analytics orchestrator for portfolio performance tracking.
    
    This orchestrator replaces the legacy analytics functionality that was
    embedded in the legacy portfolio tracker, providing modular and extensible analytics
    capabilities within the portfolio module boundaries.
    """

    portfolio_service_factory: PortfolioServiceFactory = Field(..., description="Portfolio service factory")

    # Configuration fields
    calculation_interval: float = Field(default=60.0, gt=0.0, description="Analytics calculation interval in seconds")
    snapshot_interval: float = Field(default=300.0, gt=0.0, description="Performance snapshot interval in seconds")
    report_retention_days: int = Field(default=90, ge=1, description="Days to retain reports")
    max_attribution_depth: int = Field(default=3, ge=1, description="Maximum attribution analysis depth")

    # State fields
    performance_history: list[PerformanceSnapshot] = Field(default_factory=list, description="Performance history")
    attribution_cache: dict[str, AttributionResult] = Field(default_factory=dict, description="Attribution cache")
    active_reports: dict[str, Any] = Field(default_factory=dict, description="Active report configurations")
    alert_thresholds: dict[str, Decimal] = Field(default_factory=dict, description="Alert thresholds")
    orchestrator_state: AnalyticsState = Field(default=AnalyticsState.INACTIVE, description="Orchestrator state")

    model_config = ConfigDict(extra="allow", validate_assignment=True, arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any) -> None:
        """Initialize components after Pydantic validation."""
        self.portfolio_factory = self.portfolio_service_factory
        self.portfolio_manager = self.portfolio_service_factory.get_portfolio_manager()
        
        # Get analytics services from factory if available
        # Get analytics services from factory if available
        self.performance_analytics: Optional[Any] = None
        if hasattr(self.portfolio_service_factory, 'get_performance_analytics'):
            self.performance_analytics = self.portfolio_service_factory.get_performance_analytics()
            
        self.event_dispatcher: Optional[Any] = None
        if hasattr(self.portfolio_service_factory, 'get_event_dispatcher'):
            self.event_dispatcher = self.portfolio_service_factory.get_event_dispatcher()

        # Analytics components (initialized during start)
        self.analytics_factory: Optional[AnalyticsFactory] = None
        self.performance_calculator: Optional[PerformanceCalculator] = None
        self.attribution_analyzer: Optional[AttributionAnalyzer] = None
        self.report_generator: Optional[ReportGenerator] = None
        self.alert_manager: Optional[AlertManager] = None
        self.health_monitor: Optional[HealthMonitor] = None
        self.metrics_aggregator: Optional[MetricsAggregator] = None
        self.snapshot_manager: Optional[SnapshotManager] = None

        # Background tasks
        self._tasks: list[asyncio.Task[None]] = []

        # Set default alert thresholds
        self.alert_thresholds = {
            "max_drawdown": Decimal("0.15"),  # 15% max drawdown
            "daily_loss": Decimal("0.05"),     # 5% daily loss
            "position_concentration": Decimal("0.25"),  # 25% in single position
            "leverage_limit": Decimal("3.0"),   # 3x leverage
        }

    async def start(self) -> None:
        """Start the analytics orchestrator."""
        if self.orchestrator_state != AnalyticsState.INACTIVE:
            raise RuntimeError(f"Cannot start orchestrator in state: {self.orchestrator_state}")

        self.orchestrator_state = AnalyticsState.UPDATING
        logger.info("Starting portfolio analytics orchestrator")

        try:
            # Initialize portfolio system
            await self.portfolio_factory.initialize_all()

            # Initialize analytics components
            await self._initialize_analytics_components()

            # Load historical data for analytics
            await self._load_historical_data()

            # Register event handlers
            await self._register_event_handlers()

            # Start background tasks
            await self._start_background_tasks()

            self.orchestrator_state = AnalyticsState.ACTIVE
            logger.info("Portfolio analytics orchestrator started successfully")

        except Exception as e:
            self.orchestrator_state = AnalyticsState.ERROR
            logger.error(f"Failed to start analytics orchestrator: {e}")
            raise RuntimeError(f"Failed to start analytics orchestrator: {e}") from e

    async def stop(self) -> None:
        """Stop the analytics orchestrator gracefully."""
        if self.orchestrator_state == AnalyticsState.INACTIVE:
            return

        self.orchestrator_state = AnalyticsState.UPDATING
        logger.info("Stopping portfolio analytics orchestrator")

        try:
            # Stop background tasks
            await self._stop_background_tasks()

            # Save performance data
            await self._save_performance_data()

            # Shutdown components
            await self._shutdown_analytics_components()

            self.orchestrator_state = AnalyticsState.INACTIVE
            logger.info("Portfolio analytics orchestrator stopped successfully")

        except Exception as e:
            self.orchestrator_state = AnalyticsState.ERROR
            logger.error(f"Failed to stop analytics orchestrator: {e}")
            raise RuntimeError(f"Failed to stop analytics orchestrator: {e}") from e

    async def calculate_performance_snapshot(self) -> PerformanceSnapshot:
        """Calculate current portfolio performance snapshot."""
        if self.orchestrator_state != AnalyticsState.ACTIVE:
            raise RuntimeError("Analytics orchestrator is not active")

        self.orchestrator_state = AnalyticsState.CALCULATING
        
        try:
            # Get current portfolio state
            portfolio_state = await self.portfolio_manager.get_portfolio_summary()
            
            # Calculate performance metrics
            if not self.performance_calculator:
                raise RuntimeError("Performance calculator not initialized")
            snapshot = await self.performance_calculator.calculate_snapshot(portfolio_state)
            
            # Update history
            self.performance_history.append(snapshot)
            
            # Trim history to retention period
            cutoff_date = datetime.now(UTC) - timedelta(days=self.report_retention_days)
            self.performance_history = [
                s for s in self.performance_history 
                if s.timestamp > cutoff_date
            ]
            
            self.orchestrator_state = AnalyticsState.ACTIVE
            return snapshot
            
        except Exception as e:
            self.orchestrator_state = AnalyticsState.ERROR
            logger.error(f"Failed to calculate performance snapshot: {e}")
            raise

    async def calculate_attribution(self, period: timedelta) -> AttributionResult:
        """Calculate performance attribution for specified period."""
        if self.orchestrator_state != AnalyticsState.ACTIVE:
            raise RuntimeError("Analytics orchestrator is not active")

        # Check cache first
        cache_key = f"attribution_{period.total_seconds()}"
        if cache_key in self.attribution_cache:
            cached = self.attribution_cache[cache_key]
            # Return cached if less than 5 minutes old
            if hasattr(cached, 'timestamp') and (datetime.now(UTC) - cached.timestamp) < timedelta(minutes=5):
                return cached

        self.orchestrator_state = AnalyticsState.CALCULATING
        
        try:
            # Get portfolio state
            portfolio_state = await self.portfolio_manager.get_portfolio_summary()
            
            # Calculate attribution
            if not self.attribution_analyzer:
                raise RuntimeError("Attribution analyzer not initialized")
            attribution = await self.attribution_analyzer.analyze_attribution(
                portfolio_state, 
                period,
                self.max_attribution_depth
            )
            
            # Cache result
            self.attribution_cache[cache_key] = attribution
            
            self.orchestrator_state = AnalyticsState.ACTIVE
            return attribution
            
        except Exception as e:
            self.orchestrator_state = AnalyticsState.ERROR
            logger.error(f"Failed to calculate attribution: {e}")
            raise

    async def generate_report(self, report_type: str, params: dict[str, Any]) -> dict[str, Any]:
        """Generate analytics report."""
        if self.orchestrator_state != AnalyticsState.ACTIVE:
            raise RuntimeError("Analytics orchestrator is not active")

        if not self.report_generator:
            raise RuntimeError("Report generator not initialized")
        return await self.report_generator.generate_report(
            report_type,
            self.performance_history,
            params
        )

    async def check_alerts(self) -> list[dict[str, Any]]:
        """Check for alert conditions."""
        if self.orchestrator_state != AnalyticsState.ACTIVE:
            return []

        # Get latest snapshot
        if not self.performance_history:
            return []
            
        latest = self.performance_history[-1]
        alerts = []

        # Check max drawdown
        if latest.max_drawdown > self.alert_thresholds.get("max_drawdown", Decimal("1.0")):
            alerts.append({
                "type": "max_drawdown_exceeded",
                "severity": "high",
                "value": latest.max_drawdown,
                "threshold": self.alert_thresholds["max_drawdown"],
                "timestamp": datetime.now(UTC)
            })

        # Check daily loss
        if latest.daily_pnl < -self.alert_thresholds.get("daily_loss", Decimal("1.0")) * latest.total_value:
            alerts.append({
                "type": "daily_loss_exceeded",
                "severity": "high",
                "value": latest.daily_pnl,
                "threshold": -self.alert_thresholds["daily_loss"] * latest.total_value,
                "timestamp": datetime.now(UTC)
            })

        return alerts

    async def get_health_status(self) -> dict[str, Any]:
        """Get analytics system health status."""
        if not self.health_monitor:
            raise RuntimeError("Health monitor not initialized")
        return await self.health_monitor.get_status()

    async def _initialize_analytics_components(self) -> None:
        """Initialize all analytics system components."""
        logger.info("Initializing analytics components")
        
        # Analytics factory
        self.analytics_factory = AnalyticsFactory()

        # Core analytics components
        self.performance_calculator = PerformanceCalculator(
            portfolio_manager=self.portfolio_manager
        )
        
        self.attribution_analyzer = AttributionAnalyzer(
            portfolio_manager=self.portfolio_manager
        )
        
        self.report_generator = ReportGenerator()
        
        self.alert_manager = AlertManager(
            thresholds=self.alert_thresholds
        )
        
        self.health_monitor = HealthMonitor()
        
        self.metrics_aggregator = MetricsAggregator()
        
        self.snapshot_manager = SnapshotManager(
            retention_days=self.report_retention_days
        )

        # Initialize all components
        components = [
            self.analytics_factory,
            self.performance_calculator,
            self.attribution_analyzer,
            self.report_generator,
            self.alert_manager,
            self.health_monitor,
            self.metrics_aggregator,
            self.snapshot_manager,
        ]

        for component in components:
            if hasattr(component, 'initialize'):
                await component.initialize()

        logger.info("Analytics components initialized successfully")

    async def _load_historical_data(self) -> None:
        """Load historical data for analytics calculations."""
        logger.info("Loading historical analytics data")
        
        # Load from persistence if available
        if self.snapshot_manager:
            historical = await self.snapshot_manager.load_historical_snapshots()
            self.performance_history.extend(historical)
            
        logger.info(f"Loaded {len(self.performance_history)} historical snapshots")

    async def _register_event_handlers(self) -> None:
        """Register handlers for portfolio events."""
        if not self.event_dispatcher:
            logger.warning("No event dispatcher available, skipping event handler registration")
            return
            
        logger.info("Registering analytics event handlers")
        
        # Register for relevant events
        event_handlers = {
            EventType.POSITION_UPDATED: self._handle_position_event,
            EventType.POSITION_OPENED: self._handle_position_event,
            EventType.POSITION_CLOSED: self._handle_position_event,
            EventType.TRADE_PROCESSED: self._handle_trade_event,
            EventType.TRADE_RECEIVED: self._handle_trade_event,
            EventType.BALANCE_UPDATED: self._handle_balance_event,
            EventType.PNL_REALIZED: self._handle_pnl_event,
            EventType.PNL_UNREALIZED_UPDATED: self._handle_pnl_event,
            EventType.ERROR_OCCURRED: self._handle_error_event,
        }
        
        for event_type, handler in event_handlers.items():
            await self.event_dispatcher.register_handler(event_type, handler)

    async def _start_background_tasks(self) -> None:
        """Start all background processing tasks."""
        logger.info("Starting analytics background tasks")
        
        # Performance calculation task
        self._tasks.append(
            asyncio.create_task(self._performance_calculation_loop())
        )
        
        # Alert monitoring task
        self._tasks.append(
            asyncio.create_task(self._alert_monitoring_loop())
        )
        
        # Report generation task
        self._tasks.append(
            asyncio.create_task(self._report_generation_loop())
        )
        
        # Health monitoring task
        self._tasks.append(
            asyncio.create_task(self._health_monitoring_loop())
        )

    async def _stop_background_tasks(self) -> None:
        """Stop all background tasks."""
        logger.info("Stopping analytics background tasks")
        
        for task in self._tasks:
            task.cancel()
            
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        self._tasks.clear()

    async def _save_performance_data(self) -> None:
        """Save performance data to persistence."""
        if self.snapshot_manager and self.performance_history:
            await self.snapshot_manager.save_snapshots(self.performance_history)
            logger.info(f"Saved {len(self.performance_history)} performance snapshots")

    async def _shutdown_analytics_components(self) -> None:
        """Shutdown all analytics components."""
        logger.info("Shutting down analytics components")
        
        components = [
            self.analytics_factory,
            self.performance_calculator,
            self.attribution_analyzer,
            self.report_generator,
            self.alert_manager,
            self.health_monitor,
            self.metrics_aggregator,
            self.snapshot_manager,
        ]
        
        for component in components:
            if component and hasattr(component, 'shutdown'):
                await component.shutdown()

    async def _performance_calculation_loop(self) -> None:
        """Background task for periodic performance calculations."""
        while self.orchestrator_state == AnalyticsState.ACTIVE:
            try:
                await self.calculate_performance_snapshot()
                await asyncio.sleep(self.calculation_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in performance calculation loop: {e}")
                await asyncio.sleep(self.calculation_interval)

    async def _alert_monitoring_loop(self) -> None:
        """Background task for monitoring alert conditions."""
        while self.orchestrator_state == AnalyticsState.ACTIVE:
            try:
                alerts = await self.check_alerts()
                
                if alerts and self.alert_manager:
                    for alert in alerts:
                        await self.alert_manager.process_alert(alert)
                        
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in alert monitoring loop: {e}")
                await asyncio.sleep(30)

    async def _report_generation_loop(self) -> None:
        """Background task for scheduled report generation."""
        while self.orchestrator_state == AnalyticsState.ACTIVE:
            try:
                # Generate daily report at midnight
                now = datetime.now(UTC)
                next_midnight = (now + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                sleep_seconds = (next_midnight - now).total_seconds()
                
                await asyncio.sleep(sleep_seconds)
                
                # Generate daily report
                if self.report_generator:
                    report = await self.generate_report("daily", {
                        "date": now.date(),
                        "include_attribution": True,
                        "include_positions": True,
                    })
                    
                    # Store report
                    report_key = f"daily_{now.strftime('%Y%m%d')}"
                    self.active_reports[report_key] = report
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in report generation loop: {e}")
                await asyncio.sleep(3600)  # Wait an hour before retry

    async def _health_monitoring_loop(self) -> None:
        """Background task for health monitoring."""
        while self.orchestrator_state == AnalyticsState.ACTIVE:
            try:
                if self.health_monitor:
                    await self.health_monitor.check_health()
                    
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health monitoring loop: {e}")
                await asyncio.sleep(60)

    async def _handle_position_event(self, event: PortfolioEvent) -> None:
        """Handle position-related events with real portfolio updates."""
        logger.info(f"Processing position event: {event.event_type}")
        
        try:
            # Update metrics aggregator
            if self.metrics_aggregator:
                await self.metrics_aggregator.update_position_metrics(event)
            
            # Trigger performance snapshot calculation
            if self.performance_calculator:
                snapshot = await self.performance_calculator.calculate_snapshot()
                logger.info(f"Position event triggered performance update: total_value={snapshot.total_value}")
                
            # Update attribution analysis for position changes
            if self.attribution_analyzer and event.event_type in [EventType.POSITION_OPENED, EventType.POSITION_CLOSED]:
                from datetime import timedelta
                attribution = await self.attribution_analyzer.analyze_attribution(
                    await self.portfolio_manager.get_portfolio_summary(),
                    period=timedelta(hours=24)
                )
                logger.info(f"Position attribution updated: total_pnl={attribution.total_pnl}")
                
        except Exception as e:
            logger.error(f"Error handling position event: {e}", exc_info=True)

    async def _handle_trade_event(self, event: PortfolioEvent) -> None:
        """Handle trade execution events with real portfolio updates."""
        logger.info(f"Processing trade event: {event.event_type}")
        
        try:
            # Update metrics aggregator
            if self.metrics_aggregator:
                await self.metrics_aggregator.update_trade_metrics(event)
            
            # Add trade to trade manager if available
            if hasattr(self.portfolio_manager, 'trade_manager') and event.event_type == EventType.TRADE_PROCESSED:
                trade_data = event.data
                if 'trade' in trade_data:
                    trade = trade_data['trade']
                    await self.portfolio_manager.trade_manager.add_trade(trade)
                    logger.info(f"Trade added to history: {trade.id}")
            
            # Recalculate performance metrics after trade
            if self.performance_calculator:
                snapshot = await self.performance_calculator.calculate_snapshot()
                logger.info(f"Trade event triggered performance update: realized_pnl={snapshot.realized_pnl}")
                
            # Update time-based attribution for this trade
            if self.attribution_analyzer:
                from datetime import timedelta
                attribution = await self.attribution_analyzer.analyze_attribution(
                    await self.portfolio_manager.get_portfolio_summary(),
                    period=timedelta(hours=1)  # Shorter period for trade analysis
                )
                
        except Exception as e:
            logger.error(f"Error handling trade event: {e}", exc_info=True)

    async def _handle_balance_event(self, event: PortfolioEvent) -> None:
        """Handle balance update events with real portfolio updates."""
        logger.info(f"Processing balance event: {event.event_type}")
        
        try:
            # Update metrics aggregator
            if self.metrics_aggregator:
                await self.metrics_aggregator.update_balance_metrics(event)
            
            # Recalculate portfolio value after balance change
            if self.performance_calculator:
                snapshot = await self.performance_calculator.calculate_snapshot()
                logger.info(f"Balance event triggered performance update: total_value={snapshot.total_value}")
                
            # Check if this balance change affects risk metrics
            balance_data = event.data
            if 'asset' in balance_data and balance_data['asset'] == 'USDC':
                # USDC balance changes affect total portfolio value significantly
                if self.attribution_analyzer:
                    from datetime import timedelta
                    attribution = await self.attribution_analyzer.analyze_attribution(
                        await self.portfolio_manager.get_portfolio_summary(),
                        period=timedelta(hours=24)
                    )
                    logger.info(f"Balance change attribution: by_exchange={attribution.by_exchange}")
                    
        except Exception as e:
            logger.error(f"Error handling balance event: {e}", exc_info=True)
    
    async def _handle_pnl_event(self, event: PortfolioEvent) -> None:
        """Handle P&L-related events with real portfolio updates."""
        logger.info(f"Processing P&L event: {event.event_type}")
        
        try:
            # Update metrics aggregator for P&L events
            if self.metrics_aggregator:
                await self.metrics_aggregator.update_trade_metrics(event)
            
            # Recalculate performance metrics after P&L update
            if self.performance_calculator:
                snapshot = await self.performance_calculator.calculate_snapshot()
                if event.event_type == EventType.PNL_REALIZED:
                    logger.info(f"Realized P&L event: total_realized={snapshot.realized_pnl}")
                else:
                    logger.info(f"Unrealized P&L event: total_unrealized={snapshot.unrealized_pnl}")
                
            # Update attribution analysis for realized P&L
            if event.event_type == EventType.PNL_REALIZED and self.attribution_analyzer:
                from datetime import timedelta
                attribution = await self.attribution_analyzer.analyze_attribution(
                    await self.portfolio_manager.get_portfolio_summary(),
                    period=timedelta(hours=24)
                )
                logger.info(f"P&L attribution: by_symbol={len(attribution.by_symbol)} symbols")
                
        except Exception as e:
            logger.error(f"Error handling P&L event: {e}", exc_info=True)
    
    async def _handle_error_event(self, event: PortfolioEvent) -> None:
        """Handle error events with recovery actions."""
        logger.warning(f"Processing error event: {event.event_type}")
        
        try:
            error_data = event.data
            error_type = error_data.get('error_type', 'unknown')
            error_message = error_data.get('message', 'No message')
            component = error_data.get('component', 'unknown')
            
            logger.error(f"Portfolio error in {component}: {error_type} - {error_message}")
            
            # If it's a calculation error, try to recalculate
            if 'calculation' in error_type.lower() or 'performance' in error_type.lower():
                if self.performance_calculator:
                    try:
                        snapshot = await self.performance_calculator.calculate_snapshot()
                        logger.info(f"Recovery calculation successful: total_value={snapshot.total_value}")
                    except Exception as recovery_error:
                        logger.error(f"Recovery calculation failed: {recovery_error}")
                        
            # Record error in metrics for monitoring
            if self.metrics_aggregator:
                # Create synthetic error event for metrics
                error_data = ErrorData(
                    error_type=error_type,
                    error_message=error_message,
                    component=component
                )
                error_metric_event = ErrorOccurredEvent.create(
                    error=error_data
                )
                
        except Exception as e:
            logger.error(f"Error handling error event: {e}", exc_info=True)
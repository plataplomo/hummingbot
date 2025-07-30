"""Analytics factory for creating analytics components."""
from __future__ import annotations

from typing import Any, Type, Dict

from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)


class AnalyticsFactory:
    """Factory for creating analytics components."""
    
    def __init__(self) -> None:
        """Initialize analytics factory."""
        self._component_registry: Dict[str, Type[Any]] = {}
        self._initialized = False
        
    async def initialize(self) -> None:
        """Initialize the analytics factory."""
        if self._initialized:
            return
            
        logger.info("Initializing analytics factory")
        
        # Register default components
        self._register_default_components()
        
        self._initialized = True
        logger.info("Analytics factory initialized")
        
    async def shutdown(self) -> None:
        """Shutdown the analytics factory."""
        if not self._initialized:
            return
            
        logger.info("Shutting down analytics factory")
        self._component_registry.clear()
        self._initialized = False
        
    def register_component(self, name: str, component_class: Type[Any]) -> None:
        """Register a new analytics component type."""
        self._component_registry[name] = component_class
        logger.info(f"Registered analytics component: {name}")
        
    def create_component(self, name: str, **kwargs: Any) -> Any:
        """Create an analytics component instance."""
        if name not in self._component_registry:
            raise ValueError(f"Unknown analytics component: {name}")
            
        component_class = self._component_registry[name]
        return component_class(**kwargs)
        
    def _register_default_components(self) -> None:
        """Register default analytics components."""
        # Import here to avoid circular imports
        from cyberdelta.core.portfolio.analytics.components.calculator import PerformanceCalculator
        from cyberdelta.core.portfolio.analytics.components.attribution import AttributionAnalyzer
        from cyberdelta.core.portfolio.analytics.components.reporting import ReportGenerator
        from cyberdelta.core.portfolio.analytics.components.alerts import AlertManager
        from cyberdelta.core.portfolio.analytics.components.monitoring import HealthMonitor
        from cyberdelta.core.portfolio.analytics.components.aggregator import MetricsAggregator
        from cyberdelta.core.portfolio.analytics.components.snapshot import SnapshotManager
        
        default_components = {
            "performance_calculator": PerformanceCalculator,
            "attribution_analyzer": AttributionAnalyzer,
            "report_generator": ReportGenerator,
            "alert_manager": AlertManager,
            "health_monitor": HealthMonitor,
            "metrics_aggregator": MetricsAggregator,
            "snapshot_manager": SnapshotManager,
        }
        
        for name, component_class in default_components.items():
            self.register_component(name, component_class)
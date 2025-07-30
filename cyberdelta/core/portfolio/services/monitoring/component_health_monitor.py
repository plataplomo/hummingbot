"""Component health monitoring service for individual portfolio components."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import Field
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.portfolio_types.models import HealthStatus
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService


@dataclass
class ComponentHealthMetric:
    """Health metric for a specific component."""
    component_name: str
    metric_name: str
    current_value: float
    expected_value: float | None = None
    status: HealthStatus = HealthStatus.HEALTHY
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)


class ComponentHealthMonitor(BasePortfolioService):
    """Monitors health of individual portfolio components."""

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__("component_health_monitor")
        self.config = config or {}
        self.logger = get_logger(__name__)
        
        # Component health tracking
        self.component_statuses: dict[str, HealthStatus] = {}
        self.component_metrics: dict[str, list[ComponentHealthMetric]] = {}
        self.last_check_time = datetime.now(UTC)
        
        # Configuration
        self.check_interval = timedelta(seconds=self.config.get("check_interval", 30))
        self.metric_retention_days = self.config.get("metric_retention_days", 7)

    async def _initialize_service(self) -> None:
        """Initialize component health monitoring."""
        self.logger.info("Initializing component health monitor")
        await self._initialize_component_monitoring()

    async def _shutdown_service(self) -> None:
        """Shutdown component health monitoring."""
        self.logger.info("Shutting down component health monitor")

    async def _initialize_component_monitoring(self) -> None:
        """Initialize monitoring for known components."""
        # Initialize known components
        known_components = [
            "portfolio_manager",
            "balance_manager", 
            "position_manager",
            "order_manager",
            "validation_service",
            "cache_service"
        ]
        
        for component in known_components:
            self.component_statuses[component] = HealthStatus.HEALTHY
            self.component_metrics[component] = []

    async def check_component_health(self) -> dict[str, Any]:
        """Check health of all components."""
        health_results = {}
        current_time = datetime.now(UTC)
        
        for component_name in self.component_statuses.keys():
            try:
                # Check component-specific health
                component_health = await self._check_individual_component(component_name)
                health_results[component_name] = component_health
                
                # Update component status
                self.update_component_health(
                    component_name, 
                    component_health["status"]
                )
                
            except Exception as e:
                self.logger.error(f"Error checking health for {component_name}: {e}")
                health_results[component_name] = {
                    "status": HealthStatus.CRITICAL,
                    "error": str(e),
                    "timestamp": current_time
                }
        
        self.last_check_time = current_time
        return health_results

    async def _check_individual_component(self, component_name: str) -> dict[str, Any]:
        """Check health of an individual component."""
        current_time = datetime.now(UTC)
        
        # Basic component health check (can be extended per component)
        metrics = []
        
        # Check if component is responsive
        responsiveness_metric = ComponentHealthMetric(
            component_name=component_name,
            metric_name="responsiveness",
            current_value=1.0,  # Simplified - assume responsive
            expected_value=1.0,
            status=HealthStatus.HEALTHY,
            timestamp=current_time
        )
        metrics.append(responsiveness_metric)
        
        # Store metrics
        if component_name not in self.component_metrics:
            self.component_metrics[component_name] = []
        
        self.component_metrics[component_name].extend(metrics)
        self._cleanup_old_metrics(component_name)
        
        # Determine overall component status
        overall_status = self._determine_component_status(metrics)
        
        return {
            "status": overall_status,
            "metrics": [self._metric_to_dict(m) for m in metrics],
            "timestamp": current_time,
            "last_updated": current_time
        }

    def update_component_health(self, component: str, status: HealthStatus) -> None:
        """Update health status for a component."""
        if component not in self.component_statuses:
            self.logger.warning(f"Unknown component: {component}")
            return
            
        old_status = self.component_statuses.get(component)
        self.component_statuses[component] = status
        
        if old_status != status:
            self.logger.info(
                f"Component {component} status changed from {old_status} to {status}"
            )

    def get_component_status(self, component: str) -> HealthStatus | None:
        """Get current status of a component."""
        return self.component_statuses.get(component)

    def get_all_component_statuses(self) -> dict[str, HealthStatus]:
        """Get status of all components."""
        return self.component_statuses.copy()

    def get_component_metrics(
        self, 
        component: str, 
        since: datetime | None = None
    ) -> list[ComponentHealthMetric]:
        """Get metrics for a component."""
        if component not in self.component_metrics:
            return []
        
        metrics = self.component_metrics[component]
        
        if since:
            metrics = [m for m in metrics if m.timestamp >= since]
        
        return metrics

    def _determine_component_status(self, metrics: list[ComponentHealthMetric]) -> HealthStatus:
        """Determine overall status from component metrics."""
        if not metrics:
            return HealthStatus.UNKNOWN
        
        # Find worst status
        statuses = [metric.status for metric in metrics]
        
        if HealthStatus.CRITICAL in statuses:
            return HealthStatus.CRITICAL
        if HealthStatus.WARNING in statuses:
            return HealthStatus.WARNING
        if HealthStatus.DEGRADED in statuses:
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY

    def _cleanup_old_metrics(self, component: str) -> None:
        """Remove old metrics beyond retention period."""
        if component not in self.component_metrics:
            return
        
        cutoff_time = datetime.now(UTC) - timedelta(days=self.metric_retention_days)
        self.component_metrics[component] = [
            metric for metric in self.component_metrics[component]
            if metric.timestamp >= cutoff_time
        ]

    def _metric_to_dict(self, metric: ComponentHealthMetric) -> dict[str, Any]:
        """Convert metric to dictionary."""
        return {
            "component_name": metric.component_name,
            "metric_name": metric.metric_name,
            "current_value": metric.current_value,
            "expected_value": metric.expected_value,
            "status": metric.status.value,
            "timestamp": metric.timestamp.isoformat(),
            "metadata": metric.metadata
        }
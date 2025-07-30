"""Portfolio monitoring services for health and performance tracking."""

# New focused monitoring services
from .alert_lifecycle_service import AlertLifecycleService, HealthAlert
from .alert_threshold_manager import AlertSeverity, AlertThreshold, AlertThresholdManager
from .health_alert_coordinator import HealthAlertCoordinator
from .health_check_mixin import BaseHealthCheckMixin, create_health_check_decorator
from .health_check_models import (
    HealthCheckDetails,
    HealthCheckResult,
    HealthCheckable,
    HealthStatus,
)
from .health_check_orchestrator import HealthCheckOrchestrator
from .health_metrics_collector import (
    ApplicationMetrics,
    HealthMetricsCollector,
    PortfolioMetrics,
    SystemMetrics,
)

# Legacy services (keep for now but discourage use)
from .component_health_monitor import ComponentHealthMetric, ComponentHealthMonitor
from .health_alert_manager import AlertRule, HealthAlertManager
from .portfolio_health_checker import (
    PortfolioHealthAlert,
    PortfolioHealthChecker,
    PortfolioHealthMetric,
)
from .system_health_monitor import SystemHealthAlert, SystemHealthMetric, SystemHealthMonitor


__all__ = [
    # New focused services (recommended)
    "AlertLifecycleService",
    "AlertSeverity", 
    "AlertThreshold",
    "AlertThresholdManager",
    "ApplicationMetrics",
    "BaseHealthCheckMixin",
    "HealthAlert",
    "HealthAlertCoordinator",
    "HealthCheckDetails",
    "HealthCheckOrchestrator", 
    "HealthCheckResult",
    "HealthCheckable",
    "HealthMetricsCollector",
    "HealthStatus",
    "PortfolioMetrics",
    "SystemMetrics",
    "create_health_check_decorator",
    # Legacy services
    "AlertRule",
    "ComponentHealthMetric",
    "ComponentHealthMonitor",
    "HealthAlertManager",
    "PortfolioHealthAlert",
    "PortfolioHealthChecker",
    "PortfolioHealthMetric",
    "SystemHealthAlert",
    "SystemHealthMetric",
    "SystemHealthMonitor",
]

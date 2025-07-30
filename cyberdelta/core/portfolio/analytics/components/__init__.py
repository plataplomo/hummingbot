"""Analytics framework components."""

from cyberdelta.core.portfolio.analytics.components.factory import AnalyticsFactory
from cyberdelta.core.portfolio.analytics.components.calculator import PerformanceCalculator
from cyberdelta.core.portfolio.analytics.components.attribution import AttributionAnalyzer
from cyberdelta.core.portfolio.analytics.components.reporting import ReportGenerator
from cyberdelta.core.portfolio.analytics.components.alerts import AlertManager
from cyberdelta.core.portfolio.analytics.components.monitoring import HealthMonitor
from cyberdelta.core.portfolio.analytics.components.aggregator import MetricsAggregator
from cyberdelta.core.portfolio.analytics.components.snapshot import SnapshotManager

__all__ = [
    "AnalyticsFactory",
    "PerformanceCalculator",
    "AttributionAnalyzer",
    "ReportGenerator",
    "AlertManager",
    "HealthMonitor",
    "MetricsAggregator",
    "SnapshotManager",
]
"""
Monitoring module initialization.

This module provides tools for monitoring trading system performance,
including a real-time dashboard and performance tracking.
"""

from cyberdelta.monitoring.dashboard_integration import (
    DashboardIntegration,
    get_dashboard_integration,
)
from cyberdelta.monitoring.performance_tracker import PerformanceTracker
from cyberdelta.monitoring.real_time_dashboard import (
    RealTimeDashboard,
    launch_dashboard,
)

__all__ = [
    "PerformanceTracker",
    "RealTimeDashboard",
    "launch_dashboard",
    "DashboardIntegration",
    "get_dashboard_integration",
]

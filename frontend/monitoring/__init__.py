"""Frontend monitoring module.

This module provides dashboard and monitoring UI components.
"""

from frontend.monitoring.dashboard_integration import (
    DashboardIntegration,
    get_dashboard_integration,
)
from frontend.monitoring.real_time_dashboard import (
    RealTimeDashboard,
    launch_dashboard,
)


__all__ = [
    "DashboardIntegration",
    "get_dashboard_integration",
    "RealTimeDashboard",
    "launch_dashboard",
]

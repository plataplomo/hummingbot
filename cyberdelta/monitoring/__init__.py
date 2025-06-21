"""Monitoring module initialization.

This module provides tools for monitoring trading system performance.
Frontend dashboard components should be imported directly from the frontend package.
"""

from cyberdelta.monitoring.performance_tracker import PerformanceTracker

__all__ = [
    "PerformanceTracker",
]

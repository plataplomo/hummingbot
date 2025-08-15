"""WebSocket health check-related enumerations.

This module contains enums for WebSocket health monitoring and status reporting.
"""

from enum import Enum


class HealthStatus(Enum):
    """Health check status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

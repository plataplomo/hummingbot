"""Infrastructure protocols for system components."""

from __future__ import annotations

from .monitoring import HealthCheckable, MetricsProvider


__all__ = [
    "HealthCheckable",
    "MetricsProvider",
]

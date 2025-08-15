"""Performance-related models for WebSocket operations.

This module contains models for WebSocket performance optimization
and monitoring configuration.
"""

from __future__ import annotations

from pydantic import BaseModel


class PerformanceConfig(BaseModel):
    """Configuration for WebSocket performance optimizations."""

    enable_msgspec: bool = True
    """Enable msgspec for faster validation when available."""

    enable_metrics: bool = True
    """Enable performance metrics collection."""

    enable_memory_optimization: bool = True
    """Enable memory optimization techniques."""

    validation_cache_size: int = 1000
    """Size of validation result cache."""

    metrics_window_size: int = 100
    """Number of recent operations to track for metrics."""

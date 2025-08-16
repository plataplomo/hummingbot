"""WebSocket Processing Metrics Models.

This module provides typed metrics models for WebSocket message processing,
replacing dict-based metrics collection with type-safe Pydantic models.
"""

from __future__ import annotations

# Re-export models from the proper location
from cyberdelta.apis.models.websocket.processing import ProcessingMetrics, ProcessorMetrics


__all__ = ["ProcessingMetrics", "ProcessorMetrics"]

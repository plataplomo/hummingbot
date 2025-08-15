"""WebSocket validation utilities.

This module provides validation for WebSocket error contexts and other
WebSocket-specific data structures.
"""

from .error_validator import StreamErrorContextValidator


__all__ = [
    "StreamErrorContextValidator",
]

"""WebSocket registry and factory components.

This module provides registry patterns, factory implementations, and rate
limiting functionality for managing WebSocket components and controlling
resource usage.

Modules:
- registry_builder: Builder pattern for registries
- registry_factory: Factory to eliminate circular imports
- rate_limiter: Rate limiting implementation
"""

from .rate_limiter import (
    WebSocketRateLimiter,
)
from .registry_builder import (
    WebSocketRegistryBuilder,
)
from .registry_factory import (
    WebSocketRegistryFactory,
)


__all__ = [
    # Rate limiting
    "WebSocketRateLimiter",
    # Registry patterns
    "WebSocketRegistryBuilder",
    "WebSocketRegistryFactory",
]

"""CyberDeltaEngine: WebSocket infrastructure package.

This package contains all WebSocket-related base classes, protocols, and utilities
that are used across different exchange implementations.
"""

from __future__ import annotations

# Re-export rate limiting models
from cyberdelta.apis.models.websocket.rate_limiting import (
    RateLimitConfig,
    RateLimitResult,
)

# Router error metadata
from cyberdelta.apis.models.websocket.router import RouterErrorMetadata

# Re-export MessageTransformer protocol
from cyberdelta.apis.protocols.websocket.processing import MessageTransformer

# Import unified ExchangeName enum
from cyberdelta.enums import ExchangeName

# Rate limiting components
from .rate_limiter import (
    RateLimitMiddleware,
    SlidingWindowCounter,
    TokenBucket,
    WebSocketRateLimiter,
)
from .registry.registry_factory import WebSocketRegistryFactory

# Core WebSocket components
from .ws_context import WebSocketMessageContext
from .ws_context_factory import WebSocketContextFactory
from .ws_context_registry import WebSocketContextRegistry

# Message processing components
from .ws_message_processor import (
    SimpleDictTransformer,
    WebSocketMessageProcessor,
)
from .ws_protocols import WebSocketContextProtocol


__all__ = [
    "ExchangeName",
    "MessageTransformer",
    "RateLimitConfig",
    "RateLimitMiddleware",
    "RateLimitResult",
    "RouterErrorMetadata",
    "SimpleDictTransformer",
    "SlidingWindowCounter",
    "TokenBucket",
    "WebSocketContextFactory",
    "WebSocketContextProtocol",
    "WebSocketContextRegistry",
    "WebSocketMessageContext",
    "WebSocketMessageProcessor",
    "WebSocketRateLimiter",
    "WebSocketRegistryFactory",
]

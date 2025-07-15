"""CyberDeltaEngine: Common API types.

This module contains shared type aliases used across exchange API implementations.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from cyberdelta.apis.base.ws_context import WebSocketContextUnion


# Type alias for WebSocket message handlers
# Handler receives typed context containing validated_envelope and metadata
MessageHandler = Callable[["WebSocketContextUnion"], Awaitable[None]]

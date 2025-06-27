"""CyberDeltaEngine: Common API types.

This module contains shared type aliases used across exchange API implementations.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any


# Type alias for WebSocket message handlers
# Handler receives data_payload (dict) and the full_message (dict)
MessageHandler = Callable[[dict[str, Any], dict[str, Any]], Coroutine[Any, Any, None]]

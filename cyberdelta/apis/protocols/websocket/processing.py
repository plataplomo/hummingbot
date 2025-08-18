"""Processing protocols for WebSocket message handling.

This module contains protocol definitions for message processors
and transformers in the WebSocket system.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol, TypeVar

from pydantic import BaseModel

from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol


# Protocol type variables
T_contra = TypeVar("T_contra", bound=BaseModel, contravariant=True)  # Protocol input
U_co = TypeVar("U_co", covariant=True)  # Protocol output (can be single or batch)

# Message handler type - takes typed context
MessageHandler = Callable[[WebSocketContextProtocol], Awaitable[None]]


class MessageProcessor(Protocol):
    """Protocol for message processors."""

    async def process(
        self,
        payload: dict[str, Any] | list[Any],
        handler: MessageHandler,
        context: WebSocketContextProtocol,
    ) -> None:
        """Process a message payload with typed context."""
        ...


class MessageTransformer(Protocol[T_contra, U_co]):
    """Protocol for transforming validated WebSocket messages to domain models."""

    def transform(
        self,
        validated: T_contra,
        context: WebSocketContextProtocol | None = None,
    ) -> U_co:
        """Transform validated WebSocket message to domain model.

        Args:
            validated: The validated Pydantic model from WebSocket.
            context: Optional typed context containing additional data.

        Returns:
            The transformed domain model.

        Raises:
            Exception: If transformation fails.

        """
        ...

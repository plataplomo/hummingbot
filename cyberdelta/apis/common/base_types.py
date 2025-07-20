"""CyberDeltaEngine: Base types with no dependencies.

This module contains fundamental types that have no dependencies on other modules.
It serves as the base layer for the type hierarchy.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol


# Base validation exception classes
class ApiValidationError(ValueError):
    """Base exception for validation errors across the API layer."""

    def __init__(self, context: str, message: str) -> None:
        """Initialize with context and message.

        Args:
            context: Context where validation failed (e.g., "topic", "channel")
            message: Specific error message
        """
        super().__init__(f"Invalid {context}: {message}")
        self.context = context


class FormatValidationError(ApiValidationError):
    """Exception for format validation failures."""

    def __init__(self, context: str, value: str, expected_format: str) -> None:
        """Initialize with format validation details.

        Args:
            context: Context being validated
            value: The invalid value
            expected_format: Description of expected format
        """
        message = f"'{value}'. Expected format: {expected_format}"
        super().__init__(context, message)
        self.value = value
        self.expected_format = expected_format


class InvalidTopicFormatError(FormatValidationError):
    """Raised when a topic doesn't match expected format."""

    def __init__(self, topic: str) -> None:
        """Initialize with invalid topic."""
        super().__init__("Backpack topic", topic, "'type.symbol'")


class InvalidTopicTypeError(ApiValidationError):
    """Raised when topic type is not valid."""

    def __init__(self, topic: str, topic_type: str, valid_types: set[str]) -> None:
        """Initialize with invalid topic type."""
        expected_types = ", ".join(sorted(valid_types))
        super().__init__(
            "Backpack topic",
            f"'{topic}' has invalid type '{topic_type}'. Valid types: {expected_types}",
        )


class InvalidChannelError(ApiValidationError):
    """Raised when a channel is not valid."""

    def __init__(self, channel: str, valid_channels: set[str]) -> None:
        """Initialize with invalid channel."""
        valid_list = ", ".join(sorted(valid_channels))
        super().__init__("Hyperliquid channel", f"'{channel}'. Valid channels: {valid_list}")


@runtime_checkable
class DomainModelProtocol(Protocol):
    """Protocol for domain models that can be serialized."""

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        """Dump model to dictionary (Pydantic v2)."""
        ...

    def dict(self) -> dict[str, Any]:
        """Dump model to dictionary (Pydantic v1)."""
        ...


class BaseContextProtocol(Protocol):
    """Base protocol for context objects.

    This protocol defines the minimal interface for context objects
    without depending on specific implementations.
    """

    @property
    def exchange_name(self) -> str:
        """Get exchange name."""
        ...

    @property
    def validated_envelope(self) -> object | None:
        """Get validated envelope if available."""
        ...

    @property
    def raw_model(self) -> object | None:
        """Get raw validated model if available."""
        ...

    # Domain model attribute - set by processor after transformation
    # Type is Any because it varies based on the transformer used
    domain_model: Any


# Type alias for message handlers using WebSocket protocol
MessageHandler = Callable[["WebSocketContextProtocol"], Awaitable[None]]

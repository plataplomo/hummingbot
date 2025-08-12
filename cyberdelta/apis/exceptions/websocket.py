"""WebSocket-related exceptions for CyberDelta.

DEPRECATED: These legacy WebSocket exceptions are deprecated and will be removed.
Use the new decoupled WebSocket error system from cyberdelta.apis.websocket.ws_exceptions instead.

These exceptions handled WebSocket subscription, message parsing,
and channel-specific validation errors but are no longer used.
"""

import warnings

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.api_error_codes import APIErrorCode


warnings.warn(
    "The websocket exceptions in cyberdelta.apis.exceptions.websocket are deprecated. "
    "Use cyberdelta.apis.websocket.ws_exceptions instead.",
    DeprecationWarning,
    stacklevel=2,
)


class WebSocketError(APIError):
    """Base class for WebSocket-related errors."""

    def __init__(
        self,
        message: str,
        *,
        code: int | str | None = None,
        channel: str | None = None,
        topic: str | None = None,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        metadata: dict[str, object] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize WebSocket error.

        Args:
            message: Human-readable error description
            code: Error code (defaults to UNKNOWN if not provided)
            channel: WebSocket channel name
            topic: Subscription topic
            http_status: HTTP status code
            exchange_code: Exchange-specific error code
            exchange_message: Exchange-specific error message
            retry_after: Seconds to wait before retry
            metadata: Additional error context
            original_exception: The underlying exception
        """
        # Build full metadata
        full_metadata = metadata or {}
        if channel:
            full_metadata["channel"] = channel
        if topic:
            full_metadata["topic"] = topic

        super().__init__(
            message=message,
            code=code or APIErrorCode.UNKNOWN.value,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata=full_metadata,
            original_exception=original_exception,
        )


class UserEventsSubscriptionError(ValueError):
    """Raised when attempting to subscribe to userEvents without wallet address."""

    def __init__(self) -> None:
        """Initialize user events subscription error."""
        super().__init__(
            "Cannot subscribe to userEvents without wallet address. "
            "Ensure private_key is configured in secrets.",
        )


class UnsupportedWebSocketTopicError(WebSocketError):
    """Raised when attempting to subscribe to an unsupported WebSocket topic."""

    def __init__(
        self,
        topic: str,
        supported_formats: list[str] | None = None,
    ) -> None:
        """Initialize unsupported topic error.

        Args:
            topic: The unsupported topic
            supported_formats: List of supported topic formats
        """
        self.topic = topic
        self.supported_formats = supported_formats or [
            "l2Book:COIN",
            "trades:COIN",
            "userEvents",
            "candle:COIN:INTERVAL",
            "allMids",
        ]

        message = f"Unsupported WebSocket topic: {topic}. "
        if self.supported_formats:
            message += f"Supported formats: {', '.join(repr(f) for f in self.supported_formats)}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_PARAMS.value,
            topic=topic,
            metadata={"supported_formats": self.supported_formats},
        )


class WebSocketSubscriptionError(ValueError):
    """Raised when WebSocket subscription construction fails."""

    def __init__(
        self,
        topic: str,
        reason: str | Exception,
    ) -> None:
        """Initialize subscription construction error.

        Args:
            topic: The topic that failed
            reason: Reason for failure
        """
        self.topic = topic
        self.reason = reason

        super().__init__(f"Failed to construct subscription payload for topic '{topic}': {reason}")


class InvalidWebSocketDataError(WebSocketError):
    """Raised when WebSocket data has invalid format."""

    def __init__(
        self,
        channel: str,
        expected_type: str,
        actual_type: str | None = None,
        data: object = None,
    ) -> None:
        """Initialize invalid data error.

        Args:
            channel: WebSocket channel name
            expected_type: Expected data type (e.g., "dict", "list")
            actual_type: Actual data type received
            data: The invalid data (for debugging)
        """
        self.channel = channel
        self.expected_type = expected_type
        self.actual_type = actual_type or type(data).__name__ if data is not None else "None"

        message = f"{channel} data not {expected_type}"
        if self.actual_type != expected_type:
            message = f"{message}, got {self.actual_type}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_RESPONSE.value,
            channel=channel,
            metadata={
                "expected_type": expected_type,
                "actual_type": self.actual_type,
            },
        )

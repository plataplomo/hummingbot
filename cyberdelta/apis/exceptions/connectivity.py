"""Connectivity-related exceptions for HTTP and WebSocket operations.

This module provides specific exception classes for connectivity errors to fix TRY003 violations.
"""

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.api_error_codes import APIErrorCode


class ContentTypeValidationError(APIError):
    """Content-Type header validation failed."""

    def __init__(self, content_type: str, reason: str) -> None:
        """Initialize ContentTypeValidationError.

        Args:
            content_type: The invalid content-type string
            reason: Reason for validation failure
        """
        self.content_type = content_type
        self.reason = reason

        super().__init__(
            message=f"Content-Type validation failed: {reason}",
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "content_type": content_type,
                "reason": reason,
                "error_type": "content_type_validation",
            },
        )


class ResponseParsingError(APIError):
    """Failed to parse HTTP response."""

    def __init__(
        self,
        url: str,
        status_code: int,
        reason: str,
        response_text: str | None = None,
    ) -> None:
        """Initialize ResponseParsingError.

        Args:
            url: The URL that failed to parse
            status_code: HTTP status code
            reason: Reason for parsing failure
            response_text: Optional response text preview
        """
        self.url = url
        self.status_code = status_code
        self.reason = reason
        self.response_text = response_text

        super().__init__(
            message=f"Failed to parse response from {url} (status {status_code}): {reason}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
            metadata={
                "url": url,
                "status_code": status_code,
                "reason": reason,
                "response_preview": response_text[:200] if response_text else None,
            },
        )


class WebSocketConnectionClosedError(APIError):
    """WebSocket connection is closed."""

    def __init__(self, reason: str | None = None) -> None:
        """Initialize WebSocketConnectionClosedError.

        Args:
            reason: Optional reason for connection closure
        """
        self.reason = reason

        message = "WebSocket connection is closed"
        if reason:
            message = f"{message}: {reason}"

        super().__init__(
            message=message,
            code=APIErrorCode.CONNECTION_ERROR.value,
            metadata={"reason": reason, "error_type": "websocket_closed"},
        )


class HttpTimeoutError(APIError):
    """HTTP request timed out."""

    def __init__(self, url: str, timeout: float, operation: str | None = None) -> None:
        """Initialize HttpTimeoutError.

        Args:
            url: The URL that timed out
            timeout: Timeout duration in seconds
            operation: Optional operation description
        """
        self.url = url
        self.timeout = timeout
        self.operation = operation

        message = f"HTTP request to {url} timed out after {timeout}s"
        if operation:
            message = f"{message} during {operation}"

        super().__init__(
            message=message,
            code=APIErrorCode.TIMEOUT.value,
            metadata={
                "url": url,
                "timeout": timeout,
                "operation": operation,
                "error_type": "http_timeout",
            },
        )

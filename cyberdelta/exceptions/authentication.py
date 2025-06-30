"""Authentication-related exceptions for CyberDelta.

These exceptions handle authentication errors including API key validation,
signature generation, and authenticator configuration.
"""

from typing import Any

from cyberdelta.apis.common import APIError, APIErrorCode


class AuthenticationError(APIError):
    """Base class for authentication-related errors."""

    def __init__(
        self,
        message: str,
        *,
        code: int | str | None = None,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize authentication error with default code and status.

        Args:
            message: Human-readable error description
            code: Error code (defaults to AUTHENTICATION_FAILED)
            http_status: HTTP status code (defaults to 401)
            exchange_code: Exchange-specific error code
            exchange_message: Exchange-specific error message
            retry_after: Seconds to wait before retry
            metadata: Additional error context
            original_exception: The underlying exception
        """
        # Default to AUTHENTICATION_FAILED for auth errors
        if code is None:
            code = APIErrorCode.AUTHENTICATION_FAILED.value
        # Auth errors typically not retryable
        if http_status is None:
            http_status = 401
        super().__init__(
            message=message,
            code=code,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata=metadata,
            original_exception=original_exception,
        )


class InvalidAPIKeyError(AuthenticationError):
    """Raised when API key is invalid or missing."""

    def __init__(self, key_type: str = "API", reason: str | None = None) -> None:
        """Initialize invalid API key error.

        Args:
            key_type: Type of key (default: "API")
            reason: Optional reason for invalidity
        """
        self.key_type = key_type
        self.reason = reason

        if reason:
            message = f"{key_type} key (Base64 public ED25519 key) {reason}"
        else:
            message = f"{key_type} key (Base64 public ED25519 key) cannot be empty"

        super().__init__(
            message=message,
            exchange_code="INVALID_API_KEY",
            metadata={"key_type": key_type, "reason": reason or "empty"},
        )


class InvalidPrivateKeyError(AuthenticationError):
    """Raised when private key is invalid or cannot be loaded."""

    def __init__(self, reason: str, original_error: Exception | None = None) -> None:
        """Initialize invalid private key error.

        Args:
            reason: Reason for key invalidity
            original_error: Optional original exception
        """
        self.reason = reason

        # Handle both empty key and invalid format cases
        if "cannot be empty" in reason:
            message = "Private key (Base64 private ED25519 key) cannot be empty"
        else:
            message = f"Invalid Base64 ED25519 private key: {reason}"

        super().__init__(
            message=message,
            exchange_code="INVALID_PRIVATE_KEY",
            original_exception=original_error,
            metadata={"key_type": "private", "error_reason": reason},
        )


class AuthenticationPreparationError(AuthenticationError):
    """Raised when authentication preparation fails."""

    def __init__(
        self, operation: str, reason: str, original_error: Exception | None = None
    ) -> None:
        """Initialize authentication preparation error.

        Args:
            operation: Operation that failed
            reason: Reason for failure
            original_error: Optional original exception
        """
        self.operation = operation
        self.reason = reason

        super().__init__(
            message=f"Authentication preparation failed: {reason}",
            exchange_code="AUTH_PREP_FAILED",
            original_exception=original_error,
            metadata={"operation": operation, "failure_reason": reason},
        )


class WebSocketSignatureError(AuthenticationError):
    """Raised when WebSocket signature generation fails."""

    def __init__(self, reason: str, original_error: Exception | None = None) -> None:
        """Initialize WebSocket signature error.

        Args:
            reason: Reason for signature generation failure
            original_error: Optional original exception
        """
        self.reason = reason

        super().__init__(
            message=f"WebSocket signature generation failed: {reason}",
            exchange_code="WS_SIGNATURE_FAILED",
            original_exception=original_error,
            metadata={"operation": "websocket_signature", "failure_reason": reason},
        )


class AuthenticatorNotConfiguredError(AuthenticationError):
    """Raised when required authenticator is not configured."""

    def __init__(self, auth_type: str, operation: str) -> None:
        """Initialize authenticator not configured error.

        Args:
            auth_type: Type of authenticator required
            operation: Operation requiring authentication
        """
        self.auth_type = auth_type
        self.operation = operation

        super().__init__(
            message=f"{auth_type} authenticator required for {operation}",
            exchange_code="AUTHENTICATOR_REQUIRED",
            metadata={"auth_type": auth_type, "operation": operation, "required": True},
        )


class UnknownEndpointError(APIError):
    """Raised when an API endpoint is not mapped or recognized."""

    def __init__(self, method: str, path: str, exchange: str = "Backpack") -> None:
        """Initialize unknown endpoint error.

        Args:
            method: HTTP method
            path: API endpoint path
            exchange: Exchange name (default: "Backpack")
        """
        self.method = method.upper()
        self.path = path
        self.exchange = exchange

        super().__init__(
            message=f"{exchange} instruction not found for {self.method} {self.path}",
            code=APIErrorCode.INVALID_REQUEST.value,
            exchange_code="UNKNOWN_ENDPOINT",
            metadata={"method": self.method, "path": self.path, "exchange": self.exchange},
        )

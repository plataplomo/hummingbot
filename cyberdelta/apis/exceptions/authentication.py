"""Authentication-related exceptions for CyberDelta.

These exceptions handle authentication errors including API key validation,
signature generation, and authenticator configuration.
"""

from cyberdelta.apis.common import APIError, APIErrorCode


class InvalidPrivateKeyError(APIError, ValueError):
    """Raised when private key is invalid or cannot be loaded.

    Inherits from both APIError and ValueError for backward compatibility.
    """

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

        # Initialize APIError
        APIError.__init__(
            self,
            message=message,
            code=APIErrorCode.AUTHENTICATION_FAILED.value,
            http_status=401,
            exchange_code="INVALID_PRIVATE_KEY",
            original_exception=original_error,
            metadata={"key_type": "private", "error_reason": reason},
        )

        # Initialize ValueError with same message
        ValueError.__init__(self, message)


class InvalidAPIKeyError(APIError, ValueError):
    """Raised when API key is invalid or missing.

    Inherits from both APIError and ValueError for backward compatibility.
    """

    def __init__(self, reason: str = "cannot be empty") -> None:
        """Initialize invalid API key error.

        Args:
            reason: Specific reason why the API key is invalid
        """
        message = f"API key (Base64 public ED25519 key) {reason}"

        # Initialize APIError
        APIError.__init__(
            self,
            message=message,
            code=APIErrorCode.AUTHENTICATION_FAILED.value,
            http_status=401,
            exchange_code="INVALID_API_KEY",
            metadata={"key_type": "api", "error_reason": reason},
        )

        # Initialize ValueError with same message
        ValueError.__init__(self, message)


class AuthenticationPreparationError(APIError):
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
            code=APIErrorCode.AUTHENTICATION_FAILED.value,
            http_status=401,
            exchange_code="AUTH_PREP_FAILED",
            original_exception=original_error,
            metadata={"operation": operation, "failure_reason": reason},
        )


class WebSocketSignatureError(APIError):
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
            code=APIErrorCode.AUTHENTICATION_FAILED.value,
            http_status=401,
            exchange_code="WS_SIGNATURE_FAILED",
            original_exception=original_error,
            metadata={"operation": "websocket_signature", "failure_reason": reason},
        )


class AuthenticatorNotConfiguredError(APIError):
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
            code=APIErrorCode.AUTHENTICATION_FAILED.value,
            http_status=401,
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

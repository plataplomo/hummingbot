"""API error exception class for exchange API operations."""

from typing import Any

from .api_error_codes import APIErrorCode
from .api_error_response import APIErrorResponse


# Server error range constants (avoiding circular import with core.models.enums)
SERVER_ERROR_START = 500  # Internal Server Error
SERVER_ERROR_END = 600  # End of 5xx range (exclusive)


class APIError(Exception):
    """Custom exception for API-related errors with enhanced context information.

    Enables better error handling and recovery mechanisms. Now Pydantic-compatible.
    Stores an APIErrorResponse as its model.
    """

    def __init__(
        self,
        message: str,
        code: int | str,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize an APIError with comprehensive error context.

        Args:
            message: Human-readable error description
            code: Error code (internal or exchange-specific)
            http_status: HTTP status code from the API response (optional)
            exchange_code: Exchange-specific error code (optional)
            exchange_message: Exchange-specific error message (optional)
            retry_after: Suggested retry delay in seconds (optional)
            metadata: Additional error context data (optional)
            original_exception: The underlying exception that caused this error (optional)

        """
        self.model = APIErrorResponse(
            message=message,
            code=code,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata=metadata,
            original_exception=original_exception,
        )
        super().__init__(self.model.message)

    @property
    def message(self) -> str:
        """Get the human-readable error message."""
        return self.model.message

    @property
    def code(self) -> int | str:
        """Get the error code (internal or exchange-specific)."""
        return self.model.code

    @property
    def http_status(self) -> int | None:
        """Get the HTTP status code from the API response, if available."""
        return self.model.http_status

    @property
    def exchange_code(self) -> str | int | None:
        """Get the exchange-specific error code, if provided."""
        return self.model.exchange_code

    @property
    def exchange_message(self) -> str | None:
        """Get the exchange-specific error message, if provided."""
        return self.model.exchange_message

    @property
    def retry_after(self) -> float | None:
        """Get the suggested retry delay in seconds, if provided."""
        return self.model.retry_after

    @property
    def metadata(self) -> dict[str, Any] | None:
        """Get additional error context metadata, if available."""
        return self.model.metadata

    @property
    def original_exception(self) -> Exception | None:
        """Get the underlying exception that caused this error, if available."""
        return self.model.original_exception

    @property
    def is_retryable(self) -> bool:
        """Determines if this error can be retried based on its nature.

        Rate limits, timeouts and some server errors can be retried.
        """
        # Defensive: handle both int and str code
        code_val = self.code
        if isinstance(code_val, int):
            return (
                code_val
                in {
                    APIErrorCode.RATE_LIMITED.value,  # 109
                    APIErrorCode.TIMEOUT.value,  # 1
                    APIErrorCode.CONNECTION_ERROR.value,  # 0
                }
                or (
                    code_val == APIErrorCode.SERVER_ERROR.value  # 4
                    and self.http_status
                    and SERVER_ERROR_START <= self.http_status < SERVER_ERROR_END  # Server error
                )
                or code_val == APIErrorCode.NETWORK_ISSUE.value  # 2
            )
        return False


class TransformationError(ValueError):
    """Raised when a validated Raw model cannot be transformed to Internal model.

    This exception supports enhanced context information to aid in debugging
    transformation failures in mapper classes.
    """

    def __init__(
        self,
        message: str,
        field_name: str | None = None,
        source_value: object = None,
        source_data: dict[str, Any] | None = None,
        code: str | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize TransformationError with enhanced context.

        Args:
            message: Primary error message
            field_name: Name of the field that failed transformation
            source_value: The value that caused the transformation failure
            source_data: Raw data context where transformation failed
            code: Optional error code for categorization
            original_exception: The underlying exception that caused this error
        """
        super().__init__(message)
        self.field_name = field_name
        self.source_value = source_value
        self.source_data = source_data
        self.code = code
        self.original_exception = original_exception

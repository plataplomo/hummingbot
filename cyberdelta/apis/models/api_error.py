from typing import Any

from .api_error_response import APIErrorResponse


class APIError(Exception):
    """Custom exception for API-related errors with enhanced context information
    to enable better error handling and recovery mechanisms. Now Pydantic-compatible.
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
        return self.model.message

    @property
    def code(self) -> int | str:
        return self.model.code

    @property
    def http_status(self) -> int | None:
        return self.model.http_status

    @property
    def exchange_code(self) -> str | int | None:
        return self.model.exchange_code

    @property
    def exchange_message(self) -> str | None:
        return self.model.exchange_message

    @property
    def retry_after(self) -> float | None:
        return self.model.retry_after

    @property
    def metadata(self) -> dict[str, Any] | None:
        return self.model.metadata

    @property
    def original_exception(self) -> Exception | None:
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
                in (
                    109,  # RATE_LIMITED
                    1,  # TIMEOUT
                    0,  # CONNECTION_ERROR
                )
                or (code_val == 4 and self.http_status and 500 <= self.http_status < 600)
                or code_val == 2  # NETWORK_ISSUE
            )
        return False


class TransformationError(ValueError):
    """Raised when a validated Raw model cannot be transformed to Internal model."""

    pass

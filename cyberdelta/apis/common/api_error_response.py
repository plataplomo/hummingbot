"""Standardized API error response model for exchange operations."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class APIErrorResponse(BaseModel):
    """Standardized error response model for API errors in CyberDeltaEngine.

    This model is used to validate, normalize, and transport error information from any exchange
    (e.g., Backpack, Hyperliquid) into a consistent internal format for business logic, logging,
    and user-facing error handling.

    Fields:
        message: Human-readable error message (from exchange or mapped internally).
        code: Canonical error code (int, typically from APIErrorCode enum; may be str for raw
            exchange codes).
        http_status: HTTP status code if available (e.g., 400, 404, 500).
        exchange_code: Raw error code from the exchange, if present (str or int).
        exchange_message: Raw error message from the exchange, if present.
        retry_after: If present, indicates how many seconds to wait before retrying
            (for rate limits, etc.).
        metadata: Optional dict for additional diagnostic or context info
            (extensible for future use).
        original_exception: The original exception, if chained (optional).

    Usage:
        - Use this model as the single source of truth for error handling in business logic.
        - Construct via the classmethod 'from_exchange_error' for robust validation and mapping.
        - All error mapping logic should produce or consume this model.
    """

    message: str = Field(..., description="Human-readable error message.")
    code: int | str = Field(
        ...,
        description="Canonical error code (int, or raw exchange code as str).",
    )
    http_status: int | None = Field(None, description="HTTP status code, if available.")
    exchange_code: str | int | None = Field(
        None,
        description="Raw error code from the exchange, if present.",
    )
    exchange_message: str | None = Field(
        None,
        description="Raw error message from the exchange, if present.",
    )
    retry_after: float | None = Field(
        None,
        description="Seconds to wait before retrying (for rate limits, etc.).",
    )
    metadata: dict[str, Any] | None = Field(None, description="Additional context or diagnostics.")
    original_exception: Exception | None = Field(
        None,
        description="Original exception, if chained.",
    )

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    @field_validator("code", mode="before")
    @classmethod
    def validate_code(cls, raw_code: str | float | None) -> int | str:
        """Ensure 'code' is an int if possible, otherwise leave as str.

        Args:
            raw_code: The raw code value from the exchange or mapping logic. Accepts str, int,
                float, or None.

        Returns:
            int or str: The normalized code value. If input is None, returns 'UNKNOWN'.

        """
        if raw_code is None:
            return "UNKNOWN"
        if isinstance(raw_code, int):
            return raw_code
        if isinstance(raw_code, float):
            # Accept floats but convert to int if possible
            if raw_code.is_integer():
                return int(raw_code)
            return str(raw_code)
        try:
            return int(raw_code)
        except (ValueError, TypeError):
            return str(raw_code)

    @classmethod
    def from_exchange_error(
        cls,
        *,
        message: str,
        code: int | str,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> APIErrorResponse:
        """Construct an APIErrorResponse from raw exchange error data.

        Performs validation and normalization of the error data.
        This is the preferred way to create error responses from mapping logic.

        Args:
            message: Human-readable error message
            code: Canonical error code (int or str)
            http_status: HTTP status code, if available
            exchange_code: Raw error code from the exchange, if present
            exchange_message: Raw error message from the exchange, if present
            retry_after: Seconds to wait before retrying, if applicable
            metadata: Additional context or diagnostics
            original_exception: Original exception, if chained

        Returns:
            APIErrorResponse: Validated and normalized error response model

        """
        return cls(
            message=message,
            code=code,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata=metadata,
            original_exception=original_exception,
        )

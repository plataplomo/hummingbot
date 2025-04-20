"""
Backpack API Error Models
------------------------

Defines strict Pydantic models for validating error responses from the Backpack Exchange API.
These models are used for boundary validation and error normalization, not for internal logic.
"""

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BackpackRawApiError(BaseModel):
    """
    Pydantic model for a raw error response from the Backpack REST or WebSocket API.

    This model mirrors the Backpack OpenAPI error schema exactly, enforcing strict field
    validation (extra fields forbidden). Use this model to validate and parse error payloads
    received from the exchange before mapping to internal error codes or raising exceptions.

    Attributes:
        code (str): Backpack error code (see OpenAPI enum for allowed values).
        message (str): Human-readable error message.
    """

    code: str = Field(..., description="Backpack error code", max_length=64)
    message: str = Field(..., description="Error message", max_length=1024)
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("code", "message", mode="before", check_fields=False)
    @classmethod
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        try:
            if not v.strip():
                raise ValueError("Must be a non-empty string")
            if len(v) > 1024:
                raise ValueError("String value too long (max 1024 chars)")
            v.encode("utf-8", "strict")
        except (AttributeError, TypeError, UnicodeEncodeError) as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

    @field_validator("code", mode="before")
    @classmethod
    def validate_code_enum(cls, v: str | None) -> str | None:
        allowed = {
            "FORBIDDEN",
            "INVALID_CLIENT_REQUEST",
            "INVALID_SIGNATURE",
            "SERVER_ERROR",
            "UNAUTHORIZED",
            "TIMEOUT",
            "TOO_MANY_REQUESTS",
            "RESOURCE_NOT_FOUND",
            "MAINTENANCE",
            "INVALID_QUANTITY",
            "ORDER_LIMIT",
            "INVALID_ORDER",
            "INVALID_PRICE",
            "INVALID_MARKET",
            "INVALID_SOURCE",
            "INSUFFICIENT_FUNDS",
            "INSUFFICIENT_MARGIN",
            "POSITION_LIMIT",
            "ACCOUNT_LIQUIDATING",
            "TRADING_PAUSED",
            "INVALID_ASSET",
            "INVALID_SYMBOL",
            "INVALID_POSITION_ID",
            "BORROW_REQUIRES_LEND_REDEEM",
            "LEND_REQUIRES_BORROW_REPAY",
            "INSUFFICIENT_SUPPLY",
            "BORROW_LIMIT",
            "LEND_LIMIT",
            "MAX_LEVERAGE_REACHED",
            "PRECONDITION_FAILED",
            "NOT_IMPLEMENTED",
        }  # Update as per spec
        if v is None:
            raise ValueError("Invalid error code: None")
        try:
            if v not in allowed:
                raise ValueError(f"Invalid error code: {v}")
        except TypeError as err:
            raise ValueError(f"Invalid error code type: {type(v).__name__}") from err
        return v

"""
Backpack API Error Models
------------------------

Defines strict Pydantic models for validating error responses from the Backpack Exchange API.
These models are used for boundary validation and error normalization, not for internal logic.
"""

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cyberdelta.utils.parsing import validate_enum_field, validate_str_field


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

    @field_validator("code", mode="before", check_fields=False)
    @classmethod
    def validate_code_str(cls, v: object) -> str:
        return validate_str_field(v, field_name="code", max_length=64)

    @field_validator("message", mode="before", check_fields=False)
    @classmethod
    def validate_message_str(cls, v: object) -> str:
        return validate_str_field(v, field_name="message", max_length=1024)

    @field_validator("code", mode="before")
    @classmethod
    def validate_code_enum(cls, v: object) -> str:
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
        }
        return validate_enum_field(v, allowed=allowed, field_name="code")

"""
Backpack API Error Models
------------------------

Defines strict Pydantic models for validating error responses from the Backpack Exchange API.
These models are used for boundary validation of raw error structures and error normalization,
not for internal business logic. Adheres to the Raw Model Policy.

Key models:
    - BackpackRawApiError: Validates the common structure of Backpack API errors,
      including `code` and `message` fields. Enforces known error codes and
      string constraints.
"""

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cyberdelta.utils.parsing import validate_enum_field, validate_str_field


class BackpackRawApiError(BaseModel):
    """
    Pydantic model for a raw error response from the Backpack REST or WebSocket API.

    This model mirrors the Backpack OpenAPI error schema, enforcing strict field validation
    (e.g., types, lengths, allowed enum values for `code`, `extra="forbid"`).
    Use this model to validate and parse raw error payloads received from the exchange before
    mapping them to internal standardized error codes or raising APIError exceptions.

    Attributes:
        code (str): Backpack error code (validated against a known set of enum-like strings).
        message (str): Human-readable error message from the API.
    """

    code: str = Field(..., description="Backpack error code", max_length=64)
    message: str = Field(..., description="Error message", max_length=1024)
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("code", mode="before", check_fields=False)
    @classmethod
    def validate_code_str(cls, v: object) -> str:
        """
        Validates that the raw input `v` for the `code` field is a string and meets
        basic length constraints (max 64 chars). This serves as an initial basic type and
        format check before more specific enum validation.

        Args:
            v (object): The raw input value for the error code.

        Returns:
            str: The validated raw string value for the code.

        Raises:
            TypeError: If `v` is not a string.
            ValueError: If `v` is an empty string or exceeds max length.
        """
        return validate_str_field(v, field_name="code", max_length=64)

    @field_validator("message", mode="before", check_fields=False)
    @classmethod
    def validate_message_str(cls, v: object) -> str:
        """
        Validates that the raw input `v` for the `message` field is a string and meets
        basic length constraints (max 1024 chars). Ensures the message is a valid, non-empty
        string.

        Args:
            v (object): The raw input value for the error message.

        Returns:
            str: The validated raw string value for the message.

        Raises:
            TypeError: If `v` is not a string.
            ValueError: If `v` is an empty string or exceeds max length.
        """
        return validate_str_field(v, field_name="message", max_length=1024)

    @field_validator("code", mode="before")  # Runs after the basic string check for code
    @classmethod
    def validate_code_enum(cls, v: object) -> str:
        """
        Validates that the raw string input `v` for the `code` field is one of the known
        Backpack error code strings. This check ensures the error code is recognized by
        the system, facilitating standardized error handling.

        This validator assumes `v` has already been confirmed to be a string by a preceding
        validator (like `validate_code_str`).

        Args:
            v (object): The raw string input value for the error code.

        Returns:
            str: The validated error code string, confirmed to be one of the allowed values.

        Raises:
            TypeError: If `v` is not a string (should be caught by an earlier validator if any).
            ValueError: If `v` is not one of the recognized Backpack error codes.
        """
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

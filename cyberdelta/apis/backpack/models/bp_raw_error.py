"""Backpack API Error Models.
------------------------

Defines strict Pydantic models for validating error responses from the Backpack Exchange API.
These models are used for boundary validation of raw error structures and error normalization,
not for internal business logic. Adheres to the Raw Model Policy.

Key models:
    - BackpackRawApiError: Validates the common structure of Backpack API errors,
      including `code` and `message` fields. Enforces known error codes and
      string constraints using common raw types.
"""

from pydantic import BaseModel, ConfigDict, Field

from .bp_common_raw_types import (
    RawBpErrorCodeString,
    RawBpNonEmptyStringMax1024,
)


class BackpackRawApiError(BaseModel):
    """Pydantic model for a raw error response from the Backpack REST or WebSocket API.

    This model mirrors the Backpack OpenAPI error schema, enforcing strict field validation
    (e.g., types, lengths, allowed enum values for `code`, `extra="forbid"`).
    Uses common raw types for validation.

    Attributes:
        code (RawBpErrorCodeString): Backpack error code.
        message (RawBpNonEmptyStringMax1024): Human-readable error message from the API.

    """

    code: RawBpErrorCodeString = Field(..., description="Backpack error code")
    message: RawBpNonEmptyStringMax1024 = Field(..., description="Error message")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

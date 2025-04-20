"""
Backpack API Error Models
------------------------

Defines strict Pydantic models for validating error responses from the Backpack Exchange API.
These models are used for boundary validation and error normalization, not for internal logic.
"""

from pydantic import BaseModel, ConfigDict, Field


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

    code: str = Field(..., description="Backpack error code")
    message: str = Field(..., description="Error message")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

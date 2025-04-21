"""
CyberDeltaEngine: Hyperliquid API Raw Models (API Error Group)
-------------------------------------------------------------

This module provides strict Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) error responses. It is a core part of
CyberDeltaEngine's boundary validation layer for error handling.

**Scope & Rationale:**
- Models in this file are used to validate and parse the *external* error response structures
  returned by Hyperliquid's REST and WebSocket APIs.
- All models enforce strict schema validation (`extra="forbid"`), ensuring that any unexpected or
  malformed fields in upstream data are immediately rejected. This is critical for robust, secure,
  and predictable error handling in a financial system.
- These models are the *first step* in the "validate first, then transform" pattern: validate
  external data at the boundary, then map to internal error models or raise exceptions as
  appropriate.

**References:**
- Official Hyperliquid API documentation:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawApiError.model_validate(api_response_dict)
    # ...then transform to internal error model or raise

**Note:**
Do not use these models for internal business logic—use your core models for that.
These are for boundary validation only.
"""

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawApiError(BaseModel):
    """
    Represents a raw error response from the Hyperliquid API (REST or WebSocket).

    This model is used to validate the structure of error responses, which typically include an
    error message string. It is a strict mirror of the upstream API schema and should not be used
    for internal business logic.

    Fields:
        error (str): Error message string returned by the API.
    """

    error: str = Field(..., alias="error")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @staticmethod
    def _validate_error_str(v: object) -> str:
        return validate_str_field(v, field_name="error", max_length=1024)

    _validate_error = field_validator("error", mode="before")(_validate_error_str)

"""CyberDeltaEngine: Hyperliquid API Raw Models (API Error Group).

-------------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw*
structure of all major Hyperliquid Exchange API (REST and WebSocket) error responses.
It is a core part of CyberDeltaEngine's boundary validation layer for error handling.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* error
  response structures returned by Hyperliquid's REST and WebSocket APIs.
- All models enforce strict schema validation (`extra="forbid"`), strict type checking,
  and robust format validation (e.g., max length, valid UTF-8).
- Any unexpected, malformed, or ambiguous fields in upstream data are immediately
  rejected. This is critical for robust, secure, and predictable error handling in a
  financial system.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal error models or raise
  exceptions as appropriate.
- **Never use these models for internal business logic.**

**References:**
- Official Hyperliquid API documentation:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawApiError.model_validate(api_response_dict)
    # ...then transform to internal error model or raise
"""

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import RawApiErrorStringHL


class HyperliquidRawApiError(BaseModel):
    """Strict boundary model for a raw error response from the Hyperliquid API (REST or WebSocket).

    This model validates the structure and content of error responses, enforcing strict type
    and format constraints for all fields. Never use for internal business logic.

    Fields:
        error (str): Error message string returned by the API (max length 1024).
    """

    error: RawApiErrorStringHL = Field(..., alias="error")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

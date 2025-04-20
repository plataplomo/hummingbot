"""
CyberDeltaEngine: Hyperliquid API Raw Models (API Error Group)
-------------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) error responses.

- All models are defined locally in this file to avoid cross-file imports between model files.
- Each `HyperliquidRaw*` model mirrors the official Hyperliquid OpenAPI spec, SDK,
  or WebSocket event payloads as closely as possible.
- All fields use `Field(..., alias=...)` to match the exact key names in Hyperliquid's JSON.
- All models use `extra=\"forbid\"` to ensure strict schema validation—any unexpected field
  will raise a validation error.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal models with type conversions
  and business logic.
- See the Hyperliquid OpenAPI spec, SDK, and docs for field details and allowed values.

**Authoritative Reference:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

Usage:
    raw = HyperliquidRawApiError.model_validate(api_response_dict)
    # ...then transform to internal error model

Do not use these models for internal business logic—use your core models for that.
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawApiError(BaseModel):
    """
    Raw error response from Hyperliquid API.
    Fields:
        error: Error message string
    Strictly validated (extra fields forbidden).
    """

    error: str = Field(..., alias="error")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

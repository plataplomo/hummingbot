"""
CyberDeltaEngine: Hyperliquid API Raw Models
-------------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses.

- Each `HyperliquidRaw*` model mirrors the official Hyperliquid OpenAPI spec, SDK,
  or WebSocket event payloads as closely as possible.
- All fields use `Field(..., alias=...)` to match the exact key names in Hyperliquid's JSON.
- Timestamp fields are typed as `int | str | float | None` to accept ISO8601 strings, epoch
  ms/µs/seconds, or null, per the spec.
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
    raw = HyperliquidRawOrder.model_validate(api_response_dict)
    # ...then transform to internal Order model

Do not use these models for internal business logic—use your core models for that.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawExchangeStatusObject(BaseModel):
    """
    Status object for order/cancel/modify responses.
    Fields:
        resting: Resting order (dict or None)
        filled: Filled order (dict or None)
        error: Error message (str or None)
    """

    resting: dict[str, Any] | None = Field(None, alias="resting")
    filled: dict[str, Any] | None = Field(None, alias="filled")
    error: str | None = Field(None, alias="error")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawExchangeResponseData(BaseModel):
    """
    Structure within the 'data' field of a successful exchange action.
    Fields:
        type: Type of response (str)
        statuses: List of status objects or strings
    """

    type: str = Field(..., alias="type")
    statuses: list[str | HyperliquidRawExchangeStatusObject] = Field(..., alias="statuses")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawExchangeActionResponse(BaseModel):
    """
    Top-level response for exchange actions.
    Fields:
        status: Status string (should be 'ok')
        data: Exchange response data (HyperliquidRawExchangeResponseData)
    """

    status: str = Field(..., alias="status")
    data: HyperliquidRawExchangeResponseData = Field(..., alias="data")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

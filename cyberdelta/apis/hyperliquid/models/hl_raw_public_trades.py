"""
CyberDeltaEngine: Hyperliquid API Raw Models (Public Trades Group)
-----------------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to public trades.

- All models are defined locally in this file to avoid cross-file imports between model files.
- Each `HyperliquidRaw*` model mirrors the official Hyperliquid OpenAPI spec, SDK,
  or WebSocket event payloads as closely as possible.
- All fields use `Field(..., alias=...)` to match the exact key names in Hyperliquid's JSON.
- Timestamp fields are typed as `int | str | float | None` to accept ISO8601 strings, epoch
  ms/µs/seconds, or null, per the spec.
- All models use `extra="forbid"` to ensure strict schema validation—any unexpected field
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
    raw = HyperliquidRawPublicTrade.model_validate(api_response_dict)
    # ...then transform to internal trade model

Do not use these models for internal business logic—use your core models for that.
"""

from pydantic import BaseModel, ConfigDict, Field


# --- Core Public Trade Model ---
class HyperliquidRawPublicTrade(BaseModel):
    """
    Public trade object from recent trades.
    Fields:
        coin: Asset symbol (str)
        side: Side ('B' or 'A')
        px: Price (str)
        sz: Size (str)
        time: Timestamp (int)
        hash: Trade hash (str)
    """

    coin: str = Field(..., alias="coin")
    side: str = Field(..., alias="side")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    time: int = Field(..., alias="time")
    hash: str = Field(..., alias="hash")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Batch/Array Response ---
class HyperliquidRawRecentTradesResponse(BaseModel):
    """
    Array of public trades from recentTrades response.
    Fields:
        __root__: List of HyperliquidRawPublicTrade
    """

    __root__: list[HyperliquidRawPublicTrade]
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Request Payload ---
class HyperliquidRawRecentTradesRequestPayload(BaseModel):
    """
    Request payload for 'recentTrades' info type.
    Fields:
        type: Must be 'recentTrades'
        coin: Asset symbol (str)
    """

    type: str = Field("recentTrades", alias="type")
    coin: str = Field(..., alias="coin")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

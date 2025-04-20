"""
CyberDeltaEngine: Hyperliquid API Raw Models (Order Book Group)
--------------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to the L2 order book.

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
    raw = HyperliquidRawL2Book.model_validate(api_response_dict)
    # ...then transform to internal order book model

Do not use these models for internal business logic—use your core models for that.
"""

from pydantic import BaseModel, ConfigDict, Field


# --- Price Level Submodel ---
class HyperliquidRawBookLevel(BaseModel):
    """
    A single price level in the order book.
    Fields:
        px: Price (str)
        sz: Size (str)
        n: Number of orders (int)
    """

    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    n: int = Field(..., alias="n")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- L2 Order Book Model ---
class HyperliquidRawL2Book(BaseModel):
    """
    L2 Order Book snapshot.
    Fields:
        coin: Asset symbol (str)
        levels: [bids, asks] (list[list[HyperliquidRawBookLevel]])
        time: Snapshot timestamp (int)
    """

    coin: str = Field(..., alias="coin")
    levels: list[list[HyperliquidRawBookLevel]] = Field(..., alias="levels")
    time: int = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Request Payload ---
class HyperliquidRawL2BookRequestPayload(BaseModel):
    """
    Request payload for 'l2Book' info type.
    Fields:
        type: Must be 'l2Book'
        coin: Asset symbol (str)
    """

    type: str = Field("l2Book", alias="type")
    coin: str = Field(..., alias="coin")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

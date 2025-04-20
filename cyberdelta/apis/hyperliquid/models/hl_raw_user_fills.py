"""
CyberDeltaEngine: Hyperliquid API Raw Models (User Fills Group)
--------------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to user fills.

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
    raw = HyperliquidRawUserFill.model_validate(api_response_dict)
    # ...then transform to internal fill model

Do not use these models for internal business logic—use your core models for that.
"""

from pydantic import BaseModel, ConfigDict, Field


# --- Core User Fill Model ---
class HyperliquidRawUserFill(BaseModel):
    """
    User fill/trade details from userFills response.
    Fields:
        tid: Trade ID (int)
        coin: Asset symbol (str)
        px: Price (str)
        sz: Size (str)
        time: Timestamp (int)
        side: Side ('B' or 'A')
        oid: Order ID (int)
        start_position: Start position (str)
        dir: Direction (str)
        hash: Trade hash (str)
        fee: Fee (str)
        is_maker: Is maker (bool)
        liquidation_mark_px: Liquidation mark price (str | None)
        cloid: Client order ID (str | None)
    """

    tid: int = Field(..., alias="tid")
    coin: str = Field(..., alias="coin")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    time: int = Field(..., alias="time")
    side: str = Field(..., alias="side")
    oid: int = Field(..., alias="oid")
    start_position: str = Field(..., alias="startPosition")
    dir: str = Field(..., alias="dir")
    hash: str = Field(..., alias="hash")
    fee: str = Field(..., alias="fee")
    is_maker: bool = Field(..., alias="isMaker")
    liquidation_mark_px: str | None = Field(None, alias="liquidationMarkPx")
    cloid: str | None = Field(None, alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Batch/Array Response ---
class HyperliquidRawUserFillsResponse(BaseModel):
    """
    Array of user fills from userFills response.
    Fields:
        __root__: List of HyperliquidRawUserFill
    """

    __root__: list[HyperliquidRawUserFill]
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Request Payload ---
class HyperliquidRawUserFillsRequestPayload(BaseModel):
    """
    Request payload for 'userFills' info type.
    Fields:
        type: Must be 'userFills'
        user: Wallet address (str)
    """

    type: str = Field("userFills", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

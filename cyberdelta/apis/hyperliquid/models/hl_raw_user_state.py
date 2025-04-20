"""
CyberDeltaEngine: Hyperliquid API Raw Models (User State Group)
--------------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to user state.

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
    raw = HyperliquidRawClearinghouseState.model_validate(api_response_dict)
    # ...then transform to internal user state model

Do not use these models for internal business logic—use your core models for that.
"""

from pydantic import BaseModel, ConfigDict, Field


# --- Leverage Submodel ---
class HyperliquidRawLeverage(BaseModel):
    """
    Leverage settings for a position.
    Fields:
        type: Leverage type ('cross' or 'isolated')
        value: Leverage value (int)
    """

    type: str = Field(..., alias="type")
    value: int = Field(..., alias="value")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Position Info Submodel ---
class HyperliquidRawPositionInfo(BaseModel):
    """
    Detailed info about a user position.
    Fields:
        coin: Asset symbol (str)
        entry_px: Entry price (str | None)
        leverage: Leverage settings (HyperliquidRawLeverage)
        liquidation_px: Liquidation price (str | None)
        margin_used: Margin used (str)
        max_leverage: Max leverage (int)
        position_value: Position value (str)
        return_on_equity: ROE (str)
        szi: Size (str)
        unrealized_pnl: Unrealized PnL (str)
    """

    coin: str = Field(..., alias="coin")
    entry_px: str | None = Field(None, alias="entryPx")
    leverage: HyperliquidRawLeverage = Field(..., alias="leverage")
    liquidation_px: str | None = Field(None, alias="liquidationPx")
    margin_used: str = Field(..., alias="marginUsed")
    max_leverage: int = Field(..., alias="maxLeverage")
    position_value: str = Field(..., alias="positionValue")
    return_on_equity: str = Field(..., alias="returnOnEquity")
    szi: str = Field(..., alias="szi")
    unrealized_pnl: str = Field(..., alias="unrealizedPnl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Asset Position Submodel ---
class HyperliquidRawAssetPosition(BaseModel):
    """
    User's position details for a specific asset.
    Fields:
        asset: Asset symbol (str)
        position: Position info (HyperliquidRawPositionInfo)
    """

    asset: str = Field(..., alias="asset")
    position: HyperliquidRawPositionInfo = Field(..., alias="position")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Margin Summary Submodel ---
class HyperliquidRawMarginSummary(BaseModel):
    """
    Margin summary for user state.
    Fields:
        account_value: Account value (str)
        total_margin_used: Total margin used (str)
        total_ntl_pos: Total notional position (str)
        total_raw_usd: Total raw USD (str)
    """

    account_value: str = Field(..., alias="accountValue")
    total_margin_used: str = Field(..., alias="totalMarginUsed")
    total_ntl_pos: str = Field(..., alias="totalNtlPos")
    total_raw_usd: str = Field(..., alias="totalRawUsd")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Clearinghouse State Model ---
class HyperliquidRawClearinghouseState(BaseModel):
    """
    User state including positions and margin.
    Fields:
        asset_positions: List of asset positions (list[HyperliquidRawAssetPosition])
        margin_summary: Margin summary (HyperliquidRawMarginSummary)
        cross_maintenance_margin_used: Cross maintenance margin used (str)
        cross_margin_summary: Cross margin summary (HyperliquidRawMarginSummary)
        isolated_maintenance_margin_used: Isolated maintenance margin used (str)
        isolated_margin_summary: Isolated margin summary (HyperliquidRawMarginSummary)
        withdrawable: Withdrawable amount (str)
    """

    asset_positions: list[HyperliquidRawAssetPosition] = Field(..., alias="assetPositions")
    margin_summary: HyperliquidRawMarginSummary = Field(..., alias="marginSummary")
    cross_maintenance_margin_used: str = Field(..., alias="crossMaintenanceMarginUsed")
    cross_margin_summary: HyperliquidRawMarginSummary = Field(..., alias="crossMarginSummary")
    isolated_maintenance_margin_used: str = Field(..., alias="isolatedMaintenanceMarginUsed")
    isolated_margin_summary: HyperliquidRawMarginSummary = Field(..., alias="isolatedMarginSummary")
    withdrawable: str = Field(..., alias="withdrawable")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Request Payload ---
class HyperliquidRawUserStateRequestPayload(BaseModel):
    """
    Request payload for 'clearinghouseState' info type.
    Fields:
        type: Must be 'clearinghouseState'
        user: Wallet address (str)
    """

    type: str = Field("clearinghouseState", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

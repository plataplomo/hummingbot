"""CyberDeltaEngine: Hyperliquid API Raw Models (User State Group).

--------------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw*
structure of all major Hyperliquid Exchange API (REST and WebSocket) responses related to
user state. It is a core part of CyberDeltaEngine's boundary validation layer for user
account, margin, and position data.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data
  structures returned by Hyperliquid's user state endpoints, including leverage,
  position info, margin summary, and clearinghouse state.
- All models enforce strict schema validation (`extra="forbid"`), strict type checking,
  and robust format validation (e.g., max length, finite decimals, valid UTF-8).
- Any unexpected, malformed, or ambiguous fields in upstream data are immediately
  rejected. This is critical for robust, secure, and predictable operation in a
  financial system.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal business models with
  type conversions and business logic.
- **Never use these models for internal business logic.**

**References:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawClearinghouseState.model_validate(api_response_dict)
    # ...then transform to internal user state model
"""

from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawAssetString64HL,
    RawFiniteDecimalStr,
    RawLaxEthereumAddressStrHL,
    RawLeverageTypeString,
    RawNonNegativeFiniteDecimalStr,
    RawNonNegativeInt,
)
from cyberdelta.utils.parsing import validate_str_field


# --- Leverage Submodel ---
class HyperliquidRawLeverage(BaseModel):
    """Strict boundary model for leverage settings as returned in user state endpoints.

    Validates leverage configuration data from Hyperliquid's user state API responses.
    Validation handled by Annotated types for type safety and format compliance.
    """

    type: RawLeverageTypeString = Field(..., alias="type")
    value: RawNonNegativeInt = Field(..., alias="value")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Position Info Submodel ---
class HyperliquidRawPositionInfo(BaseModel):
    """Strict boundary model for detailed user position information.

    Validates comprehensive position data including entry price, leverage settings,
    liquidation price, margin usage, and unrealized PnL from Hyperliquid API responses.
    Validation handled by Annotated types for financial precision and data integrity.
    """

    coin: RawAssetString64HL = Field(..., alias="coin")
    entry_px: RawFiniteDecimalStr | None = Field(None, alias="entryPx")
    leverage: HyperliquidRawLeverage = Field(..., alias="leverage")
    liquidation_px: RawFiniteDecimalStr | None = Field(None, alias="liquidationPx")
    margin_used: RawNonNegativeFiniteDecimalStr = Field(..., alias="marginUsed")
    max_leverage: RawNonNegativeInt = Field(..., alias="maxLeverage")
    position_value: RawFiniteDecimalStr = Field(..., alias="positionValue")
    return_on_equity: RawFiniteDecimalStr = Field(..., alias="returnOnEquity")
    szi: RawFiniteDecimalStr = Field(..., alias="szi")
    unrealized_pnl: RawFiniteDecimalStr = Field(..., alias="unrealizedPnl")
    cum_funding: dict[str, RawFiniteDecimalStr] | None = Field(None, alias="cumFunding")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Asset Position Submodel ---
class HyperliquidRawAssetPosition(BaseModel):
    """Strict boundary model for a user's position details for a specific asset.

    Combines asset identifier with detailed position information for comprehensive
    position tracking. Validation handled by Annotated types and nested models.
    Updated to handle API format changes where asset identifier is in position.coin
    and position type is provided at the asset position level.
    """

    asset: RawAssetString64HL | None = Field(None, alias="asset")
    position: HyperliquidRawPositionInfo = Field(..., alias="position")
    type: str | None = Field(None, alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Margin Summary Submodel ---
class HyperliquidRawMarginSummary(BaseModel):
    """Strict boundary model for a margin summary as returned in user state endpoints.

    Validates margin-related financial data including account value, margin usage,
    and position values from Hyperliquid's margin calculation endpoints.
    Validation handled by Annotated types for financial precision.
    """

    account_value: RawFiniteDecimalStr = Field(..., alias="accountValue")
    total_margin_used: RawFiniteDecimalStr = Field(..., alias="totalMarginUsed")
    total_ntl_pos: RawFiniteDecimalStr = Field(..., alias="totalNtlPos")
    total_raw_usd: RawFiniteDecimalStr = Field(..., alias="totalRawUsd")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Clearinghouse State Model ---
class HyperliquidRawClearinghouseState(BaseModel):
    """Represents the user's clearinghouse state.

    Comprehensive model for the complete user state including all asset positions,
    margin summaries for both cross and isolated margin modes, and withdrawable funds.
    Validation handled by Annotated types and nested models for data integrity.
    """

    asset_positions: list[HyperliquidRawAssetPosition] = Field(..., alias="assetPositions")
    margin_summary: HyperliquidRawMarginSummary = Field(..., alias="marginSummary")
    cross_maintenance_margin_used: RawNonNegativeFiniteDecimalStr = Field(
        ...,
        alias="crossMaintenanceMarginUsed",
    )
    cross_margin_summary: HyperliquidRawMarginSummary = Field(..., alias="crossMarginSummary")
    isolated_maintenance_margin_used: RawNonNegativeFiniteDecimalStr | None = Field(
        None,
        alias="isolatedMaintenanceMarginUsed",
    )
    isolated_margin_summary: HyperliquidRawMarginSummary | None = Field(
        None,
        alias="isolatedMarginSummary",
    )
    withdrawable: RawNonNegativeFiniteDecimalStr = Field(..., alias="withdrawable")
    time: RawNonNegativeInt | None = Field(None, alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- User State Request Payload ---
class HyperliquidRawUserStateRequestPayload(BaseModel):
    """Represents the request payload for the 'clearinghouseState' info type."""

    type: Annotated[
        Literal["clearinghouseState"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=32, allow_empty=False)),
    ] = Field("clearinghouseState", alias="type")
    user: RawLaxEthereumAddressStrHL = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

"""
CyberDeltaEngine: Hyperliquid API Raw Models (User State Group)
--------------------------------------------------------------

This module provides strict Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to user state.
It is a core part of CyberDeltaEngine's boundary validation layer for user account, margin, and
position data.

**Scope & Rationale:**
- Models in this file are used to validate and parse the *external* data structures returned by
  Hyperliquid's user state endpoints, including leverage, position info, margin summary, and
  clearinghouse state.
- All models enforce strict schema validation (`extra="forbid"`), ensuring that any unexpected or
  malformed fields in upstream data are immediately rejected. This is critical for robust, secure,
  and predictable operation in a financial system.
- These models are the *first step* in the "validate first, then transform" pattern: validate
  external data at the boundary, then map to internal business models with type conversions and
  business logic.

**References:**
- Official Hyperliquid API documentation:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawClearinghouseState.model_validate(api_response_dict)
    # ...then transform to internal user state model

**Note:**
Do not use these models for internal business logic—use your core models for that.
These are for boundary validation only.
"""

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


# --- Leverage Submodel ---
class HyperliquidRawLeverage(BaseModel):
    """
    Represents leverage settings for a position as returned in user state endpoints.

    This model is used as a submodel in position and clearinghouse state responses to describe
    the leverage type and value.

    Fields:
        type (str): Leverage type ('cross' or 'isolated').
        value (int): Leverage value.
    """

    type: str = Field(..., alias="type")
    value: int = Field(..., alias="value")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("type", mode="before")
    @classmethod
    def validate_type(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="type", max_length=16)


# --- Position Info Submodel ---
class HyperliquidRawPositionInfo(BaseModel):
    """
    Represents detailed information about a user position as returned in user state endpoints.

    This model is used as a submodel in asset position and clearinghouse state responses to
    describe the user's position for a given asset.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        entry_px (Optional[str]): Entry price, if present.
        leverage (HyperliquidRawLeverage): Leverage settings for this position.
        liquidation_px (Optional[str]): Liquidation price, if present.
        margin_used (str): Margin used for this position.
        max_leverage (int): Maximum leverage allowed for this asset.
        position_value (str): Value of the position.
        return_on_equity (str): Return on equity (ROE) for this position.
        szi (str): Size of the position.
        unrealized_pnl (str): Unrealized profit and loss for this position.
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

    @field_validator("coin", mode="before")
    @classmethod
    def validate_coin(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="coin", max_length=64)

    @field_validator("entry_px", "liquidation_px", mode="before")
    @classmethod
    def validate_optional_decimal_str(cls, v: object, info: ValidationInfo) -> str | None:
        if v is None:
            return v
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator(
        "margin_used", "position_value", "return_on_equity", "szi", "unrealized_pnl", mode="before"
    )
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s


# --- Asset Position Submodel ---
class HyperliquidRawAssetPosition(BaseModel):
    """
    Represents a user's position details for a specific asset as returned in user state endpoints.

    This model is used as a submodel in clearinghouse state responses to describe the user's
    position for a given asset.

    Fields:
        asset (str): Asset symbol (e.g., 'ETH', 'BTC').
        position (HyperliquidRawPositionInfo): Detailed position information for this asset.
    """

    asset: str = Field(..., alias="asset")
    position: HyperliquidRawPositionInfo = Field(..., alias="position")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("asset", mode="before")
    @classmethod
    def validate_asset(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="asset", max_length=64)


# --- Margin Summary Submodel ---
class HyperliquidRawMarginSummary(BaseModel):
    """
    Represents a margin summary for user state as returned in user state endpoints.

    This model is used as a submodel in clearinghouse state responses to describe margin usage
    and account value.

    Fields:
        account_value (str): Account value for the user.
        total_margin_used (str): Total margin used across all positions.
        total_ntl_pos (str): Total notional position size.
        total_raw_usd (str): Total raw USD value.
    """

    account_value: str = Field(..., alias="accountValue")
    total_margin_used: str = Field(..., alias="totalMarginUsed")
    total_ntl_pos: str = Field(..., alias="totalNtlPos")
    total_raw_usd: str = Field(..., alias="totalRawUsd")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator(
        "account_value", "total_margin_used", "total_ntl_pos", "total_raw_usd", mode="before"
    )
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s


# --- Clearinghouse State Model ---
class HyperliquidRawClearinghouseState(BaseModel):
    """
    Represents the user's clearinghouse state, including positions and margin, as returned in
    user state endpoints.

    This model is used to validate the structure of the clearinghouse state response, which
    includes asset positions, margin summaries, and withdrawable amounts.

    Fields:
        asset_positions (List[HyperliquidRawAssetPosition]): List of asset positions for the user.
        margin_summary (HyperliquidRawMarginSummary): Margin summary for the user.
        cross_maintenance_margin_used (str): Cross maintenance margin used.
        cross_margin_summary (HyperliquidRawMarginSummary): Cross margin summary for the user.
        isolated_maintenance_margin_used (str): Isolated maintenance margin used.
        isolated_margin_summary (HyperliquidRawMarginSummary): Isolated margin summary for the user.
        withdrawable (str): Amount withdrawable by the user.
    """

    asset_positions: list[HyperliquidRawAssetPosition] = Field(..., alias="assetPositions")
    margin_summary: HyperliquidRawMarginSummary = Field(..., alias="marginSummary")
    cross_maintenance_margin_used: str = Field(..., alias="crossMaintenanceMarginUsed")
    cross_margin_summary: HyperliquidRawMarginSummary = Field(..., alias="crossMarginSummary")
    isolated_maintenance_margin_used: str = Field(..., alias="isolatedMaintenanceMarginUsed")
    isolated_margin_summary: HyperliquidRawMarginSummary = Field(..., alias="isolatedMarginSummary")
    withdrawable: str = Field(..., alias="withdrawable")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator(
        "cross_maintenance_margin_used",
        "isolated_maintenance_margin_used",
        "withdrawable",
        mode="before",
    )
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s


# --- Request Payload ---
class HyperliquidRawUserStateRequestPayload(BaseModel):
    """
    Represents the request payload for the 'clearinghouseState' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    requesting the user's clearinghouse state.

    Fields:
        type (str): Must be 'clearinghouseState'.
        user (str): Wallet address of the user.
    """

    type: str = Field("clearinghouseState", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("user", mode="before")
    @classmethod
    def validate_user(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="user", max_length=64)

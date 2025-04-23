"""
CyberDeltaEngine: Hyperliquid API Raw Models (Open Orders Group)
---------------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to open orders, order specs,
order types, triggers, modification/cancellation, and exchange action/response.

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
    raw = HyperliquidRawOrder.model_validate(api_response_dict)
    # ...then transform to internal Order model

Do not use these models for internal business logic—use your core models for that.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, RootModel, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_enum_field, validate_str_field


# --- Trigger Info/Spec ---
class HyperliquidRawTriggerInfo(BaseModel):
    """
    Trigger details if present (for conditional orders).
    Fields:
        trigger_px: Trigger price (str)
        is_market: Is market order (bool)
        tpsl: Trigger type ('tp' or 'sl')
    """

    trigger_px: str = Field(..., alias="triggerPx")
    is_market: bool = Field(..., alias="isMarket")
    tpsl: str = Field(..., alias="tpsl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("trigger_px", mode="before")
    @classmethod
    def validate_trigger_px(cls, v: object, info: ValidationInfo) -> str:
        s = validate_str_field(v, field_name="trigger_px", max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name="trigger_px")
        if d is None or not d.is_finite():
            raise ValueError("trigger_px: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("tpsl", mode="before")
    @classmethod
    def validate_tpsl(cls, v: object, info: ValidationInfo) -> str:
        return validate_enum_field(v, allowed={"tp", "sl"}, field_name="tpsl")

    @field_validator("is_market", mode="before")
    @classmethod
    def validate_is_market_bool(cls, v: object, info: ValidationInfo) -> bool:
        """
        Strictly enforce that is_market is a bool (no coercion). This is required by the raw model policy.
        """
        if not isinstance(v, bool):
            raise ValueError(f"is_market: Expected bool, got {type(v).__name__}")
        return v


class HyperliquidRawTriggerSpec(BaseModel):
    """
    Trigger spec for conditional orders.
    Fields:
        trigger_px: Trigger price (str)
        is_market: Is market order (bool)
        tpsl: Trigger type ('tp' or 'sl')
    """

    trigger_px: str = Field(..., alias="triggerPx")
    is_market: bool = Field(..., alias="isMarket")
    tpsl: str = Field(..., alias="tpsl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Time-in-Force for Limit Orders ---
class HyperliquidRawTifLimit(BaseModel):
    """
    Time-in-force for limit orders.
    Fields:
        tif: Time in force (str: 'Gtc', 'Ioc', 'Alo')
    """

    tif: str = Field(..., alias="tif")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("tif", mode="before")
    @classmethod
    def validate_tif(cls, v: object, info: ValidationInfo) -> str:
        return validate_enum_field(v, allowed={"Gtc", "Ioc", "Alo"}, field_name="tif")


# --- Order Types ---
class HyperliquidRawOrderTypeLimit(BaseModel):
    """
    Limit order type for orderType field.
    Fields:
        limit: HyperliquidRawTifLimit
    """

    limit: HyperliquidRawTifLimit = Field(..., alias="limit")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawOrderTypeMarket(BaseModel):
    """
    Market order type for orderType field.
    Fields:
        market: dict (empty object)
    """

    market: dict[str, Any] = Field(..., alias="market")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Core Order Models ---
class HyperliquidRawOrder(BaseModel):
    """
    Core order details from open orders or order status.
    Fields:
        oid: Order ID (int)
        cloid: Client order ID (str | None)
        asset: Asset symbol (str)
        side: Side ('B' or 'A')
        limit_px: Limit price (str)
        sz: Size (str)
        timestamp: Creation timestamp (int)
        order_type: Order type (dict[str, Any])
        reduce_only: Reduce-only flag (bool)
        remaining_sz: Remaining size (str)
        status: Status string (e.g., 'open')
        status_timestamp: Last update timestamp (int)
    """

    oid: int = Field(..., alias="oid")
    cloid: str | None = Field(None, alias="cloid")
    asset: str = Field(..., alias="asset")
    side: str = Field(..., alias="side")
    limit_px: str = Field(..., alias="limitPx")
    sz: str = Field(..., alias="sz")
    timestamp: int = Field(..., alias="timestamp")
    order_type: dict[str, Any] = Field(..., alias="orderType")
    reduce_only: bool = Field(..., alias="reduceOnly")
    remaining_sz: str = Field(..., alias="remainingSz")
    status: str = Field(..., alias="status")
    status_timestamp: int = Field(..., alias="statusTimestamp")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("asset", mode="before")
    @classmethod
    def validate_asset(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="asset", max_length=64)

    @field_validator("side", mode="before")
    @classmethod
    def validate_side(cls, v: object, info: ValidationInfo) -> str:
        return validate_enum_field(v, allowed={"B", "A"}, field_name="side")

    @field_validator("limit_px", "sz", "remaining_sz", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("cloid", mode="before")
    @classmethod
    def validate_cloid(cls, v: object, info: ValidationInfo) -> str | None:
        if v is None:
            return v
        return validate_str_field(v, field_name="cloid", max_length=64)

    @field_validator("status", mode="before")
    @classmethod
    def validate_status(cls, v: object, info: ValidationInfo) -> str:
        return validate_enum_field(v, allowed={"open"}, field_name="status")


class HyperliquidRawOpenOrder(BaseModel):
    """
    Structure for one open order (with optional trigger).
    Fields:
        order: Order details (HyperliquidRawOrder)
        trigger: Trigger info (HyperliquidRawTriggerInfo | None)
    """

    order: HyperliquidRawOrder = Field(..., alias="order")
    trigger: HyperliquidRawTriggerInfo | None = Field(None, alias="trigger")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawOpenOrdersResponse(RootModel[list[HyperliquidRawOpenOrder]]):
    """
    Array of open orders from openOrders response.
    Fields:
        __root__: List of HyperliquidRawOpenOrder
    """

    pass


class HyperliquidRawOpenOrdersRequestPayload(BaseModel):
    """
    Request payload for 'openOrders' info type.
    Fields:
        type: Must be 'openOrders'
        user: Wallet address (str)
    """

    type: str = Field("openOrders", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Order Spec (for placement/modify) ---
class HyperliquidRawOrderSpec(BaseModel):
    """
    Order spec for placing an order (exchange action request).
    Fields:
        asset: Asset index (int)
        is_buy: Is buy (bool)
        limit_px: Limit price (str)
        sz: Size (float)
        reduce_only: Reduce-only flag (bool)
        order_type: One of HyperliquidRawOrderTypeLimit or HyperliquidRawOrderTypeMarket
        trigger: Optional trigger spec
        cloid: Optional client order ID (str)
    """

    asset: int = Field(..., alias="asset")
    is_buy: bool = Field(..., alias="isBuy")
    limit_px: str = Field(..., alias="limitPx")
    sz: float = Field(..., alias="sz")
    reduce_only: bool = Field(..., alias="reduceOnly")
    order_type: dict[str, Any] = Field(..., alias="orderType")
    trigger: HyperliquidRawTriggerSpec | None = Field(None, alias="trigger")
    cloid: str | None = Field(None, alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawModifyOrderRequest(BaseModel):
    """
    Modify order request payload.
    Fields:
        oid: Order ID (int)
        order: HyperliquidRawOrderSpec
    """

    oid: int = Field(..., alias="oid")
    order: HyperliquidRawOrderSpec = Field(..., alias="order")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Cancel Requests ---
class HyperliquidRawCancelRequest(BaseModel):
    """
    Cancel request payload (by exchange OID).
    Fields:
        asset: Asset index (int)
        oid: Order ID (int)
    """

    asset: int = Field(..., alias="asset")
    oid: int = Field(..., alias="oid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawCancelByCloidRequest(BaseModel):
    """
    Cancel request payload (by client OID).
    Fields:
        asset: Asset index (int)
        cloid: Client order ID (str)
    """

    asset: int = Field(..., alias="asset")
    cloid: str = Field(..., alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


# --- Exchange Action/Response Models ---
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

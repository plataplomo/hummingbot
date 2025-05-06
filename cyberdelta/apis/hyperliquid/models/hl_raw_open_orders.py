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

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, RootModel, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_enum_field, validate_str_field


# --- Trigger Info/Spec ---
class HyperliquidRawTriggerInfo(BaseModel):
    """
    Strict boundary model for trigger details if present (for conditional orders).

    This model validates the structure and content of trigger information, enforcing strict type and
    format constraints for all fields. Never use for internal business logic.

    Fields:
        trigger_px (str): Trigger price as a decimal string.
        is_market (bool): True if the trigger is for a market order.
        tpsl (str): Trigger type ('tp' for take-profit, 'sl' for stop-loss).
    """

    trigger_px: str = Field(..., alias="triggerPx")
    is_market: bool = Field(..., alias="isMarket")
    tpsl: str = Field(..., alias="tpsl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("trigger_px", mode="before")
    @classmethod
    def validate_trigger_px(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'trigger_px' field to ensure it is a string representing a finite
        decimal (not NaN/inf), with a maximum length of 64.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated trigger price string.
        Raises:
            ValueError: If the input is not a valid decimal string.
        """
        s = validate_str_field(v, field_name="trigger_px", max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name="trigger_px")
        if d is None or not d.is_finite():
            raise ValueError("trigger_px: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("tpsl", mode="before")
    @classmethod
    def validate_tpsl(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'tpsl' field to ensure it is either 'tp' or 'sl'.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated trigger type string.
        Raises:
            ValueError: If the input is not 'tp' or 'sl'.
        """
        return validate_enum_field(v, allowed={"tp", "sl"}, field_name="tpsl")

    @field_validator("is_market", mode="before")
    @classmethod
    def validate_is_market_bool(cls, v: object, info: ValidationInfo) -> bool:
        """
        Strictly enforce that is_market is a bool (no coercion). This is required by the raw
        model policy.

        Args:
            v (object): The value to validate (should be a bool).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            bool: The validated boolean value.
        Raises:
            ValueError: If the input is not a bool.
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
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Time-in-Force for Limit Orders ---
class HyperliquidRawTifLimit(BaseModel):
    """
    Time-in-force for limit orders.
    Fields:
        tif: Time in force (str: 'Gtc', 'Ioc', 'Alo')
    """

    tif: str = Field(..., alias="tif")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

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
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawOrderTypeMarket(BaseModel):
    """
    Market order type for orderType field.
    Fields:
        market: dict (empty object)
    """

    market: dict[str, Any] = Field(..., alias="market")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


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
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

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
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawOpenOrdersResponse(RootModel[list[HyperliquidRawOpenOrder]]):
    """
    Array of open orders from openOrders response.
    Fields:
        __root__: List of HyperliquidRawOpenOrder
    """

    @property
    def items(self) -> list[HyperliquidRawOpenOrder]:
        """
        Returns the validated list of open orders with full type safety.
        This is the preferred way to access the root data in Pydantic v2.
        """
        return self.root

    model_config = ConfigDict(frozen=True)


class HyperliquidRawOpenOrdersRequestPayload(BaseModel):
    """
    Request payload for 'openOrders' info type.
    Fields:
        type: Must be 'openOrders' (Literal['openOrders'])
        user: Wallet address (str)
    """

    type: Literal["openOrders"] = Field("openOrders", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type_literal(cls, v: object, info: ValidationInfo) -> str:
        """Ensures type is exactly 'openOrders'."""
        field_name = info.field_name or "type"
        s = validate_str_field(v, field_name=field_name, max_length=16)
        if s != "openOrders":
            raise ValueError(f"{field_name} must be 'openOrders', got '{s}'")
        return s

    @field_validator("user", mode="before")
    @classmethod
    def validate_user_address(cls, v: object, info: ValidationInfo) -> str:
        """Validates the user address string (e.g., Ethereum address format)."""
        field_name = info.field_name or "user"
        # Validate as a non-empty string with max length typical for addresses.
        # allow_empty=False by default in validate_str_field if not specified.
        s = validate_str_field(v, field_name=field_name, max_length=42)

        # Additional specific checks for Ethereum-like addresses can be added here.
        # For now, ensuring it's 42 characters long and starts with 0x.
        if not s.startswith("0x"):
            raise ValueError(f"{field_name}: Address '{s}' must start with '0x'.")
        if len(s) != 42:
            raise ValueError(
                f"{field_name}: Address '{s}' must be 42 characters long, got {len(s)}."
            )
        # Could add regex for hex characters: ^0x[a-fA-F0-9]{40}$
        return s


# --- Order Spec (for placement/modify) ---
class HyperliquidRawOrderSpec(BaseModel):
    """
    Order spec for placing an order (exchange action request).
    Fields:
        asset: Asset index (int, >=0).
        is_buy: Is buy (bool).
        limit_px: Limit price (str, validated as finite decimal).
        sz: Size (str, validated as positive finite decimal).
        reduce_only: Reduce-only flag (bool).
        order_type: Order type details (dict[str, Any], must not be empty).
        trigger: Optional trigger spec (HyperliquidRawTriggerSpec | None).
        cloid: Optional client order ID (str | None, non-empty if provided).
    """

    asset: int = Field(..., alias="asset", ge=0)
    is_buy: bool = Field(..., alias="isBuy")
    limit_px: str = Field(..., alias="limitPx")
    sz: str = Field(..., alias="sz")
    reduce_only: bool = Field(..., alias="reduceOnly")
    order_type: dict[str, Any] = Field(..., alias="orderType")
    trigger: HyperliquidRawTriggerSpec | None = Field(None, alias="trigger")
    cloid: str | None = Field(None, alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("asset")
    @classmethod
    def _validate_asset_index(cls, value: int) -> int:
        if value < 0:
            raise ValueError("Asset index must be non-negative.")
        return value

    @field_validator("limit_px")
    @classmethod
    def _validate_limit_px_str(cls, value: str) -> str:
        try:
            parsed_val = parse_decimal_value(value, allow_none=False, field_name="limit_px")
            if parsed_val is None:
                raise ValueError("limit_px parsing unexpectedly returned None.")
            if not parsed_val.is_finite():
                raise ValueError("limit_px must represent a finite number.")
        except (ValueError, TypeError) as e:
            raise ValueError(f"limit_px '{value}' is not a valid finite decimal string: {e}") from e
        return value

    @field_validator("sz")
    @classmethod
    def _validate_sz_str(cls, value: str) -> str:
        from decimal import Decimal

        try:
            parsed_val = parse_decimal_value(value, allow_none=False, field_name="sz")
            if parsed_val is None:
                raise ValueError("sz parsing unexpectedly returned None.")
            if not parsed_val.is_finite():
                raise ValueError("sz must represent a finite number.")
            if parsed_val <= Decimal(0):
                raise ValueError("sz must be greater than 0.")
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"sz '{value}' is not a valid positive finite decimal string: {e}"
            ) from e
        return value

    @field_validator("order_type")
    @classmethod
    def _validate_order_type(cls, value: dict[str, Any]) -> dict[str, Any]:
        if not value:
            raise ValueError("order_type dictionary cannot be empty.")
        return value

    @field_validator("trigger", mode="before")
    @classmethod
    def _validate_trigger_details(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is None:
            return None
        if not value:
            raise ValueError("trigger details dictionary cannot be empty if provided.")
        return value

    @field_validator("cloid", mode="before")
    @classmethod
    def _validate_cloid(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.strip():
            raise ValueError("cloid cannot be an empty or whitespace-only string if provided.")
        return value


class HyperliquidRawModifyOrderRequest(BaseModel):
    """
    Modify order request payload.
    Fields:
        oid: Order ID (int)
        order: HyperliquidRawOrderSpec
    """

    oid: int = Field(..., alias="oid")
    order: HyperliquidRawOrderSpec = Field(..., alias="order")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("oid")
    @classmethod
    def _validate_oid(cls, value: int) -> int:
        if value < 0:
            raise ValueError("Order ID (oid) must be non-negative.")
        return value


# --- Cancel Requests ---
class HyperliquidRawCancelRequest(BaseModel):
    """
    Cancel request payload (by exchange OID).
    Fields:
        asset: Asset index (int, >=0).
        oid: Order ID (int, >=0).
    """

    asset: int = Field(..., alias="asset", ge=0)
    oid: int = Field(..., alias="oid", ge=0)
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("asset")
    @classmethod
    def _validate_asset_index(cls, value: int) -> int:
        if value < 0:
            raise ValueError("Asset index must be non-negative.")
        return value

    @field_validator("oid")
    @classmethod
    def _validate_oid(cls, value: int) -> int:
        if value < 0:
            raise ValueError("Order ID (oid) must be non-negative.")
        return value


class HyperliquidRawCancelByCloidRequest(BaseModel):
    """
    Cancel request payload (by client OID).
    Fields:
        asset: Asset index (int)
        cloid: Client order ID (str)
    """

    asset: int = Field(..., alias="asset")
    cloid: str = Field(..., alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Exchange Action/Response Models ---
# These are now canonically defined in hl_raw_exchange_response.py
# Removing definitions from here to avoid duplication.

# class HyperliquidRawExchangeStatusObject(BaseModel): ... (REMOVED)
# class HyperliquidRawExchangeResponseData(BaseModel): ... (REMOVED)
# class HyperliquidRawExchangeActionResponse(BaseModel): ... (REMOVED)


class HyperliquidRawOrderStatusResponse(BaseModel):
    """
    Pydantic model for the response structure from Hyperliquid's /info endpoint
    when querying order status (type='orderStatus').

    Ensures the presence of the 'order' field and that it conforms to the
    HyperliquidRawOrder model. Enforces immutability and forbids extra fields.
    """

    order: HyperliquidRawOrder = Field(..., description="The details of the queried order.")

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )

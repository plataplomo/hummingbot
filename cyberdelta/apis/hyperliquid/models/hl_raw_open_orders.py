"""CyberDeltaEngine: Hyperliquid API Raw Models (Open Orders Group).

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

from typing import Annotated, Literal, cast

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    RootModel,
    ValidationInfo,
    field_validator,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawAssetString64HL,
    RawCloidString64HL,
    RawFiniteDecimalStr,
    RawLaxEthereumAddressStrHL,
    RawNonNegativeFiniteDecimalStr,
    RawNonNegativeInt,
    RawOptionalNonEmptyString64HL,
    RawOrderStatusHL,
    RawPositiveFiniteDecimalStr,
    RawSideStr,
    RawStrictBool,
    RawTifStr,
    RawTimestampMsInt,
    RawTpslStr,
)
from cyberdelta.utils.parsing import validate_str_field


# --- Trigger Info/Spec ---
class HyperliquidRawTriggerInfo(BaseModel):
    """Strict boundary model for trigger details if present (for conditional orders).

    This model validates the structure and content of trigger information, enforcing strict type and
    format constraints for all fields. Never use for internal business logic.

    Fields:
        trigger_px (RawFiniteDecimalStr): Trigger price as a decimal string.
        is_market (RawStrictBool): True if the trigger is for a market order.
        tpsl (RawTpslStr): Trigger type ('tp' for take-profit, 'sl' for stop-loss).
    """

    trigger_px: RawFiniteDecimalStr = Field(..., alias="triggerPx")
    is_market: RawStrictBool = Field(..., alias="isMarket")
    tpsl: RawTpslStr = Field(..., alias="tpsl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawTriggerSpec(BaseModel):
    """Trigger spec for conditional orders.

    Fields:
        trigger_px (RawFiniteDecimalStr): Trigger price (str)
        is_market (RawStrictBool): Is market order (bool)
        tpsl (RawTpslStr): Trigger type ('tp' or 'sl')
    """

    trigger_px: RawFiniteDecimalStr = Field(..., alias="triggerPx")
    is_market: RawStrictBool = Field(..., alias="isMarket")
    tpsl: RawTpslStr = Field(..., alias="tpsl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Time-in-Force for Limit Orders ---
class HyperliquidRawTifLimit(BaseModel):
    """Time-in-force for limit orders.

    Fields:
        tif (RawTifStr): Time in force (str: 'Gtc', 'Ioc', 'Alo')
    """

    tif: RawTifStr = Field(..., alias="tif")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Order Types ---
class HyperliquidRawOrderTypeLimit(BaseModel):
    """Limit order type for orderType field.

    Fields:
        limit: HyperliquidRawTifLimit
    """

    limit: HyperliquidRawTifLimit = Field(..., alias="limit")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawOrderTypeMarket(BaseModel):
    """Market order type for orderType field.

    Fields:
        market: dict (empty object)
    """

    market: dict[str, object] = Field(..., alias="market")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Core Order Models ---
class HyperliquidRawOrder(BaseModel):
    """Core order details from open orders or order status.

    Fields:
        oid (RawNonNegativeInt): Order ID.
        cloid (RawOptionalNonEmptyString64HL | None): Client order ID.
        asset (RawAssetString64HL): Asset symbol.
        side (RawSideStr): Side ('B' or 'A').
        limit_px (RawFiniteDecimalStr): Limit price.
        sz (RawNonNegativeFiniteDecimalStr): Size.
        timestamp (RawTimestampMsInt): Creation timestamp.
        order_type (dict[str, object]): Order type.
        reduce_only (RawStrictBool): Reduce-only flag.
        remaining_sz (RawNonNegativeFiniteDecimalStr): Remaining size.
        status (RawOrderStatusHL): Status string.
        status_timestamp (RawTimestampMsInt): Last update timestamp.
    """

    oid: RawNonNegativeInt = Field(..., alias="oid")
    cloid: RawOptionalNonEmptyString64HL = Field(None, alias="cloid")
    asset: RawAssetString64HL = Field(..., alias="asset")
    side: RawSideStr = Field(..., alias="side")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="sz")
    timestamp: RawTimestampMsInt = Field(..., alias="timestamp")
    order_type: dict[str, object] = Field(..., alias="orderType")
    reduce_only: RawStrictBool = Field(..., alias="reduceOnly")
    remaining_sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="remainingSz")
    status: RawOrderStatusHL = Field(..., alias="status")
    status_timestamp: RawTimestampMsInt = Field(..., alias="statusTimestamp")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawOpenOrder(BaseModel):
    """Structure for one open order (with optional trigger).

    Fields:
        order: Order details (HyperliquidRawOrder)
        trigger: Trigger info (HyperliquidRawTriggerInfo | None)
    """

    order: HyperliquidRawOrder = Field(..., alias="order")
    trigger: HyperliquidRawTriggerInfo | None = Field(None, alias="trigger")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawOpenOrdersResponse(RootModel[list[HyperliquidRawOpenOrder]]):
    """Array of open orders from openOrders response.

    Fields:
        __root__: List of HyperliquidRawOpenOrder
    """

    root: list[HyperliquidRawOpenOrder]

    @property
    def items(self) -> list[HyperliquidRawOpenOrder]:
        """Return the validated list of open orders with full type safety.

        This is the preferred way to access the root data in Pydantic v2.
        """
        return self.root

    model_config = ConfigDict(frozen=True)

    @field_validator("root", mode="before")
    @classmethod
    def validate_open_orders_list(cls, v: object, info: ValidationInfo) -> list[dict[str, object]]:
        """Ensure the root input is a list of dictionaries for open orders."""
        field_name = info.field_name or "open_orders_list"

        if not isinstance(v, list):
            raise ValueError(f"Field '{field_name}': Expected a list, got {type(v).__name__}.")

        # CAST 1: For type checker, v is already confirmed list by runtime check
        list_of_objects = cast(list[object], v)
        # Redundant runtime check, but harmless and good for clarity/assertion
        assert isinstance(list_of_objects, list)

        validated_items: list[dict[str, object]] = []
        for item_idx, item_obj in enumerate(list_of_objects):
            if not isinstance(item_obj, dict):
                item_type = type(item_obj).__name__
                raise ValueError(
                    f"Field '{field_name}', Item {item_idx}: Expected a dictionary, "
                    f"got {item_type}.",
                )

            # CAST 2: For type checker, item_obj is already confirmed dict by runtime check
            item_dict = cast(dict[str, object], item_obj)
            # Redundant runtime check
            assert isinstance(item_dict, dict)

            validated_items.append(item_dict)
        return validated_items


class HyperliquidRawOpenOrdersRequestPayload(BaseModel):
    """Request payload for 'openOrders' info type.

    Fields:
        type: Must be 'openOrders' (Literal['openOrders'])
        user: Wallet address (RawLaxEthereumAddressStrHL)
    """

    type: Annotated[
        Literal["openOrders"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=16, allow_empty=False)),
    ] = Field("openOrders", alias="type")
    user: RawLaxEthereumAddressStrHL = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Order Spec (for placement/modify) ---
class HyperliquidRawOrderSpec(BaseModel):
    """Order spec for placing an order (exchange action request).

    Fields:
        asset: Asset index (RawNonNegativeInt).
        is_buy: Is buy (RawStrictBool).
        limit_px: Limit price (RawFiniteDecimalStr).
        sz: Size (RawPositiveFiniteDecimalStr, must be > 0).
        reduce_only: Reduce-only flag (RawStrictBool).
        order_type: Order type details (dict[str, object], must not be empty).
        trigger: Optional trigger spec (HyperliquidRawTriggerSpec | None).
        cloid: Optional client order ID (RawOptionalNonEmptyString64HL | None).
    """

    asset: RawNonNegativeInt = Field(..., alias="asset")
    is_buy: RawStrictBool = Field(..., alias="isBuy")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    sz: RawPositiveFiniteDecimalStr = Field(..., alias="sz")
    reduce_only: RawStrictBool = Field(..., alias="reduceOnly")
    order_type: dict[str, object] = Field(..., alias="orderType")
    trigger: HyperliquidRawTriggerSpec | None = Field(None, alias="trigger")
    cloid: RawOptionalNonEmptyString64HL = Field(None, alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("order_type")
    @classmethod
    def _validate_order_type_non_empty(cls, value: dict[str, object]) -> dict[str, object]:
        if not value:
            raise ValueError("order_type dictionary cannot be empty.")
        return value

    @field_validator("trigger", mode="before")
    @classmethod
    def _validate_trigger_details_non_empty(cls, value: object) -> dict[str, object] | None:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ValueError("trigger details must be a dictionary if provided.")

        if not value:
            raise ValueError("trigger details dictionary cannot be empty if provided.")

        return cast(dict[str, object], value)


class HyperliquidRawModifyOrderRequest(BaseModel):
    """Modify order request payload.

    Fields:
        oid: Order ID (RawNonNegativeInt)
        order: HyperliquidRawOrderSpec
    """

    oid: RawNonNegativeInt = Field(..., alias="oid")
    order: HyperliquidRawOrderSpec = Field(..., alias="order")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Cancel Requests ---
class HyperliquidRawCancelRequest(BaseModel):
    """Cancel request payload (by exchange OID).

    Fields:
        asset: Asset index (RawNonNegativeInt).
        oid: Order ID (RawNonNegativeInt).
    """

    asset: RawNonNegativeInt = Field(..., alias="asset")
    oid: RawNonNegativeInt = Field(..., alias="oid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawCancelByCloidRequest(BaseModel):
    """Cancel request payload (by client OID).

    Fields:
        asset: Asset index (RawNonNegativeInt)
        cloid: Client order ID (RawCloidString64HL)
    """

    asset: RawNonNegativeInt = Field(..., alias="asset")
    cloid: RawCloidString64HL = Field(..., alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Exchange Action/Response Models ---
# These are now canonically defined in hl_raw_exchange_response.py
# Removing definitions from here to avoid duplication.

# class HyperliquidRawExchangeStatusObject(BaseModel): ... (REMOVED)
# class HyperliquidRawExchangeResponseData(BaseModel): ... (REMOVED)
# class HyperliquidRawExchangeActionResponse(BaseModel): ... (REMOVED)


class HyperliquidRawOrderStatusResponse(BaseModel):
    """Pydantic model for the response structure from Hyperliquid's /info endpoint.

    When querying order status (type='orderStatus').

    Ensures the presence of the 'order' field and that it conforms to the
    HyperliquidRawOrder model. Enforces immutability and forbids extra fields.
    """

    order: HyperliquidRawOrder = Field(..., description="The details of the queried order.")

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )

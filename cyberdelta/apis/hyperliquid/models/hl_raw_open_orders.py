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
  ms/μs/seconds, or null, per the spec.
- All models use `extra="forbid"` to ensure strict schema validation—any unexpected field
  will raise a validation error.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal models with type conversions
  and business logic.
- See the Hyperliquid OpenAPI spec, SDK, and docs for field details and allowed values.

**Authoritative Reference:**
- Official Hyperliquid API documentation:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
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

from cyberdelta.apis.exceptions.parsing import (
    EmptyDictionaryError,
    StructureTypeError,
)
from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import (
    RawAssetString64HL,
    RawFiniteDecimalStr,
    RawLaxEthereumAddressStrHL,
    RawNonNegativeFiniteDecimalStr,
    RawNonNegativeInt,
    RawOptionalCloidHL,
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


# HyperliquidRawTriggerSpec removed - using HyperliquidRawTriggerInfo instead


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
        cloid (RawOptionalCloidHL): Client order ID (128-bit hex string).
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
    cloid: RawOptionalCloidHL = Field(None, alias="cloid")
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


class HyperliquidRawSimpleOpenOrder(BaseModel):
    """Simple structure for open orders from the openOrders endpoint.

    This model matches the actual API response from the openOrders endpoint,
    which returns a flat structure with these fields directly at the top level.

    Fields:
        coin: Asset name (e.g., "ATOM", "ETH", "SOL")
        limit_px: Limit price as decimal string
        oid: Order ID
        side: Side ('B' for buy, 'A' for sell)
        sz: Current size as decimal string
        timestamp: Order timestamp in milliseconds
        orig_sz: Original size as decimal string
        cloid: Optional client order ID (128-bit hex string)
    """

    coin: RawAssetString64HL = Field(..., alias="coin")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    oid: RawNonNegativeInt = Field(..., alias="oid")
    side: RawSideStr = Field(..., alias="side")
    sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="sz")
    timestamp: RawTimestampMsInt = Field(..., alias="timestamp")
    orig_sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="origSz")
    cloid: RawOptionalCloidHL = Field(None, alias="cloid")
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


class HyperliquidRawOpenOrdersResponse(RootModel[list[HyperliquidRawSimpleOpenOrder]]):
    """Array of open orders from openOrders response.

    Fields:
        __root__: List of HyperliquidRawSimpleOpenOrder
    """

    root: list[HyperliquidRawSimpleOpenOrder]

    @property
    def items(self) -> list[HyperliquidRawSimpleOpenOrder]:
        """Return the validated list of open orders with full type safety.

        This is the preferred way to access the root data in Pydantic v2.
        """
        return self.root

    model_config = ConfigDict(frozen=True)

    @field_validator("root", mode="before")
    @classmethod
    def validate_open_orders_list(cls, v: object, info: ValidationInfo) -> list[dict[str, object]]:
        """Ensure the root input is a list of dictionaries for open orders.

        Returns:
            A validated list of dictionaries, each representing an open order.

        Raises:
            StructureTypeError: If the input is not a list or contains non-dictionary items.
        """
        field_name = info.field_name or "open_orders_list"

        if not isinstance(v, list):
            raise StructureTypeError(
                field_name=field_name,
                expected_structure="a list",
                actual_type=type(v).__name__,
            )

        # CAST 1: For type checker, v is already confirmed list by runtime check above
        list_of_objects = cast("list[object]", v)

        validated_items: list[dict[str, object]] = []
        for item_idx, item_obj in enumerate(list_of_objects):
            if not isinstance(item_obj, dict):
                item_type = type(item_obj).__name__
                raise StructureTypeError(
                    field_name=f"{field_name}[{item_idx}]",
                    expected_structure="a dictionary",
                    actual_type=item_type,
                    element_info=f"Item {item_idx}",
                )

            # CAST 2: For type checker, item_obj is already confirmed dict by runtime check above
            item_dict = cast("dict[str, object]", item_obj)

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
        trigger: Optional trigger spec (HyperliquidRawTriggerInfo | None).
        cloid: Optional client order ID (RawOptionalCloidHL).
    """

    asset: RawNonNegativeInt = Field(..., alias="asset")
    is_buy: RawStrictBool = Field(..., alias="isBuy")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    sz: RawPositiveFiniteDecimalStr = Field(..., alias="sz")
    reduce_only: RawStrictBool = Field(..., alias="reduceOnly")
    order_type: dict[str, object] = Field(..., alias="orderType")
    trigger: HyperliquidRawTriggerInfo | None = Field(None, alias="trigger")
    cloid: RawOptionalCloidHL = Field(None, alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("order_type")
    @classmethod
    def _validate_order_type_non_empty(
        cls,
        value: dict[str, object],
        info: ValidationInfo,
    ) -> dict[str, object]:
        if not value:
            field_name = info.field_name or "order_type"
            raise EmptyDictionaryError(field_name=field_name)
        return value

    @field_validator("trigger", mode="before")
    @classmethod
    def _validate_trigger_details_non_empty(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> dict[str, object] | None:
        if value is None:
            return None
        field_name = info.field_name or "trigger"
        if not isinstance(value, dict):
            raise StructureTypeError(
                field_name=field_name,
                expected_structure="a dictionary if provided",
                actual_type=type(value).__name__,
            )

        if not value:
            raise EmptyDictionaryError(
                field_name=field_name,
                context="dictionary cannot be empty if provided",
            )

        return cast("dict[str, object]", value)


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
# NOTE: Cancel request models have been moved to hl_raw_exchange_actions.py
# The HyperliquidRawCancelItem model is used for the actual API payloads
# with short field names (a, o) as required by the Hyperliquid SDK


# --- Exchange Action/Response Models ---
# These are now canonically defined in hl_raw_exchange_response.py
# Removing definitions from here to avoid duplication.


# --- Order Status Models ---


class HyperliquidRawOrderStatusOrder(BaseModel):
    """Order details from order status response (matches actual API structure).

    This model matches the actual API response structure which uses 'coin' instead of 'asset'
    and has simpler field names compared to the full HyperliquidRawOrder model.
    """

    coin: RawAssetString64HL = Field(..., alias="coin")
    side: RawSideStr = Field(..., alias="side")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="sz")
    oid: RawNonNegativeInt = Field(..., alias="oid")
    timestamp: RawTimestampMsInt = Field(..., alias="timestamp")
    trigger_condition: str = Field(..., alias="triggerCondition")
    is_trigger: RawStrictBool = Field(..., alias="isTrigger")
    trigger_px: RawFiniteDecimalStr = Field(..., alias="triggerPx")
    children: list[dict[str, object]] = Field(..., alias="children")
    is_position_tpsl: RawStrictBool = Field(..., alias="isPositionTpsl")
    reduce_only: RawStrictBool = Field(..., alias="reduceOnly")
    order_type: str = Field(..., alias="orderType")
    orig_sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="origSz")
    tif: str = Field(..., alias="tif")
    cloid: RawOptionalCloidHL = Field(None, alias="cloid")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawOrderStatusInfo(BaseModel):
    """Order status information from the API response."""

    order: HyperliquidRawOrderStatusOrder = Field(..., alias="order")
    status: str = Field(..., alias="status")
    status_timestamp: RawTimestampMsInt = Field(..., alias="statusTimestamp")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawOrderStatusResponse(BaseModel):
    """Pydantic model for the response structure from Hyperliquid's /info endpoint.

    When querying order status (type='orderStatus').

    This model matches the actual API response structure which has a nested order field.
    """

    status: str = Field(..., alias="status")
    order: HyperliquidRawOrderStatusInfo = Field(..., alias="order")

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
        populate_by_name=True,
    )

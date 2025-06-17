"""CyberDeltaEngine: Hyperliquid API Raw Models (Historical Order Group).

--------------------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of
Hyperliquid Exchange API responses related to historical or any-status orders,
such as those from 'queryOrderHistory' or 'orderStatus' (when the order might not be open).
"""

from pydantic import BaseModel, ConfigDict, Field

# Assuming common_raw_types and other necessary components are accessible
# For simplicity, copying relevant parts of HyperliquidRawOrder here
# and modifying the status field.
from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawAssetString64HL,
    RawCloidString64HL,
    RawDefaultString,
    RawFiniteDecimalStr,
    RawHistoricalOrderStatusHL,  # Use the new historical status type
    RawNonNegativeFiniteDecimalStr,
    RawNonNegativeInt,
    RawSideStr,
    RawStrictBool,
    RawTimestampMsInt,
)

# Assuming HyperliquidRawTriggerInfo is defined elsewhere (e.g., hl_raw_open_orders.py)
# If not, it would need to be defined or imported here.
# For now, let's assume it might not be part of a simple historical order view,
# or would be handled if needed. The main change is the status.


class HyperliquidRawHistoricalOrder(BaseModel):
    """Core historical order details with extended status support.

    Similar to HyperliquidRawOrder but uses RawHistoricalOrderStatusHL for broader
    status compatibility, allowing validation of orders in any state including
    filled, canceled, and other historical statuses.
    """

    oid: RawNonNegativeInt = Field(..., alias="oid")
    cloid: RawCloidString64HL | None = Field(
        None,
        alias="cloid",
    )  # Adjusted from RawOptionalNonEmptyString64HL
    asset: RawAssetString64HL = Field(..., alias="asset")
    coin: RawAssetString64HL | None = Field(None, alias="coin")  # Sometimes returned instead of asset
    side: RawSideStr = Field(..., alias="side")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="sz")
    timestamp: RawTimestampMsInt = Field(..., alias="timestamp")
    order_type: RawDefaultString | dict[str, object] = Field(..., alias="orderType")  # Can be string or dict
    reduce_only: RawStrictBool = Field(..., alias="reduceOnly")
    remaining_sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="remainingSz")
    status: RawHistoricalOrderStatusHL = Field(..., alias="status")  # KEY CHANGE
    status_timestamp: RawTimestampMsInt = Field(..., alias="statusTimestamp")
    # Additional fields that may be present
    trigger_condition: RawDefaultString | None = Field(None, alias="triggerCondition")
    is_trigger: RawStrictBool | None = Field(None, alias="isTrigger")
    trigger_px: RawFiniteDecimalStr | None = Field(None, alias="triggerPx")
    children: list[object] | None = Field(None, alias="children")
    is_position_tpsl: RawStrictBool | None = Field(None, alias="isPositionTpsl")
    orig_sz: RawNonNegativeFiniteDecimalStr | None = Field(None, alias="origSz")
    tif: RawDefaultString | None = Field(None, alias="tif")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawHistoricalOrderResponse(BaseModel):
    """Response structure for historical order query endpoints.

    This model wraps a single historical order's details and is used for endpoints
    that return individual order information. It embeds HyperliquidRawHistoricalOrder
    to provide complete order data with extended status support.
    """

    order: HyperliquidRawHistoricalOrder = Field(
        ...,
        description="The details of the queried historical order.",
    )
    model_config = ConfigDict(extra="forbid", frozen=True)

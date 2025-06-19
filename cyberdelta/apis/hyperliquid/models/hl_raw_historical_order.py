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


class HyperliquidRawHistoricalOrderData(BaseModel):
    """Order details from historicalOrders endpoint - the 'order' object only."""

    oid: RawNonNegativeInt = Field(..., alias="oid")
    cloid: RawCloidString64HL | None = Field(None, alias="cloid")
    coin: RawAssetString64HL = Field(..., alias="coin")  # The symbol/asset
    side: RawSideStr = Field(..., alias="side")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="sz")
    timestamp: RawTimestampMsInt = Field(..., alias="timestamp")
    order_type: RawDefaultString = Field(..., alias="orderType")
    reduce_only: RawStrictBool = Field(..., alias="reduceOnly")
    orig_sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="origSz")
    tif: RawDefaultString = Field(..., alias="tif")
    # Additional fields that may be present
    trigger_condition: RawDefaultString | None = Field(None, alias="triggerCondition")
    is_trigger: RawStrictBool | None = Field(None, alias="isTrigger")
    trigger_px: RawFiniteDecimalStr | None = Field(None, alias="triggerPx")
    children: list[object] | None = Field(None, alias="children")
    is_position_tpsl: RawStrictBool | None = Field(None, alias="isPositionTpsl")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawHistoricalOrder(BaseModel):
    """Complete historical order model combining order data with status.

    This model is used by the mapper and includes all fields needed for
    transformation to internal Order model.
    """

    # All fields from HyperliquidRawHistoricalOrderData
    oid: RawNonNegativeInt = Field(..., alias="oid")
    cloid: RawCloidString64HL | None = Field(None, alias="cloid")
    coin: RawAssetString64HL = Field(..., alias="coin")
    side: RawSideStr = Field(..., alias="side")
    limit_px: RawFiniteDecimalStr = Field(..., alias="limitPx")
    sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="sz")
    timestamp: RawTimestampMsInt = Field(..., alias="timestamp")
    order_type: RawDefaultString = Field(..., alias="orderType")
    reduce_only: RawStrictBool = Field(..., alias="reduceOnly")
    orig_sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="origSz")
    tif: RawDefaultString = Field(..., alias="tif")
    trigger_condition: RawDefaultString | None = Field(None, alias="triggerCondition")
    is_trigger: RawStrictBool | None = Field(None, alias="isTrigger")
    trigger_px: RawFiniteDecimalStr | None = Field(None, alias="triggerPx")
    children: list[object] | None = Field(None, alias="children")
    is_position_tpsl: RawStrictBool | None = Field(None, alias="isPositionTpsl")

    # Status fields from the parent level
    status: RawHistoricalOrderStatusHL = Field(..., alias="status")
    status_timestamp: RawTimestampMsInt = Field(..., alias="statusTimestamp")

    # For compatibility with mapper - provide asset field from coin
    @property
    def asset(self) -> str:
        """Get asset name from coin field for compatibility with mappers."""
        return self.coin

    # For compatibility - remaining_sz is always the current sz for historical orders
    @property
    def remaining_sz(self) -> str:
        """Get remaining size which equals current size for historical orders."""
        return self.sz

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawHistoricalOrderResponse(BaseModel):
    """Response structure for each item in the historicalOrders endpoint response.

    This represents each item in the array returned by historicalOrders, with
    status and statusTimestamp at the top level alongside the order object.
    """

    order: HyperliquidRawHistoricalOrderData = Field(
        ...,
        description="The order details.",
    )
    status: RawHistoricalOrderStatusHL = Field(..., alias="status")
    status_timestamp: RawTimestampMsInt = Field(..., alias="statusTimestamp")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

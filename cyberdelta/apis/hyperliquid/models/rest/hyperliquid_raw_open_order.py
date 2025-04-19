"""
Structure for one open order (with optional trigger).
Strictly validated (extra fields forbidden).
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


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

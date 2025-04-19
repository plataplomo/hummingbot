"""
Core order details from open orders or order status.
Strictly validated (extra fields forbidden).
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


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

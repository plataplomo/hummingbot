"""
Order spec for placing an order (exchange action request).
Strictly validated (extra fields forbidden).
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


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

"""
Array of open orders from openOrders response.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict

from .hyperliquid_raw_open_order import HyperliquidRawOpenOrder


class HyperliquidRawOpenOrdersResponse(BaseModel):
    """
    Array of open orders from openOrders response.
    Fields:
        __root__: List of HyperliquidRawOpenOrder
    """

    __root__: list[HyperliquidRawOpenOrder]
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

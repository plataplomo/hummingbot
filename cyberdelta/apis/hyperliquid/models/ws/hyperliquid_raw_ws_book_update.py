"""
WebSocket order book update event (l2Book channel).
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawBookLevel(BaseModel):
    """
    A single price level in the order book.
    Fields:
        px: Price (str)
        sz: Size (str)
        n: Number of orders (int)
    """

    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    n: int = Field(..., alias="n")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawWsBookUpdate(BaseModel):
    """
    WebSocket order book update event (l2Book channel).
    Fields:
        coin: Asset symbol (str)
        levels: [bids, asks] (list[list[HyperliquidRawBookLevel]])
        time: Snapshot timestamp (int)
    """

    coin: str = Field(..., alias="coin")
    levels: list[list[HyperliquidRawBookLevel]] = Field(..., alias="levels")
    time: int = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

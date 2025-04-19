"""
WebSocket fill event (user fill/execution).
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawWsFillEvent(BaseModel):
    """
    WebSocket fill event (user fill/execution).
    Fields:
        coin: Asset symbol (str)
        px: Price (str)
        sz: Size (str)
        side: Side ('B' or 'A')
        time: Timestamp (int)
        hash: Trade hash (str)
        oid: Order ID (int)
        cloid: Client order ID (str | None)
        is_maker: Is maker (bool)
    """

    coin: str = Field(..., alias="coin")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    side: str = Field(..., alias="side")
    time: int = Field(..., alias="time")
    hash: str = Field(..., alias="hash")
    oid: int = Field(..., alias="oid")
    cloid: str | None = Field(None, alias="cloid")
    is_maker: bool = Field(..., alias="isMaker")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

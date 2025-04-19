"""
WebSocket trade event (trades channel).
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawWsTradeEvent(BaseModel):
    """
    WebSocket trade event (trades channel).
    Fields:
        coin: Asset symbol (str)
        px: Price (str)
        sz: Size (str)
        side: Side ('B' or 'A')
        time: Timestamp (int)
        hash: Trade hash (str)
    """

    coin: str = Field(..., alias="coin")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    side: str = Field(..., alias="side")
    time: int = Field(..., alias="time")
    hash: str = Field(..., alias="hash")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

"""
Public trade object from recent trades.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawPublicTrade(BaseModel):
    """
    Public trade object from recent trades.
    Fields:
        coin: Asset symbol (str)
        side: Side ('B' or 'A')
        px: Price (str)
        sz: Size (str)
        time: Timestamp (int)
        hash: Trade hash (str)
    """

    coin: str = Field(..., alias="coin")
    side: str = Field(..., alias="side")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    time: int = Field(..., alias="time")
    hash: str = Field(..., alias="hash")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

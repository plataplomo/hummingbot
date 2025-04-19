"""
Request payload for 'recentTrades' info type.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawRecentTradesRequestPayload(BaseModel):
    """
    Request payload for 'recentTrades' info type.
    Fields:
        type: Must be 'recentTrades'
        coin: Asset symbol (str)
    """

    type: str = Field("recentTrades", alias="type")
    coin: str = Field(..., alias="coin")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

"""
Array of public trades from recentTrades response.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict

from .hyperliquid_raw_public_trade import HyperliquidRawPublicTrade


class HyperliquidRawRecentTradesResponse(BaseModel):
    """
    Array of public trades from recentTrades response.
    Fields:
        __root__: List of HyperliquidRawPublicTrade
    """

    __root__: list[HyperliquidRawPublicTrade]
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

"""
WebSocket position update event (user position change).
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawLeverage(BaseModel):
    """
    Leverage settings for a position.
    Fields:
        type: Leverage type ('cross' or 'isolated')
        value: Leverage value (int)
    """

    type: str = Field(..., alias="type")
    value: int = Field(..., alias="value")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawPositionInfo(BaseModel):
    """
    Detailed info about a user position.
    Fields:
        coin: Asset symbol (str)
        entry_px: Entry price (str | None)
        leverage: Leverage settings (HyperliquidRawLeverage)
        liquidation_px: Liquidation price (str | None)
        margin_used: Margin used (str)
        max_leverage: Max leverage (int)
        position_value: Position value (str)
        return_on_equity: ROE (str)
        szi: Size (str)
        unrealized_pnl: Unrealized PnL (str)
    """

    coin: str = Field(..., alias="coin")
    entry_px: str | None = Field(None, alias="entryPx")
    leverage: HyperliquidRawLeverage = Field(..., alias="leverage")
    liquidation_px: str | None = Field(None, alias="liquidationPx")
    margin_used: str = Field(..., alias="marginUsed")
    max_leverage: int = Field(..., alias="maxLeverage")
    position_value: str = Field(..., alias="positionValue")
    return_on_equity: str = Field(..., alias="returnOnEquity")
    szi: str = Field(..., alias="szi")
    unrealized_pnl: str = Field(..., alias="unrealizedPnl")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawWsPositionUpdateEvent(BaseModel):
    """
    WebSocket position update event (user position change).
    Fields:
        asset: Asset symbol (str)
        position: Position info (HyperliquidRawPositionInfo)
        time: Timestamp (int)
    """

    asset: str = Field(..., alias="asset")
    position: HyperliquidRawPositionInfo = Field(..., alias="position")
    time: int = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

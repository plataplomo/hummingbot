"""
User fill/trade details from userFills response.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawUserFill(BaseModel):
    """
    User fill/trade details from userFills response.
    Fields:
        tid: Trade ID (int)
        coin: Asset symbol (str)
        px: Price (str)
        sz: Size (str)
        time: Timestamp (int)
        side: Side ('B' or 'A')
        oid: Order ID (int)
        start_position: Start position (str)
        dir: Direction (str)
        hash: Trade hash (str)
        fee: Fee (str)
        is_maker: Is maker (bool)
        liquidation_mark_px: Liquidation mark price (str | None)
        cloid: Client order ID (str | None)
    """

    tid: int = Field(..., alias="tid")
    coin: str = Field(..., alias="coin")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    time: int = Field(..., alias="time")
    side: str = Field(..., alias="side")
    oid: int = Field(..., alias="oid")
    start_position: str = Field(..., alias="startPosition")
    dir: str = Field(..., alias="dir")
    hash: str = Field(..., alias="hash")
    fee: str = Field(..., alias="fee")
    is_maker: bool = Field(..., alias="isMaker")
    liquidation_mark_px: str | None = Field(None, alias="liquidationMarkPx")
    cloid: str | None = Field(None, alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

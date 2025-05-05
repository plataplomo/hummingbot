from pydantic import BaseModel, ConfigDict, Field

# Raw Model for User Fills from /info endpoint


class HyperliquidRawFill(BaseModel):
    """
    Raw Pydantic model for a user fill event from the /info endpoint (type: userFills).
    Based on the structure observed in the Hyperliquid SDK (types.py UserFill).
    """

    tid: int = Field(...)
    coin: str = Field(...)
    px: str = Field(...)
    sz: str = Field(...)
    time: int = Field(..., description="Timestamp in milliseconds")
    side: str = Field(...)  # Likely 'B' or 'A'
    oid: int = Field(...)
    start_position: str = Field(..., alias="startPosition")
    dir: str = Field(...)  # Direction/description of the fill
    hash: str = Field(...)  # Transaction hash
    fee: str = Field(...)
    is_maker: bool = Field(..., alias="isMaker")
    liquidation_mark_px: str | None = Field(None, alias="liquidationMarkPx")
    cloid: str | None = Field(None)

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        frozen=True,
    )

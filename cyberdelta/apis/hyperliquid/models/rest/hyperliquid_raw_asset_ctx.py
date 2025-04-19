"""
Contextual info for an asset from 'metaAndAssetCtxs'.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawAssetCtx(BaseModel):
    """
    Contextual info for an asset from 'metaAndAssetCtxs'.
    Fields:
        name: Asset symbol (str)
        funding: Hourly funding rate string (str)
        mark_px: Mark price (str)
        prev_day_px: Previous day price (str)
        day_ntl_vlm: Daily notional volume (str)
        impact_px: Impact price (str | None)
    """

    name: str = Field(..., alias="name")
    funding: str = Field(..., alias="funding")
    mark_px: str = Field(..., alias="markPx")
    prev_day_px: str = Field(..., alias="prevDayPx")
    day_ntl_vlm: str = Field(..., alias="dayNtlVlm")
    impact_px: str | None = Field(None, alias="impactPx")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

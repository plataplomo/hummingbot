"""
User's position details for a specific asset.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field

from .hyperliquid_raw_position_info import HyperliquidRawPositionInfo


class HyperliquidRawAssetPosition(BaseModel):
    """
    User's position details for a specific asset.
    Fields:
        asset: Asset symbol (str)
        position: Position info (HyperliquidRawPositionInfo)
    """

    asset: str = Field(..., alias="asset")
    position: HyperliquidRawPositionInfo = Field(..., alias="position")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

"""
User state including positions and margin.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field

from .hyperliquid_raw_asset_position import HyperliquidRawAssetPosition
from .hyperliquid_raw_margin_summary import HyperliquidRawMarginSummary


class HyperliquidRawClearinghouseState(BaseModel):
    """
    User state including positions and margin.
    Fields:
        asset_positions: List of asset positions (list[HyperliquidRawAssetPosition])
        margin_summary: Margin summary (HyperliquidRawMarginSummary)
        cross_maintenance_margin_used: Cross maintenance margin used (str)
        cross_margin_summary: Cross margin summary (HyperliquidRawMarginSummary)
        isolated_maintenance_margin_used: Isolated maintenance margin used (str)
        isolated_margin_summary: Isolated margin summary (HyperliquidRawMarginSummary)
        withdrawable: Withdrawable amount (str)
    """

    asset_positions: list[HyperliquidRawAssetPosition] = Field(..., alias="assetPositions")
    margin_summary: HyperliquidRawMarginSummary = Field(..., alias="marginSummary")
    cross_maintenance_margin_used: str = Field(..., alias="crossMaintenanceMarginUsed")
    cross_margin_summary: HyperliquidRawMarginSummary = Field(..., alias="crossMarginSummary")
    isolated_maintenance_margin_used: str = Field(..., alias="isolatedMaintenanceMarginUsed")
    isolated_margin_summary: HyperliquidRawMarginSummary = Field(..., alias="isolatedMarginSummary")
    withdrawable: str = Field(..., alias="withdrawable")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

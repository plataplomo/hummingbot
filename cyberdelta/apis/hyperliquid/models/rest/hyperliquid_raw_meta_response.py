"""
Meta response: universe/market metadata.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field

from .hyperliquid_raw_asset_definition import HyperliquidRawAssetDefinition


class HyperliquidRawMetaResponse(BaseModel):
    """
    Meta response: universe/market metadata.
    Fields:
        universe: List of HyperliquidRawAssetDefinition
    """

    universe: list[HyperliquidRawAssetDefinition] = Field(..., alias="universe")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

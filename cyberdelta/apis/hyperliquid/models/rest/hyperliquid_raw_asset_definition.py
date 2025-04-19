"""
Asset/market definition from 'meta' response.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawAssetDefinition(BaseModel):
    """
    Asset/market definition from 'meta' response.
    Fields:
        name: Asset symbol (str)
        sz_decimals: Size decimals (int)
        max_leverage: Max leverage (int)
        only_isolated: Only isolated margin allowed (bool)
    """

    name: str = Field(..., alias="name")
    sz_decimals: int = Field(..., alias="szDecimals")
    max_leverage: int = Field(..., alias="maxLeverage")
    only_isolated: bool = Field(..., alias="onlyIsolated")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

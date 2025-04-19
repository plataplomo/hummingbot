"""
Limit order type for orderType field.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field

from .hyperliquid_raw_tif_limit import HyperliquidRawTifLimit


class HyperliquidRawOrderTypeLimit(BaseModel):
    """
    Limit order type for orderType field.
    Fields:
        limit: HyperliquidRawTifLimit
    """

    limit: HyperliquidRawTifLimit = Field(..., alias="limit")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

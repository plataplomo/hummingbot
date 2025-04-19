"""
Update isolated margin request payload.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawUpdateIsolatedMarginRequest(BaseModel):
    """
    Update isolated margin request payload.
    Fields:
        asset: Asset index (int)
        is_buy: Is buy (bool)
        ntli: Amount (int)
    """

    asset: int = Field(..., alias="asset")
    is_buy: bool = Field(..., alias="isBuy")
    ntli: int = Field(..., alias="ntli")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

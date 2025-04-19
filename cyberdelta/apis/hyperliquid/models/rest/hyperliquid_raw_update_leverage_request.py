"""
Update leverage request payload.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawUpdateLeverageRequest(BaseModel):
    """
    Update leverage request payload.
    Fields:
        asset: Asset index (int)
        is_cross: Is cross margin (bool)
        leverage: Leverage value (int)
    """

    asset: int = Field(..., alias="asset")
    is_cross: bool = Field(..., alias="isCross")
    leverage: int = Field(..., alias="leverage")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

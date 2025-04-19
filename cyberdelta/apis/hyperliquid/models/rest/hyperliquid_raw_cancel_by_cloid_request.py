"""
Cancel request payload (by client OID).
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawCancelByCloidRequest(BaseModel):
    """
    Cancel request payload (by client OID).
    Fields:
        asset: Asset index (int)
        cloid: Client order ID (str)
    """

    asset: int = Field(..., alias="asset")
    cloid: str = Field(..., alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

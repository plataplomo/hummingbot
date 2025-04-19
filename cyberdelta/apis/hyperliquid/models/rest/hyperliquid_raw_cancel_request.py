"""
Cancel request payload (by exchange OID).
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawCancelRequest(BaseModel):
    """
    Cancel request payload (by exchange OID).
    Fields:
        asset: Asset index (int)
        oid: Order ID (int)
    """

    asset: int = Field(..., alias="asset")
    oid: int = Field(..., alias="oid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

"""
Request payload for 'openOrders' info type.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawOpenOrdersRequestPayload(BaseModel):
    """
    Request payload for 'openOrders' info type.
    Fields:
        type: Must be 'openOrders'
        user: Wallet address (str)
    """

    type: str = Field("openOrders", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

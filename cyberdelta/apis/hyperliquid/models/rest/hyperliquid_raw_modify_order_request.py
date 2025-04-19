"""
Modify order request payload.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field

from .hyperliquid_raw_order_spec import HyperliquidRawOrderSpec


class HyperliquidRawModifyOrderRequest(BaseModel):
    """
    Modify order request payload.
    Fields:
        oid: Order ID (int)
        order: HyperliquidRawOrderSpec
    """

    oid: int = Field(..., alias="oid")
    order: HyperliquidRawOrderSpec = Field(..., alias="order")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

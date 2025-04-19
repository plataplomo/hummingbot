"""
Request payload for 'l2Book' info type.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawL2BookRequestPayload(BaseModel):
    """
    Request payload for 'l2Book' info type.
    Fields:
        type: Must be 'l2Book'
        coin: Asset symbol (str)
    """

    type: str = Field("l2Book", alias="type")
    coin: str = Field(..., alias="coin")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

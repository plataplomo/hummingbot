"""
Request payload for 'clearinghouseState' info type.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawUserStateRequestPayload(BaseModel):
    """
    Request payload for 'clearinghouseState' info type.
    Fields:
        type: Must be 'clearinghouseState'
        user: Wallet address (str)
    """

    type: str = Field("clearinghouseState", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

"""
Request payload for 'userFills' info type.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawUserFillsRequestPayload(BaseModel):
    """
    Request payload for 'userFills' info type.
    Fields:
        type: Must be 'userFills'
        user: Wallet address (str)
    """

    type: str = Field("userFills", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

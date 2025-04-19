"""
Request payload for 'allMids' info type.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawAllMidsRequestPayload(BaseModel):
    """
    Request payload for 'allMids' info type.
    Fields:
        type: Must be 'allMids'
    """

    type: str = Field("allMids", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

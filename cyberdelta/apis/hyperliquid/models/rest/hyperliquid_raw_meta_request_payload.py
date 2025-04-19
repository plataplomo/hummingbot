"""
Request payload for 'meta' info type.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawMetaRequestPayload(BaseModel):
    """
    Request payload for 'meta' info type.
    Fields:
        type: Must be 'meta'
    """

    type: str = Field("meta", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

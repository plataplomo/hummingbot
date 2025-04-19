"""
Request payload for 'metaAndAssetCtxs' info type.
Strictly validated (extra fields forbidden).
"""

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawMetaAndAssetCtxsRequestPayload(BaseModel):
    """
    Request payload for 'metaAndAssetCtxs' info type.
    Fields:
        type: Must be 'metaAndAssetCtxs'
    """

    type: str = Field("metaAndAssetCtxs", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

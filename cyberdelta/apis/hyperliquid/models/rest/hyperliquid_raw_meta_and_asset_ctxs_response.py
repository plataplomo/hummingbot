"""
MetaAndAssetCtxs response: strict 2-tuple [meta, assetCtxs].
Strictly validated (extra fields forbidden).
"""

from typing import Any

from pydantic import BaseModel, ConfigDict

from .hyperliquid_raw_asset_ctx import HyperliquidRawAssetCtx
from .hyperliquid_raw_meta_response import HyperliquidRawMetaResponse


class HyperliquidRawMetaAndAssetCtxsResponse(BaseModel):
    """
    MetaAndAssetCtxs response: strict 2-tuple [meta, assetCtxs].
    Fields:
        meta: HyperliquidRawMetaResponse
        asset_ctxs: List of HyperliquidRawAssetCtx
    """

    meta: HyperliquidRawMetaResponse
    asset_ctxs: list[HyperliquidRawAssetCtx]

    @classmethod
    def model_validate(
        cls, obj: Any, *args: Any, **kwargs: Any
    ) -> "HyperliquidRawMetaAndAssetCtxsResponse":
        """
        Validate a MetaAndAssetCtxs response from a list [meta, assetCtxs].
        """
        if (
            isinstance(obj, list)
            and len(obj) == 2  # type: ignore[arg-type]
            and isinstance(obj[0], dict)
            and isinstance(obj[1], list)
        ):
            meta = HyperliquidRawMetaResponse.model_validate(obj[0])
            asset_ctxs = [HyperliquidRawAssetCtx.model_validate(x) for x in obj[1]]  # type: ignore[arg-type]
            return cls(meta=meta, asset_ctxs=asset_ctxs)
        raise ValueError("Invalid MetaAndAssetCtxs response structure")

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

"""
CyberDeltaEngine: Hyperliquid API Raw Models
-------------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses.

- Each `HyperliquidRaw*` model mirrors the official Hyperliquid OpenAPI spec, SDK,
  or WebSocket event payloads as closely as possible.
- All fields use `Field(..., alias=...)` to match the exact key names in Hyperliquid's JSON.
- Timestamp fields are typed as `int | str | float | None` to accept ISO8601 strings, epoch
  ms/µs/seconds, or null, per the spec.
- All models use `extra=\"forbid\"` to ensure strict schema validation—any unexpected field
  will raise a validation error.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal models with type conversions
  and business logic.
- See the Hyperliquid OpenAPI spec, SDK, and docs for field details and allowed values.

**Authoritative Reference:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

Usage:
    raw = HyperliquidRawOrder.model_validate(api_response_dict)
    # ...then transform to internal Order model

Do not use these models for internal business logic—use your core models for that.
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

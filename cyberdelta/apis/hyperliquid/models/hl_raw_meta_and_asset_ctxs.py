"""
CyberDeltaEngine: Hyperliquid API Raw Models (Meta & Asset Context Group)
-----------------------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to meta information,
asset context, and related request/response payloads.

- Each `HyperliquidRaw*` model mirrors the official Hyperliquid OpenAPI spec, SDK,
  or WebSocket event payloads as closely as possible.
- All fields use `Field(..., alias=...)` to match the exact key names in Hyperliquid's JSON.
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
    raw = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(api_response)
    # ...then transform to internal models

Do not use these models for internal business logic—use your core models for that.
"""

from typing import Self

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawAssetDefinition(BaseModel):
    """
    Asset/market definition from 'meta' response.
    Fields:
        name: Asset symbol (str)
        sz_decimals: Size decimals (int)
        max_leverage: Max leverage (int)
        only_isolated: Only isolated margin allowed (bool)
    """

    name: str = Field(..., alias="name")
    sz_decimals: int = Field(..., alias="szDecimals")
    max_leverage: int = Field(..., alias="maxLeverage")
    only_isolated: bool = Field(..., alias="onlyIsolated")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawAssetCtx(BaseModel):
    """
    Contextual info for an asset from 'metaAndAssetCtxs'.
    Fields:
        name: Asset symbol (str)
        funding: Hourly funding rate string (str)
        mark_px: Mark price (str)
        prev_day_px: Previous day price (str)
        day_ntl_vlm: Daily notional volume (str)
        impact_px: Impact price (str | None)
    """

    name: str = Field(..., alias="name")
    funding: str = Field(..., alias="funding")
    mark_px: str = Field(..., alias="markPx")
    prev_day_px: str = Field(..., alias="prevDayPx")
    day_ntl_vlm: str = Field(..., alias="dayNtlVlm")
    impact_px: str | None = Field(None, alias="impactPx")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawMetaResponse(BaseModel):
    """
    Meta response: universe/market metadata.
    Fields:
        universe: List of HyperliquidRawAssetDefinition
    """

    universe: list[HyperliquidRawAssetDefinition] = Field(..., alias="universe")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


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
        cls,
        obj: object,
        *,
        strict: bool | None = None,
        from_attributes: bool | None = None,
        context: None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        """
        Validate a MetaAndAssetCtxs response from a list [meta, assetCtxs].
        Expects obj to be a list of [MetaResponse (dict), List[AssetCtx (dict)]].
        """
        if not (isinstance(obj, list) and len(obj) == 2):  # pyright: ignore[reportUnknownArgumentType]
            raise ValueError("Invalid MetaAndAssetCtxs response structure: not a 2-element list")
        obj_list: list[object] = obj
        if (
            isinstance(obj_list[0], dict)
            and isinstance(obj_list[1], list)
            and all(isinstance(x, dict) for x in obj_list[1])  # pyright: ignore[reportUnknownVariableType]
        ):
            meta_dict: dict[str, object] = obj_list[0]
            asset_ctxs_list: list[dict[str, object]] = obj_list[1]
            meta = HyperliquidRawMetaResponse.model_validate(meta_dict)
            asset_ctxs = [HyperliquidRawAssetCtx.model_validate(x) for x in asset_ctxs_list]
            return cls(meta=meta, asset_ctxs=asset_ctxs)
        raise ValueError("Invalid MetaAndAssetCtxs response structure: element types incorrect")

    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawMetaRequestPayload(BaseModel):
    """
    Request payload for 'meta' info type.
    Fields:
        type: Must be 'meta'
    """

    type: str = Field("meta", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawMetaAndAssetCtxsRequestPayload(BaseModel):
    """
    Request payload for 'metaAndAssetCtxs' info type.
    Fields:
        type: Must be 'metaAndAssetCtxs'
    """

    type: str = Field("metaAndAssetCtxs", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawUpdateLeverageRequest(BaseModel):
    """
    Update leverage request payload.
    Fields:
        asset: Asset index (int)
        is_cross: Is cross margin (bool)
        leverage: Leverage value (int)
    """

    asset: int = Field(..., alias="asset")
    is_cross: bool = Field(..., alias="isCross")
    leverage: int = Field(..., alias="leverage")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawUpdateIsolatedMarginRequest(BaseModel):
    """
    Update isolated margin request payload.
    Fields:
        asset: Asset index (int)
        is_buy: Is buy (bool)
        ntli: Amount (int)
    """

    asset: int = Field(..., alias="asset")
    is_buy: bool = Field(..., alias="isBuy")
    ntli: int = Field(..., alias="ntli")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

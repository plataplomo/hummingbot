"""
CyberDeltaEngine: Hyperliquid API Raw Models (Meta & Asset Context Group)
-----------------------------------------------------------------------

This module provides **strict Pydantic models** for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to meta information, asset context,
and related request/response payloads. It is a core part of CyberDeltaEngine's boundary validation
layer.

**Scope & Rationale:**
- Models in this file are used to validate and parse the *external* data structures returned by
  Hyperliquid's 'meta' and 'metaAndAssetCtxs' endpoints, as well as related request payloads and
  leverage/margin updates.
- All models enforce strict schema validation (`extra="forbid"`), ensuring that any unexpected or
  malformed fields in upstream data are immediately rejected. This is critical for robust, secure,
  and predictable operation in a financial system.
- These models are the *first step* in the "validate first, then transform" pattern: validate
  external data at the boundary, then map to internal business models with type conversions and
  business logic.

**References:**
- Official Hyperliquid API documentation:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(api_response)
    # ...then transform to internal models

**Note:**
Do not use these models for internal business logic—use your core models for that. These are for
boundary validation only.
"""

from typing import Self

from pydantic import BaseModel, ConfigDict, Field


class HyperliquidRawAssetDefinition(BaseModel):
    """
    Represents a single asset/market definition as returned in the 'meta' endpoint response.

    This model is used to validate the structure of each asset entry in the Hyperliquid universe.
    It is a strict mirror of the upstream API schema and should not be used for internal business
    logic.

    Fields:
        name (str): Asset symbol (e.g., 'ETH', 'BTC').
        sz_decimals (int): Number of decimals for size/quantity precision.
        max_leverage (int): Maximum leverage allowed for this asset.
        only_isolated (bool): If True, only isolated margin is allowed for this asset.
    """

    name: str = Field(..., alias="name")
    sz_decimals: int = Field(..., alias="szDecimals")
    max_leverage: int = Field(..., alias="maxLeverage")
    only_isolated: bool = Field(..., alias="onlyIsolated")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawAssetCtx(BaseModel):
    """
    Represents contextual information for a single asset as returned in the 'metaAndAssetCtxs'
    endpoint.

    This model is used to validate the structure of each asset context entry, which includes
    funding rates, mark price, previous day price, daily notional volume, and (optionally) impact
    price.

    Fields:
        name (str): Asset symbol (e.g., 'ETH', 'BTC').
        funding (str): Hourly funding rate as a string (precise decimal, not float).
        mark_px (str): Mark price as a string.
        prev_day_px (str): Previous day's price as a string.
        day_ntl_vlm (str): Daily notional volume as a string.
        impact_px (Optional[str]): Impact price as a string, or None if not present.
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
    Represents the top-level 'meta' response from the Hyperliquid API, containing the universe of
    tradable assets.

    This model is used to validate the structure of the 'meta' endpoint response, which is a
    dictionary with a single key 'universe' mapping to a list of asset definitions.

    Fields:
        universe (List[HyperliquidRawAssetDefinition]): List of asset definitions for all tradable
            markets.
    """

    universe: list[HyperliquidRawAssetDefinition] = Field(..., alias="universe")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawMetaAndAssetCtxsResponse(BaseModel):
    """
    Represents the strict 2-tuple response [meta, assetCtxs] from the 'metaAndAssetCtxs' endpoint.

    This model is used to validate the structure of the 'metaAndAssetCtxs' endpoint response, which
    is a list containing two elements: the meta response (as a dict) and a list of asset context
    dicts.

    Fields:
        meta (HyperliquidRawMetaResponse): The meta/universe information.
        asset_ctxs (List[HyperliquidRawAssetCtx]): List of asset context objects for each asset.

    Usage:
        Use the custom classmethod `model_validate` to parse and validate a raw list response.
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
        Validates a MetaAndAssetCtxs response from a list [meta, assetCtxs].

        Args:
            obj (object): The raw response object, expected to be a list of [MetaResponse (dict),
                List[AssetCtx (dict)]].
            strict, from_attributes, context, by_alias, by_name: Passed through to Pydantic
                validation (optional).

        Returns:
            HyperliquidRawMetaAndAssetCtxsResponse: The validated and parsed response object.

        Raises:
            ValueError: If the input structure does not match the expected 2-tuple format.
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
    Represents the request payload for the 'meta' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    requesting meta/universe information.

    Fields:
        type (str): Must be 'meta'.
    """

    type: str = Field("meta", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawMetaAndAssetCtxsRequestPayload(BaseModel):
    """
    Represents the request payload for the 'metaAndAssetCtxs' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    requesting both meta and asset context information.

    Fields:
        type (str): Must be 'metaAndAssetCtxs'.
    """

    type: str = Field("metaAndAssetCtxs", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawUpdateLeverageRequest(BaseModel):
    """
    Represents the request payload for updating leverage settings for a specific asset.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    updating leverage for an asset, specifying whether cross margin is used and the leverage value.

    Fields:
        asset (int): Asset index (as used by the API).
        is_cross (bool): True if cross margin is to be used, False for isolated.
        leverage (int): The leverage value to set.
    """

    asset: int = Field(..., alias="asset")
    is_cross: bool = Field(..., alias="isCross")
    leverage: int = Field(..., alias="leverage")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class HyperliquidRawUpdateIsolatedMarginRequest(BaseModel):
    """
    Represents the request payload for updating isolated margin for a specific asset.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    updating isolated margin for an asset, specifying buy/sell and the notional amount.

    Fields:
        asset (int): Asset index (as used by the API).
        is_buy (bool): True if the operation is a buy, False for sell.
        ntli (int): The notional amount to update.
    """

    asset: int = Field(..., alias="asset")
    is_buy: bool = Field(..., alias="isBuy")
    ntli: int = Field(..., alias="ntli")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

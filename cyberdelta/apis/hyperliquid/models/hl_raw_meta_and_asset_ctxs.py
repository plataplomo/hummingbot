"""CyberDeltaEngine: Hyperliquid API Raw Models (Meta & Asset Context Group).

-----------------------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw*
structure of all major Hyperliquid Exchange API (REST and WebSocket) responses related to
meta information, asset context, and related request/response payloads. It is a core part
of CyberDeltaEngine's boundary validation layer.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data
  structures returned by Hyperliquid's 'meta' and 'metaAndAssetCtxs' endpoints, as well as
  related request payloads and leverage/margin updates.
- All models enforce strict schema validation (`extra="forbid"`), strict type checking,
  and robust format validation (e.g., max length, finite decimals, valid UTF-8).
- Any unexpected, malformed, or ambiguous fields in upstream data are immediately
  rejected. This is critical for robust, secure, and predictable operation in a
  financial system.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal business models with
  type conversions and business logic.
- **Never use these models for internal business logic.**

**References:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(api_response)
    # ...then transform to internal models

**Note:**
Do not use these models for internal business logic—use your core models for that. These are for
boundary validation only.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, cast

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawAssetString64HL,
    RawFiniteDecimalStr,
    RawNonNegativeInt,
    RawStrictBool,
)
from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawAssetDefinition(BaseModel):
    """Strict boundary model for a single asset/market definition from Hyperliquid.

    This model is used exclusively for validating the raw structure of asset entries in the upstream
    API response. It enforces strict type and format constraints to prevent malformed or ambiguous
    data from entering the system. Never use for internal business logic.

    Fields:
        name (str): Asset symbol (e.g., 'ETH', 'BTC').
        sz_decimals (int): Number of decimals for size/quantity precision (0-18).
        max_leverage (int): Maximum leverage allowed (0-1000).
        margin_table_id (int, optional): Margin table identifier for this asset.
        is_delisted (bool, optional): True if asset is delisted.
        only_isolated (bool, optional): True if only isolated margin is allowed for this asset.
    """

    name: RawAssetString64HL = Field(..., alias="name")
    sz_decimals: RawNonNegativeInt = Field(..., alias="szDecimals", le=18)
    max_leverage: RawNonNegativeInt = Field(..., alias="maxLeverage", le=1000)
    margin_table_id: RawNonNegativeInt | None = Field(None, alias="marginTableId")
    is_delisted: RawStrictBool | None = Field(None, alias="isDelisted")
    only_isolated: RawStrictBool | None = Field(None, alias="onlyIsolated")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawAssetCtx(BaseModel):
    """Strict boundary model for contextual information about a single asset.

    This model validates raw asset context entries from the 'metaAndAssetCtxs' endpoint,
    including funding rates, mark prices, and volume data. It enforces strict type and
    format constraints for all fields as received from the upstream API.
    Never use for internal business logic.

    Fields:
        name (str, optional): Asset symbol (max 64 chars) - not provided in API response.
        funding (str): Hourly funding rate as a decimal string.
        mark_px (str): Mark price as a decimal string.
        prev_day_px (str): Previous day's price as a decimal string.
        day_ntl_vlm (str): Daily notional volume as a decimal string.
        impact_px (Optional[str]): Impact price as a decimal string, or None.
        open_interest (str): Current open interest as a decimal string.
        premium (str): Premium as a decimal string.
        oracle_px (str): Oracle price as a decimal string.
        mid_px (str): Mid price as a decimal string.
        impact_pxs (list[str], optional): Impact prices as list of decimal strings.
        day_base_vlm (str): Daily base volume as a decimal string.
    """

    name: RawAssetString64HL | None = Field(None, alias="name")
    funding: RawFiniteDecimalStr = Field(..., alias="funding")
    mark_px: RawFiniteDecimalStr = Field(..., alias="markPx")
    prev_day_px: RawFiniteDecimalStr = Field(..., alias="prevDayPx")
    day_ntl_vlm: RawFiniteDecimalStr = Field(..., alias="dayNtlVlm")
    impact_px: RawFiniteDecimalStr | None = Field(None, alias="impactPx")
    open_interest: RawFiniteDecimalStr = Field(..., alias="openInterest")
    premium: RawFiniteDecimalStr | None = Field(None, alias="premium")
    oracle_px: RawFiniteDecimalStr = Field(..., alias="oraclePx")
    mid_px: RawFiniteDecimalStr | None = Field(None, alias="midPx")
    impact_pxs: list[RawFiniteDecimalStr] | None = Field(None, alias="impactPxs")
    day_base_vlm: RawFiniteDecimalStr = Field(..., alias="dayBaseVlm")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawMetaResponse(BaseModel):
    """Strict boundary model for the top-level 'meta' response from the Hyperliquid API.

    Used only for validating the raw structure of the 'meta' endpoint response, which contains
    the universe of tradable assets. Never use for internal business logic.

    Fields:
        universe (List[HyperliquidRawAssetDefinition]): List of asset definitions.
        margin_tables (Any, optional): Margin tables data from the API.
    """

    universe: list[HyperliquidRawAssetDefinition] = Field(..., alias="universe")
    margin_tables: Any | None = Field(None, alias="marginTables")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawMetaAndAssetCtxsResponse(BaseModel):
    """Strict boundary model for the [meta, assetCtxs] tuple response.

    This model validates the raw structure of the 2-tuple response from the 'metaAndAssetCtxs'
    endpoint, containing both meta information and asset contexts. It uses Pydantic's
    model_validator to handle the tuple format returned by the API.
    Never use for internal business logic.

    Fields:
        meta (HyperliquidRawMetaResponse): Meta/universe information.
        asset_ctxs (List[HyperliquidRawAssetCtx]): List of asset context objects.
    """

    meta: HyperliquidRawMetaResponse
    asset_ctxs: list[HyperliquidRawAssetCtx]

    @model_validator(mode="before")
    @classmethod
    def preprocess_tuple_response(cls, values: object) -> dict[str, object]:
        """Transform [meta, assetCtxs] tuple into dict structure for validation.

        This validator replaces the preprocessing mapper logic by handling the
        tuple response format from the metaAndAssetCtxs endpoint.
        """
        # Validate input structure
        list_obj = cls._validate_input_structure(values)
        meta_dict, asset_ctxs_list = cls._extract_tuple_elements(list_obj)

        # Preprocess meta section
        preprocessed_meta = cls._preprocess_meta_dict(meta_dict)

        # Preprocess asset contexts with name enrichment from universe
        preprocessed_asset_ctxs = cls._preprocess_asset_ctxs_list(
            asset_ctxs_list, preprocessed_meta
        )

        return {"meta": preprocessed_meta, "asset_ctxs": preprocessed_asset_ctxs}

    @classmethod
    def _validate_input_structure(cls, obj: object) -> list[object]:
        """Validate that input is a 2-element list."""
        if not isinstance(obj, list):
            raise ValueError("Invalid MetaAndAssetCtxs response: not a list")

        list_obj = cast(list[object], obj)

        if len(list_obj) != 2:
            raise ValueError("Invalid MetaAndAssetCtxs response: not a 2-element list")

        return list_obj

    @classmethod
    def _extract_tuple_elements(cls, list_obj: list[object]) -> tuple[dict[str, Any], list[object]]:
        """Extract and validate meta and asset_ctxs elements from tuple."""
        meta_obj_raw = list_obj[0]
        asset_ctxs_list_raw = list_obj[1]

        if not isinstance(meta_obj_raw, dict):
            raise ValueError("Invalid MetaAndAssetCtxs response: first element (meta) must be dict")
        if not isinstance(asset_ctxs_list_raw, list):
            raise ValueError(
                "Invalid MetaAndAssetCtxs response: second element (asset_ctxs) must be list",
            )

        meta_dict = cast(dict[str, Any], meta_obj_raw)

        asset_ctxs_list_of_objects = cast(list[object], asset_ctxs_list_raw)

        return meta_dict, asset_ctxs_list_of_objects

    @classmethod
    def _preprocess_meta_dict(cls, meta_dict: dict[str, object]) -> dict[str, object]:
        """Preprocess the meta dictionary for Pydantic validation."""
        # Create a mutable copy for preprocessing
        meta_data = dict(meta_dict)

        # Keep marginTables field if present (model handles it as optional)

        # Preprocess universe items
        if "universe" in meta_data and isinstance(meta_data["universe"], list):
            preprocessed_universe: list[object] = []
            for item in meta_data["universe"]:
                if isinstance(item, dict):
                    # Create a copy to avoid modifying original
                    item_copy = dict(item)
                    # Add 'onlyIsolated' if missing with default False
                    if "onlyIsolated" not in item_copy:
                        item_copy["onlyIsolated"] = False
                    preprocessed_universe.append(item_copy)
                else:
                    preprocessed_universe.append(item)
            meta_data["universe"] = preprocessed_universe

        return meta_data

    @classmethod
    def _preprocess_asset_ctxs_list(
        cls,
        asset_ctxs_list: list[object],
        preprocessed_meta: dict[str, object],
    ) -> list[dict[str, object]]:
        """Preprocess asset contexts list with name enrichment from universe."""
        preprocessed_ctxs: list[dict[str, object]] = []

        # Extract universe for name enrichment
        universe = preprocessed_meta.get("universe", [])
        if not isinstance(universe, list):
            universe = []

        for i, item_obj in enumerate(asset_ctxs_list):
            if not isinstance(item_obj, dict):
                raise ValueError(f"Invalid MetaAndAssetCtxs: asset_ctxs[{i}] must be a dictionary")

            item_dict = cast(dict[str, object], item_obj)

            # Create a copy to avoid modifying original
            processed_item = dict(item_dict)

            # Add name from universe if available and not already present
            if i < len(universe) and "name" not in processed_item:
                universe_item = universe[i]
                if isinstance(universe_item, dict) and "name" in universe_item:
                    processed_item["name"] = universe_item["name"]

            preprocessed_ctxs.append(processed_item)

        return preprocessed_ctxs

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawMetaRequestPayload(BaseModel):
    """Strict boundary model for the request payload for the 'meta' info type.

    Used only for constructing and validating the payload sent to the Hyperliquid API when
    requesting meta/universe information. Never use for internal business logic.

    Fields:
        type (Literal['meta']): Must be 'meta'.
    """

    type: Annotated[
        Literal["meta"],
        BeforeValidator(
            lambda v: validate_str_field(v, field_name="type", max_length=32, allow_empty=False),
        ),
    ] = Field("meta", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawMetaAndAssetCtxsRequestPayload(BaseModel):
    """Strict boundary model for the request payload for the 'metaAndAssetCtxs' info type.

    Used only for constructing and validating the payload sent to the Hyperliquid API when
    requesting both meta and asset context information. Never use for internal business logic.

    Fields:
        type (Literal['metaAndAssetCtxs']): Must be 'metaAndAssetCtxs'.
    """

    type: Annotated[
        Literal["metaAndAssetCtxs"],
        BeforeValidator(
            lambda v: validate_str_field(v, field_name="type", max_length=32, allow_empty=False),
        ),
    ] = Field("metaAndAssetCtxs", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawAllMetaRequestPayload(BaseModel):
    """Strict boundary model for the request payload for the 'allMeta' info type.

    Fields:
        type (Literal['allMeta']): Must be 'allMeta'.
    """

    type: Annotated[
        Literal["allMeta"],
        BeforeValidator(
            lambda v: validate_str_field(v, field_name="type", max_length=32, allow_empty=False),
        ),
    ] = Field("allMeta", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawUpdateLeverageRequest(BaseModel):
    """Strict boundary model for updating leverage settings for a specific asset.

    Used only for constructing and validating the payload sent to the Hyperliquid API when
    updating leverage for an asset. This model ensures proper validation of leverage
    parameters before sending to the API. Never use for internal business logic.

    Fields:
        asset (int): Asset index (API-defined).
        is_cross (bool): True for cross margin, False for isolated.
        leverage (int): Leverage value to set.
    """

    asset: RawNonNegativeInt = Field(..., alias="asset")
    is_cross: RawStrictBool = Field(..., alias="isCross")
    leverage: RawNonNegativeInt = Field(..., alias="leverage")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawUpdateIsolatedMarginRequest(BaseModel):
    """Strict boundary model for updating isolated margin for a specific asset.

    Used only for constructing and validating the payload sent to the Hyperliquid API when updating
    isolated margin for an asset. Never use for internal business logic.

    Fields:
        asset (int): Asset index (API-defined).
        is_buy (bool): True for buy, False for sell.
        ntli (int): Notional amount to update.
    """

    asset: RawNonNegativeInt = Field(..., alias="asset")
    is_buy: RawStrictBool = Field(..., alias="isBuy")
    ntli: RawNonNegativeInt = Field(..., alias="ntli")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

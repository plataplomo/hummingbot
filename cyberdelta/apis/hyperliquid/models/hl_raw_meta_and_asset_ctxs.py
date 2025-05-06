"""
CyberDeltaEngine: Hyperliquid API Raw Models (Meta & Asset Context Group)
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

from typing import Any, Literal, Self, TypeGuard

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


def _validated_list_of_dict_str_any(obj: object) -> list[dict[str, Any]] | None:
    """
    Helper for runtime validation and static type narrowing: returns a list of dict[str, Any]
    if valid, else None. This is the only way to satisfy both runtime and static type safety
    without cast.
    """
    if not isinstance(obj, list):
        return None
    obj_list: list[Any] = obj
    result: list[dict[str, Any]] = []
    for item_obj in obj_list:
        if not isinstance(item_obj, dict):
            return None
        item: dict[str, Any] = item_obj
        keys: list[Any] = list(item.keys())
        for k in keys:
            if not isinstance(k, str):
                return None
        result.append(item)
    return result


def is_list_of_dict_str_any(obj: object) -> TypeGuard[list[dict[str, Any]]]:
    return _validated_list_of_dict_str_any(obj) is not None


class HyperliquidRawAssetDefinition(BaseModel):
    """
    Strict boundary model for a single asset/market definition from the Hyperliquid 'meta' endpoint.

    This model is used exclusively for validating the raw structure of asset entries in the upstream
    API response. It enforces strict type and format constraints to prevent malformed or ambiguous
    data from entering the system. Never use for internal business logic.

    Fields:
        name (str): Asset symbol (e.g., 'ETH', 'BTC').
        sz_decimals (int): Number of decimals for size/quantity precision (0-18).
        max_leverage (int): Maximum leverage allowed (0-1000).
        only_isolated (bool): True if only isolated margin is allowed for this asset.
    """

    name: str = Field(..., alias="name")
    sz_decimals: int = Field(..., alias="szDecimals")
    max_leverage: int = Field(..., alias="maxLeverage")
    only_isolated: bool = Field(..., alias="onlyIsolated")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'name' field to ensure it is a string of max length 64.
        This prevents malformed or excessively long asset symbols from passing boundary validation.
        """
        return validate_str_field(v, field_name="name", max_length=64)

    @field_validator("sz_decimals", mode="before")
    @classmethod
    def validate_sz_decimals(cls, v: object, info: ValidationInfo) -> int:
        """
        Validates the 'sz_decimals' field to ensure it is an integer in [0, 18].
        This enforces correct precision constraints for asset sizes.
        """
        if not isinstance(v, int):
            raise ValueError("sz_decimals: Expected int")
        if v < 0 or v > 18:
            raise ValueError("sz_decimals: Must be between 0 and 18")
        return v

    @field_validator("max_leverage", mode="before")
    @classmethod
    def validate_max_leverage(cls, v: object, info: ValidationInfo) -> int:
        """
        Validates the 'max_leverage' field to ensure it is an integer in [0, 1000].
        This prevents unsafe leverage values from entering the system.
        """
        if not isinstance(v, int):
            raise ValueError("max_leverage: Expected int")
        if v < 0 or v > 1000:
            raise ValueError("max_leverage: Must be between 0 and 1000")
        return v

    @field_validator("only_isolated", mode="before")
    @classmethod
    def validate_only_isolated(cls, v: object, info: ValidationInfo) -> bool:
        """
        Validates the 'only_isolated' field to ensure it is a boolean.
        This enforces strict type safety for margin mode flags.
        """
        if not isinstance(v, bool):
            raise ValueError("only_isolated: Expected bool")
        return v


class HyperliquidRawAssetCtx(BaseModel):
    """
    Strict boundary model for contextual information about a single asset from the
    'metaAndAssetCtxs' endpoint.

    Used only for validating the raw structure of asset context entries (funding, mark price, etc.)
    as received from the upstream API. Enforces strict type and format constraints for all fields.
    Never use for internal business logic.

    Fields:
        name (str): Asset symbol (max 64 chars).
        funding (str): Hourly funding rate as a decimal string.
        mark_px (str): Mark price as a decimal string.
        prev_day_px (str): Previous day's price as a decimal string.
        day_ntl_vlm (str): Daily notional volume as a decimal string.
        impact_px (Optional[str]): Impact price as a decimal string, or None.
    """

    name: str = Field(..., alias="name")
    funding: str = Field(..., alias="funding")
    mark_px: str = Field(..., alias="markPx")
    prev_day_px: str = Field(..., alias="prevDayPx")
    day_ntl_vlm: str = Field(..., alias="dayNtlVlm")
    impact_px: str | None = Field(None, alias="impactPx")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'name' field to ensure it is a string of max length 64.
        """
        return validate_str_field(v, field_name="name", max_length=64)

    @field_validator("funding", "mark_px", "prev_day_px", "day_ntl_vlm", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the field is a string representing a finite decimal (not NaN/inf),
        with a maximum length of 64. This is critical for financial data integrity.
        """
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

    @field_validator("impact_px", mode="before")
    @classmethod
    def validate_impact_px(cls, v: object, info: ValidationInfo) -> str | None:
        """
        Validates the optional 'impact_px' field to ensure it is either None or a valid
        decimal string.
        """
        if v is None:
            return v
        field_name = info.field_name or "impact_px"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s


class HyperliquidRawMetaResponse(BaseModel):
    """
    Strict boundary model for the top-level 'meta' response from the Hyperliquid API.

    Used only for validating the raw structure of the 'meta' endpoint response, which contains
    the universe of tradable assets. Never use for internal business logic.

    Fields:
        universe (List[HyperliquidRawAssetDefinition]): List of asset definitions.
    """

    universe: list[HyperliquidRawAssetDefinition] = Field(..., alias="universe")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("universe", mode="before")
    @classmethod
    def validate_universe(cls, v: object, info: ValidationInfo) -> list[dict[str, Any]]:
        """
        Validates the 'universe' field to ensure it is a list of dicts with string keys.
        This is a critical boundary check to prevent malformed asset lists from entering the system.
        """
        if not is_list_of_dict_str_any(v):
            raise ValueError("universe: Expected a list of dict[str, Any] with str keys")
        # At this point, Pyright knows v is list[dict[str, Any]]
        return v


class HyperliquidRawMetaAndAssetCtxsResponse(BaseModel):
    """
    Strict boundary model for the [meta, assetCtxs] tuple response from the 'metaAndAssetCtxs'
    endpoint.

    Used only for validating the raw structure of the 2-tuple response: meta info and
    asset contexts. Never use for internal business logic.

    Fields:
        meta (HyperliquidRawMetaResponse): Meta/universe information.
        asset_ctxs (List[HyperliquidRawAssetCtx]): List of asset context objects.

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
        Custom validator for the [meta, assetCtxs] tuple response. Ensures the input is a
        list of length 2, with the first element a dict (meta) and the second a list of
        dicts (asset contexts). Raises ValueError if the structure is not as expected. This
        is essential for robust boundary validation of upstream API data.
        """
        if not isinstance(obj, list):
            raise ValueError("Invalid MetaAndAssetCtxs response structure: not a list")
        obj_list: list[Any] = obj
        if len(obj_list) != 2:
            raise ValueError("Invalid MetaAndAssetCtxs response structure: not a 2-element list")
        meta_obj_raw = obj_list[0]
        asset_ctxs_obj_raw = obj_list[1]
        if not isinstance(meta_obj_raw, dict):
            raise ValueError(
                "Invalid MetaAndAssetCtxs response structure: first element must be dict"
            )
        if not isinstance(asset_ctxs_obj_raw, list):
            raise ValueError(
                "Invalid MetaAndAssetCtxs response structure: second element must be list"
            )
        meta_obj: dict[str, Any] = meta_obj_raw
        asset_ctxs_obj: list[Any] = asset_ctxs_obj_raw
        asset_ctxs_checked = _validated_list_of_dict_str_any(asset_ctxs_obj)
        if asset_ctxs_checked is None:
            raise ValueError(
                "Invalid MetaAndAssetCtxs response structure: asset_ctxs must be "
                "list[dict[str, Any]]"
            )
        meta = HyperliquidRawMetaResponse.model_validate(meta_obj)
        asset_ctxs = [HyperliquidRawAssetCtx.model_validate(x) for x in asset_ctxs_checked]
        return cls(meta=meta, asset_ctxs=asset_ctxs)

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawMetaRequestPayload(BaseModel):
    """
    Strict boundary model for the request payload for the 'meta' info type.

    Used only for constructing and validating the payload sent to the Hyperliquid API when
    requesting meta/universe information. Never use for internal business logic.

    Fields:
        type (Literal['meta']): Must be 'meta'.
    """

    type: Literal["meta"] = Field("meta", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type(cls, v: object, info: ValidationInfo) -> str:
        """Ensures type is exactly 'meta'."""
        field_name = info.field_name or "type"
        s = validate_str_field(v, field_name=field_name, max_length=16)
        if s != "meta":
            raise ValueError(f"{field_name} must be 'meta', got '{s}'")
        return s


class HyperliquidRawMetaAndAssetCtxsRequestPayload(BaseModel):
    """
    Strict boundary model for the request payload for the 'metaAndAssetCtxs' info type.

    Used only for constructing and validating the payload sent to the Hyperliquid API when
    requesting both meta and asset context information. Never use for internal business logic.

    Fields:
        type (Literal['metaAndAssetCtxs']): Must be 'metaAndAssetCtxs'.
    """

    type: Literal["metaAndAssetCtxs"] = Field("metaAndAssetCtxs", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type(cls, v: object, info: ValidationInfo) -> str:
        """Ensures type is exactly 'metaAndAssetCtxs'."""
        field_name = info.field_name or "type"
        s = validate_str_field(v, field_name=field_name, max_length=32)
        if s != "metaAndAssetCtxs":
            raise ValueError(f"{field_name} must be 'metaAndAssetCtxs', got '{s}'")
        return s


class HyperliquidRawAllMetaRequestPayload(BaseModel):
    """
    Strict boundary model for the request payload for the 'allMeta' info type.

    Fields:
        type (Literal['allMeta']): Must be 'allMeta'.
    """

    type: Literal["allMeta"] = Field("allMeta", alias="type")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type(cls, v: object, info: ValidationInfo) -> str:
        """Ensures type is exactly 'allMeta'."""
        field_name = info.field_name or "type"
        s = validate_str_field(v, field_name=field_name, max_length=16)
        if s != "allMeta":
            raise ValueError(f"{field_name} must be 'allMeta', got '{s}'")
        return s


class HyperliquidRawUpdateLeverageRequest(BaseModel):
    """
    Strict boundary model for the request payload for updating leverage settings for a
    specific asset.

    Used only for constructing and validating the payload sent to the Hyperliquid API when
    updating leverage for an asset. Never use for internal business logic.

    Fields:
        asset (int): Asset index (API-defined).
        is_cross (bool): True for cross margin, False for isolated.
        leverage (int): Leverage value to set.
    """

    asset: int = Field(..., alias="asset")
    is_cross: bool = Field(..., alias="isCross")
    leverage: int = Field(..., alias="leverage")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("asset", mode="before")
    @classmethod
    def validate_asset(cls, v: object, info: ValidationInfo) -> int:
        """
        Validates the 'asset' field to ensure it is an integer (asset index).
        """
        if not isinstance(v, int):
            raise ValueError("asset: Expected int")
        if v < 0:
            raise ValueError("asset: Must be non-negative")
        return v

    @field_validator("is_cross", mode="before")
    @classmethod
    def validate_is_cross(cls, v: object, info: ValidationInfo) -> bool:
        """
        Validates the 'is_cross' field to ensure it is a boolean.
        """
        if not isinstance(v, bool):
            raise ValueError("is_cross: Expected bool")
        return v

    @field_validator("leverage", mode="before")
    @classmethod
    def validate_leverage(cls, v: object, info: ValidationInfo) -> int:
        """
        Validates the 'leverage' field to ensure it is an integer (leverage value).
        """
        if not isinstance(v, int):
            raise ValueError("leverage: Expected int")
        if v < 0 or v > 1000:
            raise ValueError("leverage: Must be between 0 and 1000")
        return v


class HyperliquidRawUpdateIsolatedMarginRequest(BaseModel):
    """
    Strict boundary model for the request payload for updating isolated margin for a specific asset.

    Used only for constructing and validating the payload sent to the Hyperliquid API when updating
    isolated margin for an asset. Never use for internal business logic.

    Fields:
        asset (int): Asset index (API-defined).
        is_buy (bool): True for buy, False for sell.
        ntli (int): Notional amount to update.
    """

    asset: int = Field(..., alias="asset")
    is_buy: bool = Field(..., alias="isBuy")
    ntli: int = Field(..., alias="ntli")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("asset", mode="before")
    @classmethod
    def validate_asset(cls, v: object, info: ValidationInfo) -> int:
        """
        Validates the 'asset' field to ensure it is an integer (asset index).
        """
        if not isinstance(v, int):
            raise ValueError("asset: Expected int")
        if v < 0:
            raise ValueError("asset: Must be non-negative")
        return v

    @field_validator("is_buy", mode="before")
    @classmethod
    def validate_is_buy(cls, v: object, info: ValidationInfo) -> bool:
        """
        Validates the 'is_buy' field to ensure it is a boolean.
        """
        if not isinstance(v, bool):
            raise ValueError("is_buy: Expected bool")
        return v

    @field_validator("ntli", mode="before")
    @classmethod
    def validate_ntli(cls, v: object, info: ValidationInfo) -> int:
        """
        Validates the 'ntli' field to ensure it is an integer (notional amount).
        """
        if not isinstance(v, int):
            raise ValueError("ntli: Expected int")
        if v < 0:
            raise ValueError("ntli: Must be non-negative")
        return v

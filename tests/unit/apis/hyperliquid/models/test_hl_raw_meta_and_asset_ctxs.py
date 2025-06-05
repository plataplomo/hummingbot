"""Unit tests for Hyperliquid Raw Meta and Asset Context Models."""

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsRequestPayload,
    HyperliquidRawMetaAndAssetCtxsResponse,
    HyperliquidRawMetaRequestPayload,
    HyperliquidRawMetaResponse,
    HyperliquidRawUpdateIsolatedMarginRequest,
    HyperliquidRawUpdateLeverageRequest,
)


# --- HyperliquidRawAssetDefinition ---
def test_asset_definition_happy_path() -> None:
    """Test asset definition happy path."""
    obj = {
        "name": "ETH",
        "szDecimals": 6,
        "maxLeverage": 50,
        "onlyIsolated": True,
    }
    model = HyperliquidRawAssetDefinition.model_validate(obj)
    assert model.name == "ETH"
    assert model.sz_decimals == 6
    assert model.max_leverage == 50
    assert model.only_isolated is True


def test_asset_definition_missing_required() -> None:
    """Test asset definition missing required."""
    obj = {"szDecimals": 6, "maxLeverage": 50, "onlyIsolated": True}
    with pytest.raises(ValidationError):
        HyperliquidRawAssetDefinition.model_validate(obj)


def test_asset_definition_type_errors() -> None:
    """Test asset definition type errors."""
    obj = {"name": 123, "szDecimals": "6", "maxLeverage": "50", "onlyIsolated": "yes"}
    with pytest.raises(ValidationError):
        HyperliquidRawAssetDefinition.model_validate(obj)


def test_asset_definition_extra_field() -> None:
    """Test asset definition extra field."""
    obj = {"name": "ETH", "szDecimals": 6, "maxLeverage": 50, "onlyIsolated": True, "foo": 1}
    with pytest.raises(ValidationError):
        HyperliquidRawAssetDefinition.model_validate(obj)


def test_asset_definition_constraints() -> None:
    """Test asset definition constraints."""
    obj = {"name": "E" * 65, "szDecimals": -1, "maxLeverage": 2000, "onlyIsolated": True}
    with pytest.raises(ValidationError):
        HyperliquidRawAssetDefinition.model_validate(obj)


def test_asset_definition_unicode_name() -> None:
    """Test asset definition unicode name."""
    # Unicode asset name
    obj = {"name": "ΞTH", "szDecimals": 6, "maxLeverage": 50, "onlyIsolated": True}
    model = HyperliquidRawAssetDefinition.model_validate(obj)
    assert model.name == "ΞTH"


def test_asset_definition_whitespace_name() -> None:
    """Test asset definition whitespace name."""
    # Asset name with whitespace
    obj = {"name": "BTC USD", "szDecimals": 6, "maxLeverage": 50, "onlyIsolated": False}
    model = HyperliquidRawAssetDefinition.model_validate(obj)
    assert model.name == "BTC USD"


def test_asset_definition_control_char_name() -> None:
    """Test asset definition control char name."""
    # Asset name with control character
    obj = {"name": "ETH\n", "szDecimals": 6, "maxLeverage": 50, "onlyIsolated": True}
    model = HyperliquidRawAssetDefinition.model_validate(obj)
    assert model.name == "ETH\n"


def test_asset_definition_min_max_values() -> None:
    """Test asset definition min max values."""
    # Min/max values for szDecimals and maxLeverage
    obj = {"name": "MIN", "szDecimals": 0, "maxLeverage": 1, "onlyIsolated": False}
    model = HyperliquidRawAssetDefinition.model_validate(obj)
    assert model.sz_decimals == 0
    obj2 = {"name": "MAX", "szDecimals": 18, "maxLeverage": 1000, "onlyIsolated": True}
    model2 = HyperliquidRawAssetDefinition.model_validate(obj2)
    assert model2.max_leverage == 1000


def test_asset_definition_testnet_name() -> None:
    """Test asset definition testnet name."""
    # Testnet asset name
    obj = {"name": "tETH", "szDecimals": 6, "maxLeverage": 50, "onlyIsolated": True}
    model = HyperliquidRawAssetDefinition.model_validate(obj)
    assert model.name == "tETH"


# --- HyperliquidRawAssetCtx ---
def test_asset_ctx_happy_path() -> None:
    """Test asset ctx happy path."""
    obj = {
        "name": "BTC",
        "funding": "0.0001",
        "markPx": "30000.0",
        "prevDayPx": "29500.0",
        "dayNtlVlm": "1000000.0",
        "impactPx": "0.1",
    }
    model = HyperliquidRawAssetCtx.model_validate(obj)
    assert model.name == "BTC"
    assert model.funding == "0.0001"
    assert model.impact_px == "0.1"


def test_asset_ctx_optional_impact_px() -> None:
    """Test asset ctx optional impact px."""
    obj = {
        "name": "BTC",
        "funding": "0.0001",
        "markPx": "30000.0",
        "prevDayPx": "29500.0",
        "dayNtlVlm": "1000000.0",
    }
    model = HyperliquidRawAssetCtx.model_validate(obj)
    assert model.impact_px is None


def test_asset_ctx_invalid_decimal() -> None:
    """Test asset ctx invalid decimal."""
    obj = {
        "name": "BTC",
        "funding": "NaN",
        "markPx": "inf",
        "prevDayPx": "29500.0",
        "dayNtlVlm": "1000000.0",
        "impactPx": "-inf",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawAssetCtx.model_validate(obj)


def test_asset_ctx_extra_field() -> None:
    """Test asset ctx extra field."""
    obj = {
        "name": "BTC",
        "funding": "0.0001",
        "markPx": "30000.0",
        "prevDayPx": "29500.0",
        "dayNtlVlm": "1000000.0",
        "impactPx": "0.1",
        "foo": 1,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawAssetCtx.model_validate(obj)


def test_asset_ctx_missing_optional_impact_px() -> None:
    """Test asset ctx missing optional impact px."""
    # impactPx omitted
    obj = {
        "name": "BTC",
        "funding": "0.0001",
        "markPx": "30000.0",
        "prevDayPx": "29500.0",
        "dayNtlVlm": "1000000.0",
    }
    model = HyperliquidRawAssetCtx.model_validate(obj)
    assert model.impact_px is None


def test_asset_ctx_impact_px_zero_negative() -> None:
    """Test asset ctx impact px zero negative."""
    # impactPx as zero and negative (both should be accepted)
    obj = {
        "name": "BTC",
        "funding": "0.0001",
        "markPx": "30000.0",
        "prevDayPx": "29500.0",
        "dayNtlVlm": "1000000.0",
        "impactPx": "0",
    }
    model = HyperliquidRawAssetCtx.model_validate(obj)
    assert model.impact_px == "0"
    obj2 = {
        "name": "BTC",
        "funding": "0.0001",
        "markPx": "30000.0",
        "prevDayPx": "29500.0",
        "dayNtlVlm": "1000000.0",
        "impactPx": "-1.0",
    }
    HyperliquidRawAssetCtx.model_validate(obj2)  # Should not raise


def test_asset_ctx_excessive_precision() -> None:
    """Test asset ctx excessive precision."""
    # Excessive precision in funding
    obj = {
        "name": "BTC",
        "funding": "0.12345678901234567890",
        "markPx": "30000.0",
        "prevDayPx": "29500.0",
        "dayNtlVlm": "1000000.0",
    }
    model = HyperliquidRawAssetCtx.model_validate(obj)
    assert model.funding == "0.12345678901234567890"


def test_asset_ctx_all_zero_negative_large() -> None:
    """Test asset ctx all zero negative large."""
    # All fields as zero, negative, or large (should be accepted)
    obj = {
        "name": "BTC",
        "funding": "0",
        "markPx": "0",
        "prevDayPx": "0",
        "dayNtlVlm": "0",
        "impactPx": "0",
    }
    model = HyperliquidRawAssetCtx.model_validate(obj)
    assert model.funding == "0"
    obj2 = {
        "name": "BTC",
        "funding": "-0.1",
        "markPx": "-1",
        "prevDayPx": "-1",
        "dayNtlVlm": "-1",
        "impactPx": "-1",
    }
    HyperliquidRawAssetCtx.model_validate(obj2)  # Should not raise
    obj3 = {
        "name": "BTC",
        "funding": "1e1000",
        "markPx": "1e1000",
        "prevDayPx": "1e1000",
        "dayNtlVlm": "1e1000",
        "impactPx": "1e1000",
    }
    HyperliquidRawAssetCtx.model_validate(obj3)  # Should not raise


# --- HyperliquidRawMetaResponse ---
def test_meta_response_happy_path() -> None:
    """Test meta response happy path."""
    obj = {
        "universe": [
            {"name": "ETH", "szDecimals": 6, "maxLeverage": 50, "onlyIsolated": True},
            {"name": "BTC", "szDecimals": 6, "maxLeverage": 100, "onlyIsolated": False},
        ],
    }
    model = HyperliquidRawMetaResponse.model_validate(obj)
    assert len(model.universe) == 2


def test_meta_response_invalid_universe() -> None:
    """Test meta response invalid universe."""
    obj = {"universe": "notalist"}
    with pytest.raises(ValidationError):
        HyperliquidRawMetaResponse.model_validate(obj)


def test_meta_response_empty_universe() -> None:
    """Test meta response empty universe."""
    # Empty universe
    obj: dict[str, list[dict[str, object]]] = {"universe": []}
    model = HyperliquidRawMetaResponse.model_validate(obj)
    assert model.universe == []


def test_meta_response_one_asset() -> None:
    """Test meta response one asset."""
    # Universe with one asset
    obj = {"universe": [{"name": "ETH", "szDecimals": 6, "maxLeverage": 50, "onlyIsolated": True}]}
    model = HyperliquidRawMetaResponse.model_validate(obj)
    assert len(model.universe) == 1


# --- HyperliquidRawMetaAndAssetCtxsResponse ---
def test_meta_and_asset_ctxs_response_happy_path() -> None:
    """Test meta and asset ctxs response happy path."""
    obj = [
        {"universe": [{"name": "ETH", "szDecimals": 6, "maxLeverage": 50, "onlyIsolated": True}]},
        [
            {
                "name": "ETH",
                "funding": "0.0001",
                "markPx": "30000.0",
                "prevDayPx": "29500.0",
                "dayNtlVlm": "1000000.0",
            },
        ],
    ]
    model = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(obj)
    assert model.meta.universe[0].name == "ETH"
    assert model.asset_ctxs[0].name == "ETH"


def test_meta_and_asset_ctxs_response_invalid_structure() -> None:
    """Test meta and asset ctxs response invalid structure."""
    obj = {"not": "alist"}
    with pytest.raises(ValueError):
        HyperliquidRawMetaAndAssetCtxsResponse.model_validate(obj)


def test_meta_and_asset_ctxs_response_wrong_tuple_length() -> None:
    """Test meta and asset ctxs response wrong tuple length."""
    # Wrong tuple length (should be 2)
    obj: list[dict[str, list[dict[str, object]]]] = [{"universe": []}]
    with pytest.raises(ValueError):
        HyperliquidRawMetaAndAssetCtxsResponse.model_validate(obj)
    obj2: list[object] = [{"universe": []}, [], []]
    with pytest.raises(ValueError):
        HyperliquidRawMetaAndAssetCtxsResponse.model_validate(obj2)


def test_meta_and_asset_ctxs_response_wrong_types() -> None:
    """Test meta and asset ctxs response wrong types."""
    # Wrong types in tuple
    obj: list[list[object]] = [[], []]
    with pytest.raises(ValueError):
        HyperliquidRawMetaAndAssetCtxsResponse.model_validate(obj)


def test_meta_and_asset_ctxs_response_empty_lists() -> None:
    """Test meta and asset ctxs response empty lists."""
    # Both meta and asset_ctxs empty
    meta: dict[str, list[dict[str, object]]] = {"universe": []}
    obj: list[object] = [meta, []]
    model = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(obj)
    assert model.meta.universe == []
    assert model.asset_ctxs == []


# --- HyperliquidRawMetaRequestPayload ---
def test_meta_request_payload_happy_path() -> None:
    """Test meta request payload happy path."""
    obj = {"type": "meta"}
    model = HyperliquidRawMetaRequestPayload.model_validate(obj)
    assert model.type == "meta"


def test_meta_request_payload_invalid_type() -> None:
    """Test meta request payload invalid type."""
    obj = {"type": "notmeta"}
    with pytest.raises(ValidationError):
        HyperliquidRawMetaRequestPayload.model_validate(obj)


# --- HyperliquidRawMetaAndAssetCtxsRequestPayload ---
def test_meta_and_asset_ctxs_request_payload_happy_path() -> None:
    """Test meta and asset ctxs request payload happy path."""
    obj = {"type": "metaAndAssetCtxs"}
    model = HyperliquidRawMetaAndAssetCtxsRequestPayload.model_validate(obj)
    assert model.type == "metaAndAssetCtxs"


def test_meta_and_asset_ctxs_request_payload_invalid_type() -> None:
    """Test meta and asset ctxs request payload invalid type."""
    obj = {"type": "notmetaandassetctxs"}
    with pytest.raises(ValidationError):
        HyperliquidRawMetaAndAssetCtxsRequestPayload.model_validate(obj)


# --- HyperliquidRawUpdateLeverageRequest ---
def test_update_leverage_request_happy_path() -> None:
    """Test update leverage request happy path."""
    obj = {"asset": 1, "isCross": True, "leverage": 10}
    model = HyperliquidRawUpdateLeverageRequest.model_validate(obj)
    assert model.asset == 1
    assert model.is_cross is True
    assert model.leverage == 10


def test_update_leverage_request_invalid_types() -> None:
    """Test update leverage request invalid types."""
    obj = {"asset": "one", "isCross": "yes", "leverage": "ten"}
    with pytest.raises(ValidationError):
        HyperliquidRawUpdateLeverageRequest.model_validate(obj)


def test_update_leverage_request_bounds() -> None:
    """Test update leverage request bounds."""
    obj = {"asset": -1, "isCross": True, "leverage": 2000}
    with pytest.raises(ValidationError):
        HyperliquidRawUpdateLeverageRequest.model_validate(obj)


# --- HyperliquidRawUpdateIsolatedMarginRequest ---
def test_update_isolated_margin_request_happy_path() -> None:
    """Test update isolated margin request happy path."""
    obj = {"asset": 1, "isBuy": False, "ntli": 100}
    model = HyperliquidRawUpdateIsolatedMarginRequest.model_validate(obj)
    assert model.asset == 1
    assert model.is_buy is False
    assert model.ntli == 100


def test_update_isolated_margin_request_invalid_types() -> None:
    """Test update isolated margin request invalid types."""
    obj = {"asset": "one", "isBuy": "no", "ntli": "hundred"}
    with pytest.raises(ValidationError):
        HyperliquidRawUpdateIsolatedMarginRequest.model_validate(obj)


def test_update_isolated_margin_request_bounds() -> None:
    """Test update isolated margin request bounds."""
    obj = {"asset": -1, "isBuy": True, "ntli": -100}
    with pytest.raises(ValidationError):
        HyperliquidRawUpdateIsolatedMarginRequest.model_validate(obj)

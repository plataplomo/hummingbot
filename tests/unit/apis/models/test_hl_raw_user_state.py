import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
    HyperliquidRawLeverage,
    HyperliquidRawMarginSummary,
    HyperliquidRawPositionInfo,
)


# --- Helpers for valid payloads ---
def valid_leverage() -> dict[str, object]:
    return {"type": "cross", "value": 5}


def valid_position_info() -> dict[str, object]:
    return {
        "coin": "ETH",
        "entryPx": "1234.56",
        "leverage": valid_leverage(),
        "liquidationPx": "1000.00",
        "marginUsed": "100.00",
        "maxLeverage": 10,
        "positionValue": "200.00",
        "returnOnEquity": "0.05",
        "szi": "1.0",
        "unrealizedPnl": "0.01",
    }


def valid_asset_position() -> dict[str, object]:
    return {"asset": "ETH", "position": valid_position_info()}


def valid_margin_summary() -> dict[str, object]:
    return {
        "accountValue": "1000.00",
        "totalMarginUsed": "100.00",
        "totalNtlPos": "200.00",
        "totalRawUsd": "1000.00",
    }


def valid_clearinghouse_state() -> dict[str, object]:
    return {
        "assetPositions": [valid_asset_position()],
        "marginSummary": valid_margin_summary(),
        "crossMaintenanceMarginUsed": "10.00",
        "crossMarginSummary": valid_margin_summary(),
        "isolatedMaintenanceMarginUsed": "5.00",
        "isolatedMarginSummary": valid_margin_summary(),
        "withdrawable": "50.00",
    }


# --- HyperliquidRawLeverage ---
def test_leverage_happy_path() -> None:
    obj = HyperliquidRawLeverage.model_validate(valid_leverage())
    assert obj.type == "cross"
    assert obj.value == 5


def test_leverage_type_enum() -> None:
    d = valid_leverage()
    d["type"] = "isolated"
    obj = HyperliquidRawLeverage.model_validate(d)
    assert obj.type == "isolated"


def test_leverage_type_invalid_enum() -> None:
    d = valid_leverage()
    d["type"] = "CROSS"
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)
    d["type"] = "other"
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_type_wrong_type() -> None:
    d = valid_leverage()
    d["type"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_value_negative() -> None:
    d = valid_leverage()
    d["value"] = -1
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_value_wrong_type() -> None:
    d = valid_leverage()
    d["value"] = "5"
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_missing_fields() -> None:
    d = valid_leverage().copy()
    del d["type"]
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)
    d = valid_leverage().copy()
    del d["value"]
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_extra_field() -> None:
    d = valid_leverage().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_type_utf8_and_length() -> None:
    d = valid_leverage().copy()
    d["type"] = "a" * 17
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)
    d["type"] = "cross\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_type_adversarial() -> None:
    d = valid_leverage().copy()
    d["type"] = "' OR 1=1 --"
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)
    d["type"] = "<script>alert(1)</script>"
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)
    d["type"] = "💣"
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


# --- HyperliquidRawPositionInfo ---
def test_position_info_happy_path() -> None:
    obj = HyperliquidRawPositionInfo.model_validate(valid_position_info())
    assert obj.coin == "ETH"
    assert obj.leverage.type == "cross"
    assert obj.max_leverage == 10


def test_position_info_optional_fields() -> None:
    d = valid_position_info().copy()
    d["entryPx"] = None
    d["liquidationPx"] = None
    obj = HyperliquidRawPositionInfo.model_validate(d)
    assert obj.entry_px is None
    assert obj.liquidation_px is None
    d2 = valid_position_info().copy()
    del d2["entryPx"]
    del d2["liquidationPx"]
    obj2 = HyperliquidRawPositionInfo.model_validate(d2)
    assert obj2.entry_px is None or obj2.liquidation_px is None


def test_position_info_missing_required() -> None:
    for field in [
        "coin",
        "leverage",
        "marginUsed",
        "maxLeverage",
        "positionValue",
        "returnOnEquity",
        "szi",
        "unrealizedPnl",
    ]:
        d = valid_position_info().copy()
        del d[field]
        with pytest.raises(ValidationError):
            HyperliquidRawPositionInfo.model_validate(d)


def test_position_info_type_errors() -> None:
    d = valid_position_info().copy()
    d["coin"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawPositionInfo.model_validate(d)
    d = valid_position_info().copy()
    d["leverage"] = "notaleverage"
    with pytest.raises(ValidationError):
        HyperliquidRawPositionInfo.model_validate(d)
    d = valid_position_info().copy()
    d["maxLeverage"] = "10"
    with pytest.raises(ValidationError):
        HyperliquidRawPositionInfo.model_validate(d)


def test_position_info_decimal_format_errors() -> None:
    for field in [
        "entryPx",
        "liquidationPx",
        "marginUsed",
        "positionValue",
        "returnOnEquity",
        "szi",
        "unrealizedPnl",
    ]:
        d = valid_position_info().copy()
        d[field] = "notanumber"
        with pytest.raises(ValidationError):
            HyperliquidRawPositionInfo.model_validate(d)
        d[field] = "NaN"
        with pytest.raises(ValidationError):
            HyperliquidRawPositionInfo.model_validate(d)
        d[field] = ""
        with pytest.raises(ValidationError):
            HyperliquidRawPositionInfo.model_validate(d)
        d[field] = "1" * 65
        with pytest.raises(ValidationError):
            HyperliquidRawPositionInfo.model_validate(d)


def test_position_info_extra_field() -> None:
    d = valid_position_info().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawPositionInfo.model_validate(d)


def test_position_info_nested_leverage_error() -> None:
    d = valid_position_info().copy()
    d["leverage"] = {"type": "bad", "value": 5}
    with pytest.raises(ValidationError):
        HyperliquidRawPositionInfo.model_validate(d)


def test_position_info_adversarial_strings() -> None:
    # Adversarial but structurally valid strings should be accepted at the Raw boundary.
    d = valid_position_info().copy()
    d["coin"] = "' OR 1=1 --"
    obj = HyperliquidRawPositionInfo.model_validate(d)
    assert obj.coin == "' OR 1=1 --"
    d["coin"] = "<script>alert(1)</script>"
    obj = HyperliquidRawPositionInfo.model_validate(d)
    assert obj.coin == "<script>alert(1)</script>"
    d["coin"] = "💣"
    obj = HyperliquidRawPositionInfo.model_validate(d)
    assert obj.coin == "💣"


# --- HyperliquidRawAssetPosition ---
def test_asset_position_happy_path() -> None:
    obj = HyperliquidRawAssetPosition.model_validate(valid_asset_position())
    assert obj.asset == "ETH"
    assert obj.position.coin == "ETH"


def test_asset_position_missing_required() -> None:
    d = valid_asset_position().copy()
    del d["asset"]
    with pytest.raises(ValidationError):
        HyperliquidRawAssetPosition.model_validate(d)
    d = valid_asset_position().copy()
    del d["position"]
    with pytest.raises(ValidationError):
        HyperliquidRawAssetPosition.model_validate(d)


def test_asset_position_type_errors() -> None:
    d = valid_asset_position().copy()
    d["asset"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawAssetPosition.model_validate(d)
    d = valid_asset_position().copy()
    d["position"] = "notaposition"
    with pytest.raises(ValidationError):
        HyperliquidRawAssetPosition.model_validate(d)


def test_asset_position_extra_field() -> None:
    d = valid_asset_position().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawAssetPosition.model_validate(d)


def test_asset_position_adversarial() -> None:
    # Adversarial but structurally valid strings should be accepted at the Raw boundary.
    d = valid_asset_position().copy()
    d["asset"] = "' OR 1=1 --"
    obj = HyperliquidRawAssetPosition.model_validate(d)
    assert obj.asset == "' OR 1=1 --"
    d["asset"] = "<script>alert(1)</script>"
    obj = HyperliquidRawAssetPosition.model_validate(d)
    assert obj.asset == "<script>alert(1)</script>"
    d["asset"] = "💣"
    obj = HyperliquidRawAssetPosition.model_validate(d)
    assert obj.asset == "💣"


# --- HyperliquidRawMarginSummary ---
def test_margin_summary_happy_path() -> None:
    obj = HyperliquidRawMarginSummary.model_validate(valid_margin_summary())
    assert obj.account_value == "1000.00"


def test_margin_summary_missing_required() -> None:
    for field in ["accountValue", "totalMarginUsed", "totalNtlPos", "totalRawUsd"]:
        d = valid_margin_summary().copy()
        del d[field]
        with pytest.raises(ValidationError):
            HyperliquidRawMarginSummary.model_validate(d)


def test_margin_summary_type_and_format_errors() -> None:
    for field in ["accountValue", "totalMarginUsed", "totalNtlPos", "totalRawUsd"]:
        d = valid_margin_summary().copy()
        d[field] = 123
        with pytest.raises(ValidationError):
            HyperliquidRawMarginSummary.model_validate(d)
        d[field] = "notanumber"
        with pytest.raises(ValidationError):
            HyperliquidRawMarginSummary.model_validate(d)
        d[field] = "NaN"
        with pytest.raises(ValidationError):
            HyperliquidRawMarginSummary.model_validate(d)
        d[field] = ""
        with pytest.raises(ValidationError):
            HyperliquidRawMarginSummary.model_validate(d)
        d[field] = "1" * 65
        with pytest.raises(ValidationError):
            HyperliquidRawMarginSummary.model_validate(d)


def test_margin_summary_extra_field() -> None:
    d = valid_margin_summary().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawMarginSummary.model_validate(d)


def test_margin_summary_adversarial() -> None:
    d = valid_margin_summary().copy()
    d["accountValue"] = "' OR 1=1 --"
    with pytest.raises(ValidationError):
        HyperliquidRawMarginSummary.model_validate(d)
    d["accountValue"] = "<script>alert(1)</script>"
    with pytest.raises(ValidationError):
        HyperliquidRawMarginSummary.model_validate(d)
    d["accountValue"] = "💣"
    with pytest.raises(ValidationError):
        HyperliquidRawMarginSummary.model_validate(d)


# --- HyperliquidRawClearinghouseState ---
def test_clearinghouse_state_happy_path() -> None:
    obj = HyperliquidRawClearinghouseState.model_validate(valid_clearinghouse_state())
    assert obj.asset_positions[0].asset == "ETH"
    assert obj.margin_summary.account_value == "1000.00"


def test_clearinghouse_state_missing_required() -> None:
    for field in [
        "assetPositions",
        "marginSummary",
        "crossMaintenanceMarginUsed",
        "crossMarginSummary",
        "isolatedMaintenanceMarginUsed",
        "isolatedMarginSummary",
        "withdrawable",
    ]:
        d = valid_clearinghouse_state().copy()
        del d[field]
        with pytest.raises(ValidationError):
            HyperliquidRawClearinghouseState.model_validate(d)


def test_clearinghouse_state_type_errors() -> None:
    d = valid_clearinghouse_state().copy()
    d["assetPositions"] = "notalist"
    with pytest.raises(ValidationError):
        HyperliquidRawClearinghouseState.model_validate(d)
    d = valid_clearinghouse_state().copy()
    d["marginSummary"] = "notasummary"
    with pytest.raises(ValidationError):
        HyperliquidRawClearinghouseState.model_validate(d)
    d = valid_clearinghouse_state().copy()
    d["crossMaintenanceMarginUsed"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawClearinghouseState.model_validate(d)


def test_clearinghouse_state_decimal_format_errors() -> None:
    for field in ["crossMaintenanceMarginUsed", "isolatedMaintenanceMarginUsed", "withdrawable"]:
        d = valid_clearinghouse_state().copy()
        d[field] = "notanumber"
        with pytest.raises(ValidationError):
            HyperliquidRawClearinghouseState.model_validate(d)
        d[field] = "NaN"
        with pytest.raises(ValidationError):
            HyperliquidRawClearinghouseState.model_validate(d)
        d[field] = ""
        with pytest.raises(ValidationError):
            HyperliquidRawClearinghouseState.model_validate(d)
        d[field] = "1" * 65
        with pytest.raises(ValidationError):
            HyperliquidRawClearinghouseState.model_validate(d)


def test_clearinghouse_state_extra_field() -> None:
    d = valid_clearinghouse_state().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawClearinghouseState.model_validate(d)


def test_clearinghouse_state_nested_model_error() -> None:
    d = valid_clearinghouse_state().copy()
    d["marginSummary"] = {
        "accountValue": "notanumber",
        "totalMarginUsed": "100.00",
        "totalNtlPos": "200.00",
        "totalRawUsd": "1000.00",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawClearinghouseState.model_validate(d)


def test_clearinghouse_state_adversarial() -> None:
    d = valid_clearinghouse_state().copy()
    d["withdrawable"] = "' OR 1=1 --"
    with pytest.raises(ValidationError):
        HyperliquidRawClearinghouseState.model_validate(d)
    d["withdrawable"] = "<script>alert(1)</script>"
    with pytest.raises(ValidationError):
        HyperliquidRawClearinghouseState.model_validate(d)
    d["withdrawable"] = "💣"
    with pytest.raises(ValidationError):
        HyperliquidRawClearinghouseState.model_validate(d)

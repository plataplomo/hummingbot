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
    """Return a valid leverage dictionary for testing."""
    return {"type": "cross", "value": 5}


def valid_position_info() -> dict[str, object]:
    """Return a valid position info dictionary for testing."""
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
    """Return a valid asset position dictionary for testing."""
    return {"asset": "ETH", "position": valid_position_info()}


def valid_margin_summary() -> dict[str, object]:
    """Return a valid margin summary dictionary for testing."""
    return {
        "accountValue": "1000.00",
        "totalMarginUsed": "100.00",
        "totalNtlPos": "200.00",
        "totalRawUsd": "1000.00",
    }


def valid_clearinghouse_state() -> dict[str, object]:
    """Return a valid clearinghouse state dictionary for testing."""
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
    """Test successful leverage validation with valid data."""
    obj = HyperliquidRawLeverage.model_validate(valid_leverage())
    assert obj.type == "cross"
    assert obj.value == 5


def test_leverage_type_enum() -> None:
    """Test leverage type enum validation."""
    d = valid_leverage()
    d["type"] = "isolated"
    obj = HyperliquidRawLeverage.model_validate(d)
    assert obj.type == "isolated"


def test_leverage_type_invalid_enum() -> None:
    """Test leverage type invalid enum."""
    d = valid_leverage()
    d["type"] = "CROSS"
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)
    d["type"] = "other"
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_type_wrong_type() -> None:
    """Test leverage type wrong type."""
    d = valid_leverage()
    d["type"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_value_negative() -> None:
    """Test leverage value negative."""
    d = valid_leverage()
    d["value"] = -1
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_value_wrong_type() -> None:
    """Test leverage value wrong type."""
    d = valid_leverage()
    d["value"] = "5"
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_missing_fields() -> None:
    """Test leverage missing fields."""
    d = valid_leverage().copy()
    del d["type"]
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)
    d = valid_leverage().copy()
    del d["value"]
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_extra_field() -> None:
    """Test leverage extra field."""
    d = valid_leverage().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_type_utf8_and_length() -> None:
    """Test leverage type utf8 and length."""
    d = valid_leverage().copy()
    d["type"] = "a" * 17
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)
    d["type"] = "cross\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_type_adversarial() -> None:
    """Test leverage type adversarial."""
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


def test_leverage_type_mixed_scripts_and_emoji() -> None:
    """Test leverage type mixed scripts and emoji."""
    # Should fail for non-enum, but test mixed scripts and emoji for type
    d = valid_leverage().copy()
    d["type"] = "cross💹"
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)
    d["type"] = "крест"  # Cyrillic for 'cross'
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


def test_leverage_value_boundaries() -> None:
    """Test leverage value boundaries."""
    # Accept 0, large int; reject negative
    d = valid_leverage().copy()
    d["value"] = 0
    obj = HyperliquidRawLeverage.model_validate(d)
    assert obj.value == 0
    d["value"] = 2**31 - 1
    obj = HyperliquidRawLeverage.model_validate(d)
    assert obj.value == 2**31 - 1
    d["value"] = -999999
    with pytest.raises(ValidationError):
        HyperliquidRawLeverage.model_validate(d)


# --- HyperliquidRawPositionInfo ---
def test_position_info_happy_path() -> None:
    """Test position info happy path."""
    obj = HyperliquidRawPositionInfo.model_validate(valid_position_info())
    assert obj.coin == "ETH"
    assert obj.leverage.type == "cross"
    assert obj.max_leverage == 10


def test_position_info_optional_fields() -> None:
    """Test position info optional fields."""
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
    """Test position info missing required."""
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
    """Test position info type errors."""
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
    """Test position info decimal format errors."""
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
    """Test position info extra field."""
    d = valid_position_info().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawPositionInfo.model_validate(d)


def test_position_info_nested_leverage_error() -> None:
    """Test position info nested leverage error."""
    d = valid_position_info().copy()
    d["leverage"] = {"type": "bad", "value": 5}
    with pytest.raises(ValidationError):
        HyperliquidRawPositionInfo.model_validate(d)


def test_position_info_adversarial_strings() -> None:
    """Test position info adversarial strings."""
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


def test_position_info_coin_edge_cases() -> None:
    """Test position info coin edge cases."""
    # Accept coin with emoji, excessive whitespace, symbols, bidirectional text
    for coin in [
        "ETH 💎",
        "   BTC   ",
        "COIN-123!@#",
        "\u202eABC\u202c",  # mirrored
    ]:
        d = valid_position_info().copy()
        d["coin"] = coin
        obj = HyperliquidRawPositionInfo.model_validate(d)
        assert obj.coin == coin


def test_position_info_decimal_leading_trailing_zeros() -> None:
    """Test position info decimal leading trailing zeros."""
    # Accept decimals with leading/trailing zeros and scientific notation
    d = valid_position_info().copy()
    d["entryPx"] = "000123.4500"
    obj = HyperliquidRawPositionInfo.model_validate(d)
    assert obj.entry_px == "000123.4500"
    # Scientific notation is allowed (Decimal accepts it and it's finite)
    d["entryPx"] = "1.23e2"
    obj = HyperliquidRawPositionInfo.model_validate(d)
    assert obj.entry_px == "1.23e2"


def test_position_info_optional_fields_empty_or_whitespace() -> None:
    """Test position info optional fields empty or whitespace."""
    # Should fail for empty/whitespace, pass for None
    # Use correct snake_case attribute names for assertions
    field_map = {"entryPx": "entry_px", "liquidationPx": "liquidation_px"}
    for field in ["entryPx", "liquidationPx"]:
        d = valid_position_info().copy()
        d[field] = ""
        with pytest.raises(ValidationError):
            HyperliquidRawPositionInfo.model_validate(d)
        d[field] = "   "
        with pytest.raises(ValidationError):
            HyperliquidRawPositionInfo.model_validate(d)
        d[field] = None
        obj = HyperliquidRawPositionInfo.model_validate(d)
        # Use the correct attribute name for the model
        assert getattr(obj, field_map[field]) is None


# --- HyperliquidRawAssetPosition ---
def test_asset_position_happy_path() -> None:
    """Test asset position happy path."""
    obj = HyperliquidRawAssetPosition.model_validate(valid_asset_position())
    assert obj.asset == "ETH"
    assert obj.position.coin == "ETH"


def test_asset_position_missing_required() -> None:
    """Test asset position missing required."""
    d = valid_asset_position().copy()
    del d["asset"]
    with pytest.raises(ValidationError):
        HyperliquidRawAssetPosition.model_validate(d)
    d = valid_asset_position().copy()
    del d["position"]
    with pytest.raises(ValidationError):
        HyperliquidRawAssetPosition.model_validate(d)


def test_asset_position_type_errors() -> None:
    """Test asset position type errors."""
    d = valid_asset_position().copy()
    d["asset"] = 123
    with pytest.raises(ValidationError):
        HyperliquidRawAssetPosition.model_validate(d)
    d = valid_asset_position().copy()
    d["position"] = "notaposition"
    with pytest.raises(ValidationError):
        HyperliquidRawAssetPosition.model_validate(d)


def test_asset_position_extra_field() -> None:
    """Test asset position extra field."""
    d = valid_asset_position().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawAssetPosition.model_validate(d)


def test_asset_position_adversarial() -> None:
    """Test asset position adversarial."""
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


def test_asset_position_asset_symbols_and_punctuation() -> None:
    """Test asset position asset symbols and punctuation."""
    # Accept asset with symbols, punctuation, emoji
    for asset in [
        "BTC-USD",
        "ASSET!@#",
        "COIN💰",
        "\u202eASSET\u202c",
    ]:
        d = valid_asset_position().copy()
        d["asset"] = asset
        obj = HyperliquidRawAssetPosition.model_validate(d)
        assert obj.asset == asset


# --- HyperliquidRawMarginSummary ---
def test_margin_summary_happy_path() -> None:
    """Test margin summary happy path."""
    obj = HyperliquidRawMarginSummary.model_validate(valid_margin_summary())
    assert obj.account_value == "1000.00"


def test_margin_summary_missing_required() -> None:
    """Test margin summary missing required."""
    for field in ["accountValue", "totalMarginUsed", "totalNtlPos", "totalRawUsd"]:
        d = valid_margin_summary().copy()
        del d[field]
        with pytest.raises(ValidationError):
            HyperliquidRawMarginSummary.model_validate(d)


def test_margin_summary_type_and_format_errors() -> None:
    """Test margin summary type and format errors."""
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
    """Test margin summary extra field."""
    d = valid_margin_summary().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawMarginSummary.model_validate(d)


def test_margin_summary_adversarial() -> None:
    """Test margin summary adversarial."""
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


def test_margin_summary_extreme_values() -> None:
    """Test margin summary extreme values."""
    # Accept very large/small decimals, reject NaN/inf
    d = valid_margin_summary().copy()
    d["accountValue"] = "0.00000001"
    obj = HyperliquidRawMarginSummary.model_validate(d)
    assert obj.account_value == "0.00000001"
    d["accountValue"] = str(10**50)
    obj = HyperliquidRawMarginSummary.model_validate(d)
    assert obj.account_value == str(10**50)
    d["accountValue"] = "NaN"
    with pytest.raises(ValidationError):
        HyperliquidRawMarginSummary.model_validate(d)
    d["accountValue"] = "inf"
    with pytest.raises(ValidationError):
        HyperliquidRawMarginSummary.model_validate(d)


# --- HyperliquidRawClearinghouseState ---
def test_clearinghouse_state_happy_path() -> None:
    """Test clearinghouse state happy path."""
    obj = HyperliquidRawClearinghouseState.model_validate(valid_clearinghouse_state())
    assert obj.asset_positions[0].asset == "ETH"
    assert obj.margin_summary.account_value == "1000.00"


def test_clearinghouse_state_missing_required() -> None:
    """Test clearinghouse state missing required."""
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
    """Test clearinghouse state type errors."""
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
    """Test clearinghouse state decimal format errors."""
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
    """Test clearinghouse state extra field."""
    d = valid_clearinghouse_state().copy()
    d["foo"] = 1
    with pytest.raises(ValidationError):
        HyperliquidRawClearinghouseState.model_validate(d)


def test_clearinghouse_state_nested_model_error() -> None:
    """Test clearinghouse state nested model error."""
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
    """Test clearinghouse state adversarial."""
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


def test_clearinghouse_state_empty_and_excessive_lists() -> None:
    """Test clearinghouse state empty and excessive lists."""
    # Accept empty assetPositions, single-item, and long lists
    d = valid_clearinghouse_state().copy()
    d["assetPositions"] = []
    obj = HyperliquidRawClearinghouseState.model_validate(d)
    assert obj.asset_positions == []
    d["assetPositions"] = [valid_asset_position()]
    obj = HyperliquidRawClearinghouseState.model_validate(d)
    assert len(obj.asset_positions) == 1
    d["assetPositions"] = [valid_asset_position()] * 1000
    obj = HyperliquidRawClearinghouseState.model_validate(d)
    assert len(obj.asset_positions) == 1000


def test_position_info_all_optional_missing_and_all_edge_cases() -> None:
    """Test position info all optional missing and all edge cases."""
    # All optional fields missing
    d = valid_position_info().copy()
    del d["entryPx"]
    del d["liquidationPx"]
    obj = HyperliquidRawPositionInfo.model_validate(d)
    assert obj.entry_px is None and obj.liquidation_px is None
    # All present, set to edge-case values
    d2 = valid_position_info().copy()
    d2["entryPx"] = "0.0"
    d2["liquidationPx"] = "0.0"
    obj2 = HyperliquidRawPositionInfo.model_validate(d2)
    assert obj2.entry_px == "0.0" and obj2.liquidation_px == "0.0"

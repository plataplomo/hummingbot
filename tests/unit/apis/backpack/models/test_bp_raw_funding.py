"""Unit tests for Backpack raw funding models.

Tests validation and processing of funding rate data from the Backpack exchange API
including current rates, historical rates, and funding interval structures.
"""
# (Test suite will be written here for BackpackRawFunding and related models)

import json
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingRate,
    BackpackRawMarkPrice,
)


# --- BackpackRawFundingRate ---
def valid_funding_rate() -> dict[str, Any]:
    """Return valid funding rate for testing."""
    return {
        "symbol": "BTC_USDC",
        "rate": "0.0001",
        "markPrice": "50000.0",
        "indexPrice": "49999.0",
        "time": 1234567890,
    }


def test_BackpackRawFundingRate_happy_path() -> None:
    """Test BackpackRawFundingRate happy path."""
    obj = BackpackRawFundingRate.model_validate(valid_funding_rate())
    assert obj.symbol == "BTC_USDC"
    assert obj.funding_rate == "0.0001"
    assert obj.mark_price == "50000.0"
    assert obj.index_price == "49999.0"
    assert obj.time == 1234567890


def test_BackpackRawFundingRate_missing_required_fields() -> None:
    """Test BackpackRawFundingRate missing required fields."""
    for field in ["symbol", "rate", "markPrice", "indexPrice", "time"]:
        p: dict[str, Any] = valid_funding_rate().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawFundingRate.model_validate(p)


def test_BackpackRawFundingRate_wrong_type_fields() -> None:
    """Test BackpackRawFundingRate wrong type fields."""
    p: dict[str, Any] = valid_funding_rate().copy()
    p["rate"] = [0.0001]
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)
    p = valid_funding_rate().copy()
    p["time"] = "notanint"
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)


def test_BackpackRawFundingRate_invalid_format_fields() -> None:
    """Test BackpackRawFundingRate invalid format fields."""
    p: dict[str, Any] = valid_funding_rate().copy()
    p["rate"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)
    p = valid_funding_rate().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)
    # Scientific notation is allowed (project policy)
    p = valid_funding_rate().copy()
    p["rate"] = "1e-3"
    obj = BackpackRawFundingRate.model_validate(p)
    assert obj.funding_rate == "1e-3"


def test_BackpackRawFundingRate_extra_field() -> None:
    """Test BackpackRawFundingRate extra field."""
    p: dict[str, Any] = valid_funding_rate().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)


def test_BackpackRawFundingRate_corruption_cases() -> None:
    """Test BackpackRawFundingRate corruption cases."""
    # Garbled numerics
    p: dict[str, Any] = valid_funding_rate().copy()
    p["markPrice"] = "notanumber"
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)
    # Null required
    p = valid_funding_rate().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)
    # Unicode/control chars
    p = valid_funding_rate().copy()
    p["symbol"] = "BTC_USDC\x00"
    obj = BackpackRawFundingRate.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Truncated JSON
    bad_json = '{"symbol": "BTC_USDC", "rate": "0.0001"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawFundingRate_real_json_example() -> None:
    """Validate BackpackRawFundingRate using a real JSON payload with edge values."""
    payload = {
        "symbol": "BTC_USDC",
        "rate": "-0.000123456789",
        "markPrice": "99999999.99999999",
        "indexPrice": "0.00000001",
        "time": 9223372036854775807,
    }
    # The raw model should reject this timestamp as out of range
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(payload)


def test_BackpackRawFundingRate_corruption_null_symbol() -> None:
    """Should fail: null value for required 'symbol'."""
    p = valid_funding_rate().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)


def test_BackpackRawFundingRate_corruption_binary_rate() -> None:
    """Should fail: binary data for 'rate'."""
    p = valid_funding_rate().copy()
    p["rate"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)


def test_BackpackRawFundingRate_corruption_nested_markPrice() -> None:
    """Should fail: nested object for 'markPrice'."""
    p = valid_funding_rate().copy()
    p["markPrice"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)


def test_BackpackRawFundingRate_corruption_list_indexPrice() -> None:
    """Should fail: list for 'indexPrice'."""
    p = valid_funding_rate().copy()
    p["indexPrice"] = ["49999.0"]
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)


def test_BackpackRawFundingRate_corruption_garbled_unicode_symbol() -> None:
    """Should fail: garbled unicode in 'symbol'."""
    p = valid_funding_rate().copy()
    p["symbol"] = "BTC_\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)


# --- BackpackRawMarkPrice ---
def valid_mark_price() -> dict[str, Any]:
    """Return valid mark price for testing."""
    return {
        "symbol": "BTC_USDC",
        "markPrice": "50000.0",
        "fundingRate": "0.0001",
    }


def test_BackpackRawMarkPrice_happy_path() -> None:
    """Test BackpackRawMarkPrice happy path."""
    obj = BackpackRawMarkPrice.model_validate(valid_mark_price())
    assert obj.symbol == "BTC_USDC"
    assert obj.mark_price == "50000.0"
    assert obj.funding_rate == "0.0001"


def test_BackpackRawMarkPrice_missing_required_fields() -> None:
    """Test BackpackRawMarkPrice missing required fields."""
    for field in ["symbol", "markPrice", "fundingRate"]:
        p: dict[str, Any] = valid_mark_price().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawMarkPrice.model_validate(p)


def test_BackpackRawMarkPrice_wrong_type_fields() -> None:
    """Test BackpackRawMarkPrice wrong type fields."""
    p: dict[str, Any] = valid_mark_price().copy()
    p["markPrice"] = [50000.0]
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)


def test_BackpackRawMarkPrice_invalid_format_fields() -> None:
    """Test BackpackRawMarkPrice invalid format fields."""
    p: dict[str, Any] = valid_mark_price().copy()
    p["markPrice"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)
    p = valid_mark_price().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)


def test_BackpackRawMarkPrice_extra_field() -> None:
    """Test BackpackRawMarkPrice extra field."""
    p: dict[str, Any] = valid_mark_price().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)


def test_BackpackRawMarkPrice_corruption_cases() -> None:
    """Test BackpackRawMarkPrice corruption cases."""
    # Garbled numerics
    p: dict[str, Any] = valid_mark_price().copy()
    p["fundingRate"] = "notanumber"
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)
    # Null required
    p = valid_mark_price().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)
    # Unicode/control chars
    p = valid_mark_price().copy()
    p["symbol"] = "BTC_USDC\x00"
    obj = BackpackRawMarkPrice.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Truncated JSON
    bad_json = '{"symbol": "BTC_USDC", "markPrice": "50000.0"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawMarkPrice_real_json_example() -> None:
    """Validate BackpackRawMarkPrice using a real JSON payload from the Backpack OpenAPI spec.

    Includes edge values.
    """
    payload = {
        "symbol": "ETH_USDC",
        "markPrice": "0.00000001",
        "fundingRate": "-0.99999999",
    }
    obj = BackpackRawMarkPrice.model_validate(payload)
    assert obj.symbol == "ETH_USDC"
    assert obj.mark_price == "0.00000001"
    assert obj.funding_rate == "-0.99999999"


def test_BackpackRawMarkPrice_corruption_null_symbol() -> None:
    """Should fail: null value for required 'symbol'."""
    p = valid_mark_price().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)


def test_BackpackRawMarkPrice_corruption_binary_markPrice() -> None:
    """Should fail: binary data for 'markPrice'."""
    p = valid_mark_price().copy()
    p["markPrice"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)


def test_BackpackRawMarkPrice_corruption_nested_fundingRate() -> None:
    """Should fail: nested object for 'fundingRate'."""
    p = valid_mark_price().copy()
    p["fundingRate"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)


def test_BackpackRawMarkPrice_corruption_list_markPrice() -> None:
    """Should fail: list for 'markPrice'."""
    p = valid_mark_price().copy()
    p["markPrice"] = ["50000.0"]
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)


def test_BackpackRawMarkPrice_corruption_garbled_unicode_symbol() -> None:
    """Should fail: garbled unicode in 'symbol'."""
    p = valid_mark_price().copy()
    p["symbol"] = "ETH_\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)

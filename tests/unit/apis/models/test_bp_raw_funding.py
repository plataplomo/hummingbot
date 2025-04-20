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
    return {
        "symbol": "BTC_USDC",
        "rate": "0.0001",
        "markPrice": "50000.0",
        "indexPrice": "49999.0",
        "time": 1234567890,
    }


def test_BackpackRawFundingRate_happy_path() -> None:
    obj = BackpackRawFundingRate.model_validate(valid_funding_rate())
    assert obj.symbol == "BTC_USDC"
    assert obj.funding_rate == "0.0001"
    assert obj.mark_price == "50000.0"
    assert obj.index_price == "49999.0"
    assert obj.time == 1234567890


def test_BackpackRawFundingRate_missing_required_fields() -> None:
    for field in ["symbol", "rate", "markPrice", "indexPrice", "time"]:
        p: dict[str, Any] = valid_funding_rate().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawFundingRate.model_validate(p)


def test_BackpackRawFundingRate_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_funding_rate().copy()
    p["rate"] = [0.0001]
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)
    p = valid_funding_rate().copy()
    p["time"] = "notanint"
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)


def test_BackpackRawFundingRate_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_funding_rate().copy()
    p["rate"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)
    p = valid_funding_rate().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)


def test_BackpackRawFundingRate_extra_field() -> None:
    p: dict[str, Any] = valid_funding_rate().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawFundingRate.model_validate(p)


def test_BackpackRawFundingRate_corruption_cases() -> None:
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


# --- BackpackRawMarkPrice ---
def valid_mark_price() -> dict[str, Any]:
    return {
        "symbol": "BTC_USDC",
        "markPrice": "50000.0",
        "fundingRate": "0.0001",
    }


def test_BackpackRawMarkPrice_happy_path() -> None:
    obj = BackpackRawMarkPrice.model_validate(valid_mark_price())
    assert obj.symbol == "BTC_USDC"
    assert obj.mark_price == "50000.0"
    assert obj.funding_rate == "0.0001"


def test_BackpackRawMarkPrice_missing_required_fields() -> None:
    for field in ["symbol", "markPrice", "fundingRate"]:
        p: dict[str, Any] = valid_mark_price().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawMarkPrice.model_validate(p)


def test_BackpackRawMarkPrice_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_mark_price().copy()
    p["markPrice"] = [50000.0]
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)


def test_BackpackRawMarkPrice_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_mark_price().copy()
    p["markPrice"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)
    p = valid_mark_price().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)


def test_BackpackRawMarkPrice_extra_field() -> None:
    p: dict[str, Any] = valid_mark_price().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawMarkPrice.model_validate(p)


def test_BackpackRawMarkPrice_corruption_cases() -> None:
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

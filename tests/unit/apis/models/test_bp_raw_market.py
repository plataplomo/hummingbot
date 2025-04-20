import json
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawMarket,
    BackpackRawOpenInterest,
    BackpackRawTicker,
)


# --- BackpackRawMarket ---
def valid_market() -> dict[str, Any]:
    return {
        "symbol": "BTC_USDC",
        "baseAsset": "BTC",
        "quoteAsset": "USDC",
    }


def test_BackpackRawMarket_happy_path() -> None:
    obj = BackpackRawMarket.model_validate(valid_market())
    assert obj.symbol == "BTC_USDC"
    assert obj.base_asset == "BTC"
    assert obj.quote_asset == "USDC"


def test_BackpackRawMarket_missing_required_fields() -> None:
    for field in ["symbol", "baseAsset", "quoteAsset"]:
        p: dict[str, Any] = valid_market().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawMarket.model_validate(p)


def test_BackpackRawMarket_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_market().copy()
    p["symbol"] = 123
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)
    p = valid_market().copy()
    p["baseAsset"] = ["BTC"]
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)


def test_BackpackRawMarket_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_market().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)
    p = valid_market().copy()
    p["baseAsset"] = "   "
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)


def test_BackpackRawMarket_extra_field() -> None:
    p: dict[str, Any] = valid_market().copy()
    p["foo"] = "bar"
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)


def test_BackpackRawMarket_corruption_cases() -> None:
    # Null required
    p: dict[str, Any] = valid_market().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)
    # Unicode/control chars
    p = valid_market().copy()
    p["symbol"] = "BTC_USDC\x00"
    obj = BackpackRawMarket.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Excessive length
    p = valid_market().copy()
    p["symbol"] = "BTC_USDC" * 1000
    obj = BackpackRawMarket.model_validate(p)
    assert obj.symbol.startswith("BTC_USDC")
    # Truncated JSON
    bad_json = '{"symbol": "BTC_USDC", "baseAsset": "BTC"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


# --- BackpackRawTicker ---
def valid_ticker() -> dict[str, Any]:
    return {
        "symbol": "BTC_USDC",
        "price": "50000.0",
        "bid": "49999.0",
        "ask": "50001.0",
        "volume": "123.456",
        "time": 1234567890,
    }


def test_BackpackRawTicker_happy_path() -> None:
    obj = BackpackRawTicker.model_validate(valid_ticker())
    assert obj.symbol == "BTC_USDC"
    assert obj.price == "50000.0"
    assert obj.bid == "49999.0"
    assert obj.ask == "50001.0"
    assert obj.volume == "123.456"
    assert obj.time == 1234567890


def test_BackpackRawTicker_missing_required_fields() -> None:
    for field in ["symbol", "time"]:
        p: dict[str, Any] = valid_ticker().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawTicker.model_validate(p)


def test_BackpackRawTicker_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_ticker().copy()
    p["price"] = [50000.0]
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)
    p = valid_ticker().copy()
    p["time"] = "notanint"
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)


def test_BackpackRawTicker_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_ticker().copy()
    p["price"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)
    p = valid_ticker().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)


def test_BackpackRawTicker_extra_field() -> None:
    p: dict[str, Any] = valid_ticker().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)


def test_BackpackRawTicker_optional_fields_all_none() -> None:
    p: dict[str, Any] = valid_ticker().copy()
    for f in ["price", "bid", "ask", "volume"]:
        p[f] = None
    obj = BackpackRawTicker.model_validate(p)
    for f in ["price", "bid", "ask", "volume"]:
        assert getattr(obj, f, "__notset__") is None


def test_BackpackRawTicker_optional_fields_omitted() -> None:
    p: dict[str, Any] = valid_ticker().copy()
    for f in ["price", "bid", "ask", "volume"]:
        if f in p:
            del p[f]
    obj = BackpackRawTicker.model_validate(p)
    for f in ["price", "bid", "ask", "volume"]:
        assert getattr(obj, f, "__notset__") is None


def test_BackpackRawTicker_corruption_cases() -> None:
    # Garbled numerics
    p: dict[str, Any] = valid_ticker().copy()
    p["bid"] = "notanumber"
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)
    # Null required
    p = valid_ticker().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)
    # Unicode/control chars
    p = valid_ticker().copy()
    p["symbol"] = "BTC_USDC\x00"
    obj = BackpackRawTicker.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Truncated JSON
    bad_json = '{"symbol": "BTC_USDC", "time": 1234567890'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


# --- BackpackRawOpenInterest ---
def valid_open_interest() -> dict[str, Any]:
    return {
        "symbol": "BTC_USDC",
        "openInterest": "12345.6789",
    }


def test_BackpackRawOpenInterest_happy_path() -> None:
    obj = BackpackRawOpenInterest.model_validate(valid_open_interest())
    assert obj.symbol == "BTC_USDC"
    assert obj.open_interest == "12345.6789"


def test_BackpackRawOpenInterest_missing_required_fields() -> None:
    for field in ["symbol", "openInterest"]:
        p: dict[str, Any] = valid_open_interest().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawOpenInterest.model_validate(p)


def test_BackpackRawOpenInterest_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_open_interest().copy()
    p["openInterest"] = [12345.6789]
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)


def test_BackpackRawOpenInterest_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_open_interest().copy()
    p["openInterest"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)
    p = valid_open_interest().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)


def test_BackpackRawOpenInterest_extra_field() -> None:
    p: dict[str, Any] = valid_open_interest().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)


def test_BackpackRawOpenInterest_corruption_cases() -> None:
    # Garbled numerics
    p: dict[str, Any] = valid_open_interest().copy()
    p["openInterest"] = "notanumber"
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)
    # Null required
    p = valid_open_interest().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)
    # Unicode/control chars
    p = valid_open_interest().copy()
    p["symbol"] = "BTC_USDC\x00"
    obj = BackpackRawOpenInterest.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Truncated JSON
    bad_json = '{"symbol": "BTC_USDC", "openInterest": "12345.6789"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)

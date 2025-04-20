import json
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawTrade,
    BackpackRawTradeEvent,
)


# --- BackpackRawTrade ---
def valid_trade() -> dict[str, Any]:
    return {
        "id": "trade123",
        "orderId": "order456",
        "symbol": "BTC_USDC",
        "price": "50000.0",
        "qty": "0.01",
        "time": 1234567890,
    }


def test_BackpackRawTrade_happy_path() -> None:
    obj = BackpackRawTrade.model_validate(valid_trade())
    assert obj.id == "trade123"
    assert obj.symbol == "BTC_USDC"
    assert obj.price == "50000.0"
    assert obj.quantity == "0.01"
    assert obj.time == 1234567890


def test_BackpackRawTrade_missing_required_fields() -> None:
    for field in ["id", "orderId", "symbol", "price", "qty", "time"]:
        p: dict[str, Any] = valid_trade().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawTrade.model_validate(p)


def test_BackpackRawTrade_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_trade().copy()
    p["price"] = [50000.0]
    with pytest.raises(ValidationError):
        BackpackRawTrade.model_validate(p)
    p = valid_trade().copy()
    p["time"] = "notanint"
    with pytest.raises(ValidationError):
        BackpackRawTrade.model_validate(p)


def test_BackpackRawTrade_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_trade().copy()
    p["price"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawTrade.model_validate(p)
    p = valid_trade().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawTrade.model_validate(p)


def test_BackpackRawTrade_extra_field() -> None:
    p: dict[str, Any] = valid_trade().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawTrade.model_validate(p)


def test_BackpackRawTrade_corruption_cases() -> None:
    # Garbled numerics
    p: dict[str, Any] = valid_trade().copy()
    p["qty"] = "notanumber"
    with pytest.raises(ValidationError):
        BackpackRawTrade.model_validate(p)
    # Null required
    p = valid_trade().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawTrade.model_validate(p)
    # Unicode/control chars
    p = valid_trade().copy()
    p["symbol"] = "BTC_USDC\x00"
    obj = BackpackRawTrade.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Truncated JSON
    bad_json = '{"id": "trade123", "orderId": "order456"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawTrade_real_json_examples() -> None:
    """
    Validate BackpackRawTrade using real JSON payloads from the Backpack OpenAPI spec.
    Covers both happy path and edge/boundary values.
    """
    # Example from OpenAPI (with plausible values)
    real_payload = {
        "id": "trade_001",
        "orderId": "order_abc",
        "symbol": "BTC_USDC",
        "price": "12345.67",
        "qty": "0.005",
        "time": 1712345678901,
    }
    obj = BackpackRawTrade.model_validate(real_payload)
    assert obj.id == "trade_001"
    assert obj.order_id == "order_abc"
    assert obj.symbol == "BTC_USDC"
    assert obj.price == "12345.67"
    assert obj.quantity == "0.005"
    assert obj.time == 1712345678901

    # Edge: Large numbers, unicode symbol, boundary price
    edge_payload = {
        "id": "trade_999999999999999999",
        "orderId": "order_Ωmega",
        "symbol": "BTC_😀",
        "price": "0.00000001",
        "qty": "1000000000",
        "time": 9999999999999,
    }
    obj = BackpackRawTrade.model_validate(edge_payload)
    assert obj.symbol == "BTC_😀"
    assert obj.price == "0.00000001"
    assert obj.quantity == "1000000000"
    assert obj.time == 9999999999999


def test_BackpackRawTrade_creative_corruption_cases() -> None:
    """
    Test BackpackRawTrade with 10 creative corruption cases simulating hostile or malformed input.
    Each case is described and should raise a ValidationError (unless otherwise noted).
    """
    base: dict[str, object] = {
        "id": "trade123",
        "orderId": "order456",
        "symbol": "BTC_USDC",
        "price": "50000.0",
        "qty": "0.01",
        "time": 1234567890,
    }
    corruption_cases = [
        ("id", None, "null value for required field"),
        ("orderId", b"\x00\x01", "binary data instead of string"),
        ("symbol", {"foo": "bar"}, "nested object instead of string"),
        ("price", ["50000.0"], "list instead of string for price"),
        ("qty", 0.01, "float instead of string for qty"),
        ("price", 50000, "int instead of string for price"),
        ("symbol", "BTC_\udce2\udc28\udc00", "garbled unicode in symbol"),
        ("id", "A" * 10**7, "overly large string (potential DoS)"),
        ("price", '{"incomplete": ', "truncated JSON-like string for price"),
        ("symbol", "BTC_USDC'; DROP TABLE trades;--", "SQL injection attempt in symbol"),
    ]
    for field, value, description in corruption_cases:
        p = base.copy()
        # Intentionally assign type-unsafe value for adversarial test
        # (mypy and Ruff allow this here)
        p[field] = value
        try:
            BackpackRawTrade.model_validate(p)
        except ValidationError:
            pass  # Expected
        else:
            import pytest

            pytest.fail(
                f"Failed corruption case: {description} ("
                f"{field}={value!r}) - ValidationError not raised"
            )


def test_BackpackRawTrade_real_json_edge_case() -> None:
    """Validate BackpackRawTrade using a real JSON payload with edge values."""
    payload = {
        "id": "trade_999999999999999999",
        "orderId": "order_Ωmega",
        "symbol": "BTC_😀",
        "price": "0.00000001",
        "qty": "1000000000",
        "time": 9999999999999,
    }
    obj = BackpackRawTrade.model_validate(payload)
    assert obj.symbol == "BTC_😀"
    assert obj.price == "0.00000001"
    assert obj.quantity == "1000000000"
    assert obj.time == 9999999999999


def test_BackpackRawTrade_corruption_null_id() -> None:
    """Should fail: null value for required 'id'."""
    p = valid_trade().copy()
    p["id"] = None
    with pytest.raises(ValidationError):
        BackpackRawTrade.model_validate(p)


def test_BackpackRawTrade_corruption_binary_orderId() -> None:
    """Should fail: binary data for 'orderId'."""
    p = valid_trade().copy()
    p["orderId"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawTrade.model_validate(p)


def test_BackpackRawTrade_corruption_nested_symbol() -> None:
    """Should fail: nested object for 'symbol'."""
    p = valid_trade().copy()
    p["symbol"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawTrade.model_validate(p)


def test_BackpackRawTrade_corruption_list_price() -> None:
    """Should fail: list for 'price'."""
    p = valid_trade().copy()
    p["price"] = ["50000.0"]
    with pytest.raises(ValidationError):
        BackpackRawTrade.model_validate(p)


def test_BackpackRawTrade_corruption_garbled_unicode_symbol() -> None:
    """Should fail: garbled unicode in 'symbol'."""
    p = valid_trade().copy()
    p["symbol"] = "BTC_\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawTrade.model_validate(p)


# --- BackpackRawTradeEvent ---
def valid_trade_event() -> dict[str, Any]:
    return {
        "e": "trade",
        "E": 1234567890,
        "s": "BTC_USDC",
        "p": "50000.0",
        "q": "0.01",
        "b": "orderB",
        "a": "orderA",
        "t": "trade789",
        "T": 1234567891,
        "m": True,
    }


def test_BackpackRawTradeEvent_happy_path() -> None:
    obj = BackpackRawTradeEvent.model_validate(valid_trade_event())
    assert obj.event_type == "trade"
    assert obj.symbol == "BTC_USDC"
    assert obj.price == "50000.0"
    assert obj.is_buyer_the_maker is True


def test_BackpackRawTradeEvent_missing_required_fields() -> None:
    for field in ["e", "E", "s", "p", "q", "b", "a", "t", "T", "m"]:
        p: dict[str, Any] = valid_trade_event().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_wrong_type_fields() -> None:
    p: dict[str, Any] = valid_trade_event().copy()
    p["m"] = "notabool"
    with pytest.raises(ValidationError):
        BackpackRawTradeEvent.model_validate(p)
    p = valid_trade_event().copy()
    p["E"] = "notanint"
    with pytest.raises(ValidationError):
        BackpackRawTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_invalid_format_fields() -> None:
    p: dict[str, Any] = valid_trade_event().copy()
    p["p"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawTradeEvent.model_validate(p)
    p = valid_trade_event().copy()
    p["s"] = ""
    with pytest.raises(ValidationError):
        BackpackRawTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_extra_field() -> None:
    p: dict[str, Any] = valid_trade_event().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_corruption_cases() -> None:
    # Garbled numerics
    p: dict[str, Any] = valid_trade_event().copy()
    p["q"] = "notanumber"
    with pytest.raises(ValidationError):
        BackpackRawTradeEvent.model_validate(p)
    # Null required
    p = valid_trade_event().copy()
    p["s"] = None
    with pytest.raises(ValidationError):
        BackpackRawTradeEvent.model_validate(p)
    # Unicode/control chars
    p = valid_trade_event().copy()
    p["s"] = "BTC_USDC\x00"
    obj = BackpackRawTradeEvent.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Boolean edge cases
    p = valid_trade_event().copy()
    p["m"] = 1
    obj = BackpackRawTradeEvent.model_validate(p)
    assert obj.is_buyer_the_maker is True
    p = valid_trade_event().copy()
    p["m"] = 0
    obj = BackpackRawTradeEvent.model_validate(p)
    assert obj.is_buyer_the_maker is False
    # Truncated JSON
    bad_json = '{"e": "trade", "E": 1234567890, "s": "BTC_USDC"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawTradeEvent_real_json_edge_case() -> None:
    """Validate BackpackRawTradeEvent using a real JSON payload with edge values."""
    payload = {
        "e": "trade",
        "E": 9223372036854775807,
        "s": "BTC_😀",
        "p": "0.00000001",
        "q": "1000000000",
        "b": "orderB",
        "a": "orderA",
        "t": "trade999999999999999999",
        "T": 9223372036854775807,
        "m": True,
    }
    obj = BackpackRawTradeEvent.model_validate(payload)
    assert obj.symbol == "BTC_😀"
    assert obj.price == "0.00000001"
    assert obj.is_buyer_the_maker is True


def test_BackpackRawTradeEvent_corruption_null_e() -> None:
    """Should fail: null value for required 'e'."""
    p = valid_trade_event().copy()
    p["e"] = None
    with pytest.raises(ValidationError):
        BackpackRawTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_corruption_binary_s() -> None:
    """Should fail: binary data for 's'."""
    p = valid_trade_event().copy()
    p["s"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_corruption_nested_p() -> None:
    """Should fail: nested object for 'p'."""
    p = valid_trade_event().copy()
    p["p"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_corruption_list_q() -> None:
    """Should fail: list for 'q'."""
    p = valid_trade_event().copy()
    p["q"] = ["0.01"]
    with pytest.raises(ValidationError):
        BackpackRawTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_corruption_garbled_unicode_symbol() -> None:
    """Should fail: garbled unicode in 's' (symbol)."""
    p = valid_trade_event().copy()
    p["s"] = "BTC_\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawTradeEvent.model_validate(p)

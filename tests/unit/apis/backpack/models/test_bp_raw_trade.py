"""Module docstring."""

import json
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFillResponse
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawPublicTrade,
    BackpackRawPublicTradeEvent,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import DateTimeParsingError, EmptyStringError


logger = get_logger(__name__)


# --- BackpackRawPublicTrade ---
def valid_trade() -> dict[str, Any]:
    """Return valid trade for testing."""
    return {
        "id": "trade123",
        "orderId": "order456",
        "symbol": "BTC_USDC",
        "price": "50000.0",
        "qty": "0.01",
        "time": 1234567890,
    }


def test_BackpackRawTrade_happy_path() -> None:
    """Test BackpackRawPublicTrade happy path."""
    obj = BackpackRawPublicTrade.model_validate(valid_trade())
    assert obj.id == "trade123"
    assert obj.symbol == "BTC_USDC"
    assert obj.price == "50000.0"
    assert obj.quantity == "0.01"
    assert obj.time == 1234567890


def test_BackpackRawTrade_missing_required_fields() -> None:
    """Test BackpackRawPublicTrade missing required fields."""
    for field in ["id", "orderId", "symbol", "price", "qty", "time"]:
        p: dict[str, Any] = valid_trade().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawPublicTrade.model_validate(p)


def test_BackpackRawTrade_wrong_type_fields() -> None:
    """Test BackpackRawPublicTrade wrong type fields."""
    p: dict[str, Any] = valid_trade().copy()
    p["price"] = [50000.0]
    with pytest.raises(TypeError):
        BackpackRawPublicTrade.model_validate(p)
    p = valid_trade().copy()
    p["time"] = "notanint"
    with pytest.raises(DateTimeParsingError):  # This is datetime parsing validation
        BackpackRawPublicTrade.model_validate(p)


def test_BackpackRawTrade_invalid_format_fields() -> None:
    """Test BackpackRawPublicTrade invalid format fields."""
    p: dict[str, Any] = valid_trade().copy()
    p["price"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawPublicTrade.model_validate(p)
    p = valid_trade().copy()
    p["symbol"] = ""
    with pytest.raises(EmptyStringError):
        BackpackRawPublicTrade.model_validate(p)


def test_BackpackRawTrade_extra_field() -> None:
    """Test BackpackRawPublicTrade extra field."""
    p: dict[str, Any] = valid_trade().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawPublicTrade.model_validate(p)


def test_BackpackRawTrade_corruption_cases() -> None:
    """Test BackpackRawPublicTrade corruption cases."""
    # Garbled numerics
    p: dict[str, Any] = valid_trade().copy()
    p["qty"] = "notanumber"
    with pytest.raises(ValidationError):
        BackpackRawPublicTrade.model_validate(p)
    # Null required
    p = valid_trade().copy()
    p["symbol"] = None
    with pytest.raises(TypeError):
        BackpackRawPublicTrade.model_validate(p)
    # Unicode/control chars
    p = valid_trade().copy()
    p["symbol"] = "BTC_USDC\x00"
    obj = BackpackRawPublicTrade.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Truncated JSON
    bad_json = '{"id": "trade123", "orderId": "order456"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawTrade_real_json_examples() -> None:
    """Validate BackpackRawPublicTrade using real JSON payloads from the Backpack OpenAPI spec.

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
    obj = BackpackRawPublicTrade.model_validate(real_payload)
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
    obj = BackpackRawPublicTrade.model_validate(edge_payload)
    assert obj.symbol == "BTC_😀"
    assert obj.price == "0.00000001"
    assert obj.quantity == "1000000000"
    assert obj.time == 9999999999999


def test_BackpackRawTrade_creative_corruption_cases() -> None:
    """Test BackpackRawPublicTrade with creative corruption cases simulating hostile/bad input.

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
        p[field] = value
        # Accept SQL injection attempt as valid for raw model (no ValidationError expected)
        if description == "SQL injection attempt in symbol":
            BackpackRawPublicTrade.model_validate(p)
            continue
        try:
            BackpackRawPublicTrade.model_validate(p)
        except (ValidationError, TypeError):
            pass
        else:
            pytest.fail(
                f"Failed corruption case: {description} ({field}={value!r}) - Exception not raised",
            )


def test_BackpackRawTrade_real_json_edge_case() -> None:
    """Validate BackpackRawPublicTrade using a real JSON payload with edge values."""
    payload = {
        "id": "trade_999999999999999999",
        "orderId": "order_Ωmega",
        "symbol": "BTC_😀",
        "price": "0.00000001",
        "qty": "1000000000",
        "time": 9999999999999,
    }
    obj = BackpackRawPublicTrade.model_validate(payload)
    assert obj.symbol == "BTC_😀"
    assert obj.price == "0.00000001"
    assert obj.quantity == "1000000000"
    assert obj.time == 9999999999999


def test_BackpackRawTrade_corruption_null_id() -> None:
    """Should fail: null value for required 'id'."""
    p = valid_trade().copy()
    p["id"] = None
    with pytest.raises(TypeError):
        BackpackRawPublicTrade.model_validate(p)


def test_BackpackRawTrade_corruption_binary_orderId() -> None:
    """Should fail: binary data for 'orderId'."""
    p = valid_trade().copy()
    p["orderId"] = b"\x00\x01"
    with pytest.raises(TypeError):
        BackpackRawPublicTrade.model_validate(p)


def test_BackpackRawTrade_corruption_nested_symbol() -> None:
    """Should fail: nested object for 'symbol'."""
    p = valid_trade().copy()
    p["symbol"] = {"foo": "bar"}
    with pytest.raises(TypeError):
        BackpackRawPublicTrade.model_validate(p)


def test_BackpackRawTrade_corruption_list_price() -> None:
    """Should fail: list for 'price'."""
    p = valid_trade().copy()
    p["price"] = ["50000.0"]
    with pytest.raises(TypeError):
        BackpackRawPublicTrade.model_validate(p)


def test_BackpackRawTrade_corruption_garbled_unicode_symbol() -> None:
    """Should fail: garbled unicode in 'symbol'."""
    p = valid_trade().copy()
    p["symbol"] = "BTC_\udce2\udc28\udc00"
    with pytest.raises(TypeFieldError):
        BackpackRawPublicTrade.model_validate(p)


# --- BackpackRawPublicTradeEvent ---
def valid_trade_event() -> dict[str, Any]:
    """Return valid trade event for testing."""
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
    """Test BackpackRawPublicTradeEvent happy path."""
    obj = BackpackRawPublicTradeEvent.model_validate(valid_trade_event())
    assert obj.event_type == "trade"
    assert obj.symbol == "BTC_USDC"
    assert obj.price == "50000.0"
    assert obj.is_buyer_the_maker is True


def test_BackpackRawTradeEvent_missing_required_fields() -> None:
    """Test BackpackRawPublicTradeEvent missing required fields."""
    for field in ["e", "E", "s", "p", "q", "b", "a", "t", "T", "m"]:
        p: dict[str, Any] = valid_trade_event().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawPublicTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_wrong_type_fields() -> None:
    """Test BackpackRawPublicTradeEvent wrong type fields."""
    p: dict[str, Any] = valid_trade_event().copy()
    p["m"] = "notabool"
    with pytest.raises(ValidationError):
        BackpackRawPublicTradeEvent.model_validate(p)
    p = valid_trade_event().copy()
    p["E"] = "notanint"
    with pytest.raises(DateTimeParsingError):
        BackpackRawPublicTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_invalid_format_fields() -> None:
    """Test BackpackRawPublicTradeEvent invalid format fields."""
    p: dict[str, Any] = valid_trade_event().copy()
    p["p"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawPublicTradeEvent.model_validate(p)
    p = valid_trade_event().copy()
    p["s"] = ""
    with pytest.raises(EmptyStringError):
        BackpackRawPublicTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_extra_field() -> None:
    """Test BackpackRawPublicTradeEvent extra field."""
    p: dict[str, Any] = valid_trade_event().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawPublicTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_corruption_cases() -> None:
    """Test BackpackRawPublicTradeEvent corruption cases."""
    # Garbled numerics
    p: dict[str, Any] = valid_trade_event().copy()
    p["q"] = "notanumber"
    with pytest.raises(ValidationError):
        BackpackRawPublicTradeEvent.model_validate(p)
    # Null required
    p = valid_trade_event().copy()
    p["s"] = None
    with pytest.raises(TypeError):
        BackpackRawPublicTradeEvent.model_validate(p)
    # Unicode/control chars
    p = valid_trade_event().copy()
    p["s"] = "BTC_USDC\x00"
    obj = BackpackRawPublicTradeEvent.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Boolean edge cases
    p = valid_trade_event().copy()
    p["m"] = 1
    obj = BackpackRawPublicTradeEvent.model_validate(p)
    assert obj.is_buyer_the_maker is True
    p = valid_trade_event().copy()
    p["m"] = 0
    obj = BackpackRawPublicTradeEvent.model_validate(p)
    assert obj.is_buyer_the_maker is False
    # Truncated JSON
    bad_json = '{"e": "trade", "E": 1234567890, "s": "BTC_USDC"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawTradeEvent_real_json_edge_case() -> None:
    """Validate BackpackRawPublicTradeEvent using a real JSON payload with edge values."""
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
    obj = BackpackRawPublicTradeEvent.model_validate(payload)
    assert obj.symbol == "BTC_😀"
    assert obj.price == "0.00000001"
    assert obj.is_buyer_the_maker is True


def test_BackpackRawTradeEvent_corruption_null_e() -> None:
    """Should fail: null value for required 'e'."""
    p = valid_trade_event().copy()
    p["e"] = None
    with pytest.raises(ValidationError):
        BackpackRawPublicTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_corruption_binary_s() -> None:
    """Should fail: binary data for 's'."""
    p = valid_trade_event().copy()
    p["s"] = b"\x00\x01"
    with pytest.raises(TypeError):
        BackpackRawPublicTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_corruption_nested_p() -> None:
    """Should fail: nested object for 'p'."""
    p = valid_trade_event().copy()
    p["p"] = {"foo": "bar"}
    with pytest.raises(TypeError):
        BackpackRawPublicTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_corruption_list_q() -> None:
    """Should fail: list for 'q'."""
    p = valid_trade_event().copy()
    p["q"] = ["0.01"]
    with pytest.raises(TypeError):
        BackpackRawPublicTradeEvent.model_validate(p)


def test_BackpackRawTradeEvent_corruption_garbled_unicode_symbol() -> None:
    """Should fail: garbled unicode in 's' (symbol)."""
    p = valid_trade_event().copy()
    p["s"] = "BTC_\udce2\udc28\udc00"
    with pytest.raises(TypeFieldError):
        BackpackRawPublicTradeEvent.model_validate(p)


# --- BackpackRawFillResponse ---


def valid_fill_data() -> dict[str, Any]:
    """Return a dictionary with valid data for BackpackRawFillResponse."""
    return {
        "fee": "0.001",
        "feeSymbol": "USDC",
        "isMaker": True,
        "orderId": "order-123456789",
        "price": "50000.12345",
        "quantity": "0.002",
        "side": "Bid",  # Changed from "Buy" to "Bid" - BackpackRawFillResponse expects "Bid"/"Ask"
        "symbol": "BTC_USDC",
        "timestamp": "2024-05-01T12:34:56.789000Z",  # Expected ISO format
        "tradeId": 987654321,
        "clientId": "client-abc-def-999",
    }


def test_BackpackRawFillResponse_happy_path() -> None:
    """Test successful validation with valid data."""
    data = valid_fill_data()
    obj = BackpackRawFillResponse.model_validate(data)

    assert obj.fee == "0.001"
    assert obj.fee_symbol == "USDC"
    assert obj.is_maker is True
    assert obj.order_id == "order-123456789"
    assert obj.price == "50000.12345"
    assert obj.quantity == "0.002"
    assert obj.side == "Bid"
    assert obj.symbol == "BTC_USDC"
    assert obj.timestamp == "2024-05-01T12:34:56.789000Z"
    assert obj.trade_id == 987654321
    assert obj.client_id == "client-abc-def-999"


def test_BackpackRawFillResponse_optional_client_id_none() -> None:
    """Test successful validation when optional clientId is None."""
    data = valid_fill_data()
    del data["clientId"]
    obj = BackpackRawFillResponse.model_validate(data)
    assert obj.client_id is None


def test_BackpackRawFillResponse_missing_required_fields() -> None:
    """Test that missing required fields raise ValidationError."""
    required_fields = [
        "fee",
        "feeSymbol",
        "isMaker",
        "orderId",
        "price",
        "quantity",
        "side",
        "symbol",
        "timestamp",
        "tradeId",
    ]
    for field in required_fields:
        data = valid_fill_data().copy()
        del data[field]
        with pytest.raises(ValidationError, match=field):  # Adjusted match for Pydantic V2
            BackpackRawFillResponse.model_validate(data)


def test_BackpackRawFillResponse_invalid_types() -> None:
    """Test that invalid types raise appropriate exceptions."""
    # Test each invalid case individually to determine exact error type
    invalid_cases = [
        ("fee", 123.45, TypeError),  # Expect string
        ("feeSymbol", 123, TypeError),
        ("isMaker", "true", ValidationError),  # Expect boolean
        ("orderId", None, TypeError),
        ("price", 10000, TypeError),
        ("quantity", 1.0, TypeError),
        ("side", ["Bid"], TypeError),  # Expect string, not list
        ("symbol", None, TypeError),
        ("timestamp", 1234567890, TypeError),  # Expect string
        ("tradeId", "abc", ValidationError),  # Expect int
        # Note: ("clientId", 123) is actually valid - clientId accepts integers
    ]

    for field, value, expected_error in invalid_cases:
        data = valid_fill_data().copy()
        data[field] = value
        with pytest.raises(expected_error):
            BackpackRawFillResponse.model_validate(data)


def test_BackpackRawFillResponse_invalid_formats_and_values() -> None:
    """Test that invalid formats and values raise ValidationError.

    Raises:
        ValidationError: For various validation failures
        TypeError: For type mismatches
        EmptyStringError: For empty string fields
        DateTimeParsingError: For invalid datetime formats
        TypeFieldError: For type field validation errors
    """
    invalid_cases = [
        ("fee", ""),  # Empty string
        ("fee", "  "),  # Whitespace string
        ("fee", "inf"),  # Non-finite decimal
        ("fee", "nan"),
        ("fee", "1.2.3"),  # Invalid decimal format
        ("feeSymbol", ""),
        ("feeSymbol", " \t"),
        ("feeSymbol", "A" * 33),  # Exceeds max_length
        ("orderId", ""),
        ("orderId", "A" * 129),  # Exceeds max_length
        ("price", ""),
        ("price", "infinity"),
        ("quantity", ""),
        ("side", "Other"),  # Invalid side value - only "Bid"/"Ask" allowed
        ("symbol", ""),
        ("symbol", "A" * 65),  # Exceeds max_length
        ("timestamp", ""),
        ("timestamp", "not-a-date"),
        ("timestamp", "2023-13-01T00:00:00Z"),  # Invalid month
        ("tradeId", -1),  # Negative integer
        ("tradeId", -1.0),  # Invalid type
        ("clientId", ""),  # Empty string for optional field - SHOULD FAIL
        ("clientId", " \n "),  # Whitespace string for optional field - SHOULD FAIL
        ("clientId", "A" * 129),  # Exceeds max_length
    ]
    for field, value in invalid_cases:
        data = valid_fill_data().copy()
        data[field] = value
        # This is less brittle when multiple fields might fail or the order changes.
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            DateTimeParsingError,
            TypeFieldError,
        )):
            try:
                BackpackRawFillResponse.model_validate(data)
            except (
                ValidationError,
                TypeError,
                EmptyStringError,
                DateTimeParsingError,
                TypeFieldError,
            ) as e:
                logger.debug(
                    "backpack_raw_fill_validation_error",
                    field=field,
                    value=repr(value),
                    error=str(e),
                    message=f"Field: {field}, Value: {value!r}, Error: {e}",
                )
                # Simple assertion that *an* error occurred is sufficient here
                raise  # Re-raise the expected exception


def test_BackpackRawFillResponse_invalid_client_id_empty_string() -> None:
    """Test failure when clientId is an empty string."""
    p = valid_fill_data().copy()
    p["clientId"] = ""
    with pytest.raises(EmptyStringError) as exc_info:
        BackpackRawFillResponse.model_validate(p)
    # Check for the specific error message from the mode='after' validator
    assert "clientId cannot be an empty or whitespace-only string if provided" in str(
        exc_info.value,
    )


def test_BackpackRawFillResponse_extra_field_forbidden() -> None:
    """Test ValidationError when extra fields are provided (extra='forbid')."""
    data = valid_fill_data()
    data["extraField"] = "should not be allowed"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        BackpackRawFillResponse.model_validate(data)


def test_BackpackRawFillResponse_frozen() -> None:
    """Test that the model is frozen (immutable) after creation."""
    data = valid_fill_data()
    obj = BackpackRawFillResponse.model_validate(data)
    with pytest.raises(ValidationError, match="Instance is frozen"):
        obj.symbol = "SOL_USDC"
    with pytest.raises(ValidationError, match="Instance is frozen"):
        obj.price = "60000.0"

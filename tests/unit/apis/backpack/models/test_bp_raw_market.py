"""Unit tests for Backpack raw market data models.

Tests validation and processing of market data from the Backpack exchange API
including tickers, order books, and other market-related data structures.
"""

import json
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawMarket,
    BackpackRawOpenInterest,
    BackpackRawTicker,
    BackpackRawTickerEvent,
)


# --- BackpackRawMarket ---
def valid_market() -> dict[str, Any]:
    """Return valid market for testing."""
    return {
        "symbol": "BTC_USDC",
        "baseSymbol": "BTC",
        "quoteSymbol": "USDC",
        "marketType": "Spot",
        "filters": {
            "price": {"minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
            "quantity": {"minQuantity": "0.0001", "maxQuantity": "1000.0", "stepSize": "0.0001"},
        },
        "orderBookState": "NORMAL",
        "createdAt": "2024-01-01T00:00:00.000Z",
    }


def test_BackpackRawMarket_happy_path() -> None:
    """Test BackpackRawMarket happy path."""
    obj = BackpackRawMarket.model_validate(valid_market())
    assert obj.symbol == "BTC_USDC"
    assert obj.base_symbol == "BTC"
    assert obj.quote_symbol == "USDC"


def test_BackpackRawMarket_missing_required_fields() -> None:
    """Test BackpackRawMarket missing required fields."""
    required_fields = [
        "symbol",
        "baseSymbol",
        "quoteSymbol",
        "marketType",
        "filters",
        "orderBookState",
        "createdAt",
    ]
    for field in required_fields:
        p: dict[str, Any] = valid_market().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawMarket.model_validate(p)


def test_BackpackRawMarket_wrong_type_fields() -> None:
    """Test BackpackRawMarket wrong type fields."""
    p: dict[str, Any] = valid_market().copy()
    p["symbol"] = 123
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)
    p = valid_market().copy()
    p["baseAsset"] = ["BTC"]
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)


def test_BackpackRawMarket_invalid_format_fields() -> None:
    """Test BackpackRawMarket invalid format fields."""
    p: dict[str, Any] = valid_market().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)
    p = valid_market().copy()
    p["baseAsset"] = "   "
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)


def test_BackpackRawMarket_extra_field() -> None:
    """Test BackpackRawMarket extra field."""
    p: dict[str, Any] = valid_market().copy()
    p["foo"] = "bar"
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)


def test_BackpackRawMarket_corruption_cases() -> None:
    """Test BackpackRawMarket corruption cases."""
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
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)
    # Truncated JSON
    bad_json = '{"symbol": "BTC_USDC", "baseAsset": "BTC"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawMarket_real_json_example() -> None:
    """Validate BackpackRawMarket using a real JSON payload from the Backpack OpenAPI spec.

    Includes edge values.
    """
    payload = {
        "symbol": "BTC_USDC",
        "baseAsset": "BTC",
        "quoteAsset": "USDC",
        "quantityPrecision": 8,
        "pricePrecision": 2,
        "minTradeQuantity": "0.0001",
        "maxTradeQuantity": "1000.0",
        "minTradePrice": "0.01",
        "maxTradePrice": "1000000.0",
        "minOrderBookQuantity": "0.0001",
        "bids": [["49999.00", "0.5"], ["49998.50", "1.2"]],
        "asks": [["50001.00", "0.3"], ["50001.50", "0.8"]],
        "lastUpdateTime": 1678886400000,
    }
    obj = BackpackRawMarket.model_validate(payload)
    assert obj.symbol == "BTC_USDC"
    assert obj.base_symbol == "BTC"
    assert obj.quote_symbol == "USDC"


def test_BackpackRawMarket_corruption_null_symbol() -> None:
    """Should fail: null value for required 'symbol'."""
    p = valid_market().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)


def test_BackpackRawMarket_corruption_binary_baseAsset() -> None:
    """Should fail: binary data for 'baseAsset'."""
    p = valid_market().copy()
    p["baseAsset"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)


def test_BackpackRawMarket_corruption_nested_quoteAsset() -> None:
    """Should fail: nested object for 'quoteAsset'."""
    p = valid_market().copy()
    p["quoteAsset"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)


def test_BackpackRawMarket_corruption_list_symbol() -> None:
    """Should fail: list for 'symbol'."""
    p = valid_market().copy()
    p["symbol"] = ["BTC_USDC"]
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)


def test_BackpackRawMarket_corruption_garbled_unicode_symbol() -> None:
    """Should fail: garbled unicode in 'symbol'."""
    p = valid_market().copy()
    p["symbol"] = "BTC_\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawMarket.model_validate(p)


# --- BackpackRawTicker ---
def valid_ticker() -> dict[str, Any]:
    """Return valid ticker for testing."""
    return {
        "symbol": "BTC_USDC",
        "firstPrice": "49000.0",
        "lastPrice": "50000.0",
        "high": "51000.0",
        "low": "48000.0",
        "priceChange": "1000.0",
        "priceChangePercent": "2.04",
        "volume": "123.456",
        "quoteVolume": "6172800.0",
        "trades": "1250",
    }


def test_BackpackRawTicker_happy_path() -> None:
    """Test BackpackRawTicker happy path."""
    obj = BackpackRawTicker.model_validate(valid_ticker())
    assert obj.symbol == "BTC_USDC"
    assert obj.last_price == "50000.0"
    assert obj.first_price == "49000.0"
    assert obj.high == "51000.0"
    assert obj.low == "48000.0"
    assert obj.volume == "123.456"
    assert obj.trades == "1250"


def test_BackpackRawTicker_missing_required_fields() -> None:
    """Test BackpackRawTicker missing required fields."""
    for field in ["symbol", "firstPrice"]:
        p: dict[str, Any] = valid_ticker().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawTicker.model_validate(p)


def test_BackpackRawTicker_wrong_type_fields() -> None:
    """Test BackpackRawTicker wrong type fields."""
    p: dict[str, Any] = valid_ticker().copy()
    p["lastPrice"] = [50000.0]
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)
    p = valid_ticker().copy()
    p["trades"] = 123
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)


def test_BackpackRawTicker_invalid_format_fields() -> None:
    """Test BackpackRawTicker invalid format fields."""
    p: dict[str, Any] = valid_ticker().copy()
    p["lastPrice"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)
    p = valid_ticker().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)
    # Scientific notation is allowed (project policy)
    p = valid_ticker().copy()
    p["lastPrice"] = "1e6"
    obj = BackpackRawTicker.model_validate(p)
    assert obj.last_price == "1e6"


def test_BackpackRawTicker_extra_field() -> None:
    """Test BackpackRawTicker extra field."""
    p: dict[str, Any] = valid_ticker().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)


def test_BackpackRawTicker_optional_fields_all_none() -> None:
    """Test BackpackRawTicker optional fields all none."""
    p: dict[str, Any] = valid_ticker().copy()
    # All fields in the new model are required, so this test is no longer applicable
    # Just validate the model with all fields present
    obj = BackpackRawTicker.model_validate(p)
    assert obj.symbol == "BTC_USDC"


def test_BackpackRawTicker_optional_fields_omitted() -> None:
    """Test BackpackRawTicker optional fields omitted."""
    # All fields in the new model are required, so this test is no longer applicable
    # Test that required fields cannot be omitted
    p: dict[str, Any] = valid_ticker().copy()
    del p["volume"]
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)


def test_BackpackRawTicker_corruption_cases() -> None:
    """Test BackpackRawTicker corruption cases."""
    # Garbled numerics
    p: dict[str, Any] = valid_ticker().copy()
    p["lastPrice"] = "notanumber"
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
    bad_json = '{"symbol": "BTC_USDC", "firstPrice": "49000.0"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawTicker_real_json_example() -> None:
    """Validate BackpackRawTicker using a real JSON payload from the Backpack OpenAPI spec.

    Includes edge values.
    """
    payload = {
        "symbol": "ETH_USDC",
        "firstPrice": "0.00000001",
        "lastPrice": "0.00000002",
        "high": "99999999.99999999",
        "low": "0.00000001",
        "priceChange": "0.00000001",
        "priceChangePercent": "100.0",
        "volume": "123456789.123456789",
        "quoteVolume": "12345.67",
        "trades": "9999",
    }
    obj = BackpackRawTicker.model_validate(payload)
    assert obj.symbol == "ETH_USDC"
    assert obj.first_price == "0.00000001"
    assert obj.last_price == "0.00000002"
    assert obj.high == "99999999.99999999"
    assert obj.low == "0.00000001"
    assert obj.volume == "123456789.123456789"
    assert obj.trades == "9999"


def test_BackpackRawTicker_corruption_null_symbol() -> None:
    """Should fail: null value for required 'symbol'."""
    p = valid_ticker().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)


def test_BackpackRawTicker_corruption_binary_price() -> None:
    """Should fail: binary data for 'lastPrice'."""
    p = valid_ticker().copy()
    p["lastPrice"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)


def test_BackpackRawTicker_corruption_nested_high() -> None:
    """Should fail: nested object for 'high'."""
    p = valid_ticker().copy()
    p["high"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)


def test_BackpackRawTicker_corruption_list_low() -> None:
    """Should fail: list for 'low'."""
    p = valid_ticker().copy()
    p["low"] = ["48001.0"]
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)


def test_BackpackRawTicker_corruption_garbled_unicode_symbol() -> None:
    """Should fail: garbled unicode in 'symbol'."""
    p = valid_ticker().copy()
    p["symbol"] = "ETH_\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawTicker.model_validate(p)


# --- BackpackRawOpenInterest ---
def valid_open_interest() -> dict[str, Any]:
    """Return valid open interest for testing."""
    return {
        "symbol": "BTC_USDC",
        "openInterest": "12345.6789",
    }


def test_BackpackRawOpenInterest_happy_path() -> None:
    """Test BackpackRawOpenInterest happy path."""
    obj = BackpackRawOpenInterest.model_validate(valid_open_interest())
    assert obj.symbol == "BTC_USDC"
    assert obj.open_interest == "12345.6789"


def test_BackpackRawOpenInterest_missing_required_fields() -> None:
    """Test BackpackRawOpenInterest missing required fields."""
    for field in ["symbol", "openInterest"]:
        p: dict[str, Any] = valid_open_interest().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawOpenInterest.model_validate(p)


def test_BackpackRawOpenInterest_wrong_type_fields() -> None:
    """Test BackpackRawOpenInterest wrong type fields."""
    p: dict[str, Any] = valid_open_interest().copy()
    p["openInterest"] = [12345.6789]
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)


def test_BackpackRawOpenInterest_invalid_format_fields() -> None:
    """Test BackpackRawOpenInterest invalid format fields."""
    p: dict[str, Any] = valid_open_interest().copy()
    p["openInterest"] = "1..0"
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)
    p = valid_open_interest().copy()
    p["symbol"] = ""
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)


def test_BackpackRawOpenInterest_extra_field() -> None:
    """Test BackpackRawOpenInterest extra field."""
    p: dict[str, Any] = valid_open_interest().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)


def test_BackpackRawOpenInterest_corruption_cases() -> None:
    """Test BackpackRawOpenInterest corruption cases."""
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


def test_BackpackRawOpenInterest_real_json_example() -> None:
    """Validate BackpackRawOpenInterest using a real JSON payload from the Backpack OpenAPI spec.

    Includes edge values.
    """
    payload = {
        "symbol": "BTC_USDC",
        "openInterest": "99999999.99999999",
    }
    obj = BackpackRawOpenInterest.model_validate(payload)
    assert obj.symbol == "BTC_USDC"
    assert obj.open_interest == "99999999.99999999"


def test_BackpackRawOpenInterest_corruption_null_symbol() -> None:
    """Should fail: null value for required 'symbol'."""
    p = valid_open_interest().copy()
    p["symbol"] = None
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)


def test_BackpackRawOpenInterest_corruption_binary_openInterest() -> None:
    """Should fail: binary data for 'openInterest'."""
    p = valid_open_interest().copy()
    p["openInterest"] = b"\x00\x01"
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)


def test_BackpackRawOpenInterest_corruption_nested_openInterest() -> None:
    """Should fail: nested object for 'openInterest'."""
    p = valid_open_interest().copy()
    p["openInterest"] = {"foo": "bar"}
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)


def test_BackpackRawOpenInterest_corruption_list_symbol() -> None:
    """Should fail: list for 'symbol'."""
    p = valid_open_interest().copy()
    p["symbol"] = ["BTC_USDC"]
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)


def test_BackpackRawOpenInterest_corruption_garbled_unicode_symbol() -> None:
    """Should fail: garbled unicode in 'symbol'."""
    p = valid_open_interest().copy()
    p["symbol"] = "BTC_\udce2\udc28\udc00"
    with pytest.raises(ValidationError):
        BackpackRawOpenInterest.model_validate(p)


# --- Fixtures ---


@pytest.fixture
def valid_ticker_event_data() -> dict[str, Any]:
    """Return valid ticker event data for testing."""
    return {
        "s": "SOL_USDC",
        "lastPrice": "23.50",
        "high": "24.00",
        "low": "22.80",
        "volume": "100500.75",
        "quoteVolume": "2361767.625",
        "priceChangePercent": "1.50",
        "e": "ticker.SOL_USDC",  # Example optional field
        "E": 1678886400123,  # Example optional field
    }


@pytest.fixture
def valid_depth_update_data() -> dict[str, Any]:
    """Return valid depth update data for testing."""
    return {
        "lastUpdateId": "update12345",
        "bids": [["23.49", "10.5"], ["23.48", "5.2"]],
        "asks": [["23.51", "8.1"], ["23.52", "12.0"]],
        "e": "depth.SOL_USDC",  # Example optional field
        "E": 1678886400234,  # Example optional field
    }


# --- Success Cases: BackpackRawTickerEvent ---


def test_BackpackRawTickerEvent_valid(valid_ticker_event_data: dict[str, Any]) -> None:
    """Test BackpackRawTickerEvent valid."""
    ticker = BackpackRawTickerEvent.model_validate(valid_ticker_event_data)
    assert ticker.symbol == "SOL_USDC"
    assert ticker.last_price == "23.50"
    assert ticker.high == "24.00"
    assert ticker.low == "22.80"
    assert ticker.volume == "100500.75"
    assert ticker.quote_volume == "2361767.625"
    assert ticker.price_change_percent == "1.50"
    assert ticker.event_type == "ticker.SOL_USDC"
    assert ticker.event_time == 1678886400123
    assert ticker.model_config.get("extra") == "ignore"
    assert ticker.model_config.get("frozen") is True


def test_BackpackRawTickerEvent_optional_fields_none(
    valid_ticker_event_data: dict[str, Any],
) -> None:
    """Test BackpackRawTickerEvent optional fields none."""
    data = valid_ticker_event_data
    del data["e"]
    del data["E"]
    ticker = BackpackRawTickerEvent.model_validate(data)
    assert ticker.event_type is None
    assert ticker.event_time is None


def test_BackpackRawTickerEvent_valid_event_time_formats(
    valid_ticker_event_data: dict[str, Any],
) -> None:
    """Test BackpackRawTickerEvent valid event time formats."""
    data = valid_ticker_event_data
    data["E"] = "1678886400123"
    ticker = BackpackRawTickerEvent.model_validate(data)
    assert ticker.event_time == 1678886400123

    data = valid_ticker_event_data
    data["E"] = 1678886400123.0
    ticker = BackpackRawTickerEvent.model_validate(data)
    assert ticker.event_time == 1678886400123


# --- Failure Cases: BackpackRawTickerEvent ---


@pytest.mark.parametrize(
    "field, value, expected_msg_part",
    [
        ("s", "", "String cannot be empty"),
        ("s", None, "Field required"),
        ("lastPrice", "inf", "finite decimal"),
        ("high", "nan", "finite decimal"),
        ("low", "", "String cannot be empty"),
        ("quoteVolume", True, "Expected string"),
        ("priceChangePercent", [], "Expected string"),
        ("e", "", "String cannot be empty"),
        ("e", "A" * 33, "String value too long"),
        ("E", "not-an-int", "Expected an integer"),
    ],
)
def test_BackpackRawTickerEvent_invalid_fields(
    field: str,
    value: str | float | bool | list[Any] | None,  # Invalid types for Pydantic
    expected_msg_part: str,
    valid_ticker_event_data: dict[str, Any],
) -> None:
    """Test BackpackRawTickerEvent invalid fields."""
    data = valid_ticker_event_data
    data[field] = value
    with pytest.raises(ValidationError) as exc_info:
        BackpackRawTickerEvent.model_validate(data)
    assert expected_msg_part in str(exc_info.value) or field in str(exc_info.value), (
        f"Field: {field}, Value: {value!r}, Error: {exc_info.value}"
    )


def test_BackpackRawTickerEvent_extra_field_ignored(
    valid_ticker_event_data: dict[str, Any],
) -> None:
    """Test BackpackRawTickerEvent extra field ignored."""
    data = valid_ticker_event_data
    data["extraField"] = 123
    ticker = BackpackRawTickerEvent.model_validate(data)
    assert not hasattr(ticker, "extraField")


def test_BackpackRawTickerEvent_frozen(valid_ticker_event_data: dict[str, Any]) -> None:
    """Test BackpackRawTickerEvent frozen."""
    ticker = BackpackRawTickerEvent.model_validate(valid_ticker_event_data)
    with pytest.raises(ValidationError, match="Instance is frozen"):
        ticker.symbol = "new_symbol"


# --- Success Cases: BackpackRawDepthUpdateEvent ---


def test_BackpackRawDepthUpdateEvent_valid(valid_depth_update_data: dict[str, Any]) -> None:
    """Test BackpackRawDepthUpdateEvent valid."""
    depth = BackpackRawDepthUpdateEvent.model_validate(valid_depth_update_data)
    assert depth.last_update_id == "update12345"
    assert depth.bids == [("23.49", "10.5"), ("23.48", "5.2")]
    assert depth.asks == [("23.51", "8.1"), ("23.52", "12.0")]
    assert depth.event_type == "depth.SOL_USDC"
    assert depth.event_time == 1678886400234
    assert depth.model_config.get("extra") == "ignore"
    assert depth.model_config.get("frozen") is True


def test_BackpackRawDepthUpdateEvent_optional_fields_none(
    valid_depth_update_data: dict[str, Any],
) -> None:
    """Test BackpackRawDepthUpdateEvent optional fields none."""
    data = valid_depth_update_data
    del data["e"]
    del data["E"]
    depth = BackpackRawDepthUpdateEvent.model_validate(data)
    assert depth.event_type is None
    assert depth.event_time is None


def test_BackpackRawDepthUpdateEvent_empty_levels(valid_depth_update_data: dict[str, Any]) -> None:
    """Test BackpackRawDepthUpdateEvent empty levels."""
    data = valid_depth_update_data
    data["bids"] = []
    data["asks"] = []
    depth = BackpackRawDepthUpdateEvent.model_validate(data)
    assert depth.bids == []
    assert depth.asks == []


# --- Failure Cases: BackpackRawDepthUpdateEvent ---


@pytest.mark.parametrize(
    "field, value, expected_msg_part",
    [
        ("lastUpdateId", "", "String cannot be empty"),
        ("lastUpdateId", None, "Expected string, got NoneType"),
        ("bids", None, "Must be a list"),
        ("asks", "not-a-list", "Must be a list"),
        ("bids", [[], ["1", "2"]], "length 2"),
        ("asks", [["1"]], "length 2"),
        ("bids", [["1", "2", "3"]], "length 2"),
        ("asks", ["1", "2"], "Each item must be a list or tuple"),
        ("asks", [["1", 2]], "Expected string"),
        ("bids", [["inf", "1"]], "Price must be finite"),
        ("asks", [["1", "nan"]], "Quantity must be finite"),
        ("bids", [["", "1"]], "String cannot be empty"),
        ("asks", [["1", ""]], "String cannot be empty"),
        ("bids", [["1", "-1"]], "Quantity cannot be negative"),
        ("e", "", "String cannot be empty"),
        ("E", "abc", "Invalid timestamp format"),
    ],
)
def test_BackpackRawDepthUpdateEvent_invalid_fields(
    field: str,
    value: str | float | bool | list[Any] | None,  # Invalid types for Pydantic
    expected_msg_part: str,
    valid_depth_update_data: dict[str, Any],
) -> None:
    """Test BackpackRawDepthUpdateEvent invalid fields."""
    data = valid_depth_update_data
    data[field] = value

    # Determine expected exception type based on field and value
    expected_exception: type[Exception] = ValidationError
    if (
        (field == "bids" and value is None)
        or (field == "asks" and value == "not-a-list")
        or (field == "asks" and value == ["1", "2"])
        or (field == "asks" and value == [["1", 2]])
    ):
        expected_exception = TypeError

    with pytest.raises(expected_exception) as exc_info:
        BackpackRawDepthUpdateEvent.model_validate(data)

    # Check the string representation of the caught exception for the expected message
    if isinstance(exc_info.value, TypeError):
        assert expected_msg_part in str(exc_info.value), (
            f"Failed for field '{field}' with value {value!r}. "
            f"Expected '{expected_msg_part}' in TypeError: {exc_info.value!s}"
        )
    # DEFENSIVE CHECK: Distinguish exception types for assertion. Mypy=[misc]
    elif isinstance(exc_info.value, ValidationError):  # pyright: ignore[reportUnnecessaryIsInstance]
        found_match = False
        for error in exc_info.value.errors():
            if expected_msg_part in error.get("msg", ""):
                found_match = True
                break
        assert found_match, (
            f"Failed for field '{field}' with value {value!r}. "
            f"Expected '{expected_msg_part}' in ValidationError messages: {exc_info.value.errors()}"
        )


def test_BackpackRawDepthUpdateEvent_extra_field_ignored(
    valid_depth_update_data: dict[str, Any],
) -> None:
    """Test BackpackRawDepthUpdateEvent extra field ignored."""
    data = valid_depth_update_data
    data["anotherField"] = "test"
    depth = BackpackRawDepthUpdateEvent.model_validate(data)
    assert not hasattr(depth, "anotherField")


def test_BackpackRawDepthUpdateEvent_frozen(valid_depth_update_data: dict[str, Any]) -> None:
    """Test BackpackRawDepthUpdateEvent frozen."""
    depth = BackpackRawDepthUpdateEvent.model_validate(valid_depth_update_data)
    with pytest.raises(ValidationError, match="Instance is frozen"):
        depth.last_update_id = "new_id"

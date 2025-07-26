"""Unit Tests for Backpack Raw Kline Model (bp_raw_kline.py)."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKlineResponse
from cyberdelta.apis.exceptions.parsing import KlineTypeError
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# --- Test Data ---

VALID_KLINE_LIST = [
    1700000000000,  # startTimeMs (int)
    "100.0",  # openPrice (str)
    "102.5",  # highPrice (str)
    "99.5",  # lowPrice (str)
    "101.0",  # closePrice (str)
    "1000.123",  # volume (str)
    1700000059999,  # endTimeMs (int)
    "101000.456",  # quoteVolume (str)
    50,  # tradeCount (int)
    "500.1",  # takerBuyBaseVolume (str)
    "50500.2",  # takerBuyQuoteVolume (str)
    "0",  # ignored (str)
]


# --- Test Cases ---


def test_valid_kline_list_parsing() -> None:
    """Test successful parsing of a valid kline list."""
    kline = BackpackRawKlineResponse.model_validate(VALID_KLINE_LIST)

    # Verify field values after validation and Pydantic coercion
    assert kline.start_time_ms == 1700000000000
    assert kline.open_price == Decimal("100.0")
    assert kline.high_price == Decimal("102.5")
    assert kline.low_price == Decimal("99.5")
    assert kline.close_price == Decimal("101.0")
    assert kline.volume == Decimal("1000.123")
    assert kline.end_time_ms == 1700000059999
    assert kline.quote_volume == Decimal("101000.456")
    assert kline.trade_count == 50
    assert kline.taker_buy_base_volume == Decimal("500.1")
    assert kline.taker_buy_quote_volume == Decimal("50500.2")
    assert kline.ignored == "0"

    # Verify aliases were populated correctly
    assert kline.model_dump(by_alias=True)["startTimeMs"] == 1700000000000
    assert kline.model_dump(by_alias=True)["openPrice"] == Decimal("100.0")
    assert kline.model_dump(by_alias=True)["endTimeMs"] == 1700000059999

    # Test immutability (frozen=True)
    with pytest.raises(ValidationError) as exc_info:
        # Attempt assignment which should fail if frozen=True and validate_assignment=True
        kline.trade_count = 51
    assert "Instance is frozen" in str(exc_info.value)


def test_invalid_structure_input_type() -> None:
    """Test failure when input is not a list or tuple."""
    # Catch ValidationError and check message
    with pytest.raises(ValidationError) as exc_info:
        BackpackRawKlineResponse.model_validate({"key": "value"})  # Dict input
    assert "Expected 12-element list/tuple" in str(exc_info.value)

    with pytest.raises(ValidationError) as exc_info_str:  # Use different var name
        BackpackRawKlineResponse.model_validate("not_a_list")  # String input
    assert "Expected 12-element list/tuple" in str(exc_info_str.value)


def test_invalid_structure_list_length() -> None:
    """Test failure when input list has incorrect length."""
    invalid_list_short = VALID_KLINE_LIST[:-1]  # Length 11
    # Catch ValueError and check substring
    with pytest.raises(ValueError) as exc_info_short:
        BackpackRawKlineResponse.model_validate(invalid_list_short)
    assert "Field 'kline data': Expected 12-element list/tuple, got length 11" in str(
        exc_info_short.value
    )

    invalid_list_long = [*VALID_KLINE_LIST, "extra"]
    # Catch ValueError and check substring
    with pytest.raises(ValueError) as exc_info_long:
        BackpackRawKlineResponse.model_validate(invalid_list_long)
    assert "Field 'kline data': Expected 12-element list/tuple, got length 13" in str(
        exc_info_long.value
    )


@pytest.mark.parametrize(
    ("index", "field_name", "invalid_value", "expected_exception", "expected_error_msg"),
    [
        # --- startTimeMs validation (index 0) ---
        (0, "start_time_ms", "1700000000000", TypeFieldError, "must be integer, got string"),
        (0, "start_time_ms", 1700000000000.5, TypeFieldError, "must be integer, got float"),
        (0, "start_time_ms", -1, ValidationError, "Value must be non-negative"),
        # --- openPrice validation (index 1) ---
        (1, "open_price", 100.0, KlineTypeError, "Raw value must be a string"),
        (1, "open_price", 100, KlineTypeError, "Raw value must be a string"),
        (1, "open_price", True, KlineTypeError, "Raw value must be a string"),
        (1, "open_price", "", EmptyStringError, "String cannot be empty"),
        (1, "open_price", " ", EmptyStringError, "String cannot be empty"),
        (1, "open_price", "NaN", ValidationError, "must represent a finite decimal"),
        (1, "open_price", "Infinity", ValidationError, "must represent a finite decimal"),
        (1, "open_price", "-inf", ValidationError, "must represent a finite decimal"),
        (
            1,
            "open_price",
            "not_a_decimal",
            ValidationError,
            "Cannot convert 'not_a_decimal' to Decimal",
        ),
        (1, "open_price", "1" * 65, TypeFieldError, "must be string with max length 64"),
        # --- highPrice validation (index 2) ---
        (2, "high_price", None, KlineTypeError, "Raw value must be a string, got NoneType"),
        (2, "high_price", 102.5, KlineTypeError, "Raw value must be a string"),
        (2, "high_price", "inf", ValidationError, "must represent a finite decimal"),
        # --- lowPrice validation (index 3) ---
        (3, "low_price", ["list"], KlineTypeError, "Raw value must be a string"),
        (3, "low_price", "-Infinity", ValidationError, "must represent a finite decimal"),
        # --- closePrice validation (index 4) ---
        (4, "close_price", 101, KlineTypeError, "Raw value must be a string"),
        (4, "close_price", "", EmptyStringError, "String cannot be empty"),
        # --- volume validation (index 5) ---
        (5, "volume", 1000.123, KlineTypeError, "Raw value must be a string"),
        (5, "volume", "NaN", ValidationError, "must represent a finite decimal"),
        # --- endTimeMs validation (index 6) ---
        (6, "end_time_ms", "1700000059999", TypeFieldError, "must be integer, got string"),
        (6, "end_time_ms", -1700000059999, ValidationError, "Value must be non-negative"),
        # --- quoteVolume validation (index 7) ---
        (7, "quote_volume", 101000.456, KlineTypeError, "Raw value must be a string"),
        (
            7,
            "quote_volume",
            "101000.456x",
            ValidationError,
            "Cannot convert '101000.456x' to Decimal",
        ),
        # --- tradeCount validation (index 8) ---
        (8, "trade_count", "50", TypeFieldError, "must be integer, got string"),
        (8, "trade_count", -10, ValidationError, "Value must be non-negative"),
        # --- takerBuyBaseVolume validation (index 9) ---
        (9, "taker_buy_base_volume", 500.1, KlineTypeError, "Raw value must be a string"),
        (
            9,
            "taker_buy_base_volume",
            "Infinity",
            ValidationError,
            "must represent a finite decimal",
        ),
        # --- takerBuyQuoteVolume validation (index 10) ---
        (10, "taker_buy_quote_volume", 50500.2, KlineTypeError, "Raw value must be a string"),
        (10, "taker_buy_quote_volume", "NaN", ValidationError, "must represent a finite decimal"),
        # --- ignored validation (index 11) ---
        (11, "ignored", 0, KlineTypeError, "Raw value must be a string"),
        (11, "ignored", "", ValidationError, "String cannot be empty or whitespace"),
        (11, "ignored", "a" * 65, ValidationError, "string_too_long"),
    ],
)
def test_field_validation_failures(
    index: int,
    field_name: str,
    invalid_value: object,
    expected_exception: type[Exception],
    expected_error_msg: str,
) -> None:
    """Test failures for various invalid raw field types or formats within the list."""
    # Create as list[object] from the start
    invalid_list: list[object] = list(VALID_KLINE_LIST)
    invalid_list[index] = invalid_value

    with pytest.raises(expected_exception) as exc_info:
        BackpackRawKlineResponse.model_validate(invalid_list)

    # Check the string representation of the caught exception for the expected message
    error_str = str(exc_info.value)
    assert expected_error_msg in error_str, (
        f"Failed for index {index} ({field_name}) with value {invalid_value!r}. "
        f"Expected '{expected_error_msg}' in error: {error_str}"
    )


# No tests for OHLC relationship needed as per Raw Model Policy

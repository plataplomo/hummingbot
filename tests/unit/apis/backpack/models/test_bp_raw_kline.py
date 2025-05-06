"""
Unit Tests for Backpack Raw Kline Model (bp_raw_kline.py)
"""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline

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
    kline = BackpackRawKline.model_validate(VALID_KLINE_LIST)

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
    with pytest.raises(TypeError, match="Expected list or tuple input, got dict"):
        BackpackRawKline.model_validate({"key": "value"})  # Dict input

    with pytest.raises(TypeError, match="Expected list or tuple input, got str"):
        BackpackRawKline.model_validate("not_a_list")  # String input


def test_invalid_structure_list_length() -> None:
    """Test failure when input list has incorrect length."""
    invalid_list_short = VALID_KLINE_LIST[:-1]  # Length 11
    with pytest.raises(ValidationError) as exc_info:
        BackpackRawKline.model_validate(invalid_list_short)
    assert "Expected 12 elements in kline data list, got 11" in str(exc_info.value)

    invalid_list_long = VALID_KLINE_LIST + ["extra"]
    with pytest.raises(ValidationError) as exc_info:
        BackpackRawKline.model_validate(invalid_list_long)
    assert "Expected 12 elements in kline data list, got 13" in str(exc_info.value)


@pytest.mark.parametrize(
    "index, field_name, invalid_value, expected_exception, expected_error_msg",
    [
        # --- startTimeMs validation (index 0) ---
        # Wrong raw type -> Expect TypeError
        (0, "start_time_ms", "1700000000000", TypeError, "Raw value must be an integer"),
        (0, "start_time_ms", 1700000000000.5, TypeError, "Raw value must be an integer"),
        # Invalid format (Correct raw type: int) -> Expect ValidationError
        (0, "start_time_ms", -1, ValidationError, "Value must be non-negative"),
        # --- openPrice validation (index 1) ---
        # Wrong raw type -> Expect TypeError
        (1, "open_price", 100.0, TypeError, "Raw value must be a string"),
        (1, "open_price", 100, TypeError, "Raw value must be a string"),
        (1, "open_price", True, TypeError, "Raw value must be a string"),
        # Invalid format (Correct raw type: str) -> Expect ValidationError
        (1, "open_price", "", ValidationError, "String cannot be empty or whitespace"),
        (1, "open_price", " ", ValidationError, "String cannot be empty or whitespace"),
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
        (1, "open_price", "1" * 65, ValidationError, "String value too long (max 64 chars)"),
        # --- highPrice validation (index 2) ---
        # Wrong raw type -> Expect TypeError
        # NOTE: None input reaches the field validator and raises TypeError
        (2, "high_price", None, TypeError, "Raw value must be a string, got NoneType"),
        (2, "high_price", 102.5, TypeError, "Raw value must be a string"),
        # Invalid format (Correct raw type: str) -> Expect ValidationError
        (2, "high_price", "inf", ValidationError, "must represent a finite decimal"),
        # --- lowPrice validation (index 3) ---
        # Wrong raw type -> Expect TypeError
        (3, "low_price", ["list"], TypeError, "Raw value must be a string"),
        # Invalid format (Correct raw type: str) -> Expect ValidationError
        (3, "low_price", "-Infinity", ValidationError, "must represent a finite decimal"),
        # --- closePrice validation (index 4) ---
        # Wrong raw type -> Expect TypeError
        (4, "close_price", 101, TypeError, "Raw value must be a string"),
        # Invalid format (Correct raw type: str) -> Expect ValidationError
        (4, "close_price", "", ValidationError, "String cannot be empty or whitespace"),
        # --- volume validation (index 5) ---
        # Wrong raw type -> Expect TypeError
        (5, "volume", 1000.123, TypeError, "Raw value must be a string"),
        # Invalid format (Correct raw type: str) -> Expect ValidationError
        (5, "volume", "NaN", ValidationError, "must represent a finite decimal"),
        # --- endTimeMs validation (index 6) ---
        # Wrong raw type -> Expect TypeError
        (6, "end_time_ms", "1700000059999", TypeError, "Raw value must be an integer"),
        # Invalid format (Correct raw type: int) -> Expect ValidationError
        (6, "end_time_ms", -1700000059999, ValidationError, "Value must be non-negative"),
        # --- quoteVolume validation (index 7) ---
        # Wrong raw type -> Expect TypeError
        (7, "quote_volume", 101000.456, TypeError, "Raw value must be a string"),
        # Invalid format (Correct raw type: str) -> Expect ValidationError
        (
            7,
            "quote_volume",
            "101000.456x",
            ValidationError,
            "Cannot convert '101000.456x' to Decimal",
        ),
        # --- tradeCount validation (index 8) ---
        # Wrong raw type -> Expect TypeError
        (8, "trade_count", "50", TypeError, "Raw value must be an integer"),
        # Invalid format (Correct raw type: int) -> Expect ValidationError
        (8, "trade_count", -10, ValidationError, "Value must be non-negative"),
        # --- takerBuyBaseVolume validation (index 9) ---
        # Wrong raw type -> Expect TypeError
        (9, "taker_buy_base_volume", 500.1, TypeError, "Raw value must be a string"),
        # Invalid format (Correct raw type: str) -> Expect ValidationError
        (
            9,
            "taker_buy_base_volume",
            "Infinity",
            ValidationError,
            "must represent a finite decimal",
        ),
        # --- takerBuyQuoteVolume validation (index 10) ---
        # Wrong raw type -> Expect TypeError
        (10, "taker_buy_quote_volume", 50500.2, TypeError, "Raw value must be a string"),
        # Invalid format (Correct raw type: str) -> Expect ValidationError
        (10, "taker_buy_quote_volume", "NaN", ValidationError, "must represent a finite decimal"),
        # --- ignored validation (index 11) ---
        # Wrong raw type -> Expect TypeError
        (11, "ignored", 0, TypeError, "Raw value must be a string"),
        # Invalid format (Correct raw type: str) -> Expect ValidationError
        (11, "ignored", "", ValidationError, "String cannot be empty or whitespace"),
        (11, "ignored", "a" * 65, ValidationError, "String value too long (max 64 chars)"),
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
        BackpackRawKline.model_validate(invalid_list)

    # Check the string representation of the caught exception for the expected message
    error_str = str(exc_info.value)
    assert expected_error_msg in error_str, (
        f"Failed for index {index} ({field_name}) with value {invalid_value!r}. "
        f"Expected '{expected_error_msg}' in error: {error_str}"
    )


# No tests for OHLC relationship needed as per Raw Model Policy

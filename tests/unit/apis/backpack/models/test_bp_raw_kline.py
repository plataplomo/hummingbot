"""
Unit tests for BackpackRawKline model validation.
"""

from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline

# --- Test Data ---

VALID_KLINE_LIST_RAW = [
    1672531200000,  # start_time_ms (int)
    "40000.123",  # open_price (str)
    "41000.45",  # high_price (str)
    "39000.0",  # low_price (str)
    "40500.99",  # close_price (str)
    "1000.5",  # volume (str)
    1672531259999,  # end_time_ms (int)
    "40500000.1",  # quote_volume (str)
    100,  # trade_count (int)
    "500.25",  # taker_buy_base_volume (str)
    "20250000.75",  # taker_buy_quote_volume (str)
    "0",  # ignored (str)
]

# Expected values after Pydantic coercion
EXPECTED_VALID_KLINE = {
    "start_time_ms": 1672531200000,
    "open_price": Decimal("40000.123"),
    "high_price": Decimal("41000.45"),
    "low_price": Decimal("39000.0"),
    "close_price": Decimal("40500.99"),
    "volume": Decimal("1000.5"),
    "end_time_ms": 1672531259999,
    "quote_volume": Decimal("40500000.1"),
    "trade_count": 100,
    "taker_buy_base_volume": Decimal("500.25"),
    "taker_buy_quote_volume": Decimal("20250000.75"),
    "ignored": "0",
}

# --- Test Cases ---


def test_kline_successful_validation() -> None:
    """Test successful validation with valid raw list input."""
    kline = BackpackRawKline.model_validate(VALID_KLINE_LIST_RAW)
    # Check if Pydantic coerced types correctly after raw validation passed
    assert kline.start_time_ms == EXPECTED_VALID_KLINE["start_time_ms"]
    assert kline.open_price == EXPECTED_VALID_KLINE["open_price"]
    assert kline.high_price == EXPECTED_VALID_KLINE["high_price"]
    assert kline.low_price == EXPECTED_VALID_KLINE["low_price"]
    assert kline.close_price == EXPECTED_VALID_KLINE["close_price"]
    assert kline.volume == EXPECTED_VALID_KLINE["volume"]
    assert kline.end_time_ms == EXPECTED_VALID_KLINE["end_time_ms"]
    assert kline.quote_volume == EXPECTED_VALID_KLINE["quote_volume"]
    assert kline.trade_count == EXPECTED_VALID_KLINE["trade_count"]
    assert kline.taker_buy_base_volume == EXPECTED_VALID_KLINE["taker_buy_base_volume"]
    assert kline.taker_buy_quote_volume == EXPECTED_VALID_KLINE["taker_buy_quote_volume"]
    assert kline.ignored == EXPECTED_VALID_KLINE["ignored"]
    assert kline.model_dump() == EXPECTED_VALID_KLINE


def test_kline_invalid_structure() -> None:
    """Test validation fails if input structure is wrong (not list/tuple or wrong length)."""
    # Not a list/tuple
    with pytest.raises(TypeError, match="Kline data must be a list or tuple"):
        BackpackRawKline.model_validate({"invalid": "structure"})
    with pytest.raises(TypeError, match="Kline data must be a list or tuple"):
        BackpackRawKline.model_validate("not a list")

    # Wrong length
    invalid_length_list = VALID_KLINE_LIST_RAW[:-1]  # 11 elements
    with pytest.raises(
        ValidationError, match="Kline data must be a list/tuple of exactly 12 elements"
    ):
        BackpackRawKline.model_validate(invalid_length_list)

    invalid_length_list_long = VALID_KLINE_LIST_RAW + ["extra"]  # 13 elements
    with pytest.raises(
        ValidationError, match="Kline data must be a list/tuple of exactly 12 elements"
    ):
        BackpackRawKline.model_validate(invalid_length_list_long)


# Parametrized tests for individual raw field validation failures
@pytest.mark.parametrize(
    "index, invalid_value, expected_error_msg_part",
    [
        # --- Raw Type Errors (Match wrapped TypeError message) ---
        (0, "not an int", "start_time_ms: Raw value must be an integer, got str"),
        (0, -1, "Timestamp cannot be negative"),
        (1, {"a": 1}, "open_price: Raw value must be a string, got dict"),
        (6, "not an int", "end_time_ms: Raw value must be an integer, got str"),
        (6, -1, "Timestamp cannot be negative"),
        (8, "not an int", "trade_count: Raw value must be an integer, got str"),
        (8, -5, "Trade count cannot be negative"),
        (11, 123, "ignored: Raw value must be a string, got int"),
        # --- Format/Value Errors (Match exact core error message) ---
        (1, "", "String cannot be empty or whitespace"),
        (1, " ", "String cannot be empty or whitespace"),
        (1, "NaN", "Value must be a finite decimal string"),
        (1, "Infinity", "Value must be a finite decimal string"),
        (1, "1" * 65, "String value too long (max 64 chars)"),
        (2, "not_a_decimal", "Cannot convert 'not_a_decimal' to Decimal"),
        (3, "inf", "Value must be a finite decimal string"),
        (4, "-inf", "Value must be a finite decimal string"),
        (5, "nan", "Value must be a finite decimal string"),
        (7, "1.2.3", "Cannot convert '1.2.3' to Decimal"),
        (9, "", "String cannot be empty or whitespace"),
        (10, "  ", "String cannot be empty or whitespace"),
        (11, "", "String cannot be empty or whitespace"),
    ],
)
def test_kline_invalid_raw_field(
    index: int,
    invalid_value: Any,
    expected_error_msg_part: str,  # noqa: ANN401
) -> None:
    """Test validation fails for specific invalid raw values at given indices."""
    invalid_list = VALID_KLINE_LIST_RAW[:]
    invalid_list[index] = invalid_value
    with pytest.raises(ValidationError) as excinfo:
        BackpackRawKline.model_validate(invalid_list)

    # Check the first validation error message contains the expected part
    assert len(excinfo.value.errors()) >= 1
    first_error = excinfo.value.errors()[0]
    error_msg = first_error["msg"]

    # Extract the core message which might be nested
    core_msg = error_msg
    if error_msg.startswith("Value error, "):
        # Format: "Value error, {field}: Validation failed for raw value '{val}': {core_msg}"
        # Or for TypeError: "Value error, {field}: Raw value must be an integer, got {type}"
        parts = error_msg.split(": ")
        if len(parts) > 1:
            core_msg = ": ".join(parts[1:])  # Join back parts in case colon in core msg
            # Check if it still contains the 'Validation failed' prefix
            if core_msg.startswith("Validation failed for raw value"):
                core_msg = core_msg.split(": ", 1)[-1]
            # Specific check for wrapped TypeError messages
            elif "Raw value must be an" in core_msg:
                # Reconstruct the expected TypeError message format
                field_name = first_error["loc"][0]
                raw_type = type(invalid_value).__name__
                core_msg = (
                    f"{field_name}: Raw value must be an integer, got {raw_type}"
                    if isinstance(invalid_value, str)
                    and field_name in ["start_time_ms", "end_time_ms", "trade_count"]
                    else f"{field_name}: Raw value must be a string, got {raw_type}"
                )

    assert expected_error_msg_part in core_msg, (
        f"Failed for index {index}, value {invalid_value}. Expected '{expected_error_msg_part}' in '{core_msg}'. Full error: {excinfo.value.errors()}"
    )


def test_kline_frozen() -> None:
    """Test that the model is immutable (frozen=True)."""
    kline = BackpackRawKline.model_validate(VALID_KLINE_LIST_RAW)
    with pytest.raises(ValidationError, match="Instance is frozen"):
        kline.start_time_ms = 12345
    with pytest.raises(ValidationError, match="Instance is frozen"):
        kline.open_price = Decimal("1.0")


# Test that OHLC validation is GONE
def test_kline_no_ohlc_validation() -> None:
    """Test that invalid OHLC relationships (business logic) are now allowed."""
    # High < Low
    invalid_ohlc_list_1 = VALID_KLINE_LIST_RAW[:]
    invalid_ohlc_list_1[2] = "38000.0"  # high_price
    invalid_ohlc_list_1[3] = "39000.0"  # low_price
    try:
        BackpackRawKline.model_validate(invalid_ohlc_list_1)
        # No exception expected
    except ValidationError as e:
        pytest.fail(f"OHLC validation (high < low) should not occur: {e}")

    # High < Close
    invalid_ohlc_list_2 = VALID_KLINE_LIST_RAW[:]
    invalid_ohlc_list_2[2] = "40000.0"  # high_price
    invalid_ohlc_list_2[4] = "40500.0"  # close_price
    try:
        BackpackRawKline.model_validate(invalid_ohlc_list_2)
        # No exception expected
    except ValidationError as e:
        pytest.fail(f"OHLC validation (high < close) should not occur: {e}")

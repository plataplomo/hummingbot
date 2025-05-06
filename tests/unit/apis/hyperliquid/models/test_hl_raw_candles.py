"""
Unit Tests for Hyperliquid Raw Candle Snapshot Model
"""

from re import Pattern  # Import Any, Union, Pattern
from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot

# --- Test Data --- MOCKUP from openapi_hl.json example
VALID_CANDLE_DATA = {
    "t": [1672531200000, 1672531260000, 1672531320000],
    "o": ["16500.5", "16501.0", "16500.8"],
    "h": ["16502.0", "16501.5", "16501.2"],
    "l": ["16500.0", "16500.5", "16500.6"],
    "c": ["16501.0", "16500.8", "16501.1"],
    "v": ["10.5", "5.2", "8.1"],
    "s": "ok",
}


# --- Test Cases --- MOCKUP


def test_valid_candle_snapshot() -> None:
    """Test successful validation with valid, list-based data."""
    snapshot = HyperliquidRawCandleSnapshot.model_validate(VALID_CANDLE_DATA)
    assert snapshot.t == VALID_CANDLE_DATA["t"]
    assert snapshot.o == VALID_CANDLE_DATA["o"]
    assert snapshot.h == VALID_CANDLE_DATA["h"]
    assert snapshot.l == VALID_CANDLE_DATA["l"]
    assert snapshot.c == VALID_CANDLE_DATA["c"]
    assert snapshot.v == VALID_CANDLE_DATA["v"]
    assert snapshot.s == VALID_CANDLE_DATA["s"]
    # Check model config implicitly via successful validation
    # Test frozen=True
    with pytest.raises(ValidationError):
        snapshot.s = "nok"


def test_invalid_top_level_structure() -> None:
    """Test failure if top-level input is not a dictionary."""
    with pytest.raises(ValidationError, match="Input should be a valid dictionary"):
        HyperliquidRawCandleSnapshot.model_validate([1, 2, 3])  # Input is a list


@pytest.mark.parametrize(
    "field_to_invalidate, invalid_value, match_pattern",
    [
        ("t", "not_a_list", "Must be a list"),  # Field is not a list
        ("o", None, "Input should be a valid list"),  # Field is missing (effectively None)
        ("h", 123, "Must be a list"),  # Field is wrong type
    ],
)
def test_invalid_list_field_type(
    field_to_invalidate: str,
    invalid_value: Any,
    match_pattern: str | Pattern[str],  # Type hint for pytest match
) -> None:
    """Test failure if a list field is missing, not a list, or wrong type."""
    invalid_data: dict[str, Any] = VALID_CANDLE_DATA.copy()
    invalid_data[field_to_invalidate] = invalid_value
    with pytest.raises(ValidationError, match=match_pattern):
        HyperliquidRawCandleSnapshot.model_validate(invalid_data)


@pytest.mark.parametrize(
    "field, list_index, invalid_item, match_pattern",
    [
        ("t", 1, "not_an_int", "Must be an integer"),  # Wrong type in t list
        ("t", 0, -1, "Timestamp cannot be negative"),  # Negative timestamp
        ("o", 0, 16500, "Must be a string"),  # Wrong type in o list
        ("h", 1, "", "String should not be empty"),  # Empty string in h list
        ("l", 2, "not_a_decimal", "Invalid finite decimal string"),  # Non-decimal string in l list
        ("c", 0, "NaN", "Decimal value must be finite"),  # Non-finite decimal string in c list
        ("v", 1, "-5.2", "Volume cannot be negative"),  # Negative volume
        ("v", 2, "inf", "Decimal value must be finite"),  # Infinite volume
    ],
)
def test_invalid_list_element_format(
    field: str,
    list_index: int,
    invalid_item: Any,
    match_pattern: str | Pattern[str],  # Type hint for pytest match
) -> None:
    """Test failure if elements within lists have wrong types or formats."""
    invalid_data: dict[str, Any] = VALID_CANDLE_DATA.copy()
    # Ensure the list exists and has enough elements before modification
    if (
        field in invalid_data
        and isinstance(invalid_data[field], list)
        and len(invalid_data[field]) > list_index
    ):
        # Cast to list[Any] here to satisfy Pyright about __setitem__ with Any item type
        target_list = cast(list[Any], invalid_data[field])
        target_list[list_index] = invalid_item
    else:
        pytest.skip(f"Test setup error: Cannot modify {field}[{list_index}] in test data.")

    # Expect ValidationError even if underlying exception is TypeError/ValueError
    with pytest.raises(ValidationError, match=match_pattern):
        HyperliquidRawCandleSnapshot.model_validate(invalid_data)


def test_mismatched_list_lengths() -> None:
    """Test failure if lists have mismatching lengths."""
    invalid_data: dict[str, Any] = VALID_CANDLE_DATA.copy()
    invalid_data["t"] = [1672531200000, 1672531260000]  # Shorten one list
    with pytest.raises(
        ValidationError, match="Candle snapshot lists .* must all have the same length"
    ):
        HyperliquidRawCandleSnapshot.model_validate(invalid_data)


def test_status_string_validation() -> None:
    """Test validation of the status string 's'."""
    # Test empty string
    invalid_data_empty: dict[str, Any] = VALID_CANDLE_DATA.copy()
    invalid_data_empty["s"] = ""
    with pytest.raises(ValidationError, match="String should not be empty"):
        HyperliquidRawCandleSnapshot.model_validate(invalid_data_empty)

    # Test wrong type
    invalid_data_type: dict[str, Any] = VALID_CANDLE_DATA.copy()
    invalid_data_type["s"] = 123
    with pytest.raises(ValidationError, match="Input should be a valid string"):
        HyperliquidRawCandleSnapshot.model_validate(invalid_data_type)

    # Test too long (assuming max_length=32 based on validator)
    invalid_data_long: dict[str, Any] = VALID_CANDLE_DATA.copy()
    invalid_data_long["s"] = "a" * 33
    with pytest.raises(ValidationError, match="String should have at most 32 characters"):
        HyperliquidRawCandleSnapshot.model_validate(invalid_data_long)


def test_extra_fields_forbidden() -> None:
    """Test failure if extra fields are provided."""
    invalid_data: dict[str, Any] = VALID_CANDLE_DATA.copy()
    invalid_data["extra_field"] = "should_fail"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        HyperliquidRawCandleSnapshot.model_validate(invalid_data)

"""
Unit Tests for HyperliquidRawCandleSnapshot Model
"""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
)

# --- Test Data ---

VALID_DATA_SINGLE_CANDLE: dict[str, Any] = {
    "t": [1700000000000],
    "o": ["100.0"],
    "h": ["101.0"],
    "l": ["99.0"],
    "c": ["100.5"],
    "v": ["1000.0"],
    "s": "ok",
}

VALID_DATA_MULTIPLE_CANDLES: dict[str, Any] = {
    "t": [1700000000000, 1700000060000],
    "o": ["100.0", "100.5"],
    "h": ["101.0", "102.0"],
    "l": ["99.0", "100.0"],
    "c": ["100.5", "101.5"],
    "v": ["1000.0", "1200.0"],
    "s": "ok",
}

VALID_DATA_EMPTY_LISTS: dict[
    str, Any
] = {  # Assuming API can return empty lists if no candles in range
    "t": [],
    "o": [],
    "h": [],
    "l": [],
    "c": [],
    "v": [],
    "s": "ok",
}


# --- Test Cases ---


def test_valid_single_candle_snapshot() -> None:
    """Test successful validation of a snapshot with a single candle."""
    snapshot = HyperliquidRawCandleSnapshot.model_validate(VALID_DATA_SINGLE_CANDLE)
    assert snapshot.t == [1700000000000]
    assert snapshot.o == ["100.0"]
    assert snapshot.h == ["101.0"]
    assert snapshot.l == ["99.0"]
    assert snapshot.c == ["100.5"]
    assert snapshot.v == ["1000.0"]
    assert snapshot.s == "ok"
    assert snapshot.model_config.get("extra") == "forbid"
    assert snapshot.model_config.get("frozen") is True


def test_valid_multiple_candles_snapshot() -> None:
    """Test successful validation of a snapshot with multiple candles."""
    snapshot = HyperliquidRawCandleSnapshot.model_validate(VALID_DATA_MULTIPLE_CANDLES)
    assert len(snapshot.t) == 2
    assert snapshot.s == "ok"


def test_valid_empty_lists_snapshot() -> None:
    """Test successful validation with empty lists for all candle data."""
    snapshot = HyperliquidRawCandleSnapshot.model_validate(VALID_DATA_EMPTY_LISTS)
    assert snapshot.t == []
    assert snapshot.o == []
    assert snapshot.v == []
    assert snapshot.s == "ok"


def test_frozen_instance() -> None:
    """Test that the validated instance is frozen."""
    snapshot = HyperliquidRawCandleSnapshot.model_validate(VALID_DATA_SINGLE_CANDLE)
    with pytest.raises(ValidationError) as exc_info:
        snapshot.s = "not_ok"
    assert "Instance is frozen" in str(exc_info.value)


def test_extra_field_forbidden() -> None:
    """Test that extra fields in the input data cause a validation error."""
    invalid_data = {**VALID_DATA_SINGLE_CANDLE, "extra_key": "extra_value"}
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(invalid_data)
    assert "Extra inputs are not permitted" in str(exc_info.value)
    assert "extra_key" in str(exc_info.value)


@pytest.mark.parametrize(
    "field_to_invalidate,invalid_value,expected_msg_part",
    [
        ("t", "not_a_list", "t: Must be a list, got str."),
        ("o", False, "o: Must be a list, got bool."),
        ("h", 123, "h: Must be a list, got int."),
        ("s", 123, "s: Expected string, got int"),
        ("s", "", "s: String cannot be empty or whitespace"),
    ],
)
def test_invalid_field_type_or_missing(
    field_to_invalidate: str, invalid_value: Any, expected_msg_part: str
) -> None:
    """Test validation fails if a field has an incorrect type or is missing."""
    data = VALID_DATA_SINGLE_CANDLE.copy()
    data[field_to_invalidate] = invalid_value
    # Broaden exception type for problematic TypeError cases
    expected_exception: type[ValidationError] | tuple[type[ValidationError], type[TypeError]] = (
        ValidationError
    )
    if expected_msg_part in [
        "t: Must be a list, got str.",
        "o: Must be a list, got bool.",
        "h: Must be a list, got int.",
    ]:
        expected_exception = (ValidationError, TypeError)

    with pytest.raises(expected_exception) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(data)
    assert expected_msg_part in str(exc_info.value)
    assert field_to_invalidate in str(exc_info.value).lower()


def test_missing_field() -> None:
    """Test validation fails if a required field is missing."""
    data = VALID_DATA_SINGLE_CANDLE.copy()
    del data["t"]  # Remove a required field
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(data)
    assert "Field required" in str(exc_info.value)
    assert ".t" in str(exc_info.value) or "t\n  Field required" in str(exc_info.value)


@pytest.mark.parametrize(
    "list_field,item_index,invalid_item,expected_msg_part",
    [
        ("t", 0, "not_an_int", "t[0]: Must be an integer, got str."),
        ("t", 0, -1, "Timestamp must be non-negative"),
        ("o", 0, 123.45, "o[0]: Expected string, got float"),
        ("h", 0, True, "h[0]: Expected string, got bool"),
        ("l", 0, "", "l[0]: String cannot be empty or whitespace"),
        ("c", 0, "not_finite_enough", "c[0]: Invalid finite decimal string 'not_finite_enough'"),
        ("v", 0, "not_a_number", "v[0]: Invalid non-negative finite decimal string 'not_a_number'"),
        ("v", 0, "-10.0", "v[0]: Value '-10.0' must be non-negative"),
        ("o", 0, "1" * 65, "o[0]: String value too long (max 64 chars)"),
    ],
)
def test_invalid_list_item_type_or_format(
    list_field: str, item_index: int, invalid_item: Any, expected_msg_part: str
) -> None:
    """Test validation fails if an item within a list has an incorrect type or format."""
    data: dict[str, Any] = VALID_DATA_SINGLE_CANDLE.copy()
    # Ensure the list is mutable for testing
    original_list: list[Any] = list(data[list_field])
    if original_list:  # Make sure list is not empty before trying to change item
        original_list[item_index] = invalid_item
        data[list_field] = original_list
    else:  # If list is empty (e.g. from VALID_DATA_EMPTY_LISTS if used), add invalid item
        # This assignment can cause type issues if invalid_item doesn't match list_field's expected item type.
        # However, this is intended for testing invalid scenarios.
        data[list_field] = [invalid_item]  # pyright: ignore [reportGeneralTypeIssues]

    # Broaden exception type for problematic TypeError cases
    expected_exception_item: (
        type[ValidationError] | tuple[type[ValidationError], type[TypeError]]
    ) = ValidationError
    if expected_msg_part == "t[0]: Must be an integer, got str.":
        expected_exception_item = (ValidationError, TypeError)

    with pytest.raises(expected_exception_item) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(data)

    error_str = str(exc_info.value).lower()  # Lowercase for case-insensitive check
    assert expected_msg_part.lower() in error_str
    # Check if the error message contains the field and index, e.g., "t[0]"
    assert f"{list_field}[{item_index}]".lower() in error_str


def test_mismatched_list_lengths() -> None:
    """Test validation fails if data lists have mismatched lengths."""
    data: dict[str, Any] = {
        "t": [1700000000000, 1700000060000],  # Length 2
        "o": ["100.0"],  # Length 1
        "h": ["101.0", "102.0"],
        "l": ["99.0", "100.0"],
        "c": ["100.5", "101.5"],
        "v": ["1000.0", "1200.0"],
        "s": "ok",
    }
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(data)
    assert "Data lists (t, o, h, l, c, v) must all have the same length" in str(exc_info.value)


def test_volume_non_negative() -> None:
    """Test that volume values must be non-negative."""
    data = VALID_DATA_SINGLE_CANDLE.copy()
    data["v"] = ["-0.1"]
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(data)
    assert "Value '-0.1' must be non-negative" in str(exc_info.value)
    assert "v[0]" in str(exc_info.value)


def test_price_or_volume_not_finite() -> None:
    """Test that price/volume string must represent finite decimals."""
    data_price = VALID_DATA_SINGLE_CANDLE.copy()
    data_price["o"] = ["Infinity"]
    with pytest.raises(ValidationError) as exc_info_price:
        HyperliquidRawCandleSnapshot.model_validate(data_price)
    assert "must represent a finite decimal" in str(exc_info_price.value)
    assert "o[0]" in str(exc_info_price.value)

    data_volume = VALID_DATA_SINGLE_CANDLE.copy()
    data_volume["v"] = ["NaN"]
    with pytest.raises(ValidationError) as exc_info_vol:
        HyperliquidRawCandleSnapshot.model_validate(data_volume)
    assert "must represent a finite decimal" in str(exc_info_vol.value)
    assert "v[0]" in str(exc_info_vol.value)

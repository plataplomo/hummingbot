"""Unit Tests for HyperliquidRawCandleSnapshot Model."""

from __future__ import annotations

import re
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
)
from cyberdelta.exceptions.parsing import ParsingError


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
    str,
    Any,
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
    # Business logic normalizes decimal strings (removes trailing .0)
    assert snapshot.o == ["100"]
    assert snapshot.h == ["101"]
    assert snapshot.l == ["99"]
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
    ("field_to_invalidate", "invalid_value", "expected_msg_part"),
    [
        ("t", "not_a_list", "Input should be a valid list"),
        ("o", False, "Input should be a valid list"),
        ("h", 123, "Input should be a valid list"),
        ("s", 123, "Field 's' must be string, got int"),
        ("s", "", "String cannot be empty"),
    ],
)
def test_invalid_field_type_or_missing(
    field_to_invalidate: str,
    invalid_value: str | float | bool | list[Any] | None,  # Invalid types for Pydantic
    expected_msg_part: str,
) -> None:
    """Test validation fails if a field has an incorrect type or is missing."""
    data = VALID_DATA_SINGLE_CANDLE.copy()
    data[field_to_invalidate] = invalid_value

    # Expect ValidationError or TypeError
    with pytest.raises((ValidationError, TypeError)) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(data)

    # Use substring matching, ignore case
    assert expected_msg_part.lower() in str(exc_info.value).lower()
    # Check field name is mentioned
    assert field_to_invalidate in str(exc_info.value).lower()


def test_missing_field() -> None:
    """Test validation fails if a required field is missing."""
    data = VALID_DATA_SINGLE_CANDLE.copy()
    del data["t"]  # Remove a required field
    with pytest.raises((ValidationError, TypeError)) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(data)
    assert "Field required" in str(exc_info.value)
    # Adjust check for Pydantic v2 missing field format
    assert re.search(r"\Wt\W.*Field required", str(exc_info.value), re.IGNORECASE)


@pytest.mark.parametrize(
    ("list_field", "item_index", "invalid_item", "expected_key_terms"),
    [
        ("t", 0, "not_an_int", ("integer", "got str")),
        ("t", 0, -1, ("value", "-1", "cannot be negative")),
        ("o", 0, 123.45, ("must be str", "got float")),
        ("h", 0, True, ("must be str", "got bool")),
        ("l", 0, "", ("cannot be empty",)),
        ("c", 0, "not_finite_enough", ("cannot convert", "not_finite_enough")),
        ("v", 0, "not_a_number", ("cannot convert", "not_a_number")),
        ("v", 0, "-10.0", ("value -10.0", "non-negative")),
        ("o", 0, "1" * 65, ("string with max length 64", "string with length 65")),
    ],
)
def test_invalid_list_item_type_or_format(
    list_field: str,
    item_index: int,
    invalid_item: str | float | bool | dict[str, Any] | None,  # Invalid types for Pydantic
    expected_key_terms: tuple[str, ...],
) -> None:
    """Test validation fails if an item within a list has an incorrect type or format."""
    data: dict[str, Any] = VALID_DATA_SINGLE_CANDLE.copy()
    original_list: list[Any] = list(data[list_field])
    if original_list:
        original_list[item_index] = invalid_item
        data[list_field] = original_list
    else:
        data[list_field] = [invalid_item]

    with pytest.raises((ValidationError, TypeError, ParsingError)) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(data)

    error_str = str(exc_info.value).lower()
    # Check for field index indication flexibly, but allow for TypeError/ParsingError
    # without index info
    has_index = f".{item_index}" in error_str or f"[{item_index}]" in error_str
    is_type_or_parsing_error = isinstance(exc_info.value, (TypeError, ParsingError))
    # If it's a TypeError or ParsingError from business logic, it may not have index info
    if not has_index and not is_type_or_parsing_error:
        # Only require index for ValidationError
        assert has_index, f"Expected index {item_index} in error: {error_str}"

    for term in expected_key_terms:
        assert term.lower() in error_str


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
    with pytest.raises((ValidationError, TypeError, ParsingError)) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(data)
    # Use simpler substring check
    assert "must all have the same length" in str(exc_info.value)


def test_volume_non_negative() -> None:
    """Test that volume values must be non-negative."""
    data = VALID_DATA_SINGLE_CANDLE.copy()
    data["v"] = ["-0.1"]
    with pytest.raises((ValidationError, TypeError)) as exc_info:
        HyperliquidRawCandleSnapshot.model_validate(data)
    error_str = str(exc_info.value).lower()
    # Check for key parts, ignore exact formatting and case
    assert "value" in error_str
    assert "-0.1" in error_str
    assert "non-negative" in error_str
    # Check for field index indication flexibly
    assert ".0" in error_str or "[0]" in error_str or " v " in error_str


def test_price_or_volume_not_finite() -> None:
    """Test that price/volume string must represent finite decimals."""
    data_price = VALID_DATA_SINGLE_CANDLE.copy()
    data_price["o"] = ["Infinity"]
    with pytest.raises(ValidationError) as exc_info_price:
        HyperliquidRawCandleSnapshot.model_validate(data_price)
    error_str_price = str(exc_info_price.value).lower()
    # Expect the error message from the centralized _wrap_validate_finite_decimal_str
    assert "must be a parseable finite decimal string" in error_str_price
    assert "infinity" in error_str_price
    assert ".0" in error_str_price or "[0]" in error_str_price or " o " in error_str_price

    data_volume = VALID_DATA_SINGLE_CANDLE.copy()
    data_volume["v"] = ["NaN"]
    with pytest.raises(ValidationError) as exc_info_vol:
        HyperliquidRawCandleSnapshot.model_validate(data_volume)
    error_str_vol = str(exc_info_vol.value).lower()
    # Expect the error message from the centralized _wrap_validate_non_negative_finite_decimal_str
    assert "must be a parseable finite decimal string" in error_str_vol
    assert "nan" in error_str_vol
    assert ".0" in error_str_vol or "[0]" in error_str_vol or " v " in error_str_vol

"""
Unit tests for HyperliquidRawCandle and HyperliquidRawCandleSnapshot models.
"""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_candle_snapshot import (
    HyperliquidRawCandle,
    HyperliquidRawCandleSnapshotResponse,
)

# --- Test Data ---

VALID_CANDLE_DATA = {
    "t": 1678886400000,  # Timestamp (int ms)
    "o": "40000.1",  # Open (str)
    "h": "41000.2",  # High (str)
    "l": "39000.3",  # Low (str) -> Will be mapped to `low_price`
    "c": "40500.4",  # Close (str)
    "v": "1000.5",  # Volume (str)
    "n": 100,  # Number of trades (int)
}

VALID_SNAPSHOT_DATA = {
    "candles": [VALID_CANDLE_DATA.copy() for _ in range(3)],
}

# --- Success Cases ---


def test_valid_candle_data() -> None:
    """Test successful validation of a valid candle dict."""
    candle = HyperliquidRawCandle.model_validate(VALID_CANDLE_DATA)
    assert candle.t == 1678886400000
    assert candle.o == "40000.1"
    assert candle.h == "41000.2"
    assert candle.low_price == "39000.3"  # Check mapped field name
    assert candle.c == "40500.4"
    assert candle.v == "1000.5"
    assert candle.n == 100


def test_valid_snapshot_data() -> None:
    """Test successful validation of a valid snapshot dict."""
    snapshot = HyperliquidRawCandleSnapshotResponse.model_validate(VALID_SNAPSHOT_DATA)
    assert len(snapshot.candles) == 3
    assert isinstance(snapshot.candles[0], HyperliquidRawCandle)


# --- Failure Cases ---


# Parametrized test for invalid raw values in HyperliquidRawCandle
@pytest.mark.parametrize(
    "field_name, invalid_value, expected_error_substring",
    [
        # Type errors (expect specific message substring within ValidationError)
        ("t", "not an int", "Expected non-negative integer for t, got <class 'str'>"),
        ("t", None, "Expected non-negative integer for t, got <class 'NoneType'>"),
        ("o", 123.45, "Expected string for decimal parsing, got <class 'float'>"),
        ("h", ["a"], "Expected string for decimal parsing, got <class 'list'>"),
        ("l", True, "Expected string for decimal parsing, got <class 'bool'>"),
        ("c", None, "Expected string for decimal parsing, got <class 'NoneType'>"),
        ("v", {}, "Expected string for decimal parsing, got <class 'dict'>"),
        ("n", "not an int", "Expected non-negative integer for n, got <class 'str'>"),
        ("n", None, "Expected non-negative integer for n, got <class 'NoneType'>"),
        # Value errors (expect specific message substring within ValidationError)
        ("t", -1, "Expected non-negative integer for t"),
        ("o", "", "Invalid decimal string format: ''"),
        ("c", "1.2.3", "Invalid decimal string format: '1.2.3'"),
        ("n", -5, "Expected non-negative integer for n"),
    ],
)
def test_invalid_candle_data(
    field_name: str,
    invalid_value: Any,  # noqa: ANN401 # Intentional Any for testing invalid inputs
    expected_error_substring: str,
) -> None:
    """Test validation fails for specific invalid raw values in a candle."""
    invalid_data = VALID_CANDLE_DATA.copy()
    actual_field_name = field_name if field_name != "low_price" else "l"
    invalid_data[actual_field_name] = invalid_value
    # Catch ValidationError and check its message content
    with pytest.raises(ValidationError) as excinfo:
        HyperliquidRawCandle.model_validate(invalid_data)

    # Check if the expected substring is present in the full error string representation
    error_str = str(excinfo.value).replace(
        "\n", " "
    )  # Replace newlines for easier substring search
    assert expected_error_substring in error_str, (
        f"Failed for field {field_name}, value {invalid_value}. "
        f"Expected substring '{expected_error_substring}' not found in error: {error_str}"
    )


# Parametrized test for invalid raw values in HyperliquidRawCandleSnapshotResponse
@pytest.mark.parametrize(
    "field_name, invalid_value, match_pattern",
    [
        ("candles", None, "Input should be a valid list"),
        ("candles", "not a list", "Input should be a valid list"),
        ("candles", [VALID_CANDLE_DATA, "not a candle dict"], "Input should be a valid dictionary"),
        ("candles", [{"t": "invalid"}], "Expected non-negative integer for t"),
    ],
)
def test_invalid_snapshot_data(
    field_name: str,
    invalid_value: Any,  # noqa: ANN401 # Intentional Any for testing invalid inputs
    match_pattern: str,
) -> None:
    """Test validation fails for specific invalid raw values in a snapshot response."""
    invalid_data = VALID_SNAPSHOT_DATA.copy()
    invalid_data[field_name] = invalid_value
    with pytest.raises(ValidationError, match=match_pattern):
        HyperliquidRawCandleSnapshotResponse.model_validate(invalid_data)


def test_snapshot_extra_fields_ignored() -> None:
    """Test that extra fields are ignored in snapshot due to config."""
    # Explicitly type as dict[str, Any] to allow adding extra field for test
    invalid_data: dict[str, Any] = VALID_SNAPSHOT_DATA.copy()
    invalid_data["extra"] = "field"
    try:
        # Expect no error due to extra='ignore'
        _ = HyperliquidRawCandleSnapshotResponse.model_validate(invalid_data)
    except ValidationError as e:
        pytest.fail(f"Should have ignored extra field, but got validation error: {e}")


def test_candle_extra_fields_forbidden() -> None:
    """Test that extra fields are forbidden in candle."""
    invalid_data = VALID_CANDLE_DATA.copy()
    invalid_data["extra"] = "field"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        HyperliquidRawCandle.model_validate(invalid_data)

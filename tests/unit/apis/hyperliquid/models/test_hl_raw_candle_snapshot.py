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
    # Catch TypeError or ValueError directly as mode='before' validators don't wrap them
    with pytest.raises((TypeError, ValueError)) as excinfo:
        HyperliquidRawCandle.model_validate(invalid_data)

    # Check if the expected substring is present in the direct exception message
    actual_msg = str(excinfo.value)
    assert expected_error_substring in actual_msg, (
        f"Failed for field {field_name}, value {invalid_value}. "
        f"Expected substring '{expected_error_substring}' not found in "
        f"error message: '{actual_msg}'"
    )


# Parametrized test for invalid raw values in HyperliquidRawCandleSnapshotResponse
@pytest.mark.parametrize(
    "field_name, invalid_value, expected_loc, expected_msg_substring",
    [
        # Cases where core validation fails (still use simple message checks)
        ("candles", None, ("candles",), "Input should be a valid list"),
        ("candles", "not a list", ("candles",), "Input should be a valid list"),
        # Cases involving nested model validation failures
        (
            "candles",
            [VALID_CANDLE_DATA, "not a candle dict"],
            ("candles", 1),
            "Input should be a valid dictionary or instance of HyperliquidRawCandle",
        ),
        (
            "candles",
            [{"t": "invalid"}],
            ("candles", 0, "t"),
            "Value error, Expected non-negative integer for t",
        ),
    ],
)
def test_invalid_snapshot_data(
    field_name: str,
    invalid_value: Any,  # noqa: ANN401 # Intentional Any for testing invalid inputs
    expected_loc: tuple[str | int, ...],
    expected_msg_substring: str,
) -> None:
    """Test validation fails for specific invalid raw values in a snapshot response."""
    invalid_data = VALID_SNAPSHOT_DATA.copy()
    invalid_data[field_name] = invalid_value
    # Catch ValidationError and inspect its structured errors
    with pytest.raises(ValidationError) as excinfo:
        HyperliquidRawCandleSnapshotResponse.model_validate(invalid_data)

    # Find the specific error by location and check its message
    errors = excinfo.value.errors()
    found_error = None
    for error in errors:
        if error["loc"] == expected_loc:
            found_error = error
            break

    assert found_error is not None, (
        f"Expected error at location {expected_loc} not found. Errors: {errors}"
    )

    actual_msg = found_error["msg"]
    assert expected_msg_substring in actual_msg, (
        f"Failed for field {field_name}, loc {expected_loc}, value {invalid_value}. "
        f"Expected substring '{expected_msg_substring}' not found in error msg: '{actual_msg}'"
    )


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

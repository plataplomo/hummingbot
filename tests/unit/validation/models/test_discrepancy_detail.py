"""Unit tests for discrepancy detail models.

Tests validation and functionality of DiscrepancyDetail and HistoricalDiscrepancyRecord models.
"""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from pydantic import TypeAdapter, ValidationError

from cyberdelta.validation.models.discrepancy_detail import (
    DiscrepancyDetail,
    HistoricalDiscrepancyRecord,
)


# Helper function for testing validate_assignment
def _get_value_for_assignment_test() -> bool:
    """Test helper function for validate_assignment testing.

    Typed to return bool, but will be mocked to return an invalid type (str)
    at runtime to test Pydantic's validate_assignment.
    """
    # This actual return value doesn't impact the test when mocked,
    # but it ensures the function itself is statically correct.
    return True


# Tests for DiscrepancyDetail


def test_discrepancy_detail_happy_path() -> None:
    """Test successful creation of DiscrepancyDetail with all fields."""
    detail = DiscrepancyDetail(
        symbol="BTC-PERP",
        discrepancy_type="size",
        exchange_value="1.0",
        local_value="0.9",
        details="Size mismatch found during reconciliation.",
    )
    assert detail.symbol == "BTC-PERP"
    assert detail.discrepancy_type == "size"
    assert detail.exchange_value == "1.0"
    assert detail.local_value == "0.9"
    assert detail.details == "Size mismatch found during reconciliation."


def test_discrepancy_detail_minimal_required_fields() -> None:
    """Test successful creation with only required fields (optional fields default to None)."""
    detail = DiscrepancyDetail(
        symbol="ETH-PERP",
        discrepancy_type="entry_price",
    )
    assert detail.symbol == "ETH-PERP"
    assert detail.discrepancy_type == "entry_price"
    assert detail.exchange_value is None
    assert detail.local_value is None
    assert detail.details is None


def test_discrepancy_detail_optional_fields_provided() -> None:
    """Test successful creation with some optional fields provided."""
    detail = DiscrepancyDetail(
        symbol="SOL-PERP",
        discrepancy_type="mark_price",
        exchange_value="150.00",
    )
    assert detail.symbol == "SOL-PERP"
    assert detail.discrepancy_type == "mark_price"
    assert detail.exchange_value == "150.00"
    assert detail.local_value is None
    assert detail.details is None


@pytest.mark.parametrize("missing_field", ["symbol", "discrepancy_type"])
def test_discrepancy_detail_missing_required_field(missing_field: str) -> None:
    """Test ValidationError when a required field is missing."""
    data_dict = {
        "symbol": "BTC-PERP",
        "discrepancy_type": "size",
        "exchange_value": "1.0",
        "local_value": "0.9",
    }
    data_copy = data_dict.copy()
    del data_copy[missing_field]
    with pytest.raises(ValidationError) as exc_info:
        TypeAdapter(DiscrepancyDetail).validate_python(data_copy)
    assert missing_field in str(exc_info.value)


def test_discrepancy_detail_invalid_discrepancy_type() -> None:
    """Test ValidationError for an invalid discrepancy_type."""
    data_dict = {
        "symbol": "BTC-PERP",
        "discrepancy_type": "invalid_type",  # This value is invalid for the Literal
    }
    with pytest.raises(ValidationError) as exc_info:
        TypeAdapter(DiscrepancyDetail).validate_python(data_dict)

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["type"] == "literal_error"
    assert errors[0]["loc"] == ("discrepancy_type",)
    assert errors[0]["input"] == "invalid_type"
    # Ensure the message indicates it's about the literal/enum type
    # assert (
    #     "Input should be" in str(exc_info.value) and "[type=literal_error]" in str(exc_info.value)
    # )


def test_discrepancy_detail_frozen() -> None:
    """Test that DiscrepancyDetail is frozen (immutable)."""
    detail = DiscrepancyDetail(
        symbol="BTC-PERP",
        discrepancy_type="size",
    )
    with pytest.raises(ValidationError) as exc_info:
        # Direct assignment to a frozen model should raise ValidationError
        # Pydantic v2 raises pydantic_core.ValidationError which includes info about frozen fields.
        # detail.symbol = "ETH-PERP" # This will be caught by mypy if not careful
        detail.symbol = "ETH-PERP"  # Use setattr to test runtime frozen validation
    assert "Instance is frozen" in str(exc_info.value) or "frozen_field" in str(exc_info.value)


def test_discrepancy_detail_extra_fields_forbidden() -> None:
    """Test ValidationError when extra fields are provided (extra='forbid')."""
    data_dict = {
        "symbol": "BTC-PERP",
        "discrepancy_type": "size",
        "unexpected_field": "some_value",
    }
    with pytest.raises(ValidationError) as exc_info:
        TypeAdapter(DiscrepancyDetail).validate_python(data_dict)
    assert "unexpected_field" in str(exc_info.value)
    assert (
        "Extra inputs are not Mypy itemized" in str(exc_info.value)
        or "Extra inputs are not permitted" in str(exc_info.value)  # Pydantic v2
    )


# Tests for HistoricalDiscrepancyRecord


def test_historical_record_happy_path() -> None:
    """Test successful creation of HistoricalDiscrepancyRecord."""
    discrepancy = DiscrepancyDetail(symbol="BTC-PERP", discrepancy_type="size")
    now = datetime.now(UTC)
    record = HistoricalDiscrepancyRecord(
        detail=discrepancy,
        exchange_id="mock_exchange_1",
        recorded_at=now,
        is_corrected=True,
    )
    assert record.detail == discrepancy
    assert record.exchange_id == "mock_exchange_1"
    assert record.recorded_at == now
    assert record.is_corrected is True


def test_historical_record_default_is_corrected() -> None:
    """Test that is_corrected defaults to False."""
    discrepancy = DiscrepancyDetail(symbol="ETH-PERP", discrepancy_type="entry_price")
    now = datetime.now(UTC)
    data_to_validate = {
        "detail": discrepancy,
        "exchange_id": "mock_exchange_2",
        "recorded_at": now,
        # is_corrected is intentionally omitted to test Pydantic's default value mechanism
    }
    record = TypeAdapter(HistoricalDiscrepancyRecord).validate_python(data_to_validate)
    assert record.is_corrected is False


@pytest.mark.parametrize("missing_field", ["detail", "exchange_id", "recorded_at"])
def test_historical_record_missing_required_field(missing_field: str) -> None:
    """Test ValidationError when a required field is missing."""
    discrepancy = DiscrepancyDetail(symbol="SOL-PERP", discrepancy_type="mark_price")
    now = datetime.now(UTC)
    data_dict = {
        "detail": discrepancy,
        "exchange_id": "mock_exchange_3",
        "recorded_at": now,
        "is_corrected": False,
    }
    data_copy = data_dict.copy()
    if missing_field in data_copy:
        del data_copy[missing_field]
    else:
        pytest.fail(f"Test setup error: missing_field '{missing_field}' not in data keys.")

    with pytest.raises(ValidationError) as exc_info:
        TypeAdapter(HistoricalDiscrepancyRecord).validate_python(data_copy)
    assert missing_field in str(exc_info.value)


def test_historical_record_invalid_detail_type() -> None:
    """Test ValidationError if detail is not a DiscrepancyDetail instance."""
    now = datetime.now(UTC)
    data_dict = {
        "detail": {
            "symbol": "FAKE",
            "discrepancy_type": "SUPER_INVALID_TYPE_NOW",
        },  # This will make DiscrepancyDetail validation fail
        "exchange_id": "mock_exchange_4",
        "recorded_at": now,
        "is_corrected": False,
    }
    with pytest.raises(ValidationError) as exc_info:
        TypeAdapter(HistoricalDiscrepancyRecord).validate_python(data_dict)

    errors = exc_info.value.errors()
    assert len(errors) == 1
    # Check for the nested validation error from DiscrepancyDetail
    assert errors[0]["type"] == "literal_error"
    assert errors[0]["loc"] == ("detail", "discrepancy_type")
    assert errors[0]["input"] == "SUPER_INVALID_TYPE_NOW"
    # assert "detail" in str(exc_info.value)
    # # Check for the nested validation error from DiscrepancyDetail
    # assert "Input should be" in str(exc_info.value)
    # assert "SUPER_INVALID_TYPE_NOW" in str(exc_info.value)
    # assert "[type=literal_error]" in str(exc_info.value)


def test_historical_record_validate_assignment_for_is_corrected() -> None:
    """Test that is_corrected can be updated and validates new value."""
    discrepancy = DiscrepancyDetail(symbol="ADA-PERP", discrepancy_type="unrealized_pnl")
    now = datetime.now(UTC)
    initial_data = {
        "detail": discrepancy,
        "exchange_id": "mock_exchange_5",
        "recorded_at": now,
        # is_corrected is intentionally omitted to test Pydantic's default, then assignment
    }
    record = TypeAdapter(HistoricalDiscrepancyRecord).validate_python(initial_data)
    assert record.is_corrected is False

    record.is_corrected = True
    assert record.is_corrected is True

    # Test invalid assignment.
    # Patch helper to return str, Mypy sees `bool = func() -> bool` (statically fine).
    # Pydantic's validate_assignment should catch the runtime str assignment to bool field.
    with patch(f"{__name__}._get_value_for_assignment_test", return_value="not_a_bool"):
        with pytest.raises(ValidationError) as exc_info:
            record.is_corrected = _get_value_for_assignment_test()

    assert "is_corrected" in str(exc_info.value)
    assert "Input should be a valid boolean" in str(exc_info.value)


def test_historical_record_extra_fields_forbidden() -> None:
    """Test ValidationError when extra fields are provided (extra='forbid')."""
    discrepancy = DiscrepancyDetail(symbol="DOT-PERP", discrepancy_type="api_parsing_error")
    now = datetime.now(UTC)
    data_dict = {
        "detail": discrepancy,
        "exchange_id": "mock_exchange_6",
        "recorded_at": now,
        "is_corrected": False,  # Explicitly providing all valid fields first
        "unexpected_field": "some_value",
    }
    with pytest.raises(ValidationError) as exc_info:
        TypeAdapter(HistoricalDiscrepancyRecord).validate_python(data_dict)
    assert "unexpected_field" in str(exc_info.value)
    assert (
        "Extra inputs are not Mypy itemized" in str(exc_info.value)
        or "Extra inputs are not permitted" in str(exc_info.value)  # Pydantic v2
    )

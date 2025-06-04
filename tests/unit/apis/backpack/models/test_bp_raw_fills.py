"""Unit tests for Backpack Raw Fill model."""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFill


# --- Fixtures ---
@pytest.fixture
def valid_fill_data() -> dict[str, Any]:
    """Provides a dictionary with valid raw fill data."""
    return {
        "fee": "0.1",
        "feeSymbol": "USDC",
        "isMaker": True,
        "orderId": "1234567890",
        "price": "3000.50",
        "quantity": "0.01",
        "side": "Bid",
        "symbol": "SOL_USDC",
        "timestamp": "2024-01-15T10:30:00.123456Z",
        "tradeId": 98765,
        "clientId": "my-client-id-optional",
    }


# --- Success Cases ---
def test_backpack_raw_fill_valid(valid_fill_data: dict[str, Any]) -> None:
    """Test successful validation with completely valid data."""
    fill = BackpackRawFill.model_validate(valid_fill_data)

    assert fill.fee == "0.1"
    assert fill.fee_symbol == "USDC"
    assert fill.is_maker is True
    assert fill.order_id == "1234567890"
    assert fill.price == "3000.50"
    assert fill.quantity == "0.01"
    assert fill.side == "Bid"
    assert fill.symbol == "SOL_USDC"
    assert fill.timestamp == "2024-01-15T10:30:00.123456Z"
    assert fill.trade_id == 98765
    assert fill.client_id == "my-client-id-optional"
    assert fill.model_config.get("extra") == "forbid"
    assert fill.model_config.get("frozen") is True


def test_backpack_raw_fill_optional_client_id_none(valid_fill_data: dict[str, Any]) -> None:
    """Test validation succeeds when optional clientId is None."""
    valid_fill_data["clientId"] = None
    fill = BackpackRawFill.model_validate(valid_fill_data)
    assert fill.client_id is None


def test_backpack_raw_fill_optional_client_id_missing(valid_fill_data: dict[str, Any]) -> None:
    """Test validation succeeds when optional clientId is missing."""
    del valid_fill_data["clientId"]
    fill = BackpackRawFill.model_validate(valid_fill_data)
    assert fill.client_id is None  # Default is None


# --- Failure Cases: Type Errors ---
@pytest.mark.parametrize(
    "field, invalid_value",
    [
        ("fee", 0.1),  # Should be string
        ("feeSymbol", 123),
        ("isMaker", "true"),
        ("orderId", 1234567890),
        ("price", 3000.50),
        ("quantity", 0.01),
        ("side", ["Bid"]),
        ("symbol", {"s": "SOL_USDC"}),
        ("timestamp", 1673788200123),  # Should be string
        ("clientId", 123),
    ],
)
def test_backpack_raw_fill_invalid_types(
    valid_fill_data: dict[str, Any],
    field: str,
    invalid_value: object,  # Changed from Any to object
) -> None:
    """Test ValidationError is raised for incorrect field types."""
    valid_fill_data[field] = invalid_value
    with pytest.raises(ValidationError) as exc_info:
        BackpackRawFill.model_validate(valid_fill_data)

    # Determine expected field name in error message (Pydantic normalizes to snake_case)
    expected_error_field = field
    if field == "feeSymbol":
        expected_error_field = "fee_symbol"
    elif field == "isMaker":
        expected_error_field = "is_maker"
    elif field == "orderId":
        expected_error_field = "order_id"
    elif field == "tradeId":
        expected_error_field = "trade_id"
    elif field == "clientId":
        expected_error_field = "client_id"
    # Add other camelCase to snake_case mappings if needed for other fields

    # Check that the field name is mentioned in the error message for type errors
    assert (
        f"'{expected_error_field}'" in str(exc_info.value)
        or f"{expected_error_field}:" in str(exc_info.value)
        or f"{expected_error_field}\\n" in str(exc_info.value)
    )


# --- Failure Cases: Format/Constraint Errors ---
@pytest.mark.parametrize(
    "field, invalid_value, expected_msg_part",
    [
        ("fee", "", "String cannot be empty"),
        ("fee", "not_a_number", "Cannot convert 'not_a_number' to Decimal"),
        ("fee", "NaN", "must be a finite decimal"),
        ("fee", "inf", "must be a finite decimal"),
        ("feeSymbol", "", "String cannot be empty"),
        ("feeSymbol", "A" * 33, "String value too long (max 32 chars)"),
        ("orderId", "", "String cannot be empty"),
        ("orderId", "B" * 129, "String value too long (max 128 chars)"),
        ("side", "Buy", ("Invalid value 'Buy'", "Expected one of")),
        ("timestamp", "not-a-valid-iso-date", "timestamp: Cannot parse ISO datetime string"),
        ("timestamp", "", "timestamp: String cannot be empty"),
        ("tradeId", -1, "Value error, trade_id: Must be >= 0, got -1"),
        (
            "clientId",
            "",
            "Value error, clientId cannot be an empty or whitespace-only string if provided.",
        ),
        ("clientId", "C" * 129, "String value too long (max 128 chars)"),
    ],
)
def test_backpack_raw_fill_invalid_formats_and_values(
    valid_fill_data: dict[str, Any],
    field: str,
    invalid_value: object,  # Changed from Any to object
    expected_msg_part: str | tuple[str, str],
) -> None:
    """Test ValidationError for format/value/constraint violations."""
    valid_fill_data[field] = invalid_value
    with pytest.raises(ValidationError) as exc_info:
        BackpackRawFill.model_validate(valid_fill_data)
    # Adjust assertion to handle tuple of expected parts for robust checking
    if isinstance(expected_msg_part, tuple):
        for part in expected_msg_part:
            assert part in str(exc_info.value), (
                f"Expected part '{part}' not found in error for "
                f"{field}={invalid_value!r}: {exc_info.value}"
            )
    else:
        assert expected_msg_part in str(exc_info.value), (
            f"Expected '{expected_msg_part}' not found in error for "
            f"{field}={invalid_value!r}: {exc_info.value}"
        )


# --- Failure Cases: Missing Required Fields ---
@pytest.mark.parametrize(
    "field_to_remove",
    [
        "fee",
        "feeSymbol",
        "isMaker",
        "orderId",
        "price",
        "quantity",
        "side",
        "symbol",
        "timestamp",
        "tradeId",
    ],
)
def test_backpack_raw_fill_missing_required(
    valid_fill_data: dict[str, Any],
    field_to_remove: str,
) -> None:
    """Test ValidationError when required fields are missing."""
    del valid_fill_data[field_to_remove]
    with pytest.raises(ValidationError) as exc_info:
        BackpackRawFill.model_validate(valid_fill_data)
    assert f"{field_to_remove}\n  Field required" in str(exc_info.value)


# --- Failure Cases: Extra Fields ---
def test_backpack_raw_fill_extra_field(valid_fill_data: dict[str, Any]) -> None:
    """Test ValidationError when extra fields are provided (extra='forbid')."""
    valid_fill_data["extraField"] = "should_not_be_here"
    with pytest.raises(ValidationError) as exc_info:
        BackpackRawFill.model_validate(valid_fill_data)
    assert "Extra inputs are not permitted" in str(exc_info.value)

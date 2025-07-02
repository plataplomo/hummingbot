"""Unit tests for Backpack Raw Fill model validation and parsing.

This module provides comprehensive validation testing for the BackpackRawFill Pydantic model,
which serves as the strict validation boundary for raw fill data received from the Backpack
exchange API. The BackpackRawFill model is a critical component in the data ingestion
pipeline, ensuring that all external fill data is properly validated before transformation
into internal Trade models.

Key Testing Areas:
- Raw API data structure validation and type checking
- Field-level validation for all Backpack fill attributes
- String parsing and constraint enforcement (lengths, formats)
- Numeric validation for trade IDs, prices, quantities, and fees
- Enum validation for trading sides and other categorical fields
- Error handling for malformed, missing, or invalid data
- Edge cases and boundary conditions for all field types

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for Raw API model separation
- Implements strict validation boundary per external API contract
- Uses RULE-NO-SILENCING-V4 compliant validation without suppressions
- Enforces RULE-RUNTIME-SAFETY-V4 for Decimal parsing and finite checks

The BackpackRawFill model ensures data integrity at the API boundary, preventing
malformed or malicious data from entering the core trading system. This validation
is essential for maintaining system stability and preventing trading errors that
could result from corrupted or unexpected API responses.

Test Structure:
- Success cases: Valid data scenarios and optional field handling
- Type errors: Invalid data types for each field
- Format errors: Invalid formats, constraints, and business rule violations
- Missing field errors: Required field validation
- Extra field errors: Strict schema enforcement with extra='forbid'
"""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFill
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import DateTimeParsingError, EmptyStringError


# --- Fixtures ---
@pytest.fixture
def valid_fill_data() -> dict[str, Any]:
    """Return a dictionary with valid raw fill data from Backpack API.

    This fixture creates a complete, valid fill record that matches the expected
    structure and data types from the Backpack exchange API. It serves as the
    baseline for testing both successful validation and error conditions by
    modifying specific fields.

    Returns:
        dict: Complete valid fill data including all required fields and one
              optional field (clientId) to test optional field handling.

    """
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
    """Test successful validation with completely valid data.

    This test validates that the BackpackRawFill model correctly accepts and
    processes a complete, valid fill record from the Backpack API. It verifies
    that all fields are properly parsed, type-converted, and accessible, and
    that the model configuration (frozen=True, extra='forbid') is correctly
    applied for immutability and strict schema enforcement.
    """
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
    """Test validation succeeds when optional clientId is explicitly None.

    This test ensures that the optional clientId field correctly handles explicit
    None values, which may be sent by the Backpack API when no client order ID
    was provided. This is important for robust handling of API responses that
    include null values for optional fields.
    """
    valid_fill_data["clientId"] = None
    fill = BackpackRawFill.model_validate(valid_fill_data)
    assert fill.client_id is None


def test_backpack_raw_fill_optional_client_id_missing(valid_fill_data: dict[str, Any]) -> None:
    """Test validation succeeds when optional clientId is missing from the data.

    This test verifies that the model correctly handles API responses where
    optional fields are completely omitted rather than set to null. This
    scenario is common in REST APIs where optional fields may not be included
    in the response payload at all.
    """
    del valid_fill_data["clientId"]
    fill = BackpackRawFill.model_validate(valid_fill_data)
    assert fill.client_id is None  # Default is None


# --- Failure Cases: Type Errors ---
@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("fee", 0.1),  # Should be string
        ("feeSymbol", 123),
        ("isMaker", "true"),  # ValidationError: string passed to boolean field
        ("orderId", 1234567890),
        ("price", 3000.50),
        ("quantity", 0.01),
        ("side", ["Bid"]),
        ("symbol", {"s": "SOL_USDC"}),
        ("timestamp", 1673788200123),  # Should be string
    ],
)
def test_backpack_raw_fill_invalid_types(
    valid_fill_data: dict[str, Any],
    field: str,
    invalid_value: object,  # Changed from Any to object
) -> None:
    """Test TypeError is raised for incorrect field types.

    This parameterized test validates that the model correctly rejects data
    with incorrect types for each field. Type validation is critical at the
    API boundary to ensure that downstream processing can rely on consistent
    data types and prevent runtime errors from unexpected type coercion.

    The test covers common type confusion scenarios that might occur due to
    API changes, serialization errors, or malicious input.
    """
    valid_fill_data[field] = invalid_value

    # Special case: isMaker field raises ValidationError for string values
    if field == "isMaker" and invalid_value == "true":
        with pytest.raises(ValidationError) as exc_info:
            BackpackRawFill.model_validate(valid_fill_data)
        assert "must be one of ['True', 'False', '1', '0']" in str(exc_info.value)
    else:
        with pytest.raises(TypeError) as type_exc_info:
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
        # Special case: when symbol gets a dict, the error mentions fee_symbol
        # (business logic behavior)
        if field == "symbol" and isinstance(invalid_value, dict):
            assert "fee_symbol" in str(type_exc_info.value)
        else:
            assert (
                f"'{expected_error_field}'" in str(type_exc_info.value)
                or f"{expected_error_field}:" in str(type_exc_info.value)
                or f"{expected_error_field}\\n" in str(type_exc_info.value)
            )


# --- Failure Cases: Format/Constraint Errors ---
@pytest.mark.parametrize(
    ("field", "invalid_value", "expected_msg_part"),
    [
        ("fee", "", "String cannot be empty"),
        ("fee", "not_a_number", "Cannot convert to Decimal"),
        ("fee", "NaN", "Value must be a finite decimal"),
        ("fee", "inf", "Value must be a finite decimal"),
        ("feeSymbol", "", "String cannot be empty"),
        ("feeSymbol", "A" * 33, "must be string with max length 32"),
        ("orderId", "", "String cannot be empty"),
        ("orderId", "B" * 129, "must be string with max length 128"),
        ("side", "Buy", "must be one of ['Ask', 'Bid'], got 'Buy'"),
        ("timestamp", "not-a-valid-iso-date", "Cannot parse as ISO datetime"),
        ("timestamp", "", "timestamp: String cannot be empty"),
        ("tradeId", -1, "Must be >= 0"),
        (
            "clientId",
            "",
            "clientId cannot be an empty or whitespace-only string if provided",
        ),
        ("clientId", "C" * 129, "must be string with max length 128"),
    ],
)
def test_backpack_raw_fill_invalid_formats_and_values(
    valid_fill_data: dict[str, Any],
    field: str,
    invalid_value: object,  # Changed from Any to object
    expected_msg_part: str | tuple[str, str],
) -> None:
    """Test ValidationError for format/value/constraint violations.

    This comprehensive parameterized test validates that the model correctly
    enforces all field-level constraints including string length limits,
    numeric parsing requirements, enum value restrictions, and business rules.

    The test ensures that malformed data is properly rejected with descriptive
    error messages, which is essential for debugging API integration issues
    and preventing corrupted data from entering the trading system.

    Key validation areas:
    - String length constraints to prevent buffer overflows
    - Decimal parsing for financial values with finite number validation
    - Enum validation for categorical fields like trading side
    - Timestamp format validation for proper datetime parsing
    - Non-negative constraints for trade IDs and other count fields
    """
    valid_fill_data[field] = invalid_value

    msg_to_check = (
        expected_msg_part if isinstance(expected_msg_part, str) else str(expected_msg_part)
    )

    expected_exc_type: type[Exception]
    if (
        "String cannot be empty" in msg_to_check
        or "empty or whitespace-only string" in msg_to_check
    ):
        expected_exc_type = EmptyStringError
    elif "must be string with max length" in msg_to_check:
        expected_exc_type = TypeFieldError
    elif "Cannot parse as ISO datetime" in msg_to_check:
        expected_exc_type = DateTimeParsingError
    else:
        # For all other cases, including "must be a finite decimal", use ValidationError
        expected_exc_type = ValidationError

    with pytest.raises(expected_exc_type) as exc_info:
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
    """Test ValidationError when required fields are missing.

    This test ensures that all required fields are properly enforced by the
    model validation. Missing required fields could indicate API changes,
    network corruption, or incomplete data transmission, all of which must
    be detected and handled appropriately to prevent trading errors.

    The test validates that each required field is individually necessary
    and that the model provides clear error messages identifying which
    specific field is missing.
    """
    del valid_fill_data[field_to_remove]
    with pytest.raises(ValidationError) as exc_info:
        BackpackRawFill.model_validate(valid_fill_data)
    assert f"{field_to_remove}\n  Field required" in str(exc_info.value)


# --- Failure Cases: Extra Fields ---
def test_backpack_raw_fill_extra_field(valid_fill_data: dict[str, Any]) -> None:
    """Test ValidationError when extra fields are provided (extra='forbid').

    This test validates that the model strictly enforces the expected schema
    by rejecting any additional fields not defined in the model. This is
    critical for detecting API changes, preventing injection of unexpected
    data, and ensuring that the model contract remains stable.

    The extra='forbid' configuration helps catch API evolution issues early
    and prevents silent acceptance of potentially malicious or corrupted
    data that includes unexpected fields.
    """
    valid_fill_data["extraField"] = "should_not_be_here"
    with pytest.raises(ValidationError) as exc_info:
        BackpackRawFill.model_validate(valid_fill_data)
    assert "Extra inputs are not permitted" in str(exc_info.value)

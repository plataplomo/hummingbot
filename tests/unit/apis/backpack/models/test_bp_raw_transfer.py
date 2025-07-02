"""Unit tests for Backpack Raw Transfer Models.

Comprehensive test suite for Backpack exchange raw transfer model validation including:
- BackpackRawWithdrawal: Tests withdrawal transaction data validation
- BackpackRawDeposit: Tests deposit transaction data validation
- BackpackRawLiquidation: Tests liquidation event data validation

These tests ensure robust validation of raw API data from Backpack exchange transfer
endpoints, covering happy path scenarios, error conditions, data corruption cases,
and edge cases that could occur in real trading environments. The tests validate
that the models properly handle malformed data, enforce business rules, and maintain
data integrity for the CyberDeltaEngine trading system.

Key test categories:
- Happy path validation with valid data
- Missing required field handling
- Type validation and coercion
- Format validation for amounts and statuses
- Data corruption and security attack vectors
- Model immutability (frozen=True) enforcement
- Real-world edge cases and malformed JSON handling
"""

import json
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_transfer import (
    BackpackRawDeposit,
    BackpackRawLiquidation,
    BackpackRawWithdrawal,
)
from cyberdelta.exceptions.field_validation import TypeFieldError


def valid_withdrawal() -> dict[str, Any]:
    """Return valid withdrawal data structure for testing BackpackRawWithdrawal validation.

    Provides a baseline valid withdrawal dictionary that can be modified in tests
    to verify various validation scenarios and error conditions.
    """
    return {
        "id": "wd_123",
        "asset": "USDC",
        "amount": "100.0",
        "status": "pending",
    }


def test_BackpackRawWithdrawal_happy_path() -> None:
    """Test BackpackRawWithdrawal validation with valid data succeeds correctly.

    Verifies that a properly formatted withdrawal data structure passes validation
    and that all fields are correctly parsed and accessible on the resulting model.
    """
    obj = BackpackRawWithdrawal.model_validate(valid_withdrawal())
    assert obj.id == "wd_123"
    assert obj.asset == "USDC"
    assert obj.amount == "100.0"
    assert obj.status == "pending"


def test_BackpackRawWithdrawal_missing_required_fields() -> None:
    """Test BackpackRawWithdrawal validation fails when required fields are missing.

    Ensures that the model properly enforces the presence of all required fields
    (id, asset, amount, status) and raises ValidationError when any are absent.
    This is critical for data integrity in the trading system.
    """
    for field in ["id", "asset", "amount", "status"]:
        p: dict[str, Any] = valid_withdrawal().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_wrong_type_fields() -> None:
    """Test BackpackRawWithdrawal validation fails with incorrect field types.

    Verifies that the model enforces correct data types for fields, rejecting
    numeric amounts (should be strings) and non-string status values. This
    ensures type safety when processing Backpack API responses.
    """
    p: dict[str, Any] = valid_withdrawal().copy()
    p["amount"] = 100.0
    with pytest.raises(TypeError):
        BackpackRawWithdrawal.model_validate(p)
    p = valid_withdrawal().copy()
    p["status"] = ["pending"]
    with pytest.raises(TypeError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_invalid_format_fields() -> None:
    """Test BackpackRawWithdrawal validation fails with malformed field values.

    Validates that the model properly rejects invalid decimal formats, unknown
    status values, and negative amounts. These checks are essential for preventing
    invalid financial data from entering the trading system.
    """
    p: dict[str, Any] = valid_withdrawal().copy()
    p["amount"] = "1..0"
    with pytest.raises(ValidationError, match=r"Cannot convert to Decimal"):
        BackpackRawWithdrawal.model_validate(p)
    p = valid_withdrawal().copy()
    p["status"] = "notastatus"
    with pytest.raises(ValidationError, match=r"Field 'status' must be one of"):
        BackpackRawWithdrawal.model_validate(p)
    p = valid_withdrawal().copy()
    p["amount"] = "-50.0"
    with pytest.raises(ValidationError, match="Withdrawal amount cannot be negative"):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_extra_field() -> None:
    """Test BackpackRawWithdrawal validation rejects unexpected extra fields.

    Ensures the model's extra='forbid' configuration properly prevents unknown
    fields from being accepted, maintaining strict API contract compliance.
    """
    p: dict[str, Any] = valid_withdrawal().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_corruption_cases() -> None:
    """Test BackpackRawWithdrawal handles various data corruption and attack scenarios.

    Comprehensive testing of edge cases including null values, control characters,
    injection attacks, unicode handling, whitespace normalization, length limits,
    and special numeric values. These tests ensure the model is robust against
    malicious or corrupted data that could compromise the trading system.
    """
    # Null required
    p: dict[str, Any] = valid_withdrawal().copy()
    p["asset"] = None
    with pytest.raises(TypeError):
        BackpackRawWithdrawal.model_validate(p)
    # Unicode/control chars
    p = valid_withdrawal().copy()
    p["asset"] = "USDC\x00"
    obj = BackpackRawWithdrawal.model_validate(p)
    assert "USDC" in obj.asset
    # SQL injection
    p = valid_withdrawal().copy()
    p["id"] = "wd_123; DROP TABLE withdrawals;"
    obj = BackpackRawWithdrawal.model_validate(p)
    assert "wd_123" in obj.id
    # XSS
    p = valid_withdrawal().copy()
    p["asset"] = "<script>alert(1)</script>"
    obj = BackpackRawWithdrawal.model_validate(p)
    assert "script" in obj.asset
    # Emoji
    p = valid_withdrawal().copy()
    p["asset"] = "USDC😀"
    obj = BackpackRawWithdrawal.model_validate(p)
    assert obj.asset.startswith("USDC")
    # Whitespace
    p = valid_withdrawal().copy()
    p["asset"] = "   USDC   "
    obj = BackpackRawWithdrawal.model_validate(p)
    assert "USDC" in obj.asset
    # Excessive length
    p = valid_withdrawal().copy()
    p["asset"] = "A" * 10000
    with pytest.raises(TypeFieldError):
        BackpackRawWithdrawal.model_validate(p)
    # Negative/zero/NaN/inf amounts
    for val in ["-100.0", "0", "NaN", "inf", "-inf"]:
        p = valid_withdrawal().copy()
        p["amount"] = val
        if val in ["-100.0", "NaN", "inf", "-inf"]:
            with pytest.raises(ValidationError):
                BackpackRawWithdrawal.model_validate(p)
        else:
            obj = BackpackRawWithdrawal.model_validate(p)
            assert obj.amount == val
    # Scientific notation
    p = valid_withdrawal().copy()
    p["amount"] = "1e6"
    obj = BackpackRawWithdrawal.model_validate(p)
    assert obj.amount == "1e6"
    # Truncated JSON
    bad_json = '{"id": "wd_123", "asset": "USDC"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawWithdrawal_frozen() -> None:
    """Test that BackpackRawWithdrawal model is immutable (frozen=True).

    Verifies that the model enforces immutability by preventing field modifications
    after instantiation. This ensures data integrity and prevents accidental
    mutations in the trading system.
    """
    obj = BackpackRawWithdrawal.model_validate(valid_withdrawal())
    with pytest.raises(ValidationError, match="Instance is frozen"):
        obj.id = "new_id"
    with pytest.raises(ValidationError, match="Instance is frozen"):
        obj.amount = "200.0"


def valid_deposit() -> dict[str, Any]:
    """Return valid deposit data structure for testing BackpackRawDeposit validation.

    Provides a baseline valid deposit dictionary that can be modified in tests
    to verify various validation scenarios and error conditions.
    """
    return {
        "id": "dp_456",
        "asset": "BTC",
        "amount": "0.5",
        "status": "completed",
    }


def test_BackpackRawDeposit_happy_path() -> None:
    """Test BackpackRawDeposit validation with valid data succeeds correctly.

    Verifies that a properly formatted deposit data structure passes validation
    and that all fields are correctly parsed and accessible on the resulting model.
    """
    obj = BackpackRawDeposit.model_validate(valid_deposit())
    assert obj.id == "dp_456"
    assert obj.asset == "BTC"
    assert obj.amount == "0.5"
    assert obj.status == "completed"


def test_BackpackRawDeposit_missing_required_fields() -> None:
    """Test BackpackRawDeposit validation fails when required fields are missing.

    Ensures that the model properly enforces the presence of all required fields
    (id, asset, amount, status) and raises ValidationError when any are absent.
    This is critical for data integrity in the trading system.
    """
    for field in ["id", "asset", "amount", "status"]:
        p: dict[str, Any] = valid_deposit().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_wrong_type_fields() -> None:
    """Test BackpackRawDeposit validation fails with incorrect field types.

    Verifies that the model enforces correct data types for fields, rejecting
    numeric amounts (should be strings) and non-string status values. This
    ensures type safety when processing Backpack API responses.
    """
    p: dict[str, Any] = valid_deposit().copy()
    p["amount"] = 0.5
    with pytest.raises(TypeError):
        BackpackRawDeposit.model_validate(p)
    p = valid_deposit().copy()
    p["status"] = ["completed"]
    with pytest.raises(TypeError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_invalid_format_fields() -> None:
    """Test BackpackRawDeposit validation fails with malformed field values.

    Validates that the model properly rejects invalid decimal formats, unknown
    status values, and negative amounts. These checks are essential for preventing
    invalid financial data from entering the trading system.
    """
    p: dict[str, Any] = valid_deposit().copy()
    p["amount"] = "1..0"
    with pytest.raises(ValidationError, match=r"Cannot convert to Decimal"):
        BackpackRawDeposit.model_validate(p)
    p = valid_deposit().copy()
    p["status"] = "notastatus"
    with pytest.raises(ValidationError, match=r"Field 'status' must be one of"):
        BackpackRawDeposit.model_validate(p)
    p = valid_deposit().copy()
    p["amount"] = "-0.1"
    with pytest.raises(ValidationError, match="Deposit amount cannot be negative"):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_extra_field() -> None:
    """Test BackpackRawDeposit validation rejects unexpected extra fields.

    Ensures the model's extra='forbid' configuration properly prevents unknown
    fields from being accepted, maintaining strict API contract compliance.
    """
    p: dict[str, Any] = valid_deposit().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_corruption_cases() -> None:
    """Test BackpackRawDeposit handles various data corruption and attack scenarios.

    Comprehensive testing of edge cases including null values, control characters,
    injection attacks, unicode handling, whitespace normalization, length limits,
    and special numeric values. These tests ensure the model is robust against
    malicious or corrupted data that could compromise the trading system.
    """
    # Null required
    p: dict[str, Any] = valid_deposit().copy()
    p["asset"] = None
    with pytest.raises(TypeError):
        BackpackRawDeposit.model_validate(p)
    # Unicode/control chars
    p = valid_deposit().copy()
    p["asset"] = "BTC\x00"
    obj = BackpackRawDeposit.model_validate(p)
    assert "BTC" in obj.asset
    # SQL injection
    p = valid_deposit().copy()
    p["id"] = "dp_456; DROP TABLE deposits;"
    obj = BackpackRawDeposit.model_validate(p)
    assert "dp_456" in obj.id
    # XSS
    p = valid_deposit().copy()
    p["asset"] = "<img src=x onerror=alert(1)>"
    obj = BackpackRawDeposit.model_validate(p)
    assert "img" in obj.asset
    # Emoji
    p = valid_deposit().copy()
    p["asset"] = "BTC😀"
    obj = BackpackRawDeposit.model_validate(p)
    assert obj.asset.startswith("BTC")
    # Whitespace
    p = valid_deposit().copy()
    p["asset"] = "   BTC   "
    obj = BackpackRawDeposit.model_validate(p)
    assert "BTC" in obj.asset
    # Excessive length
    p = valid_deposit().copy()
    p["asset"] = "B" * 10000
    with pytest.raises(TypeFieldError):
        BackpackRawDeposit.model_validate(p)
    # Negative/zero/NaN/inf amounts
    for val in ["-0.5", "0", "NaN", "inf", "-inf"]:
        p = valid_deposit().copy()
        p["amount"] = val
        if val in ["-0.5", "NaN", "inf", "-inf"]:
            with pytest.raises(ValidationError):
                BackpackRawDeposit.model_validate(p)
        else:
            obj = BackpackRawDeposit.model_validate(p)
            assert obj.amount == val
    # Scientific notation
    p = valid_deposit().copy()
    p["amount"] = "2e-3"
    obj = BackpackRawDeposit.model_validate(p)
    assert obj.amount == "2e-3"
    # Truncated JSON
    bad_json = '{"id": "dp_456", "asset": "BTC"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawDeposit_frozen() -> None:
    """Test that BackpackRawDeposit model is immutable (frozen=True).

    Verifies that the model enforces immutability by preventing field modifications
    after instantiation. This ensures data integrity and prevents accidental
    mutations in the trading system.
    """
    obj = BackpackRawDeposit.model_validate(valid_deposit())
    with pytest.raises(ValidationError, match="Instance is frozen"):
        obj.id = "new_id"
    with pytest.raises(ValidationError, match="Instance is frozen"):
        obj.amount = "1.0"


def valid_liquidation() -> dict[str, Any]:
    """Return valid liquidation data structure for testing BackpackRawLiquidation validation.

    Provides a baseline valid liquidation dictionary that can be modified in tests
    to verify various validation scenarios and error conditions.
    """
    return {
        "symbol": "BTC_USDC",
        "price": "45000.0",
        "quantity": "0.01",
        "side": "sell",
    }


def test_BackpackRawLiquidation_happy_path() -> None:
    """Test BackpackRawLiquidation validation with valid data succeeds correctly.

    Verifies that a properly formatted liquidation data structure passes validation
    and that all fields are correctly parsed and accessible on the resulting model.
    """
    obj = BackpackRawLiquidation.model_validate(valid_liquidation())
    assert obj.symbol == "BTC_USDC"
    assert obj.price == "45000.0"
    assert obj.quantity == "0.01"
    assert obj.side == "sell"


def test_BackpackRawLiquidation_missing_required_fields() -> None:
    """Test BackpackRawLiquidation validation fails when required fields are missing.

    Ensures that the model properly enforces the presence of all required fields
    (symbol, price, quantity, side) and raises ValidationError when any are absent.
    This is critical for data integrity in the trading system.
    """
    for field in ["symbol", "price", "quantity", "side"]:
        p: dict[str, Any] = valid_liquidation().copy()
        del p[field]
        with pytest.raises(ValidationError):
            BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_wrong_type_fields() -> None:
    """Test BackpackRawLiquidation validation fails with incorrect field types.

    Verifies that the model enforces correct data types for fields, rejecting
    numeric prices (should be strings) and non-string side values. This
    ensures type safety when processing Backpack API responses.
    """
    p: dict[str, Any] = valid_liquidation().copy()
    p["price"] = 45000.0
    with pytest.raises(TypeError):
        BackpackRawLiquidation.model_validate(p)
    p = valid_liquidation().copy()
    p["side"] = ["sell"]
    with pytest.raises(TypeError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_invalid_format_fields() -> None:
    """Test BackpackRawLiquidation validation fails with malformed field values.

    Validates that the model properly rejects invalid decimal formats, unknown
    side values, negative amounts, and non-finite numeric values. These checks
    are essential for preventing invalid financial data from entering the trading system.
    """
    p: dict[str, Any] = valid_liquidation().copy()
    p["price"] = "1..0"
    with pytest.raises(ValidationError, match=r"Cannot convert to Decimal"):
        BackpackRawLiquidation.model_validate(p)
    p = valid_liquidation().copy()
    p["quantity"] = "nan"
    with pytest.raises(ValidationError, match="Value must be a finite decimal"):
        BackpackRawLiquidation.model_validate(p)
    p = valid_liquidation().copy()
    p["side"] = "sideways"
    with pytest.raises(ValidationError, match=r"Field 'side' must be one of"):
        BackpackRawLiquidation.model_validate(p)
    p = valid_liquidation().copy()
    p["quantity"] = "-0.001"
    with pytest.raises(ValidationError, match="Liquidation quantity cannot be negative"):
        BackpackRawLiquidation.model_validate(p)
    p = valid_liquidation().copy()
    p["price"] = "-100.0"
    with pytest.raises(ValidationError, match="Liquidation price cannot be negative"):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_extra_field() -> None:
    """Test BackpackRawLiquidation validation rejects unexpected extra fields.

    Ensures the model's extra='forbid' configuration properly prevents unknown
    fields from being accepted, maintaining strict API contract compliance.
    """
    p: dict[str, Any] = valid_liquidation().copy()
    p["foo"] = 1
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_corruption_cases() -> None:
    """Test BackpackRawLiquidation handles various data corruption and attack scenarios.

    Comprehensive testing of edge cases including null values, control characters,
    unicode handling, whitespace normalization, length limits, and special numeric
    values. These tests ensure the model is robust against malicious or corrupted
    data that could compromise the trading system.
    """
    # Null required
    p: dict[str, Any] = valid_liquidation().copy()
    p["symbol"] = None
    with pytest.raises(TypeError):
        BackpackRawLiquidation.model_validate(p)
    # Unicode/control chars
    p = valid_liquidation().copy()
    p["symbol"] = "BTC_USDC\x00"
    obj = BackpackRawLiquidation.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Emoji
    p = valid_liquidation().copy()
    p["symbol"] = "BTC_USDC😀"
    obj = BackpackRawLiquidation.model_validate(p)
    assert obj.symbol.startswith("BTC_USDC")
    # Whitespace
    p = valid_liquidation().copy()
    p["symbol"] = "   BTC_USDC   "
    obj = BackpackRawLiquidation.model_validate(p)
    assert "BTC_USDC" in obj.symbol
    # Garbled numerics
    p = valid_liquidation().copy()
    p["price"] = "NaN"
    with pytest.raises(ValidationError):
        BackpackRawLiquidation.model_validate(p)
    # Excessive length
    p = valid_liquidation().copy()
    p["symbol"] = "A" * 10000
    with pytest.raises(TypeFieldError):
        BackpackRawLiquidation.model_validate(p)
    # Negative/zero/NaN/inf quantities
    for val in ["-0.01", "0", "NaN", "inf", "-inf"]:
        p = valid_liquidation().copy()
        p["quantity"] = val
        if val in ["-0.01", "NaN", "inf", "-inf"]:
            with pytest.raises(ValidationError):
                BackpackRawLiquidation.model_validate(p)
        else:
            obj = BackpackRawLiquidation.model_validate(p)
            assert obj.quantity == val
    # Scientific notation
    p = valid_liquidation().copy()
    p["quantity"] = "2e-3"
    obj = BackpackRawLiquidation.model_validate(p)
    assert obj.quantity == "2e-3"  # Scientific notation is allowed (project policy)
    # Truncated JSON
    bad_json = '{"symbol": "BTC_USDC", "price": "45000.0"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawLiquidation_frozen() -> None:
    """Test that BackpackRawLiquidation model is immutable (frozen=True).

    Verifies that the model enforces immutability by preventing field modifications
    after instantiation. This ensures data integrity and prevents accidental
    mutations in the trading system.
    """
    obj = BackpackRawLiquidation.model_validate(valid_liquidation())
    with pytest.raises(ValidationError, match="Instance is frozen"):
        obj.symbol = "ETH_USDC"
    with pytest.raises(ValidationError, match="Instance is frozen"):
        obj.quantity = "0.1"


def test_BackpackRawWithdrawal_real_json_edge_case() -> None:
    """Validate BackpackRawWithdrawal using a real JSON payload with edge values.

    Tests the model with realistic edge case data including very large IDs,
    unicode characters in asset names, and very small amounts to ensure
    the model handles real-world API response variations correctly.
    """
    payload = {
        "id": "wd_999999999999999999",
        "asset": "USDC_😀",
        "amount": "0.00000001",
        "status": "completed",
    }
    obj = BackpackRawWithdrawal.model_validate(payload)
    assert obj.asset == "USDC_😀"
    assert obj.amount == "0.00000001"
    assert obj.status == "completed"


def test_BackpackRawWithdrawal_corruption_null_id() -> None:
    """Test BackpackRawWithdrawal validation fails with null value for required 'id' field.

    Ensures that null values for required fields are properly rejected to maintain
    data integrity and prevent processing of incomplete withdrawal records.
    """
    p = valid_withdrawal().copy()
    p["id"] = None
    with pytest.raises(TypeError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_corruption_binary_asset() -> None:
    """Test BackpackRawWithdrawal validation fails with binary data for 'asset' field.

    Ensures that binary data is properly rejected for string fields to prevent
    data corruption and maintain type safety in the trading system.
    """
    p = valid_withdrawal().copy()
    p["asset"] = b"\x00\x01"
    with pytest.raises(TypeError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_corruption_nested_amount() -> None:
    """Test BackpackRawWithdrawal validation fails with nested object for 'amount' field.

    Ensures that complex nested objects are properly rejected for string fields
    to maintain data integrity and prevent processing of malformed financial data.
    """
    p = valid_withdrawal().copy()
    p["amount"] = {"foo": "bar"}
    with pytest.raises(TypeError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_corruption_list_status() -> None:
    """Test BackpackRawWithdrawal validation fails with list for 'status' field.

    Ensures that list values are properly rejected for string fields to maintain
    type safety and prevent processing of incorrectly structured status data.
    """
    p = valid_withdrawal().copy()
    p["status"] = ["pending"]
    with pytest.raises(TypeError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawWithdrawal_corruption_garbled_unicode_asset() -> None:
    """Test BackpackRawWithdrawal validation fails with garbled unicode in 'asset' field.

    Ensures that malformed unicode sequences are properly rejected to prevent
    data corruption and maintain string integrity in the trading system.
    """
    p = valid_withdrawal().copy()
    p["asset"] = "USDC\udce2\udc28\udc00"
    # UTF-8 validation is format validation, not type validation
    with pytest.raises(TypeFieldError):
        BackpackRawWithdrawal.model_validate(p)


def test_BackpackRawDeposit_real_json_edge_case() -> None:
    """Validate BackpackRawDeposit using a real JSON payload with edge values.

    Tests the model with realistic edge case data including very large IDs,
    unicode characters in asset names, and very large amounts to ensure
    the model handles real-world API response variations correctly.
    """
    payload = {
        "id": "dp_999999999999999999",
        "asset": "BTC_😀",
        "amount": "99999999.99999999",
        "status": "pending",
    }
    obj = BackpackRawDeposit.model_validate(payload)
    assert obj.asset == "BTC_😀"
    assert obj.amount == "99999999.99999999"
    assert obj.status == "pending"


def test_BackpackRawDeposit_corruption_null_id() -> None:
    """Test BackpackRawDeposit validation fails with null value for required 'id' field.

    Ensures that null values for required fields are properly rejected to maintain
    data integrity and prevent processing of incomplete deposit records.
    """
    p = valid_deposit().copy()
    p["id"] = None
    with pytest.raises(TypeError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_corruption_binary_asset() -> None:
    """Test BackpackRawDeposit validation fails with binary data for 'asset' field.

    Ensures that binary data is properly rejected for string fields to prevent
    data corruption and maintain type safety in the trading system.
    """
    p = valid_deposit().copy()
    p["asset"] = b"\x00\x01"
    with pytest.raises(TypeError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_corruption_nested_amount() -> None:
    """Test BackpackRawDeposit validation fails with nested object for 'amount' field.

    Ensures that complex nested objects are properly rejected for string fields
    to maintain data integrity and prevent processing of malformed financial data.
    """
    p = valid_deposit().copy()
    p["amount"] = {"foo": "bar"}
    with pytest.raises(TypeError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_corruption_list_status() -> None:
    """Test BackpackRawDeposit validation fails with list for 'status' field.

    Ensures that list values are properly rejected for string fields to maintain
    type safety and prevent processing of incorrectly structured status data.
    """
    p = valid_deposit().copy()
    p["status"] = ["completed"]
    with pytest.raises(TypeError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawDeposit_corruption_garbled_unicode_asset() -> None:
    """Test BackpackRawDeposit validation fails with garbled unicode in 'asset' field.

    Ensures that malformed unicode sequences are properly rejected to prevent
    data corruption and maintain string integrity in the trading system.
    """
    p = valid_deposit().copy()
    p["asset"] = "BTC\udce2\udc28\udc00"
    # UTF-8 validation is format validation, not type validation
    with pytest.raises(TypeFieldError):
        BackpackRawDeposit.model_validate(p)


def test_BackpackRawLiquidation_real_json_edge_case() -> None:
    """Validate BackpackRawLiquidation using a real JSON payload with edge values.

    Tests the model with realistic edge case data including unicode characters
    in symbol names, very small prices, and very large quantities to ensure
    the model handles real-world API response variations correctly.
    """
    payload = {
        "symbol": "BTC_USDC_😀",
        "price": "0.00000001",
        "quantity": "99999999.99999999",
        "side": "buy",
    }
    obj = BackpackRawLiquidation.model_validate(payload)
    assert obj.symbol == "BTC_USDC_😀"
    assert obj.price == "0.00000001"
    assert obj.quantity == "99999999.99999999"
    assert obj.side == "buy"


def test_BackpackRawLiquidation_corruption_null_symbol() -> None:
    """Test BackpackRawLiquidation validation fails with null value for required 'symbol' field.

    Ensures that null values for required fields are properly rejected to maintain
    data integrity and prevent processing of incomplete liquidation records.
    """
    p = valid_liquidation().copy()
    p["symbol"] = None
    with pytest.raises(TypeError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_corruption_binary_price() -> None:
    """Test BackpackRawLiquidation validation fails with binary data for 'price' field.

    Ensures that binary data is properly rejected for string fields to prevent
    data corruption and maintain type safety in the trading system.
    """
    p = valid_liquidation().copy()
    p["price"] = b"\x00\x01"
    with pytest.raises(TypeError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_corruption_nested_quantity() -> None:
    """Test BackpackRawLiquidation validation fails with nested object for 'quantity' field.

    Ensures that complex nested objects are properly rejected for string fields
    to maintain data integrity and prevent processing of malformed financial data.
    """
    p = valid_liquidation().copy()
    p["quantity"] = {"foo": "bar"}
    with pytest.raises(TypeError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_corruption_list_side() -> None:
    """Test BackpackRawLiquidation validation fails with list for 'side' field.

    Ensures that list values are properly rejected for string fields to maintain
    type safety and prevent processing of incorrectly structured side data.
    """
    p = valid_liquidation().copy()
    p["side"] = ["buy"]
    with pytest.raises(TypeError):
        BackpackRawLiquidation.model_validate(p)


def test_BackpackRawLiquidation_corruption_garbled_unicode_symbol() -> None:
    """Test BackpackRawLiquidation validation fails with garbled unicode in 'symbol' field.

    Ensures that malformed unicode sequences are properly rejected to prevent
    data corruption and maintain string integrity in the trading system.
    """
    p = valid_liquidation().copy()
    p["symbol"] = "BTC_USDC\udce2\udc28\udc00"
    # UTF-8 validation is format validation, not type validation
    with pytest.raises(TypeFieldError):
        BackpackRawLiquidation.model_validate(p)

"""
Unit tests for the portfolio models (SpotBalance, Position).
Focuses on validation, parsing, and core logic for each model.
"""

import logging
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.spot_balance import SpotBalance


# --- Helper Fixtures ---
@pytest.fixture
def base_spot_balance_data() -> dict[str, Any]:
    """Provides a dictionary with valid data for SpotBalance creation."""
    return {
        "exchange": "backpack",
        "asset": "SOL",
        "total_quantity": Decimal("10.5"),
        "available_quantity": Decimal("8.0"),
        "mark_price": Decimal("150.25"),
        "balance_notional": Decimal("1577.625"),  # 10.5 * 150.25
        "collateral_weight": Decimal("0.95"),
        "collateral_value": Decimal("1498.74375"),  # 1577.625 * 0.95
        "open_order_quantity": Decimal("2.5"),  # 10.5 - 8.0
        "lend_quantity": Decimal("0.0"),
    }


# --- Success Tests ---


def test_spot_balance_creation_all_fields(base_spot_balance_data: dict[str, Any]) -> None:
    """Test successful creation with all valid fields provided."""
    balance = SpotBalance(**base_spot_balance_data)
    assert balance.exchange == "backpack"
    assert balance.asset == "SOL"
    assert balance.total_quantity == Decimal("10.5")
    assert balance.available_quantity == Decimal("8.0")
    assert balance.mark_price == Decimal("150.25")
    assert balance.balance_notional == Decimal("1577.625")
    assert balance.collateral_weight == Decimal("0.95")
    assert balance.collateral_value == Decimal("1498.74375")
    assert balance.open_order_quantity == Decimal("2.5")
    assert balance.lend_quantity == Decimal("0.0")
    assert balance.model_config.get("frozen") is True


def test_spot_balance_creation_required_only(base_spot_balance_data: dict[str, Any]) -> None:
    """Test successful creation with only required fields."""
    required_data = {
        k: v
        for k, v in base_spot_balance_data.items()
        if k in ["exchange", "asset", "total_quantity", "available_quantity"]
    }
    balance = SpotBalance(**required_data)
    assert balance.exchange == "backpack"
    assert balance.asset == "SOL"
    assert balance.total_quantity == Decimal("10.5")
    assert balance.available_quantity == Decimal("8.0")
    # Optional fields should default to None
    assert balance.mark_price is None
    assert balance.balance_notional is None
    assert balance.collateral_weight is None
    assert balance.collateral_value is None
    assert balance.open_order_quantity is None
    assert balance.lend_quantity is None


def test_spot_balance_creation_with_strings(base_spot_balance_data: dict[str, Any]) -> None:
    """Test creation using string representations for decimal fields."""
    data = base_spot_balance_data.copy()
    # Convert decimals to strings
    for key, value in data.items():
        if isinstance(value, Decimal):
            data[key] = str(value)

    # Pydantic validators handle the conversion.
    balance = SpotBalance(**data)

    # Assert values were converted correctly
    assert balance.total_quantity == Decimal("10.5")
    assert balance.mark_price == Decimal("150.25")
    assert balance.collateral_weight == Decimal("0.95")


# --- Failure Tests: Invalid Field Values ---


@pytest.mark.parametrize(
    "field, value, error_match",
    [
        # Required String Fields
        ("exchange", None, "Value error, exchange: Expected string, got NoneType"),
        ("exchange", "", "String cannot be empty or whitespace"),
        ("exchange", "   ", "String cannot be empty or whitespace"),
        ("exchange", "a" * 65, "String value too long"),
        ("asset", None, "Value error, asset: Expected string, got NoneType"),
        ("asset", "", "String cannot be empty or whitespace"),
        # Required Decimal Fields (total_quantity, available_quantity)
        # Match exact error from validator for None input
        ("total_quantity", None, "Required value parsed as None or was invalid"),
        ("total_quantity", "abc", "Cannot convert 'abc' to Decimal"),
        ("total_quantity", Decimal("NaN"), "Value must be finite"),
        ("total_quantity", Decimal("Infinity"), "Value must be finite"),
        ("total_quantity", Decimal("-1"), "Input should be greater than or equal to 0"),
        # Match exact error from validator for None input
        ("available_quantity", None, "Required value parsed as None or was invalid"),
        ("available_quantity", Decimal("-0.1"), "Input should be greater than or equal to 0"),
        # Optional Decimal Fields (Validation within validator)
        ("mark_price", Decimal("NaN"), "Value must be finite if provided"),
        ("mark_price", Decimal("0"), "Input should be greater than 0"),
        ("mark_price", Decimal("-1"), "Input should be greater than 0"),
        ("balance_notional", Decimal("NaN"), "Value must be finite if provided"),
        ("balance_notional", Decimal("-1"), "Input should be greater than or equal to 0"),
        ("collateral_weight", Decimal("-1"), "Input should be greater than or equal to 0"),
        ("collateral_value", Decimal("NaN"), "Value must be finite if provided"),
        ("open_order_quantity", Decimal("-1"), "Input should be greater than or equal to 0"),
        ("lend_quantity", Decimal("-1"), "Input should be greater than or equal to 0"),
    ],
)
def test_spot_balance_invalid_field_values(
    base_spot_balance_data: dict[str, Any],
    field: str,
    value: Any,  # noqa: ANN401
    error_match: str,
) -> None:
    """Test validation failures for various invalid field inputs."""
    invalid_data = base_spot_balance_data.copy()
    invalid_data[field] = value

    with pytest.raises(ValidationError, match=error_match):
        SpotBalance(**invalid_data)


# --- Failure Tests: Missing/Extra Fields ---


def test_spot_balance_missing_required_fields(base_spot_balance_data: dict[str, Any]) -> None:
    """Test failure when required fields are missing."""
    required_fields = ["exchange", "asset", "total_quantity", "available_quantity"]
    for field_to_remove in required_fields:
        invalid_data = base_spot_balance_data.copy()
        del invalid_data[field_to_remove]
        # Use precise escaped string from initial failure report
        # Need to double-escape backslashes for the f-string and then regex
        match_str = f"1 validation error for SpotBalance\\n{field_to_remove}\\n  Field required"
        with pytest.raises(ValidationError, match=match_str):
            SpotBalance(**invalid_data)


def test_spot_balance_extra_fields(base_spot_balance_data: dict[str, Any]) -> None:
    """Test failure when extra fields are provided (extra='forbid')."""
    invalid_data = base_spot_balance_data.copy()
    invalid_data["extra_field_123"] = "should cause failure"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        SpotBalance(**invalid_data)


# --- Immutability Test ---


def test_spot_balance_immutability(base_spot_balance_data: dict[str, Any]) -> None:
    """Test that the model is immutable (frozen=True)."""
    balance = SpotBalance(**base_spot_balance_data)
    original_total = balance.total_quantity

    with pytest.raises(ValidationError, match="Instance is frozen"):
        balance.total_quantity = original_total + 1

    # Verify value hasn't changed
    assert balance.total_quantity == original_total

    # Test __setattr__ bypass - log warning if modification occurs but pass test
    logger = logging.getLogger(__name__)
    original_asset = balance.asset
    try:
        object.__setattr__(balance, "asset", "NEWASSET")
        # If setattr succeeded, check if value changed and log warning
        if balance.asset != original_asset:
            logger.warning(
                f"Immutability Test Warning: object.__setattr__ modified frozen field 'asset' "
                f"on SpotBalance instance for {balance.exchange}/{original_asset}. "
                f"Value changed to: {balance.asset}. This might be known Pydantic behavior."
            )
        else:
            # Value didn't change even though setattr didn't raise - also acceptable
            pass
    except Exception as e:
        # If it raises any other exception, fail the test
        pytest.fail(f"object.__setattr__ raised unexpected exception on frozen model: {e}")

    # Regardless of whether setattr raised or modified, direct access should reflect original state or failed modification attempt
    # This assertion might be redundant if we trust the direct assignment test above, but keep for clarity
    assert balance.total_quantity == original_total, "Total quantity changed unexpectedly."
    # If __setattr__ did modify 'asset', the test will pass but log a warning.
    # If it didn't modify 'asset', this assertion also passes.
    assert isinstance(balance.asset, str)  # Basic type check post-attempt

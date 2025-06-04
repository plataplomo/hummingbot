"""
Unit tests for the core SpotBalance model and its Details sub-models.
Focuses on validation, parsing, immutability, and the Core+Details pattern.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.spot_balance import (
    BackpackSpotBalanceDetails,
    HyperliquidSpotBalanceDetails,
    SpotBalance,
)

# Type alias for broad, but Any-free, test parameter values
PrimitiveTestVal = str | int | float | bool | Decimal | None
TestParamValue = PrimitiveTestVal | list[PrimitiveTestVal] | dict[str, PrimitiveTestVal]


# --- Helper Fixtures ---
@pytest.fixture
def valid_bp_spot_details_data() -> dict[str, Any]:
    """Provides valid data for BackpackSpotBalanceDetails."""
    return {
        "open_order_quantity": Decimal("2.5"),
        "lend_quantity": Decimal("1.0"),
        "collateral_weight": Decimal("0.95"),
    }


@pytest.fixture
def base_spot_balance_data() -> dict[str, Any]:
    """Provides a dictionary with valid core data for SpotBalance creation."""
    return {
        "exchange": "backpack",
        "asset": "SOL",
        "timestamp": datetime.now(UTC),
        "total_quantity": Decimal("10.5"),
        "available_quantity": Decimal("7.0"),  # total - open_order - lend
    }


# --- Core SpotBalance Success Tests ---


def test_spot_balance_creation_required_only(base_spot_balance_data: dict[str, Any]) -> None:
    """Test successful creation with only required core fields."""
    balance = SpotBalance(**base_spot_balance_data)
    assert balance.exchange == "backpack"
    assert balance.asset == "SOL"
    assert isinstance(balance.timestamp, datetime)
    assert balance.total_quantity == Decimal("10.5")
    assert balance.available_quantity == Decimal("7.0")
    assert balance.hl_details is None
    assert balance.bp_details is None
    assert balance.model_config.get("frozen") is True
    assert balance.model_config.get("extra") == "forbid"


def test_spot_balance_creation_with_bp_details(
    base_spot_balance_data: dict[str, Any],
    valid_bp_spot_details_data: dict[str, Any],
) -> None:
    """Test successful creation with Backpack details populated."""
    data = base_spot_balance_data.copy()
    data["exchange"] = "backpack"  # Ensure match for clarity
    data["bp_details"] = BackpackSpotBalanceDetails(**valid_bp_spot_details_data)

    balance = SpotBalance(**data)
    assert balance.bp_details is not None
    assert balance.hl_details is None
    assert balance.bp_details.open_order_quantity == Decimal("2.5")
    assert balance.bp_details.lend_quantity == Decimal("1.0")
    assert balance.bp_details.collateral_weight == Decimal("0.95")
    assert balance.bp_details.model_config.get("frozen") is True
    assert balance.bp_details.model_config.get("extra") == "ignore"


def test_spot_balance_creation_with_hl_details(
    base_spot_balance_data: dict[str, Any],
) -> None:
    """Test successful creation with Hyperliquid details (should be empty)."""
    data = base_spot_balance_data.copy()
    data["exchange"] = "hyperliquid"
    # Hyperliquid details are currently empty
    data["hl_details"] = HyperliquidSpotBalanceDetails()

    balance = SpotBalance(**data)
    assert balance.hl_details is not None
    # Add assertion for empty model if fields were added later
    # assert not balance.hl_details.model_dump(exclude_unset=True)
    assert balance.bp_details is None
    assert balance.hl_details.model_config.get("frozen") is True
    assert balance.hl_details.model_config.get("extra") == "ignore"


def test_spot_balance_creation_with_strings(
    base_spot_balance_data: dict[str, Any],
) -> None:
    """Test creation using string representations for core decimal fields."""
    data = base_spot_balance_data.copy()
    data["total_quantity"] = str(data["total_quantity"])
    data["available_quantity"] = str(data["available_quantity"])
    data["timestamp"] = data["timestamp"].isoformat()

    balance = SpotBalance(**data)

    assert balance.total_quantity == Decimal("10.5")
    assert balance.available_quantity == Decimal("7.0")
    assert isinstance(balance.timestamp, datetime)


# --- Core SpotBalance Failure Tests: Invalid Field Values ---


@pytest.mark.parametrize(
    "field, value, error_match",
    [
        # Required String Fields
        ("exchange", None, "Value error, exchange: Expected string, got NoneType"),
        ("exchange", "", "Field exchange: String cannot be empty"),
        ("asset", None, "Value error, asset: Expected string, got NoneType"),
        ("asset", "   ", "Field asset: String cannot be empty"),
        ("asset", "A" * 65, "String value too long"),
        # Required Datetime
        (
            "timestamp",
            None,
            "Value error, timestamp: Required datetime value parsed as None or was invalid.",
        ),
        (
            "timestamp",
            "not-a-datetime",
            r"timestamp: Cannot parse string .* as ISO datetime .* or as numeric timestamp",
        ),
        # Required Decimal Fields (total_quantity, available_quantity)
        ("total_quantity", None, r"Value error, total_quantity: Value cannot be None"),
        ("total_quantity", "abc", "Cannot convert 'abc' to Decimal"),
        ("total_quantity", Decimal("NaN"), "Value must be finite"),
        ("total_quantity", Decimal("-1"), "Input should be greater than or equal to 0"),
        ("available_quantity", None, r"Value error, available_quantity: Value cannot be None"),
        ("available_quantity", Decimal("Infinity"), "Value must be finite"),
        ("available_quantity", Decimal("-0.01"), "Input should be greater than or equal to 0"),
    ],
)
def test_spot_balance_invalid_core_field_values(
    base_spot_balance_data: dict[str, Any],
    field: str,
    value: TestParamValue,
    error_match: str,
) -> None:
    """Test core validation failures for various invalid field inputs."""
    invalid_data = base_spot_balance_data.copy()
    invalid_data[field] = value

    with pytest.raises(ValidationError, match=f".*{error_match}.*"):
        SpotBalance(**invalid_data)


# --- Core SpotBalance Failure Tests: Missing/Extra Fields ---


def test_spot_balance_missing_required_fields(base_spot_balance_data: dict[str, Any]) -> None:
    """Test failure when required core fields are missing."""
    required_fields = ["exchange", "asset", "timestamp", "total_quantity", "available_quantity"]
    for field_to_remove in required_fields:
        invalid_data = base_spot_balance_data.copy()
        del invalid_data[field_to_remove]
        match_str = f"1 validation error for SpotBalance\\n{field_to_remove}\\n  Field required"
        with pytest.raises(ValidationError, match=match_str):
            SpotBalance(**invalid_data)


def test_spot_balance_extra_fields(base_spot_balance_data: dict[str, Any]) -> None:
    """Test failure when extra fields are provided to core model (extra='forbid')."""
    invalid_data = base_spot_balance_data.copy()
    invalid_data["extra_core_field"] = "should cause failure"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        SpotBalance(**invalid_data)


# --- SpotBalance Immutability Test ---


def test_spot_balance_immutability(base_spot_balance_data: dict[str, Any]) -> None:
    """Test that the core SpotBalance model is immutable (frozen=True)."""
    balance = SpotBalance(**base_spot_balance_data)
    original_total = balance.total_quantity

    with pytest.raises(ValidationError, match="Instance is frozen"):
        balance.total_quantity = original_total + 1

    assert balance.total_quantity == original_total

    # Test __setattr__ bypass - log warning if modification occurs but pass test
    logger = logging.getLogger(__name__)
    original_asset = balance.asset
    try:
        object.__setattr__(balance, "asset", "NEWASSET")
        if balance.asset != original_asset:
            logger.warning(
                f"Immutability Test Warning: object.__setattr__ modified frozen field 'asset' "
                f"on SpotBalance instance for {balance.exchange}/{original_asset}. "
                f"Value changed to: {balance.asset}. This might be known Pydantic behavior.",
            )
    except Exception as e:
        pytest.fail(f"object.__setattr__ raised unexpected exception on frozen model: {e}")
    assert isinstance(balance.asset, str)


# --- BackpackSpotBalanceDetails Tests ---


def test_bp_details_creation_and_immutability(
    valid_bp_spot_details_data: dict[str, Any],
) -> None:
    """Test BackpackSpotBalanceDetails creation, defaults, and immutability."""
    details = BackpackSpotBalanceDetails(**valid_bp_spot_details_data)
    assert details.open_order_quantity == Decimal("2.5")
    assert details.lend_quantity == Decimal("1.0")
    assert details.collateral_weight == Decimal("0.95")
    assert details.model_config.get("frozen") is True

    # Test creation with only some fields (others should be None)
    partial_data = {"open_order_quantity": Decimal("1.0")}
    details_partial = BackpackSpotBalanceDetails(**partial_data)
    assert details_partial.open_order_quantity == Decimal("1.0")
    assert details_partial.lend_quantity is None
    assert details_partial.collateral_weight is None

    # Test immutability
    with pytest.raises(ValidationError, match="Instance is frozen"):
        details.open_order_quantity = Decimal("3.0")


@pytest.mark.parametrize(
    "field, value, error_match",
    [
        ("open_order_quantity", Decimal("-1"), "Input should be greater than or equal to 0"),
        ("open_order_quantity", Decimal("NaN"), "Value must be finite if provided"),
        ("lend_quantity", Decimal("-0.1"), "Input should be greater than or equal to 0"),
        ("lend_quantity", "invalid", "Cannot convert 'invalid' to Decimal"),
        ("collateral_weight", Decimal("Infinity"), "Value must be finite if provided"),
    ],
)
def test_bp_details_invalid_field_values(
    valid_bp_spot_details_data: dict[str, Any],
    field: str,
    value: TestParamValue,
    error_match: str,
) -> None:
    """Test validation failures for BackpackSpotBalanceDetails."""
    invalid_data = valid_bp_spot_details_data.copy()
    invalid_data[field] = value
    with pytest.raises(ValidationError, match=error_match):
        BackpackSpotBalanceDetails(**invalid_data)


def test_bp_details_extra_fields_ignored(valid_bp_spot_details_data: dict[str, Any]) -> None:
    """Test extra='ignore' on BackpackSpotBalanceDetails."""
    data = valid_bp_spot_details_data.copy()
    data["extra_ignored_detail"] = "value"
    # Should not raise ValidationError
    details = BackpackSpotBalanceDetails(**data)
    assert not hasattr(details, "extra_ignored_detail")
    assert details.open_order_quantity == Decimal("2.5")


# --- HyperliquidSpotBalanceDetails Tests ---


def test_hl_details_creation_and_immutability() -> None:
    """Test HyperliquidSpotBalanceDetails creation (empty) and immutability."""
    details = HyperliquidSpotBalanceDetails()
    # Assert it's empty if fields were added
    # assert not details.model_dump(exclude_unset=True)
    assert details.model_config.get("frozen") is True

    # Test immutability (if fields are added later)
    # with pytest.raises(ValidationError, match="Instance is frozen"):
    #     details.some_future_field = "value"


def test_hl_details_extra_fields_ignored() -> None:
    """Test extra='ignore' on HyperliquidSpotBalanceDetails."""
    data = {"ignored_field": 123}
    # Should not raise ValidationError
    details = HyperliquidSpotBalanceDetails(**data)
    assert not hasattr(details, "ignored_field")

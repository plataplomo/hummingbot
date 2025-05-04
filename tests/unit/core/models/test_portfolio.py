"""
Unit tests for the portfolio models (SpotBalance, Position).
Focuses on validation, parsing, and core logic for each model.
"""

import logging  # Import logging
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.portfolio import SpotBalance  # Import the refactored model

# --- SpotBalance Tests ---


def test_spot_balance_successful_creation() -> None:
    """Test successful creation with valid data types."""
    balance = SpotBalance(
        exchange="backpack",
        asset="USDC",
        total=Decimal("1000.50"),
        available=Decimal("950.25"),
    )
    assert balance.exchange == "backpack"
    assert balance.asset == "USDC"
    assert balance.total == Decimal("1000.50")
    assert balance.available == Decimal("950.25")
    assert balance.model_config.get("frozen") is True


def test_spot_balance_creation_with_string_numbers() -> None:
    """Test creation using string representations for decimal fields."""
    balance = SpotBalance(
        exchange="hyperliquid",
        asset="BTC",
        total="1.23456",  # type: ignore[arg-type] # Pydantic handles str conversion via validator
        available="0.5",  # type: ignore[arg-type] # Pydantic handles str conversion via validator
    )
    assert balance.total == Decimal("1.23456")
    assert balance.available == Decimal("0.5")


def test_spot_balance_creation_with_integer_numbers() -> None:
    """Test creation using integer representations for decimal fields."""
    balance = SpotBalance(
        exchange="backpack",
        asset="SOL",
        total=100,  # type: ignore[arg-type] # Pydantic handles int conversion via validator
        available=90,  # type: ignore[arg-type] # Pydantic handles int conversion via validator
    )
    assert balance.total == Decimal("100")
    assert balance.available == Decimal("90")


@pytest.mark.parametrize(
    "field, value",
    [
        ("exchange", None),
        ("exchange", ""),
        ("exchange", "   "),
        ("exchange", "a" * 65),
        ("asset", None),
        ("asset", ""),
        ("asset", "   "),
        ("asset", "b" * 65),
        ("total", None),  # Should fail parse_decimal_value(allow_none=False)
        ("total", "abc"),  # Invalid decimal string
        ("total", Decimal("NaN")),
        ("total", Decimal("Infinity")),
        ("total", Decimal("-100")),  # Fails Field(ge=0)
        ("available", None),  # Should fail parse_decimal_value(allow_none=False)
        ("available", "xyz"),  # Invalid decimal string
        ("available", Decimal("-50")),  # Fails Field(ge=0)
        ("available", Decimal("NaN")),
        ("available", Decimal("-Infinity")),
    ],
)
def test_spot_balance_invalid_field_values(field: str, value: Any) -> None:  # noqa: ANN401 - value needs Any for parametrize
    """Test validation failures for various invalid field inputs."""
    valid_data: dict[str, Any] = {
        "exchange": "backpack",
        "asset": "USDC",
        "total": Decimal("100"),
        "available": Decimal("100"),
    }
    invalid_data = valid_data.copy()
    invalid_data[field] = value

    with pytest.raises(ValidationError):
        SpotBalance(**invalid_data)


def test_spot_balance_missing_required_fields() -> None:
    """Test failure when required fields are missing."""
    with pytest.raises(ValidationError) as excinfo:
        # Intentionally missing 'exchange'
        SpotBalance(asset="USDC", total=Decimal("100"), available=Decimal("100"))  # type: ignore[call-arg]
    assert "exchange" in str(excinfo.value)

    with pytest.raises(ValidationError) as excinfo:
        # Intentionally missing 'asset'
        SpotBalance(exchange="backpack", total=Decimal("100"), available=Decimal("100"))  # type: ignore[call-arg]
    assert "asset" in str(excinfo.value)

    with pytest.raises(ValidationError) as excinfo:
        # Intentionally missing 'total'
        SpotBalance(exchange="backpack", asset="USDC", available=Decimal("100"))  # type: ignore[call-arg]
    assert "total" in str(excinfo.value)

    with pytest.raises(ValidationError) as excinfo:
        # Intentionally missing 'available'
        SpotBalance(exchange="backpack", asset="USDC", total=Decimal("100"))  # type: ignore[call-arg]
    assert "available" in str(excinfo.value)


def test_spot_balance_extra_fields_forbidden() -> None:
    """Test that extra fields are forbidden due to model_config."""
    with pytest.raises(ValidationError) as excinfo:
        # Intentionally add extra field
        SpotBalance(  # type: ignore[call-arg]
            exchange="backpack",
            asset="USDC",
            total=Decimal("100"),
            available=Decimal("100"),
            extra_field="should_fail",
        )
    assert "extra_field" in str(excinfo.value)
    assert "Extra inputs are not permitted" in str(excinfo.value)


def test_spot_balance_immutability() -> None:
    """Test that the model is immutable (frozen=True)."""
    balance = SpotBalance(
        exchange="backpack",
        asset="USDC",
        total=Decimal("1000.00"),
        available=Decimal("900.00"),
    )

    # Test standard attribute assignment (should definitely fail)
    with pytest.raises(ValidationError) as excinfo1:
        balance.total = Decimal("1100.00")
    assert "Instance is frozen" in str(excinfo1.value)

    # Test assignment via object.__setattr__
    # Pydantic v2 might not raise ValidationError here for frozen models via object.__setattr__
    # See: https://github.com/pydantic/pydantic/issues/7071 (or similar issues)
    # We will check if the value actually changed instead of expecting an exception.
    logger = logging.getLogger(__name__)  # Get logger instance
    try:
        object.__setattr__(balance, "available", Decimal("950.00"))
        # If the above line doesn't raise, check that the value didn't actually change
        assert balance.available == Decimal("900.00"), (
            "object.__setattr__ unexpectedly modified a frozen model field."
        )
        logger.warning(
            "test_spot_balance_immutability: object.__setattr__ did not raise ValidationError "
            "on a frozen model, but the value remained unchanged (expected Pydantic v2 behavior)."
        )
    except ValidationError as e:
        # This is the older/expected behavior if it *does* raise
        # Check the error message content directly from the exception string
        assert "Instance is frozen" in str(e)


# --- TODO: Add Position Tests Below ---
# Tests for the Position class should be added here later.

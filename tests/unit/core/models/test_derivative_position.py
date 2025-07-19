"""Unit tests for the core DerivativePosition model and its Details sub-models.

Tests validation, parsing, mutability, and the Core+Details pattern.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.derivative_position import (
    BackpackPositionDetails,
    DerivativePosition,
    HyperliquidPositionDetails,
)
from cyberdelta.enums import OrderSide
from cyberdelta.exceptions.field_validation import FieldNameMissingError
from cyberdelta.exceptions.parsing import ParsingError


pytestmark = pytest.mark.timing

# Type alias for broad, but Any-free, test parameter values
PrimitiveTestVal = str | int | float | bool | Decimal | None
TestParamValue = PrimitiveTestVal | list[PrimitiveTestVal] | dict[str, PrimitiveTestVal]

# --- Helper Fixtures ---


@pytest.fixture
def valid_hl_details_data() -> dict[str, Any]:
    """Provide valid data for HyperliquidPositionDetails.

    Returns:
        dict[str, Any]: Valid data dictionary for HyperliquidPositionDetails testing.
    """
    return {
        "leverage_type": "cross",
        "leverage_value": 10,
        "max_leverage": 20,
        "margin_used": Decimal("50.5"),
    }


@pytest.fixture
def valid_bp_details_data() -> dict[str, Any]:
    """Provide valid data for BackpackPositionDetails.

    Returns:
        dict[str, Any]: Valid data dictionary for BackpackPositionDetails testing.
    """
    return {
        "imf_base": Decimal("0.1"),
        "imf_factor": Decimal("0.01"),
        "mmf_base": Decimal("0.05"),
        "mmf_factor": Decimal("0.005"),
        "cumulative_funding": Decimal("-1.23"),
    }


@pytest.fixture
def base_derivative_position_data() -> dict[str, Any]:
    """Provide a dictionary with valid core data for DerivativePosition creation.

    Returns:
        dict[str, Any]: Valid core data dictionary for DerivativePosition testing.
    """
    return {
        "exchange": "hyperliquid",
        "symbol": "BTC-PERP",
        "side": OrderSide.BUY,
        "size": Decimal("1.5"),
        "entry_price": Decimal("50000.0"),
        "timestamp": datetime.now(UTC),
        "mark_price": Decimal("51000.0"),
        "liquidation_price": Decimal("45000.0"),
        "unrealized_pnl": Decimal("1500.0"),
        "realized_pnl": Decimal("0.0"),
        "strategy_name": "FundingArb",
        "signal_id": "signal_123",
    }


# --- Core DerivativePosition Success Tests ---


def test_derivative_position_successful_creation(
    base_derivative_position_data: dict[str, Any],
) -> None:
    """Test successful creation with valid core data, no details."""
    pos = DerivativePosition(**base_derivative_position_data)
    assert pos.exchange == "hyperliquid"
    assert pos.symbol == "BTC-PERP"
    assert pos.side == OrderSide.BUY
    assert pos.size == Decimal("1.5")
    assert pos.entry_price == Decimal("50000.0")
    assert isinstance(pos.timestamp, datetime)
    assert pos.mark_price == Decimal("51000.0")
    assert pos.is_active() is True
    assert pos.model_config.get("frozen") is not True  # Should be mutable
    assert pos.model_config.get("validate_assignment") is True
    assert pos.hl_details is None
    assert pos.bp_details is None


def test_derivative_position_flat_creation(
    base_derivative_position_data: dict[str, Any],
) -> None:
    """Test successful creation of a flat position (size=0)."""
    data = base_derivative_position_data.copy()
    data["size"] = Decimal(0)
    data["entry_price"] = None  # Required for size=0
    data["side"] = OrderSide.SELL  # Side can be last known side when flat
    pos = DerivativePosition(**data)
    assert pos.size == Decimal(0)
    assert pos.entry_price is None
    assert pos.is_active() is False


def test_derivative_position_short_creation(
    base_derivative_position_data: dict[str, Any],
) -> None:
    """Test successful creation of a short position."""
    data = base_derivative_position_data.copy()
    data["side"] = OrderSide.SELL
    data["size"] = Decimal("-1.5")
    data["unrealized_pnl"] = Decimal("-1500.0")  # PnL can be negative
    pos = DerivativePosition(**data)
    assert pos.side == OrderSide.SELL
    assert pos.size == Decimal("-1.5")
    assert pos.is_active() is True


def test_derivative_position_with_hl_details(
    base_derivative_position_data: dict[str, Any],
    valid_hl_details_data: dict[str, Any],
) -> None:
    """Test creation with valid Hyperliquid details (Idea 5)."""
    data = base_derivative_position_data.copy()
    data["exchange"] = "hyperliquid"
    data["hl_details"] = HyperliquidPositionDetails(**valid_hl_details_data)
    pos = DerivativePosition(**data)
    assert pos.hl_details is not None
    assert pos.hl_details.leverage_type == "cross"
    assert pos.bp_details is None
    assert pos.hl_details.model_config.get("frozen") is True  # Details immutable


def test_derivative_position_with_bp_details(
    base_derivative_position_data: dict[str, Any],
    valid_bp_details_data: dict[str, Any],
) -> None:
    """Test creation with valid Backpack details (Idea 5)."""
    data = base_derivative_position_data.copy()
    data["exchange"] = "backpack"
    data["bp_details"] = BackpackPositionDetails(**valid_bp_details_data)
    pos = DerivativePosition(**data)
    assert pos.bp_details is not None
    assert pos.bp_details.imf_base == Decimal("0.1")
    assert pos.hl_details is None
    assert pos.bp_details.model_config.get("frozen") is True  # Details immutable


def test_derivative_position_mutability(
    base_derivative_position_data: dict[str, Any],
) -> None:
    """Test core model mutability and assignment validation."""
    pos = DerivativePosition(**base_derivative_position_data)
    original_timestamp = pos.timestamp

    # Mutate core fields
    new_timestamp = datetime.now(UTC)
    pos.size = Decimal("2.0")
    pos.timestamp = new_timestamp
    pos.mark_price = Decimal("52000.0")

    assert pos.size == Decimal("2.0")
    assert pos.timestamp == new_timestamp
    assert pos.timestamp != original_timestamp
    assert pos.mark_price == Decimal("52000.0")

    # Test invalid assignment (negative mark price via Field constraint)
    match_str = (
        "1 validation error for DerivativePosition\\nmark_price\\n"
        "  Input should be greater than or equal to 0"
    )
    with pytest.raises(ValidationError, match=match_str):
        pos.mark_price = Decimal(-100)

    # Test assignment triggering model validation (size vs entry_price)
    pos.size = Decimal("1.0")
    pos.entry_price = Decimal(50000)  # Set valid entry price first
    # Setting size to 0 when entry_price is non-None should fail
    with pytest.raises(ValidationError, match="entry_price must be None if size is zero"):
        pos.size = Decimal(0)

    # Correct the state first to allow valid assignment
    pos.entry_price = None
    pos.size = Decimal(0)  # Should now succeed
    assert pos.size == Decimal(0)
    assert pos.entry_price is None

    # Test mutating details slot (should be allowed)
    pos.exchange = "backpack"  # Change exchange first to pass model validation
    pos.bp_details = BackpackPositionDetails(imf_base=Decimal("0.2"))
    assert pos.bp_details is not None
    assert pos.bp_details.imf_base == Decimal("0.2")


# --- Core DerivativePosition Failure Tests ---


@pytest.mark.parametrize(
    ("field", "value", "error_match"),
    [
        # Required Strings
        ("exchange", None, "Field 'exchange' must be str, got NoneType"),
        ("exchange", "", "Field exchange: String cannot be empty"),
        ("symbol", None, "Field 'symbol' must be str, got NoneType"),
        ("symbol", "   ", "Field symbol: String cannot be empty"),
        (
            "symbol",
            "S" * 65,
            "Field 'symbol' must be string with max length 64, got string with length 65",
        ),
        # Required Enum
        ("side", None, "Input should be 'BUY' or 'SELL'"),
        ("side", "NEUTRAL", "Input should be 'BUY' or 'SELL'"),
        # Required Decimal (Size)
        ("size", None, "size: Required value parsed as None or was invalid"),
        ("size", "not-a-number", "Cannot convert to Decimal"),
        ("size", Decimal("NaN"), "Field 'size' must be finite"),
        ("size", Decimal("Infinity"), "Field 'size' must be finite"),
        # Required Datetime
        ("timestamp", None, "timestamp: Required datetime parsed as None or invalid"),
        (
            "timestamp",
            "2023-13-01T00:00:00Z",
            r"Cannot parse as ISO datetime.*month must be in 1\.\.12",
        ),
        # Optional Decimals (with constraints)
        ("entry_price", Decimal("NaN"), "Field 'entry_price' must be finite if provided"),
        ("mark_price", Decimal("-0.01"), "Input should be greater than or equal to 0"),
        ("mark_price", Decimal("Infinity"), "Field 'mark_price' must be finite if provided"),
        ("liquidation_price", Decimal(-100), "Input should be greater than or equal to 0"),
        ("unrealized_pnl", Decimal("NaN"), "Field 'unrealized_pnl' must be finite if provided"),
        # Optional Strings
        ("strategy_name", 12345, "Field 'strategy_name' must be str, got int"),
        (
            "strategy_name",
            "A" * 129,
            "Field 'strategy_name' must be string with max length 128, got string with length 129",
        ),
        ("signal_id", {"a": 1}, "Field 'signal_id' must be str, got dict"),
    ],
)
def test_derivative_position_invalid_field_inputs(
    base_derivative_position_data: dict[str, Any],
    field: str,
    value: TestParamValue,
    error_match: str,
) -> None:
    """Test validation failures for individual field invalid inputs."""
    data = base_derivative_position_data.copy()
    # Ensure base state is valid before testing the target field
    if data.get("size", Decimal(1)) == Decimal(0) and field != "entry_price":
        data["entry_price"] = None
    elif data.get("size", Decimal(1)) != Decimal(0) and data.get("entry_price") is None:
        data["entry_price"] = Decimal(50000)  # Need valid entry for non-zero size

    data[field] = value
    # Use a more general regex for Pydantic's verbose error messages
    # This matches the specific error_match string within the larger Pydantic message.
    # Some validators raise TypeError for type mismatches, ParsingError for parsing issues
    with pytest.raises(
        (ValidationError, TypeError, ValueError, ParsingError), match=f".*{error_match}.*"
    ):
        DerivativePosition(**data)


def test_derivative_position_model_validation_failures(
    base_derivative_position_data: dict[str, Any],
    valid_hl_details_data: dict[str, Any],
    valid_bp_details_data: dict[str, Any],
) -> None:
    """Test model validation failures (cross-field logic)."""
    data = base_derivative_position_data.copy()

    # Case 1: Size positive, Side SELL
    data["size"] = Decimal("1.0")
    data["side"] = OrderSide.SELL
    data["entry_price"] = Decimal(100)
    with pytest.raises(ValidationError, match="side must be BUY if size is positive"):
        DerivativePosition(**data)

    # Case 2: Size negative, Side BUY
    data["size"] = Decimal("-1.0")
    data["side"] = OrderSide.BUY
    data["entry_price"] = Decimal(100)
    with pytest.raises(ValidationError, match="side must be SELL if size is negative"):
        DerivativePosition(**data)

    # Case 3: Size non-zero, Entry Price None
    data["size"] = Decimal("1.0")
    data["side"] = OrderSide.BUY
    data["entry_price"] = None
    with pytest.raises(ValidationError, match="entry_price must be provided if size is non-zero"):
        DerivativePosition(**data)

    # Case 4: Size non-zero, Entry Price zero
    data["size"] = Decimal("1.0")
    data["entry_price"] = Decimal(0)
    with pytest.raises(
        ValidationError,
        match=r"entry_price must be positive .* if size is non-zero",
    ):
        DerivativePosition(**data)

    # Case 5: Size non-zero, Entry Price negative
    data["size"] = Decimal("1.0")
    data["entry_price"] = Decimal(-10)
    with pytest.raises(
        ValidationError,
        match=r"entry_price must be positive .* if size is non-zero",
    ):
        DerivativePosition(**data)

    # Case 6: Size zero, Entry Price non-None
    data["size"] = Decimal(0)
    data["entry_price"] = Decimal(100)
    data["side"] = OrderSide.BUY  # Reset side
    with pytest.raises(ValidationError, match="entry_price must be None if size is zero"):
        DerivativePosition(**data)

    # Case 7: HL exchange with BP details
    data = base_derivative_position_data.copy()
    data["exchange"] = "hyperliquid"
    data["bp_details"] = BackpackPositionDetails(**valid_bp_details_data)
    data["hl_details"] = None
    with pytest.raises(
        ValidationError,
        match=r"Backpack details .* must be None for a Hyperliquid position",
    ):
        DerivativePosition(**data)

    # Case 8: BP exchange with HL details
    data = base_derivative_position_data.copy()
    data["exchange"] = "backpack"
    data["hl_details"] = HyperliquidPositionDetails(**valid_hl_details_data)
    data["bp_details"] = None
    with pytest.raises(
        ValidationError,
        match=r"Hyperliquid details .* must be None for a Backpack position",
    ):
        DerivativePosition(**data)

    # Case 9: Other exchange with details provided
    data = base_derivative_position_data.copy()
    data["exchange"] = "other_exchange"
    data["hl_details"] = HyperliquidPositionDetails(**valid_hl_details_data)
    data["bp_details"] = None
    with pytest.raises(
        ValidationError,
        match="Exchange-specific details provided for unrecognized exchange: other_exchange",
    ):
        DerivativePosition(**data)


def test_derivative_position_extra_fields(base_derivative_position_data: dict[str, Any]) -> None:
    """Test extra='forbid' on core model."""
    data = base_derivative_position_data.copy()
    data["unexpected_core_field"] = "value"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DerivativePosition(**data)


# --- Details Model Specific Tests ---


def test_hyperliquid_details_creation_and_immutability(
    valid_hl_details_data: dict[str, Any],
) -> None:
    """Test HyperliquidPositionDetails creation and immutability."""
    details = HyperliquidPositionDetails(**valid_hl_details_data)
    assert details.leverage_type == "cross"
    assert details.leverage_value == 10
    assert details.max_leverage == 20
    assert details.margin_used == Decimal("50.5")
    assert details.model_config.get("frozen") is True

    # Test immutability
    with pytest.raises(ValidationError, match="Instance is frozen"):
        details.leverage_value = 15


@pytest.mark.parametrize(
    ("field", "value", "error_match"),
    [
        ("leverage_type", "sideways", "Field name is unexpectedly None during validation"),
        ("leverage_type", 123, "Field name is unexpectedly None during validation"),
        ("leverage_value", -1, "Must be non-negative"),
        ("leverage_value", "abc", "Field 'leverage_value' must be int, got str"),
        ("max_leverage", -5, "Must be non-negative"),
        ("margin_used", Decimal(-1), "Input should be greater than or equal to 0"),
        ("margin_used", Decimal("NaN"), "Field 'margin_used' must be finite if provided"),
    ],
)
def test_hyperliquid_details_invalid_fields(
    valid_hl_details_data: dict[str, Any],
    field: str,
    value: TestParamValue,
    error_match: str,
) -> None:
    """Test validation failures for HyperliquidPositionDetails."""
    data = valid_hl_details_data.copy()
    data[field] = value
    # Some validators raise TypeError directly for type mismatches, and field validation errors
    with pytest.raises(
        (ValidationError, TypeError, ValueError, FieldNameMissingError), match=error_match
    ):
        HyperliquidPositionDetails(**data)


def test_hyperliquid_details_extra_fields_ignored(valid_hl_details_data: dict[str, Any]) -> None:
    """Test extra='ignore' on HyperliquidPositionDetails."""
    data = valid_hl_details_data.copy()
    data["extra_ignored_field"] = "does not matter"
    # Should not raise ValidationError
    details = HyperliquidPositionDetails(**data)
    assert not hasattr(details, "extra_ignored_field")
    assert details.leverage_value == 10  # Check original fields still correct


def test_backpack_details_creation_and_immutability(valid_bp_details_data: dict[str, Any]) -> None:
    """Test BackpackPositionDetails creation and immutability."""
    details = BackpackPositionDetails(**valid_bp_details_data)
    assert details.imf_base == Decimal("0.1")
    assert details.cumulative_funding == Decimal("-1.23")
    assert details.model_config.get("frozen") is True

    # Test immutability
    with pytest.raises(ValidationError, match="Instance is frozen"):
        details.cumulative_funding = Decimal(0)


@pytest.mark.parametrize(
    ("field", "value", "error_match"),
    [
        ("imf_base", Decimal("NaN"), "Field 'imf_base' must be finite if provided"),
        (
            "imf_factor",
            "invalid",
            "Field 'imf_factor' decimal validation failed: Cannot convert to Decimal",
        ),
        (
            "cumulative_funding",
            Decimal("Infinity"),
            "Field 'cumulative_funding' must be finite if provided",
        ),
    ],
)
def test_backpack_details_invalid_fields(
    valid_bp_details_data: dict[str, Any],
    field: str,
    value: TestParamValue,
    error_match: str,
) -> None:
    """Test validation failures for BackpackPositionDetails."""
    data = valid_bp_details_data.copy()
    data[field] = value
    with pytest.raises(ValidationError, match=error_match):
        BackpackPositionDetails(**data)


def test_backpack_details_extra_fields_ignored(valid_bp_details_data: dict[str, Any]) -> None:
    """Test extra='ignore' on BackpackPositionDetails."""
    data_with_extra = valid_bp_details_data.copy()
    data_with_extra["some_random_field"] = "should_be_ignored"
    # Expect no error, as extra fields should be ignored by default or if extra='ignore'
    details = BackpackPositionDetails(**data_with_extra)
    assert not hasattr(details, "some_random_field")

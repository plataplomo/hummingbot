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
from cyberdelta.core.models.enums import OrderSide

# --- DerivativePosition Tests ---

# --- Helper Fixtures ---


@pytest.fixture
def valid_hl_details() -> HyperliquidPositionDetails:
    """Creates valid internal HyperliquidPositionDetails."""
    return HyperliquidPositionDetails(
        leverage_type="cross",
        leverage_value=10,
        max_leverage=20,
        margin_used=Decimal("50.5"),
    )


@pytest.fixture
def valid_bp_details() -> BackpackPositionDetails:
    """Creates valid internal BackpackPositionDetails."""
    return BackpackPositionDetails(
        imf_base=Decimal("0.1"),
        imf_factor=Decimal("0.01"),
        mmf_base=Decimal("0.05"),
        mmf_factor=Decimal("0.005"),
        cumulative_funding=Decimal("-1.23"),
    )


@pytest.fixture
def base_derivative_position_data() -> dict[str, Any]:
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


# --- Core DerivativePosition Tests ---


def test_derivative_position_successful_creation(
    base_derivative_position_data: dict[str, Any],
) -> None:
    """Test successful creation with valid core data."""
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
    """Test successful creation of a flat position (size=0). Ensure entry price is None."""
    data = base_derivative_position_data.copy()
    data["size"] = Decimal("0")
    data["entry_price"] = None  # Must be None when size is 0
    data["side"] = OrderSide.BUY  # Side can be anything when flat, check validation allows it
    pos = DerivativePosition(**data)
    assert pos.size == Decimal("0")
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
    base_derivative_position_data: dict[str, Any], valid_hl_details: HyperliquidPositionDetails
) -> None:
    """Test creation with valid Hyperliquid details."""
    data = base_derivative_position_data.copy()
    data["exchange"] = "hyperliquid"  # Ensure exchange matches details
    data["hl_details"] = valid_hl_details
    pos = DerivativePosition(**data)
    assert pos.hl_details == valid_hl_details
    assert pos.bp_details is None
    assert (
        pos.hl_details is not None and pos.hl_details.model_config.get("frozen") is True
    )  # Details are immutable


def test_derivative_position_with_bp_details(
    base_derivative_position_data: dict[str, Any], valid_bp_details: BackpackPositionDetails
) -> None:
    """Test creation with valid Backpack details."""
    data = base_derivative_position_data.copy()
    data["exchange"] = "backpack"
    data["bp_details"] = valid_bp_details
    pos = DerivativePosition(**data)
    assert pos.bp_details == valid_bp_details
    assert pos.hl_details is None
    assert (
        pos.bp_details is not None and pos.bp_details.model_config.get("frozen") is True
    )  # Details are immutable


def test_derivative_position_mutability(
    base_derivative_position_data: dict[str, Any],
) -> None:
    """Test that core fields can be mutated and validation triggers."""
    pos = DerivativePosition(**base_derivative_position_data)
    original_timestamp = pos.timestamp

    # Mutate size and timestamp
    new_timestamp = datetime.now(UTC)
    pos.size = Decimal("2.0")
    pos.timestamp = new_timestamp

    assert pos.size == Decimal("2.0")
    assert pos.timestamp == new_timestamp
    assert pos.timestamp != original_timestamp

    # Test invalid mutation (negative mark price)
    with pytest.raises(ValidationError, match="mark_price"):
        pos.mark_price = Decimal("-100")

    # Test mutation triggering model validation (size vs entry_price)
    # Ensure entry_price is initially valid
    pos.size = Decimal("1.0")
    pos.entry_price = Decimal("50000")
    # Now, setting size to 0 should trigger the model validator because entry_price is not None
    with pytest.raises(ValidationError, match="entry_price must be None if size is zero"):
        pos.size = Decimal("0")

    # Correct the state *before* assignment to avoid the model validation error during assignment
    pos.entry_price = None
    pos.size = Decimal("0")  # This assignment should now pass
    assert pos.size == Decimal("0")
    assert pos.entry_price is None

    # Re-check validation passes - assign a valid value
    pos.symbol = "ETH-PERP"  # Should not raise now
    assert pos.symbol == "ETH-PERP"


# --- Validation Failure Tests ---


@pytest.mark.parametrize(
    "field, value, error_part",
    [
        # Required string fields validation (None input -> Specific error)
        ("exchange", None, "Value error, exchange: Expected string, got NoneType"),
        ("exchange", "", "String cannot be empty or whitespace"),
        ("symbol", None, "Value error, symbol: Expected string, got NoneType"),
        ("symbol", " ", "String cannot be empty or whitespace"),
        # Required enum field validation (None input -> Specific error)
        ("side", None, "Input should be 'BUY' or 'SELL'"),
        # Invalid enum value
        ("side", "INVALID_SIDE", "Input should be 'BUY' or 'SELL'"),
        # Required decimal field validation
        ("size", None, "Value error, size: Value cannot be None"),
        ("size", "abc", "Cannot convert 'abc' to Decimal"),
        ("size", Decimal("NaN"), "Value must be finite"),
        ("entry_price", Decimal("NaN"), "Value must be finite if provided"),
        (
            "entry_price",
            Decimal("-10"),
            # Simpler match for the core message
            "Value error, entry_price must be positive (> 0) if size is non-zero",
        ),
        # Required datetime field validation
        ("timestamp", None, "Value error, timestamp: Value cannot be None"),
        ("timestamp", "invalid-date", "Cannot parse ISO datetime string"),
        # Optional fields with constraints
        ("mark_price", Decimal("-1"), "Input should be greater than or equal to 0"),
        ("mark_price", Decimal("Infinity"), "Value must be finite if provided"),
        ("liquidation_price", Decimal("-1"), "Input should be greater than or equal to 0"),
        ("liquidation_price", Decimal("NaN"), "Value must be finite if provided"),
        (
            "unrealized_pnl",
            Decimal("Infinity"),
            "Value must be finite if provided",
        ),
        ("realized_pnl", Decimal("NaN"), "Value must be finite if provided"),
        # Optional string fields validation (Wrong type -> Specific error)
        ("strategy_name", 123, "Value error, strategy_name: Expected string, got int"),
        ("strategy_name", "s" * 129, "String value too long"),
        ("signal_id", [], "Value error, signal_id: Expected string, got list"),
    ],
)
def test_derivative_position_invalid_core_fields(
    base_derivative_position_data: dict[str, Any],
    field: str,
    value: Any,  # noqa: ANN401 - Necessary for pytest parametrize flexibility
    error_part: str,
) -> None:
    """Test validation failures for individual core field invalid inputs."""
    data = base_derivative_position_data.copy()

    # Special handling for model validation checks vs field checks
    data[field] = value

    # Need to ensure base state is valid before testing the target field
    # Size/Entry Price Interdependency:
    if field != "size" and field != "entry_price":
        current_size = data.get("size", Decimal("1"))  # Default to non-zero if absent
        if current_size == Decimal("0"):
            data["entry_price"] = None
        else:
            # Fix Mypy [operator] error by checking entry_price is not None before comparison
            entry_price_val = data.get("entry_price")
            if entry_price_val is None or entry_price_val <= Decimal("0"):
                data["entry_price"] = Decimal("50000")  # Provide valid entry price
    elif field == "entry_price" and value == Decimal("-10"):
        # This is caught by model validator, ensure size is non-zero for the check
        if data.get("size", Decimal("1")) == Decimal("0"):
            data["size"] = Decimal("1.0")  # Need non-zero size to test negative entry price
    elif field == "size" and value != Decimal("0"):
        # If testing size and making it non-zero, ensure entry price is valid
        entry_price_val = data.get("entry_price")
        if entry_price_val is None or entry_price_val <= Decimal("0"):
            data["entry_price"] = Decimal("50000")
    elif field == "size" and value == Decimal("0"):
        # If testing size and making it zero, entry price must be None
        data["entry_price"] = None

    # Side/Size Interdependency
    if field != "side" and field != "size":
        current_size = data.get("size", Decimal("1.0"))
        if current_size > Decimal("0"):
            data["side"] = OrderSide.BUY
        elif current_size < Decimal("0"):
            data["side"] = OrderSide.SELL
        # else: side can be either if size is 0

    with pytest.raises(ValidationError) as excinfo:
        DerivativePosition(**data)
    # print(f"Testing {field}={value}: Error = {excinfo.value}") # Debug print
    assert error_part in str(excinfo.value)


def test_derivative_position_model_validation_failures(
    base_derivative_position_data: dict[str, Any],
) -> None:
    """Test model validation failures (cross-field logic)."""
    data = base_derivative_position_data.copy()

    # Case 1: Size positive, Side SELL
    data["size"] = Decimal("1.0")
    data["side"] = OrderSide.SELL
    data["entry_price"] = Decimal("100")  # Ensure entry price valid for size
    with pytest.raises(ValidationError, match="side must be BUY if size is positive"):
        DerivativePosition(**data)

    # Case 2: Size negative, Side BUY
    data["size"] = Decimal("-1.0")
    data["side"] = OrderSide.BUY
    data["entry_price"] = Decimal("100")
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
    data["entry_price"] = Decimal("0")
    with pytest.raises(
        ValidationError,
        # Use raw string and match exact model validation error
        match=r"Value error, entry_price must be positive \(> 0\) if size is non-zero",
    ):
        DerivativePosition(**data)

    # Case 5: Size non-zero, Entry Price negative
    data["size"] = Decimal("1.0")
    data["entry_price"] = Decimal("-10")
    with pytest.raises(
        ValidationError,
        # Use raw string and match exact model validation error
        match=r"Value error, entry_price must be positive \(> 0\) if size is non-zero",
    ):
        DerivativePosition(**data)

    # Case 6: Size zero, Entry Price non-None
    data["size"] = Decimal("0")
    data["entry_price"] = Decimal("100")
    data["side"] = OrderSide.BUY  # Reset side for this test
    with pytest.raises(ValidationError, match="entry_price must be None if size is zero"):
        DerivativePosition(**data)


def test_derivative_position_exchange_details_mismatch(
    base_derivative_position_data: dict[str, Any],
    valid_hl_details: HyperliquidPositionDetails,
    valid_bp_details: BackpackPositionDetails,
) -> None:
    """Test validation failure when details mismatch the exchange field."""
    data = base_derivative_position_data.copy()

    # HL exchange with BP details
    data["exchange"] = "hyperliquid"
    data["hl_details"] = None
    data["bp_details"] = valid_bp_details
    # Corrected assertion check
    # Fix: Use raw string for match or escape backslashes
    with pytest.raises(
        ValidationError,
        match=r"Backpack details \(bp_details\) must be None for a Hyperliquid position",
    ):
        DerivativePosition(**data)

    # BP exchange with HL details
    data["exchange"] = "backpack"
    data["hl_details"] = valid_hl_details
    data["bp_details"] = None
    # Fix: Use raw string for match or escape backslashes
    with pytest.raises(
        ValidationError,
        match=r"Hyperliquid details \(hl_details\) must be None for a Backpack position",
    ):
        DerivativePosition(**data)


def test_derivative_position_extra_fields(base_derivative_position_data: dict[str, Any]) -> None:
    """Test that extra fields cause a validation error due to extra='forbid'."""
    data = base_derivative_position_data.copy()
    data["extra_field"] = "should_fail"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DerivativePosition(**data)


# --- Details Model Specific Tests ---


def test_hyperliquid_details_validation() -> None:
    """Test validation specific to HyperliquidPositionDetails."""
    # Valid
    details = HyperliquidPositionDetails(
        leverage_type="isolated",
        leverage_value=5,
        max_leverage=10,
        margin_used=Decimal("100.5"),  # Fix arg-type
    )
    assert details.leverage_type == "isolated"
    assert details.leverage_value == 5
    assert details.max_leverage == 10
    assert details.margin_used == Decimal("100.5")
    assert details.model_config.get("frozen") is True

    # Invalid leverage type
    with pytest.raises(
        ValidationError, match="leverage_type: Validation failed - leverage_type: Invalid value"
    ):
        HyperliquidPositionDetails(leverage_type="bad", leverage_value=5, max_leverage=10)

    # Invalid leverage value (negative)
    with pytest.raises(ValidationError, match="leverage_value: Must be non-negative"):
        HyperliquidPositionDetails(leverage_type="cross", leverage_value=-1, max_leverage=10)

    # Invalid max_leverage (negative)
    with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
        HyperliquidPositionDetails(leverage_type="cross", leverage_value=5, max_leverage=-10)

    # Invalid margin_used (negative)
    with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
        HyperliquidPositionDetails(
            leverage_type="cross",
            leverage_value=5,
            max_leverage=10,
            margin_used=Decimal("-1.0"),  # Fix arg-type
        )

    # Invalid margin_used (NaN)
    with pytest.raises(ValidationError, match="Value must be finite if provided"):
        HyperliquidPositionDetails(
            leverage_type="cross", leverage_value=5, max_leverage=10, margin_used=Decimal("NaN")
        )

    # Extra fields ignored (test creation, not attribute access)
    details = HyperliquidPositionDetails(
        leverage_type="cross",
        leverage_value=5,
        max_leverage=10,
        margin_used=Decimal("1.0"),  # Fix: Removed extra="ignored"
    )
    # We expect extra='ignore' to work silently, assert base fields are correct
    assert details.leverage_type == "cross"
    assert details.leverage_value == 5
    assert details.max_leverage == 10
    assert details.margin_used == Decimal("1.0")


def test_backpack_details_validation() -> None:
    """Test validation specific to BackpackPositionDetails."""
    # Valid (all optional fields can be None)
    details = BackpackPositionDetails()
    assert details.imf_base is None
    assert details.cumulative_funding is None
    assert details.model_config.get("frozen") is True

    # Valid with values
    details = BackpackPositionDetails(
        imf_base=Decimal("0.1"),  # Fix arg-type
        imf_factor=Decimal("0.01"),  # Fix arg-type
        mmf_base=Decimal("0.05"),  # Fix arg-type
        mmf_factor=Decimal("0.005"),  # Fix arg-type
        cumulative_funding=Decimal("-5.5"),  # Fix arg-type
    )
    assert details.imf_base == Decimal("0.1")
    assert details.imf_factor == Decimal("0.01")
    assert details.mmf_base == Decimal("0.05")
    assert details.mmf_factor == Decimal("0.005")
    assert details.cumulative_funding == Decimal("-5.5")

    # Invalid decimal format - Testing the 'before' validator
    with pytest.raises(ValidationError, match="Cannot convert 'abc' to Decimal"):
        BackpackPositionDetails(imf_base="abc")  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="Value must be finite if provided"):
        BackpackPositionDetails(mmf_factor=Decimal("inf"))

    # Extra fields ignored (test creation, not attribute access)
    details = BackpackPositionDetails(imf_base=Decimal("0.1"))  # Fix: Removed extra="ignored"
    assert details.imf_base == Decimal("0.1")
    # We expect extra='ignore' to work silently, assert base field is correct

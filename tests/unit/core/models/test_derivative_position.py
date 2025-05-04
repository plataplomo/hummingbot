from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.derivative_position import (
    BackpackPositionDetails,
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
    DerivativePosition,
    HyperliquidPositionDetails,
    HyperliquidRawLeverage,
)
from cyberdelta.core.models.enums import OrderSide

# --- DerivativePosition Tests ---

# --- Helper Fixtures ---


@pytest.fixture
def valid_hl_raw_leverage() -> HyperliquidRawLeverage:
    return HyperliquidRawLeverage(type="cross", value=10)


@pytest.fixture
def valid_hl_details(valid_hl_raw_leverage: HyperliquidRawLeverage) -> HyperliquidPositionDetails:
    return HyperliquidPositionDetails(
        leverage=valid_hl_raw_leverage, max_leverage=20, margin_used=Decimal("50.5")
    )


@pytest.fixture
def valid_bp_imf() -> BackpackRawImfFunction:
    return BackpackRawImfFunction(a=Decimal("0.1"), b=Decimal("0.2"), c=Decimal("0.3"))


@pytest.fixture
def valid_bp_mmf() -> BackpackRawMmfFunction:
    return BackpackRawMmfFunction(base=Decimal("0.05"), factor=Decimal("0.01"))


@pytest.fixture
def valid_bp_details(
    valid_bp_imf: BackpackRawImfFunction, valid_bp_mmf: BackpackRawMmfFunction
) -> BackpackPositionDetails:
    return BackpackPositionDetails(
        cumulative_funding=Decimal("-1.23"),
        imf_function=valid_bp_imf,
        mmf_function=valid_bp_mmf,
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
    with pytest.raises(ValidationError):
        pos.mark_price = Decimal("-100")

    # Test mutation triggering model validation (size vs entry_price)
    pos.size = Decimal("0")
    # Should fail because entry_price is still set
    with pytest.raises(ValidationError) as excinfo:
        object.__setattr__(
            pos, "_check_position_logic_trigger", True
        )  # Force validation check if needed
        # Pydantic v2 validation on assignment + model validator should catch this
        # Forcing a dummy assignment might be needed if model validation doesn't trigger on size=0 alone
        pos.symbol = pos.symbol  # Reassign to potentially trigger validation
    assert "Entry price must be None if position size is zero" in str(excinfo.value)

    # Correct the state
    pos.entry_price = None
    assert pos.size == Decimal("0")
    assert pos.entry_price is None
    # Re-check validation passes
    pos.symbol = pos.symbol  # Should not raise now


# --- Validation Failure Tests ---


@pytest.mark.parametrize(
    "field, value, error_part",
    [
        ("exchange", None, "Field required"),  # Missing required
        ("exchange", "", "String should have at least 1 character"),
        ("symbol", None, "Field required"),
        ("symbol", " ", "String should have at least 1 character"),
        ("side", None, "Field required"),
        ("side", "INVALID_SIDE", "Input should be 'buy' or 'sell'"),
        ("size", None, "Field required"),
        ("size", "abc", "Input should be a valid number"),
        ("size", Decimal("NaN"), "Value must be finite"),
        ("entry_price", Decimal("NaN"), "Value must be finite"),
        ("entry_price", Decimal("-10"), "Input should be greater than 0"),  # Field constraint
        ("timestamp", None, "Field required"),
        ("timestamp", "invalid-date", "Invalid datetime format"),
        ("mark_price", Decimal("-1"), "Input should be greater than or equal to 0"),
        ("mark_price", Decimal("Infinity"), "Value must be finite"),
        ("liquidation_price", Decimal("-1"), "Input should be greater than or equal to 0"),
        ("liquidation_price", Decimal("NaN"), "Value must be finite"),
        ("unrealized_pnl", Decimal("Infinity"), "Value must be finite"),
        ("realized_pnl", Decimal("NaN"), "Value must be finite"),
        ("strategy_name", 123, "Input should be a valid string"),
        ("strategy_name", "s" * 129, "String should have at most 128 characters"),
        ("signal_id", [], "Input should be a valid string"),
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
    data[field] = value
    # Ensure size/entry_price are valid initially if we're testing another field
    if field != "size" and field != "entry_price":
        if data.get("size", Decimal("1")) == Decimal("0"):
            data["entry_price"] = None
        elif data.get("entry_price") is None or (
            entry_price := data.get("entry_price"),
            isinstance(entry_price, Decimal) and entry_price <= Decimal("0"),
        ):
            data["entry_price"] = Decimal("1")  # Ensure valid entry for non-zero size

    with pytest.raises(ValidationError) as excinfo:
        DerivativePosition(**data)
    # print(f"Testing {field}={value}: Error = {excinfo.value}") # Debug print
    assert error_part in str(excinfo.value)


def test_derivative_position_model_validation_failures(
    base_derivative_position_data: dict[str, Any],
) -> None:
    """Test failures caught by the model validator (cross-field logic)."""
    # 1. Size > 0, but entry_price is None
    data1 = base_derivative_position_data.copy()
    data1["size"] = Decimal("1")
    data1["entry_price"] = None
    with pytest.raises(ValidationError, match="Entry price must be provided and positive"):
        DerivativePosition(**data1)

    # 2. Size > 0, but entry_price is zero
    data2 = base_derivative_position_data.copy()
    data2["size"] = Decimal("1")
    data2["entry_price"] = Decimal("0")
    with pytest.raises(ValidationError, match="Entry price must be provided and positive"):
        DerivativePosition(**data2)

    # 3. Size == 0, but entry_price is not None
    data3 = base_derivative_position_data.copy()
    data3["size"] = Decimal("0")
    data3["entry_price"] = Decimal("50000")
    with pytest.raises(ValidationError, match="Entry price must be None if position size is zero"):
        DerivativePosition(**data3)

    # 4. Size > 0, but side is SELL
    data4 = base_derivative_position_data.copy()
    data4["size"] = Decimal("1")
    data4["side"] = OrderSide.SELL
    with pytest.raises(ValidationError, match="Position side must be BUY if size is positive"):
        DerivativePosition(**data4)

    # 5. Size < 0, but side is BUY
    data5 = base_derivative_position_data.copy()
    data5["size"] = Decimal("-1")
    data5["side"] = OrderSide.BUY
    data5["entry_price"] = Decimal("50000")  # Need valid entry for non-zero size
    with pytest.raises(ValidationError, match="Position side must be SELL if size is negative"):
        DerivativePosition(**data5)


def test_derivative_position_exchange_details_mismatch(
    base_derivative_position_data: dict[str, Any],
    valid_hl_details: HyperliquidPositionDetails,
    valid_bp_details: BackpackPositionDetails,
) -> None:
    """Test validation failure when exchange name mismatches provided details."""
    # HL exchange with BP details
    data1 = base_derivative_position_data.copy()
    data1["exchange"] = "hyperliquid"
    data1["bp_details"] = valid_bp_details
    data1["hl_details"] = None
    with pytest.raises(
        ValidationError, match="Backpack details .* provided for a Hyperliquid position"
    ):
        DerivativePosition(**data1)

    # BP exchange with HL details
    data2 = base_derivative_position_data.copy()
    data2["exchange"] = "backpack"
    data2["hl_details"] = valid_hl_details
    data2["bp_details"] = None
    with pytest.raises(
        ValidationError, match="Hyperliquid details .* provided for a Backpack position"
    ):
        DerivativePosition(**data2)


def test_derivative_position_extra_fields(base_derivative_position_data: dict[str, Any]) -> None:
    """Test failure when extra fields are provided (extra='forbid')."""
    data = base_derivative_position_data.copy()
    data["extra_field_123"] = "should cause failure"
    with pytest.raises(ValidationError) as excinfo:
        DerivativePosition(**data)
    assert "Extra inputs are not permitted" in str(excinfo.value)


# --- Details Model Tests ---


def test_hyperliquid_details_validation() -> None:
    """Test validation within HyperliquidPositionDetails."""
    valid_leverage = HyperliquidRawLeverage(type="isolated", value=5)

    # Valid
    details = HyperliquidPositionDetails(
        leverage=valid_leverage, max_leverage=10, margin_used=Decimal("100.5")
    )
    assert details.margin_used == Decimal("100.5")
    assert details.model_config.get("frozen") is True

    # Invalid max_leverage
    with pytest.raises(ValidationError):
        HyperliquidPositionDetails(leverage=valid_leverage, max_leverage=-1)

    # Invalid margin_used (negative)
    with pytest.raises(ValidationError):
        HyperliquidPositionDetails(
            leverage=valid_leverage, max_leverage=10, margin_used=Decimal("-1")
        )

    # Invalid margin_used (NaN)
    with pytest.raises(ValidationError):
        HyperliquidPositionDetails(
            leverage=valid_leverage, max_leverage=10, margin_used=Decimal("NaN")
        )

    # Invalid leverage sub-model (value negative)
    with pytest.raises(ValidationError):
        HyperliquidPositionDetails(
            leverage=HyperliquidRawLeverage(type="cross", value=-5), max_leverage=10
        )


def test_backpack_details_validation() -> None:
    """Test validation within BackpackPositionDetails."""
    valid_imf = BackpackRawImfFunction(a=Decimal("0.1"), b=Decimal("0.2"), c=Decimal("0.3"))
    valid_mmf = BackpackRawMmfFunction(base=Decimal("0.05"), factor=Decimal("0.01"))

    # Valid
    details = BackpackPositionDetails(
        cumulative_funding=Decimal("-0.5"), imf_function=valid_imf, mmf_function=valid_mmf
    )
    assert details.cumulative_funding == Decimal("-0.5")
    assert details.model_config.get("frozen") is True

    # Invalid cumulative_funding (NaN)
    with pytest.raises(ValidationError):
        BackpackPositionDetails(cumulative_funding=Decimal("NaN"))

    # Invalid imf sub-model (non-finite)
    with pytest.raises(ValidationError):
        BackpackPositionDetails(
            imf_function=BackpackRawImfFunction(a=Decimal("inf"), b=Decimal("0"), c=Decimal("0"))
        )

    # Invalid mmf sub-model (missing required)
    with pytest.raises(ValidationError):
        BackpackPositionDetails(
            mmf_function=BackpackRawMmfFunction(base=Decimal("0.1"), factor=Decimal("0"))
        )


# Ensure old Position tests are removed or fully adapted
# (No actual tests for a class named 'Position' should remain)

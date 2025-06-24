"""Unit tests for the CyberDeltaEngine internal TradeSignal model.

Covers:
- Initialization with required/optional fields.
- Validation of types, formats, and constraints (e.g., gt=0).
- Parsing logic for decimals, floats, datetimes.
- Handling of exchange field (str or list[str]).
- `is_valid()` method functionality.
- Mutability and assignment validation.
- `extra='forbid'`.
"""

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.enums import OrderSide, SignalType
from cyberdelta.core.models.trade_signal import TradeSignal


pytestmark = pytest.mark.timing

# --- Helper Fixtures ---


@pytest.fixture
def minimal_signal_data() -> dict[str, Any]:
    """Provide data for a minimal valid TradeSignal."""
    return {
        "symbol": "BTC-PERP",
        "signal_type": SignalType.ENTER_LONG,
        "side": OrderSide.BUY,
        "price": Decimal("60000.123"),
        "exchange": "hyperliquid",
        # quantity is optional
    }


@pytest.fixture
def full_signal_data(minimal_signal_data: dict[str, Any]) -> dict[str, Any]:
    """Provide data for a TradeSignal with all fields populated."""
    # Ensure enum is recognized
    assert isinstance(SignalType.ENTER_LONG, SignalType)
    now = datetime.now(UTC)
    data = minimal_signal_data.copy()
    data.update(
        {
            "quantity": Decimal("0.5"),
            "exchange": ["hyperliquid", "backpack"],  # Test list
            "timestamp": now - timedelta(seconds=10),
            "confidence": 0.85,
            "source_strategy": "MomentumStratV1",
            "stop_loss": Decimal("59000.0"),
            "take_profit": Decimal("65000.0"),
            "expiration": now + timedelta(minutes=5),
            "metadata": {"source_indicator": "RSI", "value": 75},
            "signal_id": str(uuid.uuid4()),  # Override default
        },
    )
    return data


# --- Initialization and Basic Field Tests ---


def test_tradesignal_minimal_valid(minimal_signal_data: dict[str, Any]) -> None:
    """Test creating a valid TradeSignal with minimal required fields."""
    signal = TradeSignal(**minimal_signal_data)

    assert signal.symbol == "BTC-PERP"
    assert signal.signal_type == SignalType.ENTER_LONG
    assert signal.side == OrderSide.BUY
    assert signal.price == Decimal("60000.123")
    assert signal.exchange == "hyperliquid"
    assert signal.quantity is None  # Check optional field default
    assert isinstance(signal.timestamp, datetime)
    assert signal.timestamp.tzinfo == UTC
    assert signal.confidence is None
    assert signal.source_strategy is None
    assert signal.stop_loss is None
    assert signal.take_profit is None
    assert signal.expiration is None
    assert signal.metadata is None
    assert isinstance(uuid.UUID(signal.signal_id), uuid.UUID)  # Validate ID format
    assert signal.model_config.get("extra") == "forbid"
    assert signal.model_config.get("validate_assignment") is True
    assert signal.model_config.get("frozen") is not True


def test_tradesignal_full_valid(full_signal_data: dict[str, Any]) -> None:
    """Test creating a valid TradeSignal with all fields populated."""
    signal = TradeSignal(**full_signal_data)

    assert signal.symbol == "BTC-PERP"
    assert signal.signal_type == SignalType.ENTER_LONG
    assert signal.side == OrderSide.BUY
    assert signal.price == Decimal("60000.123")
    assert signal.quantity == Decimal("0.5")
    assert signal.exchange == ["hyperliquid", "backpack"]
    assert isinstance(signal.timestamp, datetime)
    assert signal.confidence == 0.85
    assert signal.source_strategy == "MomentumStratV1"
    assert signal.stop_loss == Decimal("59000.0")
    assert signal.take_profit == Decimal("65000.0")
    assert isinstance(signal.expiration, datetime)
    assert signal.metadata == {"source_indicator": "RSI", "value": 75}
    assert signal.signal_id == full_signal_data["signal_id"]


def test_tradesignal_missing_required_fields(minimal_signal_data: dict[str, Any]) -> None:
    """Test that missing required fields raise ValidationError."""
    required_fields = ["symbol", "signal_type", "side", "price", "exchange"]
    for field in required_fields:
        invalid_data = minimal_signal_data.copy()
        del invalid_data[field]
        with pytest.raises(ValidationError, match="Field required"):
            TradeSignal(**invalid_data)


def test_tradesignal_extra_fields_forbidden(minimal_signal_data: dict[str, Any]) -> None:
    """Test that extra fields are forbidden."""
    invalid_data = minimal_signal_data.copy()
    invalid_data["extra_field"] = "not allowed"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        TradeSignal(**invalid_data)


# --- Field Validation Tests (Types, Formats, Constraints) ---


@pytest.mark.parametrize(
    "field, value, error_match",
    [
        # String validations
        ("symbol", "", r"symbol.*String should not be empty"),
        ("symbol", None, r"symbol.*cannot be None"),
        ("source_strategy", " ", r"source_strategy.*String should not be empty"),
        (
            "source_strategy",
            "a" * 100,
            r"source_strategy.*ensure this value has at most 64 characters",
        ),
        # Exchange validation (str)
        ("exchange", "", r"exchange.*String should not be empty"),
        ("exchange", "a" * 100, r"exchange.*ensure this value has at most 64 characters"),
        # Decimal validations (Positive required)
        ("price", "0", r"price.*Input should be greater than 0"),
        ("price", "-1.0", r"price.*Input should be greater than 0"),
        ("price", Decimal("NaN"), r"price.*Must be finite"),
        ("price", "invalid", r"price.*Cannot convert 'invalid' to Decimal"),
        # Decimal validations (Optional, Positive if set)
        ("quantity", "0", r"quantity.*Input should be greater than 0"),
        ("quantity", Decimal("Infinity"), r"quantity.*Must be finite if provided"),
        ("stop_loss", "-50000", r"stop_loss.*Input should be greater than 0"),
        ("take_profit", "invalid", r"take_profit.*Cannot convert 'invalid' to Decimal"),
        # Float validation (Optional)
        ("confidence", "not a float", r"confidence.*Invalid float value"),
        ("confidence", [1.0], r"confidence.*Invalid float value"),
        # Datetime validation (Optional)
        ("expiration", "not a datetime", r"expiration.*Cannot parse ISO datetime string"),
        ("expiration", ["a"], r"expiration.*Unsupported datetime type"),
        # Enum validation
        ("signal_type", "UNKNOWN", r"signal_type.*Input should be .*SignalType"),
        ("side", 1, r"side.*Input should be .*BUY.* or .*SELL"),
    ],
)
def test_tradesignal_invalid_field_values(
    minimal_signal_data: dict[str, Any],
    field: str,
    value: str | float | Decimal | list[Any] | None,
    error_match: str,
) -> None:
    """Test various invalid field inputs raise appropriate ValidationErrors."""
    invalid_data = minimal_signal_data.copy()
    invalid_data[field] = value
    with pytest.raises((ValidationError, ValueError, TypeError)):
        TradeSignal(**invalid_data)


def test_tradesignal_exchange_list_validation(minimal_signal_data: dict[str, Any]) -> None:
    """Test validation specific to the 'exchange' field when it's a list."""
    # Valid list
    valid_data = minimal_signal_data.copy()
    valid_data["exchange"] = ["hyperliquid", "backpack"]
    signal = TradeSignal(**valid_data)
    assert signal.exchange == ["hyperliquid", "backpack"]

    # Empty list
    invalid_data_empty = minimal_signal_data.copy()
    invalid_data_empty["exchange"] = []
    with pytest.raises(ValueError, match="exchange list cannot be empty"):
        TradeSignal(**invalid_data_empty)

    # List with non-string
    invalid_data_type = minimal_signal_data.copy()
    invalid_data_type["exchange"] = ["hyperliquid", 123]
    with pytest.raises(TypeError, match="exchange list item 1 must be a string"):
        TradeSignal(**invalid_data_type)

    # List with empty string
    invalid_data_content = minimal_signal_data.copy()
    invalid_data_content["exchange"] = ["hyperliquid", ""]
    with pytest.raises(ValueError, match=r"exchange\[1\].*String cannot be empty"):
        TradeSignal(**invalid_data_content)


# --- Parsing Logic Tests ---


def test_tradesignal_decimal_parsing(minimal_signal_data: dict[str, Any]) -> None:
    """Test successful parsing of various valid decimal inputs."""
    data = minimal_signal_data.copy()
    data["price"] = "60000.5"
    data["quantity"] = "1.23"
    data["stop_loss"] = 58000  # Test int
    data["take_profit"] = 62000.0  # Test float
    signal = TradeSignal(**data)
    assert signal.price == Decimal("60000.5")
    assert signal.quantity == Decimal("1.23")
    assert signal.stop_loss == Decimal("58000")
    assert signal.take_profit == Decimal("62000.0")


def test_tradesignal_float_parsing(minimal_signal_data: dict[str, Any]) -> None:
    """Test successful parsing of various valid float inputs for confidence."""
    data = minimal_signal_data.copy()
    data["confidence"] = "0.75"
    signal1 = TradeSignal(**data)
    assert signal1.confidence == 0.75

    data["confidence"] = 0.9  # Test float input
    signal2 = TradeSignal(**data)
    assert signal2.confidence == 0.9

    data["confidence"] = 1  # Test int input
    signal3 = TradeSignal(**data)
    assert signal3.confidence == 1.0


def test_tradesignal_datetime_parsing(minimal_signal_data: dict[str, Any]) -> None:
    """Test successful parsing of various valid datetime inputs."""
    now_dt = datetime.now(UTC)
    now_iso = now_dt.isoformat()
    now_timestamp = now_dt.timestamp()

    data = minimal_signal_data.copy()
    data["expiration"] = now_iso
    signal1 = TradeSignal(**data)
    assert isinstance(signal1.expiration, datetime)
    # Precision differences might occur, compare with tolerance or check timezone
    assert signal1.expiration.tzinfo == UTC
    assert abs((signal1.expiration - now_dt).total_seconds()) < 1

    data["expiration"] = now_timestamp
    signal2 = TradeSignal(**data)
    assert isinstance(signal2.expiration, datetime)
    assert signal2.expiration.tzinfo == UTC
    assert abs((signal2.expiration - now_dt).total_seconds()) < 1

    data["expiration"] = now_dt  # Direct datetime object
    signal3 = TradeSignal(**data)
    assert signal3.expiration == now_dt


# --- is_valid() Method Tests ---


def test_tradesignal_is_valid(minimal_signal_data: dict[str, Any]) -> None:
    """Test the is_valid() method based on expiration."""
    now = datetime.now(UTC)

    # No expiration - always valid
    signal_no_expiry = TradeSignal(**minimal_signal_data)
    assert signal_no_expiry.expiration is None
    assert signal_no_expiry.is_valid() is True

    # Expiration in the future - valid
    future_expiry = now + timedelta(minutes=1)
    signal_future = TradeSignal(**minimal_signal_data | {"expiration": future_expiry})
    assert signal_future.is_valid() is True

    # Expiration in the past - invalid
    past_expiry = now - timedelta(minutes=1)
    signal_past = TradeSignal(**minimal_signal_data | {"expiration": past_expiry})
    assert signal_past.is_valid() is False

    # Expiration exactly now - should be invalid (strictly less than)
    signal_now = TradeSignal(**minimal_signal_data | {"expiration": now})
    assert signal_now.is_valid() is False


# --- Mutability Tests ---


def test_tradesignal_mutability(minimal_signal_data: dict[str, Any]) -> None:
    """Test that fields can be modified after creation and validation runs."""
    signal = TradeSignal(**minimal_signal_data)

    # Modify validly
    signal.quantity = Decimal("0.1")
    assert signal.quantity == Decimal("0.1")
    signal.source_strategy = "NewStrat"
    assert signal.source_strategy == "NewStrat"
    signal.confidence = 0.99
    assert signal.confidence == 0.99
    signal.exchange = ["backpack"]
    assert signal.exchange == ["backpack"]

    # Modify invalidly - Constraint (gt=0)
    with pytest.raises(ValidationError):
        signal.quantity = Decimal("-1")

    # Modify invalidly - Type
    with pytest.raises(ValidationError, match=r"price.*Cannot convert .* to Decimal"):
        signal.price = "not a price"  # type: ignore[assignment]

    # Modify invalidly - Exchange format
    with pytest.raises(TypeError, match=r"exchange.*must be a string or a list of strings"):
        signal.exchange = 123  # type: ignore[assignment]

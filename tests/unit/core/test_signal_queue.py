"""
Tests for the Priority Signal Queue functionality.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch  # Import patch
from uuid import UUID

import pytest

from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.utils.config import Config

# Import BreakerState for mocking states
from cyberdelta.validation.circuit_breaker import BreakerState, CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity


@pytest.fixture
def mock_config() -> MagicMock:
    """Fixture for a mock configuration object."""
    mock = MagicMock(spec=Config)
    mock_storage: dict[str, Any] = {}
    default_config_values = {
        "default_signal_expiration_seconds": 60.0,
        "max_signal_queue_size": 100,
        "queue_cleanup_interval": 5.0,
    }

    # Define a regular function for the side effect with type hints
    def mock_get_side_effect(key: str, default: Any = None) -> Any:
        # Check mock_storage first, then fallback to defaults, then to the provided default
        return mock_storage.get(key, default_config_values.get(key, default))

    # Define the mock_set function
    def mock_set(key: str, value: Any) -> None:
        mock_storage[key] = value

    # Assign the functions to the mock object's methods
    mock.get.side_effect = mock_get_side_effect
    mock.set = mock_set  # Keep the set method

    # Populate mock_storage with defaults so initial get works as expected
    mock_storage.update(default_config_values)

    return mock


@pytest.fixture
def mock_circuit_breaker() -> MagicMock:
    """Fixture for a mock CircuitBreakerSystem."""
    mock_system = MagicMock(spec=CircuitBreakerSystem)
    # Mock the get_exchange_breaker to return another mock
    mock_breaker_instance = MagicMock()
    mock_breaker_instance.state = BreakerState.CLOSED  # Default state
    mock_breaker_instance.trip_reason = None
    mock_system.get_exchange_breaker.return_value = mock_breaker_instance
    # Mock can_execute to return True by default
    mock_system.can_execute.return_value = (True, None)
    return mock_system


@pytest.fixture
def signal_queue(mock_config: Config, mock_circuit_breaker: MagicMock) -> PrioritySignalQueue:
    """Fixture for a PrioritySignalQueue instance with mocks."""
    # Pass the mock circuit breaker during initialization
    return PrioritySignalQueue(mock_config, mock_circuit_breaker)


@pytest.fixture
def sample_signal() -> TradeSignal:
    """Sample trade signal for testing."""
    now = datetime.now(UTC)  # Use UTC
    return TradeSignal(
        timestamp=now,
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000"),
        quantity=Decimal("1"),
        source_strategy="test_strategy",
        metadata={"utility_score": 0.8},
        exchange="mock_exchange",  # String is valid
    )


@pytest.fixture
def sample_opportunity() -> ArbitrageOpportunity:
    """Fixture to create a sample ArbitrageOpportunity."""
    now = datetime.now(UTC)
    return ArbitrageOpportunity(
        symbol="ETH/USDT",
        long_exchange="exA",
        short_exchange="exB",
        long_price=Decimal("2000"),
        short_price=Decimal("1995"),
        long_funding_rate=Decimal("0.0001"),
        short_funding_rate=Decimal("-0.0001"),
        net_funding_differential=Decimal("-0.0002"),
        timestamp=now,
        utility_score=0.9,
        expected_profit=Decimal("1.5"),
        basis_volatility=0.0005,
        confidence_score=0.85,
    )


# --- Helper Function ---
def create_test_signal(
    symbol: str,
    score: float,
    price: Decimal,  # Added required price
    signal_type: SignalType = SignalType.ENTER_LONG,
    side: OrderSide = OrderSide.BUY,
    quantity: Decimal | None = Decimal("1.0"),
    expiration_offset: int = 60,  # seconds
    exchange_name: str = "mock_exchange",
    source_strategy: str = "test_strategy",
    exchange_pair: tuple[str, str] | None = None,
) -> TradeSignal:
    """Helper function to create a test signal with a utility score."""
    now = datetime.now(UTC)  # Use UTC
    expiration = now + timedelta(seconds=expiration_offset)
    metadata = {"utility_score": score}
    if exchange_pair:
        # Pyright might complain here as dict value is Any, ignoring.
        metadata["long_exchange"] = exchange_pair[0]  # type: ignore
        metadata["short_exchange"] = exchange_pair[1]  # type: ignore

    return TradeSignal(
        timestamp=now,
        symbol=symbol,
        signal_type=signal_type,
        side=side,
        price=price,
        quantity=quantity,
        exchange=exchange_name,
        source_strategy=source_strategy,
        expiration=expiration,
        metadata=metadata,
    )


# --- Basic Queue Tests ---


def test_initialization(mock_config: Config) -> None:
    """Test queue initialization."""
    queue = PrioritySignalQueue(mock_config)
    assert queue.counter == 0
    assert queue.default_expiration_seconds == 60
    assert queue.max_queue_size == 100
    assert queue.cleanup_interval == 5


@pytest.mark.asyncio
async def test_add_signal(signal_queue: PrioritySignalQueue, sample_signal: TradeSignal) -> None:
    """Test adding a valid signal to the queue asynchronously."""
    result = await signal_queue.add_signal(sample_signal)
    assert result is True
    assert await signal_queue.count() == 1
    assert not await signal_queue.is_empty()

    # Check queue contents asynchronously if possible (using get_signals)
    signals = await signal_queue.get_signals()
    assert len(signals) == 1
    signal = signals[0]
    assert signal.symbol == "BTC/USDT"
    assert signal.expiration is not None
    assert signal.exchange == "mock_exchange"
    # Check that the signal_id is a valid UUID string
    try:
        UUID(signal.signal_id, version=4)
    except ValueError:
        pytest.fail("signal_id is not a valid UUID")


@pytest.mark.asyncio
async def test_add_signal_with_circuit_breaker_open(
    mock_config: Config, mock_circuit_breaker: MagicMock, sample_signal: TradeSignal
) -> None:
    """Test adding a signal is rejected when the relevant circuit breaker is OPEN."""
    # Set the mock can_execute to return False for the target exchange
    target_exchange = "reject_exchange"
    sample_signal.exchange = target_exchange
    mock_circuit_breaker.can_execute.side_effect = (
        lambda ex_id, sym: (False, "Test trip") if ex_id == target_exchange else (True, None)
    )

    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)
    result = await queue.add_signal(sample_signal)

    # Assert that can_execute was called correctly
    mock_circuit_breaker.can_execute.assert_called_with(target_exchange, sample_signal.symbol)
    assert result is False, "Signal should be rejected when relevant breaker is OPEN"
    assert await queue.count() == 0


@pytest.mark.asyncio
async def test_add_signal_with_circuit_breaker_closed(
    signal_queue: PrioritySignalQueue, mock_circuit_breaker: MagicMock, sample_signal: TradeSignal
) -> None:
    """Test adding a signal succeeds when the relevant circuit breaker is CLOSED."""
    # Ensure the mock can_execute returns True (default fixture behavior)
    target_exchange = "allow_exchange"
    sample_signal.exchange = target_exchange

    # Reset can_execute mock side effect if set in other tests
    # The fixture already sets return_value=(True, None)
    mock_circuit_breaker.can_execute.side_effect = None
    mock_circuit_breaker.can_execute.return_value = (True, None)

    result = await signal_queue.add_signal(sample_signal)

    assert result is True, "Signal should be added when relevant breaker is CLOSED"
    assert await signal_queue.count() == 1
    # Assert can_execute was called, not get_exchange_breaker
    mock_circuit_breaker.can_execute.assert_called_with(target_exchange, sample_signal.symbol)


@pytest.mark.asyncio
async def test_add_from_opportunity(
    signal_queue: PrioritySignalQueue, sample_opportunity: ArbitrageOpportunity
) -> None:
    """Test creating and adding a signal from an arbitrage opportunity asynchronously."""
    signal = await signal_queue.add_from_opportunity(
        opportunity=sample_opportunity,
        strategy_name="funding_arb_strategy",
    )

    assert signal is not None
    assert await signal_queue.count() == 1
    added_signal = await signal_queue.peek_next_signal()
    assert added_signal is not None
    assert added_signal.source_strategy == "funding_arb_strategy"
    assert added_signal.symbol == "ETH/USDT"
    assert added_signal.metadata is not None
    assert added_signal.metadata["utility_score"] == 0.9
    assert added_signal.metadata["long_exchange"] == "exA"
    assert added_signal.metadata["short_exchange"] == "exB"
    # Check that the exchange field is populated correctly (e.g., list of involved exchanges)
    assert isinstance(added_signal.exchange, list)
    assert set(added_signal.exchange) == {"exA", "exB"}


@pytest.mark.asyncio
async def test_get_next_signal(
    signal_queue: PrioritySignalQueue, sample_signal: TradeSignal
) -> None:
    """Test getting the next signal removes it from the queue asynchronously."""
    await signal_queue.add_signal(sample_signal)
    assert await signal_queue.count() == 1
    retrieved_signal = await signal_queue.get_next_signal()

    assert retrieved_signal == sample_signal
    assert await signal_queue.count() == 0
    assert await signal_queue.is_empty()


@pytest.mark.asyncio
async def test_peek_next_signal(
    signal_queue: PrioritySignalQueue, sample_signal: TradeSignal
) -> None:
    """Test peeking at the next signal without removing it asynchronously."""
    await signal_queue.add_signal(sample_signal)
    assert await signal_queue.count() == 1
    peeked_signal = await signal_queue.peek_next_signal()

    assert peeked_signal == sample_signal
    assert await signal_queue.count() == 1
    assert not await signal_queue.is_empty()


@pytest.mark.asyncio
async def test_get_signals(signal_queue: PrioritySignalQueue) -> None:
    """Test getting signals by symbol or all signals asynchronously."""
    now_utc = datetime.now(UTC)
    signal1 = TradeSignal(
        timestamp=now_utc,
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000"),
        quantity=Decimal("1"),
        source_strategy="test_strategy_1",
        exchange="mock_exchange1",
        metadata={"utility_score": 0.8},
    )
    signal2 = TradeSignal(
        timestamp=now_utc,
        symbol="ETH/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("3000"),
        quantity=Decimal("1"),
        source_strategy="test_strategy_2",
        exchange="mock_exchange2",
        metadata={"utility_score": 0.7},
    )

    await signal_queue.add_signal(signal1)
    await signal_queue.add_signal(signal2)

    # Get signals for BTC/USDT
    btc_signals = await signal_queue.get_signals(symbol="BTC/USDT")
    assert len(btc_signals) == 1
    assert btc_signals[0].symbol == "BTC/USDT"

    # Get all signals
    all_signals = await signal_queue.get_signals()
    assert len(all_signals) == 2
    # Order depends on priority (utility_score)
    assert {s.symbol for s in all_signals} == {"BTC/USDT", "ETH/USDT"}


@pytest.mark.asyncio
async def test_count(signal_queue: PrioritySignalQueue, sample_signal: TradeSignal) -> None:
    """Test counting signals in the queue asynchronously."""
    assert await signal_queue.count() == 0
    assert await signal_queue.is_empty()

    await signal_queue.add_signal(sample_signal)
    assert await signal_queue.count() == 1
    assert not await signal_queue.is_empty()

    await signal_queue.get_next_signal()
    assert await signal_queue.count() == 0
    assert await signal_queue.is_empty()


@pytest.mark.asyncio
async def test_clear(signal_queue: PrioritySignalQueue, sample_signal: TradeSignal) -> None:
    """Test clearing the queue asynchronously."""
    await signal_queue.add_signal(sample_signal)
    assert await signal_queue.count() == 1
    await signal_queue.clear()
    assert await signal_queue.count() == 0
    assert await signal_queue.is_empty()


# --- Advanced Queue Logic Tests ---


# Note: The test `test_clean_expired_signals` below uses direct datetime patching.
# It's kept for reference but can be tricky.
# `test_signal_expiration_logic` and `test_clean_expired_signals_with_helper` are preferred.
@pytest.mark.xfail(reason="Patching datetime can be complex and lead to subtle issues.")
@patch("cyberdelta.core.signal_queue.datetime")
def test_clean_expired_signals_direct_patch(
    mock_dt: MagicMock, mock_config: Config, mock_circuit_breaker: MagicMock
) -> None:
    """Test cleaning expired signals from the queue (direct datetime patch)."""
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)

    # Define fixed points in time for clarity
    time_now = datetime(2023, 10, 27, 12, 0, 0, tzinfo=UTC)
    time_expired = time_now - timedelta(seconds=70)  # Before default expiry window
    time_valid = time_now - timedelta(seconds=30)  # Within default expiry window
    explicit_expiry_past = time_now - timedelta(seconds=1)
    explicit_expiry_future = time_now + timedelta(minutes=5)

    mock_dt.now.return_value = time_now  # Set the mocked 'now'
    mock_dt.UTC = UTC  # Ensure the mock datetime object has UTC

    # Signal 1: Should expire based on default (timestamp too old)
    signal_default_expired = TradeSignal(
        timestamp=time_expired,
        symbol="DEF_EXP",
        price=Decimal(1),
        exchange="ex",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,  # Added defaults
    )
    # Signal 2: Should be valid based on default (timestamp recent)
    signal_default_valid = TradeSignal(
        timestamp=time_valid,
        symbol="DEF_VAL",
        price=Decimal(1),
        exchange="ex",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,  # Added defaults
    )
    # Signal 3: Explicitly expired
    signal_explicit_expired = TradeSignal(
        timestamp=time_valid,
        symbol="EXP_EXP",
        price=Decimal(1),
        exchange="ex",
        expiration=explicit_expiry_past,
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,  # Added defaults
    )
    # Signal 4: Explicitly valid
    signal_explicit_valid = TradeSignal(
        timestamp=time_valid,
        symbol="EXP_VAL",
        price=Decimal(1),
        exchange="ex",
        expiration=explicit_expiry_future,
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,  # Added defaults
    )

    queue.add_signal(signal_default_expired)
    queue.add_signal(signal_default_valid)
    queue.add_signal(signal_explicit_expired)
    queue.add_signal(signal_explicit_valid)

    assert queue.count() == 4

    # Trigger cleanup by attempting to get a signal - uses the mocked datetime.now() internally
    _ = queue.get_next_signal()  # We expect signal_default_valid or signal_explicit_valid
    # The internal cleanup runs *before* returning the signal.

    # Assertions
    # One signal was retrieved, and two expired signals were cleaned.
    assert queue.count() == 1  # Only the other valid signal should remain
    remaining_symbols = {s.symbol for s in queue.get_signals()}
    assert len(remaining_symbols) == 1
    assert remaining_symbols.issubset({"DEF_VAL", "EXP_VAL"})  # Check it's one of the valid ones


@pytest.mark.asyncio  # Mark test as async
async def test_trim_queue(mock_config: Config, mock_circuit_breaker: MagicMock) -> None:
    """Test trimming the queue when it exceeds the maximum size."""
    # Set max size low for testing using the Config object's set method
    mock_config.set("max_signal_queue_size", 3)
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)

    # Create signals using the helper
    signal1 = create_test_signal(symbol="S1", score=0.1, price=Decimal("10"))
    signal2 = create_test_signal(symbol="S2", score=0.9, price=Decimal("10"))  # Highest
    signal3 = create_test_signal(symbol="S3", score=0.5, price=Decimal("10"))
    signal4 = create_test_signal(symbol="S4", score=0.3, price=Decimal("10"))  # Lowest to be added

    await queue.add_signal(signal1)  # Score 0.1 - Await async call
    await queue.add_signal(signal2)  # Score 0.9 - Await async call
    await queue.add_signal(signal3)  # Score 0.5 - Await async call

    assert await queue.count() == 3  # Await async count

    # Adding the 4th signal should trigger trim and remove the lowest priority (signal1)
    result = await queue.add_signal(signal4)  # Score 0.3 - Await async call

    assert result is True
    assert await queue.count() == 3  # Still max size - Await async count

    # Check which signals remain (should be S2, S3, S4)
    remaining_signals = await queue.get_signals()  # Await async get_signals
    remaining_symbols = {s.symbol for s in remaining_signals}
    assert remaining_symbols == {"S2", "S3", "S4"}
    assert "S1" not in remaining_symbols


@patch("cyberdelta.core.models.trade_signal.datetime")  # Patch where datetime.now(UTC) is used
def test_signal_expiration_logic(
    mock_dt: MagicMock, mock_config: Config, mock_circuit_breaker: MagicMock
) -> None:
    """Test that signals expire correctly based on default and explicit times using patching."""
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)
    start_time = datetime.now(UTC)
    # Use .get() for safe access with a default
    _default_expiry_seconds_raw = mock_config.get("default_signal_expiration_seconds", 60)
    assert _default_expiry_seconds_raw is not None, (
        "Config default_signal_expiration_seconds cannot be None"
    )
    default_expiry_seconds: int = int(_default_expiry_seconds_raw)  # Ensure int type

    # Signal using default expiration (should be valid initially)
    signal_default = TradeSignal(
        symbol="DEFAULT/EXP",
        price=Decimal("100"),
        exchange="mock",
        timestamp=start_time,
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,  # Added defaults
    )
    queue.add_signal(signal_default)
    assert queue.count() == 1

    # Signal with explicit future expiration
    explicit_future = start_time + timedelta(minutes=5)
    signal_explicit = TradeSignal(
        symbol="EXPLICIT/EXP",
        price=Decimal("200"),
        exchange="mock",
        timestamp=start_time,
        expiration=explicit_future,
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,  # Added defaults
    )
    queue.add_signal(signal_explicit)
    assert queue.count() == 2

    # Mock time advancing just past the default expiration
    time_after_default_expiry = start_time + timedelta(seconds=default_expiry_seconds + 1)
    mock_dt.now.return_value = time_after_default_expiry
    mock_dt.UTC = UTC

    # Clean expired signals by trying to get the next one. This uses the mocked datetime.now()
    retrieved_after_default = queue.get_next_signal()

    # Default signal should be gone. Explicit should have been returned.
    assert retrieved_after_default is not None
    assert retrieved_after_default.symbol == "EXPLICIT/EXP"  # Default was older
    assert queue.count() == 0  # Explicit was returned, default cleaned

    # Add the explicit signal back to test its own expiration
    # Re-create it as mutable state might be unexpected
    signal_explicit_readd = TradeSignal(
        symbol="EXPLICIT/EXP",
        price=Decimal("200"),
        exchange="mock",
        timestamp=start_time,  # Original timestamp
        expiration=explicit_future,  # Original expiration
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
    )
    queue.add_signal(signal_explicit_readd)
    assert queue.count() == 1

    # Mock time advancing past the explicit expiration
    time_after_explicit_expiry = explicit_future + timedelta(seconds=1)
    mock_dt.now.return_value = time_after_explicit_expiry

    # Try get signal again. It should now be cleaned as expired.
    retrieved_after_explicit = queue.get_next_signal()
    # Queue should now be empty
    assert retrieved_after_explicit is None  # Expired signal is cleaned, not returned
    assert queue.count() == 0
    assert queue.is_empty()


@patch("cyberdelta.core.models.trade_signal.datetime")  # Patch where datetime.now(UTC) is used
def test_clean_expired_signals_with_helper(
    mock_dt: MagicMock, mock_config: Config, mock_circuit_breaker: MagicMock
) -> None:
    """Test cleaning expired signals using the helper and patching datetime."""
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)

    # Create signals with different expirations relative to a fixed future time
    future_now = datetime.now(UTC) + timedelta(minutes=10)

    # Signal that should NOT expire (explicit future expiry)
    signal_valid = create_test_signal(symbol="VALID/USDT", score=0.8, price=Decimal("100"))
    signal_valid.expiration = future_now + timedelta(seconds=120)

    # Signal that SHOULD expire (explicit past expiry)
    signal_expired = create_test_signal(symbol="EXPIRED/USDT", score=0.7, price=Decimal("200"))
    signal_expired.expiration = future_now - timedelta(seconds=30)

    queue.add_signal(signal_valid)
    queue.add_signal(signal_expired)
    assert queue.count() == 2

    # Patch datetime.now to return the fixed future time
    mock_dt.now.return_value = future_now
    mock_dt.UTC = UTC  # Ensure UTC is accessible

    # Trigger cleanup by getting the next signal
    retrieved_signal = queue.get_next_signal()

    # Assertions
    # get_next_signal should have cleaned the expired one and returned the valid one.
    assert retrieved_signal is not None
    assert retrieved_signal.symbol == "VALID/USDT"
    assert queue.count() == 0  # The valid one was retrieved
    assert queue.is_empty()


# --- Signal Specific Tests ---


def test_signal_creation(sample_signal: TradeSignal) -> None:
    """Test basic TradeSignal properties and methods."""
    assert isinstance(sample_signal.signal_id, str)
    try:
        UUID(sample_signal.signal_id, version=4)
    except ValueError:
        pytest.fail("signal_id is not a valid UUIDv4")

    # Default expiration is calculated on add, test is_valid with explicit values
    assert sample_signal.is_valid()  # Relies on internal check, less robust

    # Test expiration explicitly
    past_time = datetime.now(UTC) - timedelta(minutes=1)
    sample_signal.expiration = past_time
    assert not sample_signal.is_valid()

    future_time = datetime.now(UTC) + timedelta(minutes=1)
    sample_signal.expiration = future_time
    assert sample_signal.is_valid()

    sample_signal.expiration = None
    assert sample_signal.is_valid()  # None expiration means always valid


def test_add_signal_different_priorities(
    signal_queue: PrioritySignalQueue, mock_circuit_breaker: MagicMock
) -> None:
    """Test adding signals with different utility scores are retrieved in priority order."""
    # Ensure breaker allows signals
    mock_circuit_breaker.get_exchange_breaker.return_value.state = BreakerState.CLOSED

    # Create signals with varying utility scores using the helper
    signal_low = create_test_signal(symbol="LOW/USDT", score=0.1, price=Decimal("10"))
    signal_med = create_test_signal(symbol="MED/USDT", score=0.5, price=Decimal("20"))
    signal_high = create_test_signal(symbol="HIGH/USDT", score=0.9, price=Decimal("30"))

    # Add signals in arbitrary order
    signal_queue.add_signal(signal_med)
    signal_queue.add_signal(signal_low)
    signal_queue.add_signal(signal_high)

    assert signal_queue.count() == 3

    # Get signals - should come out in order of highest priority (score) first
    first = signal_queue.get_next_signal()
    second = signal_queue.get_next_signal()
    third = signal_queue.get_next_signal()

    assert first is not None and first.symbol == "HIGH/USDT"
    assert second is not None and second.symbol == "MED/USDT"
    assert third is not None and third.symbol == "LOW/USDT"
    assert signal_queue.is_empty()

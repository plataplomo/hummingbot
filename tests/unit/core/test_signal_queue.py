"""Tests for the Priority Signal Queue functionality."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch  # Import patch
from uuid import UUID

import pytest

from cyberdelta.config.config_models import AppSettings

# import pytest_asyncio # Remove if not needed elsewhere in the file
from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
from cyberdelta.core.signal_queue import PrioritySignalQueue

# Import BreakerState for mocking states
from cyberdelta.validation.circuit_breaker import BreakerState, CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity


@pytest.fixture
def mock_config() -> MagicMock:
    """Fixture for a mock AppSettings object."""
    mock = MagicMock(spec=AppSettings)
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
def signal_queue(mock_config: AppSettings, mock_circuit_breaker: MagicMock) -> PrioritySignalQueue:
    """Fixture for a PrioritySignalQueue instance with mocks."""
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)
    return queue


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
    """Create a test signal with a utility score."""
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


def test_initialization(mock_config: AppSettings) -> None:
    """Test that the signal queue initializes correctly."""
    queue = PrioritySignalQueue(mock_config)
    assert queue.app_settings == mock_config
    assert queue.circuit_breaker_system is None
    assert len(queue.signal_queue) == 0
    assert queue.counter == 0
    # Check the actual default values used by PrioritySignalQueue
    assert queue.default_expiration_seconds == 300.0  # 5 minutes default
    assert queue.max_queue_size == 100  # Default max queue size
    assert queue.cleanup_interval == 10.0  # Default cleanup interval


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
async def test_add_signal_idempotency(
    signal_queue: PrioritySignalQueue,
    sample_signal: TradeSignal,
) -> None:
    """Test idempotency of adding a signal to the queue."""
    result1 = await signal_queue.add_signal(sample_signal)
    assert result1 is True
    assert await signal_queue.count() == 1
    assert not await signal_queue.is_empty()

    # Add the same signal again - should now be rejected due to duplicate ID
    result2 = await signal_queue.add_signal(sample_signal)
    assert result2 is False  # Expect False because it's a duplicate
    assert await signal_queue.count() == 1  # Count should remain 1


@pytest.mark.asyncio
async def test_add_signal_full_queue(signal_queue: PrioritySignalQueue) -> None:
    """Test adding a signal when the queue is full (triggers trimming)."""
    # Create a new queue with a small max size for testing
    mock_config = MagicMock(spec=AppSettings)
    queue = PrioritySignalQueue(mock_config, signal_queue.circuit_breaker_system)
    # Set the max_queue_size directly on the queue instance for testing
    queue.max_queue_size = 3

    # Create signals using the helper
    signal1 = create_test_signal(symbol="S1", score=0.1, price=Decimal("10"))
    signal2 = create_test_signal(symbol="S2", score=0.9, price=Decimal("10"))  # Highest
    signal3 = create_test_signal(symbol="S3", score=0.5, price=Decimal("10"))

    await queue.add_signal(signal1)
    await queue.add_signal(signal2)
    await queue.add_signal(signal3)

    assert await queue.count() == 3

    # Add one more signal (should trigger trim)
    signal_extra = create_test_signal(
        symbol="EXTRA",
        score=0.05,
        price=Decimal("10"),
    )  # Lowest score, should be trimmed if logic is correct
    result = await queue.add_signal(signal_extra)
    assert result is True  # Add should be successful, even if it triggers trimming
    # The queue should be trimmed back to 3, and signal_extra (lowest score)
    # should be the one removed.
    assert await queue.count() == 3

    # Verify that signal_extra was NOT added (or was added then trimmed)
    # and signal1 (score 0.1) is still there.
    all_signals = await queue.get_signals(max_count=5)
    signal_symbols = {s.symbol for s in all_signals}
    assert "EXTRA" not in signal_symbols
    assert "S1" in signal_symbols  # S1 (0.1) should be kept over EXTRA (0.05)


@pytest.mark.asyncio
async def test_add_signal_with_circuit_breaker_open(
    mock_config: AppSettings,
    mock_circuit_breaker: MagicMock,
    sample_signal: TradeSignal,
) -> None:
    """Test adding a signal is rejected when the relevant circuit breaker is OPEN."""
    # Set the mock can_execute to return False for the target exchange
    target_exchange = "reject_exchange"
    sample_signal.exchange = target_exchange

    # Define the side_effect function with type hints
    def can_execute_side_effect(exchange_id: str, symbol: str) -> tuple[bool, str | None]:
        """Handle can execute side effect for testing."""
        if exchange_id == target_exchange:
            return (False, "Test trip")
        return (True, None)

    # Assign the correctly typed function to side_effect
    mock_circuit_breaker.can_execute.side_effect = can_execute_side_effect

    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)
    result = await queue.add_signal(sample_signal)

    # Assert that can_execute was called correctly
    mock_circuit_breaker.can_execute.assert_called_with(target_exchange, sample_signal.symbol)
    assert result is False, "Signal should be rejected when relevant breaker is OPEN"
    assert await queue.count() == 0


@pytest.mark.asyncio
async def test_add_signal_with_circuit_breaker_closed(
    signal_queue: PrioritySignalQueue,
    mock_circuit_breaker: MagicMock,
    sample_signal: TradeSignal,
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
    signal_queue: PrioritySignalQueue,
    sample_opportunity: ArbitrageOpportunity,
) -> None:
    """Test adding a signal derived from an arbitrage opportunity asynchronously."""
    result_signal = await signal_queue.add_from_opportunity(sample_opportunity, "test_strat")
    assert result_signal is not None
    assert isinstance(result_signal, TradeSignal)
    assert await signal_queue.count() == 1
    # Verify signal properties based on opportunity
    assert result_signal.symbol == sample_opportunity.symbol
    assert result_signal.side == OrderSide.BUY
    # Add more specific assertions if needed


@pytest.mark.asyncio
async def test_get_next_signal(
    signal_queue: PrioritySignalQueue,
    sample_signal: TradeSignal,
) -> None:
    """Test retrieving the highest priority signal asynchronously."""
    await signal_queue.add_signal(sample_signal)
    retrieved = await signal_queue.get_next_signal()
    assert retrieved == sample_signal
    assert await signal_queue.count() == 0


@pytest.mark.asyncio
async def test_peek_next_signal(
    signal_queue: PrioritySignalQueue,
    sample_signal: TradeSignal,
) -> None:
    """Test peeking at the highest priority signal without removing it asynchronously."""
    await signal_queue.add_signal(sample_signal)
    retrieved = await signal_queue.peek_next_signal()
    assert retrieved == sample_signal
    assert await signal_queue.count() == 1


@pytest.mark.asyncio
async def test_get_signals(signal_queue: PrioritySignalQueue) -> None:
    """Test retrieving multiple signals, sorted by priority."""
    signal1 = create_test_signal(symbol="S1", score=0.1, price=Decimal("10"))
    signal2 = create_test_signal(symbol="S2", score=0.9, price=Decimal("10"))  # Highest
    signal3 = create_test_signal(symbol="S3", score=0.5, price=Decimal("10"))

    await signal_queue.add_signal(signal1)
    await signal_queue.add_signal(signal2)
    await signal_queue.add_signal(signal3)

    # Get all signals (default max_count is high enough)
    signals = await signal_queue.get_signals()
    assert len(signals) == 3
    # Verify order - highest score first
    assert signals[0].symbol == "S2"
    assert signals[1].symbol == "S3"
    assert signals[2].symbol == "S1"

    # Get signals with max_count
    signals_limited = await signal_queue.get_signals(max_count=2)
    assert len(signals_limited) == 2
    assert signals_limited[0].symbol == "S2"
    assert signals_limited[1].symbol == "S3"

    # Get signals by symbol
    signal4 = create_test_signal(symbol="S2", score=0.8, price=Decimal("10"))  # Another S2
    await signal_queue.add_signal(signal4)
    signals_s2 = await signal_queue.get_signals(symbol="S2")
    assert len(signals_s2) == 2
    assert all(s.symbol == "S2" for s in signals_s2)
    # Scores should be 0.9 and 0.8, order depends on internal tie-breaking (counter)
    # Safe access to metadata
    assert {s.metadata["utility_score"] for s in signals_s2 if s.metadata is not None} == {0.9, 0.8}


@pytest.mark.asyncio
async def test_count(signal_queue: PrioritySignalQueue, sample_signal: TradeSignal) -> None:
    """Test the count method asynchronously."""
    assert await signal_queue.count() == 0
    await signal_queue.add_signal(sample_signal)
    assert await signal_queue.count() == 1


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
@pytest.mark.asyncio  # Mark as async
async def test_clean_expired_signals_direct_patch(
    mock_config: MagicMock,
    mock_circuit_breaker: MagicMock,
) -> None:  # Needs to be async
    """Test cleaning expired signals using patched datetime with context managers.

    This test verifies that expired signals are properly removed from the queue.
    Decorators were replaced by context managers for better test isolation.
    """
    real_start_time = datetime.now(UTC)
    future_time_for_expirations = real_start_time + timedelta(seconds=100)

    # Use context managers for patching
    with (
        patch("cyberdelta.core.signal_queue.datetime") as mock_dt_sq,
        patch("cyberdelta.core.models.trade_signal.datetime") as mock_dt_ts,
    ):
        # Configure mocks
        mock_dt_sq.now = MagicMock()
        mock_dt_ts.now = mock_dt_sq.now  # Point ts mock to sq mock's now
        mock_dt_sq.UTC = UTC
        mock_dt_ts.UTC = UTC

        # *** Set initial mock time BEFORE queue instantiation ***
        mock_dt_sq.now.return_value = real_start_time

        # Override config values for the test
        def specific_get_for_cleanup_test(key: str, default: object = None) -> object:
            """Handle specific get for cleanup test."""
            if key == "queue_cleanup_interval":
                return 1.0  # Short interval for testing
            if key == "default_signal_expiration_seconds":
                return 60.0
            if key == "max_signal_queue_size":
                return 100
            # Fallback for mock_storage (less ideal but for existing xfail structure)
            if hasattr(mock_config, "mock_storage") and isinstance(mock_config.mock_storage, dict):
                return mock_config.mock_storage.get(key, default)
            return default

        mock_config.get.side_effect = specific_get_for_cleanup_test

        queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)
        try:
            # Assert that last_cleanup is initialized correctly
            assert isinstance(queue.last_cleanup, datetime)

            # Set the main operational time for the test
            mock_dt_sq.now.return_value = future_time_for_expirations

            # Create signals
            signal_def_exp = create_test_signal("DEF_EXP", 0.5, Decimal("1"), expiration_offset=-10)
            signal_def_val = create_test_signal("DEF_VAL", 0.6, Decimal("2"), expiration_offset=120)
            signal_exp_exp = create_test_signal(
                "EXP_EXP",
                0.7,
                Decimal("3"),
                expiration_offset=0,  # Explicitly expired
            )
            signal_exp_val = create_test_signal("EXP_VAL", 0.8, Decimal("4"), expiration_offset=180)

            # Add signals asynchronously
            await queue.add_signal(signal_def_exp)
            await queue.add_signal(signal_def_val)
            await queue.add_signal(signal_exp_exp)
            await queue.add_signal(signal_exp_val)

            assert await queue.count() == 4

            # Trigger cleanup by calling get_signals() which checks cleanup interval
            # Since cleanup_interval is 1.0s and we're using mocked time, this will trigger cleanup
            await queue.get_signals()  # This will trigger cleanup internally

            # Assertions after explicit cleanup
            current_signals = await queue.get_signals()
            assert len(current_signals) == 2, "Expected 2 valid signals after cleanup"
            symbols_remaining = {s.symbol for s in current_signals}
            assert symbols_remaining == {"DEF_VAL", "EXP_VAL"}

            # Verify cleanup happened (using the internal counter for simplicity in this test)
            # Note: Accessing _cleaned_count is not ideal practice outside testing.
            # assert queue._cleaned_count == 2 # This attribute doesn't exist, remove assertion

        finally:
            # Clean shutdown - allow any background tasks to complete naturally
            await asyncio.sleep(0.1)


@pytest.mark.asyncio  # Mark test as async
async def test_trim_queue(mock_config: AppSettings, mock_circuit_breaker: MagicMock) -> None:
    """Test trimming the queue when it exceeds the maximum size."""
    # Create a new queue and set max size low for testing
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)
    # Set the max_queue_size directly on the queue instance for testing
    queue.max_queue_size = 3

    # Create signals using the helper
    signal1 = create_test_signal(symbol="S1", score=0.1, price=Decimal("10"))
    signal2 = create_test_signal(symbol="S2", score=0.9, price=Decimal("10"))  # Highest
    signal3 = create_test_signal(symbol="S3", score=0.5, price=Decimal("10"))
    signal4 = create_test_signal(symbol="S4", score=0.3, price=Decimal("10"))  # Lowest to be added

    await queue.add_signal(signal1)  # Score 0.1 - Await async call
    await queue.add_signal(signal2)  # Score 0.9 - Await async call
    await queue.add_signal(signal3)  # Score 0.5 - Await async call

    assert await queue.count() == 3  # Await async count

    # Adding the 4th signal (signal4, score 0.3) should now succeed, and trim S1 (score 0.1)
    result = await queue.add_signal(signal4)  # Score 0.3 - Await async call

    assert result is True  # Add itself is successful
    assert await queue.count() == 3  # Still max size after trimming - Await async count

    # Verify the lowest priority signal (signal1, score 0.1) was removed
    remaining_signals = await queue.get_signals(max_count=5)  # Await async call
    remaining_symbols = {s.symbol for s in remaining_signals}
    assert "S1" not in remaining_symbols
    assert "S2" in remaining_symbols
    assert "S3" in remaining_symbols
    assert "S4" in remaining_symbols


@pytest.mark.asyncio  # Mark as async
async def test_signal_expiration_logic(
    mock_config: AppSettings,
    mock_circuit_breaker: MagicMock,
) -> None:  # Needs to be async to use await
    """Test signal expiration logic with explicit cleanup task management."""
    # Create queue and reduce expiration and cleanup times for faster testing
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)
    queue.default_expiration_seconds = 2.0
    queue.cleanup_interval = 1.0
    try:
        # Get current time (using a real datetime for the test logic's reference)
        real_current_time = datetime.now(UTC)

        # Patch datetime used by PrioritySignalQueue AND TradeSignal
        # This ensures that internal `now_val` in queue and `is_valid` in signal use mocked time
        with (
            patch("cyberdelta.core.signal_queue.datetime") as mock_dt_sq,
            patch("cyberdelta.core.models.trade_signal.datetime") as mock_dt_ts,
        ):
            # Configure mocks
            mock_dt_sq.now = MagicMock()
            mock_dt_ts.now = mock_dt_sq.now  # Point ts mock to sq mock's now
            mock_dt_sq.UTC = UTC
            mock_dt_ts.UTC = UTC
            # Set the initial time for both mocks
            mock_dt_sq.now.return_value = real_current_time

            # --- Test Case 1: Signal that should expire ---
            signal_to_expire = create_test_signal(
                symbol="BTC/USDT_EXP",
                score=0.7,
                price=Decimal("50001"),
                expiration_offset=1,  # Expires in 1 (mocked) second
                exchange_name="exchange1",
            )
            await queue.add_signal(signal_to_expire)
            assert await queue.count() == 1, "Signal_to_expire should be added"

            # --- Test Case 2: Signal that should NOT expire yet ---
            signal_not_to_expire = create_test_signal(
                symbol="BTC/USDT_VALID",
                score=0.8,
                price=Decimal("50002"),
                expiration_offset=10,  # Expires in 10 (mocked) seconds
                exchange_name="exchange2",
            )
            await queue.add_signal(signal_not_to_expire)
            assert await queue.count() == 2, "Signal_not_to_expire should be added"

            # Advance mocked time by 3 seconds (past expiration of signal_to_expire)
            mock_dt_sq.now.return_value = real_current_time + timedelta(seconds=3)

            # Allow cleanup task to run (queue_cleanup_interval is 1s)
            # The cleanup task uses the mocked time.
            await asyncio.sleep(1.5)  # Real sleep, cleanup task runs in background

            # Assertions
            current_signals_after_cleanup = await queue.get_signals()
            signal_ids_after_cleanup = {s.signal_id for s in current_signals_after_cleanup}

            assert signal_to_expire.signal_id not in signal_ids_after_cleanup, (
                "Expired signal should have been removed by cleanup task"
            )
            assert signal_not_to_expire.signal_id in signal_ids_after_cleanup, (
                "Valid signal should remain after cleanup task"
            )
            assert len(current_signals_after_cleanup) == 1, (
                f"Expected 1 signal after cleanup, got {len(current_signals_after_cleanup)}"
            )

            # --- Test Case 3: Explicitly clean and verify ---
            # Advance time further to ensure signal_not_to_expire also expires
            mock_dt_sq.now.return_value = real_current_time + timedelta(seconds=15)
            # Trigger cleanup by waiting for background cleanup task
            await asyncio.sleep(1.5)  # Wait for cleanup to occur automatically

            current_signals_after_manual_clean = await queue.get_signals()
            assert not current_signals_after_manual_clean, (
                "Queue should be empty after all signals expire and manual cleanup"
            )
            assert await queue.is_empty(), "Queue should be empty"

    finally:
        # Clean shutdown - allow any background tasks to complete naturally
        await asyncio.sleep(0.1)


@pytest.mark.asyncio  # Mark as async
async def test_clean_expired_signals_with_helper(
    mock_config: AppSettings,
    mock_circuit_breaker: MagicMock,
) -> None:  # Needs to be async
    """Test cleaning expired signals using the helper and patching datetime."""
    # Use real datetime for test setup and defining future points
    real_setup_time = datetime.now(UTC)
    future_now_for_expirations = real_setup_time + timedelta(minutes=10)

    # Patch datetime used by PrioritySignalQueue AND TradeSignal
    with (
        patch("cyberdelta.core.signal_queue.datetime") as mock_dt_sq,
        patch("cyberdelta.core.models.trade_signal.datetime") as mock_dt_ts,
    ):
        # Ensure both mocks behave identically
        mock_dt_sq.now = MagicMock()
        mock_dt_ts.now = mock_dt_sq.now  # Point ts mock to sq mock's now
        mock_dt_sq.UTC = UTC
        mock_dt_ts.UTC = UTC

        # *** IMPORTANT: Set the initial mock time BEFORE queue instantiation ***
        mock_dt_sq.now.return_value = (
            real_setup_time  # Ensures last_cleanup is initialized correctly
        )
        # mock_dt_ts.now.return_value is implicitly set too

        # Instantiate the queue *after* setting the initial mock time
        queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)
        # Verify queue properties if needed
        assert isinstance(queue.cleanup_interval, float), (
            f"cleanup_interval is not float after init: {type(queue.cleanup_interval)}"
        )
        assert isinstance(queue.last_cleanup, datetime), (
            f"last_cleanup should be datetime after init, got: {type(queue.last_cleanup)}"
        )

        # Create signals relative to the future time point
        signal_valid = create_test_signal(symbol="VALID/USDT", score=0.8, price=Decimal("100"))
        signal_valid.expiration = future_now_for_expirations + timedelta(seconds=120)

        signal_expired = create_test_signal(symbol="EXPIRED/USDT", score=0.7, price=Decimal("200"))
        signal_expired.expiration = future_now_for_expirations - timedelta(seconds=30)

        # *** Set the mock time to the future point BEFORE adding signals ***
        # This tests the cleanup logic that runs during add_signal
        mock_dt_sq.now.return_value = future_now_for_expirations

        await queue.add_signal(signal_valid)
        await queue.add_signal(signal_expired)

        # *** Explicitly trigger cleanup AFTER adding signals ***
        # Cleanup happens during queue operations when interval has passed
        # Trigger cleanup by calling get_signals() which checks cleanup interval
        await queue.get_signals()  # This will trigger cleanup internally

        # Assertions after explicit cleanup
        current_signals = await queue.get_signals()
        # Cleanup should have removed the expired one
        assert len(current_signals) == 1, (
            f"Expected 1 signal after explicit cleanup, found {len(current_signals)}"
        )


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


@pytest.mark.asyncio
async def test_add_signal_different_priorities(
    signal_queue: PrioritySignalQueue,
    mock_circuit_breaker: MagicMock,
) -> None:
    """Test adding signals with different priorities and retrieving them in order."""
    signal_low = create_test_signal(symbol="LOW", score=0.1, price=Decimal("10"))
    signal_high = create_test_signal(symbol="HIGH", score=0.9, price=Decimal("10"))
    signal_mid = create_test_signal(symbol="MID", score=0.5, price=Decimal("10"))

    await signal_queue.add_signal(signal_low)
    await signal_queue.add_signal(signal_high)
    await signal_queue.add_signal(signal_mid)

    assert await signal_queue.count() == 3

    # Retrieve signals and check order
    retrieved1 = await signal_queue.get_next_signal()
    assert retrieved1 is not None and retrieved1.symbol == "HIGH"

    retrieved2 = await signal_queue.get_next_signal()
    assert retrieved2 is not None and retrieved2.symbol == "MID"

    retrieved3 = await signal_queue.get_next_signal()
    assert retrieved3 is not None and retrieved3.symbol == "LOW"

    # Queue should be empty now
    assert await signal_queue.count() == 0
    assert await signal_queue.is_empty()

    # Test peeking doesn't remove the item
    await signal_queue.add_signal(signal_high)  # Add back the high priority one
    peeked = await signal_queue.peek_next_signal()
    assert peeked is not None and peeked.symbol == "HIGH"
    assert await signal_queue.count() == 1
    assert not await signal_queue.is_empty()

    # Ensure get_next_signal still retrieves it after peeking
    retrieved_after_peek = await signal_queue.get_next_signal()
    assert retrieved_after_peek is not None and retrieved_after_peek.symbol == "HIGH"
    assert await signal_queue.count() == 0
    assert await signal_queue.is_empty()

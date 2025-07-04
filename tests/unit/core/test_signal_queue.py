"""Tests for the Priority Signal Queue functionality."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock  # Import patch
from uuid import UUID

import pytest

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
from cyberdelta.core.signal_queue import PrioritySignalQueue

# Import BreakerState for mocking states
from cyberdelta.validation.circuit_breaker import BreakerState, CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity
from tests.fixtures.time_fixtures import FreezerProtocol


pytestmark = pytest.mark.timing

# Tests use timing operations so marked with timing marker


@pytest.fixture
def mock_config() -> MagicMock:
    """Fixture for a mock AppSettings object.

    Returns:
        Mock AppSettings instance for testing
    """
    return MagicMock(spec=AppSettings)


@pytest.fixture
def mock_circuit_breaker() -> MagicMock:
    """Fixture for a mock CircuitBreakerSystem.

    Returns:
        Mock CircuitBreakerSystem instance for testing
    """
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
    """Fixture for a PrioritySignalQueue instance with mocks.

    Returns:
        PrioritySignalQueue instance configured with mock dependencies
    """
    return PrioritySignalQueue(mock_config, mock_circuit_breaker)


@pytest.fixture
def sample_signal() -> TradeSignal:
    """Sample trade signal for testing.

    Returns:
        TradeSignal instance for test scenarios
    """
    now = datetime.now(UTC)  # Use UTC
    return TradeSignal(
        timestamp=now,
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal(50000),
        quantity=Decimal(1),
        source_strategy="test_strategy",
        metadata={"utility_score": 0.8},
        exchange="mock_exchange",  # String is valid
    )


@pytest.fixture
def sample_opportunity() -> ArbitrageOpportunity:
    """Fixture to create a sample ArbitrageOpportunity.

    Returns:
        ArbitrageOpportunity instance for testing
    """
    now = datetime.now(UTC)
    return ArbitrageOpportunity(
        symbol="ETH/USDT",
        long_exchange="exA",
        short_exchange="exB",
        long_price=Decimal(2000),
        short_price=Decimal(1995),
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
    base_time: datetime | None = None,  # Allow passing explicit time for mocked tests
) -> TradeSignal:
    """Create a test signal with a utility score.

    Returns:
        TradeSignal configured with the specified parameters
    """
    now = (
        base_time if base_time is not None else datetime.now(UTC)
    )  # Use provided time or current UTC
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
    signal1 = create_test_signal(symbol="S1", score=0.1, price=Decimal(10))
    signal2 = create_test_signal(symbol="S2", score=0.9, price=Decimal(10))  # Highest
    signal3 = create_test_signal(symbol="S3", score=0.5, price=Decimal(10))

    await queue.add_signal(signal1)
    await queue.add_signal(signal2)
    await queue.add_signal(signal3)

    assert await queue.count() == 3

    # Add one more signal (should trigger trim)
    signal_extra = create_test_signal(
        symbol="EXTRA",
        score=0.05,
        price=Decimal(10),
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
    # Set the target exchange and configure the signal
    target_exchange = "reject_exchange"
    sample_signal.exchange = target_exchange

    # Mock the circuit breaker methods that are actually called by the signal queue
    # The signal queue calls get_breaker for symbol-level breakers
    mock_symbol_breaker = MagicMock()
    mock_symbol_breaker.state = BreakerState.OPEN  # Set to actual OPEN state
    mock_symbol_breaker.trip_reason = "Test trip"

    def get_breaker_side_effect(breaker_name: str) -> MagicMock | None:
        """Mock get_breaker to return open breaker for target symbol.

        Returns:
            Mock breaker set to OPEN state or None for other breakers
        """
        if f"symbol_{sample_signal.symbol}_main" in breaker_name:
            return mock_symbol_breaker
        return None

    # Also mock get_exchange_breaker for exchange-level checks
    mock_circuit_breaker.get_exchange_breaker.return_value = None
    mock_circuit_breaker.get_breaker.side_effect = get_breaker_side_effect

    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)
    result = await queue.add_signal(sample_signal)

    # Assert that get_breaker was called correctly for symbol-level breaker
    expected_symbol_breaker_name = f"symbol_{sample_signal.symbol}_main"
    mock_circuit_breaker.get_breaker.assert_called_with(expected_symbol_breaker_name)
    assert result is False, "Signal should be rejected when relevant breaker is OPEN"
    assert await queue.count() == 0


@pytest.mark.asyncio
async def test_add_signal_with_circuit_breaker_closed(
    signal_queue: PrioritySignalQueue,
    mock_circuit_breaker: MagicMock,
    sample_signal: TradeSignal,
) -> None:
    """Test adding a signal succeeds when the relevant circuit breaker is CLOSED."""
    # Set the target exchange and configure the signal
    target_exchange = "allow_exchange"
    sample_signal.exchange = target_exchange

    # Mock the circuit breaker methods for CLOSED state
    mock_symbol_breaker = MagicMock()
    mock_symbol_breaker.state = BreakerState.CLOSED  # Set to CLOSED state

    mock_pair_breaker = MagicMock()
    mock_pair_breaker.state = BreakerState.CLOSED  # Set to CLOSED state

    def get_breaker_side_effect(breaker_name: str) -> MagicMock | None:
        """Mock get_breaker to return closed breaker for symbol and pair.

        Returns:
            Mock breaker set to CLOSED state or None for other breakers
        """
        if f"symbol_{sample_signal.symbol}_main" in breaker_name:
            return mock_symbol_breaker
        if f"pair_{target_exchange}_{sample_signal.symbol}_main" in breaker_name:
            return mock_pair_breaker
        return None

    # Also mock get_exchange_breaker for exchange-level checks
    mock_circuit_breaker.get_exchange_breaker.return_value = None
    mock_circuit_breaker.get_breaker.side_effect = get_breaker_side_effect

    result = await signal_queue.add_signal(sample_signal)

    assert result is True, "Signal should be added when relevant breaker is CLOSED"
    assert await signal_queue.count() == 1
    # Assert that get_breaker was called - it should be called multiple times for different breakers
    # The last call should be for the pair breaker
    expected_pair_breaker_name = f"pair_{target_exchange}_{sample_signal.symbol}_main"
    mock_circuit_breaker.get_breaker.assert_called_with(expected_pair_breaker_name)


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
    signal1 = create_test_signal(symbol="S1", score=0.1, price=Decimal(10))
    signal2 = create_test_signal(symbol="S2", score=0.9, price=Decimal(10))  # Highest
    signal3 = create_test_signal(symbol="S3", score=0.5, price=Decimal(10))

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
    signal4 = create_test_signal(symbol="S2", score=0.8, price=Decimal(10))  # Another S2
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
    frozen_time: FreezerProtocol,
) -> None:  # Needs to be async
    """Test cleaning expired signals using patched datetime with context managers.

    This test verifies that expired signals are properly removed from the queue.
    Decorators were replaced by context managers for better test isolation.
    """
    real_start_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
    future_time_for_expirations = real_start_time + timedelta(seconds=100)

    # Set initial time using pytest-freezer
    frozen_time.move_to(real_start_time)

    # Create queue with the mock config (signal queue uses hardcoded defaults, not config.get)
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)

    # Modify the queue's settings directly since it doesn't read from config
    queue.cleanup_interval = 1.0  # Short interval for testing
    queue.default_expiration_seconds = 60.0
    try:
        # Assert that last_cleanup is initialized correctly
        assert isinstance(queue.last_cleanup, datetime)

        # Set the main operational time for the test
        frozen_time.move_to(future_time_for_expirations)

        # Reset last_cleanup to align with frozen time so cleanup will be triggered
        queue.last_cleanup = real_start_time

        # Create signals using the mocked base time
        signal_def_exp = create_test_signal(
            "DEF_EXP",
            0.5,
            Decimal(1),
            expiration_offset=-10,
            base_time=real_start_time,
        )
        signal_def_val = create_test_signal(
            "DEF_VAL",
            0.6,
            Decimal(2),
            expiration_offset=120,
            base_time=real_start_time,
        )
        signal_exp_exp = create_test_signal(
            "EXP_EXP",
            0.7,
            Decimal(3),
            expiration_offset=0,  # Explicitly expired
            base_time=real_start_time,
        )
        signal_exp_val = create_test_signal(
            "EXP_VAL",
            0.8,
            Decimal(4),
            expiration_offset=180,
            base_time=real_start_time,
        )

        # Add signals asynchronously
        await queue.add_signal(signal_def_exp)
        await queue.add_signal(signal_def_val)
        await queue.add_signal(signal_exp_exp)
        await queue.add_signal(signal_exp_val)

        assert await queue.count() == 4

        # Trigger cleanup by adding a dummy signal which will trigger automatic cleanup
        # Since we've advanced time past the cleanup interval, cleanup will run automatically
        dummy_cleanup_signal = create_test_signal(
            symbol="CLEANUP_TRIGGER",
            score=0.1,
            price=Decimal(1),
            base_time=future_time_for_expirations,
        )
        await queue.add_signal(dummy_cleanup_signal)

        # Force cleanup by manipulating last_cleanup time to trigger cleanup
        queue.last_cleanup = future_time_for_expirations - timedelta(
            seconds=queue.cleanup_interval + 1
        )

        # Trigger a method that would call _cleanup_expired_signals_if_needed
        await queue.get_signals()

        # Assertions after cleanup
        current_signals = await queue.get_signals()
        # Should have dummy signal plus the 2 valid signals (DEF_VAL and EXP_VAL)
        assert len(current_signals) == 3, "Expected 3 signals after cleanup"
        symbols_remaining = {s.symbol for s in current_signals}
        assert "DEF_VAL" in symbols_remaining
        assert "EXP_VAL" in symbols_remaining
        assert "CLEANUP_TRIGGER" in symbols_remaining

        # Verify cleanup happened (using the internal counter for simplicity in this test)
        # Note: Accessing _cleaned_count is not ideal practice outside testing.

    finally:
        # No background tasks to wait for
        pass


@pytest.mark.asyncio  # Mark test as async
async def test_trim_queue(mock_config: AppSettings, mock_circuit_breaker: MagicMock) -> None:
    """Test trimming the queue when it exceeds the maximum size."""
    # Create a new queue and set max size low for testing
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)
    # Set the max_queue_size directly on the queue instance for testing
    queue.max_queue_size = 3

    # Create signals using the helper
    signal1 = create_test_signal(symbol="S1", score=0.1, price=Decimal(10))
    signal2 = create_test_signal(symbol="S2", score=0.9, price=Decimal(10))  # Highest
    signal3 = create_test_signal(symbol="S3", score=0.5, price=Decimal(10))
    signal4 = create_test_signal(symbol="S4", score=0.3, price=Decimal(10))  # Lowest to be added

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
    frozen_time: FreezerProtocol,
) -> None:  # Needs to be async to use await
    """Test signal expiration logic with explicit cleanup task management."""
    # Create queue and reduce expiration and cleanup times for faster testing
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)
    queue.default_expiration_seconds = 2.0
    queue.cleanup_interval = 1.0
    try:
        # Set current time using frozen_time
        real_current_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        frozen_time.move_to(real_current_time)

        # --- Test Case 1: Signal that should expire ---
        signal_to_expire = create_test_signal(
            symbol="BTC/USDT_EXP",
            score=0.7,
            price=Decimal(50001),
            expiration_offset=1,  # Expires in 1 (mocked) second
            exchange_name="exchange1",
        )
        await queue.add_signal(signal_to_expire)
        assert await queue.count() == 1, "Signal_to_expire should be added"

        # --- Test Case 2: Signal that should NOT expire yet ---
        signal_not_to_expire = create_test_signal(
            symbol="BTC/USDT_VALID",
            score=0.8,
            price=Decimal(50002),
            expiration_offset=10,  # Expires in 10 (mocked) seconds
            exchange_name="exchange2",
        )
        await queue.add_signal(signal_not_to_expire)
        assert await queue.count() == 2, "Signal_not_to_expire should be added"

        # Advance mocked time by 3 seconds (past expiration of signal_to_expire)
        frozen_time.move_to(real_current_time + timedelta(seconds=3))

        # There's no background cleanup task - cleanup only happens on add_signal
        # We need to trigger it manually or add a new signal
        # Let's add a dummy signal to trigger cleanup
        dummy_signal = create_test_signal(
            symbol="DUMMY",
            score=0.1,
            price=Decimal(1),
            base_time=real_current_time + timedelta(seconds=3),
        )
        await queue.add_signal(dummy_signal)

        # Assertions
        current_signals_after_cleanup = await queue.get_signals()
        signal_ids_after_cleanup = {s.signal_id for s in current_signals_after_cleanup}

        assert signal_to_expire.signal_id not in signal_ids_after_cleanup, (
            "Expired signal should have been removed by cleanup"
        )
        assert signal_not_to_expire.signal_id in signal_ids_after_cleanup, (
            "Valid signal should remain after cleanup"
        )
        # We added dummy signal so we should have 2 signals
        assert dummy_signal.signal_id in signal_ids_after_cleanup, "Dummy signal should be in queue"
        assert len(current_signals_after_cleanup) == 2, (
            f"Expected 2 signals after cleanup (valid + dummy), got "
            f"{len(current_signals_after_cleanup)}"
        )

        # --- Test Case 3: Explicitly clean and verify ---
        # Advance time further to ensure signal_not_to_expire also expires
        frozen_time.move_to(real_current_time + timedelta(seconds=15))
        # Trigger cleanup by adding another signal
        dummy_signal2 = create_test_signal(
            symbol="DUMMY2",
            score=0.1,
            price=Decimal(1),
            base_time=real_current_time + timedelta(seconds=15),
        )
        await queue.add_signal(dummy_signal2)

        current_signals_after_manual_clean = await queue.get_signals()
        # Both dummy signals should remain (they have default 60s expiration)
        assert len(current_signals_after_manual_clean) == 2, (
            f"Expected 2 signals (dummy + dummy2) after cleanup, got "
            f"{len(current_signals_after_manual_clean)}"
        )
        signal_ids_final = {s.signal_id for s in current_signals_after_manual_clean}
        assert dummy_signal.signal_id in signal_ids_final
        assert dummy_signal2.signal_id in signal_ids_final

    finally:
        # No background tasks to wait for
        pass


@pytest.mark.asyncio  # Mark as async
async def test_clean_expired_signals_with_helper(
    mock_config: AppSettings,
    mock_circuit_breaker: MagicMock,
    frozen_time: FreezerProtocol,
) -> None:  # Needs to be async
    """Test cleaning expired signals using the helper and patching datetime."""
    # Set initial time
    real_setup_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
    future_now_for_expirations = real_setup_time + timedelta(minutes=10)

    # Set initial time with frozen_time
    frozen_time.move_to(real_setup_time)

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
    signal_valid = create_test_signal(symbol="VALID/USDT", score=0.8, price=Decimal(100))
    signal_valid.expiration = future_now_for_expirations + timedelta(seconds=120)

    signal_expired = create_test_signal(symbol="EXPIRED/USDT", score=0.7, price=Decimal(200))
    signal_expired.expiration = future_now_for_expirations - timedelta(seconds=30)

    # *** Set the mock time to the future point BEFORE adding signals ***
    # This tests the cleanup logic that runs during add_signal
    frozen_time.move_to(future_now_for_expirations)

    # Reset last_cleanup to align with frozen time so cleanup will be triggered
    queue.last_cleanup = real_setup_time

    await queue.add_signal(signal_valid)
    await queue.add_signal(signal_expired)

    # *** Trigger cleanup by adding a dummy signal ***
    # This will trigger automatic cleanup since we've advanced time past cleanup interval
    dummy_trigger_signal = create_test_signal(
        symbol="TRIGGER_CLEANUP",
        score=0.1,
        price=Decimal(1),
        base_time=future_now_for_expirations,
    )
    await queue.add_signal(dummy_trigger_signal)

    # Force cleanup by manipulating last_cleanup time to trigger cleanup
    queue.last_cleanup = future_now_for_expirations - timedelta(seconds=queue.cleanup_interval + 1)

    # Trigger a method that would call _cleanup_expired_signals_if_needed
    await queue.get_signals()

    # Assertions after cleanup
    current_signals = await queue.get_signals()
    # Cleanup should have removed the expired one, leaving valid + dummy
    assert len(current_signals) == 2, (
        f"Expected 2 signals after cleanup, found {len(current_signals)}"
    )
    symbols_remaining = {s.symbol for s in current_signals}
    assert "VALID/USDT" in symbols_remaining
    assert "TRIGGER_CLEANUP" in symbols_remaining


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
    signal_low = create_test_signal(symbol="LOW", score=0.1, price=Decimal(10))
    signal_high = create_test_signal(symbol="HIGH", score=0.9, price=Decimal(10))
    signal_mid = create_test_signal(symbol="MID", score=0.5, price=Decimal(10))

    await signal_queue.add_signal(signal_low)
    await signal_queue.add_signal(signal_high)
    await signal_queue.add_signal(signal_mid)

    assert await signal_queue.count() == 3

    # Retrieve signals and check order
    retrieved1 = await signal_queue.get_next_signal()
    assert retrieved1 is not None
    assert retrieved1.symbol == "HIGH"

    retrieved2 = await signal_queue.get_next_signal()
    assert retrieved2 is not None
    assert retrieved2.symbol == "MID"

    retrieved3 = await signal_queue.get_next_signal()
    assert retrieved3 is not None
    assert retrieved3.symbol == "LOW"

    # Queue should be empty now
    assert await signal_queue.count() == 0
    assert await signal_queue.is_empty()

    # Test peeking doesn't remove the item
    await signal_queue.add_signal(signal_high)  # Add back the high priority one
    peeked = await signal_queue.peek_next_signal()
    assert peeked is not None
    assert peeked.symbol == "HIGH"
    assert await signal_queue.count() == 1
    assert not await signal_queue.is_empty()

    # Ensure get_next_signal still retrieves it after peeking
    retrieved_after_peek = await signal_queue.get_next_signal()
    assert retrieved_after_peek is not None
    assert retrieved_after_peek.symbol == "HIGH"
    assert await signal_queue.count() == 0
    assert await signal_queue.is_empty()

"""
Tests for the Priority Signal Queue functionality.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch  # Import patch

import pytest

from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.utils.config import Config

# Import BreakerState for mocking states
from cyberdelta.validation.circuit_breaker import BreakerState
from cyberdelta.validation.funding_data import ArbitrageOpportunity


@pytest.fixture
def mock_config() -> Config:
    """Mock configuration for testing."""
    return Config(
        {
            "default_signal_expiration_seconds": 60,
            "max_signal_queue_size": 100,
            "queue_cleanup_interval": 5,
        }
    )


@pytest.fixture
def mock_circuit_breaker() -> MagicMock:
    """Mock circuit breaker system for testing."""
    mock = MagicMock()
    # Default to all checks passing
    mock.check_exchange.return_value = True
    mock.check_symbol.return_value = True
    return mock


@pytest.fixture
def signal_queue(mock_config: Config) -> PrioritySignalQueue:
    """Mock signal queue for testing."""
    return PrioritySignalQueue(mock_config)


@pytest.fixture
def sample_signal() -> TradeSignal:
    """Sample trade signal for testing."""
    now = datetime.now()
    return TradeSignal(
        timestamp=now,
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000"),
        quantity=Decimal("1"),
        source_strategy="test_strategy",
        metadata={"utility_score": 0.8},
        exchange="mock_exchange",
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


def test_initialization(mock_config: Config) -> None:
    """Test queue initialization."""
    queue = PrioritySignalQueue(mock_config)

    assert queue.signal_queue == []
    assert queue.counter == 0
    assert queue.default_expiration_seconds == 60
    assert queue.max_queue_size == 100
    assert queue.cleanup_interval == 5


def test_add_signal(mock_config: Config, sample_signal: TradeSignal) -> None:
    """Test adding a signal to the queue."""
    queue = PrioritySignalQueue(mock_config)

    # Add signal
    result = queue.add_signal(sample_signal)

    assert result is True
    assert len(queue.signal_queue) == 1
    assert queue.counter == 1

    # Check queue contents
    priority, _, signal = queue.signal_queue[0]
    assert priority == -0.8  # Negative for max-heap
    assert signal.symbol == "BTC/USDT"
    assert signal.expiration is not None
    assert signal.exchange == "mock_exchange"


def test_add_signal_with_circuit_breaker(
    mock_config: Config, mock_circuit_breaker: MagicMock, sample_signal: TradeSignal
) -> None:
    """Test adding a signal with circuit breaker integration."""
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)

    # Simulate the relevant breaker being OPEN
    mock_breaker_instance = MagicMock()
    mock_breaker_instance.state = BreakerState.OPEN
    mock_breaker_instance.trip_reason = "Test trip"
    # Mock get_exchange_breaker to return this OPEN breaker for the relevant exchange
    # Infer exchange from signal symbol (e.g., assuming BTC/USDT implies 'binance'
    # if not in metadata)
    # We need a symbol mapper or assume metadata for a robust test, let's add metadata
    sample_signal.metadata = {"long_exchange": "test_exchange"}  # Add metadata
    mock_circuit_breaker.get_exchange_breaker.return_value = mock_breaker_instance

    # Add signal (should be rejected)
    result = queue.add_signal(sample_signal)

    assert result is False, "Signal should be rejected when relevant breaker is OPEN"
    assert len(queue.signal_queue) == 0
    mock_circuit_breaker.get_exchange_breaker.assert_called()  # Check that the breaker was checked

    # Simulate the breaker being CLOSED (or non-existent)
    mock_breaker_instance.state = BreakerState.CLOSED
    # Or mock get_exchange_breaker to return None or a CLOSED breaker
    mock_circuit_breaker.get_exchange_breaker.return_value = mock_breaker_instance

    # Add signal (should succeed)
    result = queue.add_signal(sample_signal)

    assert result is True, "Signal should be added when relevant breaker is CLOSED"
    assert len(queue.signal_queue) == 1


def test_add_from_opportunity(
    mock_config: Config, sample_opportunity: ArbitrageOpportunity
) -> None:
    """Test creating and adding a signal from an arbitrage opportunity."""
    queue = PrioritySignalQueue(mock_config)

    # Add from opportunity using the method
    signal = queue.add_from_opportunity(
        opportunity=sample_opportunity,
        strategy_name="funding_arb_strategy",
    )

    assert signal is not None
    assert len(queue.signal_queue) == 1
    assert signal.source_strategy == "funding_arb_strategy"
    assert signal.symbol == "ETH/USDT"
    assert signal.metadata is not None
    assert signal.metadata["utility_score"] == 0.9
    assert signal.metadata["long_exchange"] == "exA"
    assert signal.metadata["short_exchange"] == "exB"


def test_get_next_signal(mock_config: Config, sample_signal: TradeSignal) -> None:
    """Test getting the next signal from the queue."""
    queue = PrioritySignalQueue(mock_config)

    # Add signal
    queue.add_signal(sample_signal)

    # Get next signal
    signal = queue.get_next_signal()

    assert signal is not None
    assert signal.symbol == "BTC/USDT"
    assert len(queue.signal_queue) == 0  # Signal should be removed


def test_peek_next_signal(mock_config: Config, sample_signal: TradeSignal) -> None:
    """Test peeking at the next signal without removing it."""
    queue = PrioritySignalQueue(mock_config)

    # Add signal
    queue.add_signal(sample_signal)

    # Peek at next signal
    signal = queue.peek_next_signal()

    assert signal is not None
    assert signal.symbol == "BTC/USDT"
    assert len(queue.signal_queue) == 1  # Signal should still be in queue


def test_get_signals(mock_config: Config) -> None:
    """Test getting signals by symbol or all signals."""
    queue = PrioritySignalQueue(mock_config)

    # Add two signals with different symbols
    signal1 = TradeSignal(
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000"),
        quantity=Decimal("1"),
        exchange="mock_exchange1",
    )
    signal2 = TradeSignal(
        symbol="ETH/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("3000"),
        quantity=Decimal("1"),
        exchange="mock_exchange2",
    )

    queue.add_signal(signal1)
    queue.add_signal(signal2)

    # Get signals for BTC/USDT
    btc_signals = queue.get_signals(symbol="BTC/USDT")
    assert len(btc_signals) == 1
    assert btc_signals[0].symbol == "BTC/USDT"

    # Get all signals
    all_signals = queue.get_signals()
    assert len(all_signals) == 2


def test_count(mock_config: Config, sample_signal: TradeSignal) -> None:
    """Test counting signals in the queue."""
    queue = PrioritySignalQueue(mock_config)

    assert queue.count() == 0

    queue.add_signal(sample_signal)
    assert queue.count() == 1


def test_clear(mock_config: Config, sample_signal: TradeSignal) -> None:
    """Test clearing the queue."""
    queue = PrioritySignalQueue(mock_config)

    queue.add_signal(sample_signal)
    assert queue.count() == 1

    queue.clear()
    assert queue.count() == 0
    assert queue.signal_queue == []


@patch("cyberdelta.core.signal_queue.datetime")  # Patch datetime used within the queue methods
@patch(
    "cyberdelta.core.models.datetime"
)  # Patch datetime used within the model methods (like is_valid)
def test_clean_expired_signals(
    mock_models_dt: MagicMock, mock_queue_dt: MagicMock, mock_config: Config
) -> None:
    """Test cleaning up expired signals."""
    queue = PrioritySignalQueue(mock_config)

    # Mock current time
    now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    mock_queue_dt.now.return_value = now
    mock_models_dt.now.return_value = now

    # Add signals with different expiration times
    valid_signal = TradeSignal(
        timestamp=now - timedelta(seconds=10),
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000"),
        quantity=Decimal("1"),
        source_strategy="test",
        metadata={"utility_score": 0.8},
        expiration=(now + timedelta(seconds=30)),  # Explicit future expiration
        exchange="mock_exchange",
    )
    expired_signal = TradeSignal(
        timestamp=now - timedelta(seconds=70),
        symbol="ETH/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("3000"),
        quantity=Decimal("1"),
        source_strategy="test",
        metadata={"utility_score": 0.7},
        expiration=(now - timedelta(seconds=10)),  # Explicit past expiration
        exchange="mock_exchange",
    )
    default_expired_signal = TradeSignal(
        timestamp=now - timedelta(seconds=70),  # Created > 60s ago
        symbol="SOL/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("100"),
        quantity=Decimal("10"),
        source_strategy="test",
        metadata={"utility_score": 0.6},
        # No explicit expiration, should use default (60s)
        exchange="mock_exchange",
    )
    lowest_priority_signal = TradeSignal(
        timestamp=now - timedelta(seconds=10),
        symbol="LOW_PRIORITY",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("1"),
        quantity=Decimal("1"),
        source_strategy="test",
        metadata={"utility_score": 0.1},
        expiration=(now + timedelta(seconds=30)),
        exchange="mock_exchange",
    )

    queue.add_signal(valid_signal)
    queue.add_signal(expired_signal)
    queue.add_signal(default_expired_signal)
    queue.add_signal(lowest_priority_signal)

    assert queue.count() == 4

    # Clean expired signals
    cleaned_count = queue.clean_expired_signals()

    assert cleaned_count == 2
    assert queue.count() == 2

    # Verify remaining signals
    remaining_signals = [s for _, _, s in queue.signal_queue]
    symbols = {s.symbol for s in remaining_signals}
    assert "BTC/USDT" in symbols
    assert "LOW_PRIORITY" in symbols
    assert "ETH/USDT" not in symbols
    assert "SOL/USDT" not in symbols
    # Verify lowest priority signal is still present
    # lowest_priority_signal should be defined within this scope or passed as fixture
    assert lowest_priority_signal.symbol == "LOW_PRIORITY"


def test_trim_queue(mock_config: Config) -> None:
    """Test trimming the queue when it exceeds max size."""
    # Override max queue size for this test
    config = Config(
        {
            "default_signal_expiration_seconds": 60,
            "max_signal_queue_size": 2,
            "queue_cleanup_interval": 5,
        }
    )
    queue = PrioritySignalQueue(config)

    # Add signals with different priorities
    signal1 = TradeSignal(
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal(str(50000 + 0)),
        quantity=Decimal("1"),
        metadata={"utility_score": 0.1 * 0},
        exchange=f"mock_exchange_{0}",
    )

    signal2 = TradeSignal(
        symbol="ETH/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal(str(50000 + 1)),
        quantity=Decimal("1"),
        metadata={"utility_score": 0.1 * 1},
        exchange=f"mock_exchange_{1}",
    )

    signal3 = TradeSignal(
        symbol="SOL/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal(str(50000 + 2)),
        quantity=Decimal("1"),
        metadata={"utility_score": 0.1 * 2},
        exchange=f"mock_exchange_{2}",
    )

    queue.add_signal(signal1)
    queue.add_signal(signal2)
    queue.add_signal(signal3)

    # Queue should automatically trim to max size of 2
    assert queue.count() == 2

    # Check which signals remain (should be the highest priority ones)
    signals = queue.get_signals()
    symbols = [s.symbol for s in signals]
    assert "BTC/USDT" in symbols
    assert "ETH/USDT" in symbols
    assert "SOL/USDT" not in symbols

    # Verify signals are added
    assert len(queue.signal_queue) == 3

    # Add one more signal to trigger trimming
    signal4 = TradeSignal(
        symbol="LOW_PRIORITY",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal(str(50000 + 3)),
        quantity=Decimal("1"),
        metadata={"utility_score": 0.1 * 3},
        exchange=f"mock_exchange_{3}",
    )
    queue.add_signal(signal4)

    # Check which signals remain (should be the highest priority ones)
    signals = queue.get_signals()
    symbols = [s.symbol for s in signals]
    assert "BTC/USDT" in symbols
    assert "ETH/USDT" in symbols
    assert "SOL/USDT" not in symbols
    assert lowest_priority_signal.symbol == "LOW_PRIORITY"


def test_calculate_expiration(mock_config: Config, sample_signal: TradeSignal) -> None:
    """Test expiration calculation for signals."""
    queue = PrioritySignalQueue(mock_config)

    # Signal without expiration
    now = datetime.now(UTC)
    no_expiration = TradeSignal(
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000"),
        quantity=Decimal("1"),
        expiration=None,  # No expiration set
    )

    # Calculate expiration
    signal_with_expiration = queue._calculate_expiration(no_expiration)

    # Should set default expiration (60 seconds)
    assert signal_with_expiration.expiration is not None

    # Now that we've verified it's not None, we can use it
    expiration = signal_with_expiration.expiration
    expected_expiration_time = now + timedelta(seconds=60)

    # Allow small difference due to test execution time
    assert abs((expiration - expected_expiration_time).total_seconds()) < 5

    # Signal with expiration already set should not be modified
    preset_expiration_time = now + timedelta(minutes=5)
    with_expiration = TradeSignal(
        symbol="ETH/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("3000"),
        quantity=Decimal("1"),
        expiration=preset_expiration_time,
    )

    result = queue._calculate_expiration(with_expiration)
    assert result.expiration is not None
    assert result.expiration == preset_expiration_time


def test_check_circuit_breakers(mock_config: Config, mock_circuit_breaker: MagicMock) -> None:
    """Test circuit breaker checks."""
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)

    signal = TradeSignal(
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000"),
        quantity=Decimal("1"),
        metadata={
            "long_exchange": "exchange_a",
            "short_exchange": "exchange_b",
        },
        exchange="exA",
    )

    # Set up circuit breaker to check for different cases

    # Case 1: All checks pass
    mock_circuit_breaker.check_symbol.return_value = True
    mock_circuit_breaker.check_exchange.return_value = True

    assert queue._check_circuit_breakers(signal) is True

    # Case 2: Symbol check fails
    mock_circuit_breaker.check_symbol.return_value = False
    mock_circuit_breaker.check_exchange.return_value = True

    assert queue._check_circuit_breakers(signal) is False

    # Case 3: Exchange check fails
    mock_circuit_breaker.check_symbol.return_value = True
    mock_circuit_breaker.check_exchange.return_value = False

    assert queue._check_circuit_breakers(signal) is False

    # Case 4: Both checks fail
    mock_circuit_breaker.check_symbol.return_value = False
    mock_circuit_breaker.check_exchange.return_value = False

    assert queue._check_circuit_breakers(signal) is False

    # Case 5: No circuit breaker configured
    queue_no_cb = PrioritySignalQueue(mock_config, None)
    assert queue_no_cb._check_circuit_breakers(signal) is True

    # Case 6: Signal with no metadata
    signal_no_metadata = TradeSignal(
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000"),
        quantity=Decimal("1"),
        exchange="exA",
    )

    mock_circuit_breaker.check_symbol.return_value = True
    assert queue._check_circuit_breakers(signal_no_metadata) is True


def test_queue_init(signal_queue: PrioritySignalQueue) -> None:
    """Test queue initialization with fixture."""
    assert signal_queue.signal_queue == []


def test_add_basic_signal(signal_queue: PrioritySignalQueue, sample_signal: TradeSignal) -> None:
    """Test adding a basic signal."""
    assert signal_queue.add_signal(sample_signal) is True


def test_add_from_opportunity_fixture(
    signal_queue: PrioritySignalQueue, sample_opportunity: ArbitrageOpportunity
) -> None:
    """Test add from opportunity with fixture."""
    signal = signal_queue.add_from_opportunity(
        opportunity=sample_opportunity,
        strategy_name="funding_arb_strategy",
    )
    assert signal is not None


def test_get_next_signal_fixture(
    signal_queue: PrioritySignalQueue, sample_signal: TradeSignal
) -> None:
    """Test get next signal with fixture."""
    signal_queue.add_signal(sample_signal)
    assert signal_queue.get_next_signal() is not None


def test_queue_is_empty(signal_queue: PrioritySignalQueue) -> None:
    """Test checking if queue is empty."""
    assert signal_queue.is_empty() is True

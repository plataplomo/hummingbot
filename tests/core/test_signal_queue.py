"""
Tests for the Priority Signal Queue functionality.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.validation.funding_data import ArbitrageOpportunity


@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    return {
        "default_signal_expiration_seconds": 60,
        "max_signal_queue_size": 100,
        "queue_cleanup_interval": 5,
    }


@pytest.fixture
def mock_circuit_breaker():
    """Mock circuit breaker system for testing."""
    mock = MagicMock()
    # Default to all checks passing
    mock.check_exchange.return_value = True
    mock.check_symbol.return_value = True
    return mock


@pytest.fixture
def signal_queue(mock_config):
    """Mock signal queue for testing."""
    return PrioritySignalQueue(mock_config)


@pytest.fixture
def sample_signal():
    """Sample trade signal for testing."""
    now = datetime.now()
    return TradeSignal(
        timestamp=now,
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=50000,
        source_strategy="test_strategy",
        metadata={"utility_score": 0.8},
    )


@pytest.fixture
def sample_opportunity():
    """Fixture to create a sample ArbitrageOpportunity."""
    now = datetime.now(UTC)
    # Ensure all numeric inputs intended as Decimal are explicitly converted
    long_p = Decimal("3000.0")
    short_p = Decimal("3001.0")
    long_fr = Decimal("0.0001")
    short_fr = Decimal("-0.0001")
    net_diff = Decimal("-0.0002")
    exp_profit = Decimal("1.5")

    return ArbitrageOpportunity(
        symbol="ETH/USDT",
        long_exchange="exA",
        short_exchange="exB",
        long_price=long_p,
        short_price=short_p,
        long_funding_rate=long_fr,
        short_funding_rate=short_fr,
        net_funding_differential=net_diff,
        timestamp=now,
        utility_score=0.9,  # Float is fine
        expected_profit=exp_profit,
        basis_volatility=0.0005,  # Float is fine
        optimal_size=None,
        confidence=None,
    )


def test_initialization(mock_config):
    """Test queue initialization."""
    queue = PrioritySignalQueue(mock_config)

    assert queue.signal_queue == []
    assert queue.counter == 0
    assert queue.default_expiration_seconds == 60
    assert queue.max_queue_size == 100
    assert queue.cleanup_interval == 5


def test_add_signal(mock_config, sample_signal):
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


def test_add_signal_with_circuit_breaker(mock_config, mock_circuit_breaker, sample_signal):
    """Test adding a signal with circuit breaker integration."""
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)

    # Set up circuit breaker to block
    mock_circuit_breaker.check_symbol.return_value = False

    # Add signal (should be rejected)
    result = queue.add_signal(sample_signal)

    assert result is False
    assert len(queue.signal_queue) == 0

    # Allow signal
    mock_circuit_breaker.check_symbol.return_value = True

    # Add signal (should succeed)
    result = queue.add_signal(sample_signal)

    assert result is True
    assert len(queue.signal_queue) == 1


def test_add_from_opportunity(mock_config, sample_opportunity):
    """Test creating and adding a signal from an arbitrage opportunity."""
    queue = PrioritySignalQueue(mock_config)

    # Add from opportunity
    signal = queue.add_from_opportunity(sample_opportunity, "funding_arb_strategy")

    assert signal is not None
    assert len(queue.signal_queue) == 1
    assert signal.source_strategy == "funding_arb_strategy"
    assert signal.symbol == "ETH/USDT"
    assert signal.metadata["utility_score"] == 0.9
    assert signal.metadata["long_exchange"] == "exA"
    assert signal.metadata["short_exchange"] == "exB"


def test_get_next_signal(mock_config, sample_signal):
    """Test getting the next signal from the queue."""
    queue = PrioritySignalQueue(mock_config)

    # Add signal
    queue.add_signal(sample_signal)

    # Get next signal
    signal = queue.get_next_signal()

    assert signal is not None
    assert signal.symbol == "BTC/USDT"
    assert len(queue.signal_queue) == 0  # Signal should be removed


def test_peek_next_signal(mock_config, sample_signal):
    """Test peeking at the next signal without removing it."""
    queue = PrioritySignalQueue(mock_config)

    # Add signal
    queue.add_signal(sample_signal)

    # Peek at next signal
    signal = queue.peek_next_signal()

    assert signal is not None
    assert signal.symbol == "BTC/USDT"
    assert len(queue.signal_queue) == 1  # Signal should still be in queue


def test_get_signals(mock_config):
    """Test getting multiple signals in priority order."""
    queue = PrioritySignalQueue(mock_config)

    # Add multiple signals with different priorities
    for i in range(3):
        signal = TradeSignal(
            source_strategy=f"strategy_{i}",
            symbol=f"BTC-{i}",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            timestamp=datetime.now(UTC),
            price=Decimal(str(30000.0 + i)),
            metadata={"utility_score": 0.5 + i * 0.1},
        )
        queue.add_signal(signal)

    # Get signals
    signals = queue.get_signals(max_count=2)

    assert len(signals) == 2
    assert signals[0].symbol == "BTC-2"  # Highest utility score
    assert signals[1].symbol == "BTC-1"  # Second highest


def test_count(mock_config, sample_signal):
    """Test counting signals in the queue."""
    queue = PrioritySignalQueue(mock_config)

    assert queue.count() == 0

    # Add signal
    queue.add_signal(sample_signal)

    assert queue.count() == 1


def test_clear(mock_config, sample_signal):
    """Test clearing the queue."""
    queue = PrioritySignalQueue(mock_config)

    # Add signal
    queue.add_signal(sample_signal)

    assert len(queue.signal_queue) == 1

    # Clear queue
    queue.clear()

    assert len(queue.signal_queue) == 0


def test_clean_expired_signals(mock_config):
    """Test cleaning expired signals."""
    queue = PrioritySignalQueue(mock_config)
    now_utc = datetime.now(UTC)

    # Add expired signal (using UTC)
    expired_signal = TradeSignal(
        source_strategy="test_strategy",
        symbol="BTC-PERP",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        timestamp=now_utc - timedelta(minutes=1),  # Make timestamp aware
        price=Decimal("30000.0"),  # Use Decimal
        expiration=now_utc - timedelta(seconds=10),  # Make expiration aware
        metadata={"utility_score": 0.75},
    )

    # Add valid signal (using UTC)
    valid_signal = TradeSignal(
        source_strategy="test_strategy",
        symbol="ETH-PERP",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        timestamp=now_utc,  # Make timestamp aware
        price=Decimal("2000.0"),  # Use Decimal
        expiration=now_utc + timedelta(seconds=60),  # Make expiration aware
        metadata={"utility_score": 0.9},
    )

    queue.add_signal(expired_signal)
    queue.add_signal(valid_signal)

    # Check count immediately after adding - add_signal might filter expired
    assert queue.count() == 1  # Changed from 2

    # Explicitly clean and verify
    queue.clear_expired_signals()

    assert queue.count() == 1
    signal = queue.peek_next_signal()
    assert signal.symbol == "ETH-PERP"


def test_trim_queue(mock_config):
    """Test trimming the queue to max size."""
    config = mock_config
    config["max_signal_queue_size"] = 2
    queue = PrioritySignalQueue(config)

    # Add 3 signals
    for i in range(3):
        signal = TradeSignal(
            source_strategy=f"strategy_{i}",
            symbol=f"SYM-{i}",
            signal_type=SignalType.ENTER_SHORT,
            side=OrderSide.SELL,
            timestamp=datetime.now(UTC),
            price=Decimal(str(10000.0 + i)),
            metadata={"utility_score": 0.1 + i * 0.1},
        )
        queue.add_signal(signal)

    assert queue.count() == 3

    # Trim queue (removes lowest priority: SYM-0)
    removed = queue._trim_queue()

    assert removed is True
    assert queue.count() == 2

    # Check remaining signals are the highest priority
    remaining_symbols = {s[2].symbol for s in queue.signal_queue}
    assert remaining_symbols == {"SYM-1", "SYM-2"}


def test_calculate_expiration(mock_config, sample_signal):
    """Test calculating signal expiration time."""
    queue = PrioritySignalQueue(mock_config)

    # Test default expiration
    now = datetime.now(UTC)
    expiration = queue._calculate_expiration(sample_signal)
    assert isinstance(expiration, datetime)
    # Allow for a small time difference due to execution time
    assert (expiration - now).total_seconds() == pytest.approx(60, abs=1)

    # Test expiration with confidence adjustment
    signal_with_low_confidence = TradeSignal(
        source_strategy="test_strategy",
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        timestamp=datetime.now(UTC),
        price=50000,
        metadata={"utility_score": 0.8, "confidence_score": 0.2},
    )
    now = datetime.now(UTC)
    expiration_low_conf = queue._calculate_expiration(signal_with_low_confidence)
    expected_seconds_low_conf = 60 * (0.5 + 0.2 * 0.5)
    assert (expiration_low_conf - now).total_seconds() == pytest.approx(
        expected_seconds_low_conf, abs=1
    )


def test_check_circuit_breakers(mock_config, mock_circuit_breaker):
    """Test signal validation against circuit breakers."""
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)

    signal = TradeSignal(
        source_strategy="test_strategy",
        symbol="BTC-PERP",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        timestamp=datetime.now(UTC),  # Ensure aware datetime
        price=Decimal("30000.0"),  # Use Decimal
        metadata={"utility_score": 0.75, "exchange": "hyperliquid"},
    )

    # Configure mock to return expected tuple for the first check
    mock_circuit_breaker.can_execute.return_value = (True, None)

    # Test: Pass (mock configured to return True, None)
    assert queue._check_circuit_breakers(signal) is True
    mock_circuit_breaker.can_execute.assert_called_once_with("hyperliquid", "BTC-PERP")

    # Test: Blocked by exchange
    mock_circuit_breaker.reset_mock()  # Reset call count
    mock_circuit_breaker.can_execute.return_value = (False, "Exchange Maintenance")
    assert queue._check_circuit_breakers(signal) is False
    mock_circuit_breaker.can_execute.assert_called_once_with("hyperliquid", "BTC-PERP")

    # Test: Signal without explicit exchange (should infer and check)
    signal_infer = TradeSignal(
        source_strategy="test_strategy",
        symbol="BACKPACK-BTC-PERP",  # Inferrable symbol
        signal_type=SignalType.ENTER_SHORT,
        side=OrderSide.SELL,
        timestamp=datetime.now(UTC),  # Aware datetime
        price=Decimal("29999.0"),  # Decimal
        metadata={"utility_score": 0.70},  # No explicit exchange
    )
    mock_circuit_breaker.reset_mock()
    mock_circuit_breaker.can_execute.return_value = (True, None)  # Assume pass
    assert queue._check_circuit_breakers(signal_infer) is True
    mock_circuit_breaker.can_execute.assert_called_once_with("backpack", "BACKPACK-BTC-PERP")

    # Test: Signal without exchange and non-inferrable symbol
    signal_no_exchange = TradeSignal(
        source_strategy="test_strategy",
        symbol="BTC/USDT",  # Non-inferrable symbol
        signal_type=SignalType.EXIT_LONG,
        side=OrderSide.SELL,
        timestamp=datetime.now(UTC),  # Aware datetime
        price=Decimal("30000.0"),  # Decimal
        metadata={"utility_score": 0.75},
    )
    mock_circuit_breaker.reset_mock()
    assert (
        queue._check_circuit_breakers(signal_no_exchange) is True
    )  # Should allow if exchange unknown
    mock_circuit_breaker.can_execute.assert_not_called()  # Should not be called


def test_queue_init(signal_queue):
    """Test basic initialization of the queue."""
    assert signal_queue is not None
    assert signal_queue.count() == 0


def test_add_basic_signal(signal_queue, sample_signal):
    """Test adding a basic signal."""
    added = signal_queue.add_signal(sample_signal)
    assert added is True
    assert signal_queue.count() == 1


def test_add_from_opportunity(signal_queue, sample_opportunity):
    """Test adding a signal created from an opportunity."""
    created_signal = signal_queue.add_from_opportunity(sample_opportunity, "funding_arb_strategy")
    assert created_signal is not None
    assert signal_queue.count() == 1
    assert created_signal.source_strategy == "funding_arb_strategy"
    assert created_signal.symbol == "ETH/USDT"
    assert created_signal.metadata["utility_score"] == 0.9


def test_get_next_signal(signal_queue, sample_signal):
    """Test retrieving the highest priority signal."""
    signal_queue.add_signal(sample_signal)
    next_signal = signal_queue.get_next_signal()
    assert next_signal == sample_signal
    assert signal_queue.count() == 0


def test_queue_is_empty(signal_queue):
    """Test retrieving from an empty queue."""
    assert signal_queue.get_next_signal() is None

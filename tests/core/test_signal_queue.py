"""
Tests for the Priority Signal Queue functionality.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock
import heapq

from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.types import TradeSignal, SignalType, OrderType
from cyberdelta.validation.funding_data import ArbitrageOpportunity


@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    return {
        "default_signal_expiration_seconds": 60,
        "max_signal_queue_size": 5,
        "queue_cleanup_interval": 1,
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
def sample_trade_signal():
    """Sample trade signal for testing."""
    return TradeSignal(
        strategy_name="test_strategy",
        symbol="BTC-PERP",
        signal_type=SignalType.ENTER_LONG,
        timestamp=datetime.now(),
        price=30000.0,
        quantity=1.0,
        order_type=OrderType.MARKET,
        metadata={
            "utility_score": 0.75,
            "confidence_score": 0.8,
            "expected_profit": 100.0,
        },
    )


@pytest.fixture
def sample_arbitrage_opportunity():
    """Sample arbitrage opportunity for testing."""
    return ArbitrageOpportunity(
        symbol="ETH-PERP",
        long_exchange="exchange1",
        short_exchange="exchange2",
        long_funding_rate=0.001,
        short_funding_rate=-0.002,
        net_funding_differential=0.003,
        timestamp=datetime.now(),
        expected_profit=150.0,
        utility_score=0.85,
        confidence_score=0.9,
        basis_volatility=0.05,
    )


def test_initialization(mock_config):
    """Test queue initialization."""
    queue = PrioritySignalQueue(mock_config)

    assert queue.signal_queue == []
    assert queue.counter == 0
    assert queue.default_expiration_seconds == 60
    assert queue.max_queue_size == 5
    assert queue.cleanup_interval == 1


def test_add_signal(mock_config, sample_trade_signal):
    """Test adding a signal to the queue."""
    queue = PrioritySignalQueue(mock_config)

    # Add signal
    result = queue.add_signal(sample_trade_signal)

    assert result is True
    assert len(queue.signal_queue) == 1
    assert queue.counter == 1

    # Check queue contents
    priority, _, signal = queue.signal_queue[0]
    assert priority == -0.75  # Negative for max-heap
    assert signal.symbol == "BTC-PERP"
    assert signal.expiration is not None


def test_add_signal_with_circuit_breaker(
    mock_config, mock_circuit_breaker, sample_trade_signal
):
    """Test adding a signal with circuit breaker integration."""
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)

    # Set up circuit breaker to block
    mock_circuit_breaker.check_symbol.return_value = False

    # Add signal (should be rejected)
    result = queue.add_signal(sample_trade_signal)

    assert result is False
    assert len(queue.signal_queue) == 0

    # Allow signal
    mock_circuit_breaker.check_symbol.return_value = True

    # Add signal (should succeed)
    result = queue.add_signal(sample_trade_signal)

    assert result is True
    assert len(queue.signal_queue) == 1


def test_add_from_opportunity(mock_config, sample_arbitrage_opportunity):
    """Test creating and adding a signal from an arbitrage opportunity."""
    queue = PrioritySignalQueue(mock_config)

    # Add from opportunity
    signal = queue.add_from_opportunity(sample_arbitrage_opportunity, "arb_strategy")

    assert signal is not None
    assert len(queue.signal_queue) == 1
    assert signal.strategy_name == "arb_strategy"
    assert signal.symbol == "ETH-PERP"
    assert signal.metadata["utility_score"] == 0.85
    assert signal.metadata["long_exchange"] == "exchange1"
    assert signal.metadata["short_exchange"] == "exchange2"


def test_get_next_signal(mock_config, sample_trade_signal):
    """Test getting the next signal from the queue."""
    queue = PrioritySignalQueue(mock_config)

    # Add signal
    queue.add_signal(sample_trade_signal)

    # Get next signal
    signal = queue.get_next_signal()

    assert signal is not None
    assert signal.symbol == "BTC-PERP"
    assert len(queue.signal_queue) == 0  # Signal should be removed


def test_peek_next_signal(mock_config, sample_trade_signal):
    """Test peeking at the next signal without removing it."""
    queue = PrioritySignalQueue(mock_config)

    # Add signal
    queue.add_signal(sample_trade_signal)

    # Peek at next signal
    signal = queue.peek_next_signal()

    assert signal is not None
    assert signal.symbol == "BTC-PERP"
    assert len(queue.signal_queue) == 1  # Signal should still be in queue


def test_get_signals(mock_config):
    """Test getting multiple signals in priority order."""
    queue = PrioritySignalQueue(mock_config)

    # Add multiple signals with different priorities
    for i in range(3):
        signal = TradeSignal(
            strategy_name=f"strategy_{i}",
            symbol=f"BTC-{i}",
            signal_type=SignalType.ENTER_LONG,
            timestamp=datetime.now(),
            price=30000.0 + i,
            metadata={"utility_score": 0.5 + i * 0.1},
        )
        queue.add_signal(signal)

    # Get signals
    signals = queue.get_signals(max_count=2)

    assert len(signals) == 2
    assert signals[0].symbol == "BTC-2"  # Highest utility score
    assert signals[1].symbol == "BTC-1"  # Second highest


def test_count(mock_config, sample_trade_signal):
    """Test counting signals in the queue."""
    queue = PrioritySignalQueue(mock_config)

    assert queue.count() == 0

    # Add signal
    queue.add_signal(sample_trade_signal)

    assert queue.count() == 1


def test_clear(mock_config, sample_trade_signal):
    """Test clearing the queue."""
    queue = PrioritySignalQueue(mock_config)

    # Add signal
    queue.add_signal(sample_trade_signal)

    assert len(queue.signal_queue) == 1

    # Clear queue
    queue.clear()

    assert len(queue.signal_queue) == 0


def test_clean_expired_signals(mock_config):
    """Test cleaning expired signals."""
    queue = PrioritySignalQueue(mock_config)

    # Add expired signal
    expired_signal = TradeSignal(
        strategy_name="test_strategy",
        symbol="BTC-PERP",
        signal_type=SignalType.ENTER_LONG,
        timestamp=datetime.now(),
        price=30000.0,
        expiration=datetime.now() - timedelta(seconds=10),
        metadata={"utility_score": 0.75},
    )

    # Add valid signal
    valid_signal = TradeSignal(
        strategy_name="test_strategy",
        symbol="ETH-PERP",
        signal_type=SignalType.ENTER_LONG,
        timestamp=datetime.now(),
        price=2000.0,
        expiration=datetime.now() + timedelta(seconds=60),
        metadata={"utility_score": 0.5},
    )

    queue.add_signal(expired_signal)
    queue.add_signal(valid_signal)

    assert len(queue.signal_queue) == 2

    # Clean expired signals
    removed = queue._clean_expired_signals()

    assert removed == 1
    assert len(queue.signal_queue) == 1
    assert queue.signal_queue[0][2].symbol == "ETH-PERP"


def test_trim_queue(mock_config):
    """Test trimming the queue to max size."""
    # Use smaller max size for test
    config = mock_config.copy()
    config["max_signal_queue_size"] = 3

    queue = PrioritySignalQueue(config)

    # Add signals without trimming
    for i in range(5):
        signal = TradeSignal(
            strategy_name=f"strategy_{i}",
            symbol=f"BTC-{i}",
            signal_type=SignalType.ENTER_LONG,
            timestamp=datetime.now(),
            price=30000.0 + i,
            metadata={"utility_score": 0.5 + i * 0.1},
        )
        # Use heapq directly to bypass the automatic trimming
        queue.counter += 1
        heapq.heappush(
            queue.signal_queue,
            (-signal.metadata["utility_score"], queue.counter, signal),
        )

    # Verify we have 5 signals
    assert len(queue.signal_queue) == 5

    # Call trim_queue directly
    queue._trim_queue()

    # Queue should now be at max size
    assert len(queue.signal_queue) == 3

    # Check that highest priority signals were kept (lowest negative values)
    signals = queue.get_signals(max_count=3)
    symbols = [s.symbol for s in signals]

    # Should have kept highest utility scores
    assert "BTC-4" in symbols
    assert "BTC-3" in symbols
    assert "BTC-2" in symbols


def test_calculate_expiration(mock_config, sample_trade_signal):
    """Test calculating signal expiration time."""
    queue = PrioritySignalQueue(mock_config)

    # Signal with high confidence
    high_confidence = sample_trade_signal
    high_confidence.metadata["confidence_score"] = 1.0

    # Signal with low confidence
    low_confidence = TradeSignal(
        strategy_name="test_strategy",
        symbol="ETH-PERP",
        signal_type=SignalType.ENTER_LONG,
        timestamp=datetime.now(),
        price=2000.0,
        metadata={"utility_score": 0.5, "confidence_score": 0.0},
    )

    # Calculate expirations
    high_exp = queue._calculate_expiration(high_confidence)
    low_exp = queue._calculate_expiration(low_confidence)

    # High confidence should have longer expiration
    high_seconds = (high_exp - datetime.now()).total_seconds()
    low_seconds = (low_exp - datetime.now()).total_seconds()

    assert high_seconds > low_seconds
    assert abs(high_seconds - 60) < 1.0  # Should be close to 60 seconds
    assert abs(low_seconds - 30) < 1.0  # Should be close to 30 seconds


def test_check_circuit_breakers(mock_config, mock_circuit_breaker):
    """Test checking circuit breakers for a signal."""
    queue = PrioritySignalQueue(mock_config, mock_circuit_breaker)

    # Create signal with exchange metadata
    signal = TradeSignal(
        strategy_name="test_strategy",
        symbol="BTC-PERP",
        signal_type=SignalType.ENTER_LONG,
        timestamp=datetime.now(),
        price=30000.0,
        metadata={
            "utility_score": 0.75,
            "long_exchange": "exchange1",
            "short_exchange": "exchange2",
        },
    )

    # All breakers active
    mock_circuit_breaker.check_exchange.return_value = True
    mock_circuit_breaker.check_symbol.return_value = True

    assert queue._check_circuit_breakers(signal) is True

    # Long exchange breaker active
    mock_circuit_breaker.check_exchange.side_effect = lambda x: x != "exchange1"

    assert queue._check_circuit_breakers(signal) is False

    # Short exchange breaker active
    mock_circuit_breaker.check_exchange.side_effect = lambda x: x != "exchange2"

    assert queue._check_circuit_breakers(signal) is False

    # Symbol breaker active
    mock_circuit_breaker.check_exchange.side_effect = None
    mock_circuit_breaker.check_exchange.return_value = True
    mock_circuit_breaker.check_symbol.return_value = False

    assert queue._check_circuit_breakers(signal) is False

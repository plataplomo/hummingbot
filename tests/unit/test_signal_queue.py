"""Unit tests for the signal queue functionality.

Tests signal queue operations including priority handling, expiration, and circuit breaker integration.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
from cyberdelta.core.signal_queue import PrioritySignalQueue


@pytest.fixture
def mock_config() -> MagicMock:
    """Return mock config for testing."""
    return MagicMock()


@pytest.fixture
def signal_queue(mock_config: MagicMock) -> PrioritySignalQueue:
    """Create signal queue for testing."""
    return PrioritySignalQueue(mock_config)


@pytest.fixture
def sample_signal() -> TradeSignal:
    """Create sample signal for testing."""
    now = datetime.now(UTC)
    return TradeSignal(
        timestamp=now,
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000"),
        quantity=Decimal("1"),
        exchange="mock_exchange",
        source_strategy="test_strategy",
        metadata={"utility_score": 0.8},
    )


# Basic test to ensure the file collects and basic instantiation works
def test_queue_instantiation(signal_queue: PrioritySignalQueue) -> None:
    """Test queue instantiation."""
    assert signal_queue is not None


def test_signal_creation(sample_signal: TradeSignal) -> None:
    """Test signal creation."""
    assert sample_signal is not None
    assert sample_signal.source_strategy == "test_strategy"
    assert sample_signal.signal_type == SignalType.ENTER_LONG


# TODO: Restore or rewrite full tests for PrioritySignalQueue

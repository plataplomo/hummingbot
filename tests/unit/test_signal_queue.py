from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
from cyberdelta.core.signal_queue import PrioritySignalQueue


@pytest.fixture
def mock_config() -> MagicMock:
    return MagicMock()


@pytest.fixture
def signal_queue(mock_config: MagicMock) -> PrioritySignalQueue:
    return PrioritySignalQueue(mock_config)


@pytest.fixture
def sample_signal() -> TradeSignal:
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
    assert signal_queue is not None


def test_signal_creation(sample_signal: TradeSignal) -> None:
    assert sample_signal is not None
    assert sample_signal.source_strategy == "test_strategy"
    assert sample_signal.signal_type == SignalType.ENTER_LONG


# TODO: Restore or rewrite full tests for PrioritySignalQueue

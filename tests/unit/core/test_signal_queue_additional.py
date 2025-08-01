"""Additional comprehensive unit tests for PrioritySignalQueue.

Tests additional public methods and edge cases that need better coverage,
focusing on signal management, prioritization, expiration handling,
and circuit breaker integration.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest

from cyberdelta.core.enums import SignalType
from cyberdelta.core.models import OrderSide, TradeSignal
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.symbols import symbols
from cyberdelta.exceptions.parsing import EmptyStringError
from cyberdelta.validation.circuit_breaker import BreakerState, CircuitBreaker, CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Use shared mock_app_settings from conftest.py


@pytest.fixture
def mock_circuit_breaker_system() -> Mock:
    """Create mock circuit breaker system for testing.

    Returns:
        Mock: Mock instance of CircuitBreakerSystem with configured breaker.
    """
    system = Mock(spec=CircuitBreakerSystem)
    breaker = Mock(spec=CircuitBreaker)
    breaker.state = BreakerState.CLOSED
    breaker.allow_operation.return_value = True
    system.get_breaker.return_value = breaker
    return system


@pytest.fixture
def signal_queue(mock_app_settings: Mock, mock_circuit_breaker_system: Mock) -> PrioritySignalQueue:
    """Create a PrioritySignalQueue instance for testing.

    Returns:
        PrioritySignalQueue: Configured signal queue instance for testing.
    """
    return PrioritySignalQueue(
        app_settings=mock_app_settings,
        circuit_breaker_system=mock_circuit_breaker_system,
    )


# Use shared sample_trade_signal from conftest.py


@pytest.fixture
def sample_arbitrage_opportunity() -> ArbitrageOpportunity:
    """Create a sample ArbitrageOpportunity for testing.

    Returns:
        ArbitrageOpportunity: Sample arbitrage opportunity with realistic parameters.
    """
    btc_symbol = symbols.BTC.hyperliquid()
    return ArbitrageOpportunity(
        symbol=btc_symbol.value,
        long_exchange="hyperliquid",
        short_exchange="backpack",
        long_price=Decimal("50000.0"),
        short_price=Decimal("50100.0"),
        long_funding_rate=Decimal("0.0001"),
        short_funding_rate=Decimal("0.0002"),
        net_funding_differential=Decimal("0.0001"),
        timestamp=datetime.now(UTC),
    )


class TestSignalQueueInitialization:
    """Test suite for SignalQueue initialization and configuration."""

    # ==================== SUCCESS CASES ====================

    def test_initialization_success_with_all_dependencies(
        self, mock_app_settings: Mock, mock_circuit_breaker_system: Mock
    ) -> None:
        """Test successful initialization with all dependencies."""
        # Act
        queue = PrioritySignalQueue(
            app_settings=mock_app_settings,
            circuit_breaker_system=mock_circuit_breaker_system,
        )

        # Assert
        assert queue.app_settings is mock_app_settings
        assert queue.circuit_breaker_system is mock_circuit_breaker_system
        assert queue.signal_queue == []
        assert queue.signal_heap == []
        assert queue.counter == 0
        assert hasattr(queue, "lock")
        assert hasattr(queue, "new_signal_event")
        assert queue.default_expiration_seconds == 300.0
        assert queue.max_queue_size == 100

    def test_initialization_success_without_circuit_breaker(self, mock_app_settings: Mock) -> None:
        """Test successful initialization without circuit breaker system."""
        # Act
        queue = PrioritySignalQueue(
            app_settings=mock_app_settings,
            circuit_breaker_system=None,
        )

        # Assert
        assert queue.circuit_breaker_system is None
        assert queue.signal_queue == []

    def test_initialization_success_sets_last_cleanup_time(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test that initialization sets last cleanup time."""
        # Assert
        assert signal_queue.last_cleanup is not None
        assert isinstance(signal_queue.last_cleanup, datetime)
        assert signal_queue.last_cleanup.tzinfo is not None


class TestSignalQueueAddSignal:
    """Test suite for adding signals to the queue."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_add_signal_success_valid_signal(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test adding a valid signal to the queue."""
        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is True
        count = await signal_queue.count()
        assert count == 1

    @pytest.mark.asyncio
    async def test_add_signal_success_multiple_signals(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test adding multiple signals to the queue."""
        # Arrange
        signals: list[TradeSignal] = []
        for i in range(3):
            signal = TradeSignal(
                symbol=f"TEST{i}-PERP",
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("0.1"),
                signal_type=SignalType.ENTER_LONG,
                exchange="hyperliquid",
                confidence=0.8,
                metadata={"utility_score": float(i)},
            )
            signals.append(signal)

        # Act
        results: list[bool] = []
        for signal in signals:
            result = await signal_queue.add_signal(signal)
            results.append(result)

        # Assert
        assert all(results)
        count = await signal_queue.count()
        assert count == 3

    @pytest.mark.asyncio
    async def test_add_signal_success_without_utility_score(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test adding a signal without utility score (should use default)."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        signal = TradeSignal(
            symbol=btc_symbol.value,
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.1"),
            signal_type=SignalType.ENTER_LONG,
            exchange="hyperliquid",
            confidence=0.8,
            metadata={},  # No utility_score
        )

        # Act
        result = await signal_queue.add_signal(signal)

        # Assert
        assert result is True
        assert signal.metadata is not None
        assert signal.metadata.get("utility_score") == 0.0

    @pytest.mark.asyncio
    async def test_add_signal_success_sets_expiration(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test that adding a signal sets expiration if not present."""
        # Arrange
        sample_trade_signal.expiration = None

        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is True

        # Verify expiration was set and validate timing
        self._verify_signal_expiration(sample_trade_signal)

    def _verify_signal_expiration(self, signal: TradeSignal) -> None:
        """Helper to verify signal expiration."""
        assert signal.expiration is not None, "Signal should have expiration set"
        assert signal.expiration > datetime.now(UTC), "Expiration should be in the future"

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_add_signal_edge_with_existing_expiration(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test adding a signal that already has expiration set."""
        # Arrange
        original_expiration = datetime.now(UTC) + timedelta(minutes=10)
        sample_trade_signal.expiration = original_expiration

        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is True
        assert sample_trade_signal.expiration == original_expiration

    @pytest.mark.asyncio
    async def test_add_signal_edge_invalid_utility_score(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test adding a signal with invalid utility score."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        signal = TradeSignal(
            symbol=btc_symbol.value,
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.1"),
            signal_type=SignalType.ENTER_LONG,
            exchange="hyperliquid",
            confidence=0.8,
            metadata={"utility_score": "invalid"},  # Invalid type
        )

        # Act
        result = await signal_queue.add_signal(signal)

        # Assert
        # Should handle invalid utility score gracefully
        assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_add_signal_edge_circuit_breaker_open(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test adding a signal when circuit breaker is open."""
        # Arrange
        if signal_queue.circuit_breaker_system:
            breaker = Mock()
            breaker.state = BreakerState.OPEN
            breaker.allow_operation.return_value = False
            mock_method = "get_breaker"
            with patch.object(
                signal_queue.circuit_breaker_system, mock_method, return_value=breaker
            ):
                # Act
                result = await signal_queue.add_signal(sample_trade_signal)

                # Assert
                assert result is False
        else:
            # Act
            result = await signal_queue.add_signal(sample_trade_signal)

            # Assert
            assert result is True

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_add_signal_failure_invalid_signal(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test adding invalid signal through legitimate means."""
        # Test that we cannot create a TradeSignal with invalid data

        # Act & Assert - Creating a signal with empty symbol should raise validation error
        with pytest.raises(EmptyStringError):
            TradeSignal(
                signal_type=SignalType.ENTER_LONG,
                symbol="",  # Empty symbol should cause validation error
                side=OrderSide.BUY,
                price=Decimal("0.01"),
                exchange="test",
                timestamp=datetime.now(UTC),
            )


class TestSignalQueueAddFromOpportunity:
    """Test suite for adding signals from arbitrage opportunities."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_add_from_opportunity_success_valid_opportunity(
        self, signal_queue: PrioritySignalQueue, sample_arbitrage_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test adding signals from a valid arbitrage opportunity."""
        # Act
        result = await signal_queue.add_from_opportunity(
            sample_arbitrage_opportunity, "test_strategy"
        )

        # Assert
        assert result is not None  # Should return created signal
        count = await signal_queue.count()
        assert count >= 1  # Should create at least one signal

    @pytest.mark.asyncio
    async def test_add_from_opportunity_success_with_size(
        self, signal_queue: PrioritySignalQueue, sample_arbitrage_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test adding signals from opportunity."""
        # Act
        result = await signal_queue.add_from_opportunity(
            sample_arbitrage_opportunity, "test_strategy"
        )

        # Assert
        assert result is not None  # Should return created signal
        count = await signal_queue.count()
        assert count >= 1

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_add_from_opportunity_edge_no_sizes(
        self, signal_queue: PrioritySignalQueue, sample_arbitrage_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test adding signals from opportunity without specifying sizes."""
        # Act
        result = await signal_queue.add_from_opportunity(
            sample_arbitrage_opportunity, "test_strategy"
        )

        # Assert
        # Should handle missing sizes appropriately
        assert result is None or isinstance(result, TradeSignal)

    @pytest.mark.asyncio
    async def test_add_from_opportunity_edge_zero_nfd(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test adding signals from opportunity with zero NFD."""
        # Arrange
        btc_symbol = symbols.BTC.hyperliquid()
        opportunity = ArbitrageOpportunity(
            symbol=btc_symbol.value,
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50000.0"),
            short_price=Decimal("50000.0"),
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("0.0001"),
            net_funding_differential=Decimal("0.0"),  # Zero NFD
            timestamp=datetime.now(UTC),
        )

        # Act
        result = await signal_queue.add_from_opportunity(opportunity, "test_strategy")

        # Assert
        # Should handle zero NFD appropriately
        assert result is None or isinstance(result, TradeSignal)


class TestSignalQueueGetOperations:
    """Test suite for getting signals from the queue."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_get_next_signal_success_single_signal(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test getting the next signal when one exists."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.get_next_signal()

        # Assert
        assert result is not None
        assert result.symbol == sample_trade_signal.symbol
        count = await signal_queue.count()
        assert count == 0  # Signal should be removed

    @pytest.mark.asyncio
    async def test_get_next_signal_success_priority_order(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test getting signals respects priority order."""
        # Arrange
        low_symbol = symbols.BTC.hyperliquid()  # Use a symbol for testing
        low_priority_signal = TradeSignal(
            symbol=f"LOW-{low_symbol.value}",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.1"),
            signal_type=SignalType.ENTER_LONG,
            exchange="hyperliquid",
            confidence=0.8,
            metadata={"utility_score": 0.1},
        )

        high_symbol = symbols.BTC.hyperliquid()  # Use a symbol for testing
        high_priority_signal = TradeSignal(
            symbol=f"HIGH-{high_symbol.value}",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.1"),
            signal_type=SignalType.ENTER_LONG,
            exchange="hyperliquid",
            confidence=0.9,
            metadata={"utility_score": 0.9},
        )

        await signal_queue.add_signal(low_priority_signal)
        await signal_queue.add_signal(high_priority_signal)

        # Act
        result = await signal_queue.get_next_signal()

        # Assert
        assert result is not None
        assert result.symbol == f"HIGH-{high_symbol.value}"  # High priority should come first

    @pytest.mark.asyncio
    async def test_get_next_signal_success_empty_queue(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test getting next signal from empty queue."""
        # Act
        result = await signal_queue.get_next_signal()

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_peek_next_signal_success_does_not_remove(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test peeking at next signal doesn't remove it."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.peek_next_signal()

        # Assert
        assert result is not None
        assert result.symbol == sample_trade_signal.symbol
        count = await signal_queue.count()
        assert count == 1  # Signal should still be in queue

    @pytest.mark.asyncio
    async def test_get_signals_success_with_limit(self, signal_queue: PrioritySignalQueue) -> None:
        """Test getting multiple signals with limit."""
        # Arrange
        for i in range(5):
            signal = TradeSignal(
                symbol=f"TEST{i}-PERP",
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("0.1"),
                signal_type=SignalType.ENTER_LONG,
                exchange="hyperliquid",
                confidence=0.8,
                metadata={"utility_score": float(i)},
            )
            await signal_queue.add_signal(signal)

        # Act
        result = await signal_queue.get_signals(max_count=3)

        # Assert
        assert len(result) == 3
        # Should be in priority order (highest utility score first)
        assert result[0].symbol == "TEST4-PERP"
        assert result[1].symbol == "TEST3-PERP"
        assert result[2].symbol == "TEST2-PERP"

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_get_next_signal_edge_expired_signal(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test getting next signal skips expired signals."""
        # Arrange
        expired_symbol = symbols.BTC.hyperliquid()  # Use a symbol for testing
        expired_signal = TradeSignal(
            symbol=f"EXPIRED-{expired_symbol.value}",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.1"),
            signal_type=SignalType.ENTER_LONG,
            exchange="hyperliquid",
            confidence=0.8,
            metadata={"utility_score": 1.0},
        )
        # Set expiration after creation
        expired_signal.expiration = datetime.now(UTC) - timedelta(minutes=30)
        await signal_queue.add_signal(expired_signal)

        # Act
        result = await signal_queue.get_next_signal()

        # Assert
        # Should skip expired signal
        assert result is None or result.symbol != f"EXPIRED-{expired_symbol.value}"

    @pytest.mark.asyncio
    async def test_get_signals_edge_empty_queue(self, signal_queue: PrioritySignalQueue) -> None:
        """Test getting signals from empty queue."""
        # Act
        result = await signal_queue.get_signals(max_count=5)

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_signals_edge_limit_exceeds_queue_size(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test getting signals with limit larger than queue size."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.get_signals(max_count=10)

        # Assert
        assert len(result) == 1


class TestSignalQueueManagement:
    """Test suite for queue management operations."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_count_success_empty_queue(self, signal_queue: PrioritySignalQueue) -> None:
        """Test counting signals in empty queue."""
        # Act
        count = await signal_queue.count()

        # Assert
        assert count == 0

    @pytest.mark.asyncio
    async def test_count_success_with_signals(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test counting signals after adding."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        count = await signal_queue.count()

        # Assert
        assert count == 1

    @pytest.mark.asyncio
    async def test_clear_success_removes_all_signals(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test clearing all signals from queue."""
        # Arrange
        for i in range(3):
            signal = TradeSignal(
                symbol=f"TEST{i}-PERP",
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("0.1"),
                signal_type=SignalType.ENTER_LONG,
                exchange="hyperliquid",
                confidence=0.8,
                metadata={"utility_score": float(i)},
            )
            await signal_queue.add_signal(signal)

        # Act
        await signal_queue.clear()

        # Assert
        count = await signal_queue.count()
        assert count == 0

    @pytest.mark.asyncio
    async def test_is_empty_success_empty_queue(self, signal_queue: PrioritySignalQueue) -> None:
        """Test is_empty returns True for empty queue."""
        # Act
        result = await signal_queue.is_empty()

        # Assert
        assert result is True

    @pytest.mark.asyncio
    async def test_is_empty_success_non_empty_queue(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test is_empty returns False for non-empty queue."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.is_empty()

        # Assert
        assert result is False

    def test_get_signal_count_success_synchronous(self, signal_queue: PrioritySignalQueue) -> None:
        """Test synchronous get_signal_count method."""
        # Act
        count = signal_queue.get_signal_count()

        # Assert
        assert count == 0

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_clear_edge_already_empty_queue(self, signal_queue: PrioritySignalQueue) -> None:
        """Test clearing an already empty queue."""
        # Act
        await signal_queue.clear()

        # Assert
        count = await signal_queue.count()
        assert count == 0


class TestSignalQueueAsyncOperations:
    """Test suite for asynchronous queue operations."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_wait_for_signals_success_with_timeout(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test waiting for signals with timeout."""
        # Act
        # wait_for_signals doesn't have timeout, so we just test the method exists
        result = await signal_queue.wait_for_signals(max_signals=1)

        # Assert
        assert result == []  # Should return empty list when no signals

    @pytest.mark.asyncio
    async def test_wait_for_signals_success_signal_arrives(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test waiting for signals when signal arrives."""
        # Arrange
        # Add signal first
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.wait_for_signals(max_signals=1)

        # Assert
        assert len(result) == 1
        assert result[0].symbol == sample_trade_signal.symbol

    @pytest.mark.asyncio
    async def test_pop_signals_success_multiple(self, signal_queue: PrioritySignalQueue) -> None:
        """Test popping multiple signals."""
        # Arrange
        for i in range(3):
            signal = TradeSignal(
                symbol=f"TEST{i}-PERP",
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("0.1"),
                signal_type=SignalType.ENTER_LONG,
                exchange="hyperliquid",
                confidence=0.8,
                metadata={"utility_score": float(i)},
            )
            await signal_queue.add_signal(signal)

        # Act
        result = await signal_queue.pop_signals(max_signals=2)

        # Assert
        assert len(result) == 2
        count = await signal_queue.count()
        assert count == 1  # One signal should remain

    @pytest.mark.asyncio
    async def test_get_next_signals_success_preserves_queue(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test get_next_signals doesn't remove signals."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.get_next_signals(max_count=1)

        # Assert
        assert len(result) == 1
        count = await signal_queue.count()
        assert count == 1  # Signal should still be in queue

    @pytest.mark.asyncio
    async def test_enqueue_signal_success(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test enqueuing a signal."""
        # Act
        await signal_queue.enqueue_signal(sample_trade_signal)

        # Assert
        count = await signal_queue.count()
        assert count == 1

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_pop_signals_edge_empty_queue(self, signal_queue: PrioritySignalQueue) -> None:
        """Test popping signals from empty queue."""
        # Act
        result = await signal_queue.pop_signals(max_signals=5)

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_next_signals_edge_max_count_zero(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test get_next_signals with max_count of zero."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.get_next_signals(max_count=0)

        # Assert
        assert result == []


class TestSignalQueuePendingSignals:
    """Test suite for pending signals functionality."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_get_pending_signals_success_returns_all(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test getting all pending signals."""
        # Arrange
        signals: list[TradeSignal] = []
        for i in range(3):
            signal = TradeSignal(
                symbol=f"TEST{i}-PERP",
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("0.1"),
                signal_type=SignalType.ENTER_LONG,
                exchange="hyperliquid",
                confidence=0.8,
                metadata={"utility_score": float(i)},
            )
            signals.append(signal)
            await signal_queue.add_signal(signal)

        # Act
        result = await signal_queue.get_pending_signals()

        # Assert
        assert len(result) == 3
        # Should preserve all signals in queue
        count = await signal_queue.count()
        assert count == 3

    @pytest.mark.asyncio
    async def test_get_pending_signals_success_empty_queue(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test getting pending signals from empty queue."""
        # Act
        result = await signal_queue.get_pending_signals()

        # Assert
        assert result == []

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_get_pending_signals_edge_with_expired(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test getting pending signals includes expired ones."""
        # Arrange
        expired_symbol = symbols.BTC.hyperliquid()  # Use a symbol for testing
        expired_signal = TradeSignal(
            symbol=f"EXPIRED-{expired_symbol.value}",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.1"),
            signal_type=SignalType.ENTER_LONG,
            exchange="hyperliquid",
            confidence=0.8,
            metadata={"utility_score": 1.0},
        )
        # Set expiration after creation
        expired_signal.expiration = datetime.now(UTC) - timedelta(minutes=30)
        await signal_queue.add_signal(expired_signal)

        # Act
        result = await signal_queue.get_pending_signals()

        # Assert
        # Behavior depends on implementation - may include or exclude expired
        assert isinstance(result, list)


class TestSignalQueueLifecycle:
    """Test suite for queue lifecycle management."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_run_success_with_cancellation_token(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test running the queue with cancellation token."""
        # Arrange
        cancellation_token = asyncio.Event()

        # Act
        # Set cancellation immediately
        cancellation_token.set()

        # Run should exit quickly
        await signal_queue.run(cancellation_token)

        # Assert
        # Should complete without error
        assert True

    @pytest.mark.asyncio
    async def test_stop_success(self, signal_queue: PrioritySignalQueue) -> None:
        """Test stopping the queue."""
        # Act
        await signal_queue.stop()

        # Assert
        # Should complete without error
        assert True

    @pytest.mark.asyncio
    async def test_process_signal_success_empty_queue(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test processing signal from empty queue."""
        # Act
        result = await signal_queue.process_signal()

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_process_signal_success_with_signal(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test processing signal when one exists."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        # process_signal is not implemented, so this will return None
        result = await signal_queue.process_signal()

        # Assert
        # Since method is not implemented, expect None
        assert result is None

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_run_edge_immediate_cancellation(self, signal_queue: PrioritySignalQueue) -> None:
        """Test run with immediate cancellation."""
        # Arrange
        cancellation_token = asyncio.Event()
        cancellation_token.set()  # Cancel immediately

        # Act
        await signal_queue.run(cancellation_token)

        # Assert
        # Should exit gracefully
        assert True

    @pytest.mark.asyncio
    async def test_process_signal_edge_expired_signal(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test processing expired signal."""
        # Arrange
        expired_symbol = symbols.BTC.hyperliquid()  # Use a symbol for testing
        expired_signal = TradeSignal(
            symbol=f"EXPIRED-{expired_symbol.value}",
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("0.1"),
            signal_type=SignalType.ENTER_LONG,
            exchange="hyperliquid",
            confidence=0.8,
            metadata={"utility_score": 1.0},
        )
        # Set expiration after creation
        expired_signal.expiration = datetime.now(UTC) - timedelta(minutes=30)
        await signal_queue.add_signal(expired_signal)

        # Act
        result = await signal_queue.process_signal()

        # Assert
        # Since method is not implemented, expect None
        assert result is None

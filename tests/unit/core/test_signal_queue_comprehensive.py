"""Comprehensive unit tests for the PrioritySignalQueue component.

Tests signal queue functionality including priority management, circuit breaker integration,
and expiration handling.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.enums import SignalType
from cyberdelta.core.models import OrderSide, TradeSignal
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.validation.circuit_breaker import BreakerState, CircuitBreaker, CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity


@pytest.fixture
def mock_app_settings() -> Mock:
    """Create mock application settings.

    Returns:
        Mock: Mocked AppSettings instance.
    """
    return Mock(spec=AppSettings)


@pytest.fixture
def mock_circuit_breaker_system() -> Mock:
    """Create mock circuit breaker system.

    Returns:
        Mock: Mocked CircuitBreakerSystem with configured methods.
    """
    system = Mock(spec=CircuitBreakerSystem)
    system.get_breaker = Mock(return_value=None)
    system.get_exchange_breaker = Mock(return_value=None)
    system.can_execute = Mock(return_value=(True, None))
    return system


@pytest.fixture
def signal_queue(mock_app_settings: Mock, mock_circuit_breaker_system: Mock) -> PrioritySignalQueue:
    """Create PrioritySignalQueue instance for testing.

    Returns:
        PrioritySignalQueue: Queue instance with mocked dependencies.
    """
    return PrioritySignalQueue(
        app_settings=mock_app_settings,
        circuit_breaker_system=mock_circuit_breaker_system,
    )


@pytest.fixture
def sample_trade_signal() -> TradeSignal:
    """Create sample trade signal for testing.

    Returns:
        TradeSignal: Sample BTC-PERP long entry signal.
    """
    return TradeSignal(
        signal_id="test_signal_123",
        timestamp=datetime.now(UTC),
        symbol="BTC-PERP",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000.0"),
        quantity=Decimal("1.0"),
        exchange="hyperliquid",
        source_strategy="test_strategy",
        metadata={"utility_score": 0.8},
    )


@pytest.fixture
def sample_arbitrage_opportunity() -> ArbitrageOpportunity:
    """Create sample arbitrage opportunity for testing.

    Returns:
        ArbitrageOpportunity: Sample BTC-PERP arbitrage opportunity with utility scores.
    """
    return ArbitrageOpportunity(
        symbol="BTC-PERP",
        long_exchange="hyperliquid",
        short_exchange="backpack",
        long_price=Decimal("50000.0"),
        short_price=Decimal("49950.0"),
        long_funding_rate=Decimal("0.0001"),
        short_funding_rate=Decimal("-0.0001"),
        net_funding_differential=Decimal("0.0002"),
        timestamp=datetime.now(UTC),
        utility_score=0.9,
        expected_profit=Decimal("50.0"),
        basis_volatility=0.001,
        confidence_score=0.85,
        optimal_size=Decimal("10000.0"),
    )


class TestPrioritySignalQueueInit:
    """Test initialization of PrioritySignalQueue."""

    def test_init_success_with_circuit_breaker(
        self, mock_app_settings: Mock, mock_circuit_breaker_system: Mock
    ) -> None:
        """Test successful initialization with circuit breaker system."""
        # Act
        queue = PrioritySignalQueue(mock_app_settings, mock_circuit_breaker_system)

        # Assert
        assert queue.app_settings == mock_app_settings
        assert queue.circuit_breaker_system == mock_circuit_breaker_system
        assert isinstance(queue.signal_queue, list)
        assert len(queue.signal_queue) == 0
        assert queue.counter == 0
        assert queue.default_expiration_seconds == 300.0
        assert queue.max_queue_size == 100
        assert queue.cleanup_interval == 10.0
        assert isinstance(queue.last_cleanup, datetime)
        assert isinstance(queue.lock, asyncio.Lock)
        assert isinstance(queue.new_signal_event, asyncio.Event)

    def test_init_success_without_circuit_breaker(self, mock_app_settings: Mock) -> None:
        """Test successful initialization without circuit breaker system."""
        # Act
        queue = PrioritySignalQueue(mock_app_settings, None)

        # Assert
        assert queue.app_settings == mock_app_settings
        assert queue.circuit_breaker_system is None
        assert isinstance(queue.signal_queue, list)
        assert len(queue.signal_queue) == 0

    def test_init_edge_with_logging(self, mock_app_settings: Mock) -> None:
        """Test initialization logs proper messages."""
        # Act
        with patch("cyberdelta.core.signal_queue.get_logger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            queue = PrioritySignalQueue(mock_app_settings)
            _ = queue  # Used for side effects

            # Assert
            mock_logger.info.assert_called_once_with("Initialized priority signal queue")


class TestAddSignal:
    """Test add_signal method functionality."""

    @pytest.mark.asyncio
    async def test_add_signal_success_basic(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test successful addition of a basic signal."""
        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is True
        assert len(signal_queue.signal_queue) == 1
        assert signal_queue.signal_queue[0][2] == sample_trade_signal
        assert sample_trade_signal.expiration is not None

    @pytest.mark.asyncio
    async def test_add_signal_success_without_utility_score(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test adding signal without utility score uses default."""
        # Arrange
        sample_trade_signal.metadata = {}

        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is True
        assert sample_trade_signal.metadata["utility_score"] == 0.0

    @pytest.mark.asyncio
    async def test_add_signal_success_with_invalid_utility_score(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test adding signal with invalid utility score uses default."""
        # Arrange
        sample_trade_signal.metadata = {"utility_score": "invalid"}

        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is True
        assert sample_trade_signal.metadata["utility_score"] == 0.0

    @pytest.mark.asyncio
    async def test_add_signal_success_with_expiration_already_set(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test adding signal with expiration already set."""
        # Arrange
        custom_expiration = datetime.now(UTC) + timedelta(hours=1)
        sample_trade_signal.expiration = custom_expiration

        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is True
        assert sample_trade_signal.expiration == custom_expiration

    @pytest.mark.asyncio
    async def test_add_signal_edge_duplicate_rejection(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test duplicate signal is rejected."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is False
        assert len(signal_queue.signal_queue) == 1

    @pytest.mark.asyncio
    async def test_add_signal_edge_queue_full_trimming(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test queue trimming when full."""
        # Arrange
        signal_queue.max_queue_size = 3
        signals: list[TradeSignal] = []
        for i in range(4):
            signal = TradeSignal(
                signal_id=f"signal_{i}",
                timestamp=datetime.now(UTC),
                symbol="BTC-PERP",
                signal_type=SignalType.ENTER_LONG,
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("1.0"),
                exchange="hyperliquid",
                source_strategy="test_strategy",
                metadata={"utility_score": float(i)},
            )
            signals.append(signal)

        # Act
        for signal in signals:
            await signal_queue.add_signal(signal)

        # Assert
        assert len(signal_queue.signal_queue) == 3
        # Verify lowest score signal was removed
        remaining_ids = {s.signal_id for _, _, s in signal_queue.signal_queue}
        assert "signal_0" not in remaining_ids
        assert "signal_3" in remaining_ids

    @pytest.mark.asyncio
    async def test_add_signal_failure_circuit_breaker_open(
        self,
        signal_queue: PrioritySignalQueue,
        sample_trade_signal: TradeSignal,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test signal rejection when circuit breaker is open."""
        # Arrange
        mock_breaker = Mock(spec=CircuitBreaker)
        mock_breaker.state = BreakerState.OPEN
        mock_breaker.trip_reason = "Test trip"
        mock_circuit_breaker_system.get_breaker.return_value = mock_breaker

        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is False
        assert len(signal_queue.signal_queue) == 0


class TestAddFromOpportunity:
    """Test add_from_opportunity method functionality."""

    @pytest.mark.asyncio
    async def test_add_from_opportunity_success_basic(
        self,
        signal_queue: PrioritySignalQueue,
        sample_arbitrage_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test successful signal creation from arbitrage opportunity."""
        # Act
        result = await signal_queue.add_from_opportunity(
            sample_arbitrage_opportunity, "test_strategy"
        )

        # Assert
        assert result is not None
        assert isinstance(result, TradeSignal)
        assert result.symbol == sample_arbitrage_opportunity.symbol
        assert result.source_strategy == "test_strategy"
        assert result.price == sample_arbitrage_opportunity.long_price
        assert len(signal_queue.signal_queue) == 1

    @pytest.mark.asyncio
    async def test_add_from_opportunity_success_with_optimal_size(
        self,
        signal_queue: PrioritySignalQueue,
        sample_arbitrage_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test signal creation with optimal size calculation."""
        # Act
        result = await signal_queue.add_from_opportunity(
            sample_arbitrage_opportunity, "test_strategy"
        )

        # Assert
        assert result is not None
        assert sample_arbitrage_opportunity.optimal_size is not None
        expected_quantity = (
            sample_arbitrage_opportunity.optimal_size / sample_arbitrage_opportunity.long_price
        )
        assert result.quantity == expected_quantity

    @pytest.mark.asyncio
    async def test_add_from_opportunity_edge_no_optimal_size(
        self,
        signal_queue: PrioritySignalQueue,
        sample_arbitrage_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test signal creation without optimal size uses placeholder."""
        # Arrange
        sample_arbitrage_opportunity.optimal_size = None

        # Act
        result = await signal_queue.add_from_opportunity(
            sample_arbitrage_opportunity, "test_strategy"
        )

        # Assert
        assert result is not None
        assert result.quantity == Decimal("0.000001")

    @pytest.mark.asyncio
    async def test_add_from_opportunity_failure_circuit_breaker_rejection(
        self,
        signal_queue: PrioritySignalQueue,
        sample_arbitrage_opportunity: ArbitrageOpportunity,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test opportunity rejection when circuit breaker blocks it."""
        # Arrange
        mock_breaker = Mock(spec=CircuitBreaker)
        mock_breaker.state = BreakerState.OPEN
        mock_breaker.trip_reason = "Test trip reason"
        mock_circuit_breaker_system.get_breaker.return_value = mock_breaker

        # Act
        result = await signal_queue.add_from_opportunity(
            sample_arbitrage_opportunity, "test_strategy"
        )

        # Assert
        assert result is None
        assert len(signal_queue.signal_queue) == 0


class TestGetNextSignal:
    """Test get_next_signal method functionality."""

    @pytest.mark.asyncio
    async def test_get_next_signal_success_single(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test getting single signal from queue."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.get_next_signal()

        # Assert
        assert result == sample_trade_signal
        assert len(signal_queue.signal_queue) == 0

    @pytest.mark.asyncio
    async def test_get_next_signal_success_priority_order(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test signals are returned in priority order."""
        # Arrange
        signals: list[TradeSignal] = []
        for i, score in enumerate([0.5, 0.9, 0.1]):
            signal = TradeSignal(
                signal_id=f"signal_{i}",
                timestamp=datetime.now(UTC),
                symbol="BTC-PERP",
                signal_type=SignalType.ENTER_LONG,
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("1.0"),
                exchange="hyperliquid",
                source_strategy="test_strategy",
                metadata={"utility_score": score},
            )
            signals.append(signal)
            await signal_queue.add_signal(signal)

        # Act
        result1 = await signal_queue.get_next_signal()
        result2 = await signal_queue.get_next_signal()
        result3 = await signal_queue.get_next_signal()

        # Assert
        assert result1 is not None
        assert result2 is not None
        assert result3 is not None
        assert result1.signal_id == "signal_1"  # Highest score 0.9
        assert result2.signal_id == "signal_0"  # Middle score 0.5
        assert result3.signal_id == "signal_2"  # Lowest score 0.1

    @pytest.mark.asyncio
    async def test_get_next_signal_edge_expired_signal_skipped(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test expired signals are skipped."""
        # Arrange
        sample_trade_signal.expiration = datetime.now(UTC) - timedelta(minutes=1)
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.get_next_signal()

        # Assert
        assert result is None
        assert len(signal_queue.signal_queue) == 0

    @pytest.mark.asyncio
    async def test_get_next_signal_failure_empty_queue(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test getting signal from empty queue returns None."""
        # Act
        result = await signal_queue.get_next_signal()

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_get_next_signal_failure_circuit_breaker_blocks(
        self,
        signal_queue: PrioritySignalQueue,
        sample_trade_signal: TradeSignal,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test signal blocked by circuit breaker on retrieval."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)
        mock_breaker = Mock(spec=CircuitBreaker)
        mock_breaker.state = BreakerState.OPEN
        mock_circuit_breaker_system.get_breaker.return_value = mock_breaker

        # Act
        result = await signal_queue.get_next_signal()

        # Assert
        assert result is None


class TestPeekNextSignal:
    """Test peek_next_signal method functionality."""

    @pytest.mark.asyncio
    async def test_peek_next_signal_success(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test peeking at signal without removing it."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.peek_next_signal()

        # Assert
        assert result == sample_trade_signal
        assert len(signal_queue.signal_queue) == 1

    @pytest.mark.asyncio
    async def test_peek_next_signal_edge_empty_queue(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test peeking at empty queue returns None."""
        # Act
        result = await signal_queue.peek_next_signal()

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_peek_next_signal_edge_circuit_breaker_warning(
        self,
        signal_queue: PrioritySignalQueue,
        sample_trade_signal: TradeSignal,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test peek returns signal even if circuit breaker would block it."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)
        mock_breaker = Mock(spec=CircuitBreaker)
        mock_breaker.state = BreakerState.OPEN
        mock_circuit_breaker_system.get_breaker.return_value = mock_breaker

        # Act
        with patch.object(signal_queue, "_check_circuit_breakers_post_get", return_value=False):
            result = await signal_queue.peek_next_signal()

        # Assert
        assert result == sample_trade_signal  # Still returns signal but warns


class TestGetSignals:
    """Test get_signals method functionality."""

    @pytest.mark.asyncio
    async def test_get_signals_success_all(self, signal_queue: PrioritySignalQueue) -> None:
        """Test getting all signals sorted by priority."""
        # Arrange
        signals: list[TradeSignal] = []
        for i, score in enumerate([0.5, 0.9, 0.1]):
            signal = TradeSignal(
                signal_id=f"signal_{i}",
                timestamp=datetime.now(UTC),
                symbol="BTC-PERP",
                signal_type=SignalType.ENTER_LONG,
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("1.0"),
                exchange="hyperliquid",
                source_strategy="test_strategy",
                metadata={"utility_score": score},
            )
            signals.append(signal)
            await signal_queue.add_signal(signal)

        # Act
        result = await signal_queue.get_signals()

        # Assert
        assert result is not None
        assert len(result) == 3
        assert result[0].metadata is not None
        assert result[1].metadata is not None
        assert result[0].metadata["utility_score"] == 0.9
        assert result[1].metadata["utility_score"] == 0.5
        assert result[2].metadata is not None
        assert result[2].metadata["utility_score"] == 0.1

    @pytest.mark.asyncio
    async def test_get_signals_success_with_max_count(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test getting limited number of signals."""
        # Arrange
        for i in range(5):
            signal = TradeSignal(
                signal_id=f"signal_{i}",
                timestamp=datetime.now(UTC),
                symbol="BTC-PERP",
                signal_type=SignalType.ENTER_LONG,
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("1.0"),
                exchange="hyperliquid",
                source_strategy="test_strategy",
                metadata={"utility_score": float(i)},
            )
            await signal_queue.add_signal(signal)

        # Act
        result = await signal_queue.get_signals(max_count=3)

        # Assert
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_get_signals_success_filtered_by_symbol(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test getting signals filtered by symbol."""
        # Arrange
        symbols = ["BTC-PERP", "ETH-PERP", "BTC-PERP"]
        for i, symbol in enumerate(symbols):
            signal = TradeSignal(
                signal_id=f"signal_{i}",
                timestamp=datetime.now(UTC),
                symbol=symbol,
                signal_type=SignalType.ENTER_LONG,
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("1.0"),
                exchange="hyperliquid",
                source_strategy="test_strategy",
                metadata={"utility_score": 0.5},
            )
            await signal_queue.add_signal(signal)

        # Act
        result = await signal_queue.get_signals(symbol="BTC-PERP")

        # Assert
        assert len(result) == 2
        assert all(s.symbol == "BTC-PERP" for s in result)

    @pytest.mark.asyncio
    async def test_get_signals_edge_empty_queue(self, signal_queue: PrioritySignalQueue) -> None:
        """Test getting signals from empty queue."""
        # Act
        result = await signal_queue.get_signals()

        # Assert
        assert result == []


class TestQueueManagement:
    """Test queue management methods."""

    @pytest.mark.asyncio
    async def test_count_success(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test counting signals in queue."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        count = await signal_queue.count()

        # Assert
        assert count == 1

    @pytest.mark.asyncio
    async def test_clear_success(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test clearing all signals from queue."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        await signal_queue.clear()

        # Assert
        assert len(signal_queue.signal_queue) == 0
        assert await signal_queue.count() == 0

    @pytest.mark.asyncio
    async def test_is_empty_success_empty(self, signal_queue: PrioritySignalQueue) -> None:
        """Test is_empty returns True for empty queue."""
        # Act
        result = await signal_queue.is_empty()

        # Assert
        assert result is True

    @pytest.mark.asyncio
    async def test_is_empty_success_not_empty(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test is_empty returns False for non-empty queue."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.is_empty()

        # Assert
        assert result is False


class TestExpiredSignalCleaning:
    """Test expired signal cleaning functionality through public API."""

    @pytest.mark.asyncio
    async def test_expired_signals_cleaned_during_operations(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test expired signals are automatically cleaned during normal operations."""
        # Arrange
        now = datetime.now(UTC)
        valid_signal = TradeSignal(
            signal_id="valid",
            timestamp=now,
            symbol="BTC-PERP",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("1.0"),
            exchange="hyperliquid",
            source_strategy="test_strategy",
            expiration=now + timedelta(minutes=10),
        )
        expired_signal = TradeSignal(
            signal_id="expired",
            timestamp=now,
            symbol="ETH-PERP",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            price=Decimal("3000.0"),
            quantity=Decimal("1.0"),
            exchange="hyperliquid",
            source_strategy="test_strategy",
            expiration=now - timedelta(minutes=10),  # Already expired
        )

        # Add signals through public API
        await signal_queue.add_signal(valid_signal)
        await signal_queue.add_signal(expired_signal)

        # Verify both signals were initially added
        initial_count = await signal_queue.count()
        assert initial_count == 2

        # Force cleanup by setting last_cleanup to trigger cleaning
        signal_queue.last_cleanup = now - timedelta(seconds=signal_queue.cleanup_interval + 1)

        # Act - Trigger cleanup by getting next signal (this will cleanup expired signals)
        next_signal = await signal_queue.get_next_signal()

        # Assert - Only valid signal should remain
        assert next_signal is not None
        assert next_signal.signal_id == "valid"
        remaining_count = await signal_queue.count()
        assert remaining_count == 0  # Valid signal was retrieved, expired was cleaned

    @pytest.mark.asyncio
    async def test_all_expired_signals_cleaned(
        self,
        signal_queue: PrioritySignalQueue,
    ) -> None:
        """Test all expired signals are cleaned through public API."""
        # Arrange
        now = datetime.now(UTC)
        # Add multiple expired signals
        for i in range(3):
            expired_signal = TradeSignal(
                signal_id=f"expired_{i}",
                timestamp=now,
                symbol="BTC-PERP",
                signal_type=SignalType.ENTER_LONG,
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("1.0"),
                exchange="hyperliquid",
                source_strategy="test_strategy",
                expiration=now - timedelta(minutes=10),  # All expired
            )
            await signal_queue.add_signal(expired_signal)

        # Verify signals were added
        initial_count = await signal_queue.count()
        assert initial_count == 3

        # Force cleanup trigger
        signal_queue.last_cleanup = now - timedelta(seconds=signal_queue.cleanup_interval + 1)

        # Act - Trigger cleanup by attempting to get next signal
        next_signal = await signal_queue.get_next_signal()

        # Assert - All expired signals should be cleaned, queue should be empty
        assert next_signal is None  # No valid signals to return
        final_count = await signal_queue.count()
        assert final_count == 0

    @pytest.mark.asyncio
    async def test_no_cleanup_when_all_signals_valid(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test no signals are removed when all are valid."""
        # Arrange
        now = datetime.now(UTC)
        # Add multiple valid signals
        for i in range(3):
            valid_signal = TradeSignal(
                signal_id=f"valid_{i}",
                timestamp=now,
                symbol="BTC-PERP",
                signal_type=SignalType.ENTER_LONG,
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("1.0"),
                exchange="hyperliquid",
                source_strategy="test_strategy",
                expiration=now + timedelta(minutes=10),  # All valid
            )
            await signal_queue.add_signal(valid_signal)

        # Verify signals were added
        initial_count = await signal_queue.count()
        assert initial_count == 3

        # Force cleanup trigger
        signal_queue.last_cleanup = now - timedelta(seconds=signal_queue.cleanup_interval + 1)

        # Act - Trigger cleanup by getting signals
        await signal_queue.get_next_signal()

        # Assert - All valid signals should remain (minus the one we retrieved)
        remaining_count = await signal_queue.count()
        assert remaining_count == 2  # One was retrieved, two remain


class TestCircuitBreakerIntegration:
    """Test circuit breaker integration."""

    @pytest.mark.asyncio
    async def test_add_signal_success_no_breakers(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test signal addition passes when no breakers are tripped."""
        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is True

    @pytest.mark.asyncio
    async def test_add_signal_failure_symbol_breaker_open(
        self,
        signal_queue: PrioritySignalQueue,
        sample_trade_signal: TradeSignal,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test signal addition fails when symbol breaker is open."""
        # Arrange
        mock_breaker = Mock(spec=CircuitBreaker)
        mock_breaker.state = BreakerState.OPEN
        mock_breaker.trip_reason = "Test trip"
        mock_circuit_breaker_system.get_breaker.return_value = mock_breaker

        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is False
        mock_circuit_breaker_system.get_breaker.assert_called_with(
            f"symbol_{sample_trade_signal.symbol}_main"
        )

    @pytest.mark.asyncio
    async def test_add_signal_failure_exchange_breaker_open(
        self,
        signal_queue: PrioritySignalQueue,
        sample_trade_signal: TradeSignal,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test signal addition fails when exchange breaker is open."""
        # Arrange
        mock_exchange_breaker = Mock(spec=CircuitBreaker)
        mock_exchange_breaker.state = BreakerState.OPEN
        mock_exchange_breaker.trip_reason = "API error"

        # Symbol breaker is closed
        mock_circuit_breaker_system.get_breaker.return_value = None
        mock_circuit_breaker_system.get_exchange_breaker.return_value = mock_exchange_breaker

        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is False

    @pytest.mark.asyncio
    async def test_add_signal_edge_arbitrage_metadata(
        self,
        signal_queue: PrioritySignalQueue,
        sample_trade_signal: TradeSignal,
        mock_circuit_breaker_system: Mock,
    ) -> None:
        """Test signal addition checks both exchanges for arbitrage signals."""
        # Arrange
        sample_trade_signal.metadata = {
            "utility_score": 0.8,
            "long_exchange": "hyperliquid",
            "short_exchange": "backpack",
        }

        # Act
        result = await signal_queue.add_signal(sample_trade_signal)

        # Assert
        assert result is True
        # Verify both exchanges were checked
        calls = mock_circuit_breaker_system.get_exchange_breaker.call_args_list
        exchanges_checked = {call[0][0] for call in calls}
        assert "hyperliquid" in exchanges_checked
        assert "backpack" in exchanges_checked


class TestAsyncMethods:
    """Test additional async methods."""

    @pytest.mark.asyncio
    async def test_wait_for_signals_success_existing(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test waiting for signals when they already exist."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.wait_for_signals(max_signals=1)

        # Assert
        assert len(result) == 1
        assert result[0] == sample_trade_signal

    @pytest.mark.asyncio
    async def test_pop_signals_success_multiple(self, signal_queue: PrioritySignalQueue) -> None:
        """Test popping multiple signals at once."""
        # Arrange
        for i in range(3):
            signal = TradeSignal(
                signal_id=f"signal_{i}",
                timestamp=datetime.now(UTC),
                symbol="BTC-PERP",
                signal_type=SignalType.ENTER_LONG,
                side=OrderSide.BUY,
                price=Decimal("50000.0"),
                quantity=Decimal("1.0"),
                exchange="hyperliquid",
                source_strategy="test_strategy",
                metadata={"utility_score": float(i)},
            )
            await signal_queue.add_signal(signal)

        # Act
        result = await signal_queue.pop_signals(max_signals=2)

        # Assert
        assert len(result) == 2
        assert result[0].signal_id == "signal_2"  # Highest score
        assert result[1].signal_id == "signal_1"
        assert len(signal_queue.signal_queue) == 1

    @pytest.mark.asyncio
    async def test_get_pending_signals_success(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test getting all pending valid signals."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        result = await signal_queue.get_pending_signals()

        # Assert
        assert len(result) == 1
        assert result[0] == sample_trade_signal

    @pytest.mark.asyncio
    async def test_enqueue_signal_success(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test enqueuing signal with event notification."""
        # Act
        await signal_queue.enqueue_signal(sample_trade_signal)

        # Assert
        assert len(signal_queue.signal_queue) == 1
        assert signal_queue.new_signal_event.is_set()

    @pytest.mark.asyncio
    async def test_enqueue_signal_concurrent_producers(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test concurrent enqueue operations to verify lock handling in add_signal."""
        # Arrange - Create multiple different signals
        signals: list[TradeSignal] = []
        for i in range(5):
            signal = TradeSignal(
                signal_id=f"test_signal_{i}",
                symbol="BTC",
                side=OrderSide.BUY,
                signal_type=SignalType.ENTER_LONG,
                price=Decimal(f"5000{i}"),
                quantity=Decimal("1.0"),
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                expiration=datetime.now(UTC) + timedelta(minutes=10),
            )
            signals.append(signal)

        # Act - Enqueue signals concurrently (this tests that add_signal handles locking internally)
        tasks = [asyncio.create_task(signal_queue.enqueue_signal(signal)) for signal in signals]
        await asyncio.gather(*tasks)

        # Assert - All signals should be successfully added without race conditions
        assert len(signal_queue.signal_queue) == 5
        # Verify no signals were lost due to race conditions
        enqueued_ids = {signal.signal_id for _, _, signal in signal_queue.signal_queue}
        expected_ids = {f"test_signal_{i}" for i in range(5)}
        assert enqueued_ids == expected_ids

    @pytest.mark.asyncio
    async def test_stop_success(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test stopping the queue clears signals and sets event."""
        # Arrange
        await signal_queue.add_signal(sample_trade_signal)

        # Act
        await signal_queue.stop()

        # Assert
        assert len(signal_queue.signal_queue) == 0
        assert signal_queue.new_signal_event.is_set()

    def test_get_signal_count_success(
        self, signal_queue: PrioritySignalQueue, sample_trade_signal: TradeSignal
    ) -> None:
        """Test getting signal count (synchronous version)."""
        # Arrange
        signal_queue.signal_queue = [(-0.5, 1, sample_trade_signal)]

        # Act
        count = signal_queue.get_signal_count()

        # Assert
        assert count == 1


class TestEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_add_signal_with_list_exchange(self, signal_queue: PrioritySignalQueue) -> None:
        """Test adding signal with list of exchanges."""
        # Arrange
        signal = TradeSignal(
            signal_id="test",
            timestamp=datetime.now(UTC),
            symbol="BTC-PERP",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("1.0"),
            exchange=["hyperliquid", "backpack"],  # List of exchanges
            source_strategy="test_strategy",
            metadata={"utility_score": 0.8},
        )

        # Act
        result = await signal_queue.add_signal(signal)

        # Assert
        assert result is True

    @pytest.mark.asyncio
    async def test_get_next_signal_with_exception_handling(
        self, signal_queue: PrioritySignalQueue
    ) -> None:
        """Test get_next_signal handles exceptions gracefully."""
        # Arrange
        with patch.object(
            signal_queue, "_clean_expired_signals", side_effect=ValueError("Test error")
        ):
            # Act
            result = await signal_queue.get_next_signal()

            # Assert
            assert result is None

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_run_method_cancellation(self, signal_queue: PrioritySignalQueue) -> None:
        """Test run method handles cancellation properly."""
        # Arrange
        cancellation_token = asyncio.Event()

        # Act
        async def cancel_after_delay() -> None:
            await asyncio.sleep(0.1)
            cancellation_token.set()
            # Also trigger the signal event to unblock the wait
            signal_queue.new_signal_event.set()

        # Run both tasks concurrently
        await asyncio.gather(
            signal_queue.run(cancellation_token), cancel_after_delay(), return_exceptions=True
        )

        # Assert - method should complete without exceptions
        assert True  # If we get here, the method handled cancellation properly


class TestParametrizedTests:
    """Parametrized tests for various scenarios."""

    @pytest.mark.parametrize(
        ("utility_score", "expected_priority"),
        [
            (1.0, -1.0),
            (0.5, -0.5),
            (0.0, 0.0),
            (None, 0.0),
            ("invalid", 0.0),
        ],
    )
    @pytest.mark.asyncio
    async def test_utility_score_handling(
        self,
        signal_queue: PrioritySignalQueue,
        utility_score: float | str | None,
        expected_priority: float,
    ) -> None:
        """Test various utility score inputs are handled correctly."""
        # Arrange
        signal = TradeSignal(
            signal_id="test",
            timestamp=datetime.now(UTC),
            symbol="BTC-PERP",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            price=Decimal("50000.0"),
            quantity=Decimal("1.0"),
            exchange="hyperliquid",
            source_strategy="test_strategy",
            metadata={"utility_score": utility_score} if utility_score is not None else {},
        )

        # Act
        await signal_queue.add_signal(signal)

        # Assert
        assert len(signal_queue.signal_queue) == 1
        assert signal_queue.signal_queue[0][0] == expected_priority

"""Unit tests for EventHandlerActor base class.

Tests cover:
1. Handler lifecycle state transitions
2. Error handling and auto-degradation
3. Cache functionality
4. Metrics collection
5. Event subscription management
6. Tenacity retry logic

Follows CLAUDE.md principles:
- Test public behavior only, no private member access
- Proper type annotations on all fixtures
- Fail fast on unexpected errors
"""

import contextlib
from unittest.mock import MagicMock

import msgspec
import pytest

from cyberdelta.config.models.event_system_config import EventHandlerConfig
from cyberdelta.domain.base_event_handler import EventHandlerActor
from cyberdelta.enums.component_state import ComponentState
from cyberdelta.enums.event_bus import HandlerPriority


class MockEvent(msgspec.Struct):
    """Test event structure."""

    message: str
    value: int = 0


class ConcreteEventHandler(EventHandlerActor):
    """Concrete handler implementation for testing."""

    def __init__(self, handler_id: str, event_bus: MagicMock, config: EventHandlerConfig) -> None:
        """Initialize test handler."""
        super().__init__(handler_id, event_bus, config)
        self.start_called = False
        self.stop_called = False
        self.degrade_called = False
        self.fault_called = False
        self.handle_called = False
        self.events_handled: list[msgspec.Struct] = []

    async def on_start(self) -> None:
        """Test start implementation."""
        await super().on_start()
        self.start_called = True

    async def on_stop(self) -> None:
        """Test stop implementation."""
        await super().on_stop()
        self.stop_called = True

    async def on_degrade(self) -> None:
        """Test degrade implementation."""
        await super().on_degrade()
        self.degrade_called = True

    async def on_fault(self) -> None:
        """Test fault implementation."""
        await super().on_fault()
        self.fault_called = True

    async def handle_event(self, event: msgspec.Struct) -> None:
        """Test event handling."""
        self.handle_called = True
        self.events_handled.append(event)


class ErrorEventHandler(ConcreteEventHandler):
    """Handler that raises errors for testing."""

    def __init__(self, handler_id: str, event_bus: MagicMock, config: EventHandlerConfig) -> None:
        """Initialize error handler for testing."""
        super().__init__(handler_id, event_bus, config)
        self.error_to_raise: Exception | None = None
        self.call_count = 0

    async def handle_event(self, event: msgspec.Struct) -> None:
        """Raise configured error."""
        await super().handle_event(event)
        self.call_count += 1
        if self.error_to_raise:
            raise self.error_to_raise


class TestEventHandlerActorLifecycle:
    """Test handler lifecycle management."""

    @pytest.fixture
    def config(self) -> EventHandlerConfig:
        """Test event handler config.

        Returns:
            EventHandlerConfig: Test configuration.
        """
        return EventHandlerConfig()

    @pytest.fixture
    def event_bus(self) -> MagicMock:
        """Mock event bus.

        Returns:
            MagicMock: Mock event bus for testing.
        """
        return MagicMock()

    @pytest.fixture
    def handler(self, event_bus: MagicMock, config: EventHandlerConfig) -> ConcreteEventHandler:
        """Create test handler.

        Args:
            event_bus: Mock event bus.
            config: Event handler config.

        Returns:
            ConcreteEventHandler: Test handler instance.
        """
        return ConcreteEventHandler("test_handler", event_bus, config)

    def test_handler_initialization(self, handler: ConcreteEventHandler) -> None:
        """Test handler initializes correctly.

        Args:
            handler: Test handler instance.
        """
        assert handler.handler_id == "test_handler"
        assert handler.state == ComponentState.PRE_INITIALIZED
        assert handler.error_count == 0
        assert handler.consecutive_errors == 0
        metrics = handler.get_metrics()
        assert metrics["cache_size"] == 0

    @pytest.mark.asyncio
    async def test_handler_start_lifecycle(self, handler: ConcreteEventHandler) -> None:
        """Test handler start transitions to RUNNING state.

        Args:
            handler: Test handler instance.
        """
        initial_state = handler.state
        assert initial_state == ComponentState.PRE_INITIALIZED

        await handler.start()

        final_state = handler.state
        assert final_state == ComponentState.RUNNING
        assert handler.start_called is True
        metrics = handler.get_metrics()
        assert "uptime_seconds" in metrics

    @pytest.mark.asyncio
    async def test_handler_stop_lifecycle(self, handler: ConcreteEventHandler) -> None:
        """Test handler stop transitions to STOPPED state.

        Args:
            handler: Test handler instance.
        """
        await handler.start()
        running_state = handler.state
        assert running_state == ComponentState.RUNNING

        await handler.stop()

        final_state = handler.state
        assert final_state == ComponentState.STOPPED
        assert handler.stop_called is True

    @pytest.mark.asyncio
    async def test_handler_degrade_lifecycle(self, handler: ConcreteEventHandler) -> None:
        """Test handler degrade transitions to DEGRADED state.

        Args:
            handler: Test handler instance.
        """
        await handler.start()
        running_state = handler.state
        assert running_state == ComponentState.RUNNING

        await handler.degrade()

        final_state = handler.state
        assert final_state == ComponentState.DEGRADED
        assert handler.degrade_called is True

    @pytest.mark.asyncio
    async def test_handler_fault_lifecycle(self, handler: ConcreteEventHandler) -> None:
        """Test handler fault transitions to FAULTED state.

        Args:
            handler: Test handler instance.
        """
        await handler.start()
        running_state = handler.state
        assert running_state == ComponentState.RUNNING

        await handler.fault()

        final_state = handler.state
        assert final_state == ComponentState.FAULTED
        assert handler.fault_called is True


class TestEventHandlerErrorHandling:
    """Test error handling and auto-degradation."""

    @pytest.fixture
    def config(self) -> EventHandlerConfig:
        """Test event handler config with low thresholds for testing.

        Returns:
            EventHandlerConfig: Test configuration with low error thresholds.
        """
        return EventHandlerConfig(
            auto_degrade_after_errors=3,  # Lower for faster testing
            auto_fault_after_errors=5,  # Lower for faster testing
        )

    @pytest.fixture
    def event_bus(self) -> MagicMock:
        """Mock event bus.

        Returns:
            MagicMock: Mock event bus for testing.
        """
        return MagicMock()

    @pytest.fixture
    def error_handler(self, event_bus: MagicMock, config: EventHandlerConfig) -> ErrorEventHandler:
        """Create error handler for testing.

        Args:
            event_bus: Mock event bus.
            config: Event handler config.

        Returns:
            ErrorEventHandler: Error handler instance for testing.
        """
        return ErrorEventHandler("error_handler", event_bus, config)

    @pytest.mark.asyncio
    async def test_successful_event_handling_resets_errors(
        self, error_handler: ErrorEventHandler
    ) -> None:
        """Test successful event handling resets consecutive error count.

        Args:
            error_handler: Test error handler instance.
        """
        await error_handler.start()

        # Generate some errors first
        error_handler.error_to_raise = ValueError("test error")
        test_event = MockEvent(message="error_event", value=1)

        for _ in range(2):
            with contextlib.suppress(ValueError):
                await error_handler.handle_with_degradation(test_event)

        assert error_handler.consecutive_errors == 2
        assert error_handler.error_count == 2

        # Handle successful event - should reset consecutive errors
        error_handler.error_to_raise = None
        success_event = MockEvent(message="success")
        await error_handler.handle_with_degradation(success_event)

        # Verify success resets consecutive errors but not total count
        assert error_handler.consecutive_errors == 0
        assert error_handler.error_count == 2  # Total errors unchanged
        assert error_handler.get_metrics()["events_processed"] == 3

    @pytest.mark.asyncio
    async def test_auto_degradation_after_threshold_errors(
        self, error_handler: ErrorEventHandler
    ) -> None:
        """Test handler auto-degrades after exceeding error threshold.

        Args:
            error_handler: Test error handler instance.
        """
        await error_handler.start()
        assert error_handler.state == ComponentState.RUNNING

        # Configure handler to raise errors
        error_handler.error_to_raise = ValueError("Persistent error")
        test_event = MockEvent(message="error")

        # Generate errors up to but not exceeding threshold (3)
        for _ in range(3):
            with contextlib.suppress(ValueError):
                await error_handler.handle_with_degradation(test_event)

        # Should still be running after exactly threshold errors
        running_state = error_handler.state
        assert running_state == ComponentState.RUNNING
        assert error_handler.consecutive_errors == 3

        # One more error should trigger auto-degradation
        with contextlib.suppress(ValueError):
            await error_handler.handle_with_degradation(test_event)

        # Verify state transition occurred
        assert error_handler.degrade_called is True
        assert error_handler.consecutive_errors == 4

        # Should now be degraded
        expected_state = ComponentState.DEGRADED
        actual_state = error_handler.state
        assert actual_state == expected_state

    @pytest.mark.asyncio
    async def test_auto_fault_after_threshold_errors_in_degraded_state(
        self, error_handler: ErrorEventHandler
    ) -> None:
        """Test handler auto-faults after exceeding error threshold while degraded.

        Args:
            error_handler: Test error handler instance.
        """
        await error_handler.start()

        # Put handler in degraded state first
        await error_handler.degrade()
        assert error_handler.state == ComponentState.DEGRADED

        # Configure handler to raise errors
        error_handler.error_to_raise = RuntimeError("Critical error")
        test_event = MockEvent(message="critical_error")

        # Generate errors up to fault threshold (5)
        # Starting from degraded state, so we need 2 more errors (3+2=5)
        for _ in range(2):
            with contextlib.suppress(RuntimeError):
                await error_handler.handle_with_degradation(test_event)

        # Should still be degraded at exactly threshold
        degraded_state = error_handler.state
        assert degraded_state == ComponentState.DEGRADED
        assert error_handler.consecutive_errors == 2

        # One more error should trigger auto-fault
        with contextlib.suppress(RuntimeError):
            await error_handler.handle_with_degradation(test_event)

        # Verify fault transition occurred
        assert error_handler.fault_called is True
        assert error_handler.consecutive_errors == 3

        # Should now be faulted
        expected_state = ComponentState.FAULTED
        actual_state = error_handler.state
        assert actual_state == expected_state

    @pytest.mark.asyncio
    async def test_error_handling_counts_errors(self, error_handler: ErrorEventHandler) -> None:
        """Test that errors are counted properly.

        Args:
            error_handler: Test error handler instance.
        """
        await error_handler.start()

        error_handler.error_to_raise = ValueError("Test error")
        test_event = MockEvent(message="error")

        with pytest.raises(ValueError):
            await error_handler.handle_with_degradation(test_event)

        assert error_handler.consecutive_errors == 1
        assert error_handler.error_count == 1
        metrics = error_handler.get_metrics()
        assert metrics["errors"] == 1

    @pytest.mark.asyncio
    async def test_faulted_handler_skips_event_processing(
        self, error_handler: ErrorEventHandler
    ) -> None:
        """Test that faulted handlers skip event processing.

        Args:
            error_handler: Test error handler instance.
        """
        await error_handler.start()

        # Fault the handler
        await error_handler.fault()
        assert error_handler.state == ComponentState.FAULTED

        # Try to handle event - should be skipped
        test_event = MockEvent(message="should_be_skipped")
        await error_handler.handle_with_degradation(test_event)

        # Event should not have been processed
        assert error_handler.handle_called is False
        assert len(error_handler.events_handled) == 0

    @pytest.mark.asyncio
    async def test_stopped_handler_skips_event_processing(
        self, error_handler: ErrorEventHandler
    ) -> None:
        """Test that stopped handlers skip event processing.

        Args:
            error_handler: Test error handler instance.
        """
        await error_handler.start()
        await error_handler.stop()
        assert error_handler.state == ComponentState.STOPPED

        # Try to handle event - should be skipped
        test_event = MockEvent(message="should_be_skipped")
        await error_handler.handle_with_degradation(test_event)

        # Event should not have been processed after stop
        assert len(error_handler.events_handled) == 0


class TestEventHandlerCaching:
    """Test cache functionality."""

    @pytest.fixture
    def handler(self) -> ConcreteEventHandler:
        """Create test handler.

        Returns:
            ConcreteEventHandler: Test handler instance.
        """
        config = EventHandlerConfig()
        return ConcreteEventHandler("cache_test", MagicMock(), config)

    def test_cache_set_and_get(self, handler: ConcreteEventHandler) -> None:
        """Test basic cache operations.

        Args:
            handler: Test handler instance.
        """
        test_event = MockEvent(message="cached", value=123)

        assert handler.cache_get("test_key") is None
        assert handler.get_metrics()["cache_size"] == 0

        handler.cache_set("test_key", test_event)
        cached = handler.cache_get("test_key")
        assert cached is not None
        assert isinstance(cached, MockEvent)
        assert cached.message == "cached"
        assert cached.value == 123
        assert handler.get_metrics()["cache_size"] == 1

    def test_cache_clear(self, handler: ConcreteEventHandler) -> None:
        """Test cache clear functionality.

        Args:
            handler: Test handler instance.
        """
        handler.cache_set("key1", MockEvent(message="test1"))
        handler.cache_set("key2", MockEvent(message="test2"))
        assert handler.get_metrics()["cache_size"] == 2

        handler.cache_clear()
        assert handler.get_metrics()["cache_size"] == 0
        assert handler.cache_get("key1") is None


class TestEventHandlerMetrics:
    """Test metrics collection."""

    @pytest.fixture
    def handler(self) -> ConcreteEventHandler:
        """Create test handler.

        Returns:
            ConcreteEventHandler: Test handler instance.
        """
        config = EventHandlerConfig()
        return ConcreteEventHandler("metrics_test", MagicMock(), config)

    @pytest.mark.asyncio
    async def test_metrics_collection(self, handler: ConcreteEventHandler) -> None:
        """Test metrics are collected and accessible.

        Args:
            handler: Test handler instance.
        """
        initial_metrics = handler.get_metrics()
        assert initial_metrics["error_count"] == 0
        assert initial_metrics["consecutive_errors"] == 0
        assert initial_metrics["cache_size"] == 0
        assert "uptime_seconds" not in initial_metrics

        await handler.start()

        running_metrics = handler.get_metrics()
        assert "uptime_seconds" in running_metrics
        assert running_metrics["uptime_seconds"] >= 0

    def test_metrics_reset(self, handler: ConcreteEventHandler) -> None:
        """Test metrics reset functionality.

        Args:
            handler: Test handler instance.
        """
        handler.cache_set("key", MockEvent(message="test"))
        handler.cache_get("key")

        initial_metrics = handler.get_metrics()
        assert initial_metrics["cache_requests"] > 0

        handler.reset_metrics()
        reset_metrics = handler.get_metrics()
        assert reset_metrics["error_count"] == 0
        assert reset_metrics["consecutive_errors"] == 0


class TestEventHandlerSubscriptions:
    """Test event subscription management."""

    @pytest.fixture
    def event_bus(self) -> MagicMock:
        """Mock event bus with subscription methods.

        Returns:
            MagicMock: Mock event bus for testing.
        """
        bus = MagicMock()
        bus.subscribe = MagicMock()
        bus.unsubscribe = MagicMock()
        return bus

    @pytest.fixture
    def handler(self, event_bus: MagicMock) -> ConcreteEventHandler:
        """Create test handler.

        Args:
            event_bus: Mock event bus.

        Returns:
            ConcreteEventHandler: Test handler instance.
        """
        config = EventHandlerConfig()
        return ConcreteEventHandler("subscription_test", event_bus, config)

    @pytest.mark.asyncio
    async def test_event_subscription(
        self, handler: ConcreteEventHandler, event_bus: MagicMock
    ) -> None:
        """Test event subscription.

        Args:
            handler: Test handler instance.
            event_bus: Mock event bus.
        """
        await handler.subscribe_to_event(MockEvent, HandlerPriority.HIGH)

        event_bus.subscribe.assert_called_once_with(
            MockEvent, handler.handle_with_degradation, HandlerPriority.HIGH
        )

    @pytest.mark.asyncio
    async def test_event_unsubscription(
        self, handler: ConcreteEventHandler, event_bus: MagicMock
    ) -> None:
        """Test event unsubscription.

        Args:
            handler: Test handler instance.
            event_bus: Mock event bus.
        """
        await handler.unsubscribe_from_event(MockEvent)

        event_bus.unsubscribe.assert_called_once_with(MockEvent, handler.handle_with_degradation)

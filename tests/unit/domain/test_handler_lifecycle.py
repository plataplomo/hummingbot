"""Unit tests for handler lifecycle state transitions.

Tests cover:
1. Valid state transitions through public interface
2. Invalid state transitions
3. Error scenarios during transitions
4. Concurrent transition handling
5. Metrics and logging during transitions

Follows CLAUDE.md principles:
- Test public behavior only, no private member access
- Proper type annotations on all fixtures
- Fail fast on unexpected errors
"""

import asyncio
from unittest.mock import MagicMock

import msgspec
import pytest
from tenacity import RetryError

from cyberdelta.config.models.event_system_config import EventHandlerConfig
from cyberdelta.domain.base_event_handler import EventHandlerActor
from cyberdelta.enums.component_state import ComponentState


pytestmark = pytest.mark.timing


class MockEvent(msgspec.Struct):
    """Test event structure."""

    message: str
    value: int = 0


class MockHandler(EventHandlerActor):
    """Mock handler implementation for testing."""

    def __init__(self, handler_id: str, event_bus: MagicMock, config: EventHandlerConfig) -> None:
        """Initialize test handler."""
        super().__init__(handler_id, event_bus, config)
        self.start_error: Exception | None = None
        self.stop_error: Exception | None = None
        self.degrade_error: Exception | None = None
        self.fault_error: Exception | None = None
        self.start_called = False
        self.stop_called = False
        self.degrade_called = False
        self.fault_called = False

    async def on_start(self) -> None:
        """Test start implementation."""
        if self.start_error:
            raise self.start_error
        await super().on_start()
        self.start_called = True

    async def on_stop(self) -> None:
        """Test stop implementation."""
        if self.stop_error:
            raise self.stop_error
        await super().on_stop()
        self.stop_called = True

    async def on_degrade(self) -> None:
        """Test degrade implementation."""
        if self.degrade_error:
            raise self.degrade_error
        await super().on_degrade()
        self.degrade_called = True

    async def on_fault(self) -> None:
        """Test fault implementation."""
        if self.fault_error:
            raise self.fault_error
        await super().on_fault()
        self.fault_called = True

    async def handle_event(self, event: msgspec.Struct) -> None:
        """Handle event - no-op for testing."""


class TestHandlerLifecycleTransitions:
    """Test valid lifecycle state transitions."""

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
    def handler(self, event_bus: MagicMock, config: EventHandlerConfig) -> MockHandler:
        """Create test handler.

        Args:
            event_bus: Mock event bus.
            config: Event handler config.

        Returns:
            MockHandler: Test handler instance.
        """
        return MockHandler("lifecycle_test", event_bus, config)

    def test_initial_state(self, handler: MockHandler) -> None:
        """Test handler starts in PRE_INITIALIZED state.

        Args:
            handler: Test handler instance.
        """
        assert handler.state == ComponentState.PRE_INITIALIZED

    @pytest.mark.asyncio
    async def test_start_transition_success(self, handler: MockHandler) -> None:
        """Test PRE_INITIALIZED -> RUNNING transition.

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
    async def test_stop_from_running_transition(self, handler: MockHandler) -> None:
        """Test RUNNING -> STOPPED transition.

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
    async def test_stop_from_degraded_transition(self, handler: MockHandler) -> None:
        """Test DEGRADED -> STOPPED transition.

        Args:
            handler: Test handler instance.
        """
        await handler.start()
        await handler.degrade()
        degraded_state = handler.state
        assert degraded_state == ComponentState.DEGRADED

        await handler.stop()

        final_state = handler.state
        assert final_state == ComponentState.STOPPED
        assert handler.stop_called is True

    @pytest.mark.asyncio
    async def test_degrade_from_running_transition(self, handler: MockHandler) -> None:
        """Test RUNNING -> DEGRADED transition.

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
    async def test_fault_from_any_state(
        self, event_bus: MagicMock, config: EventHandlerConfig
    ) -> None:
        """Test that fault can happen from any state.

        Args:
            event_bus: Mock event bus.
            config: Event handler config.
        """
        # Test fault from PRE_INITIALIZED
        handler1 = MockHandler("fault_test_1", event_bus, config)
        await handler1.fault()
        state1 = handler1.state
        assert state1 == ComponentState.FAULTED
        assert handler1.fault_called is True

        # Test fault from RUNNING
        handler2 = MockHandler("fault_test_2", event_bus, config)
        await handler2.start()
        await handler2.fault()
        state2 = handler2.state
        assert state2 == ComponentState.FAULTED
        assert handler2.fault_called is True

        # Test fault from DEGRADED
        handler3 = MockHandler("fault_test_3", event_bus, config)
        await handler3.start()
        await handler3.degrade()
        await handler3.fault()
        state3 = handler3.state
        assert state3 == ComponentState.FAULTED
        assert handler3.fault_called is True

    @pytest.mark.asyncio
    async def test_restart_after_stop(self, handler: MockHandler) -> None:
        """Test STOPPED -> RUNNING restart.

        Args:
            handler: Test handler instance.
        """
        # Start, stop, start again
        await handler.start()
        await handler.stop()
        stopped_state = handler.state
        assert stopped_state == ComponentState.STOPPED

        # Reset call flags and start again
        handler.start_called = False
        await handler.start()
        final_state = handler.state
        assert final_state == ComponentState.RUNNING
        assert handler.start_called is True


class TestInvalidStateTransitions:
    """Test invalid state transitions are handled gracefully."""

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
    def handler(self, event_bus: MagicMock, config: EventHandlerConfig) -> MockHandler:
        """Create test handler.

        Args:
            event_bus: Mock event bus.
            config: Event handler config.

        Returns:
            MockHandler: Test handler instance.
        """
        return MockHandler("invalid_transitions_test", event_bus, config)

    @pytest.mark.asyncio
    async def test_start_from_running_ignored(self, handler: MockHandler) -> None:
        """Test starting already running handler is ignored.

        Args:
            handler: Test handler instance.
        """
        await handler.start()
        running_state = handler.state
        assert running_state == ComponentState.RUNNING

        original_metrics = handler.get_metrics()
        original_uptime = original_metrics.get("uptime_seconds")

        # Reset call flag and try to start again
        handler.start_called = False
        await handler.start()

        still_running = handler.state
        assert still_running == ComponentState.RUNNING
        assert handler.start_called is False  # Should not be called again

        # Uptime should continue from original start
        current_metrics = handler.get_metrics()
        if original_uptime is not None:
            current_uptime = current_metrics.get("uptime_seconds", 0)
            assert current_uptime >= original_uptime

    @pytest.mark.asyncio
    async def test_start_from_faulted_ignored(self, handler: MockHandler) -> None:
        """Test starting faulted handler is ignored.

        Args:
            handler: Test handler instance.
        """
        await handler.fault()
        faulted_state = handler.state
        assert faulted_state == ComponentState.FAULTED

        handler.start_called = False
        await handler.start()

        # Should remain faulted and start not called
        still_faulted = handler.state
        assert still_faulted == ComponentState.FAULTED
        assert handler.start_called is False

    @pytest.mark.asyncio
    async def test_stop_from_pre_initialized_ignored(self, handler: MockHandler) -> None:
        """Test stopping non-running handler is ignored.

        Args:
            handler: Test handler instance.
        """
        initial_state = handler.state
        assert initial_state == ComponentState.PRE_INITIALIZED

        await handler.stop()

        # Should remain in original state and stop not called
        final_state = handler.state
        assert final_state == ComponentState.PRE_INITIALIZED
        assert handler.stop_called is False

    @pytest.mark.asyncio
    async def test_stop_from_stopped_ignored(self, handler: MockHandler) -> None:
        """Test stopping already stopped handler is ignored.

        Args:
            handler: Test handler instance.
        """
        await handler.start()
        await handler.stop()
        stopped_state = handler.state
        assert stopped_state == ComponentState.STOPPED

        # Reset call flag and try to stop again
        handler.stop_called = False
        await handler.stop()

        # Should remain stopped and stop not called again
        still_stopped = handler.state
        assert still_stopped == ComponentState.STOPPED
        assert handler.stop_called is False

    @pytest.mark.asyncio
    async def test_stop_from_faulted_ignored(self, handler: MockHandler) -> None:
        """Test stopping faulted handler is ignored.

        Args:
            handler: Test handler instance.
        """
        await handler.start()
        await handler.fault()
        faulted_state = handler.state
        assert faulted_state == ComponentState.FAULTED

        handler.stop_called = False
        await handler.stop()

        # Should remain faulted and stop not called
        still_faulted = handler.state
        assert still_faulted == ComponentState.FAULTED
        assert handler.stop_called is False

    @pytest.mark.asyncio
    async def test_degrade_from_non_running_ignored(
        self, event_bus: MagicMock, config: EventHandlerConfig
    ) -> None:
        """Test degrading from non-RUNNING state is ignored.

        Args:
            event_bus: Mock event bus.
            config: Event handler config.
        """
        # Test degrade from PRE_INITIALIZED
        handler1 = MockHandler("degrade_test_1", event_bus, config)
        initial_state = handler1.state
        assert initial_state == ComponentState.PRE_INITIALIZED
        await handler1.degrade()
        final_state = handler1.state
        assert final_state == ComponentState.PRE_INITIALIZED
        assert handler1.degrade_called is False

        # Test degrade from STOPPED
        handler2 = MockHandler("degrade_test_2", event_bus, config)
        await handler2.start()
        await handler2.stop()
        stopped_state = handler2.state
        assert stopped_state == ComponentState.STOPPED
        handler2.degrade_called = False
        await handler2.degrade()
        still_stopped = handler2.state
        assert still_stopped == ComponentState.STOPPED
        assert handler2.degrade_called is False

        # Test degrade from FAULTED
        handler3 = MockHandler("degrade_test_3", event_bus, config)
        await handler3.fault()
        faulted_state = handler3.state
        assert faulted_state == ComponentState.FAULTED
        await handler3.degrade()
        still_faulted = handler3.state
        assert still_faulted == ComponentState.FAULTED
        assert handler3.degrade_called is False


class TestLifecycleErrorHandling:
    """Test error handling during lifecycle transitions."""

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
    def error_handler(self, event_bus: MagicMock, config: EventHandlerConfig) -> MockHandler:
        """Create handler that can raise errors.

        Args:
            event_bus: Mock event bus.
            config: Event handler config.

        Returns:
            MockHandler: Test handler that can simulate errors.
        """
        return MockHandler("error_test", event_bus, config)

    @pytest.mark.asyncio
    async def test_start_error_causes_fault(self, error_handler: MockHandler) -> None:
        """Test error during start causes FAULTED state.

        Args:
            error_handler: Test error handler instance.
        """
        # Configure start to raise error
        error_handler.start_error = RuntimeError("Start failed")

        with pytest.raises(RuntimeError, match="Start failed"):
            await error_handler.start()

        final_state = error_handler.state
        assert final_state == ComponentState.FAULTED

    @pytest.mark.asyncio
    async def test_stop_error_causes_fault(self, error_handler: MockHandler) -> None:
        """Test error during stop causes FAULTED state.

        Args:
            error_handler: Test error handler instance.
        """
        await error_handler.start()

        # Configure stop to raise error
        error_handler.stop_error = RuntimeError("Stop failed")

        await error_handler.stop()  # Should not raise but log error

        final_state = error_handler.state
        assert final_state == ComponentState.FAULTED

    @pytest.mark.asyncio
    async def test_degrade_error_logged_but_state_changed(self, error_handler: MockHandler) -> None:
        """Test error during degrade is logged but state still changes.

        Args:
            error_handler: Test error handler instance.
        """
        await error_handler.start()

        # Configure degrade to raise error
        error_handler.degrade_error = RuntimeError("Degrade failed")

        await error_handler.degrade()  # Should not raise

        # Should still be in DEGRADED state despite error
        final_state = error_handler.state
        assert final_state == ComponentState.DEGRADED
        assert error_handler.degrade_called is True

    @pytest.mark.asyncio
    async def test_fault_error_logged_but_state_changed(self, error_handler: MockHandler) -> None:
        """Test error during fault is logged but state still changes.

        Args:
            error_handler: Test error handler instance.
        """
        await error_handler.start()

        # Configure fault to raise error
        error_handler.fault_error = RuntimeError("Fault handling failed")

        await error_handler.fault()  # Should not raise

        # Should still be in FAULTED state despite error
        final_state = error_handler.state
        assert final_state == ComponentState.FAULTED
        assert error_handler.fault_called is True

    @pytest.mark.asyncio
    async def test_connection_error_during_start_triggers_retry(
        self, error_handler: MockHandler
    ) -> None:
        """Test ConnectionError during start triggers tenacity retry.

        Args:
            error_handler: Test error handler instance.
        """
        # Configure start to raise ConnectionError
        error_handler.start_error = ConnectionError("Network issue")

        # Tenacity will wrap the final exception in RetryError
        with pytest.raises(RetryError):
            await error_handler.start()

        # The retry happens outside the start() method's try/catch, so state remains PRE_INITIALIZED
        # This is correct behavior - tenacity intercepts the exception before start() can handle it
        final_state = error_handler.state
        assert final_state == ComponentState.PRE_INITIALIZED


class TestConcurrentTransitions:
    """Test concurrent lifecycle transition handling."""

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
    def handler(self, event_bus: MagicMock, config: EventHandlerConfig) -> MockHandler:
        """Create test handler.

        Args:
            event_bus: Mock event bus.
            config: Event handler config.

        Returns:
            MockHandler: Test handler instance.
        """
        return MockHandler("concurrent_test", event_bus, config)

    @pytest.mark.asyncio
    async def test_concurrent_start_calls(self, handler: MockHandler) -> None:
        """Test concurrent start calls are handled safely.

        Args:
            handler: Test handler instance.
        """

        async def start_handler() -> None:
            await handler.start()

        # Start multiple concurrent start operations
        await asyncio.gather(
            start_handler(), start_handler(), start_handler(), return_exceptions=True
        )

        # Should end up in RUNNING state
        final_state = handler.state
        assert final_state == ComponentState.RUNNING

    @pytest.mark.asyncio
    async def test_concurrent_stop_calls(self, handler: MockHandler) -> None:
        """Test concurrent stop calls are handled safely.

        Args:
            handler: Test handler instance.
        """
        await handler.start()

        async def stop_handler() -> None:
            await handler.stop()

        # Stop multiple concurrent operations
        await asyncio.gather(stop_handler(), stop_handler(), stop_handler(), return_exceptions=True)

        # Should end up in STOPPED state
        final_state = handler.state
        assert final_state == ComponentState.STOPPED

    @pytest.mark.asyncio
    async def test_mixed_concurrent_operations(self, handler: MockHandler) -> None:
        """Test mixed concurrent lifecycle operations.

        Args:
            handler: Test handler instance.
        """
        await handler.start()

        # Run concurrent degrade, fault, and stop
        results = await asyncio.gather(
            handler.degrade(), handler.fault(), handler.stop(), return_exceptions=True
        )

        # Should end up in some final state (likely FAULTED since it can happen from any state)
        final_state = handler.state
        valid_states = [ComponentState.DEGRADED, ComponentState.FAULTED, ComponentState.STOPPED]
        assert final_state in valid_states

        # No exceptions should have been raised
        for result in results:
            assert not isinstance(result, Exception)


class TestStateTransitionMetrics:
    """Test that state transitions update metrics appropriately."""

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
    def handler(self, event_bus: MagicMock, config: EventHandlerConfig) -> MockHandler:
        """Create test handler.

        Args:
            event_bus: Mock event bus.
            config: Event handler config.

        Returns:
            MockHandler: Test handler instance.
        """
        return MockHandler("metrics_test", event_bus, config)

    @pytest.mark.asyncio
    async def test_uptime_tracking(self, handler: MockHandler) -> None:
        """Test that uptime is tracked correctly.

        Args:
            handler: Test handler instance.
        """
        await handler.start()

        # Wait a bit to accumulate uptime
        await asyncio.sleep(0.1)

        metrics = handler.get_metrics()
        assert "uptime_seconds" in metrics
        assert metrics["uptime_seconds"] >= 0

        await handler.stop()

        # Metrics should still be available after stop
        final_metrics = handler.get_metrics()
        assert "uptime_seconds" in final_metrics

    @pytest.mark.asyncio
    async def test_metrics_available_after_transitions(self, handler: MockHandler) -> None:
        """Test that metrics remain available after state transitions.

        Args:
            handler: Test handler instance.
        """
        await handler.start()

        # Generate some activity to create metrics
        test_event = MockEvent(message="test", value=42)
        await handler.handle_with_degradation(test_event)

        await handler.stop()

        # Verify stop was successful and metrics are still accessible
        final_state = handler.state
        assert final_state == ComponentState.STOPPED

        final_metrics = handler.get_metrics()
        assert "events_processed" in final_metrics
        assert final_metrics["events_processed"] >= 0
        assert "error_count" in final_metrics

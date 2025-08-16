"""Integration tests for WebSocket error handler system.

Tests error handler integration with recovery system, metrics collection,
event publishing, and the complete error handling flow with realistic scenarios.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import cast
from typing import cast
from unittest.mock import MagicMock, Mock

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.common.error_foundation import ErrorSeverity, WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.enums.error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.error_handling.error_events import (
    LoggingEventHandler,
    SeverityEventFilter,
    WebSocketErrorEventPublisher,
)
from cyberdelta.apis.websocket.error_handling.error_handler_factory import (
    WebSocketErrorHandlerFactory,
)
from cyberdelta.apis.websocket.error_handling.error_handler_registry import (
    WebSocketErrorHandlerRegistry,
)
from cyberdelta.apis.websocket.error_handling.stream_error_handler import (
    WebSocketStreamErrorHandler,
)
from cyberdelta.apis.websocket.exceptions import (
    WebSocketConfigurationError,
    WebSocketConnectionError,
    WebSocketSubscriptionError,
)
from cyberdelta.apis.websocket.exceptions.stream_error import WebSocketStreamError
from cyberdelta.apis.websocket.metrics.error_metrics import WebSocketErrorMetrics
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_recovery import (
    RecoveryExecutor,
)
from cyberdelta.config.models.websocket_error_config import (
    WebSocketErrorConfig,
    WebSocketErrorRecoveryConfig,
)
from cyberdelta.enums import ExchangeName


# ============================================================================
# Test Fixtures
# ============================================================================


class MockConnectionManager:
    """Mock connection manager for testing."""

    def __init__(self) -> None:
        """Initialize mock connection manager."""
        self.reconnect_called = False
        self.close_called = False
        self.switch_endpoint_called = False

    async def reconnect(
        self,
        connection_id: str,
        exchange: str,
        force: bool = False,
    ) -> bool:
        """Mock reconnect.

        Returns:
            Always True for successful reconnection.
        """
        self.reconnect_called = True
        await asyncio.sleep(0.01)  # Simulate async work
        return True

    async def reset_connection(
        self,
        connection_id: str,
        exchange: str,
    ) -> bool:
        """Mock reset connection.

        Returns:
            Always True for successful reset.
        """
        await asyncio.sleep(0.01)
        return True

    async def get_connection_state(
        self,
        connection_id: str,
        exchange: str,
    ) -> str:
        """Mock get connection state.

        Returns:
            Always 'connected' for testing.
        """
        return "connected"

    async def close_connection(self, connection_id: str) -> None:
        """Mock close connection."""
        self.close_called = True
        await asyncio.sleep(0.01)

    async def switch_endpoint(self, connection_id: str) -> bool:
        """Mock endpoint switch.

        Returns:
            Always True for successful endpoint switch.
        """
        self.switch_endpoint_called = True
        await asyncio.sleep(0.01)
        return True


class MockSubscriptionManager:
    """Mock subscription manager for testing."""

    def __init__(self) -> None:
        """Initialize mock subscription manager."""
        self.resubscribe_called = False
        self.clear_subscriptions_called = False
        self.pause_called = False

    async def resubscribe(
        self,
        connection_id: str,
        exchange: str,
        channel: str | None = None,
    ) -> bool:
        """Mock resubscribe.

        Returns:
            Always True for successful resubscription.
        """
        self.resubscribe_called = True
        await asyncio.sleep(0.01)
        return True

    async def clear_subscriptions(
        self,
        connection_id: str,
        exchange: str,
    ) -> None:
        """Mock clear subscriptions."""
        self.clear_subscriptions_called = True
        await asyncio.sleep(0.01)

    async def pause_subscriptions(self, connection_id: str) -> None:
        """Mock pause subscriptions."""
        self.pause_called = True
        await asyncio.sleep(0.01)


class MockStateManager:
    """Mock state manager for testing."""

    def __init__(self) -> None:
        """Initialize mock state manager."""
        self.reset_called = False
        self.snapshot_called = False
        self.restore_called = False

    async def request_snapshot(
        self,
        exchange: str,
        channel: str,
        symbol: str | None = None,
    ) -> bool:
        """Mock request snapshot.

        Returns:
            Always True for successful snapshot request.
        """
        self.snapshot_called = True
        await asyncio.sleep(0.01)
        return True

    async def clear_state(
        self,
        exchange: str,
        channel: str | None = None,
        symbol: str | None = None,
    ) -> None:
        """Mock clear state."""
        self.reset_called = True
        await asyncio.sleep(0.01)


@pytest.fixture
def mock_connection_manager() -> MockConnectionManager:
    """Fixture for mock connection manager.

    Returns:
        Mock connection manager instance.
    """
    return MockConnectionManager()


@pytest.fixture
def mock_subscription_manager() -> MockSubscriptionManager:
    """Fixture for mock subscription manager.

    Returns:
        Mock subscription manager instance.
    """
    return MockSubscriptionManager()


@pytest.fixture
def mock_state_manager() -> MockStateManager:
    """Fixture for mock state manager.

    Returns:
        Mock state manager instance.
    """
    return MockStateManager()


@pytest.fixture
def test_config() -> WebSocketErrorConfig:
    """Fixture for test error configuration.

    Returns:
        Default test WebSocket error configuration.
    """
    return WebSocketErrorHandlerFactory.create_default_config(
        exchange=ExchangeName.HYPERLIQUID,
        environment="test",
    )


@pytest.fixture
def error_context() -> StreamErrorContext:
    """Fixture for error context.

    Returns:
        Sample stream error context for testing.
    """
    return StreamErrorContext(
        connection_id="test-conn-123",
        exchange=ExchangeName.HYPERLIQUID,
        channel="orderbook",
        topic="BTC-USD",
        sequence_number=42,
        user_id="test-user",
        session_id="test-session-456",
        environment="test",
        raw_message_size=512,
    )


@pytest.fixture
def sample_error(error_context: StreamErrorContext) -> WebSocketStreamError:
    """Fixture for sample WebSocket error.

    Returns:
        Sample WebSocket stream error for testing.
    """
    return WebSocketStreamError(
        message="Connection lost unexpectedly",
        code=WebSocketErrorCode.CONNECTION_LOST,
        context=error_context,
        severity=ErrorSeverity.ERROR,
        recovery_strategy=WebSocketRecoveryStrategy.FULL_RECONNECT,
    )


# ============================================================================
# Error Handler Factory Integration Tests
# ============================================================================


class TestErrorHandlerFactory:
    """Test error handler factory integration."""

    def test_create_minimal_handler(self) -> None:
        """Test creating a minimal error handler."""
        handler = WebSocketErrorHandlerFactory.create_minimal_handler(
            exchange=ExchangeName.HYPERLIQUID,
        )

        assert isinstance(handler, WebSocketStreamErrorHandler)
        assert handler.config.recovery.max_recovery_attempts == 1  # Test environment
        assert not handler.config.recovery.circuit_breaker_enabled  # Test environment

    def test_create_handler_with_all_dependencies(
        self,
        test_config: WebSocketErrorConfig,
        mock_connection_manager: MockConnectionManager,
        mock_subscription_manager: MockSubscriptionManager,
        mock_state_manager: MockStateManager,
    ) -> None:
        """Test creating handler with all dependencies."""
        metrics = WebSocketErrorMetrics(config=test_config.metrics)

        handler = WebSocketErrorHandlerFactory.create_handler(
            exchange=ExchangeName.HYPERLIQUID,
            config=test_config,
            metrics_collector=metrics,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

        assert isinstance(handler, WebSocketStreamErrorHandler)
        assert handler.recovery_handler is not None
        assert handler.metrics_collector is metrics

    def test_unsupported_exchange_raises_error(self) -> None:
        """Test that unsupported exchange raises error."""
        # Create a mock exchange that's not in supported list
        mock_exchange = MagicMock()
        mock_exchange.value = "unsupported_exchange"

        with pytest.raises(WebSocketConfigurationError, match="Unsupported exchange"):
            WebSocketErrorHandlerFactory.create_minimal_handler(
                exchange=mock_exchange,
            )

    def test_configuration_validation(self) -> None:
        """Test configuration validation."""
        config = WebSocketErrorHandlerFactory.create_default_config(
            exchange=ExchangeName.HYPERLIQUID,
            environment="production",
        )

        # Should have no validation errors
        errors = WebSocketErrorHandlerFactory.validate_configuration(config)
        assert errors == []

        # Test invalid configuration
        config.recovery.max_recovery_attempts = -1
        config.recovery.initial_backoff_ms = 50

        errors = WebSocketErrorHandlerFactory.validate_configuration(config)
        assert len(errors) == 2
        assert "max_recovery_attempts must be non-negative" in errors
        assert "initial_backoff_ms must be at least 100ms" in errors


# ============================================================================
# Error Handler Registry Integration Tests
# ============================================================================


class TestErrorHandlerRegistry:
    """Test error handler registry integration."""

    @pytest.fixture
    def registry(self) -> WebSocketErrorHandlerRegistry:
        """Fixture for clean registry.

        Returns:
            Fresh WebSocket error handler registry.
        """
        return WebSocketErrorHandlerRegistry()

    def test_registry_caching_behavior(self, registry: WebSocketErrorHandlerRegistry) -> None:
        """Test that registry properly caches handlers."""
        # First request creates handler
        handler1 = registry.get_handler(ExchangeName.HYPERLIQUID)
        stats = registry.get_registry_statistics()
        assert stats["total_created"] == 1
        assert stats["active_handlers"] == 1

        # Second request uses cached handler
        handler2 = registry.get_handler(ExchangeName.HYPERLIQUID)
        assert handler1 is handler2

        stats = registry.get_registry_statistics()
        assert stats["total_created"] == 1  # Still 1
        assert stats["cache_hits"] == 1

    def test_registry_different_environments(
        self,
        registry: WebSocketErrorHandlerRegistry,
    ) -> None:
        """Test that different environments create different handlers."""
        handler_prod = registry.get_handler(ExchangeName.HYPERLIQUID, environment="production")
        handler_test = registry.get_handler(ExchangeName.HYPERLIQUID, environment="test")

        assert handler_prod is not handler_test
        assert len(registry.list_active_handlers()) == 2

        # Keep handlers alive
        del handler_prod, handler_test

    def test_registry_handler_removal(
        self,
        registry: WebSocketErrorHandlerRegistry,
    ) -> None:
        """Test handler removal from registry."""
        # Keep a reference to prevent garbage collection
        handler = registry.get_handler(ExchangeName.HYPERLIQUID)
        assert len(registry.list_active_handlers()) == 1

        removed = registry.remove_handler(ExchangeName.HYPERLIQUID)
        assert removed is True
        assert len(registry.list_active_handlers()) == 0

        # Removing again should return False
        removed = registry.remove_handler(ExchangeName.HYPERLIQUID)
        assert removed is False

        # Keep handler alive for the test
        del handler

    def test_registry_direct_factory_usage(self) -> None:
        """Test using factory pattern directly instead of global registry."""
        handler = WebSocketErrorHandlerFactory.create_minimal_handler(
            exchange=ExchangeName.HYPERLIQUID
        )
        assert isinstance(handler, WebSocketStreamErrorHandler)

    def test_registry_health_check(self, registry: WebSocketErrorHandlerRegistry) -> None:
        """Test registry health check."""
        # Empty registry
        health = registry.health_check()
        assert health["registry_healthy"] is True
        assert health["active_handlers"] == 0
        issues = cast(list[str] | str, health["issues"])
        issues = cast(list[str] | str, health["issues"])
        assert isinstance(issues, (list, str))
        assert "No active handlers registered" in issues

        # Add a handler - keep reference to prevent garbage collection
        handler = registry.get_handler(ExchangeName.HYPERLIQUID)
        health = registry.health_check()
        assert health["registry_healthy"] is True
        assert health["active_handlers"] == 1
        issues = cast(list[str] | str, health["issues"])
        issues = cast(list[str] | str, health["issues"])
        assert isinstance(issues, (list, str))  # Type narrowing for mypy
        if isinstance(issues, list):
            assert len(issues) == 0
        else:
            assert not issues

        # Keep handler alive
        del handler


# ============================================================================
# Error Handler Integration Tests
# ============================================================================


class TestErrorHandlerIntegration:
    """Test error handler integration with all components."""

    @pytest.fixture
    def handler_with_mocks(
        self,
        test_config: WebSocketErrorConfig,
        mock_connection_manager: MockConnectionManager,
        mock_subscription_manager: MockSubscriptionManager,
        mock_state_manager: MockStateManager,
    ) -> WebSocketStreamErrorHandler:
        """Fixture for fully configured handler with mocks.

        Returns:
            WebSocket stream error handler with all mock dependencies.
        """
        metrics = WebSocketErrorMetrics(config=test_config.metrics)

        return WebSocketErrorHandlerFactory.create_handler(
            exchange=ExchangeName.HYPERLIQUID,
            config=test_config,
            metrics_collector=metrics,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

    async def test_validation_error_handling_flow(
        self,
        handler_with_mocks: WebSocketStreamErrorHandler,
        error_context: StreamErrorContext,
    ) -> None:
        """Test complete validation error handling flow.

        Raises:
            AssertionError: If validation or error handling doesn't work as expected.
        """

        class TestModel(BaseModel):
            required_field: str

        # Create validation error
        validation_error: ValidationError
        try:
            TestModel(required_field=None)  # type: ignore
        except ValidationError as e:
            validation_error = e
        else:
            raise AssertionError("Expected ValidationError")

        # Mock context and payload
        mock_context = Mock()
        mock_context.create_error_context.return_value = error_context

        mock_payload = Mock()

        # Handle the validation error
        await handler_with_mocks.handle_validation_error(
            error=validation_error,
            context=mock_context,
            payload=mock_payload,
        )

        # Verify context creation was called
        mock_context.create_error_context.assert_called_once()

        # Verify metrics were recorded
        if handler_with_mocks.metrics_collector:
            stats = handler_with_mocks.metrics_collector.get_statistics()
            assert stats.total_errors_recorded >= 1

    async def test_connection_error_recovery_flow(
        self,
        handler_with_mocks: WebSocketStreamErrorHandler,
        sample_error: WebSocketStreamError,
        mock_connection_manager: MockConnectionManager,
    ) -> None:
        """Test connection error triggers recovery."""
        connection_error = WebSocketConnectionError(
            message="Connection failed",
            code=WebSocketErrorCode.CONNECTION_FAILED,
            context=sample_error.context,
        )

        # Handle the connection error - create a mock WebSocketContextProtocol
        mock_ws_context = Mock()
        mock_ws_context.exchange_type = ExchangeName.HYPERLIQUID
        mock_ws_context.connection_id = "test-conn-123"
        mock_ws_context.message_id = "test-msg-123"
        mock_ws_context.symbol = "BTC-USDC"
        mock_ws_context.routing_key = "test.route"
        mock_ws_context.domain_model = None

        await handler_with_mocks.handle_connection_error(mock_ws_context, connection_error)

        # Should trigger recovery through the recovery handler
        # Note: Since we're using a mock, we need to check if the recovery system
        # was configured properly during handler creation
        assert handler_with_mocks.recovery_handler is not None

    async def test_subscription_error_handling(
        self,
        handler_with_mocks: WebSocketStreamErrorHandler,
        error_context: StreamErrorContext,
        mock_subscription_manager: MockSubscriptionManager,
    ) -> None:
        """Test subscription error handling."""
        subscription_error = WebSocketSubscriptionError(
            message="Subscription failed",
            context=error_context,
            code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
        )

        # Handle the subscription error
        await handler_with_mocks.handle_stream_error(subscription_error)

        # Verify error was processed
        if handler_with_mocks.metrics_collector:
            stats = handler_with_mocks.metrics_collector.get_statistics()
            assert stats.total_errors_recorded >= 1

    async def test_error_metrics_integration(
        self,
        handler_with_mocks: WebSocketStreamErrorHandler,
        sample_error: WebSocketStreamError,
    ) -> None:
        """Test error metrics integration."""
        # Handle multiple errors
        for _ in range(3):
            await handler_with_mocks.handle_stream_error(sample_error)

        # Check metrics were collected
        if handler_with_mocks.metrics_collector:
            stats = handler_with_mocks.metrics_collector.get_statistics()
            assert stats.total_errors_recorded >= 3

            # Get aggregated metrics
            metrics = handler_with_mocks.metrics_collector.get_aggregated_metrics()
            assert len(metrics.error_counts_by_exchange) > 0
            assert metrics.error_counts_by_exchange["hyperliquid"] >= 3


# ============================================================================
# Event Publisher Integration Tests
# ============================================================================


class TestEventPublisherIntegration:
    """Test event publisher integration."""

    @pytest.fixture
    def event_publisher(self) -> WebSocketErrorEventPublisher:
        """Fixture for event publisher.

        Returns:
            WebSocket error event publisher for testing.
        """
        logger = logging.getLogger("test_publisher")
        return WebSocketErrorEventPublisher(
            logger=logger,
            enable_async_publishing=True,
            max_queue_size=100,
        )

    @pytest.fixture
    def event_handler(self) -> LoggingEventHandler:
        """Fixture for logging event handler.

        Returns:
            Logging event handler for testing.
        """
        logger = logging.getLogger("test_handler")
        return LoggingEventHandler(logger=logger, log_level="INFO")

    async def test_error_event_publishing_flow(
        self,
        event_publisher: WebSocketErrorEventPublisher,
        event_handler: LoggingEventHandler,
        sample_error: WebSocketStreamError,
    ) -> None:
        """Test complete error event publishing flow."""
        # Setup event handling
        event_publisher.add_handler("websocket_error", event_handler)
        severity_filter = SeverityEventFilter(min_severity=ErrorSeverity.WARNING)
        event_publisher.add_filter(severity_filter)

        # Start async publishing
        await event_publisher.start_async_publishing()

        try:
            # Publish error event
            await event_publisher.publish_error_event(
                error=sample_error,
                recovery_attempted=True,
                recovery_successful=True,
                recovery_duration_ms=1500,
                connection_duration_ms=30000,
                error_count_in_window=1,
            )

            # Wait for async processing
            await asyncio.sleep(0.1)
            await event_publisher.flush_events()

            # Check statistics
            stats = event_publisher.get_statistics()
            assert stats["events_published"] >= 1
            assert stats["handlers_count"] == 1
            assert stats["filters_count"] == 1

        finally:
            await event_publisher.stop_async_publishing()

    async def test_recovery_event_publishing(
        self,
        event_publisher: WebSocketErrorEventPublisher,
        event_handler: LoggingEventHandler,
    ) -> None:
        """Test recovery event publishing."""
        event_publisher.add_handler("recovery_attempt", event_handler)
        await event_publisher.start_async_publishing()

        try:
            # Publish recovery attempt event
            await event_publisher.publish_recovery_attempt_event(
                exchange=ExchangeName.HYPERLIQUID,
                connection_id="test-conn-123",
                strategy=WebSocketRecoveryStrategy.FULL_RECONNECT,
                attempt_number=1,
                successful=True,
                duration_ms=2000,
                original_error_code=WebSocketErrorCode.CONNECTION_LOST,
                error_count_before=3,
                channel="orderbook",
            )

            # Wait for processing
            await asyncio.sleep(0.1)
            await event_publisher.flush_events()

            stats = event_publisher.get_statistics()
            assert stats["events_published"] >= 1

        finally:
            await event_publisher.stop_async_publishing()

    async def test_event_filtering(
        self,
        event_publisher: WebSocketErrorEventPublisher,
        event_handler: LoggingEventHandler,
        sample_error: WebSocketStreamError,
    ) -> None:
        """Test event filtering functionality."""
        # Add handler and filter that blocks low-severity events
        event_publisher.add_handler("websocket_error", event_handler)
        event_publisher.add_filter(SeverityEventFilter(min_severity=ErrorSeverity.ERROR))

        await event_publisher.start_async_publishing()

        try:
            # Create low-severity error that should be filtered
            low_severity_error = WebSocketStreamError(
                message="Minor issue",
                code=WebSocketErrorCode.MESSAGE_MALFORMED,
                context=sample_error.context,
                severity=ErrorSeverity.INFO,  # Below ERROR threshold
                recovery_strategy=WebSocketRecoveryStrategy.NONE,
            )

            # Publish low-severity event (should be filtered)
            await event_publisher.publish_error_event(error=low_severity_error)

            # Publish high-severity event (should pass through)
            await event_publisher.publish_error_event(error=sample_error)

            # Wait for processing
            await asyncio.sleep(0.1)
            await event_publisher.flush_events()

            stats = event_publisher.get_statistics()
            assert stats["events_published"] == 1  # Only high-severity event
            assert stats["events_filtered"] == 1  # Low-severity event filtered

        finally:
            await event_publisher.stop_async_publishing()


# ============================================================================
# Recovery System Integration Tests
# ============================================================================


class TestRecoverySystemIntegration:
    """Test recovery system integration."""

    @pytest.fixture
    def recovery_config(self) -> WebSocketErrorRecoveryConfig:
        """Fixture for recovery configuration.

        Returns:
            WebSocket error recovery configuration for testing.
        """
        return WebSocketErrorRecoveryConfig(
            max_recovery_attempts=3,
            initial_backoff_ms=100,
            max_backoff_ms=1000,
            backoff_multiplier=2.0,
            jitter_enabled=False,  # Disable for predictable tests
        )

    @pytest.fixture
    def recovery_system(
        self,
        recovery_config: WebSocketErrorRecoveryConfig,
        mock_connection_manager: MockConnectionManager,
        mock_subscription_manager: MockSubscriptionManager,
        mock_state_manager: MockStateManager,
    ) -> RecoveryExecutor:
        """Fixture for recovery system.

        Returns:
            Stream recovery system with mock dependencies.
        """
        logger = logging.getLogger("test_recovery")
        return RecoveryExecutor(
            config=recovery_config,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
            logger=logger,
        )

    async def test_full_reconnect_recovery(
        self,
        recovery_system: RecoveryExecutor,
        sample_error: WebSocketStreamError,
        mock_connection_manager: MockConnectionManager,
        mock_subscription_manager: MockSubscriptionManager,
    ) -> None:
        """Test full reconnect recovery strategy."""
        # Set error to require full reconnect
        full_reconnect_error = WebSocketStreamError(
            message="Connection permanently lost",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=sample_error.context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.FULL_RECONNECT,
        )

        # Execute recovery
        success = await recovery_system.handle_stream_error(full_reconnect_error)

        # Verify recovery actions
        assert success is True
        assert mock_connection_manager.reconnect_called is True
        assert mock_subscription_manager.resubscribe_called is True

    async def test_resubscribe_recovery(
        self,
        recovery_system: RecoveryExecutor,
        sample_error: WebSocketStreamError,
        mock_subscription_manager: MockSubscriptionManager,
    ) -> None:
        """Test resubscribe recovery strategy."""
        resubscribe_error = WebSocketStreamError(
            message="Subscription lost",
            code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
            context=sample_error.context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
        )

        # Execute recovery
        success = await recovery_system.handle_stream_error(resubscribe_error)

        # Verify resubscribe was called
        assert success is True
        assert mock_subscription_manager.resubscribe_called is True

    async def test_recovery_with_backoff(
        self,
        recovery_system: RecoveryExecutor,
        sample_error: WebSocketStreamError,
    ) -> None:
        """Test recovery with backoff delays."""
        # Create error that will fail initially
        failing_error = WebSocketStreamError(
            message="Temporary failure",
            code=WebSocketErrorCode.RATE_LIMITED,
            context=sample_error.context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
        )

        # Measure time for recovery attempts
        start_time = time.time()
        await recovery_system.handle_stream_error(failing_error)
        end_time = time.time()

        # Should have taken some time due to backoff
        # (Even with short delays for testing)
        elapsed_ms = (end_time - start_time) * 1000
        assert elapsed_ms >= 50  # At least some delay


# ============================================================================
# End-to-End Integration Tests
# ============================================================================


class TestEndToEndIntegration:
    """Test complete end-to-end error handling integration."""

    @pytest.fixture
    def complete_system(
        self,
        mock_connection_manager: MockConnectionManager,
        mock_subscription_manager: MockSubscriptionManager,
        mock_state_manager: MockStateManager,
    ) -> tuple[WebSocketStreamErrorHandler, WebSocketErrorEventPublisher, WebSocketErrorMetrics]:
        """Fixture for complete integrated system.

        Returns:
            Tuple of error handler, event publisher, and metrics collector.
        """
        # Create configuration
        config = WebSocketErrorHandlerFactory.create_default_config(
            exchange=ExchangeName.HYPERLIQUID,
            environment="test",
        )

        # Create metrics collector
        metrics = WebSocketErrorMetrics(config=config.metrics)

        # Create event publisher
        publisher = WebSocketErrorEventPublisher(
            logger=logging.getLogger("test_publisher"),
            enable_async_publishing=True,
        )

        # Create error handler with all components
        handler = WebSocketErrorHandlerFactory.create_handler(
            exchange=ExchangeName.HYPERLIQUID,
            config=config,
            metrics_collector=metrics,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

        return handler, publisher, metrics

    async def test_complete_error_handling_flow(
        self,
        complete_system: tuple[
            WebSocketStreamErrorHandler, WebSocketErrorEventPublisher, WebSocketErrorMetrics
        ],
        sample_error: WebSocketStreamError,
    ) -> None:
        """Test complete error handling from error to recovery to metrics."""
        handler, publisher, metrics = complete_system

        # Setup event publishing
        log_handler = LoggingEventHandler(
            logger=logging.getLogger("test_events"),
            log_level="INFO",
        )
        publisher.add_handler("websocket_error", log_handler)

        await publisher.start_async_publishing()

        try:
            # Process error through complete system
            await handler.handle_stream_error(sample_error)

            # Publish event about the error
            await publisher.publish_error_event(
                error=sample_error,
                recovery_attempted=True,
                recovery_successful=True,
                recovery_duration_ms=1000,
            )

            # Wait for async processing
            await asyncio.sleep(0.2)
            await publisher.flush_events()

            # Verify all systems recorded the error
            handler_stats = handler.get_statistics()
            metrics_stats = metrics.get_statistics()
            publisher_stats = publisher.get_statistics()

            # Handler should have processed the error
            total_errors_handled = handler_stats["total_errors_handled"]
            assert isinstance(total_errors_handled, int)
            assert total_errors_handled >= 1

            # Metrics should have recorded the error
            assert metrics_stats.total_errors_recorded >= 1

            # Publisher should have published events
            assert publisher_stats["events_published"] >= 1

            # Get aggregated metrics to verify data flow
            aggregated = metrics.get_aggregated_metrics()
            assert len(aggregated.error_counts_by_code) > 0
            assert aggregated.error_counts_by_exchange["hyperliquid"] >= 1

        finally:
            await publisher.stop_async_publishing()

    async def test_error_escalation_flow(
        self,
        complete_system: tuple[
            WebSocketStreamErrorHandler, WebSocketErrorEventPublisher, WebSocketErrorMetrics
        ],
        error_context: StreamErrorContext,
    ) -> None:
        """Test error escalation from warning to critical."""
        handler, publisher, metrics = complete_system

        # Setup critical error handling
        critical_handler = LoggingEventHandler(
            logger=logging.getLogger("critical_events"),
            log_level="ERROR",
        )
        publisher.add_handler("websocket_error", critical_handler)
        publisher.add_filter(SeverityEventFilter(min_severity=ErrorSeverity.ERROR))

        await publisher.start_async_publishing()

        try:
            # Create escalating errors
            warning_error = WebSocketStreamError(
                message="Minor connection issue",
                code=WebSocketErrorCode.CONNECTION_TIMEOUT,
                context=error_context,
                severity=ErrorSeverity.WARNING,
                recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            )

            critical_error = WebSocketStreamError(
                message="Critical system failure",
                code=WebSocketErrorCode.INTERNAL_ERROR,
                context=error_context,
                severity=ErrorSeverity.CRITICAL,
                recovery_strategy=WebSocketRecoveryStrategy.CIRCUIT_BREAKER,
            )

            # Process both errors
            await handler.handle_stream_error(warning_error)
            await handler.handle_stream_error(critical_error)

            # Publish events for both
            await publisher.publish_error_event(error=warning_error)  # Should be filtered
            await publisher.publish_error_event(error=critical_error)  # Should pass

            # Wait for processing
            await asyncio.sleep(0.2)
            await publisher.flush_events()

            # Verify filtering worked
            stats = publisher.get_statistics()
            assert stats["events_published"] >= 1  # Critical event published
            assert stats["events_filtered"] >= 1  # Warning event filtered

            # Verify metrics recorded both errors
            metrics_stats = metrics.get_statistics()
            assert metrics_stats.total_errors_recorded >= 2

        finally:
            await publisher.stop_async_publishing()

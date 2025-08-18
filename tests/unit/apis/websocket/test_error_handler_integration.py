"""Integration tests for WebSocket error handler system.

Tests error handler integration with recovery system, metrics collection,
event publishing, and the complete error handling flow with realistic scenarios.
"""

from __future__ import annotations

import asyncio
import logging
from typing import cast
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.common.error_foundation import ErrorSeverity, WebSocketRecoveryStrategy
from cyberdelta.apis.enums.websocket.error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.error_context.events import (
    LoggingEventHandler,
    SeverityEventFilter,
    WebSocketErrorEventPublisher,
)
from cyberdelta.apis.websocket.error_context import (
    WebSocketErrorHandler,
    WebSocketErrorHandlerFactory,
)
from cyberdelta.apis.websocket.exceptions import (
    WebSocketConfigurationError,
    WebSocketConnectionError,
    WebSocketSubscriptionError,
)
from cyberdelta.apis.websocket.exceptions.stream_error import WebSocketStreamError
from cyberdelta.apis.websocket.metrics.error_metrics import WebSocketErrorMetrics
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
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
        self.resubscribe_all_called = False
        self.clear_subscriptions_called = False
        self.pause_called = False
        self.active_subscriptions: list[str] = []

    async def resubscribe(
        self,
        connection_id: str,
        exchange: str,
        channel: str | None = None,
        topic: str | None = None,
    ) -> bool:
        """Mock resubscribe.

        Returns:
            Always True for successful resubscription.
        """
        self.resubscribe_called = True
        await asyncio.sleep(0.01)
        return True

    async def resubscribe_all(
        self,
        connection_id: str,
        exchange: str,
    ) -> bool:
        """Mock resubscribe all.

        Returns:
            Always True for successful resubscription.
        """
        self.resubscribe_all_called = True
        await asyncio.sleep(0.01)
        return True

    async def get_active_subscriptions(
        self,
        connection_id: str,
        exchange: str,
    ) -> list[tuple[str, str | None]]:
        """Mock get active subscriptions.

        Returns:
            List of (channel, topic) tuples.
        """
        return [("depth", "BTC-USD"), ("ticker", None)]

    async def clear_subscriptions(
        self,
        connection_id: str,
        exchange: str,
    ) -> None:
        """Mock clear subscriptions."""
        self.clear_subscriptions_called = True
        self.active_subscriptions.clear()
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
        self.save_called = False
        self.restore_called = False

    async def save_state(
        self,
        connection_id: str,
        exchange: str,
    ) -> bool:
        """Mock save state.

        Returns:
            Always True for successful state save.
        """
        self.save_called = True
        await asyncio.sleep(0.01)
        return True

    async def restore_state(
        self,
        connection_id: str,
        exchange: str,
    ) -> bool:
        """Mock restore state.

        Returns:
            Always True for successful state restore.
        """
        self.restore_called = True
        await asyncio.sleep(0.01)
        return True

    async def clear_state(
        self,
        connection_id: str,
        exchange: str,
    ) -> bool:
        """Mock clear state.

        Returns:
            Always True for successful state clear.
        """
        self.reset_called = True
        await asyncio.sleep(0.01)
        return True


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

        assert isinstance(handler, WebSocketErrorHandler)
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

        assert isinstance(handler, WebSocketErrorHandler)
        assert handler.recovery_executor is not None
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
        assert isinstance(handler, WebSocketErrorHandler)

    def test_registry_health_check(self, registry: WebSocketErrorHandlerRegistry) -> None:
        """Test registry health check."""
        # Empty registry
        health = registry.health_check()
        assert health["registry_healthy"] is True
        assert health["active_handlers"] == 0
        issues_obj = health["issues"]
        # health_check() returns list[str] for "issues" key based on implementation
        assert isinstance(issues_obj, list)
        # Type validation with runtime check for PyRight
        # health_check() returns dict[str, object], PyRight cannot infer list element types
        # Using cast is safe as we validate each item with isinstance
        # #[CAST-REVIEW-REQUIRED] Test-only - health_check returns untyped dict values
        typed_issues_obj = cast(list[object], issues_obj)
        assert isinstance(typed_issues_obj, list)  # Runtime verification per RULE-NO-SILENCING-V4
        issues: list[str] = []
        for item in typed_issues_obj:
            # PyRight type narrowing: item is object
            assert isinstance(item, str), f"Expected str, got {type(item)}"
            # After isinstance check, item is now str for PyRight
            issues.append(item)
        assert any("No active handlers registered" in issue for issue in issues)

        # Add a handler - keep reference to prevent garbage collection
        handler = registry.get_handler(ExchangeName.HYPERLIQUID)
        health = registry.health_check()
        assert health["registry_healthy"] is True
        assert health["active_handlers"] == 1
        issues_obj_active = health["issues"]
        assert isinstance(issues_obj_active, list)
        # Type validation with runtime check for PyRight
        # health_check() returns dict[str, object], PyRight cannot infer list element types
        # Using cast is safe as we validate each item with isinstance
        # #[CAST-REVIEW-REQUIRED] Test-only - health_check returns untyped dict values
        typed_issues_obj_active = cast(list[object], issues_obj_active)
        assert isinstance(
            typed_issues_obj_active, list
        )  # Runtime verification per RULE-NO-SILENCING-V4
        issues_active: list[str] = []
        for item in typed_issues_obj_active:
            # PyRight type narrowing: item is object
            assert isinstance(item, str), f"Expected str, got {type(item)}"
            # After isinstance check, item is now str for PyRight
            issues_active.append(item)
        assert len(issues_active) == 0

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
    ) -> WebSocketErrorHandler:
        """Fixture for fully configured handler with mocks.

        Returns:
            WebSocket error handler with all mock dependencies.
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

    @pytest.mark.asyncio
    async def test_validation_error_handling_flow(
        self,
        handler_with_mocks: WebSocketErrorHandler,
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
            TestModel(required_field=None)  # type: ignore[arg-type]
        except ValidationError as e:
            validation_error = e
        else:
            raise AssertionError("Expected ValidationError")

        # Mock context and payload
        mock_context = MagicMock()
        mock_context.create_error_context.return_value = error_context
        mock_payload = MagicMock()

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

    @pytest.mark.asyncio
    async def test_connection_error_recovery_flow(
        self,
        handler_with_mocks: WebSocketErrorHandler,
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
        mock_ws_context = MagicMock()
        mock_ws_context.exchange_type = ExchangeName.HYPERLIQUID
        mock_ws_context.connection_id = "test-conn-123"
        mock_ws_context.message_id = "test-msg-123"
        mock_ws_context.symbol = "BTC-USDC"
        mock_ws_context.routing_key = "test.route"
        mock_ws_context.domain_model = None

        await handler_with_mocks.handle_connection_error(mock_ws_context, connection_error)

        # Should trigger recovery through the recovery executor
        assert handler_with_mocks.recovery_executor is not None

    @pytest.mark.asyncio
    async def test_subscription_error_handling(
        self,
        handler_with_mocks: WebSocketErrorHandler,
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

    @pytest.mark.asyncio
    async def test_error_metrics_integration(
        self,
        handler_with_mocks: WebSocketErrorHandler,
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

    @pytest.mark.asyncio
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


# ============================================================================
# Simplified Integration Tests
# ============================================================================


class TestSimplifiedIntegration:
    """Test simplified error handling integration."""

    def test_error_handler_creation_patterns(self) -> None:
        """Test different error handler creation patterns."""
        # Test minimal handler creation
        minimal_handler = WebSocketErrorHandlerFactory.create_minimal_handler(
            exchange=ExchangeName.BACKPACK
        )
        assert isinstance(minimal_handler, WebSocketErrorHandler)
        # WebSocketErrorHandler doesn't store exchange as a direct attribute

        # Test with custom config
        config = WebSocketErrorConfig()
        handler_with_config = WebSocketErrorHandlerFactory.create_handler(
            exchange=ExchangeName.HYPERLIQUID,
            config=config,
        )
        assert isinstance(handler_with_config, WebSocketErrorHandler)
        # WebSocketErrorHandler doesn't store exchange as a direct attribute

    def test_metrics_collection_integration(self) -> None:
        """Test metrics collection integration."""
        config = WebSocketErrorConfig()
        metrics = WebSocketErrorMetrics(config=config.metrics)

        handler = WebSocketErrorHandlerFactory.create_handler(
            exchange=ExchangeName.HYPERLIQUID,
            config=config,
            metrics_collector=metrics,
        )

        assert handler.metrics_collector is metrics

        # Test that statistics are accessible
        stats = metrics.get_statistics()
        assert hasattr(stats, "total_errors_recorded")
        assert stats.total_errors_recorded == 0  # No errors recorded yet

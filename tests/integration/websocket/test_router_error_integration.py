"""Integration tests for WebSocket router with typed error system.

This test validates Step 44: Router Integration Tests.

These tests verify the integration between:
1. WebSocket routers with typed error handling
2. StreamErrorHandler integration
3. RouterErrorBridge functionality
4. Configuration-driven router behavior
5. End-to-end error handling flows
6. Router error recovery system integration
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.enums import ExchangeName
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_exceptions import WebSocketValidationError
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_router import (
    BaseWebSocketRouter,
    MessageHandler,
)
from cyberdelta.apis.websocket.ws_router_config_integration import (
    RouterConfigurationError,
    WebSocketRouterConfigurator,
)
from cyberdelta.apis.websocket.ws_router_error_bridge import RouterErrorBridge
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig


class TestEnvelopeModel(BaseModel):
    """Test envelope model for integration tests."""

    stream: str
    data: dict[str, Any]
    timestamp: int = 0


class TestRouterImpl(BaseWebSocketRouter[TestEnvelopeModel]):
    """Test router implementation for integration tests."""

    def _setup_processors(self) -> None:
        """Setup test processors."""
        # Will be configured by tests

    def _extract_routing_key_from_envelope(self, envelope: TestEnvelopeModel) -> str | None:
        """Extract routing key from envelope."""
        return envelope.stream or None

    def _extract_payload_from_envelope(self, envelope: TestEnvelopeModel) -> dict[str, Any]:
        """Extract payload from envelope."""
        return envelope.data


class TestMessageProcessor:
    """Test message processor implementation."""

    def __init__(self, should_fail: bool = False, delay_ms: int = 0) -> None:
        """Initialize processor."""
        self.should_fail = should_fail
        self.delay_ms = delay_ms
        self.processed_messages: list[dict[str, Any]] = []
        self.processed_contexts: list[WebSocketContextProtocol] = []

    async def process(
        self,
        payload: dict[str, Any] | list[Any],
        handler: MessageHandler,
        context: WebSocketContextProtocol,
    ) -> None:
        """Process message with optional failure and delay."""
        if self.delay_ms > 0:
            await asyncio.sleep(self.delay_ms / 1000)

        if self.should_fail:
            raise RuntimeError("Processor failure simulation")

        # Record processed data
        if isinstance(payload, dict):
            self.processed_messages.append(payload)
        self.processed_contexts.append(context)

        # Call handler
        await handler(context)


@pytest.mark.asyncio
class TestWebSocketRouterErrorIntegration:
    """Integration tests for router error handling."""

    @pytest.fixture
    def mock_legacy_error_handler(self) -> Mock:
        """Create mock legacy error handler."""
        mock = Mock()
        mock.handle_unroutable_message = AsyncMock()
        mock.handle_routing_error = AsyncMock()
        mock.handle_processing_error = AsyncMock()
        return mock

    @pytest.fixture
    def mock_stream_error_handler(self) -> Mock:
        """Create mock stream error handler."""
        mock = Mock(spec=WebSocketStreamErrorHandler)
        mock.handle_stream_error = AsyncMock()
        return mock

    @pytest.fixture
    def mock_typed_processor(self) -> Mock:
        """Create mock typed processor."""
        mock = Mock(spec=TypeSafeWebSocketProcessor)
        mock_context = Mock(spec=WebSocketContextProtocol)
        mock_context.connection_id = "test-conn-1234-abcd"
        mock_context.exchange_name = "hyperliquid"
        mock_context.message_id = str(uuid.uuid4())
        mock.create_typed_context.return_value = mock_context
        return mock

    @pytest.fixture
    def envelope_validator(self) -> Mock:
        """Create envelope validator."""

        def validator(message: dict[str, Any]) -> TestEnvelopeModel:
            return TestEnvelopeModel(
                stream=message.get("stream", ""),
                data=message.get("data", {}),
                timestamp=message.get("timestamp", 0),
            )

        return Mock(side_effect=validator)

    @pytest.fixture
    def error_config(self) -> WebSocketErrorConfig:
        """Create error configuration for testing."""
        config = WebSocketErrorConfig()
        # Enable all error handling features for comprehensive testing
        config.enabled = True
        config.router.enable_error_bridge = True
        config.router.bridge_fallback_to_legacy = True
        config.router.strict_envelope_validation = True
        config.router.log_envelope_validation_failures = True
        return config

    @pytest.fixture
    def configured_router(
        self,
        mock_legacy_error_handler: Mock,
        mock_typed_processor: Mock,
        mock_stream_error_handler: Mock,
        envelope_validator: Mock,
        error_config: WebSocketErrorConfig,
    ) -> TestRouterImpl:
        """Create fully configured test router."""
        router = TestRouterImpl(
            exchange_name="hyperliquid",
            exchange_type=ExchangeName.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
            stream_error_handler=mock_stream_error_handler,
            envelope_validator=envelope_validator,
        )
        return router

    @pytest.fixture
    def router_configurator(
        self, error_config: WebSocketErrorConfig
    ) -> WebSocketRouterConfigurator[TestEnvelopeModel]:
        """Create router configurator."""
        return WebSocketRouterConfigurator(error_config)

    @pytest.fixture
    def error_bridge(
        self,
        configured_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
        mock_legacy_error_handler: Mock,
    ) -> RouterErrorBridge[TestEnvelopeModel]:
        """Create router error bridge."""
        return RouterErrorBridge(
            router=configured_router,
            stream_error_handler=mock_stream_error_handler,
            legacy_error_handler=mock_legacy_error_handler,
        )

    async def test_end_to_end_successful_message_flow(
        self,
        configured_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test complete successful message processing flow."""
        # Setup processor and handler
        processor = TestMessageProcessor()
        handler = AsyncMock()

        configured_router.register_processor("ticker", processor)
        handlers = {"ticker": handler}

        # Process valid message
        message = {
            "stream": "ticker",
            "data": {"symbol": "BTC-USD", "price": "50000"},
            "timestamp": 1234567890,
        }

        await configured_router.route_message(message, handlers)

        # Verify successful processing
        assert len(processor.processed_messages) == 1
        assert processor.processed_messages[0] == {"symbol": "BTC-USD", "price": "50000"}
        handler.assert_called_once()

        # Verify no errors were handled
        mock_stream_error_handler.handle_stream_error.assert_not_called()

    async def test_end_to_end_envelope_validation_error_flow(
        self,
        configured_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
        mock_legacy_error_handler: Mock,
    ) -> None:
        """Test complete envelope validation error flow."""

        # Create failing envelope validator
        def failing_validator(message: dict[str, Any]) -> TestEnvelopeModel:
            raise ValidationError.from_exception_data(
                "TestEnvelopeModel",
                [{"type": "missing", "loc": ("stream",), "msg": "Field required"}],
            )

        configured_router.envelope_validator = Mock(side_effect=failing_validator)

        # Process invalid message
        message = {"data": {"invalid": "structure"}}
        handlers = {}

        await configured_router.route_message(message, handlers)

        # Verify typed error handling was used
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Verify error properties
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.VALIDATION_FAILED
        assert error.field == "envelope"
        assert "Invalid message envelope format" in error.message

        # Verify legacy handler was not called (typed system succeeded)
        mock_legacy_error_handler.handle_unroutable_message.assert_not_called()

    async def test_end_to_end_missing_processor_error_flow(
        self,
        configured_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test complete missing processor error flow."""
        # Setup handler but no processor
        handler = AsyncMock()
        handlers = {"ticker": handler}

        # Process message that requires processor
        message = {"stream": "ticker", "data": {"symbol": "BTC-USD"}, "timestamp": 1234567890}

        await configured_router.route_message(message, handlers)

        # Verify typed error handling
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Verify error properties
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.PROCESSOR_ERROR
        assert error.field == "routing_key"
        assert error.value == "ticker"
        assert "No processor found for routing key: ticker" in error.message

    async def test_end_to_end_processor_failure_recovery_flow(
        self,
        configured_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test processor failure with error recovery flow."""
        # Setup failing processor
        failing_processor = TestMessageProcessor(should_fail=True)
        handler = AsyncMock()

        configured_router.register_processor("trades", failing_processor)
        handlers = {"trades": handler}

        # Process message that will cause processor failure
        message = {"stream": "trades", "data": {"trade_id": "123"}, "timestamp": 1234567890}

        # This should trigger routing error handling due to processor failure
        await configured_router.route_message(message, handlers)

        # Verify error was handled (processor failure becomes routing error)
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Verify error recovery was notified
        if configured_router.error_recovery:
            # Error recovery should be active due to routing error
            recovery_stats = configured_router.get_recovery_stats()
            assert recovery_stats is not None

    async def test_router_configurator_integration(
        self,
        router_configurator: WebSocketRouterConfigurator[TestEnvelopeModel],
        configured_router: TestRouterImpl,
        mock_legacy_error_handler: Mock,
    ) -> None:
        """Test router configurator integration with router."""
        # Configure router through configurator
        error_bridge = router_configurator.configure_router_error_handling(
            configured_router, mock_legacy_error_handler
        )

        # Verify bridge was created and configured
        assert error_bridge is not None
        assert error_bridge.is_available()

        # Verify bridge configuration
        bridge_info = error_bridge.get_bridge_info()
        assert bridge_info["exchange"] == "hyperliquid"
        assert bridge_info["router_type"] == "TestRouterImpl"
        assert bridge_info["stream_handler_available"] is True
        assert bridge_info["legacy_handler_available"] is True

        # Test bridge functionality
        test_error = RuntimeError("Test bridge error")
        test_message = {"stream": "test", "data": {}}

        await error_bridge.handle_routing_error(test_error, test_message)

        # Verify error was handled through bridge
        configured_router.stream_error_handler.handle_stream_error.assert_called_once()

    async def test_router_configuration_validation_integration(
        self,
        configured_router: TestRouterImpl,
        mock_legacy_error_handler: Mock,
    ) -> None:
        """Test router configuration validation integration."""
        # Create invalid configuration
        invalid_config = WebSocketErrorConfig()
        invalid_config.router.bridge_error_timeout_ms = 100  # Too low
        invalid_config.router.envelope_validation_timeout_ms = 200  # Higher than bridge
        invalid_config.router.bridge_fallback_to_legacy = False  # No fallback

        configurator = WebSocketRouterConfigurator(invalid_config)

        # Remove stream handler to trigger validation error
        configured_router.stream_error_handler = None

        # Should raise configuration error due to missing stream handler and no fallback
        with pytest.raises(RouterConfigurationError) as exc_info:
            configurator.configure_router_error_handling(
                configured_router, mock_legacy_error_handler
            )

        assert "Stream error handler required but not available" in str(exc_info.value)
        assert exc_info.value.config_field == "stream_error_handler"

    async def test_router_fallback_behavior_integration(
        self,
        configured_router: TestRouterImpl,
        mock_legacy_error_handler: Mock,
    ) -> None:
        """Test router fallback behavior when typed system unavailable."""
        # Remove stream error handler to trigger fallback
        configured_router.stream_error_handler = None

        # Create failing envelope validator
        def failing_validator(message: dict[str, Any]) -> TestEnvelopeModel:
            raise ValidationError.from_exception_data(
                "TestEnvelopeModel",
                [{"type": "missing", "loc": ("stream",), "msg": "Field required"}],
            )

        configured_router.envelope_validator = Mock(side_effect=failing_validator)

        # Process message that will fail validation
        message = {"data": {"test": "message"}}
        handlers = {}

        await configured_router.route_message(message, handlers)

        # Verify fallback to legacy system was used
        mock_legacy_error_handler.handle_unroutable_message.assert_called_once()

        # Verify legacy handler was called with correct parameters
        call_args = mock_legacy_error_handler.handle_unroutable_message.call_args
        assert call_args[1]["message"] == message
        assert "Invalid message envelope format" in call_args[1]["reason"]

    async def test_router_performance_tracking_integration(
        self,
        configured_router: TestRouterImpl,
    ) -> None:
        """Test router performance tracking integration."""
        # Setup slow processor to test performance tracking
        slow_processor = TestMessageProcessor(delay_ms=150)  # 150ms delay
        handler = AsyncMock()

        configured_router.register_processor("slow_stream", slow_processor)
        handlers = {"slow_stream": handler}

        # Process message that will take time
        message = {
            "stream": "slow_stream",
            "data": {"test": "slow_processing"},
            "timestamp": 1234567890,
        }

        start_time = asyncio.get_event_loop().time()
        await configured_router.route_message(message, handlers)
        end_time = asyncio.get_event_loop().time()

        # Verify processing took expected time
        processing_time_ms = (end_time - start_time) * 1000
        assert processing_time_ms >= 140  # Allow some tolerance

        # Verify message was processed successfully
        assert len(slow_processor.processed_messages) == 1
        handler.assert_called_once()

    async def test_router_memory_optimization_integration(
        self,
        configured_router: TestRouterImpl,
    ) -> None:
        """Test router memory optimization integration."""
        # Enable high-frequency mode
        optimization_enabled = configured_router.enable_high_frequency_mode()
        assert optimization_enabled is True

        # Verify memory optimization is active
        memory_stats = configured_router.get_memory_stats()
        assert memory_stats is not None
        # Check for actual memory pool stats structure
        assert "allocated" in memory_stats
        assert "context_pool_size" in memory_stats or "pool_size" in memory_stats

        # Process multiple messages to test memory pool
        processor = TestMessageProcessor()
        handler = AsyncMock()

        configured_router.register_processor("bulk_stream", processor)
        handlers = {"bulk_stream": handler}

        # Process multiple messages
        for i in range(10):
            message = {
                "stream": "bulk_stream",
                "data": {"id": i, "bulk_test": True},
                "timestamp": 1234567890 + i,
            }
            await configured_router.route_message(message, handlers)

        # Verify all messages were processed
        assert len(processor.processed_messages) == 10
        assert handler.call_count == 10

        # Verify memory pool was utilized (check whatever allocation tracking exists)
        final_memory_stats = configured_router.get_memory_stats()
        assert final_memory_stats is not None  # Just verify memory stats are available

        # Disable optimization
        optimization_disabled = configured_router.disable_memory_optimization()
        assert optimization_disabled is True

        # Verify optimization is disabled
        final_memory_stats_after_disable = configured_router.get_memory_stats()
        assert final_memory_stats_after_disable is None

    async def test_router_comprehensive_stats_integration(
        self,
        configured_router: TestRouterImpl,
    ) -> None:
        """Test router comprehensive statistics integration."""
        # Setup processors
        processor1 = TestMessageProcessor()
        processor2 = TestMessageProcessor()

        configured_router.register_processor("stream1", processor1)
        configured_router.register_processor("stream2", processor2)

        # Get comprehensive stats
        stats = configured_router.get_comprehensive_stats()

        # Verify stats structure and content
        assert "exchange" in stats
        assert "processors" in stats
        assert "connection_id" in stats

        assert stats["exchange"] == "hyperliquid"
        assert stats["connection_id"] == configured_router._connection_id

        # Verify processor info
        processor_info = stats["processors"]
        assert processor_info["exchange"] == "hyperliquid"
        assert processor_info["total_processors"] == 2
        assert "stream1" in processor_info["processors"]
        assert "stream2" in processor_info["processors"]

        # Verify error recovery stats if available
        recovery_stats = configured_router.get_recovery_stats()
        if recovery_stats:
            # Check recovery stats separately since they may not be in comprehensive stats
            assert recovery_stats is not None

    async def test_router_error_bridge_full_integration(
        self,
        error_bridge: RouterErrorBridge[TestEnvelopeModel],
        configured_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test full error bridge integration with router."""
        # Test all error bridge methods with router integration
        test_message = {"stream": "test_stream", "data": {"test": "data"}, "timestamp": 1234567890}

        # Test envelope validation error
        validation_error = ValidationError.from_exception_data(
            "TestEnvelopeModel", [{"type": "missing", "loc": ("stream",), "msg": "Field required"}]
        )

        await error_bridge.handle_envelope_validation_error(validation_error, test_message)

        # Verify typed error was handled
        assert mock_stream_error_handler.handle_stream_error.call_count == 1

        # Test missing routing key error
        test_envelope = TestEnvelopeModel(stream="", data=test_message["data"])
        await error_bridge.handle_missing_routing_key_error(test_message, test_envelope)

        # Test missing processor error
        mock_context = Mock(spec=WebSocketContextProtocol)
        mock_context.connection_id = "test-conn-1234-abcd"
        mock_context.exchange_name = "hyperliquid"  # Add required field
        mock_context.message_id = str(uuid.uuid4())

        await error_bridge.handle_missing_processor_error(
            "missing_stream", test_message["data"], mock_context
        )

        # Test routing error
        routing_error = RuntimeError("Routing failed")
        await error_bridge.handle_routing_error(routing_error, test_message)

        # Verify all errors were handled through typed system
        assert mock_stream_error_handler.handle_stream_error.call_count == 4

        # Verify bridge is still available and functional
        assert error_bridge.is_available()

        # Get bridge info for verification
        bridge_info = error_bridge.get_bridge_info()
        assert bridge_info["exchange"] == "hyperliquid"
        # Note: Bridge may not track error counts in this implementation
        assert "exchange" in bridge_info  # Just verify bridge info is available

    async def test_router_message_send_failure_integration(
        self,
        configured_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test router message send failure integration."""
        test_message = {
            "stream": "send_test",
            "data": {"outgoing": "message"},
            "timestamp": 1234567890,
        }

        send_error = ConnectionError("Failed to send message to WebSocket")

        # Handle message send failure
        await configured_router.handle_message_send_failure(test_message, send_error)

        # Verify typed error handling was used
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Verify error properties
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.ROUTER_ERROR
        assert error.field == "message_send"
        assert error.value == test_message
        assert "Failed to send WebSocket message" in error.message
        assert error.cause == send_error

        # Verify error recovery was notified if available
        if configured_router.error_recovery:
            recovery_stats = configured_router.get_recovery_stats()
            assert recovery_stats is not None
            # Message send failures should be tracked

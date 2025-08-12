"""Test router error bridge for typed WebSocket error handling.

This test validates Step 41: Create Router Error Handler Bridge.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.websocket.ws_context import ExchangeType
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_exceptions import WebSocketValidationError
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_router import BaseWebSocketRouter
from cyberdelta.apis.websocket.ws_router_error_bridge import RouterErrorBridge
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor


class TestEnvelopeModel(BaseModel):
    """Test envelope model."""

    stream: str
    data: dict


class TestRouterImpl(BaseWebSocketRouter[TestEnvelopeModel]):
    """Test router implementation."""

    def _setup_processors(self) -> None:
        """Setup test processors."""

    def _extract_routing_key_from_envelope(self, envelope: TestEnvelopeModel) -> str | None:
        """Extract routing key."""
        return envelope.stream

    def _extract_payload_from_envelope(self, envelope: TestEnvelopeModel) -> dict:
        """Extract payload."""
        return envelope.data


class TestRouterErrorBridge:
    """Test router error bridge functionality."""

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
        return Mock(spec=TypeSafeWebSocketProcessor)

    @pytest.fixture
    def test_router(
        self, mock_legacy_error_handler: Mock, mock_typed_processor: Mock
    ) -> TestRouterImpl:
        """Create test router."""
        return TestRouterImpl(
            exchange_name="hyperliquid",
            exchange_type=ExchangeType.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
        )

    @pytest.fixture
    def router_error_bridge(
        self,
        test_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
        mock_legacy_error_handler: Mock,
    ) -> RouterErrorBridge[TestEnvelopeModel]:
        """Create router error bridge."""
        return RouterErrorBridge(
            router=test_router,
            stream_error_handler=mock_stream_error_handler,
            legacy_error_handler=mock_legacy_error_handler,
        )

    @pytest.mark.asyncio
    async def test_envelope_validation_error_handling(
        self,
        router_error_bridge: RouterErrorBridge[TestEnvelopeModel],
        mock_stream_error_handler: Mock,
        mock_legacy_error_handler: Mock,
    ) -> None:
        """Test envelope validation error handling through bridge."""
        # Test data
        message = {"invalid": "structure"}
        validation_error = ValidationError.from_exception_data(
            "TestEnvelopeModel", [{"type": "missing", "loc": ("stream",), "msg": "Field required"}]
        )

        # Handle error through bridge
        await router_error_bridge.handle_envelope_validation_error(validation_error, message)

        # Verify stream error handler was called
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Get the error passed to stream handler
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        # Verify error properties
        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.VALIDATION_FAILED
        assert error.field == "envelope"
        assert error.value == message
        assert "Invalid message envelope format" in error.message
        assert error.cause == validation_error

        # Verify legacy handler was not called
        mock_legacy_error_handler.handle_unroutable_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_envelope_validation_error_bridge_failure_fallback(
        self,
        router_error_bridge: RouterErrorBridge[TestEnvelopeModel],
        mock_stream_error_handler: Mock,
        mock_legacy_error_handler: Mock,
    ) -> None:
        """Test fallback to legacy when bridge fails."""
        # Make stream handler fail
        mock_stream_error_handler.handle_stream_error.side_effect = RuntimeError("Bridge failure")

        # Test data
        message = {"invalid": "structure"}
        validation_error = ValueError("Test validation error")

        # Handle error through bridge
        await router_error_bridge.handle_envelope_validation_error(validation_error, message)

        # Verify fallback to legacy handler
        mock_legacy_error_handler.handle_unroutable_message.assert_called_once()
        call_args = mock_legacy_error_handler.handle_unroutable_message.call_args
        assert call_args[1]["message"] == message
        assert "Invalid message envelope format" in call_args[1]["reason"]

    @pytest.mark.asyncio
    async def test_missing_routing_key_error_handling(
        self,
        router_error_bridge: RouterErrorBridge[TestEnvelopeModel],
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test missing routing key error handling through bridge."""
        # Test data
        message = {"stream": "", "data": {}}
        envelope = TestEnvelopeModel(stream="", data={})

        # Handle error through bridge
        await router_error_bridge.handle_missing_routing_key_error(message, envelope)

        # Verify stream error handler was called
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Get the error passed to stream handler
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        # Verify error properties
        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.ROUTER_ERROR
        assert error.field == "routing_key"
        assert error.value == envelope
        assert "Unable to extract routing key" in error.message

    @pytest.mark.asyncio
    async def test_missing_processor_error_handling(
        self,
        router_error_bridge: RouterErrorBridge[TestEnvelopeModel],
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test missing processor error handling through bridge."""
        # Mock context with valid connection_id format
        mock_context = Mock(spec=WebSocketContextProtocol)
        mock_context.connection_id = "conn-123-test-abcd"  # Valid 8+ character format
        mock_context.exchange_name = "hyperliquid"

        # Test data
        routing_key = "unknown_key"
        payload = {"test": "data"}

        # Handle error through bridge
        await router_error_bridge.handle_missing_processor_error(routing_key, payload, mock_context)

        # Verify stream error handler was called
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Get the error passed to stream handler
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        # Verify error properties
        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.PROCESSOR_ERROR
        assert error.field == "routing_key"
        assert error.value == routing_key
        assert f"No processor found for routing key: {routing_key}" in error.message

    @pytest.mark.asyncio
    async def test_missing_handler_error_handling(
        self,
        router_error_bridge: RouterErrorBridge[TestEnvelopeModel],
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test missing handler error handling through bridge."""
        # Test data
        routing_key = "unknown_handler"
        message = {"stream": routing_key, "data": {}}
        available_handlers = ["handler1", "handler2"]

        # Handle error through bridge
        await router_error_bridge.handle_missing_handler_error(
            routing_key, message, available_handlers
        )

        # Verify stream error handler was called
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Get the error passed to stream handler
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        # Verify error properties
        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.HANDLER_ERROR
        assert error.field == "routing_key"
        assert error.value == routing_key
        assert f"No handler found for routing key: {routing_key}" in error.message

    @pytest.mark.asyncio
    async def test_routing_error_handling(
        self,
        router_error_bridge: RouterErrorBridge[TestEnvelopeModel],
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test general routing error handling through bridge."""
        # Test data
        routing_error = RuntimeError("Routing failure")
        message = {"stream": "test", "data": {}}
        routing_stage = "envelope_processing"

        # Handle error through bridge
        await router_error_bridge.handle_routing_error(routing_error, message, routing_stage)

        # Verify stream error handler was called
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Get the error passed to stream handler
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        # Verify error properties
        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.ROUTER_ERROR
        assert error.field == "message_routing"
        assert error.value == message
        assert f"WebSocket routing error: {routing_error!s}" in error.message
        assert error.cause == routing_error

    @pytest.mark.asyncio
    async def test_message_send_failure_handling(
        self,
        router_error_bridge: RouterErrorBridge[TestEnvelopeModel],
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test message send failure handling through bridge."""
        # Test data
        send_error = ConnectionError("Send failure")
        message = {"stream": "test", "data": {}}

        # Handle error through bridge
        await router_error_bridge.handle_message_send_failure(message, send_error)

        # Verify stream error handler was called
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Get the error passed to stream handler
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        # Verify error properties
        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.ROUTER_ERROR
        assert error.field == "message_send"
        assert error.value == message
        assert f"Failed to send WebSocket message: {send_error}" in error.message
        assert error.cause == send_error

    def test_bridge_availability_check(
        self,
        router_error_bridge: RouterErrorBridge[TestEnvelopeModel],
    ) -> None:
        """Test bridge availability checking."""
        # Bridge should be available with all components
        assert router_error_bridge.is_available()

        # Test with missing stream handler
        bridge_without_stream = RouterErrorBridge(
            router=router_error_bridge.router,
            stream_error_handler=None,  # type: ignore[arg-type]
            legacy_error_handler=router_error_bridge.legacy_error_handler,
        )
        assert not bridge_without_stream.is_available()

    def test_bridge_info(
        self,
        router_error_bridge: RouterErrorBridge[TestEnvelopeModel],
    ) -> None:
        """Test bridge information retrieval."""
        info = router_error_bridge.get_bridge_info()

        # Verify info structure
        assert info["bridge_type"] == "RouterErrorBridge"
        assert info["exchange"] == "hyperliquid"
        assert info["router_type"] == "TestRouterImpl"
        assert info["stream_handler_available"] is True
        assert info["legacy_handler_available"] is True
        assert info["is_available"] is True

    @pytest.mark.asyncio
    async def test_missing_processor_bridge_failure_fallback(
        self,
        router_error_bridge: RouterErrorBridge[TestEnvelopeModel],
        mock_stream_error_handler: Mock,
        mock_legacy_error_handler: Mock,
    ) -> None:
        """Test fallback to legacy when missing processor bridge fails."""
        # Make stream handler fail
        mock_stream_error_handler.handle_stream_error.side_effect = RuntimeError("Bridge failure")

        # Mock context for legacy fallback
        mock_context = Mock(spec=WebSocketContextProtocol)
        mock_context.model_dump.return_value = {"connection_id": "conn-123-test-abcd"}

        # Test data
        routing_key = "unknown_key"
        payload = {"test": "data"}

        # Handle error through bridge
        await router_error_bridge.handle_missing_processor_error(routing_key, payload, mock_context)

        # Verify fallback to legacy handler
        mock_legacy_error_handler.handle_processing_error.assert_called_once()
        call_args = mock_legacy_error_handler.handle_processing_error.call_args
        assert f"No processor found for routing key: {routing_key}" in str(call_args[1]["error"])

    @pytest.mark.asyncio
    async def test_error_context_creation_validation(
        self,
        router_error_bridge: RouterErrorBridge[TestEnvelopeModel],
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test that error contexts are created correctly by the bridge."""
        # Test envelope validation error context
        message = {"invalid": "structure"}
        validation_error = ValueError("Test validation error")

        await router_error_bridge.handle_envelope_validation_error(validation_error, message)

        # Get the error and verify context
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        # Verify context contains router information
        assert error.context.exchange == "hyperliquid"
        assert error.context.connection_id == router_error_bridge.router._connection_id

        # Verify router metadata
        router_metadata = error.context.extra_context["router_metadata"]
        assert router_metadata["router_type"] == "TestRouterImpl"
        assert router_metadata["exchange_name"] == "hyperliquid"
        assert router_metadata["error_stage"] == "envelope_validation"

        # Verify raw message is preserved
        assert error.context.extra_context["raw_message"] == message

"""Test router integration with typed error system for routing errors.

This test specifically validates Step 39: Update ws_router.py - Routing Errors.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_exceptions import WebSocketValidationError
from cyberdelta.apis.websocket.ws_router import BaseWebSocketRouter
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor
from cyberdelta.enums import ExchangeName


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


class TestRouterRoutingErrorsIntegration:
    """Test router integration with typed error system for routing errors."""

    @pytest.fixture
    def mock_legacy_error_handler(self) -> Mock:
        """Create mock legacy error handler."""
        mock = Mock()
        mock.handle_unroutable_message = AsyncMock()
        mock.handle_routing_error = AsyncMock()
        return mock

    @pytest.fixture
    def mock_typed_error_handler(self) -> Mock:
        """Create mock typed error handler."""
        mock = Mock(spec=WebSocketStreamErrorHandler)
        mock.handle_stream_error = AsyncMock()
        return mock

    @pytest.fixture
    def mock_typed_processor(self) -> Mock:
        """Create mock typed processor."""
        return Mock(spec=TypeSafeWebSocketProcessor)

    @pytest.mark.asyncio
    async def test_envelope_validation_error_with_typed_handler(
        self,
        mock_legacy_error_handler: Mock,
        mock_typed_error_handler: Mock,
        mock_typed_processor: Mock,
    ) -> None:
        """Test envelope validation error uses typed error system when available."""
        # Create router with typed error handler
        router = TestRouterImpl(
            exchange_name=ExchangeName.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
            stream_error_handler=mock_typed_error_handler,
        )

        # Test invalid message that will fail envelope validation
        invalid_message = {"invalid": "structure"}
        validation_error = ValidationError.from_exception_data(
            "TestEnvelopeModel", [{"type": "missing", "loc": ("stream",), "msg": "Field required"}]
        )

        # Call envelope validation error handler directly
        await router._handle_envelope_validation_error(validation_error, invalid_message)

        # Verify typed error handler was called
        mock_typed_error_handler.handle_stream_error.assert_called_once()

        # Get the error that was passed
        call_args = mock_typed_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        # Verify error properties
        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.VALIDATION_FAILED
        assert error.field == "envelope"
        assert error.value == invalid_message
        assert "Invalid message envelope format" in error.message
        assert error.cause == validation_error

        # Verify legacy handler was not called
        mock_legacy_error_handler.handle_unroutable_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_envelope_validation_error_fallback_to_legacy(
        self,
        mock_legacy_error_handler: Mock,
        mock_typed_processor: Mock,
    ) -> None:
        """Test envelope validation error falls back to legacy when no typed handler."""
        # Create router without typed error handler
        router = TestRouterImpl(
            exchange_name=ExchangeName.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
            stream_error_handler=None,  # No typed handler
        )

        # Test invalid message
        invalid_message = {"invalid": "structure"}
        validation_error = ValueError("Test validation error")

        # Call envelope validation error handler
        await router._handle_envelope_validation_error(validation_error, invalid_message)

        # Verify legacy handler was called
        mock_legacy_error_handler.handle_unroutable_message.assert_called_once()

        call_args = mock_legacy_error_handler.handle_unroutable_message.call_args
        assert call_args[1]["message"] == invalid_message
        assert "Invalid message envelope format" in call_args[1]["reason"]

    @pytest.mark.asyncio
    async def test_missing_routing_key_error_with_typed_handler(
        self,
        mock_legacy_error_handler: Mock,
        mock_typed_error_handler: Mock,
        mock_typed_processor: Mock,
    ) -> None:
        """Test missing routing key error uses typed error system when available."""
        # Create router with typed error handler
        router = TestRouterImpl(
            exchange_name=ExchangeName.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
            stream_error_handler=mock_typed_error_handler,
        )

        # Test message and envelope
        message = {"stream": "", "data": {}}  # Empty stream will result in no routing key
        envelope = TestEnvelopeModel(stream="", data={})

        # Call missing routing key handler
        await router._handle_missing_routing_key(message, envelope)

        # Verify typed error handler was called
        mock_typed_error_handler.handle_stream_error.assert_called_once()

        # Get the error that was passed
        call_args = mock_typed_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        # Verify error properties
        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.ROUTER_ERROR
        assert error.field == "routing_key"
        assert error.value == envelope
        assert "Unable to extract routing key" in error.message

        # Verify legacy handler was not called
        mock_legacy_error_handler.handle_unroutable_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_general_routing_error_with_typed_handler(
        self,
        mock_legacy_error_handler: Mock,
        mock_typed_error_handler: Mock,
        mock_typed_processor: Mock,
    ) -> None:
        """Test general routing error uses typed error system when available."""

        # Create a mock envelope validator that will throw an exception during routing
        def failing_envelope_validator(message: dict) -> TestEnvelopeModel:
            raise RuntimeError("Envelope validator failure during routing")

        # Create router with typed error handler and failing envelope validator
        router = TestRouterImpl(
            exchange_name=ExchangeName.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
            stream_error_handler=mock_typed_error_handler,
            envelope_validator=failing_envelope_validator,
        )

        # Test message and empty handlers
        message = {"stream": "test", "data": {}}
        handlers = {}

        # Call route_message which should trigger RuntimeError during envelope validation
        await router.route_message(message, handlers)

        # Verify typed error handler was called
        mock_typed_error_handler.handle_stream_error.assert_called_once()

        # Get the error that was passed
        call_args = mock_typed_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        # Verify error properties
        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.ROUTER_ERROR
        assert error.field == "message_routing"
        assert error.value == message
        assert "WebSocket routing error" in error.message

        # Verify legacy handler was not called for the routing error
        mock_legacy_error_handler.handle_routing_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_routing_error_context_creation(
        self,
        mock_legacy_error_handler: Mock,
        mock_typed_error_handler: Mock,
        mock_typed_processor: Mock,
    ) -> None:
        """Test that routing error context is created correctly."""

        # Create a failing envelope validator
        def failing_envelope_validator(message: dict) -> TestEnvelopeModel:
            raise KeyError("Missing required key during routing")

        # Create router with typed error handler and failing envelope validator
        router = TestRouterImpl(
            exchange_name=ExchangeName.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
            stream_error_handler=mock_typed_error_handler,
            envelope_validator=failing_envelope_validator,
        )

        # Call route_message to trigger error
        message = {"stream": "test", "data": {"key": "value"}}
        await router.route_message(message, {})

        # Get the error that was passed to the typed handler
        call_args = mock_typed_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        # Verify error context contains router information
        assert error.context.exchange == "hyperliquid"
        assert error.context.topic is None  # General routing errors don't have specific topic

        # Verify router metadata in extra_context
        router_metadata = error.context.extra_context["router_metadata"]
        assert router_metadata["router_type"] == "TestRouterImpl"
        assert router_metadata["exchange_name"] == "hyperliquid"
        assert router_metadata["connection_id"] == router._connection_id
        assert router_metadata["error_stage"] == "message_routing"

        # Verify raw message is preserved
        assert error.context.extra_context["raw_message"] == message

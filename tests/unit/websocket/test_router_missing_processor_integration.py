"""Test router integration with typed error system for missing processor handling.

This test specifically validates Step 38: Update ws_router.py - Missing Processor Handling.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel

from cyberdelta.apis.websocket.ws_context import ExchangeType
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_exceptions import WebSocketValidationError
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_router import BaseWebSocketRouter
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
        # Leave empty to test missing processor scenario

    def _extract_routing_key_from_envelope(self, envelope: TestEnvelopeModel) -> str | None:
        """Extract routing key."""
        return envelope.stream

    def _extract_payload_from_envelope(self, envelope: TestEnvelopeModel) -> dict:
        """Extract payload."""
        return envelope.data


class TestRouterMissingProcessorIntegration:
    """Test router integration with typed error system for missing processors."""

    @pytest.fixture
    def mock_context(self) -> Mock:
        """Create mock WebSocket context."""
        context = Mock(spec=WebSocketContextProtocol)
        context.connection_id = "test-conn-12345"
        context.exchange_name = "hyperliquid"
        context.channel = "test-channel"
        context.sequence_number = 123
        context.model_dump.return_value = {
            "connection_id": "test-conn-12345",
            "exchange_name": "hyperliquid",
            "channel": "test-channel",
            "sequence_number": 123,
        }
        return context

    @pytest.fixture
    def mock_legacy_error_handler(self) -> Mock:
        """Create mock legacy error handler."""
        handler = Mock()
        handler.handle_processing_error = AsyncMock()
        return handler

    @pytest.fixture
    def mock_typed_error_handler(self) -> Mock:
        """Create mock typed error handler."""
        handler = Mock(spec=WebSocketStreamErrorHandler)
        handler.handle_stream_error = AsyncMock()
        return handler

    @pytest.fixture
    def mock_typed_processor(self) -> Mock:
        """Create mock typed processor."""
        return Mock(spec=TypeSafeWebSocketProcessor)

    @pytest.mark.asyncio
    async def test_missing_processor_with_typed_error_handler(
        self,
        mock_context: Mock,
        mock_legacy_error_handler: Mock,
        mock_typed_error_handler: Mock,
        mock_typed_processor: Mock,
    ) -> None:
        """Test missing processor handling uses typed error system when available."""
        # Create router with typed error handler
        router = TestRouterImpl(
            exchange_name="hyperliquid",
            exchange_type=ExchangeType.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
            stream_error_handler=mock_typed_error_handler,
        )

        # Call missing processor handler
        await router._handle_missing_processor(
            routing_key="unknown.route",
            payload={"test": "data"},
            context=mock_context,
        )

        # Verify typed error handler was called
        mock_typed_error_handler.handle_stream_error.assert_called_once()

        # Verify the error passed to typed handler
        call_args = mock_typed_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.PROCESSOR_ERROR
        assert "No processor found for routing key: unknown.route" in error.message
        assert error.field == "routing_key"
        assert error.value == "unknown.route"

        # Verify legacy error handler was NOT called
        mock_legacy_error_handler.handle_processing_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_processor_fallback_to_legacy(
        self,
        mock_context: Mock,
        mock_legacy_error_handler: Mock,
        mock_typed_processor: Mock,
    ) -> None:
        """Test missing processor handling falls back to legacy system when no typed handler."""
        # Create router without typed error handler
        router = TestRouterImpl(
            exchange_name="hyperliquid",
            exchange_type=ExchangeType.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
            stream_error_handler=None,  # No typed handler
        )

        # Call missing processor handler
        await router._handle_missing_processor(
            routing_key="unknown.route",
            payload={"test": "data"},
            context=mock_context,
        )

        # Verify legacy error handler was called
        mock_legacy_error_handler.handle_processing_error.assert_called_once()

        # Verify the arguments passed to legacy handler
        call_args = mock_legacy_error_handler.handle_processing_error.call_args
        assert call_args[1]["error"] is not None
        assert "No processor found for routing key: unknown.route" in str(call_args[1]["error"])
        assert call_args[1]["payload"] == {"test": "data"}
        assert call_args[1]["context"]["connection_id"] == "test-conn-12345"

    @pytest.mark.asyncio
    async def test_missing_processor_with_list_payload(
        self,
        mock_context: Mock,
        mock_legacy_error_handler: Mock,
        mock_typed_error_handler: Mock,
        mock_typed_processor: Mock,
    ) -> None:
        """Test missing processor handling with list payload (should wrap in dict for legacy)."""
        # Create router with typed error handler
        router = TestRouterImpl(
            exchange_name="hyperliquid",
            exchange_type=ExchangeType.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
            stream_error_handler=mock_typed_error_handler,
        )

        list_payload = ["item1", "item2"]

        # Call missing processor handler
        await router._handle_missing_processor(
            routing_key="unknown.route",
            payload=list_payload,
            context=mock_context,
        )

        # Verify typed error handler was called
        mock_typed_error_handler.handle_stream_error.assert_called_once()

        # Verify the error context was created with the list payload
        call_args = mock_typed_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        assert isinstance(error, WebSocketValidationError)
        assert error.code == WebSocketErrorCode.PROCESSOR_ERROR

        # Check that the RouterErrorContextBuilder received the list payload
        assert error.context.extra_context["raw_message"] == {"data": list_payload}

    @pytest.mark.asyncio
    async def test_router_error_context_creation(
        self,
        mock_context: Mock,
        mock_legacy_error_handler: Mock,
        mock_typed_error_handler: Mock,
        mock_typed_processor: Mock,
    ) -> None:
        """Test that router error context is created correctly."""
        # Create router with typed error handler
        router = TestRouterImpl(
            exchange_name="hyperliquid",
            exchange_type=ExchangeType.HYPERLIQUID,
            error_handler=mock_legacy_error_handler,
            typed_processor=mock_typed_processor,
            stream_error_handler=mock_typed_error_handler,
        )

        # Call missing processor handler
        await router._handle_missing_processor(
            routing_key="test.route",
            payload={"message": "test"},
            context=mock_context,
        )

        # Get the error that was passed to the typed handler
        call_args = mock_typed_error_handler.handle_stream_error.call_args
        error = call_args[0][0]

        # Verify error context contains router information
        assert error.context.connection_id == "test-conn-12345"
        assert error.context.exchange == "hyperliquid"
        assert error.context.channel == "test-channel"
        assert error.context.topic == "test.route"  # Uses routing key as topic
        assert error.context.sequence_number == 123

        # Verify router metadata in extra_context
        router_metadata = error.context.extra_context["router_metadata"]
        assert router_metadata["router_type"] == "TestRouterImpl"
        assert router_metadata["exchange_name"] == "hyperliquid"
        assert router_metadata["connection_id"] == router._connection_id
        assert router_metadata["routing_key"] == "test.route"
        assert router_metadata["error_stage"] == "processor_lookup"
        assert router_metadata["available_processors"] == []  # Empty because no processors set up

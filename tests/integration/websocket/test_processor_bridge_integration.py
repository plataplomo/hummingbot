"""Focused integration tests for WebSocket Processor Error Bridge.

This module provides focused integration tests for the PydanticWebSocketProcessor
integration with the ProcessorErrorBridge and typed error handling system.
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel

from cyberdelta.apis.websocket.ws_processor import (
    MessageTransformer,
    PydanticWebSocketProcessor,
    SimpleDictTransformer,
)
from cyberdelta.apis.websocket.ws_processor_error_bridge import ProcessorErrorBridge
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler


class IntegrationMessage(BaseModel):
    """Integration test message model."""

    message_id: str
    data: dict[str, Any]


class IntegrationDomainModel(BaseModel):
    """Integration test domain model."""

    processed_id: str
    processed_data: dict[str, Any]
    processing_timestamp: float


class IntegrationTransformer(MessageTransformer[IntegrationMessage, IntegrationDomainModel]):
    """Integration test transformer."""

    def transform(
        self, validated: IntegrationMessage, context: WebSocketContextProtocol | None = None
    ) -> IntegrationDomainModel:
        return IntegrationDomainModel(
            processed_id=f"processed_{validated.message_id}",
            processed_data=validated.data,
            processing_timestamp=time.time(),
        )


class TestProcessorBridgeIntegration:
    """Test processor error bridge integration."""

    @pytest.fixture
    def mock_stream_error_handler(self) -> AsyncMock:
        """Create mock stream error handler."""
        handler = AsyncMock(spec=WebSocketStreamErrorHandler)
        handler.handle_validation_error = AsyncMock()
        handler.handle_stream_error = AsyncMock()
        return handler

    @pytest.fixture
    def mock_legacy_error_handler(self) -> AsyncMock:
        """Create mock legacy error handler."""
        handler = AsyncMock()
        handler.handle_validation_error = AsyncMock()
        handler.handle_processing_error = AsyncMock()
        handler.handle_connection_error = AsyncMock()
        return handler

    @pytest.fixture
    def mock_context(self) -> Mock:
        """Create mock WebSocket context."""
        context = Mock(spec=WebSocketContextProtocol)
        context.connection_id = "bridge-integration-test-conn-12345"
        context.exchange_name = "hyperliquid"
        context.exchange_type = "hyperliquid"
        context.routing_key = "bridge.integration.test"
        context.channel = "bridge_integration_channel"
        context.sequence_number = 54321
        context.domain_model = None

        # Mock context creation for error handling
        context.create_error_context.return_value = StreamErrorContext(
            connection_id="bridge-integration-test-conn-12345",
            exchange="hyperliquid",
            channel="bridge_integration_channel",
            topic="bridge.integration.test",
            sequence_number=54321,
            error_timestamp_ms=int(time.time() * 1000),
        )

        return context

    def test_processor_with_error_bridge_initialization(
        self,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
    ) -> None:
        """Test processor initialization with error bridge."""
        processor = PydanticWebSocketProcessor(
            raw_model=IntegrationMessage,
            transformer=IntegrationTransformer(),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="BridgeIntegrationProcessor",
        )

        # Verify processor initialization
        assert processor.processor_name == "BridgeIntegrationProcessor"
        assert processor.raw_model == IntegrationMessage
        assert isinstance(processor.transformer, IntegrationTransformer)
        assert processor.stream_error_handler is mock_stream_error_handler

        # Verify error bridge was created and properly connected
        assert hasattr(processor, "error_bridge")
        assert processor.error_bridge is not None
        assert isinstance(processor.error_bridge, ProcessorErrorBridge)
        assert processor.error_bridge.processor is processor
        assert processor.error_bridge.stream_error_handler is mock_stream_error_handler

    @pytest.mark.asyncio
    async def test_successful_processing_through_bridge(
        self,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
        mock_context: Mock,
    ) -> None:
        """Test successful message processing through error bridge."""
        processor = PydanticWebSocketProcessor(
            raw_model=IntegrationMessage,
            transformer=IntegrationTransformer(),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="SuccessBridgeProcessor",
        )

        message_handler = AsyncMock()

        # Valid test payload
        payload = {
            "message_id": "bridge_success_001",
            "data": {"bridge": "integration", "success": True},
        }

        # Process message
        await processor.process(payload, message_handler, mock_context)

        # Verify handler was called
        message_handler.assert_called_once()

        # Verify domain model was set on context
        assert mock_context.domain_model is not None
        domain_model = mock_context.domain_model
        assert isinstance(domain_model, IntegrationDomainModel)
        assert domain_model.processed_id == "processed_bridge_success_001"
        assert domain_model.processed_data == {"bridge": "integration", "success": True}

        # Verify no error handlers were called
        mock_stream_error_handler.handle_validation_error.assert_not_called()
        mock_stream_error_handler.handle_stream_error.assert_not_called()
        mock_legacy_error_handler.handle_validation_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_validation_error_through_bridge(
        self,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
        mock_context: Mock,
    ) -> None:
        """Test validation error handling through error bridge."""
        processor = PydanticWebSocketProcessor(
            raw_model=IntegrationMessage,
            transformer=IntegrationTransformer(),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="ValidationBridgeProcessor",
        )

        message_handler = AsyncMock()

        # Invalid payload (missing required fields)
        invalid_payload = {
            "message_id": "bridge_validation_error",
            # Missing 'data' field
        }

        # Process invalid message
        await processor.process(invalid_payload, message_handler, mock_context)

        # Verify handler was not called
        message_handler.assert_not_called()

        # Verify typed error handler was called through bridge
        mock_stream_error_handler.handle_validation_error.assert_called_once()

        # Check the call arguments
        call_args = mock_stream_error_handler.handle_validation_error.call_args
        assert call_args.kwargs["context"] is mock_context
        assert isinstance(call_args.kwargs["payload"], IntegrationMessage)

        # Verify legacy handler was not called (bridge handles it)
        mock_legacy_error_handler.handle_validation_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_handler_error_through_bridge(
        self,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
        mock_context: Mock,
    ) -> None:
        """Test handler error handling through error bridge."""
        processor = PydanticWebSocketProcessor(
            raw_model=IntegrationMessage,
            transformer=IntegrationTransformer(),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="HandlerBridgeProcessor",
        )

        # Handler that raises an expected error
        failing_handler = AsyncMock(side_effect=KeyError("Bridge handler error"))

        # Valid payload
        payload = {"message_id": "bridge_handler_error", "data": {"test": "handler_error"}}

        # Process message with failing handler
        await processor.process(payload, failing_handler, mock_context)

        # Verify handler was called but failed
        failing_handler.assert_called_once()

        # Verify domain model was set before handler failure
        assert mock_context.domain_model is not None

        # Verify typed error handler was called through bridge
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Verify legacy handler was not called
        mock_legacy_error_handler.handle_processing_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_error_bridge_context_creation(
        self,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
        mock_context: Mock,
    ) -> None:
        """Test error bridge creates enhanced context correctly."""
        processor = PydanticWebSocketProcessor(
            raw_model=IntegrationMessage,
            transformer=IntegrationTransformer(),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="ContextBridgeProcessor",
        )

        error_bridge = processor.error_bridge
        assert error_bridge is not None

        # Test creating enhanced context
        test_error = ValueError("Bridge context test")
        enhanced_context = await error_bridge.create_enhanced_error_context(
            base_context=mock_context, error=test_error, stage="bridge_test"
        )

        # Verify enhanced context contains required fields
        assert enhanced_context["connection_id"] == "bridge-integration-test-conn-12345"
        assert enhanced_context["exchange"] == "hyperliquid"
        assert enhanced_context["processor_name"] == "ContextBridgeProcessor"
        assert enhanced_context["error_type"] == "ValueError"
        assert enhanced_context["stage"] == "bridge_test"
        assert "processor_stats" in enhanced_context
        # Enhanced context contains all required fields
        # Note: timestamp field is not included in this implementation

        # Verify processor stats are included
        processor_stats = enhanced_context["processor_stats"]
        assert "total_processed" in processor_stats
        assert "total_errors" in processor_stats
        assert "error_rate" in processor_stats
        assert "uptime_seconds" in processor_stats

    def test_error_bridge_availability_check(
        self,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
    ) -> None:
        """Test error bridge availability checking."""
        processor = PydanticWebSocketProcessor(
            raw_model=IntegrationMessage,
            transformer=IntegrationTransformer(),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="AvailabilityBridgeProcessor",
        )

        error_bridge = processor.error_bridge
        assert error_bridge is not None

        # Test bridge availability
        assert error_bridge.should_use_typed_handler() is True

        # Test processor without typed handler
        processor_no_typed = PydanticWebSocketProcessor(
            raw_model=IntegrationMessage,
            transformer=SimpleDictTransformer[IntegrationMessage](),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=None,  # No typed handler
            processor_name="NoTypedProcessor",
        )

        # Should have no error bridge when no typed handler
        assert processor_no_typed.error_bridge is None

    @pytest.mark.asyncio
    async def test_processor_metrics_with_bridge(
        self,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
        mock_context: Mock,
    ) -> None:
        """Test processor metrics integration with error bridge."""
        processor = PydanticWebSocketProcessor(
            raw_model=IntegrationMessage,
            transformer=IntegrationTransformer(),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="MetricsBridgeProcessor",
        )

        message_handler = AsyncMock()

        # Process successful message
        success_payload = {"message_id": "metrics_success", "data": {"metrics": "test"}}
        await processor.process(success_payload, message_handler, mock_context)

        # Process validation error
        invalid_payload = {"message_id": "metrics_invalid"}
        await processor.process(invalid_payload, message_handler, mock_context)

        # Verify processor metrics
        assert processor.metrics.total_processed == 1  # Only successful counts
        assert processor.metrics.validation_errors == 1
        assert processor.metrics.transformation_errors == 0
        assert processor.metrics.handler_errors == 0

        # Test bridge stats integration
        error_bridge = processor.error_bridge
        assert error_bridge is not None

        bridge_stats = error_bridge.get_processor_stats()
        assert bridge_stats["total_processed"] == 1
        assert bridge_stats["total_errors"] == 1
        assert bridge_stats["error_rate"] == 1.0  # 1 error vs 1 successfully processed
        assert "uptime_seconds" in bridge_stats

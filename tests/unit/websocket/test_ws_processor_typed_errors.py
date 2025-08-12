"""Unit tests for PydanticWebSocketProcessor with typed error handling.

This module tests the processor integration with the new typed WebSocket error system,
ensuring proper error handling and bridge functionality.
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.websocket.ws_exceptions import WebSocketValidationError
from cyberdelta.apis.websocket.ws_processor import (
    MessageTransformer,
    PydanticWebSocketProcessor,
    SimpleDictTransformer,
)
from cyberdelta.apis.websocket.ws_processor_error_context import ProcessorErrorContextBuilder
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler


class MessageForTest(BaseModel):
    """Test message model."""

    id: str
    value: int
    timestamp: float = None


class DomainModelForTest(BaseModel):
    """Test domain model."""

    message_id: str
    processed_value: int
    processing_time: float


class TransformerForTest(MessageTransformer[MessageForTest, DomainModelForTest]):
    """Test transformer."""

    def transform(
        self, validated: MessageForTest, context: WebSocketContextProtocol | None = None
    ) -> DomainModelForTest:
        return DomainModelForTest(
            message_id=validated.id,
            processed_value=validated.value * 2,
            processing_time=time.time(),
        )


class FailingTransformer(MessageTransformer[MessageForTest, DomainModelForTest]):
    """Transformer that always fails."""

    def transform(
        self, validated: MessageForTest, context: WebSocketContextProtocol | None = None
    ) -> DomainModelForTest:
        raise ValueError("Transformation failed")


class TestTypedProcessorErrorHandling:
    """Test processor error handling with typed error system."""

    @pytest.fixture
    def mock_context(self) -> Mock:
        """Create mock WebSocket context."""
        context = Mock(spec=WebSocketContextProtocol)
        context.connection_id = "test-connection-123"
        context.exchange_name = "hyperliquid"
        context.exchange_type = "hyperliquid"  # Add missing attribute
        context.routing_key = "test.route"
        context.channel = "test_channel"
        context.sequence_number = 12345
        context.domain_model = None  # Initialize domain_model attribute
        context.create_error_context.return_value = StreamErrorContext(
            connection_id="test-connection-123",
            exchange="hyperliquid",
            channel="test_channel",
            topic="test.route",
            sequence_number=12345,
            error_timestamp_ms=int(time.time() * 1000),
        )
        return context

    @pytest.fixture
    def mock_stream_error_handler(self) -> AsyncMock:
        """Create mock stream error handler."""
        handler = AsyncMock(spec=WebSocketStreamErrorHandler)
        return handler

    @pytest.fixture
    def mock_legacy_error_handler(self) -> AsyncMock:
        """Create mock legacy error handler."""
        handler = AsyncMock()
        handler.handle_validation_error = AsyncMock()
        handler.handle_processing_error = AsyncMock()
        handler.handle_connection_error = AsyncMock()
        return handler

    def test_processor_with_typed_error_handler_creation(
        self,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
    ) -> None:
        """Test processor creation with typed error handler."""
        processor = PydanticWebSocketProcessor(
            raw_model=MessageForTest,
            transformer=SimpleDictTransformer[MessageForTest](),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="TestProcessor",
        )

        assert processor.stream_error_handler is mock_stream_error_handler
        assert processor.processor_name == "TestProcessor"
        # Bridge pattern removed - processor uses direct stream error handler
        assert processor.stream_error_handler is not None
        assert isinstance(processor.stream_error_handler, WebSocketStreamErrorHandler)

    @pytest.mark.asyncio
    async def test_validation_error_with_typed_handler(
        self,
        mock_context: Mock,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
    ) -> None:
        """Test validation error handling with typed error system."""
        processor = PydanticWebSocketProcessor(
            raw_model=MessageForTest,
            transformer=SimpleDictTransformer[MessageForTest](),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="ValidationTestProcessor",
        )

        handler = AsyncMock()

        # Invalid payload (missing required fields)
        invalid_payload = {"id": "test", "invalid_field": "value"}

        # Execute
        await processor.process(invalid_payload, handler, mock_context)

        # Verify typed error handler was called
        mock_stream_error_handler.handle_validation_error.assert_called_once()

        # Check call arguments
        call_args = mock_stream_error_handler.handle_validation_error.call_args
        assert isinstance(call_args.kwargs["error"], ValidationError)
        assert call_args.kwargs["context"] is mock_context
        assert isinstance(call_args.kwargs["payload"], MessageForTest)

        # Legacy handler should not be called
        mock_legacy_error_handler.handle_validation_error.assert_not_called()

        # Message handler should not be called
        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_transformation_error_with_typed_handler(
        self,
        mock_context: Mock,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
    ) -> None:
        """Test transformation error handling with typed error system."""
        processor = PydanticWebSocketProcessor(
            raw_model=MessageForTest,
            transformer=FailingTransformer(),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="TransformationTestProcessor",
        )

        handler = AsyncMock()

        # Valid payload
        valid_payload = {"id": "test-123", "value": 42, "timestamp": time.time()}

        # Execute
        await processor.process(valid_payload, handler, mock_context)

        # Verify typed error handler handled the stream error
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Check the error was a WebSocketValidationError
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args.args[0]
        assert isinstance(error, WebSocketValidationError)
        assert "Transformation failed" in error.message
        assert error.field == "transformation"
        assert isinstance(error.cause, ValueError)

        # Legacy handler should not be called
        mock_legacy_error_handler.handle_processing_error.assert_not_called()

        # Message handler should not be called
        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_handler_error_with_typed_handler(
        self,
        mock_context: Mock,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
    ) -> None:
        """Test message handler error handling with typed error system."""
        processor = PydanticWebSocketProcessor(
            raw_model=MessageForTest,
            transformer=TransformerForTest(),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="HandlerTestProcessor",
        )

        # Handler that raises an exception (use KeyError which is expected)
        failing_handler_mock = AsyncMock(side_effect=KeyError("Handler failed"))

        # Valid payload
        valid_payload = {"id": "test-123", "value": 42, "timestamp": time.time()}

        # Execute
        await processor.process(valid_payload, failing_handler_mock, mock_context)

        # Verify handler was called but failed
        failing_handler_mock.assert_called_once()

        # Verify typed error handler handled the stream error
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Check the error details
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args.args[0]
        assert isinstance(error, WebSocketValidationError)
        assert "Handler invocation failed" in error.message
        assert error.field == "handler"
        assert isinstance(error.cause, KeyError)

        # Legacy handler should not be called
        mock_legacy_error_handler.handle_processing_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_unexpected_error_with_typed_handler(
        self,
        mock_context: Mock,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
    ) -> None:
        """Test unexpected error handling with typed error system."""

        # Create processor with a transformer that raises an unexpected error
        class UnexpectedErrorTransformer(MessageTransformer[MessageForTest, DomainModelForTest]):
            def transform(
                self, validated: MessageForTest, context: WebSocketContextProtocol | None = None
            ) -> DomainModelForTest:
                raise OSError("Unexpected system error")

        processor = PydanticWebSocketProcessor(
            raw_model=MessageForTest,
            transformer=UnexpectedErrorTransformer(),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="UnexpectedErrorTestProcessor",
        )

        handler = AsyncMock()

        # Valid payload
        valid_payload = {"id": "test-123", "value": 42, "timestamp": time.time()}

        # Execute
        await processor.process(valid_payload, handler, mock_context)

        # Verify typed error handler handled the unexpected error
        mock_stream_error_handler.handle_stream_error.assert_called_once()

        # Check the error details
        call_args = mock_stream_error_handler.handle_stream_error.call_args
        error = call_args.args[0]
        assert isinstance(error, WebSocketValidationError)
        assert "Unexpected processing error" in error.message
        assert error.field == "processing_pipeline"
        assert isinstance(error.cause, OSError)

        # Handler should not be called due to transformation failure
        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_error_when_no_typed_handler_swallowed(
        self,
        mock_context: Mock,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
    ) -> None:
        """Test processor behavior when no typed handler is available - error is caught and logged."""
        processor = PydanticWebSocketProcessor(
            raw_model=MessageForTest,
            transformer=SimpleDictTransformer[MessageForTest](),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=None,  # No typed handler
            processor_name="NoTypedHandlerTestProcessor",
        )

        handler = AsyncMock()

        # Invalid payload
        invalid_payload = {"id": "test", "invalid_field": "value"}

        # Execute - error should be caught and logged, not raised
        # The test passes if this doesn't raise an exception
        await processor.process(invalid_payload, handler, mock_context)

        # Handler should not be called
        handler.assert_not_called()

        # Legacy error handler should not be called since error is swallowed
        mock_legacy_error_handler.handle_validation_error.assert_not_called()

        # Typed error handler should be called if available (but it's None here)
        mock_stream_error_handler.handle_stream_error.assert_not_called()

        # Processor metrics should reflect the validation error
        assert processor.metrics.validation_errors > 0

    @pytest.mark.asyncio
    async def test_successful_processing_with_typed_handler(
        self,
        mock_context: Mock,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
    ) -> None:
        """Test successful message processing with typed error handler available."""
        processor = PydanticWebSocketProcessor(
            raw_model=MessageForTest,
            transformer=TransformerForTest(),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="SuccessTestProcessor",
        )

        handler = AsyncMock()

        # Valid payload
        valid_payload = {"id": "test-123", "value": 42, "timestamp": time.time()}

        # Execute
        await processor.process(valid_payload, handler, mock_context)

        # Verify successful processing
        handler.assert_called_once()

        # Check handler was called with context that has domain_model
        call_args = handler.call_args
        context_arg = call_args.args[0]
        assert context_arg is mock_context

        # Check that the domain_model was set on the context
        assert hasattr(mock_context, "domain_model")
        domain_model = mock_context.domain_model
        assert isinstance(domain_model, DomainModelForTest)
        assert domain_model.message_id == "test-123"
        assert domain_model.processed_value == 84  # 42 * 2

        # No error handlers should be called
        mock_stream_error_handler.handle_validation_error.assert_not_called()
        mock_stream_error_handler.handle_stream_error.assert_not_called()
        mock_legacy_error_handler.handle_validation_error.assert_not_called()
        mock_legacy_error_handler.handle_processing_error.assert_not_called()

    def test_processor_error_bridge_integration(
        self,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
    ) -> None:
        """Test that processor correctly initializes error bridge."""
        processor = PydanticWebSocketProcessor(
            raw_model=MessageForTest,
            transformer=TransformerForTest(),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="BridgeTestProcessor",
        )

        # Verify bridge was created
        assert hasattr(processor, "error_bridge")
        assert processor.error_bridge is not None
        # Bridge pattern removed - processor uses direct stream error handler
        assert processor.stream_error_handler is not None

        # Verify bridge references are correct
        assert processor.error_bridge.processor is processor
        assert processor.error_bridge.stream_error_handler is mock_stream_error_handler

    def test_processor_metrics_with_typed_errors(
        self,
        mock_stream_error_handler: AsyncMock,
        mock_legacy_error_handler: AsyncMock,
    ) -> None:
        """Test processor metrics are updated correctly with typed error system."""
        processor = PydanticWebSocketProcessor(
            raw_model=MessageForTest,
            transformer=SimpleDictTransformer[MessageForTest](),
            error_handler=mock_legacy_error_handler,
            stream_error_handler=mock_stream_error_handler,
            processor_name="MetricsTestProcessor",
        )

        # Initial metrics
        initial_metrics = processor.get_metrics()
        assert initial_metrics.processing_metrics.total_processed == 0
        assert initial_metrics.processing_metrics.get_total_errors() == 0

        # The processor should still track metrics even with typed error handling
        assert hasattr(processor, "metrics")
        assert processor.metrics.total_processed == 0
        assert processor.metrics.validation_errors == 0

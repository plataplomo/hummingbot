"""Unit tests for PydanticWebSocketProcessor with typed error system.

This module tests the processor with the new typed error handling system,
including ProcessingMetrics, ProcessorErrorBridge, and WebSocketStreamErrorHandler.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from cyberdelta.apis.backpack.bp_ws_context import BackpackMessageContext
from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.websocket.ws_context import ExchangeType
from cyberdelta.apis.websocket.ws_error_handler import BaseErrorHandler
from cyberdelta.apis.websocket.ws_processing_metrics import ProcessingMetrics, ProcessorMetrics
from cyberdelta.apis.websocket.ws_processor import (
    ProcessorFactory,
    PydanticWebSocketProcessor,
    SimpleDictTransformer,
)
from cyberdelta.apis.websocket.ws_processor_error_bridge import ProcessorErrorBridge
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler


class MessageModel(BaseModel):
    """Test Pydantic model for processor testing."""

    id: str
    value: int
    data: dict[str, Any]


class DomainModel(BaseModel):
    """Test domain model for transformation testing."""

    message_id: str
    processed_value: int
    metadata: dict[str, Any]


class TestTransformer:
    """Test transformer for converting MessageModel to DomainModel."""

    def transform(
        self, validated: MessageModel, context: WebSocketContextProtocol | None = None
    ) -> DomainModel:
        """Transform test message to domain model.

        Returns:
            DomainModel: Transformed domain model with processed values.
        """
        return DomainModel(
            message_id=validated.id,
            processed_value=validated.value * 2,
            metadata=validated.data,
        )


class FailingTransformer:
    """Transformer that always fails for testing error handling."""

    def transform(
        self, validated: MessageModel, context: WebSocketContextProtocol | None = None
    ) -> DomainModel:
        """Always raise an exception.

        Raises:
            ValueError: Always raises with "Transformation failed".
        """
        raise ValueError("Transformation failed")


class TestProcessingMetrics:
    """Test ProcessingMetrics functionality."""

    def test_initialization(self) -> None:
        """Test metrics initialization."""
        metrics = ProcessingMetrics()
        assert metrics.total_processed == 0
        assert metrics.validation_errors == 0
        assert metrics.transformation_errors == 0
        assert metrics.handler_errors == 0
        assert metrics.total_processing_time_seconds == 0.0

    def test_record_processing_time(self) -> None:
        """Test recording processing time."""
        metrics = ProcessingMetrics()
        metrics.record_processing_time(0.1)
        metrics.record_processing_time(0.2)

        assert metrics.total_processed == 2
        assert abs(metrics.total_processing_time_seconds - 0.3) < 1e-10

    def test_record_errors(self) -> None:
        """Test recording different error types."""
        metrics = ProcessingMetrics()
        metrics.record_validation_error()
        metrics.record_transformation_error()
        metrics.record_handler_error()

        assert metrics.validation_errors == 1
        assert metrics.transformation_errors == 1
        assert metrics.handler_errors == 1

    def test_get_metrics_methods(self) -> None:
        """Test metrics calculation methods."""
        metrics = ProcessingMetrics()
        metrics.record_processing_time(0.1)
        metrics.record_validation_error()

        assert metrics.get_total_errors() == 1
        assert metrics.get_error_rate() == 1.0  # 1 error / 1 processed
        assert metrics.get_average_processing_time_ms() == 100.0
        assert metrics.get_uptime_seconds() > 0
        assert metrics.get_messages_per_second() > 0

    def test_reset_metrics(self) -> None:
        """Test resetting metrics."""
        metrics = ProcessingMetrics()
        metrics.record_processing_time(0.1)
        metrics.record_validation_error()

        reset_metrics = metrics.reset()
        assert reset_metrics.total_processed == 0
        assert reset_metrics.validation_errors == 0
        assert reset_metrics.total_processing_time_seconds == 0.0


class TestPydanticWebSocketProcessorWithTypedErrors:
    """Test PydanticWebSocketProcessor with typed error system."""

    @pytest.fixture
    def error_handler(self) -> AsyncMock:
        """Create mock error handler.

        Returns:
            AsyncMock: Mocked BaseErrorHandler for testing.
        """
        return AsyncMock(spec=BaseErrorHandler)

    @pytest.fixture
    def stream_error_handler(self) -> AsyncMock:
        """Create mock stream error handler.

        Returns:
            AsyncMock: Mocked WebSocketStreamErrorHandler for testing.
        """
        return AsyncMock(spec=WebSocketStreamErrorHandler)

    @pytest.fixture
    def transformer(self) -> TestTransformer:
        """Create test transformer.

        Returns:
            TestTransformer: Test transformer instance.
        """
        return TestTransformer()

    @pytest.fixture
    def processor(
        self,
        error_handler: AsyncMock,
        stream_error_handler: AsyncMock,
        transformer: TestTransformer,
    ) -> PydanticWebSocketProcessor[MessageModel, DomainModel]:
        """Create processor for testing with typed error system.

        Returns:
            PydanticWebSocketProcessor: Configured processor with test dependencies.
        """
        return PydanticWebSocketProcessor(
            raw_model=MessageModel,
            transformer=transformer,
            error_handler=error_handler,
            processor_name="test_processor",
            stream_error_handler=stream_error_handler,
        )

    @pytest.mark.asyncio
    async def test_successful_processing_with_typed_errors(
        self,
        processor: PydanticWebSocketProcessor[MessageModel, DomainModel],
        error_handler: AsyncMock,
        stream_error_handler: AsyncMock,
    ) -> None:
        """Test successful message processing with typed error system."""
        # Setup
        handler = AsyncMock()
        payload = {"id": "test-123", "value": 42, "data": {"key": "value"}}
        # Create a proper envelope
        envelope = BackpackRawWebSocketEnvelope(stream="ticker.BTC_USDC", data={"test": "data"})
        context = BackpackMessageContext(
            validated_envelope=envelope,
            exchange_type=ExchangeType.BACKPACK,
            routing_key="test",
            timestamp=datetime.now(UTC),
            message_id="test-msg-123",
            connection_id="test-conn-1",
            symbol="BTC_USDC",
        )

        # Execute
        await processor.process(payload, handler, cast(WebSocketContextProtocol, context))

        # Verify
        handler.assert_called_once()
        args, _ = handler.call_args
        typed_context = args[0]

        # Check transformed data is in context.domain_model
        assert hasattr(typed_context, "domain_model")
        processed_data = typed_context.domain_model
        assert processed_data.message_id == "test-123"
        assert processed_data.processed_value == 84  # 42 * 2
        assert processed_data.metadata == {"key": "value"}
        assert isinstance(typed_context, BackpackMessageContext)
        assert typed_context.routing_key == "test"

        # Check metrics
        assert processor.metrics.total_processed == 1
        assert processor.metrics.validation_errors == 0
        assert processor.metrics.transformation_errors == 0
        assert processor.metrics.handler_errors == 0

        # Neither error handler should be called
        error_handler.handle_validation_error.assert_not_called()
        error_handler.handle_processing_error.assert_not_called()
        stream_error_handler.handle_validation_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_validation_error_with_typed_handler(
        self,
        processor: PydanticWebSocketProcessor[MessageModel, DomainModel],
        error_handler: AsyncMock,
        stream_error_handler: AsyncMock,
    ) -> None:
        """Test handling of validation errors with typed error handler."""
        # Setup
        handler = AsyncMock()
        invalid_payload: dict[str, Any] = {
            "id": "test",
            "value": "not-an-int",
            "data": {},
        }  # Invalid value type

        # Create a mock envelope to avoid circular reference
        envelope = MagicMock(spec=BackpackRawWebSocketEnvelope)
        envelope.stream = "ticker.BTC_USDC"
        envelope.data = {"test": "data"}
        envelope.model_dump = MagicMock(
            return_value={"stream": "ticker.BTC_USDC", "data": {"test": "data"}}
        )

        context = BackpackMessageContext(
            validated_envelope=envelope,
            exchange_type=ExchangeType.BACKPACK,
            routing_key="test",
            timestamp=datetime.now(UTC),
            message_id="test-msg-123",
            connection_id="test-conn-1",
            symbol="BTC_USDC",
        )

        # Execute
        await processor.process(invalid_payload, handler, cast(WebSocketContextProtocol, context))

        # Verify
        handler.assert_not_called()

        # Since error bridge is initialized in processor, it should use typed handler
        # The bridge will call the stream error handler's handle_validation_error
        # Note: We can't directly assert on bridge calls since it's created internally
        # But we can check metrics
        assert processor.metrics.validation_errors == 1
        assert processor.metrics.total_processed == 0

    @pytest.mark.asyncio
    async def test_transformation_error_with_typed_handler(
        self,
        error_handler: AsyncMock,
        stream_error_handler: AsyncMock,
    ) -> None:
        """Test handling of transformation errors with typed error handler."""
        # Setup processor with failing transformer
        processor: PydanticWebSocketProcessor[MessageModel, Any] = PydanticWebSocketProcessor(
            raw_model=MessageModel,
            transformer=FailingTransformer(),
            error_handler=error_handler,
            stream_error_handler=stream_error_handler,
        )

        handler = AsyncMock()
        payload: dict[str, Any] = {"id": "test-123", "value": 42, "data": {}}
        # Create a proper envelope
        envelope = BackpackRawWebSocketEnvelope(stream="ticker.BTC_USDC", data={"test": "data"})
        context = BackpackMessageContext(
            validated_envelope=envelope,
            exchange_type=ExchangeType.BACKPACK,
            routing_key="test",
            timestamp=datetime.now(UTC),
            message_id="test-msg-123",
            connection_id="test-conn-1",
            symbol="BTC_USDC",
        )

        # Execute
        await processor.process(payload, handler, cast(WebSocketContextProtocol, context))

        # Verify
        handler.assert_not_called()

        # Check metrics
        assert processor.metrics.transformation_errors == 1
        assert processor.metrics.total_processed == 0

    @pytest.mark.asyncio
    async def test_handler_error_with_typed_system(
        self,
        processor: PydanticWebSocketProcessor[MessageModel, DomainModel],
        error_handler: AsyncMock,
        stream_error_handler: AsyncMock,
    ) -> None:
        """Test handling of handler errors with typed error system."""
        # Setup
        handler = AsyncMock()
        handler.side_effect = RuntimeError("Handler failed")
        payload: dict[str, Any] = {"id": "test-123", "value": 42, "data": {}}
        # Create a proper envelope
        envelope = BackpackRawWebSocketEnvelope(stream="ticker.BTC_USDC", data={"test": "data"})
        context = BackpackMessageContext(
            validated_envelope=envelope,
            exchange_type=ExchangeType.BACKPACK,
            routing_key="test",
            timestamp=datetime.now(UTC),
            message_id="test-msg-123",
            connection_id="test-conn-1",
            symbol="BTC_USDC",
        )

        # Execute
        await processor.process(payload, handler, cast(WebSocketContextProtocol, context))

        # Verify
        handler.assert_called_once()

        # Check metrics
        assert processor.metrics.handler_errors == 1
        assert processor.metrics.total_processed == 0

    def test_get_typed_metrics(
        self,
        processor: PydanticWebSocketProcessor[MessageModel, DomainModel],
    ) -> None:
        """Test getting processor metrics as typed ProcessorMetrics."""
        processor.metrics.record_processing_time(0.1)
        metrics = processor.get_metrics()

        # Should return ProcessorMetrics now
        assert isinstance(metrics, ProcessorMetrics)
        assert metrics.processor_name == "test_processor"
        assert metrics.raw_model_name == "MessageModel"
        assert metrics.transformer_type == "TestTransformer"
        assert isinstance(metrics.processing_metrics, ProcessingMetrics)
        assert metrics.processing_metrics.total_processed == 1

    def test_reset_typed_metrics(
        self,
        processor: PydanticWebSocketProcessor[MessageModel, DomainModel],
    ) -> None:
        """Test resetting processor metrics with typed system."""
        processor.metrics.record_processing_time(0.1)
        processor.metrics.record_validation_error()

        assert processor.metrics.total_processed == 1
        assert processor.metrics.validation_errors == 1

        processor.reset_metrics()

        assert processor.metrics.total_processed == 0
        assert processor.metrics.validation_errors == 0


class TestProcessorErrorBridgeIntegration:
    """Test ProcessorErrorBridge integration with processor."""

    @pytest.mark.asyncio
    async def test_error_bridge_creation(self) -> None:
        """Test that error bridge is created when stream error handler is provided."""
        error_handler = AsyncMock(spec=BaseErrorHandler)
        stream_error_handler = AsyncMock(spec=WebSocketStreamErrorHandler)

        processor: PydanticWebSocketProcessor[MessageModel, DomainModel] = (
            PydanticWebSocketProcessor(
                raw_model=MessageModel,
                transformer=TestTransformer(),
                error_handler=error_handler,
                stream_error_handler=stream_error_handler,
            )
        )

        # Error bridge should be created
        assert processor.error_bridge is not None
        assert isinstance(processor.error_bridge, ProcessorErrorBridge)
        assert processor.error_bridge.stream_error_handler == stream_error_handler

    @pytest.mark.asyncio
    async def test_no_error_bridge_without_stream_handler(self) -> None:
        """Test that error bridge is not created without stream error handler."""
        error_handler = AsyncMock(spec=BaseErrorHandler)

        processor: PydanticWebSocketProcessor[MessageModel, DomainModel] = (
            PydanticWebSocketProcessor(
                raw_model=MessageModel,
                transformer=TestTransformer(),
                error_handler=error_handler,
                stream_error_handler=None,
            )
        )

        # Error bridge should not be created
        assert processor.error_bridge is None


class TestSimpleDictTransformer:
    """Test SimpleDictTransformer functionality."""

    def test_transform(self) -> None:
        """Test that transform returns the model as-is."""
        transformer: SimpleDictTransformer[MessageModel] = SimpleDictTransformer()
        message = MessageModel(id="test", value=42, data={})

        result = transformer.transform(message)

        assert result is message  # Should be the same object


class TestProcessorFactory:
    """Test ProcessorFactory functionality."""

    def test_create_simple_processor(self) -> None:
        """Test creating simple processor."""
        error_handler = MagicMock(spec=BaseErrorHandler)

        processor = ProcessorFactory.create_simple_processor(
            raw_model=MessageModel,
            error_handler=error_handler,
            processor_name="test_simple",
        )

        assert isinstance(processor, PydanticWebSocketProcessor)
        assert processor.raw_model == MessageModel
        assert processor.processor_name == "test_simple"
        assert isinstance(processor.transformer, SimpleDictTransformer)

    def test_create_processor(self) -> None:
        """Test creating processor with custom transformer."""
        error_handler = MagicMock(spec=BaseErrorHandler)
        transformer = TestTransformer()

        processor: PydanticWebSocketProcessor[MessageModel, Any] = (
            ProcessorFactory.create_processor(
                raw_model=MessageModel,
                transformer=transformer,
                error_handler=error_handler,
                processor_name="test_custom",
            )
        )

        assert isinstance(processor, PydanticWebSocketProcessor)
        assert processor.raw_model == MessageModel
        assert processor.processor_name == "test_custom"
        # Type checker can't verify identity due to protocol typing, but we can check type
        assert isinstance(processor.transformer, TestTransformer)

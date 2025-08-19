"""Unit tests for WebSocketMessageProcessor."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.backpack.bp_ws_context import BackpackMessageContext
from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.websocket.error_context.error_handler import (
    WebSocketErrorHandler,
)
from cyberdelta.apis.websocket.ws_message_processor import (
    ProcessorFactory,
    SimpleDictTransformer,
    WebSocketMessageProcessor,
)
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.enums import ExchangeName


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


class TestWebSocketMessageProcessor:
    """Test WebSocketMessageProcessor functionality."""

    @pytest.fixture
    def error_handler(self) -> MagicMock:
        """Create mock error handler.

        Returns:
            MagicMock: Mocked WebSocketErrorHandler for testing.
        """
        return MagicMock(spec=WebSocketErrorHandler)

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
        error_handler: MagicMock,
        transformer: TestTransformer,
    ) -> WebSocketMessageProcessor[MessageModel, DomainModel]:
        """Create processor for testing.

        Returns:
            WebSocketMessageProcessor: Configured processor with test dependencies.
        """
        return WebSocketMessageProcessor(
            raw_model=MessageModel,
            transformer=transformer,
            stream_error_handler=error_handler,
            processor_name="test_processor",
        )

    @pytest.mark.asyncio
    async def test_successful_processing(
        self,
        processor: WebSocketMessageProcessor[MessageModel, DomainModel],
        error_handler: MagicMock,
    ) -> None:
        """Test successful message processing."""
        # Setup
        handler = AsyncMock()
        payload = {"id": "test-123", "value": 42, "data": {"key": "value"}}
        envelope = BackpackRawWebSocketEnvelope(stream="ticker.BTC_USDC", data={"test": "data"})
        context = BackpackMessageContext(
            validated_envelope=envelope,
            exchange_type=ExchangeName.BACKPACK,
            routing_key="test",
            timestamp=datetime.now(UTC),
            message_id="test-msg-123",
            connection_id="test-conn-1",
            symbol="BTC_USDC",
        )

        # Execute
        await processor.process(payload, handler, cast(WebSocketContextProtocol, context))

        # Verify handler was called
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

        # Error handler should not be called
        error_handler.handle_validation_error.assert_not_called()
        error_handler.handle_processing_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_validation_error(
        self,
        processor: WebSocketMessageProcessor[MessageModel, DomainModel],
        error_handler: MagicMock,
    ) -> None:
        """Test handling of validation errors."""
        # Setup
        handler = AsyncMock()
        invalid_payload: dict[str, Any] = {
            "id": "test",
            "value": "not-an-int",
            "data": {},
        }  # Invalid value type
        envelope = MagicMock(spec=BackpackRawWebSocketEnvelope)
        envelope.stream = "ticker.BTC_USDC"
        envelope.data = {"test": "data"}
        envelope.model_dump = MagicMock(
            return_value={"stream": "ticker.BTC_USDC", "data": {"test": "data"}}
        )

        context = BackpackMessageContext(
            validated_envelope=envelope,
            exchange_type=ExchangeName.BACKPACK,
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
        error_handler.handle_validation_error.assert_called_once()

        # Check error handler was called with correct arguments
        call_args = error_handler.handle_validation_error.call_args
        assert isinstance(call_args.kwargs["error"], ValidationError)
        assert call_args.kwargs["payload"] == invalid_payload
        # Context should be converted to dict by the processor
        assert isinstance(call_args.kwargs["context"], dict)
        assert call_args.kwargs["context"]["routing_key"] == context.routing_key
        assert call_args.kwargs["context"]["message_id"] == context.message_id

        # Check metrics
        assert processor.metrics.validation_errors == 1
        assert processor.metrics.total_processed == 0

    @pytest.mark.asyncio
    async def test_transformation_error(
        self,
        error_handler: MagicMock,
    ) -> None:
        """Test handling of transformation errors."""
        # Setup processor with failing transformer
        processor: WebSocketMessageProcessor[MessageModel, Any] = WebSocketMessageProcessor(
            raw_model=MessageModel,
            transformer=FailingTransformer(),
            stream_error_handler=error_handler,
        )

        handler = AsyncMock()
        payload: dict[str, Any] = {"id": "test-123", "value": 42, "data": {}}
        envelope = BackpackRawWebSocketEnvelope(stream="ticker.BTC_USDC", data={"test": "data"})
        context = BackpackMessageContext(
            validated_envelope=envelope,
            exchange_type=ExchangeName.BACKPACK,
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
        error_handler.handle_processing_error.assert_called_once()

        # Check metrics
        assert processor.metrics.transformation_errors == 1
        assert processor.metrics.total_processed == 0

    @pytest.mark.asyncio
    async def test_handler_error(
        self,
        processor: WebSocketMessageProcessor[MessageModel, DomainModel],
        error_handler: MagicMock,
    ) -> None:
        """Test handling of handler errors."""
        # Setup
        handler = AsyncMock()
        handler.side_effect = RuntimeError("Handler failed")
        payload: dict[str, Any] = {"id": "test-123", "value": 42, "data": {}}
        envelope = BackpackRawWebSocketEnvelope(stream="ticker.BTC_USDC", data={"test": "data"})
        context = BackpackMessageContext(
            validated_envelope=envelope,
            exchange_type=ExchangeName.BACKPACK,
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
        error_handler.handle_processing_error.assert_called_once()

        # Check metrics
        assert processor.metrics.handler_errors == 1
        assert processor.metrics.total_processed == 0

    def test_get_metrics(
        self,
        processor: WebSocketMessageProcessor[MessageModel, DomainModel],
    ) -> None:
        """Test getting processor metrics."""
        processor.metrics.record_processing_time(0.1)
        metrics = processor.get_metrics()

        assert metrics.processor_name == "test_processor"
        assert metrics.raw_model_name == "MessageModel"
        assert metrics.transformer_type == "TestTransformer"
        assert metrics.processing_metrics is not None
        assert metrics.processing_metrics.total_processed == 1

    def test_reset_metrics(
        self,
        processor: WebSocketMessageProcessor[MessageModel, DomainModel],
    ) -> None:
        """Test resetting processor metrics."""
        processor.metrics.record_processing_time(0.1)
        processor.metrics.record_validation_error()

        assert processor.metrics.total_processed == 1
        assert processor.metrics.validation_errors == 1

        processor.reset_metrics()

        assert processor.metrics.total_processed == 0
        assert processor.metrics.validation_errors == 0


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
        error_handler = MagicMock(spec=WebSocketErrorHandler)

        processor = ProcessorFactory.create_simple_processor(
            raw_model=MessageModel,
            stream_error_handler=error_handler,
            processor_name="test_simple",
        )

        assert isinstance(processor, WebSocketMessageProcessor)
        assert processor.raw_model == MessageModel
        assert processor.processor_name == "test_simple"
        assert isinstance(processor.transformer, SimpleDictTransformer)

    def test_create_processor(self) -> None:
        """Test creating processor with custom transformer."""
        error_handler = MagicMock(spec=WebSocketErrorHandler)
        transformer = TestTransformer()

        processor: WebSocketMessageProcessor[MessageModel, Any] = ProcessorFactory.create_processor(
            raw_model=MessageModel,
            transformer=transformer,
            stream_error_handler=error_handler,
            processor_name="test_custom",
        )

        assert isinstance(processor, WebSocketMessageProcessor)
        assert processor.raw_model == MessageModel
        assert processor.processor_name == "test_custom"
        assert isinstance(processor.transformer, TestTransformer)

"""Tests for WebSocket Processor Error Bridge.

This module tests the ProcessorErrorBridge which provides integration between
the PydanticWebSocketProcessor and the new typed WebSocket error system.
"""

from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.websocket.ws_processing_metrics import ProcessingMetrics
from cyberdelta.apis.websocket.ws_processor_error_bridge import (
    ProcessorErrorBridge,
    ProcessorErrorBridgeFactory,
)
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler


class TestModel(BaseModel):
    """Test model for bridge testing."""

    value: str


class MockProcessor:
    """Mock processor for testing."""

    def __init__(self, processor_name: str = "TestProcessor") -> None:
        self.processor_name = processor_name
        self.raw_model = TestModel
        self.transformer = Mock()
        self.metrics = ProcessingMetrics()
        self.stream_error_handler = None

    def get_metrics(self) -> Mock:
        """Return mock metrics."""
        mock_metrics = Mock()
        mock_metrics.processing_metrics = self.metrics
        return mock_metrics


class MockContext:
    """Mock WebSocket context for testing."""

    def __init__(self) -> None:
        self.connection_id = "test_connection"
        self.exchange_name = "test_exchange"
        self.routing_key = "test_routing"
        self.channel = "test_channel"
        self.sequence_number = 123


class TestProcessorErrorBridge:
    """Test the ProcessorErrorBridge class."""

    def test_initialization(self) -> None:
        """Test bridge initialization."""
        processor = MockProcessor()
        stream_error_handler = AsyncMock(spec=WebSocketStreamErrorHandler)

        bridge = ProcessorErrorBridge(
            processor=processor,
            stream_error_handler=stream_error_handler,
        )

        assert bridge.processor == processor
        assert bridge.stream_error_handler == stream_error_handler
        assert bridge.logger is not None

    @pytest.mark.asyncio
    async def test_handle_validation_error_valid_payload(self) -> None:
        """Test handling validation error with valid payload construction."""
        processor = MockProcessor()
        stream_error_handler = AsyncMock(spec=WebSocketStreamErrorHandler)
        context = MockContext()
        bridge = ProcessorErrorBridge(processor, stream_error_handler)

        validation_error = ValidationError.from_exception_data(
            "TestModel", [{"type": "missing", "loc": ("value",), "msg": "Field required"}]
        )
        payload = {"value": "test"}

        await bridge.handle_validation_error(
            error=validation_error,
            payload=payload,
            context=context,
        )

        stream_error_handler.handle_validation_error.assert_called_once()
        call_args = stream_error_handler.handle_validation_error.call_args
        assert call_args[1]["error"] == validation_error
        assert call_args[1]["context"] == context
        assert isinstance(call_args[1]["payload"], TestModel)

    @pytest.mark.asyncio
    async def test_handle_validation_error_invalid_payload(self) -> None:
        """Test handling validation error with invalid payload construction."""
        processor = MockProcessor()
        stream_error_handler = AsyncMock(spec=WebSocketStreamErrorHandler)
        context = MockContext()
        bridge = ProcessorErrorBridge(processor, stream_error_handler)

        validation_error = ValidationError.from_exception_data(
            "TestModel", [{"type": "missing", "loc": ("value",), "msg": "Field required"}]
        )
        payload = "invalid_payload"  # This will fail model_construct

        await bridge.handle_validation_error(
            error=validation_error,
            payload=payload,
            context=context,
        )

        stream_error_handler.handle_validation_error.assert_called_once()
        call_args = stream_error_handler.handle_validation_error.call_args
        assert call_args[1]["error"] == validation_error
        assert call_args[1]["context"] == context
        # Should create minimal model when construction fails
        assert isinstance(call_args[1]["payload"], TestModel)

    @pytest.mark.asyncio
    async def test_handle_transformation_error(self) -> None:
        """Test handling transformation error."""
        processor = MockProcessor()
        stream_error_handler = AsyncMock(spec=WebSocketStreamErrorHandler)
        context = MockContext()
        bridge = ProcessorErrorBridge(processor, stream_error_handler)

        transformation_error = ValueError("Transform failed")
        validated_payload = TestModel(value="test")

        await bridge.handle_transformation_error(
            error=transformation_error,
            validated_payload=validated_payload,
            context=context,
        )

        stream_error_handler.handle_stream_error.assert_called_once()
        call_args = stream_error_handler.handle_stream_error.call_args[0][0]
        assert "Transformation failed" in str(call_args)

    @pytest.mark.asyncio
    async def test_handle_handler_error_expected(self) -> None:
        """Test handling expected handler error."""
        processor = MockProcessor()
        stream_error_handler = AsyncMock(spec=WebSocketStreamErrorHandler)
        context = MockContext()
        bridge = ProcessorErrorBridge(processor, stream_error_handler)

        handler_error = KeyError("Key not found")
        domain_model = TestModel(value="test")

        await bridge.handle_handler_error(
            error=handler_error,
            domain_model=domain_model,
            context=context,
            is_unexpected=False,
        )

        stream_error_handler.handle_stream_error.assert_called_once()
        call_args = stream_error_handler.handle_stream_error.call_args[0][0]
        assert "Handler invocation failed" in str(call_args)

    @pytest.mark.asyncio
    async def test_handle_handler_error_unexpected(self) -> None:
        """Test handling unexpected handler error."""
        processor = MockProcessor()
        stream_error_handler = AsyncMock(spec=WebSocketStreamErrorHandler)
        context = MockContext()
        bridge = ProcessorErrorBridge(processor, stream_error_handler)

        handler_error = RuntimeError("Unexpected error")
        domain_model = [TestModel(value="test1"), TestModel(value="test2")]

        await bridge.handle_handler_error(
            error=handler_error,
            domain_model=domain_model,
            context=context,
            is_unexpected=True,
        )

        stream_error_handler.handle_stream_error.assert_called_once()
        call_args = stream_error_handler.handle_stream_error.call_args[0][0]
        assert "Unexpected handler error" in str(call_args)

    @pytest.mark.asyncio
    async def test_handle_unexpected_error(self) -> None:
        """Test handling unexpected processor error."""
        processor = MockProcessor()
        stream_error_handler = AsyncMock(spec=WebSocketStreamErrorHandler)
        context = MockContext()
        bridge = ProcessorErrorBridge(processor, stream_error_handler)

        unexpected_error = Exception("Something went wrong")
        stage = "processing_pipeline"

        await bridge.handle_unexpected_error(
            error=unexpected_error,
            context=context,
            stage=stage,
        )

        stream_error_handler.handle_stream_error.assert_called_once()
        call_args = stream_error_handler.handle_stream_error.call_args[0][0]
        assert "Unexpected processing error in processing_pipeline" in str(call_args)

    def test_get_processor_stats(self) -> None:
        """Test getting processor statistics."""
        processor = MockProcessor()
        processor.metrics.record_processing_time(0.05)
        processor.metrics.record_validation_error()

        stream_error_handler = AsyncMock(spec=WebSocketStreamErrorHandler)
        bridge = ProcessorErrorBridge(processor, stream_error_handler)

        stats = bridge.get_processor_stats()

        assert stats["total_processed"] == 1
        assert stats["total_errors"] == 1
        assert stats["error_rate"] == 1.0  # 1 error / 1 processed
        assert "uptime_seconds" in stats

    def test_should_use_typed_handler(self) -> None:
        """Test checking if typed handler should be used."""
        processor = MockProcessor()
        stream_error_handler = AsyncMock(spec=WebSocketStreamErrorHandler)
        bridge = ProcessorErrorBridge(processor, stream_error_handler)

        assert bridge.should_use_typed_handler() is True

    @pytest.mark.asyncio
    async def test_create_enhanced_error_context(self) -> None:
        """Test creating enhanced error context."""
        processor = MockProcessor()
        processor.metrics.record_processing_time(0.05)

        stream_error_handler = AsyncMock(spec=WebSocketStreamErrorHandler)
        bridge = ProcessorErrorBridge(processor, stream_error_handler)

        context = MockContext()
        error = ValueError("Test error")
        stage = "validation"

        enhanced_context = await bridge.create_enhanced_error_context(
            base_context=context,
            error=error,
            stage=stage,
        )

        assert enhanced_context["connection_id"] == "test_connection"
        assert enhanced_context["exchange"] == "test_exchange"
        assert enhanced_context["processor_name"] == "TestProcessor"
        assert enhanced_context["error_type"] == "ValueError"
        assert enhanced_context["stage"] == "validation"
        assert "processor_stats" in enhanced_context


class TestProcessorErrorBridgeFactory:
    """Test the ProcessorErrorBridgeFactory class."""

    def test_create_bridge_with_handler(self) -> None:
        """Test creating bridge with stream error handler."""
        processor = MockProcessor()
        stream_error_handler = AsyncMock(spec=WebSocketStreamErrorHandler)

        bridge = ProcessorErrorBridgeFactory.create_bridge(
            processor=processor,
            stream_error_handler=stream_error_handler,
        )

        assert bridge is not None
        assert isinstance(bridge, ProcessorErrorBridge)
        assert bridge.processor == processor
        assert bridge.stream_error_handler == stream_error_handler

    def test_create_bridge_without_handler(self) -> None:
        """Test creating bridge without stream error handler."""
        processor = MockProcessor()

        bridge = ProcessorErrorBridgeFactory.create_bridge(
            processor=processor,
            stream_error_handler=None,
        )

        assert bridge is None

    def test_is_bridge_available_with_handler(self) -> None:
        """Test checking bridge availability with stream error handler."""
        processor = MockProcessor()
        processor.stream_error_handler = AsyncMock()

        available = ProcessorErrorBridgeFactory.is_bridge_available(processor)

        assert available is True

    def test_is_bridge_available_without_handler(self) -> None:
        """Test checking bridge availability without stream error handler."""
        processor = MockProcessor()
        processor.stream_error_handler = None

        available = ProcessorErrorBridgeFactory.is_bridge_available(processor)

        assert available is False

    def test_is_bridge_available_no_attribute(self) -> None:
        """Test checking bridge availability when attribute doesn't exist."""
        processor = MockProcessor()
        delattr(processor, "stream_error_handler")  # Remove the attribute

        available = ProcessorErrorBridgeFactory.is_bridge_available(processor)

        assert available is False

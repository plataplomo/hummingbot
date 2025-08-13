"""Unit tests for Router Error Context Builder.

Tests the RouterErrorContextBuilder and RouterErrorMetadata for creating
typed error contexts from router state and operations.
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import Mock

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_router_error_context import (
    RouterErrorContextBuilder,
    RouterErrorMetadata,
)
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext


class TestEnvelopeModel(BaseModel):
    """Test envelope model for router error context tests."""

    stream: str
    data: dict[str, Any]


class TestRouterErrorMetadata:
    """Test RouterErrorMetadata model."""

    def test_router_error_metadata_creation(self) -> None:
        """Test basic RouterErrorMetadata creation."""
        metadata = RouterErrorMetadata(
            router_type="TestRouter",
            exchange_name="hyperliquid",
            connection_id="test-conn-123",
            error_stage="test_stage",
        )

        assert metadata.router_type == "TestRouter"
        assert metadata.exchange_name == "hyperliquid"
        assert metadata.connection_id == "test-conn-123"
        assert metadata.error_stage == "test_stage"
        assert metadata.error_timestamp_ms is not None
        assert isinstance(metadata.error_timestamp_ms, int)

    def test_router_error_metadata_with_all_fields(self) -> None:
        """Test RouterErrorMetadata with all optional fields."""
        start_time = int(time.time() * 1000)

        metadata = RouterErrorMetadata(
            router_type="TestRouter",
            exchange_name="hyperliquid",
            connection_id="test-conn-123",
            routing_key="test.route",
            available_processors=["proc1", "proc2"],
            available_handlers=["handler1", "handler2"],
            envelope_type="TestEnvelope",
            message_keys=["stream", "data"],
            message_size_bytes=256,
            error_stage="processor_lookup",
            processing_start_time_ms=start_time,
            error_timestamp_ms=start_time + 100,
        )

        assert metadata.routing_key == "test.route"
        assert metadata.available_processors == ["proc1", "proc2"]
        assert metadata.available_handlers == ["handler1", "handler2"]
        assert metadata.envelope_type == "TestEnvelope"
        assert metadata.message_keys == ["stream", "data"]
        assert metadata.message_size_bytes == 256
        assert metadata.processing_start_time_ms == start_time
        assert metadata.error_timestamp_ms == start_time + 100

    def test_router_error_metadata_frozen(self) -> None:
        """Test that RouterErrorMetadata is frozen."""
        metadata = RouterErrorMetadata(
            router_type="TestRouter",
            exchange_name="hyperliquid",
            connection_id="test-conn-123",
            error_stage="test_stage",
        )

        with pytest.raises(ValidationError):
            metadata.router_type = "NewRouter"  # Should fail - frozen model

    def test_router_error_metadata_auto_timestamp(self) -> None:
        """Test automatic timestamp setting in post_init."""
        before_time = int(time.time() * 1000)

        metadata = RouterErrorMetadata(
            router_type="TestRouter",
            exchange_name="hyperliquid",
            connection_id="test-conn-123",
            error_stage="test_stage",
        )

        after_time = int(time.time() * 1000)

        # The timestamp should be set automatically in model_post_init
        assert metadata.error_timestamp_ms is not None
        assert before_time <= metadata.error_timestamp_ms <= after_time


class TestRouterErrorContextBuilder:
    """Test RouterErrorContextBuilder functionality."""

    @pytest.fixture
    def mock_router(self) -> Mock:
        """Create mock router for testing.

        Returns:
            Mock: Mock WebSocket router with predefined test configuration.
        """
        router = Mock()
        router.__class__.__name__ = "TestWebSocketRouter"
        router.exchange_name = "hyperliquid"  # Valid exchange name
        router.get_connection_id = Mock(return_value="test-conn-12345")
        router.processors = {"route1": Mock(), "route2": Mock()}
        return router

    @pytest.fixture
    def mock_context(self) -> Mock:
        """Create mock WebSocket context.

        Returns:
            Mock: Mock WebSocket context implementing WebSocketContextProtocol.
        """
        context = Mock(spec=WebSocketContextProtocol)
        context.connection_id = "test-conn-12345"
        context.exchange_name = "hyperliquid"  # Valid exchange name
        context.channel = "test-channel"
        context.sequence_number = 123
        return context

    def test_from_envelope_validation_error(self, mock_router: Mock) -> None:
        """Test creating context from envelope validation error."""
        message = {"stream": "invalid", "bad_field": "value"}
        validation_error = ValidationError.from_exception_data(
            "ValidationError", [{"type": "missing", "loc": ("data",), "input": {}}]
        )

        context = RouterErrorContextBuilder.from_envelope_validation_error(
            router=mock_router,
            message=message,
            validation_error=validation_error,
            envelope_type="TestEnvelope",
        )

        assert isinstance(context, StreamErrorContext)
        assert context.connection_id == "test-conn-12345"
        assert context.exchange == "hyperliquid"
        assert context.channel is None  # Not available at validation stage
        assert context.topic is None  # Not available at validation stage
        assert context.extra_context["raw_message"] == message

        # Check router metadata in extra_context
        assert context.extra_context is not None
        router_metadata = context.extra_context["router_metadata"]
        assert router_metadata is not None
        assert router_metadata["router_type"] == "TestWebSocketRouter"
        assert router_metadata["error_stage"] == "envelope_validation"
        assert router_metadata["envelope_type"] == "TestEnvelope"
        assert router_metadata["message_keys"] == ["stream", "bad_field"]
        assert router_metadata["message_size_bytes"] > 0

    def test_from_missing_routing_key_error(self, mock_router: Mock) -> None:
        """Test creating context from missing routing key error."""
        message = {"stream": "test.stream", "data": {"value": 123}}
        envelope = TestEnvelopeModel(stream="test.stream", data={"value": 123})

        context = RouterErrorContextBuilder.from_missing_routing_key_error(
            router=mock_router,
            message=message,
            envelope=envelope,
        )

        assert isinstance(context, StreamErrorContext)
        assert context.connection_id == "test-conn-12345"
        assert context.exchange == "hyperliquid"
        assert context.channel is None  # Cannot determine without routing key
        assert context.topic is None  # Cannot determine without routing key
        assert context.extra_context["raw_message"] == message

        # Check router metadata in extra_context
        assert context.extra_context is not None
        router_metadata = context.extra_context["router_metadata"]
        assert router_metadata is not None
        assert router_metadata["error_stage"] == "routing_key_extraction"
        assert router_metadata["envelope_type"] == "TestEnvelopeModel"
        assert router_metadata["message_keys"] == ["stream", "data"]

    def test_from_missing_processor_error(self, mock_router: Mock, mock_context: Mock) -> None:
        """Test creating context from missing processor error."""
        routing_key = "unknown.route"
        payload = {"type": "test", "value": 42}

        context = RouterErrorContextBuilder.from_missing_processor_error(
            router=mock_router,
            routing_key=routing_key,
            payload=payload,
            context=mock_context,
        )

        assert isinstance(context, StreamErrorContext)
        assert context.connection_id == "test-conn-12345"
        assert context.exchange == "hyperliquid"
        assert context.channel == "test-channel"  # From mock_context
        assert context.topic == "unknown.route"  # Uses routing key as topic
        assert context.sequence_number == 123  # From mock_context
        assert context.extra_context["raw_message"] == payload

        # Check router metadata in extra_context
        assert context.extra_context is not None
        router_metadata = context.extra_context["router_metadata"]
        assert router_metadata is not None
        assert router_metadata["error_stage"] == "processor_lookup"
        assert router_metadata["routing_key"] == "unknown.route"
        assert router_metadata["available_processors"] == ["route1", "route2"]

    def test_from_missing_processor_error_with_list_payload(
        self, mock_router: Mock, mock_context: Mock
    ) -> None:
        """Test creating context from missing processor error with list payload."""
        routing_key = "unknown.route"
        payload = [{"item": 1}, {"item": 2}]  # List payload

        context = RouterErrorContextBuilder.from_missing_processor_error(
            router=mock_router,
            routing_key=routing_key,
            payload=payload,
            context=mock_context,
        )

        assert isinstance(context, StreamErrorContext)
        assert context.extra_context["raw_message"] == {"data": payload}  # List wrapped in dict

        # Check router metadata in extra_context
        assert context.extra_context is not None
        router_metadata = context.extra_context["router_metadata"]
        assert router_metadata is not None
        assert router_metadata["message_size_bytes"] > 0

    def test_from_missing_handler_error(self, mock_router: Mock) -> None:
        """Test creating context from missing handler error."""
        routing_key = "test.route"
        message = {"stream": "test.stream", "data": {"value": 123}}
        available_handlers = ["handler1", "handler2", "handler3"]

        context = RouterErrorContextBuilder.from_missing_handler_error(
            router=mock_router,
            routing_key=routing_key,
            message=message,
            available_handlers=available_handlers,
        )

        assert isinstance(context, StreamErrorContext)
        assert context.connection_id == "test-conn-12345"
        assert context.exchange == "hyperliquid"
        assert context.channel is None  # Not determined at handler lookup
        assert context.topic == "test.route"
        assert context.extra_context["raw_message"] == message

        # Check router metadata in extra_context
        assert context.extra_context is not None
        router_metadata = context.extra_context["router_metadata"]
        assert router_metadata is not None
        assert router_metadata["error_stage"] == "handler_lookup"
        assert router_metadata["routing_key"] == "test.route"
        assert router_metadata["available_handlers"] == available_handlers

    def test_from_routing_error(self, mock_router: Mock) -> None:
        """Test creating context from general routing error."""
        error = ValueError("Test routing error")
        message = {"stream": "test.stream", "data": {"value": 123}}
        routing_stage = "custom_routing_stage"

        context = RouterErrorContextBuilder.from_routing_error(
            router=mock_router,
            error=error,
            message=message,
            routing_stage=routing_stage,
        )

        assert isinstance(context, StreamErrorContext)
        assert context.connection_id == "test-conn-12345"
        assert context.exchange == "hyperliquid"
        assert context.channel is None  # Not available for general errors
        assert context.topic is None  # Not available for general errors
        assert context.extra_context["raw_message"] == message

        # Check router metadata in extra_context
        assert context.extra_context is not None
        router_metadata = context.extra_context["router_metadata"]
        assert router_metadata is not None
        assert router_metadata["error_stage"] == "custom_routing_stage"
        assert router_metadata["message_keys"] == ["stream", "data"]

    def test_enhance_context_with_timing(self) -> None:
        """Test enhancing existing context with timing information."""
        # Create base context
        from cyberdelta.apis.common.error_foundation import ErrorMetadata

        base_metadata = ErrorMetadata()
        original_context = StreamErrorContext(
            connection_id="test-conn-123",
            exchange="hyperliquid",
            error_timestamp_ms=int(time.time() * 1000),
            metadata=base_metadata,
            extra_context={"original": "data"},
        )

        processing_start = original_context.error_timestamp_ms - 100

        enhanced_context = RouterErrorContextBuilder.enhance_context_with_timing(
            context=original_context,
            processing_start_time_ms=processing_start,
        )

        assert enhanced_context.connection_id == original_context.connection_id
        assert enhanced_context.exchange == original_context.exchange
        assert enhanced_context.error_timestamp_ms == original_context.error_timestamp_ms

        # Check enhanced extra_context (timing information added)
        assert enhanced_context.extra_context is not None
        assert enhanced_context.extra_context["processing_start_time_ms"] == processing_start
        assert enhanced_context.extra_context["processing_duration_ms"] == 100
        assert enhanced_context.extra_context["original"] == "data"  # Original data preserved

    def test_enhance_context_with_timing_no_extra_context(self) -> None:
        """Test enhancing context that has no existing extra_context."""
        original_context = StreamErrorContext(
            connection_id="test-conn-123",
            exchange="hyperliquid",
            error_timestamp_ms=int(time.time() * 1000),
            extra_context={},
        )

        enhanced_context = RouterErrorContextBuilder.enhance_context_with_timing(
            context=original_context,
            processing_start_time_ms=12345,
        )

        # Should return enhanced context with timing information
        assert enhanced_context.extra_context["processing_start_time_ms"] == 12345

    def test_create_recovery_context(self, mock_router: Mock) -> None:
        """Test creating recovery context from original error context."""
        original_context = StreamErrorContext(
            connection_id="test-conn-123",
            exchange="hyperliquid",
            channel="test_channel",
            topic="test.topic",
            sequence_number=456,
            error_timestamp_ms=int(time.time() * 1000),
            extra_context={
                "raw_message": {"original": "message"},
                "routing_key": "original.route",
                "error_timestamp_ms": int(time.time() * 1000) - 1000,
            },
        )

        # Small delay to ensure different timestamps
        time.sleep(0.001)

        recovery_context = RouterErrorContextBuilder.create_recovery_context(
            router=mock_router,
            original_context=original_context,
            recovery_stage="reconnection",
        )

        assert isinstance(recovery_context, StreamErrorContext)
        # Should preserve original context fields
        assert recovery_context.connection_id == "test-conn-123"
        assert recovery_context.exchange == "hyperliquid"
        assert recovery_context.channel == "test_channel"
        assert recovery_context.topic == "test.topic"
        assert recovery_context.sequence_number == 456
        assert recovery_context.extra_context["raw_message"] == {"original": "message"}

        # Should have new recovery metadata in extra_context
        assert recovery_context.extra_context is not None
        router_metadata = recovery_context.extra_context["router_metadata"]
        assert router_metadata["router_type"] == "TestWebSocketRouter"
        assert router_metadata["error_stage"] == "recovery_reconnection"
        assert router_metadata["routing_key"] == "original.route"

        # Should have new error timestamp (not same as original)
        assert recovery_context.error_timestamp_ms != original_context.error_timestamp_ms

    def test_context_builder_with_non_dict_message(self, mock_router: Mock) -> None:
        """Test context builder handles non-dict messages gracefully."""
        # Test with dict message (as required by the method signature)
        dict_message = {"raw_message": "not a dict", "type": "invalid"}
        error = ValueError("Test error")

        context = RouterErrorContextBuilder.from_routing_error(
            router=mock_router,
            error=error,
            message=dict_message,
        )

        # Should handle gracefully and set message_keys to None
        assert context.extra_context is not None
        router_metadata = context.extra_context["router_metadata"]
        assert router_metadata["message_keys"] is None
        assert context.extra_context["raw_message"] == dict_message

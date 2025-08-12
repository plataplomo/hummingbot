"""Integration tests for WebSocket Processor with typed error handling.

This module provides comprehensive integration tests for the PydanticWebSocketProcessor
working with the new typed WebSocket error system in real scenarios.
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel

from cyberdelta.apis.websocket.ws_error_handler_factory import WebSocketErrorHandlerFactory

# Mock used for metrics collector in integration tests
from cyberdelta.apis.websocket.ws_processor import (
    MessageTransformer,
    PydanticWebSocketProcessor,
)
from cyberdelta.apis.websocket.ws_processor_error_bridge import ProcessorErrorBridge
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol

# Recovery system integration will be tested in separate recovery integration tests
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig


class TestIntegrationMessage(BaseModel):
    """Test message model for integration tests."""

    message_id: str
    data: dict[str, Any]
    timestamp: float


class TestIntegrationDomainModel(BaseModel):
    """Test domain model for integration tests."""

    processed_id: str
    processed_data: dict[str, Any]
    processing_timestamp: float


class TestIntegrationTransformer(
    MessageTransformer[TestIntegrationMessage, TestIntegrationDomainModel]
):
    """Test transformer for integration tests."""

    def transform(
        self, validated: TestIntegrationMessage, context: WebSocketContextProtocol | None = None
    ) -> TestIntegrationDomainModel:
        return TestIntegrationDomainModel(
            processed_id=f"processed_{validated.message_id}",
            processed_data=validated.data,
            processing_timestamp=time.time(),
        )


class TestProcessorErrorIntegration:
    """Test processor integration with complete error handling stack."""

    @pytest.fixture
    def error_config(self) -> WebSocketErrorConfig:
        """Create error configuration."""
        return WebSocketErrorConfig(
            max_retries=3,
            retry_delay_base_ms=100,
            circuit_breaker_threshold=5,
            circuit_breaker_timeout_ms=30000,
            enable_detailed_logging=True,
        )

    # Recovery configuration removed - will be tested in recovery integration tests

    @pytest.fixture
    def metrics_collector(self) -> Mock:
        """Create mock metrics collector."""
        mock_collector = Mock()
        mock_collector.record_error = Mock()
        mock_collector.get_summary = Mock(
            return_value=Mock(
                total_errors=0, validation_errors=0, handler_errors=0, transformation_errors=0
            )
        )
        mock_collector.get_statistics = Mock(
            return_value={"total_errors_recorded": 0, "uptime_seconds": 0}
        )
        return mock_collector

    # Recovery system removed - will be tested in recovery integration tests

    @pytest.fixture
    def stream_error_handler(
        self,
        error_config: WebSocketErrorConfig,
        metrics_collector: Mock,
    ) -> WebSocketStreamErrorHandler:
        """Create stream error handler."""
        factory = WebSocketErrorHandlerFactory()
        return factory.create_handler(
            exchange="hyperliquid",
            config=error_config,
            # recovery_system=None,  # Will be tested in recovery integration tests
            # metrics_collector=metrics_collector,  # Simplified for processor focus
        )

    @pytest.fixture
    def test_context(self) -> Mock:
        """Create test WebSocket context."""
        context = Mock(spec=WebSocketContextProtocol)
        context.connection_id = "integration-test-conn-12345"
        context.exchange_name = "hyperliquid"
        context.exchange_type = "hyperliquid"
        context.routing_key = "integration.test"
        context.channel = "integration_channel"
        context.sequence_number = 98765
        context.domain_model = None

        # Mock context creation for error handling
        context.create_error_context.return_value = StreamErrorContext(
            connection_id="integration-test-conn-12345",
            exchange="hyperliquid",
            channel="integration_channel",
            topic="integration.test",
            sequence_number=98765,
            error_timestamp_ms=int(time.time() * 1000),
        )

        return context

    @pytest.fixture
    def legacy_error_handler(self) -> AsyncMock:
        """Create legacy error handler."""
        handler = AsyncMock()
        handler.handle_validation_error = AsyncMock()
        handler.handle_processing_error = AsyncMock()
        handler.handle_connection_error = AsyncMock()
        return handler

    def test_processor_integration_initialization(
        self,
        stream_error_handler: WebSocketStreamErrorHandler,
        legacy_error_handler: AsyncMock,
        metrics_collector: Mock,
    ) -> None:
        """Test processor initialization with full error system integration."""
        processor = PydanticWebSocketProcessor(
            raw_model=TestIntegrationMessage,
            transformer=TestIntegrationTransformer(),
            error_handler=legacy_error_handler,
            stream_error_handler=stream_error_handler,
            processor_name="IntegrationTestProcessor",
            metrics_collector=metrics_collector,
        )

        # Verify processor is properly configured
        assert processor.processor_name == "IntegrationTestProcessor"
        assert processor.raw_model == TestIntegrationMessage
        assert isinstance(processor.transformer, TestIntegrationTransformer)
        assert processor.stream_error_handler is stream_error_handler
        assert processor.metrics_collector is metrics_collector

        # Verify error bridge was created
        assert hasattr(processor, "error_bridge")
        assert processor.error_bridge is not None
        assert isinstance(processor.error_bridge, ProcessorErrorBridge)
        assert processor.error_bridge.stream_error_handler is stream_error_handler

    @pytest.mark.asyncio
    async def test_successful_message_processing_integration(
        self,
        stream_error_handler: WebSocketStreamErrorHandler,
        legacy_error_handler: AsyncMock,
        metrics_collector: Mock,
        test_context: Mock,
    ) -> None:
        """Test complete successful message processing flow."""
        processor = PydanticWebSocketProcessor(
            raw_model=TestIntegrationMessage,
            transformer=TestIntegrationTransformer(),
            error_handler=legacy_error_handler,
            stream_error_handler=stream_error_handler,
            processor_name="SuccessIntegrationProcessor",
            metrics_collector=metrics_collector,
        )

        message_handler = AsyncMock()

        # Valid test payload
        payload = {
            "message_id": "integration_test_001",
            "data": {"test": "integration", "value": 42},
            "timestamp": time.time(),
        }

        # Process message
        await processor.process(payload, message_handler, test_context)

        # Verify handler was called
        message_handler.assert_called_once()
        call_args = message_handler.call_args
        context_arg = call_args.args[0]
        assert context_arg is test_context

        # Verify domain model was set on context
        assert hasattr(test_context, "domain_model")
        domain_model = test_context.domain_model
        assert isinstance(domain_model, TestIntegrationDomainModel)
        assert domain_model.processed_id == "processed_integration_test_001"
        assert domain_model.processed_data == {"test": "integration", "value": 42}

        # Verify metrics were updated
        processing_metrics = processor.get_metrics()
        assert processing_metrics["metrics"]["total_processed"] == 1
        assert processing_metrics["metrics"]["validation_errors"] == 0
        assert processing_metrics["metrics"]["transformation_errors"] == 0
        assert processing_metrics["metrics"]["handler_errors"] == 0

        # Verify no error handlers were called
        legacy_error_handler.handle_validation_error.assert_not_called()
        legacy_error_handler.handle_processing_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_validation_error_recovery_integration(
        self,
        stream_error_handler: WebSocketStreamErrorHandler,
        legacy_error_handler: AsyncMock,
        metrics_collector: Mock,
        test_context: Mock,
    ) -> None:
        """Test validation error handling with recovery system integration."""
        processor = PydanticWebSocketProcessor(
            raw_model=TestIntegrationMessage,
            transformer=TestIntegrationTransformer(),
            error_handler=legacy_error_handler,
            stream_error_handler=stream_error_handler,
            processor_name="ValidationErrorIntegrationProcessor",
            metrics_collector=metrics_collector,
        )

        message_handler = AsyncMock()

        # Invalid test payload (missing required fields)
        invalid_payload = {
            "message_id": "integration_test_002",
            # Missing data and timestamp fields
            "invalid_field": "should_cause_validation_error",
        }

        # Process invalid message
        await processor.process(invalid_payload, message_handler, test_context)

        # Verify handler was not called
        message_handler.assert_not_called()

        # Verify metrics were updated
        assert processor.metrics.validation_errors == 1
        assert processor.metrics.total_processed == 0

        # Verify metrics collector recorded the error
        summary = metrics_collector.get_summary()
        assert summary.total_errors > 0
        assert summary.validation_errors > 0

        # Verify legacy error handler was not called (typed system handles it)
        legacy_error_handler.handle_validation_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_handler_error_recovery_integration(
        self,
        stream_error_handler: WebSocketStreamErrorHandler,
        legacy_error_handler: AsyncMock,
        metrics_collector: Mock,
        test_context: Mock,
    ) -> None:
        """Test handler error with recovery system integration."""
        processor = PydanticWebSocketProcessor(
            raw_model=TestIntegrationMessage,
            transformer=TestIntegrationTransformer(),
            error_handler=legacy_error_handler,
            stream_error_handler=stream_error_handler,
            processor_name="HandlerErrorIntegrationProcessor",
            metrics_collector=metrics_collector,
        )

        # Handler that raises an expected error
        failing_handler = AsyncMock(side_effect=ValueError("Handler integration test error"))

        # Valid test payload
        payload = {
            "message_id": "integration_test_003",
            "data": {"test": "handler_error"},
            "timestamp": time.time(),
        }

        # Process message with failing handler
        await processor.process(payload, failing_handler, test_context)

        # Verify handler was called but failed
        failing_handler.assert_called_once()

        # Verify domain model was set before handler failure
        assert hasattr(test_context, "domain_model")
        domain_model = test_context.domain_model
        assert isinstance(domain_model, TestIntegrationDomainModel)

        # Verify metrics were updated
        assert processor.metrics.handler_errors == 1
        assert processor.metrics.total_processed == 0  # Not counted as successful

        # Verify metrics collector recorded the error
        summary = metrics_collector.get_summary()
        assert summary.total_errors > 0
        assert summary.handler_errors > 0

        # Verify legacy error handler was not called
        legacy_error_handler.handle_processing_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_metrics_integration_across_operations(
        self,
        stream_error_handler: WebSocketStreamErrorHandler,
        legacy_error_handler: AsyncMock,
        metrics_collector: Mock,
        test_context: Mock,
    ) -> None:
        """Test metrics integration across multiple operations."""
        processor = PydanticWebSocketProcessor(
            raw_model=TestIntegrationMessage,
            transformer=TestIntegrationTransformer(),
            error_handler=legacy_error_handler,
            stream_error_handler=stream_error_handler,
            processor_name="MetricsIntegrationProcessor",
            metrics_collector=metrics_collector,
        )

        success_handler = AsyncMock()
        failing_handler = AsyncMock(side_effect=KeyError("Integration test error"))

        # Test successful processing
        success_payload = {
            "message_id": "integration_success",
            "data": {"result": "success"},
            "timestamp": time.time(),
        }
        await processor.process(success_payload, success_handler, test_context)

        # Test validation error
        invalid_payload = {"message_id": "invalid"}
        await processor.process(invalid_payload, success_handler, test_context)

        # Test handler error
        error_payload = {
            "message_id": "integration_error",
            "data": {"result": "error"},
            "timestamp": time.time(),
        }
        await processor.process(error_payload, failing_handler, test_context)

        # Verify processor metrics
        processor_metrics = processor.get_metrics()["metrics"]
        assert processor_metrics["total_processed"] == 1  # Only success counts
        assert processor_metrics["validation_errors"] == 1
        assert processor_metrics["handler_errors"] == 1
        assert processor_metrics["transformation_errors"] == 0

        # Verify metrics collector aggregation
        summary = metrics_collector.get_summary()
        assert summary.total_errors >= 2  # At least validation + handler errors
        assert summary.validation_errors >= 1
        assert summary.handler_errors >= 1

        # Verify error rate calculations
        assert processor.metrics.get_error_rate() > 0
        assert 0 < processor.metrics.get_error_rate() <= 1.0

    # Recovery system integration test removed - will be implemented in recovery-specific tests

    @pytest.mark.asyncio
    async def test_error_bridge_integration_completeness(
        self,
        stream_error_handler: WebSocketStreamErrorHandler,
        legacy_error_handler: AsyncMock,
        metrics_collector: Mock,
        test_context: Mock,
    ) -> None:
        """Test that error bridge provides complete integration between systems."""
        processor = PydanticWebSocketProcessor(
            raw_model=TestIntegrationMessage,
            transformer=TestIntegrationTransformer(),
            error_handler=legacy_error_handler,
            stream_error_handler=stream_error_handler,
            processor_name="ErrorBridgeIntegrationProcessor",
            metrics_collector=metrics_collector,
        )

        # Verify error bridge has all required components
        error_bridge = processor.error_bridge
        assert error_bridge is not None
        assert error_bridge.processor is processor
        assert error_bridge.stream_error_handler is stream_error_handler

        # Test that error bridge can create enhanced context
        test_error = ValueError("Bridge integration test")
        enhanced_context = await error_bridge.create_enhanced_error_context(
            base_context=test_context, error=test_error, stage="integration_test"
        )

        # Verify enhanced context has all required fields
        assert enhanced_context["connection_id"] == "integration-test-conn-12345"
        assert enhanced_context["exchange"] == "test_exchange"
        assert enhanced_context["processor_name"] == "ErrorBridgeIntegrationProcessor"
        assert enhanced_context["error_type"] == "ValueError"
        assert enhanced_context["stage"] == "integration_test"
        assert "processor_stats" in enhanced_context
        assert "timestamp_ms" in enhanced_context

        # Test that bridge correctly reports availability
        assert error_bridge.should_use_typed_handler() is True

        # Test processor stats integration
        processor_stats = error_bridge.get_processor_stats()
        assert "total_processed" in processor_stats
        assert "total_errors" in processor_stats
        assert "error_rate" in processor_stats
        assert "uptime_seconds" in processor_stats

"""Tests for ProcessorErrorContextBuilder.

This test module validates the ProcessorErrorContextBuilder's ability to create
typed StreamErrorContext objects from processor state, eliminating dict conversions.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import Mock

import pytest
from pydantic import BaseModel, Field, ValidationError

from cyberdelta.apis.websocket.ws_processor import PydanticWebSocketProcessor
from cyberdelta.apis.websocket.ws_processor_error_context import (
    ProcessorErrorContextBuilder,
    ProcessorErrorMetadata,
)
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.enums import ExchangeName


class MockRawModel(BaseModel):
    """Mock raw model for testing."""

    price: Decimal = Field(...)
    symbol: str = Field(...)


class MockTransformer:
    """Mock transformer for testing."""

    def transform(
        self,
        validated: MockRawModel,
        context: WebSocketContextProtocol | None = None,
    ) -> MockRawModel | None:
        """Mock transform method.

        Returns:
            MockRawModel | None: The validated raw model or None.
        """
        return validated


def create_mock_processor() -> PydanticWebSocketProcessor[MockRawModel, MockRawModel]:
    """Create a mock processor for testing.

    Returns:
        PydanticWebSocketProcessor[MockRawModel, MockRawModel]: Mock processor.
    """
    # We need a mock stream error handler that implements the required interface
    mock_error_handler = Mock(spec=WebSocketStreamErrorHandler)

    return PydanticWebSocketProcessor(
        raw_model=MockRawModel,
        transformer=MockTransformer(),
        stream_error_handler=mock_error_handler,
        processor_name="TestProcessor",
    )


class MockContext:
    """Mock WebSocket context for testing."""

    def __init__(self) -> None:
        """Initialize mock WebSocket context."""
        # Required by WebSocketContextProtocol
        self.exchange_type = ExchangeName.HYPERLIQUID
        self.connection_id = "test-connection-123"
        self.message_id = "test-msg-123"
        self.timestamp = datetime.now(UTC)
        self.symbol: str | None = "BTC-USDC"
        self.routing_key = "trade.btc.usdc"
        self.domain_model: object = None

        # Required by BaseContextProtocol
        self.exchange_name = "hyperliquid"
        self.validated_envelope = None
        self.raw_model = None

        # Additional properties
        self.channel = "trades"
        self.sequence_number = 1234

    def model_dump(self, *, mode: str = "python") -> dict[str, object]:
        """Serialize context data.

        Returns:
            dict[str, object]: Serialized context data.
        """
        return {
            "connection_id": self.connection_id,
            "exchange_name": self.exchange_name,
            "channel": self.channel,
            "routing_key": self.routing_key,
            "sequence_number": self.sequence_number,
        }

    def create_error_context(self) -> StreamErrorContext:
        """Create error context from mock context.

        Returns:
            StreamErrorContext: Error context with mock connection and processor metadata.
        """
        return StreamErrorContext(
            connection_id=self.connection_id,
            exchange=self.exchange_name,
            channel=self.channel,
            topic=self.routing_key,
            sequence_number=self.sequence_number,
            metadata=ProcessorErrorMetadata(
                processor_name="TestProcessor",
                stage="validation",
                retry_count=0,
                backoff_ms=1000,
            ),
        )

    def get_transformer_params(self) -> dict[str, str]:
        """Get transformer parameters.

        Returns:
            dict[str, str]: Transformer parameters.
        """
        return {"symbol": "BTC-USDC"}

    def get_symbol_param(self) -> dict[str, str] | None:
        """Get symbol parameter.

        Returns:
            dict[str, str] | None: Symbol parameter or None.
        """
        return {"symbol": "BTC-USDC"}

    def get_coin_param(self) -> dict[str, str] | None:
        """Get coin parameter.

        Returns:
            dict[str, str] | None: Coin parameter or None.
        """
        return None


class TestProcessorErrorContextBuilder:
    """Test ProcessorErrorContextBuilder functionality."""

    @pytest.fixture
    def mock_processor(self) -> PydanticWebSocketProcessor[MockRawModel, MockRawModel]:
        """Create mock processor.

        Returns:
            PydanticWebSocketProcessor: Mock processor with predefined test configuration.
        """
        return create_mock_processor()

    @pytest.fixture
    def mock_context(self) -> MockContext:
        """Create mock WebSocket context.

        Returns:
            MockContext: Mock WebSocket context with test connection details.
        """
        return MockContext()

    @pytest.fixture
    def validation_error(self) -> ValidationError:
        """Create validation error for testing.

        Returns:
            ValidationError: Pydantic validation error from invalid model data.

        Raises:
            AssertionError: If ValidationError is not raised as expected.
        """
        try:
            MockRawModel.model_validate({"price": "invalid", "symbol": 123})
        except ValidationError as e:
            return e
        raise AssertionError("Expected ValidationError")

    @pytest.fixture
    def mock_payload(self) -> dict[str, Any]:
        """Create mock payload.

        Returns:
            dict[str, Any]: Mock payload data for testing processor error contexts.
        """
        return {"price": "150.50", "symbol": "BTC"}

    def test_from_validation_error_with_context_method(
        self,
        mock_processor: PydanticWebSocketProcessor[MockRawModel, MockRawModel],
        mock_context: MockContext,
        validation_error: ValidationError,
        mock_payload: dict[str, Any],
    ) -> None:
        """Test creating error context from validation error using context method."""
        # Act
        result = ProcessorErrorContextBuilder.from_validation_error(
            processor=mock_processor,
            payload=mock_payload,
            context=mock_context,
            validation_error=validation_error,
        )

        # Assert
        assert isinstance(result, StreamErrorContext)
        assert result.connection_id == "test-connection-123"
        assert result.exchange == "hyperliquid"
        assert result.channel == "trades"
        assert result.topic == "trade.btc.usdc"
        assert result.sequence_number == 1234

        # Check metadata
        assert result.metadata is not None
        assert isinstance(result.metadata, ProcessorErrorMetadata)
        assert result.metadata.processor_name == "TestProcessor"
        assert result.metadata.stage == "validation"
        assert result.metadata.raw_model_name == "MockRawModel"
        assert result.metadata.payload_type == "dict"
        assert result.metadata.validation_error_count == len(validation_error.errors())

    def test_from_validation_error_fallback(
        self,
        mock_processor: PydanticWebSocketProcessor[MockRawModel, MockRawModel],
        validation_error: ValidationError,
        mock_payload: dict[str, Any],
    ) -> None:
        """Test creating error context using fallback when context has no method.

        create_error_context method.
        """
        # Create mock context without create_error_context method
        mock_context = Mock(spec=WebSocketContextProtocol)
        mock_context.connection_id = "fallback-connection-456"
        mock_context.exchange_name = "backpack"
        mock_context.channel = None
        mock_context.routing_key = "orderbook.eth.usdc"
        mock_context.sequence_number = None

        # Remove create_error_context method to test fallback
        del mock_context.create_error_context

        # Act
        result = ProcessorErrorContextBuilder.from_validation_error(
            processor=mock_processor,
            payload=mock_payload,
            context=mock_context,
            validation_error=validation_error,
        )

        # Assert - should use fallback construction
        assert isinstance(result, StreamErrorContext)
        assert result.connection_id == "fallback-connection-456"
        assert result.exchange == "backpack"
        assert result.channel is None
        assert result.topic == "orderbook.eth.usdc"
        assert result.sequence_number is None

        # Check metadata
        assert result.metadata is not None
        assert isinstance(result.metadata, ProcessorErrorMetadata)
        assert result.metadata.processor_name == "TestProcessor"
        assert result.metadata.stage == "validation"

    def test_from_transformation_error(
        self,
        mock_processor: PydanticWebSocketProcessor[MockRawModel, MockRawModel],
        mock_context: MockContext,
    ) -> None:
        """Test creating error context from transformation error."""
        # Arrange
        validated_payload = MockRawModel(price=Decimal("150.50"), symbol="BTC")
        transformation_error = ValueError("Failed to transform domain model")

        # Act
        result = ProcessorErrorContextBuilder.from_transformation_error(
            processor=mock_processor,
            validated_payload=validated_payload,
            context=mock_context,
            transformation_error=transformation_error,
        )

        # Assert
        assert isinstance(result, StreamErrorContext)
        assert result.metadata is not None
        assert isinstance(result.metadata, ProcessorErrorMetadata)
        assert result.metadata.processor_name == "TestProcessor"
        assert result.metadata.stage == "transformation"
        assert result.metadata.raw_model_name == "MockRawModel"
        assert result.metadata.transformer_type == "Mock"
        assert result.metadata.validated_model_name == "MockRawModel"
        assert result.metadata.error_type == "ValueError"
        assert result.metadata.backoff_ms == 2000  # Longer backoff for transformation errors

    def test_from_handler_error_single_model(
        self,
        mock_processor: PydanticWebSocketProcessor[MockRawModel, MockRawModel],
        mock_context: MockContext,
    ) -> None:
        """Test creating error context from handler error with single domain model."""
        # Arrange
        domain_model = MockRawModel(price=Decimal("150.50"), symbol="BTC")
        handler_error = RuntimeError("Handler execution failed")

        # Act
        result = ProcessorErrorContextBuilder.from_handler_error(
            processor=mock_processor,
            domain_model=domain_model,
            context=mock_context,
            handler_error=handler_error,
            is_unexpected=False,
        )

        # Assert
        assert isinstance(result, StreamErrorContext)
        assert result.metadata is not None
        assert isinstance(result.metadata, ProcessorErrorMetadata)
        assert result.metadata.processor_name == "TestProcessor"
        assert result.metadata.stage == "handler_invocation"
        assert result.metadata.domain_model_name == "MockRawModel"
        assert result.metadata.domain_model_count == 1
        assert result.metadata.error_type == "RuntimeError"
        assert result.metadata.is_unexpected_error is False
        assert result.metadata.backoff_ms == 1500  # Standard handler error backoff

    def test_from_handler_error_batch_models(
        self,
        mock_processor: PydanticWebSocketProcessor[MockRawModel, MockRawModel],
        mock_context: MockContext,
    ) -> None:
        """Test creating error context from handler error with batch domain models."""
        # Arrange
        domain_models = [
            MockRawModel(price=Decimal("150.50"), symbol="BTC"),
            MockRawModel(price=Decimal("3500.00"), symbol="ETH"),
        ]
        handler_error = RuntimeError("Batch handler execution failed")

        # Act
        result = ProcessorErrorContextBuilder.from_handler_error(
            processor=mock_processor,
            domain_model=domain_models,
            context=mock_context,
            handler_error=handler_error,
            is_unexpected=True,
        )

        # Assert
        assert isinstance(result, StreamErrorContext)
        assert result.metadata is not None
        assert isinstance(result.metadata, ProcessorErrorMetadata)
        assert result.metadata.domain_model_name == "list[MockRawModel]"
        assert result.metadata.domain_model_count == 2
        assert result.metadata.is_unexpected_error is True
        assert result.metadata.backoff_ms == 3000  # Longer backoff for unexpected errors

    def test_from_handler_error_empty_batch(
        self,
        mock_processor: PydanticWebSocketProcessor[MockRawModel, MockRawModel],
        mock_context: MockContext,
    ) -> None:
        """Test creating error context from handler error with empty batch."""
        # Arrange
        domain_models: list[MockRawModel] = []
        handler_error = ValueError("Empty batch error")

        # Act
        result = ProcessorErrorContextBuilder.from_handler_error(
            processor=mock_processor,
            domain_model=domain_models,
            context=mock_context,
            handler_error=handler_error,
        )

        # Assert
        assert isinstance(result, StreamErrorContext)
        assert result.metadata is not None
        assert isinstance(result.metadata, ProcessorErrorMetadata)
        assert result.metadata.domain_model_name == "list[Unknown]"
        assert result.metadata.domain_model_count == 0

    def test_from_unexpected_error(
        self,
        mock_processor: PydanticWebSocketProcessor[MockRawModel, MockRawModel],
        mock_context: MockContext,
    ) -> None:
        """Test creating error context from unexpected processor error."""
        # Arrange
        unexpected_error = MemoryError("Out of memory")
        stage = "validation"

        # Act
        result = ProcessorErrorContextBuilder.from_unexpected_error(
            processor=mock_processor,
            context=mock_context,
            unexpected_error=unexpected_error,
            stage=stage,
        )

        # Assert
        assert isinstance(result, StreamErrorContext)
        assert result.metadata is not None
        assert isinstance(result.metadata, ProcessorErrorMetadata)
        assert result.metadata.processor_name == "TestProcessor"
        assert result.metadata.stage == "validation"
        assert result.metadata.error_type == "MemoryError"
        assert result.metadata.is_unexpected_error is True
        assert result.metadata.is_critical is True
        assert result.metadata.backoff_ms == 5000  # Longest backoff for unexpected errors

    def test_extract_payload_summary_dict(self) -> None:
        """Test extracting summary from dict payload."""
        # Arrange
        payload: dict[str, object] = {"price": 150.50, "symbol": "BTC", "timestamp": 1234567890}

        # Act
        result = ProcessorErrorContextBuilder.extract_payload_summary(payload, max_chars=100)

        # Assert
        assert "dict(keys=" in result
        assert "price" in result
        assert "symbol" in result
        assert "timestamp" in result
        assert len(result) <= 100

    def test_extract_payload_summary_large_dict(self) -> None:
        """Test extracting summary from large dict payload."""
        # Arrange - create dict with many keys
        payload: dict[str, object] = {f"key_{i}": f"value_{i}" for i in range(10)}

        # Act
        result = ProcessorErrorContextBuilder.extract_payload_summary(payload, max_chars=50)

        # Assert
        assert "dict(keys=" in result
        assert "+5 more" in result or "+more" in result  # Should show truncation
        assert len(result) <= 50

    def test_extract_payload_summary_list(self) -> None:
        """Test extracting summary from list payload."""
        # Arrange
        payload: list[object] = [{"price": 150.50}, {"price": 151.00}, {"price": 149.50}]

        # Act
        result = ProcessorErrorContextBuilder.extract_payload_summary(payload)

        # Assert
        assert result == "list(length=3, type=dict)"

    def test_extract_payload_summary_empty_list(self) -> None:
        """Test extracting summary from empty list."""
        # Arrange
        payload: list[Any] = []

        # Act
        result = ProcessorErrorContextBuilder.extract_payload_summary(payload)

        # Assert
        assert result == "list(empty)"

    def test_extract_payload_summary_other_type(self) -> None:
        """Test extracting summary from other payload types."""
        # Arrange
        payload = "some string payload"

        # Act
        result = ProcessorErrorContextBuilder.extract_payload_summary(payload, max_chars=20)

        # Assert
        assert result.startswith("str(")
        assert "some string" in result or result.endswith("...)")

    def test_extract_payload_summary_exception_handling(self) -> None:
        """Test payload summary extraction handles exceptions gracefully."""

        # Arrange - create object that will raise exception during processing
        class BadObject(BaseModel):
            def __len__(self) -> int:
                raise RuntimeError("Cannot get length")

            def __str__(self) -> str:
                raise RuntimeError("Cannot convert to string")

        payload = BadObject()

        # Act
        result = ProcessorErrorContextBuilder.extract_payload_summary(payload)

        # Assert
        assert result == "BadObject(summary_failed)"

    def test_enhance_context_with_metrics(
        self,
        mock_processor: PydanticWebSocketProcessor[MockRawModel, MockRawModel],
        mock_context: MockContext,
    ) -> None:
        """Test enhancing error context with processor metrics."""
        # Arrange
        context = mock_context.create_error_context()

        # Act
        enhanced_context = ProcessorErrorContextBuilder.enhance_context_with_metrics(
            context=context,
            processor=mock_processor,
        )

        # Assert
        assert enhanced_context is context  # Should modify in place
        assert enhanced_context.metadata is not None
        assert isinstance(enhanced_context.metadata, ProcessorErrorMetadata)
        assert enhanced_context.metadata.total_processed == 100
        assert enhanced_context.metadata.total_errors == 8  # 5 + 2 + 1 = 8
        assert enhanced_context.metadata.error_rate == 0.08  # 8/100 = 0.08

    def test_enhance_context_with_metrics_no_metadata(
        self,
        mock_processor: PydanticWebSocketProcessor[MockRawModel, MockRawModel],
    ) -> None:
        """Test enhancing context when it has basic metadata."""
        # Arrange
        context = StreamErrorContext(
            connection_id="test-123",
            exchange="hyperliquid",
            # Uses default metadata from factory
        )

        # Act
        enhanced_context = ProcessorErrorContextBuilder.enhance_context_with_metrics(
            context=context,
            processor=mock_processor,
        )

        # Assert - should handle gracefully without error
        assert enhanced_context is context
        assert enhanced_context.metadata is not None  # Has default metadata from factory

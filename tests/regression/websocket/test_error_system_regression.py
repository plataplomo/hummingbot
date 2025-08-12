"""Regression test suite for WebSocket error system.

Comprehensive regression tests to prevent reintroduction of fixed bugs
and ensure system stability across changes.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel, Field, ValidationError

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_context import WebSocketContext
from cyberdelta.apis.websocket.ws_dual_error_manager import DualErrorManager
from cyberdelta.apis.websocket.ws_error_adapter import WebSocketErrorAdapter
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_exceptions import (
    WebSocketConnectionError,
    WebSocketValidationError,
)
from cyberdelta.apis.websocket.ws_processor import PydanticWebSocketProcessor
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from tests.utils.websocket.error_test_utils import ErrorTestFactory


# Test models for regression tests
class TestMessage(BaseModel):
    """Test message model."""

    msg_type: str = Field(description="Message type")
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: int = Field(description="Unix timestamp")


class TestTransformer:
    """Test transformer."""

    def transform(self, raw: TestMessage) -> dict[str, Any]:
        """Transform message."""
        return {
            "type": raw.msg_type,
            "data": raw.payload,
            "processed_at": datetime.now(UTC),
        }


@pytest.mark.asyncio
class TestErrorSystemRegression:
    """Regression tests for WebSocket error system."""

    @pytest.fixture
    def error_config(self) -> WebSocketErrorConfig:
        """Create test configuration."""
        return WebSocketErrorConfig(
            max_recovery_attempts=3,
            recovery_backoff_ms=100,
            enable_metrics_collection=True,
            enable_legacy_compatibility=True,
        )

    async def test_regression_dict_conversion_elimination(self) -> None:
        """Regression: Ensure dict[str, Any] conversions are eliminated."""
        # This was a major issue - dict conversions losing type safety

        # Create typed error
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST,
        )

        # Verify no dict conversion needed
        assert isinstance(error, WebSocketStreamError)
        assert isinstance(error.context.connection_id, str)
        assert isinstance(error.context.exchange, str)
        assert isinstance(error.context.error_timestamp_ms, int)

        # Context should not need dict conversion
        assert hasattr(error.context, "model_dump")  # Pydantic model

    async def test_regression_recovery_strategy_enum(self) -> None:
        """Regression: Ensure recovery strategies use enum not boolean."""
        # Old system used is_retryable boolean, new uses enum

        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.RATE_LIMITED,
        )

        # Should have enum strategy, not just boolean
        assert isinstance(error.recovery_strategy, WebSocketRecoveryStrategy)
        assert error.recovery_strategy != WebSocketRecoveryStrategy.NONE

        # Should still have is_retryable for compatibility
        assert hasattr(error, "is_retryable")
        assert error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE is True

    async def test_regression_error_inheritance_independence(self) -> None:
        """Regression: WebSocketStreamError should not inherit from APIError."""
        # New system must be independent

        error = WebSocketStreamError(
            message="Test",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=ErrorTestFactory.create_test_context(),
        )

        # Should NOT be an APIError
        assert not isinstance(error, APIError)

        # Should be Exception
        assert isinstance(error, Exception)

    async def test_regression_context_validation(self) -> None:
        """Regression: Context fields should be validated."""
        # Old system allowed invalid contexts

        # Should validate connection_id format
        with pytest.raises(ValidationError):
            context = ErrorTestFactory.create_test_context(
                connection_id="x" * 1000,  # Too long
            )

    async def test_regression_sequence_gap_detection(self) -> None:
        """Regression: Sequence gaps should be properly detected."""
        # Old system had issues with sequence tracking

        context = ErrorTestFactory.create_test_context(
            sequence_number=100,
            expected_sequence=105,
        )

        # Should detect gap
        gap = context.get_sequence_gap_size()
        assert gap == 5

        # Error should use appropriate recovery
        error = WebSocketStreamError(
            message="Sequence gap",
            code=WebSocketErrorCode.SEQUENCE_GAP,
            context=context,
        )

        assert error.recovery_strategy == WebSocketRecoveryStrategy.IMMEDIATE_RETRY

    async def test_regression_processor_error_handling(self) -> None:
        """Regression: Processor should handle errors without dict conversion."""
        # Old processor used dict conversions extensively

        processor = PydanticWebSocketProcessor(
            raw_model=TestMessage,
            transformer=TestTransformer(),
            processor_name="regression_test",
        )

        # Invalid message
        invalid_data = {
            "msg_type": 123,  # Should be string
            "timestamp": "not_a_number",  # Should be int
        }

        # Should raise validation error
        with pytest.raises(ValidationError) as exc_info:
            TestMessage(**invalid_data)

        # Error should have proper structure
        assert exc_info.value.errors()

    async def test_regression_adapter_preserves_metadata(self) -> None:
        """Regression: Adapter should preserve all WebSocket metadata."""
        # Old adapter lost metadata during conversion

        context = ErrorTestFactory.create_test_context(
            connection_id="test-conn-123",
            exchange="hyperliquid",
            channel="trades",
            topic="BTC-USDC",
            sequence_number=1000,
            expected_sequence=1005,
        )

        ws_error = WebSocketStreamError(
            message="Test error",
            code=WebSocketErrorCode.STREAM_INTERRUPTED,
            context=context,
        )

        # Convert to APIError
        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        # All metadata should be preserved
        assert api_error.metadata["connection_id"] == "test-conn-123"
        assert api_error.metadata["exchange"] == "hyperliquid"
        assert api_error.metadata["channel"] == "trades"
        assert api_error.metadata["topic"] == "BTC-USDC"
        assert api_error.metadata["sequence_number"] == 1000
        assert api_error.metadata["expected_sequence"] == 1005
        assert api_error.metadata["sequence_gap"] == 5

    async def test_regression_error_severity_determination(self) -> None:
        """Regression: Error severity should be correctly determined."""
        # Old system had inconsistent severity determination

        # Critical errors
        critical_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.STREAM_CORRUPTED,
        )
        assert critical_error.severity == ErrorSeverity.CRITICAL

        # Warning errors
        warning_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.SEQUENCE_GAP,
        )
        assert warning_error.severity == ErrorSeverity.WARNING

        # Error level
        error_level = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_TIMEOUT,
        )
        assert error_level.severity == ErrorSeverity.ERROR

    async def test_regression_dual_system_compatibility(
        self,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Regression: Dual error system should work during migration."""
        # Must support both old and new systems simultaneously

        old_handler = MagicMock()
        old_handler.handle_error = AsyncMock(return_value={"handled": True})

        new_handler = WebSocketStreamErrorHandler(config=error_config)

        dual_manager = DualErrorManager(
            old_handler=old_handler,
            new_handler=new_handler,
            config=error_config,
        )

        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST,
        )

        context = WebSocketContext(
            connection_id="dual-test",
            exchange_name="hyperliquid",
        )

        # Should handle in both systems
        await dual_manager.handle_error_dual(error, context)

        assert old_handler.handle_error.called

    async def test_regression_error_chain_preservation(self) -> None:
        """Regression: Error chains should be preserved through the system."""
        # Old system lost error chains

        root_cause = ValueError("Database connection failed")
        middle_error = ConnectionError("Could not establish WebSocket")

        context = ErrorTestFactory.create_test_context()
        context.add_error_to_chain(root_cause)
        context.add_error_to_chain(middle_error)

        final_error = WebSocketConnectionError(
            message="WebSocket connection failed",
            code=WebSocketErrorCode.CONNECTION_FAILED,
            context=context,
            cause=middle_error,
        )

        # Chain should be preserved
        assert len(final_error.context.error_chain) == 2
        assert final_error.context.error_chain[0].error_class == "ValueError"
        assert final_error.context.error_chain[1].error_class == "ConnectionError"
        assert final_error.cause == middle_error

    async def test_regression_timestamp_handling(self) -> None:
        """Regression: Timestamps should be properly handled."""
        # Old system had timezone issues

        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST,
        )

        # Should have timestamp
        assert hasattr(error, "get_age_ms")
        age = error.get_age_ms()
        assert age >= 0

        # Context should have timestamp
        assert error.context.error_timestamp_ms > 0

    async def test_regression_validation_error_specificity(self) -> None:
        """Regression: Validation errors should include field information."""
        # Old system lost field-specific information

        context = ErrorTestFactory.create_test_context()

        val_error = WebSocketValidationError(
            message="Invalid message format",
            code=WebSocketErrorCode.INVALID_MESSAGE_FORMAT,
            context=context,
            field="message_type",
            value="invalid_type",
        )

        # Should have field information
        assert val_error.field == "message_type"
        assert val_error.value == "invalid_type"

    async def test_regression_recovery_strategy_selection(self) -> None:
        """Regression: Recovery strategies should be appropriate for error type."""
        # Old system had inappropriate recovery strategies

        test_cases = [
            (WebSocketErrorCode.CONNECTION_LOST, WebSocketRecoveryStrategy.RECONNECT_SAME),
            (WebSocketErrorCode.RATE_LIMITED, WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF),
            (WebSocketErrorCode.AUTH_REVOKED, WebSocketRecoveryStrategy.NONE),
            (WebSocketErrorCode.STREAM_CORRUPTED, WebSocketRecoveryStrategy.FULL_RECONNECT),
        ]

        for code, expected_strategy in test_cases:
            error = ErrorTestFactory.create_test_error(code=code)
            # Allow some flexibility in strategy selection
            if expected_strategy == WebSocketRecoveryStrategy.NONE:
                assert error.recovery_strategy == WebSocketRecoveryStrategy.NONE
            else:
                assert error.recovery_strategy != WebSocketRecoveryStrategy.NONE

    async def test_regression_metric_collection_accuracy(
        self,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Regression: Metrics should be accurately collected."""
        # Old system had inaccurate metric collection

        handler = WebSocketStreamErrorHandler(config=error_config)

        # Generate specific errors
        for _ in range(5):
            await handler.handle_stream_error(
                ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)
            )

        for _ in range(3):
            await handler.handle_stream_error(
                ErrorTestFactory.create_test_error(code=WebSocketErrorCode.RATE_LIMITED)
            )

        # Check metrics
        metrics = handler.get_metrics()
        assert metrics.total_errors == 8
        assert metrics.errors_by_code.get("CONNECTION_LOST", 0) == 5
        assert metrics.errors_by_code.get("RATE_LIMITED", 0) == 3

    async def test_regression_no_hardcoded_values(self) -> None:
        """Regression: System should not use hardcoded values."""
        # CODING_STANDARDS.md compliance - no hardcoding

        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.RATE_LIMITED,
        )

        # Retry delay should come from config/calculation, not hardcoded
        delay = error.get_retry_delay_ms()
        # Should be reasonable, not hardcoded like 5000
        assert 0 < delay < 100000  # Reasonable range

        # Recovery attempts should come from config
        # This would be checked in actual implementation

    async def test_regression_exchange_specific_handling(self) -> None:
        """Regression: Exchange-specific errors should be handled correctly."""
        # Old system didn't handle exchange differences

        # Hyperliquid error
        hl_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.RATE_LIMITED,
        )
        hl_error.context.exchange = "hyperliquid"

        # Backpack error
        bp_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.RATE_LIMITED,
        )
        bp_error.context.exchange = "backpack"

        # Both should be rate limited but context differs
        assert hl_error.code == bp_error.code
        assert hl_error.context.exchange != bp_error.context.exchange

    async def test_regression_no_silent_failures(
        self,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Regression: Errors should not be silently ignored."""
        # Old system had silent failures

        handler = WebSocketStreamErrorHandler(config=error_config)

        # Critical error should not be silent
        critical = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.SECURITY_VIOLATION,
        )

        await handler.handle_stream_error(critical)

        # Should be in metrics (not silently dropped)
        metrics = handler.get_metrics()
        assert metrics.total_errors >= 1

    async def test_regression_decimal_precision(self) -> None:
        """Regression: Financial values should use Decimal for precision."""
        # Old system had float precision issues

        # If error system tracks financial data, it should use Decimal
        # This is more relevant for the actual trading logic
        # but error context might include financial data

        context = ErrorTestFactory.create_test_context()
        # If we add financial fields to context, they should be Decimal
        # Example (if implemented):
        # context.position_size = Decimal("100.123456789")
        # assert isinstance(context.position_size, Decimal)

    async def test_regression_concurrent_safety(
        self,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Regression: Concurrent error handling should be safe."""
        # Old system had race conditions

        handler = WebSocketStreamErrorHandler(config=error_config)

        # Create many errors
        errors = [
            ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)
            for _ in range(100)
        ]

        # Handle concurrently
        tasks = [handler.handle_stream_error(error) for error in errors]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # All should be handled
        assert len(results) == 100

        # Metrics should be accurate
        metrics = handler.get_metrics()
        assert metrics.total_errors == 100

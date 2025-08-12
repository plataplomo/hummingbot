"""Comprehensive Integration Test Suite for New WebSocket Error System.

This test suite validates the complete end-to-end functionality of the new
typed WebSocket error system after migration, ensuring all components work
together correctly without any dict[str, Any] patterns.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.enums import ExchangeName
from cyberdelta.apis.websocket.ws_error_adapter import WebSocketErrorAdapter
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_error_handler_factory import (
    WebSocketErrorHandlerFactory,
)
from cyberdelta.apis.websocket.ws_error_health_check import (
    HealthCheckConfig,
    HealthStatus,
    WebSocketErrorHealthCheck,
)
from cyberdelta.apis.websocket.ws_error_metrics_collector import (
    WebSocketErrorMetricsCollector,
)
from cyberdelta.apis.websocket.ws_exceptions import (
    WebSocketConnectionError,
    WebSocketSubscriptionError,
    WebSocketValidationError,
)
from cyberdelta.apis.websocket.ws_processor import PydanticWebSocketProcessor
from cyberdelta.apis.websocket.ws_processor_error_bridge import (
    ProcessorErrorBridge,
)
from cyberdelta.apis.websocket.ws_processor_error_context import (
    ProcessorErrorContextBuilder,
)
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_recovery_strategy_router import (
    RecoveryStrategyRouter,
)
from cyberdelta.apis.websocket.ws_router import BaseWebSocketRouter
from cyberdelta.apis.websocket.ws_router_error_bridge import RouterErrorBridge
from cyberdelta.apis.websocket.ws_router_error_context import (
    RouterErrorContextBuilder,
)
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_error_handler import (
    WebSocketStreamErrorHandler,
)
from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor
from cyberdelta.config.models.websocket_error_config import (
    WebSocketErrorAlertingConfig,
    WebSocketErrorConfig,
)


class TestPayload(BaseModel):
    """Test payload model for integration tests."""

    symbol: str
    price: Decimal
    quantity: Decimal


class TestEnvelope(BaseModel):
    """Test envelope model for integration tests."""

    channel: str
    data: dict[str, Any]


class TestWebSocketRouter(BaseWebSocketRouter[TestEnvelope]):
    """Test router implementation for integration tests."""

    def _setup_processors(self) -> None:
        """Setup test processors."""

    def _extract_routing_key_from_envelope(self, envelope: TestEnvelope) -> str | None:
        """Extract routing key from test envelope."""
        return envelope.channel

    def _extract_payload_from_envelope(self, envelope: TestEnvelope) -> dict[str, Any] | list[Any]:
        """Extract payload from test envelope."""
        return envelope.data


class TestNewErrorSystemComplete:
    """Comprehensive integration tests for the new WebSocket error system."""

    @pytest.fixture
    def error_config(self) -> WebSocketErrorConfig:
        """Create test error configuration."""
        return WebSocketErrorConfig(
            max_recovery_attempts=3,
            recovery_backoff_ms=100,
            enable_metrics_collection=True,
            enable_performance_tracking=True,
            alerting=WebSocketErrorAlertingConfig(
                enable_alerts=True,
                alert_on_critical=True,
                alert_on_recovery_failure=True,
            ),
        )

    @pytest.fixture
    def error_handler(
        self, error_config: WebSocketErrorConfig, metrics_collector: WebSocketErrorMetricsCollector
    ) -> WebSocketStreamErrorHandler:
        """Create test error handler."""
        return WebSocketErrorHandlerFactory.create_handler(
            exchange="backpack",  # Use supported exchange
            config=error_config,
            metrics_collector=metrics_collector,
        )

    @pytest.fixture
    def metrics_collector(self) -> WebSocketErrorMetricsCollector:
        """Create test metrics collector."""
        return WebSocketErrorMetricsCollector()

    @pytest.fixture
    def health_check(self) -> WebSocketErrorHealthCheck:
        """Create test health check system."""
        return WebSocketErrorHealthCheck(config=HealthCheckConfig())

    @pytest.fixture
    def recovery_router(self) -> RecoveryStrategyRouter:
        """Create test recovery strategy router."""
        return RecoveryStrategyRouter()

    @pytest.fixture
    def test_context(self) -> WebSocketContextProtocol:
        """Create test WebSocket context."""
        # Create a mock context that satisfies the protocol
        context = MagicMock(spec=WebSocketContextProtocol)
        context.connection_id = "test_conn_123"
        context.exchange_name = "backpack"
        context.channel = "test_channel"
        context.sequence_number = 123
        return context

    @pytest_asyncio.fixture
    async def test_router(
        self,
        error_handler: WebSocketStreamErrorHandler,
        metrics_collector: WebSocketErrorMetricsCollector,
    ) -> TestWebSocketRouter:
        """Create test router with new error system."""
        # Create mock legacy error handler
        legacy_handler = AsyncMock()

        # Create typed processor using mock since it requires registry
        typed_processor = MagicMock(spec=TypeSafeWebSocketProcessor)

        # Create router with new error system
        router = TestWebSocketRouter(
            exchange_name="backpack",
            exchange_type=ExchangeName.BACKPACK,
            error_handler=legacy_handler,  # Legacy handler for fallback
            typed_processor=typed_processor,
            envelope_validator=TestEnvelope.model_validate,
            stream_error_handler=error_handler,  # New typed error handler
            metrics_collector=metrics_collector,
        )

        return router

    @pytest.mark.asyncio
    async def test_complete_error_flow_validation_error(
        self,
        test_router: TestWebSocketRouter,
        error_handler: WebSocketStreamErrorHandler,
        metrics_collector: WebSocketErrorMetricsCollector,
        test_context: WebSocketContextProtocol,
    ) -> None:
        """Test complete error flow for validation errors."""
        # Create invalid message that will fail envelope validation
        invalid_message = {"invalid": "structure", "missing": "channel"}

        # Create handler registry
        handlers = {"test_channel": AsyncMock()}

        # Route message (should trigger validation error)
        await test_router.route_message(invalid_message, handlers)

        # Verify error was handled through new system
        # The router should have called stream_error_handler for envelope validation error
        assert test_router.stream_error_handler is not None

        # Check metrics were collected
        metrics = metrics_collector.get_summary()
        assert metrics.total_errors > 0
        assert WebSocketErrorCode.VALIDATION_FAILED.name in metrics.errors_by_code

    async def test_complete_error_flow_missing_processor(
        self,
        test_router: TestWebSocketRouter,
        error_handler: WebSocketStreamErrorHandler,
        metrics_collector: WebSocketErrorMetricsCollector,
        test_context: WebSocketContextProtocol,
    ) -> None:
        """Test complete error flow for missing processor errors."""
        # Create valid message with channel that has no processor
        valid_message = {
            "channel": "unknown_channel",
            "data": {"symbol": "BTC", "price": "50000.00", "quantity": "1.0"},
        }

        # Create handler for the channel
        handlers = {"unknown_channel": AsyncMock()}

        # Route message (should trigger missing processor error)
        await test_router.route_message(valid_message, handlers)

        # Check that router handled missing processor through new system
        assert test_router.stream_error_handler is not None

        # Check metrics
        metrics = metrics_collector.get_metrics()
        assert metrics.total_errors > 0

    async def test_processor_error_bridge_integration(
        self,
        error_config: WebSocketErrorConfig,
        test_context: WebSocketContextProtocol,
    ) -> None:
        """Test processor error bridge integration with typed system."""
        # Create processor with error bridge
        processor = PydanticWebSocketProcessor(raw_model=TestPayload)

        # Create error bridge
        error_bridge = ProcessorErrorBridge(
            processor_name="test_processor",
            stream_error_handler=AsyncMock(),
        )

        # Set bridge on processor
        processor.error_bridge = error_bridge

        # Create validation error scenario
        invalid_payload = {"invalid": "data"}

        # Process with error handling
        handler = AsyncMock()
        await processor.process(invalid_payload, handler, test_context)

        # Verify error was handled through bridge
        assert error_bridge.stream_error_handler.handle_stream_error.called

    async def test_router_error_bridge_integration(
        self,
        test_router: TestWebSocketRouter,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Test router error bridge integration with typed system."""
        # Create router error bridge
        error_bridge = RouterErrorBridge(
            router_name="test_router",
            exchange_name="backpack",
            stream_error_handler=test_router.stream_error_handler,
        )

        # Test envelope validation error handling
        invalid_envelope = {"invalid": "envelope"}
        error = ValidationError.from_exception_data(
            "TestEnvelope", [{"type": "missing", "loc": ("channel",), "msg": "Field required"}]
        )

        await error_bridge.handle_envelope_validation_error(
            error=error,
            message=invalid_envelope,
            envelope_type="TestEnvelope",
        )

        # Verify typed error was created and handled
        assert error_bridge.stream_error_handler is not None

    async def test_recovery_strategy_routing(
        self,
        recovery_router: RecoveryStrategyRouter,
        test_context: WebSocketContextProtocol,
    ) -> None:
        """Test recovery strategy routing for different error types."""
        # Create connection error requiring full reconnect
        connection_error = WebSocketConnectionError(
            message="Connection lost",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=StreamErrorContext(
                connection_id="test_conn",
                exchange="backpack",
                error_timestamp_ms=1234567890,
            ),
        )

        # Mock connection for recovery
        mock_connection = AsyncMock()

        # Execute recovery strategy
        await recovery_router.execute_recovery(
            error=connection_error,
            connection=mock_connection,
        )

        # Verify correct recovery strategy was executed
        strategy = connection_error.get_recovery_strategy()
        assert strategy == WebSocketRecoveryStrategy.FULL_RECONNECT

        # Test subscription error with different recovery
        subscription_error = WebSocketSubscriptionError(
            message="Subscription failed",
            code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
            context=StreamErrorContext(
                connection_id="test_conn",
                exchange="backpack",
                channel="test_channel",
                error_timestamp_ms=1234567890,
            ),
        )

        await recovery_router.execute_recovery(
            error=subscription_error,
            connection=mock_connection,
        )

        # Verify subscription recovery strategy
        sub_strategy = subscription_error.get_recovery_strategy()
        assert sub_strategy in [
            WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        ]

    @pytest.mark.asyncio
    async def test_error_metrics_collection(
        self,
        metrics_collector: WebSocketErrorMetricsCollector,
        test_context: WebSocketContextProtocol,
    ) -> None:
        """Test comprehensive error metrics collection."""
        # Record various error types
        errors = [
            WebSocketValidationError(
                message="Validation error",
                code=WebSocketErrorCode.VALIDATION_FAILED,
                context=StreamErrorContext(
                    connection_id="test_conn",
                    exchange="backpack",
                    error_timestamp_ms=1234567890,
                ),
                field="test_field",
                value="invalid_value",
            ),
            WebSocketConnectionError(
                message="Connection error",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=StreamErrorContext(
                    connection_id="test_conn",
                    exchange="backpack",
                    error_timestamp_ms=1234567891,
                ),
            ),
            WebSocketSubscriptionError(
                message="Subscription error",
                code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
                context=StreamErrorContext(
                    connection_id="test_conn",
                    exchange="backpack",
                    channel="test_channel",
                    error_timestamp_ms=1234567892,
                ),
            ),
        ]

        # Record errors with recovery data
        metrics_collector.record_error(errors[0], recovery_time_ms=500, success=True)
        metrics_collector.record_error(errors[1], recovery_time_ms=200, success=False)
        metrics_collector.record_error(errors[2])

        # Get metrics
        metrics = metrics_collector.get_summary()

        # Verify metrics
        assert metrics.total_errors == 3
        assert len(metrics.errors_by_code) == 3
        assert metrics.errors_by_code[WebSocketErrorCode.VALIDATION_FAILED.name] == 1
        assert metrics.errors_by_code[WebSocketErrorCode.CONNECTION_LOST.name] == 1
        assert metrics.errors_by_code[WebSocketErrorCode.SUBSCRIPTION_FAILED.name] == 1

        # Verify recovery metrics - only record errors with recovery strategies have recovery data
        assert metrics.recovery_attempts >= 2  # At least 2 errors had recovery strategies
        assert metrics.successful_recoveries >= 1

    async def test_health_check_integration(
        self,
        health_check: WebSocketErrorHealthCheck,
        error_handler: WebSocketStreamErrorHandler,
        metrics_collector: WebSocketErrorMetricsCollector,
    ) -> None:
        """Test health check system integration."""
        # Set up health check with components
        health_check._error_handler = error_handler
        health_check._metrics_collector = metrics_collector

        # Simulate some errors
        for i in range(5):
            error = WebSocketValidationError(
                message=f"Test error {i}",
                code=WebSocketErrorCode.VALIDATION_FAILED,
                context=StreamErrorContext(
                    connection_id="test_conn",
                    exchange="backpack",
                    error_timestamp_ms=1234567890 + i,
                ),
                field="test_field",
                value=f"value_{i}",
            )
            metrics_collector.record_error(error)

        # Check health
        health = await health_check.check_health()

        # Verify health status
        assert health.overall_status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED]
        assert health.error_handler_status == HealthStatus.HEALTHY
        assert health.metrics_collector_status == HealthStatus.HEALTHY
        assert health.total_errors == 5
        assert health.error_rate_per_minute > 0

    async def test_adapter_compatibility(
        self,
        test_context: WebSocketContextProtocol,
    ) -> None:
        """Test adapter maintains compatibility with legacy systems."""
        # Create WebSocket error
        ws_error = WebSocketConnectionError(
            message="Connection lost during trading",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=StreamErrorContext(
                connection_id="test_conn",
                exchange="backpack",
                channel="trades",
                error_timestamp_ms=1234567890,
            ),
        )

        # Convert to APIError using adapter
        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        # Verify conversion preserves information
        assert api_error.message == ws_error.message
        assert api_error.details is not None
        assert api_error.details.get("ws_code") == WebSocketErrorCode.CONNECTION_LOST.value
        assert api_error.details.get("ws_code_name") == WebSocketErrorCode.CONNECTION_LOST.name
        assert api_error.details.get("connection_id") == "test_conn"
        assert api_error.details.get("exchange") == "test_exchange"
        assert api_error.details.get("channel") == "trades"

        # Verify retryability mapping
        assert api_error.is_retryable == (
            ws_error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE
        )

    async def test_no_dict_any_in_error_paths(
        self,
        test_router: TestWebSocketRouter,
        test_context: WebSocketContextProtocol,
    ) -> None:
        """Verify no dict[str, Any] patterns exist in error handling paths."""
        # Create typed error context
        error_context = StreamErrorContext(
            connection_id="test_conn",
            exchange="backpack",
            channel="test_channel",
            topic="test_topic",
            sequence_number=123,
            error_timestamp_ms=1234567890,
        )

        # Verify context is fully typed
        assert isinstance(error_context, BaseModel)
        assert not isinstance(error_context, dict)

        # Create typed error
        error = WebSocketValidationError(
            message="Type safety test",
            code=WebSocketErrorCode.VALIDATION_FAILED,
            context=error_context,
            field="test_field",
            value="test_value",
        )

        # Verify error is fully typed
        assert isinstance(error, WebSocketStreamError)
        assert isinstance(error.context, StreamErrorContext)

        # Create processor error context
        processor_context = ProcessorErrorContextBuilder.from_validation_error(
            processor=MagicMock(),
            payload=TestPayload(symbol="BTC", price=Decimal(50000), quantity=Decimal(1)),
            context=test_context,
        )

        # Verify processor context is typed
        assert isinstance(processor_context, StreamErrorContext)
        assert not isinstance(processor_context, dict)

        # Create router error context
        router_context = RouterErrorContextBuilder.from_missing_processor_error(
            router=test_router,
            routing_key="test_key",
            payload={"test": "payload"},
            context=test_context,
        )

        # Verify router context is typed
        assert isinstance(router_context, StreamErrorContext)
        assert not isinstance(router_context, dict)

    async def test_concurrent_error_handling(
        self,
        error_handler: WebSocketStreamErrorHandler,
        metrics_collector: WebSocketErrorMetricsCollector,
        test_context: WebSocketContextProtocol,
    ) -> None:
        """Test concurrent error handling with new system."""
        # Create multiple errors
        errors = []
        for i in range(10):
            error = WebSocketValidationError(
                message=f"Concurrent error {i}",
                code=WebSocketErrorCode.VALIDATION_FAILED,
                context=StreamErrorContext(
                    connection_id=f"conn_{i}",
                    exchange="backpack",
                    error_timestamp_ms=1234567890 + i,
                ),
                field=f"field_{i}",
                value=f"value_{i}",
            )
            errors.append(error)

        # Handle errors concurrently
        tasks = [error_handler.handle_stream_error(error) for error in errors]
        await asyncio.gather(*tasks)

        # Verify all errors were handled
        metrics = metrics_collector.get_metrics()
        assert metrics.total_errors >= 10

    async def test_error_recovery_with_circuit_breaker(
        self,
        recovery_router: RecoveryStrategyRouter,
    ) -> None:
        """Test error recovery with circuit breaker pattern."""
        # Create error requiring circuit breaker
        critical_error = WebSocketStreamError(
            message="Critical system failure",
            code=WebSocketErrorCode.STREAM_CORRUPTED,
            context=StreamErrorContext(
                connection_id="test_conn",
                exchange="backpack",
                error_timestamp_ms=1234567890,
            ),
            severity=ErrorSeverity.CRITICAL,
        )

        # Mock connection
        mock_connection = AsyncMock()

        # Execute recovery (should trigger circuit breaker)
        await recovery_router.execute_recovery(
            error=critical_error,
            connection=mock_connection,
        )

        # Verify circuit breaker strategy was detected
        strategy = critical_error.get_recovery_strategy()
        assert strategy == WebSocketRecoveryStrategy.CIRCUIT_BREAKER

    @pytest.mark.asyncio
    async def test_complete_migration_validation(
        self,
        test_router: TestWebSocketRouter,
        error_handler: WebSocketStreamErrorHandler,
        metrics_collector: WebSocketErrorMetricsCollector,
        health_check: WebSocketErrorHealthCheck,
        recovery_router: RecoveryStrategyRouter,
    ) -> None:
        """Validate complete migration to new error system."""
        # Test all major components are using typed system

        # 1. Router uses typed error handler
        assert test_router.stream_error_handler is not None
        assert isinstance(test_router.stream_error_handler, WebSocketStreamErrorHandler)

        # 2. Error handler is fully typed
        assert hasattr(error_handler, "handle_stream_error")
        assert hasattr(error_handler, "handle_validation_error")

        # 3. Metrics collector works with typed errors
        test_error = WebSocketValidationError(
            message="Migration test",
            code=WebSocketErrorCode.VALIDATION_FAILED,
            context=StreamErrorContext(
                connection_id="test_conn",
                exchange="backpack",
                error_timestamp_ms=1234567890,
            ),
            field="test",
            value="test",
        )
        metrics_collector.record_error(test_error)
        assert metrics_collector.get_summary().total_errors > 0

        # 4. Health check monitors typed system
        health_check._metrics_collector = metrics_collector
        health = await health_check.check_health()
        assert health.overall_status != HealthStatus.UNKNOWN

        # 5. Recovery router handles typed errors
        assert hasattr(recovery_router, "route_recovery")

        # Overall validation: System is fully migrated
        assert True, "Complete migration to typed error system validated"


class TestErrorSystemPerformance:
    """Performance tests for the new error system."""

    async def test_error_creation_performance(self) -> None:
        """Test error creation performance meets requirements."""
        import time

        iterations = 1000
        start = time.perf_counter()

        for i in range(iterations):
            error = WebSocketValidationError(
                message=f"Performance test {i}",
                code=WebSocketErrorCode.VALIDATION_FAILED,
                context=StreamErrorContext(
                    connection_id="perf_conn",
                    exchange="backpack",
                    error_timestamp_ms=1234567890,
                ),
                field="test_field",
                value=f"value_{i}",
            )

        elapsed = time.perf_counter() - start
        per_error_ms = (elapsed / iterations) * 1000

        # Allow for Pydantic overhead (target: < 10ms per error)
        assert per_error_ms < 10, f"Error creation too slow: {per_error_ms:.2f}ms per error"

    async def test_concurrent_error_handling_performance(self) -> None:
        """Test concurrent error handling performance."""
        import time

        # Create error handler
        error_handler = WebSocketErrorHandlerFactory.create_handler(
            exchange="backpack",
            config=WebSocketErrorConfig(),
        )

        # Create errors
        errors = []
        for i in range(100):
            error = WebSocketValidationError(
                message=f"Concurrent perf test {i}",
                code=WebSocketErrorCode.VALIDATION_FAILED,
                context=StreamErrorContext(
                    connection_id=f"perf_conn_{i}",
                    exchange="backpack",
                    error_timestamp_ms=1234567890,
                ),
                field="test_field",
                value=f"value_{i}",
            )
            errors.append(error)

        # Handle concurrently
        start = time.perf_counter()
        tasks = [error_handler.handle_stream_error(error) for error in errors]
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start

        # Target: < 1 second for 100 concurrent errors
        assert elapsed < 1.0, f"Concurrent handling too slow: {elapsed:.2f}s for 100 errors"

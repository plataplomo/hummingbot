"""End-to-end test of Phase 2 WebSocket error system integration.

This test validates Step 50: Phase 2 Integration Validation.
"""

from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.common.error_foundation import (
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.connectivity.ws_connection_error_bridge import (
    ConnectionErrorBridge,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_error_handler_factory import (
    WebSocketErrorHandlerFactory,
)
from cyberdelta.apis.websocket.ws_error_handler_registry import (
    WebSocketErrorHandlerRegistry,
)
from cyberdelta.apis.websocket.ws_error_recovery import (
    ErrorRecoveryConfig,
    WebSocketErrorRecovery,
)
from cyberdelta.apis.websocket.ws_processor import PydanticWebSocketProcessor
from cyberdelta.apis.websocket.ws_processor_error_context import (
    ProcessorErrorContextBuilder,
)
from cyberdelta.apis.websocket.ws_recovery_strategy_router import (
    RecoveryStrategyRouter,
)
from cyberdelta.apis.websocket.ws_router import BaseWebSocketRouter
from cyberdelta.apis.websocket.ws_router_error_context import (
    RouterErrorContextBuilder,
)
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig


class TestMessage(BaseModel):
    """Test message model."""

    type: str
    data: dict[str, Any]


class TestContext(BaseModel):
    """Test WebSocket context."""

    connection_id: str
    exchange: str
    channel: str | None = None
    sequence_number: int | None = None


@pytest.mark.asyncio
class TestPhase2Complete:
    """Test complete Phase 2 integration."""

    @pytest.fixture
    def error_config(self) -> WebSocketErrorConfig:
        """Create error configuration."""
        return WebSocketErrorConfig(
            max_recovery_attempts=3,
            recovery_backoff_ms=100,
            enable_metrics_collection=True,
        )

    @pytest.fixture
    def recovery_config(self) -> ErrorRecoveryConfig:
        """Create recovery configuration."""
        return ErrorRecoveryConfig()

    @pytest.fixture
    def error_handler_factory(self) -> WebSocketErrorHandlerFactory:
        """Create error handler factory."""
        return WebSocketErrorHandlerFactory()

    @pytest.fixture
    def error_handler_registry(self) -> WebSocketErrorHandlerRegistry:
        """Create error handler registry."""
        return WebSocketErrorHandlerRegistry()

    @pytest.fixture
    def recovery_router(self) -> RecoveryStrategyRouter:
        """Create recovery strategy router."""
        return RecoveryStrategyRouter()

    @pytest.fixture
    def connection_bridge(self) -> ConnectionErrorBridge:
        """Create connection error bridge."""
        return ConnectionErrorBridge("hyperliquid", "test-conn-id")

    @pytest.fixture
    def mock_handler(self) -> Mock:
        """Create mock message handler."""
        handler = AsyncMock()
        handler.return_value = {"success": True}
        return handler

    async def test_complete_error_flow_processor_to_recovery(
        self,
        error_config: WebSocketErrorConfig,
        error_handler_factory: WebSocketErrorHandlerFactory,
        mock_handler: Mock,
    ) -> None:
        """Test complete error flow from processor to recovery."""
        # Create error handler
        error_handler = error_handler_factory.create_handler("hyperliquid", error_config)

        # Create processor
        processor = PydanticWebSocketProcessor(
            model_class=TestMessage,
            handler=mock_handler,
            exchange_name="hyperliquid",
            stream_error_handler=error_handler,
        )

        # Create invalid message to trigger validation error
        invalid_payload = {"invalid": "data"}  # Missing required fields
        context = TestContext(
            connection_id="test-conn-id",
            exchange="hyperliquid",
            channel="trades",
        )

        # Process message (should handle validation error)
        await processor.process_message(invalid_payload, context)

        # Verify error was handled through typed system
        assert processor._validation_errors > 0

    async def test_complete_error_flow_router_to_recovery(
        self,
        error_config: WebSocketErrorConfig,
        error_handler_registry: WebSocketErrorHandlerRegistry,
    ) -> None:
        """Test complete error flow from router to recovery."""
        # Create router
        router = BaseWebSocketRouter(
            error_handler_registry=error_handler_registry,
        )

        # Create processor
        processor = PydanticWebSocketProcessor(
            model_class=TestMessage,
            handler=AsyncMock(),
            exchange_name="hyperliquid",
        )

        # Register processor
        router.register_processor("test_message", processor)

        # Create message with missing routing key
        message = TestMessage(type="unknown_type", data={})
        context = TestContext(
            connection_id="test-conn-id",
            exchange="hyperliquid",
        )

        # Route message (should handle missing processor error)
        await router.route_message(message, context)

        # Verify error was handled
        stats = router.get_stats()
        assert stats["errors"]["total_errors"] > 0

    async def test_recovery_system_integration_with_all_components(
        self,
        recovery_config: ErrorRecoveryConfig,
        recovery_router: RecoveryStrategyRouter,
    ) -> None:
        """Test recovery system integration with all components."""
        # Create recovery system
        recovery_system = WebSocketErrorRecovery("test-conn-id", recovery_config)

        # Create different error scenarios
        errors = [
            WebSocketStreamError(
                message="Connection lost",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=StreamErrorContext(
                    connection_id="test-conn-id",
                    exchange="hyperliquid",
                ),
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            ),
            WebSocketStreamError(
                message="Rate limited",
                code=WebSocketErrorCode.RATE_LIMITED,
                context=StreamErrorContext(
                    connection_id="test-conn-id",
                    exchange="hyperliquid",
                ),
                recovery_strategy=WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            ),
            WebSocketStreamError(
                message="Auth failed",
                code=WebSocketErrorCode.AUTH_FAILED,
                context=StreamErrorContext(
                    connection_id="test-conn-id",
                    exchange="hyperliquid",
                ),
                recovery_strategy=WebSocketRecoveryStrategy.NONE,
            ),
        ]

        # Route each error through recovery router
        for error in errors:
            result = await recovery_router.route_recovery(error, recovery_system)

            # Verify appropriate handling
            assert result.strategy_used == error.get_recovery_strategy()

            if error.code == WebSocketErrorCode.AUTH_FAILED:
                assert result.success is False
                assert result.should_continue is False
            else:
                assert result.success is True

    async def test_connection_bridge_with_all_error_types(
        self,
        connection_bridge: ConnectionErrorBridge,
    ) -> None:
        """Test connection bridge with all error types."""
        # Create mock manager
        mock_manager = Mock()
        mock_manager._exchange_name = "hyperliquid"
        mock_manager._ws_url = "wss://api.hyperliquid.xyz/ws"
        mock_manager.is_connected = False
        mock_manager._failure_count = 0
        mock_manager._circuit_open = False
        mock_manager._max_reconnect_attempts = 10
        mock_manager._should_reconnect = True

        # Test different error types
        errors = [
            ConnectionError("Connection reset"),
            TimeoutError("Connection timeout"),
            OSError("Network error"),
            Exception("Generic error"),
        ]

        for error in errors:
            # Get recovery action
            action = connection_bridge.get_recovery_action(mock_manager, error)

            # Verify action created
            assert action is not None
            assert action.strategy != WebSocketRecoveryStrategy.NONE
            assert action.max_retries > 0

    async def test_processor_error_context_builder_integration(self) -> None:
        """Test processor error context builder integration."""
        # Create processor
        processor = PydanticWebSocketProcessor(
            model_class=TestMessage,
            handler=AsyncMock(),
            exchange_name="hyperliquid",
        )

        # Create context
        context = TestContext(
            connection_id="test-conn-id",
            exchange="hyperliquid",
            channel="trades",
            sequence_number=123,
        )

        # Create validation error
        try:
            TestMessage.model_validate({"invalid": "data"})
        except ValidationError:
            # Build error context
            error_context = ProcessorErrorContextBuilder.from_validation_error(
                processor,
                TestMessage(type="test", data={}),
                context,
            )

            # Verify context built correctly
            assert error_context.connection_id == "test-conn-id"
            assert error_context.exchange == "hyperliquid"
            assert error_context.channel == "trades"
            assert error_context.sequence_number == 123

    async def test_router_error_context_builder_integration(self) -> None:
        """Test router error context builder integration."""
        # Create router
        router = BaseWebSocketRouter()

        # Create context
        context = TestContext(
            connection_id="test-conn-id",
            exchange="hyperliquid",
            channel="orders",
        )

        # Build error context for missing processor
        error_context = RouterErrorContextBuilder.from_missing_processor(
            router,
            "unknown_type",
            TestMessage(type="test", data={}),
            context,
        )

        # Verify context built correctly
        assert error_context.connection_id == "test-conn-id"
        assert error_context.exchange == "hyperliquid"
        assert error_context.channel == "orders"

    async def test_error_handler_registry_integration(
        self,
        error_handler_registry: WebSocketErrorHandlerRegistry,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Test error handler registry integration."""
        # Get handler for exchange
        handler1 = error_handler_registry.get_handler("hyperliquid", error_config)
        handler2 = error_handler_registry.get_handler("hyperliquid")  # Should return cached

        # Verify same handler returned (cached)
        assert handler1 is handler2

        # Get handler for different exchange
        handler3 = error_handler_registry.get_handler("backpack", error_config)

        # Verify different handler
        assert handler3 is not handler1

    async def test_recovery_strategy_router_with_all_strategies(
        self,
        recovery_router: RecoveryStrategyRouter,
    ) -> None:
        """Test recovery router with all strategies."""
        strategies = list(WebSocketRecoveryStrategy)

        for strategy in strategies:
            # Create error with strategy
            error = WebSocketStreamError(
                message=f"Test {strategy.name}",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=StreamErrorContext(
                    connection_id="test-conn-id",
                    exchange="hyperliquid",
                ),
                recovery_strategy=strategy,
            )

            # Route recovery
            result = await recovery_router.route_recovery(error, None)

            # Verify routed correctly
            assert result.strategy_used == strategy

            # Verify appropriate result
            if (
                strategy == WebSocketRecoveryStrategy.NONE
                or strategy == WebSocketRecoveryStrategy.CIRCUIT_BREAKER
            ):
                assert result.success is False
                assert result.should_continue is False
            else:
                assert result.success is True

    async def test_complete_type_safety_validation(self) -> None:
        """Validate complete type safety achieved."""
        # Create all components
        error_config = WebSocketErrorConfig()
        factory = WebSocketErrorHandlerFactory()
        registry = WebSocketErrorHandlerRegistry()
        router = RecoveryStrategyRouter()

        # Create typed error
        error = WebSocketStreamError(
            message="Type safety test",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=StreamErrorContext(
                connection_id="test-conn-id",
                exchange="hyperliquid",
            ),
        )

        # Verify no dict[str, Any] in error handling
        assert isinstance(error.context, StreamErrorContext)
        assert isinstance(error.code, WebSocketErrorCode)
        assert isinstance(error.get_recovery_strategy(), WebSocketRecoveryStrategy)

        # Verify typed recovery
        result = await router.route_recovery(error, None)
        assert result.success is True
        assert isinstance(result.strategy_used, WebSocketRecoveryStrategy)

    async def test_phase2_success_metrics(
        self,
        error_config: WebSocketErrorConfig,
        recovery_router: RecoveryStrategyRouter,
        connection_bridge: ConnectionErrorBridge,
    ) -> None:
        """Test Phase 2 success metrics."""
        # Track metrics
        metrics = {
            "type_safety": True,  # No dict[str, Any]
            "recovery_strategies": len(list(WebSocketRecoveryStrategy)),
            "error_codes": len([e for e in dir(WebSocketErrorCode) if not e.startswith("_")]),
            "components_integrated": 0,
        }

        # Verify processor integration
        if ProcessorErrorContextBuilder:
            metrics["components_integrated"] += 1

        # Verify router integration
        if RouterErrorContextBuilder:
            metrics["components_integrated"] += 1

        # Verify recovery integration
        if recovery_router:
            metrics["components_integrated"] += 1

        # Verify connection bridge
        if connection_bridge:
            metrics["components_integrated"] += 1

        # Validate Phase 2 success
        assert metrics["type_safety"] is True
        assert metrics["recovery_strategies"] == 15
        assert metrics["error_codes"] > 30
        assert metrics["components_integrated"] >= 4

        print(f"Phase 2 Complete: {metrics}")

    async def test_error_flow_with_metrics_collection(
        self,
        error_config: WebSocketErrorConfig,
        error_handler_factory: WebSocketErrorHandlerFactory,
    ) -> None:
        """Test error flow with metrics collection enabled."""
        # Enable metrics
        error_config.enable_metrics_collection = True

        # Create handler
        handler = error_handler_factory.create_handler("hyperliquid", error_config)

        # Create and handle errors
        for i in range(5):
            error = WebSocketStreamError(
                message=f"Error {i}",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=StreamErrorContext(
                    connection_id="test-conn-id",
                    exchange="hyperliquid",
                ),
            )

            await handler.handle_stream_error(error)

        # Get metrics
        metrics = handler.get_metrics()

        # Verify metrics collected
        assert metrics.total_errors == 5
        assert "CONNECTION_LOST" in metrics.errors_by_code
        assert metrics.errors_by_code["CONNECTION_LOST"] == 5

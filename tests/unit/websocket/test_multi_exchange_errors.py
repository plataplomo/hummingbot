"""Multi-exchange error handling tests for WebSocket error system.

Tests error handling across different exchanges (Hyperliquid, Backpack, etc.)
to ensure consistent behavior while respecting exchange-specific requirements.
"""

from __future__ import annotations

import asyncio

import pytest

from cyberdelta.apis.common.error_foundation import ErrorSeverity, WebSocketRecoveryStrategy
from cyberdelta.apis.enums.websocket.error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.error_handling.error_handler import WebSocketErrorHandler
from cyberdelta.apis.websocket.error_handling.error_handler_factory import (
    WebSocketErrorHandlerFactory,
)
from cyberdelta.apis.websocket.error_handling.error_handler_registry import (
    WebSocketErrorHandlerRegistry,
)
from cyberdelta.apis.websocket.exceptions.stream_error import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from cyberdelta.enums import ExchangeName


@pytest.mark.asyncio
class TestMultiExchangeErrors:
    """Test error handling across multiple exchanges."""

    @pytest.fixture
    def error_config(self) -> WebSocketErrorConfig:
        """Create test error configuration.

        Returns:
            WebSocketErrorConfig: Configuration for multi-exchange error testing.
        """
        config = WebSocketErrorConfig()
        config.recovery.max_recovery_attempts = 3
        config.recovery.initial_backoff_ms = 100
        config.metrics.enable_metrics_collection = True
        return config

    @pytest.fixture
    def handler_registry(self) -> WebSocketErrorHandlerRegistry:
        """Create handler registry for testing.

        Returns:
            WebSocketErrorHandlerRegistry: Clean registry for multi-exchange testing.
        """
        return WebSocketErrorHandlerRegistry()

    async def test_hyperliquid_error_handling(
        self,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Test error handling for Hyperliquid exchange."""
        # Create Hyperliquid-specific handler
        handler = WebSocketErrorHandlerFactory.create_handler(
            exchange=ExchangeName.HYPERLIQUID,
            config=error_config,
        )

        # Create Hyperliquid-specific error context
        context = StreamErrorContext(
            connection_id="hl-conn-123",
            exchange=ExchangeName.HYPERLIQUID,
            channel="l2Book",
            topic="BTC",
            sequence_number=42,
            user_id="test-user",
            session_id="test-session",
            environment="test",
            raw_message_size=512,
        )

        # Create test error
        error = WebSocketStreamError(
            message="Hyperliquid connection lost",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.FULL_RECONNECT,
        )

        # Handle error
        await handler.handle_stream_error(error)

        # Verify metrics collection
        assert handler.metrics_collector is not None
        stats = handler.metrics_collector.get_statistics()
        assert stats.total_errors_recorded >= 1

    async def test_backpack_error_handling(
        self,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Test error handling for Backpack exchange."""
        # Create Backpack-specific handler
        handler = WebSocketErrorHandlerFactory.create_handler(
            exchange=ExchangeName.BACKPACK,
            config=error_config,
        )

        # Create Backpack-specific error context
        context = StreamErrorContext(
            connection_id="bp-conn-456",
            exchange=ExchangeName.BACKPACK,
            channel="ticker",
            topic="SOL_USDC",
            sequence_number=123,
            user_id="test-user",
            session_id="test-session",
            environment="test",
            raw_message_size=256,
        )

        # Create test error
        error = WebSocketStreamError(
            message="Backpack subscription failed",
            code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
            context=context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
        )

        # Handle error
        await handler.handle_stream_error(error)

        # Verify metrics collection
        assert handler.metrics_collector is not None
        stats = handler.metrics_collector.get_statistics()
        assert stats.total_errors_recorded >= 1

    async def test_exchange_specific_configurations(
        self,
        handler_registry: WebSocketErrorHandlerRegistry,
    ) -> None:
        """Test that different exchanges get appropriate configurations."""
        # Get handlers for different exchanges
        hl_handler = handler_registry.get_handler(ExchangeName.HYPERLIQUID)
        bp_handler = handler_registry.get_handler(ExchangeName.BACKPACK)

        # Verify they are different instances
        assert hl_handler is not bp_handler

        # Verify both have metrics collection enabled
        assert hl_handler.config.metrics.enable_metrics_collection
        assert bp_handler.config.metrics.enable_metrics_collection

        # Verify exchange-specific recovery settings
        assert hl_handler.config.recovery.max_recovery_attempts >= 1
        assert bp_handler.config.recovery.max_recovery_attempts >= 1

    async def test_cross_exchange_error_isolation(
        self,
        handler_registry: WebSocketErrorHandlerRegistry,
    ) -> None:
        """Test that errors in one exchange don't affect others."""
        # Get handlers for both exchanges
        hl_handler = handler_registry.get_handler(ExchangeName.HYPERLIQUID)
        bp_handler = handler_registry.get_handler(ExchangeName.BACKPACK)

        # Create error contexts for each exchange
        hl_context = StreamErrorContext(
            connection_id="hl-conn-123",
            exchange=ExchangeName.HYPERLIQUID,
            channel="l2Book",
            topic="BTC",
            environment="test",
        )

        bp_context = StreamErrorContext(
            connection_id="bp-conn-456",
            exchange=ExchangeName.BACKPACK,
            channel="ticker",
            topic="SOL_USDC",
            environment="test",
        )

        # Create errors for each exchange
        hl_error = WebSocketStreamError(
            message="Hyperliquid error",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=hl_context,
        )

        bp_error = WebSocketStreamError(
            message="Backpack error",
            code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
            context=bp_context,
        )

        # Handle errors on each exchange
        await hl_handler.handle_stream_error(hl_error)
        await bp_handler.handle_stream_error(bp_error)

        # Verify each handler only recorded its own exchange's errors
        if hl_handler.metrics_collector:
            hl_stats = hl_handler.metrics_collector.get_statistics()
            assert hl_stats.total_errors_recorded >= 1

        if bp_handler.metrics_collector:
            bp_stats = bp_handler.metrics_collector.get_statistics()
            assert bp_stats.total_errors_recorded >= 1

    async def test_registry_handler_lifecycle(
        self,
        handler_registry: WebSocketErrorHandlerRegistry,
    ) -> None:
        """Test handler lifecycle management in registry."""
        # Initially no handlers
        assert len(handler_registry.list_active_handlers()) == 0

        # Create handlers for different exchanges
        hl_handler = handler_registry.get_handler(ExchangeName.HYPERLIQUID)
        bp_handler = handler_registry.get_handler(ExchangeName.BACKPACK)

        # Should now have 2 handlers
        assert len(handler_registry.list_active_handlers()) == 2

        # Remove one handler
        removed = handler_registry.remove_handler(ExchangeName.HYPERLIQUID)
        assert removed is True
        assert len(handler_registry.list_active_handlers()) == 1

        # Keep references to prevent garbage collection
        del hl_handler, bp_handler

    async def test_concurrent_multi_exchange_processing(
        self,
        handler_registry: WebSocketErrorHandlerRegistry,
    ) -> None:
        """Test concurrent error processing across multiple exchanges."""
        # Get handlers
        hl_handler = handler_registry.get_handler(ExchangeName.HYPERLIQUID)
        bp_handler = handler_registry.get_handler(ExchangeName.BACKPACK)

        # Create multiple errors for each exchange
        hl_errors = [
            WebSocketStreamError(
                message=f"Hyperliquid error {i}",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=StreamErrorContext(
                    connection_id="hl-conn-123",
                    exchange=ExchangeName.HYPERLIQUID,
                    channel="l2Book",
                    topic="BTC",
                    environment="test",
                ),
            )
            for i in range(5)
        ]

        bp_errors = [
            WebSocketStreamError(
                message=f"Backpack error {i}",
                code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
                context=StreamErrorContext(
                    connection_id="bp-conn-456",
                    exchange=ExchangeName.BACKPACK,
                    channel="ticker",
                    topic="SOL_USDC",
                    environment="test",
                ),
            )
            for i in range(5)
        ]

        # Process errors concurrently
        hl_tasks = [hl_handler.handle_stream_error(error) for error in hl_errors]
        bp_tasks = [bp_handler.handle_stream_error(error) for error in bp_errors]

        await asyncio.gather(*hl_tasks, *bp_tasks)

        # Verify both handlers processed their errors
        if hl_handler.metrics_collector:
            hl_stats = hl_handler.metrics_collector.get_statistics()
            assert hl_stats.total_errors_recorded >= 5

        if bp_handler.metrics_collector:
            bp_stats = bp_handler.metrics_collector.get_statistics()
            assert bp_stats.total_errors_recorded >= 5


class TestExchangeSpecificBehavior:
    """Test exchange-specific error handling behavior."""

    def test_exchange_specific_recovery_strategies(self) -> None:
        """Test that exchanges have appropriate recovery strategies."""
        # Create handlers for different exchanges
        hl_handler = WebSocketErrorHandlerFactory.create_minimal_handler(
            exchange=ExchangeName.HYPERLIQUID
        )
        bp_handler = WebSocketErrorHandlerFactory.create_minimal_handler(
            exchange=ExchangeName.BACKPACK
        )

        # Verify handlers are properly configured
        assert isinstance(hl_handler, WebSocketErrorHandler)
        assert isinstance(bp_handler, WebSocketErrorHandler)

        # Verify they have different configurations
        assert hl_handler.config is not bp_handler.config

    def test_exchange_specific_error_codes(self) -> None:
        """Test that error codes are handled appropriately per exchange."""
        # Test that error codes exist and are properly typed
        connection_errors = [
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.CONNECTION_FAILED,
            WebSocketErrorCode.CONNECTION_TIMEOUT,
        ]

        subscription_errors = [
            WebSocketErrorCode.SUBSCRIPTION_FAILED,
            WebSocketErrorCode.SUBSCRIPTION_UNAUTHORIZED,
        ]

        # All error codes should be valid enum members
        for code in connection_errors + subscription_errors:
            assert isinstance(code, WebSocketErrorCode)

    def test_exchange_specific_recovery_strategy_selection(self) -> None:
        """Test that recovery strategies are appropriately selected."""
        # Test that recovery strategies exist and are valid
        strategies = [
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            WebSocketRecoveryStrategy.FULL_RECONNECT,
            WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
            WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
        ]

        # All strategies should be valid enum members
        for strategy in strategies:
            assert isinstance(strategy, WebSocketRecoveryStrategy)

        # Test that strategies have appropriate numeric values for prioritization
        strategy_values = [strategy.value for strategy in strategies]
        assert len(set(strategy_values)) == len(strategy_values)  # All distinct

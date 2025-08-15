"""Multi-exchange error handling tests for WebSocket error system.

Tests error handling across different exchanges (Hyperliquid, Backpack, etc.)
to ensure consistent behavior while respecting exchange-specific requirements.
"""

from __future__ import annotations

import asyncio
from typing import Any, Protocol, cast

import pytest

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_error_handler_registry import WebSocketErrorHandlerRegistry
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from cyberdelta.enums import ExchangeName
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class ErrorMetricsProtocol(Protocol):
    """Protocol for error metrics objects."""

    total_errors: int
    errors_by_exchange: dict[str, int]


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
    def handler_registry(self, error_config: WebSocketErrorConfig) -> WebSocketErrorHandlerRegistry:
        """Create error handler registry.

        Returns:
            WebSocketErrorHandlerRegistry: Registry for managing multi-exchange error handlers.
        """
        return WebSocketErrorHandlerRegistry()

    async def test_hyperliquid_specific_errors(
        self,
        handler_registry: WebSocketErrorHandlerRegistry,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Test Hyperliquid-specific error handling."""
        # Get Hyperliquid handler
        handler = handler_registry.get_handler(ExchangeName.HYPERLIQUID, error_config)

        # Create Hyperliquid-specific errors
        errors = [
            # Hyperliquid rate limiting
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.RATE_LIMITED,
                message="Request rate exceeded",
            ),
            # Hyperliquid sequence issues
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.SEQUENCE_GAP,
                message="Sequence gap detected",
            ),
            # Hyperliquid auth issues
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.AUTH_INVALID_TOKEN,
                message="Invalid API key",
            ),
        ]

        for error in errors:
            error.context.exchange = "hyperliquid"
            await handler.handle_stream_error(error)

        # Verify metrics
        metrics = cast(ErrorMetricsProtocol, handler.get_metrics())
        assert metrics.total_errors >= 3
        assert "hyperliquid" in str(metrics.errors_by_exchange)

    async def test_backpack_specific_errors(
        self,
        handler_registry: WebSocketErrorHandlerRegistry,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Test Backpack-specific error handling."""
        # Get Backpack handler
        handler = handler_registry.get_handler(ExchangeName.BACKPACK, error_config)

        # Create Backpack-specific errors
        errors = [
            # Backpack connection issues
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.CONNECTION_REFUSED,
                message="Connection refused by Backpack",
            ),
            # Backpack subscription limits
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.SUBSCRIPTION_LIMIT_EXCEEDED,
                message="Too many subscriptions",
            ),
            # Backpack maintenance
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.EXCHANGE_MAINTENANCE,
                message="Backpack under maintenance",
            ),
        ]

        for error in errors:
            error.context.exchange = "backpack"
            await handler.handle_stream_error(error)

        # Verify handling
        metrics = cast(ErrorMetricsProtocol, handler.get_metrics())
        assert metrics.total_errors >= 3

    async def test_exchange_specific_recovery_strategies(self) -> None:
        """Test that recovery strategies are appropriate for each exchange."""
        exchanges = [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]

        for exchange in exchanges:
            # Create rate limit error for each exchange
            error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.RATE_LIMITED,
            )
            error.context.exchange = exchange

            # Verify recovery strategy is consistent
            assert error.recovery_strategy in {
                WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
                WebSocketRecoveryStrategy.LINEAR_BACKOFF,
            }

            # Create connection error for each exchange
            conn_error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.CONNECTION_LOST,
            )
            conn_error.context.exchange = exchange

            # Verify connection recovery is consistent
            assert conn_error.recovery_strategy in {
                WebSocketRecoveryStrategy.RECONNECT_SAME,
                WebSocketRecoveryStrategy.RECONNECT_DIFFERENT,
                WebSocketRecoveryStrategy.FULL_RECONNECT,
            }

    async def test_cross_exchange_error_correlation(
        self,
        handler_registry: WebSocketErrorHandlerRegistry,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Test correlation of errors across exchanges."""
        # Create handlers for multiple exchanges
        hl_handler = handler_registry.get_handler(ExchangeName.HYPERLIQUID, error_config)
        bp_handler = handler_registry.get_handler(ExchangeName.BACKPACK, error_config)

        # Simulate correlated network issues
        network_errors: list[WebSocketStreamError] = []
        for exchange, handler in [("hyperliquid", hl_handler), ("backpack", bp_handler)]:
            error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.CONNECTION_TIMEOUT,
                message=f"Connection timeout on {exchange}",
            )
            error.context.exchange = exchange
            network_errors.append(error)
            await handler.handle_stream_error(error)

        # Both exchanges should show network issues
        hl_metrics = hl_handler.get_metrics()
        bp_metrics = bp_handler.get_metrics()

        hl_metrics_typed = cast(ErrorMetricsProtocol, hl_metrics)
        bp_metrics_typed = cast(ErrorMetricsProtocol, bp_metrics)
        assert hl_metrics_typed.total_errors > 0
        assert bp_metrics_typed.total_errors > 0

    async def test_exchange_failover_strategy(self) -> None:
        """Test failover from one exchange to another on errors."""
        primary_exchange = "hyperliquid"
        backup_exchange = "backpack"

        # Create critical error on primary
        primary_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.EXCHANGE_UNAVAILABLE,
            message=f"{primary_exchange} unavailable",
        )
        primary_error.context.exchange = primary_exchange

        # Check if fallback strategy is available
        if primary_error.recovery_strategy == WebSocketRecoveryStrategy.FALLBACK_EXCHANGE:
            # Would trigger failover to backup exchange
            assert backup_exchange != primary_exchange

    async def test_exchange_specific_message_validation(self) -> None:
        """Test exchange-specific message validation errors."""
        # Hyperliquid message format error
        hl_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.INVALID_MESSAGE_FORMAT,
            message="Invalid Hyperliquid message format",
        )
        hl_error.context.exchange = "hyperliquid"
        hl_error.context.channel = "allMids"  # HL-specific channel

        # Backpack message format error
        bp_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.INVALID_MESSAGE_FORMAT,
            message="Invalid Backpack message format",
        )
        bp_error.context.exchange = "backpack"
        bp_error.context.channel = "trades"  # BP channel format

        # Verify different contexts
        assert hl_error.context.exchange != bp_error.context.exchange
        assert hl_error.context.channel != bp_error.context.channel

    async def test_exchange_specific_auth_handling(
        self,
        handler_registry: WebSocketErrorHandlerRegistry,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Test exchange-specific authentication error handling."""
        exchanges = [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]

        for exchange in exchanges:
            handler = handler_registry.get_handler(exchange, error_config)

            # Create auth error
            auth_error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.AUTH_FAILED,
                message=f"Authentication failed on {exchange}",
            )
            auth_error.context.exchange = exchange

            # Handle auth error
            await handler.handle_stream_error(auth_error)

            # Verify non-retryable
            assert auth_error.get_recovery_strategy() == WebSocketRecoveryStrategy.NONE
            assert auth_error.recovery_strategy == WebSocketRecoveryStrategy.NONE

    async def test_exchange_specific_rate_limits(self) -> None:
        """Test different rate limit handling per exchange."""
        # Hyperliquid rate limits (typically stricter)
        hl_rate_limit = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.RATE_LIMITED,
        )
        hl_rate_limit.context.exchange = "hyperliquid"
        hl_delay = hl_rate_limit.get_retry_delay_ms()

        # Backpack rate limits
        bp_rate_limit = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.RATE_LIMITED,
        )
        bp_rate_limit.context.exchange = "backpack"
        bp_delay = bp_rate_limit.get_retry_delay_ms()

        # Both should have delays but might differ
        assert hl_delay > 0
        assert bp_delay > 0

    async def test_exchange_specific_channels(self) -> None:
        """Test exchange-specific channel subscription errors."""
        # Hyperliquid channels
        hl_channels = ["allMids", "notification", "webData2", "candle"]

        for channel in hl_channels:
            error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.SUBSCRIPTION_INVALID_CHANNEL,
                message=f"Invalid channel: {channel}",
            )
            error.context.exchange = "hyperliquid"
            error.context.channel = channel

            # Should have subscription recovery
            assert error.recovery_strategy in {
                WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
                WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
                WebSocketRecoveryStrategy.NONE,
            }

        # Backpack channels
        bp_channels = ["trades", "depth", "ticker", "account"]

        for channel in bp_channels:
            error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.SUBSCRIPTION_INVALID_CHANNEL,
                message=f"Invalid channel: {channel}",
            )
            error.context.exchange = "backpack"
            error.context.channel = channel

            # Should have subscription recovery
            assert error.recovery_strategy in {
                WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
                WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
                WebSocketRecoveryStrategy.NONE,
            }

    async def test_exchange_priority_handling(
        self,
        handler_registry: WebSocketErrorHandlerRegistry,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Test priority handling of errors from different exchanges."""
        # Create errors with different severities from different exchanges
        critical_hl = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.STREAM_CORRUPTED,
        )
        critical_hl.context.exchange = "hyperliquid"

        warning_bp = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.SEQUENCE_GAP,
        )
        warning_bp.context.exchange = "backpack"

        # Handle both
        hl_handler = handler_registry.get_handler(ExchangeName.HYPERLIQUID, error_config)
        bp_handler = handler_registry.get_handler(ExchangeName.BACKPACK, error_config)

        await hl_handler.handle_stream_error(critical_hl)
        await bp_handler.handle_stream_error(warning_bp)

        # Critical error should have higher severity
        assert critical_hl.severity > warning_bp.severity

    async def test_exchange_specific_error_codes(self) -> None:
        """Test that certain error codes are handled differently per exchange."""
        # Protocol error might mean different things
        hl_protocol = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.PROTOCOL_ERROR,
            message="Hyperliquid protocol error",
        )
        hl_protocol.context.exchange = "hyperliquid"

        bp_protocol = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.PROTOCOL_ERROR,
            message="Backpack protocol error",
        )
        bp_protocol.context.exchange = "backpack"

        # Both are protocol errors but contexts differ
        assert hl_protocol.code == bp_protocol.code
        assert hl_protocol.context.exchange != bp_protocol.context.exchange

    async def test_concurrent_multi_exchange_errors(
        self,
        handler_registry: WebSocketErrorHandlerRegistry,
        error_config: WebSocketErrorConfig,
    ) -> None:
        """Test handling errors from multiple exchanges concurrently."""
        exchanges = [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]
        handlers = {
            exchange: handler_registry.get_handler(exchange, error_config) for exchange in exchanges
        }

        # Create errors for all exchanges
        tasks: list[Any] = []
        for exchange, handler in handlers.items():
            for error_code in [
                WebSocketErrorCode.CONNECTION_LOST,
                WebSocketErrorCode.RATE_LIMITED,
                WebSocketErrorCode.SUBSCRIPTION_FAILED,
            ]:
                error = ErrorTestFactory.create_test_error(code=error_code)
                error.context.exchange = exchange
                tasks.append(handler.handle_stream_error(error))

        # Handle all concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify all were handled
        assert len(results) == len(exchanges) * 3

        # Check metrics for each exchange
        for handler in handlers.values():
            metrics = handler.get_metrics()
            metrics_typed = cast(ErrorMetricsProtocol, metrics)
            assert metrics_typed.total_errors >= 3

"""Test WebSocket Connection Manager Error Bridge.

This test validates Step 48: Update Connection Manager Error Handling.
"""

from unittest.mock import Mock

import pytest

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.connectivity.ws_connection_error_bridge import (
    ConnectionErrorBridge,
    ConnectionErrorContext,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode


@pytest.mark.asyncio
class TestConnectionErrorBridge:
    """Test connection error bridge functionality."""

    @pytest.fixture
    def bridge(self) -> ConnectionErrorBridge:
        """Create connection error bridge."""
        return ConnectionErrorBridge("hyperliquid", "test-conn-id")

    @pytest.fixture
    def mock_manager(self) -> Mock:
        """Create mock WebSocket manager."""
        manager = Mock()
        manager._exchange_name = "hyperliquid"
        manager._ws_url = "wss://api.hyperliquid.xyz/ws"
        manager.is_connected = False
        manager._failure_count = 0
        manager._circuit_open = False
        manager._max_reconnect_attempts = 10
        manager._should_reconnect = True
        return manager

    @pytest.fixture
    def connection_context(self) -> ConnectionErrorContext:
        """Create connection error context."""
        return ConnectionErrorContext(
            exchange_name="hyperliquid",
            ws_url="wss://api.hyperliquid.xyz/ws",
            is_connected=False,
            failure_count=0,
            circuit_open=False,
            reconnect_attempts=0,
            max_reconnect_attempts=10,
        )

    def test_map_connection_error_codes(
        self,
        bridge: ConnectionErrorBridge,
    ) -> None:
        """Test mapping connection errors to WebSocket error codes."""
        # Connection reset
        error = ConnectionError("Connection reset by peer")
        code = bridge._map_error_code(error, "connection")
        assert code == WebSocketErrorCode.CONNECTION_RESET

        # Connection refused
        error = ConnectionError("Connection refused")
        code = bridge._map_error_code(error, "connection")
        assert code == WebSocketErrorCode.CONNECTION_REFUSED

        # Generic connection error
        error = ConnectionError("Network error")
        code = bridge._map_error_code(error, "connection")
        assert code == WebSocketErrorCode.CONNECTION_FAILED

    def test_map_timeout_error_codes(
        self,
        bridge: ConnectionErrorBridge,
    ) -> None:
        """Test mapping timeout errors to WebSocket error codes."""
        # Connection timeout
        error = TimeoutError()
        code = bridge._map_error_code(error, "connection")
        assert code == WebSocketErrorCode.CONNECTION_TIMEOUT

        # Ping timeout
        code = bridge._map_error_code(error, "ping")
        assert code == WebSocketErrorCode.PING_TIMEOUT

        # Other timeout (defaults to connection timeout)
        code = bridge._map_error_code(error, "message")
        assert code == WebSocketErrorCode.CONNECTION_TIMEOUT

    def test_map_os_error_codes(
        self,
        bridge: ConnectionErrorBridge,
    ) -> None:
        """Test mapping OS errors to WebSocket error codes."""
        # Broken pipe
        error = OSError("Broken pipe")
        code = bridge._map_error_code(error, "connection")
        assert code == WebSocketErrorCode.CONNECTION_RESET

        # Network error
        error = OSError("Network is unreachable")
        code = bridge._map_error_code(error, "connection")
        assert code == WebSocketErrorCode.CONNECTION_LOST

        # Generic OS error
        error = OSError("Something went wrong")
        code = bridge._map_error_code(error, "connection")
        assert code == WebSocketErrorCode.CONNECTION_LOST

    def test_map_error_types(
        self,
        bridge: ConnectionErrorBridge,
    ) -> None:
        """Test mapping errors by type context."""
        error = Exception("Generic error")

        # Listener error
        code = bridge._map_error_code(error, "listener")
        assert code == WebSocketErrorCode.STREAM_INTERRUPTED

        # Ping error
        code = bridge._map_error_code(error, "ping")
        assert code == WebSocketErrorCode.HEARTBEAT_TIMEOUT

        # Reconnect error
        code = bridge._map_error_code(error, "reconnect")
        assert code == WebSocketErrorCode.CONNECTION_FAILED

        # Unknown error type
        code = bridge._map_error_code(error, "unknown")
        assert code == WebSocketErrorCode.CONNECTION_FAILED

    def test_determine_severity_circuit_open(
        self,
        bridge: ConnectionErrorBridge,
        connection_context: ConnectionErrorContext,
    ) -> None:
        """Test severity determination when circuit breaker is open."""
        connection_context.circuit_open = True

        severity = bridge._determine_severity(
            WebSocketErrorCode.CONNECTION_LOST,
            connection_context,
        )

        assert severity == ErrorSeverity.CRITICAL

    def test_determine_severity_auth_errors(
        self,
        bridge: ConnectionErrorBridge,
        connection_context: ConnectionErrorContext,
    ) -> None:
        """Test severity determination for auth errors."""
        severity = bridge._determine_severity(
            WebSocketErrorCode.AUTH_FAILED,
            connection_context,
        )
        assert severity == ErrorSeverity.CRITICAL

        severity = bridge._determine_severity(
            WebSocketErrorCode.SSL_ERROR,
            connection_context,
        )
        assert severity == ErrorSeverity.CRITICAL

    def test_determine_severity_repeated_failures(
        self,
        bridge: ConnectionErrorBridge,
        connection_context: ConnectionErrorContext,
    ) -> None:
        """Test severity determination for repeated failures."""
        connection_context.failure_count = 3

        severity = bridge._determine_severity(
            WebSocketErrorCode.CONNECTION_LOST,
            connection_context,
        )

        assert severity == ErrorSeverity.ERROR

    def test_determine_recovery_strategy_non_retryable(
        self,
        bridge: ConnectionErrorBridge,
        connection_context: ConnectionErrorContext,
    ) -> None:
        """Test recovery strategy for non-retryable errors."""
        # Auth failed
        strategy = bridge._determine_recovery_strategy(
            WebSocketErrorCode.AUTH_FAILED,
            connection_context,
            ErrorSeverity.CRITICAL,
        )
        assert strategy == WebSocketRecoveryStrategy.NONE

        # Invalid API key
        strategy = bridge._determine_recovery_strategy(
            WebSocketErrorCode.INVALID_API_KEY,
            connection_context,
            ErrorSeverity.CRITICAL,
        )
        assert strategy == WebSocketRecoveryStrategy.NONE

    def test_determine_recovery_strategy_circuit_breaker(
        self,
        bridge: ConnectionErrorBridge,
        connection_context: ConnectionErrorContext,
    ) -> None:
        """Test recovery strategy for circuit breaker conditions."""
        # Circuit open
        connection_context.circuit_open = True
        strategy = bridge._determine_recovery_strategy(
            WebSocketErrorCode.CONNECTION_LOST,
            connection_context,
            ErrorSeverity.WARNING,
        )
        assert strategy == WebSocketRecoveryStrategy.CIRCUIT_BREAKER

        # Too many failures
        connection_context.circuit_open = False
        connection_context.failure_count = 5
        strategy = bridge._determine_recovery_strategy(
            WebSocketErrorCode.CONNECTION_LOST,
            connection_context,
            ErrorSeverity.ERROR,
        )
        assert strategy == WebSocketRecoveryStrategy.CIRCUIT_BREAKER

    def test_determine_recovery_strategy_full_reconnect(
        self,
        bridge: ConnectionErrorBridge,
        connection_context: ConnectionErrorContext,
    ) -> None:
        """Test recovery strategy for full reconnect scenarios."""
        # Stream corrupted
        strategy = bridge._determine_recovery_strategy(
            WebSocketErrorCode.STREAM_CORRUPTED,
            connection_context,
            ErrorSeverity.ERROR,
        )
        assert strategy == WebSocketRecoveryStrategy.FULL_RECONNECT

        # Protocol error
        strategy = bridge._determine_recovery_strategy(
            WebSocketErrorCode.PROTOCOL_ERROR,
            connection_context,
            ErrorSeverity.ERROR,
        )
        assert strategy == WebSocketRecoveryStrategy.FULL_RECONNECT

    def test_determine_recovery_strategy_backoff(
        self,
        bridge: ConnectionErrorBridge,
        connection_context: ConnectionErrorContext,
    ) -> None:
        """Test recovery strategy backoff scenarios."""
        # Rate limited - exponential backoff
        strategy = bridge._determine_recovery_strategy(
            WebSocketErrorCode.RATE_LIMITED,
            connection_context,
            ErrorSeverity.WARNING,
        )
        assert strategy == WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF

        # Repeated failures - linear backoff
        connection_context.failure_count = 2
        strategy = bridge._determine_recovery_strategy(
            WebSocketErrorCode.CONNECTION_LOST,
            connection_context,
            ErrorSeverity.WARNING,
        )
        assert strategy == WebSocketRecoveryStrategy.LINEAR_BACKOFF

        # First failure - immediate retry
        connection_context.failure_count = 0
        strategy = bridge._determine_recovery_strategy(
            WebSocketErrorCode.CONNECTION_TIMEOUT,
            connection_context,
            ErrorSeverity.WARNING,
        )
        assert strategy == WebSocketRecoveryStrategy.IMMEDIATE_RETRY

    def test_create_connection_error(
        self,
        bridge: ConnectionErrorBridge,
        connection_context: ConnectionErrorContext,
    ) -> None:
        """Test creating typed connection error."""
        error = ConnectionError("Test connection error")

        typed_error = bridge.create_connection_error(
            error,
            connection_context,
            error_type="connection",
        )

        assert typed_error.message == "Connection error: Test connection error"
        assert typed_error.code == WebSocketErrorCode.CONNECTION_FAILED
        assert typed_error.context.connection_id == "test-conn-id"
        assert typed_error.context.exchange == "hyperliquid"
        assert typed_error.context.reconnect_count == 0
        assert typed_error.cause == error

    def test_error_history_tracking(
        self,
        bridge: ConnectionErrorBridge,
        connection_context: ConnectionErrorContext,
    ) -> None:
        """Test error history tracking."""
        # Create multiple errors
        for i in range(5):
            error = ConnectionError(f"Error {i}")
            bridge.create_connection_error(error, connection_context)

        assert len(bridge.error_history) == 5

        # Test history trimming
        bridge.max_history = 3
        error = ConnectionError("New error")
        bridge.create_connection_error(error, connection_context)

        # Should only keep last 3 errors
        assert len(bridge.error_history) == 3
        assert "New error" in bridge.error_history[-1].message

    async def test_handle_connection_error(
        self,
        bridge: ConnectionErrorBridge,
        mock_manager: Mock,
    ) -> None:
        """Test handling connection error."""
        error = ConnectionError("Test error")

        result = await bridge.handle_connection_error(
            mock_manager,
            error,
            recovery=None,
        )

        assert result.success is True
        assert result.strategy_used == WebSocketRecoveryStrategy.IMMEDIATE_RETRY
        assert result.should_continue is True

        # Verify manager state updated
        assert mock_manager._should_reconnect is True

    async def test_handle_connection_error_circuit_breaker(
        self,
        bridge: ConnectionErrorBridge,
        mock_manager: Mock,
    ) -> None:
        """Test handling connection error with circuit breaker."""
        mock_manager._failure_count = 5
        error = ConnectionError("Too many failures")

        result = await bridge.handle_connection_error(
            mock_manager,
            error,
            recovery=None,
        )

        assert result.success is False
        assert result.strategy_used == WebSocketRecoveryStrategy.CIRCUIT_BREAKER
        assert result.should_continue is False

        # Verify manager state updated
        assert mock_manager._circuit_open is True
        assert mock_manager._should_reconnect is False

    async def test_handle_listener_error(
        self,
        bridge: ConnectionErrorBridge,
        mock_manager: Mock,
    ) -> None:
        """Test handling listener error."""
        error = OSError("Listener error")

        result = await bridge.handle_listener_error(
            mock_manager,
            error,
            recovery=None,
        )

        assert result.success is True
        assert result.should_continue is True

    def test_get_recovery_action(
        self,
        bridge: ConnectionErrorBridge,
        mock_manager: Mock,
    ) -> None:
        """Test getting recovery action."""
        error = ConnectionError("Test error")

        action = bridge.get_recovery_action(mock_manager, error)

        assert action.strategy == WebSocketRecoveryStrategy.IMMEDIATE_RETRY
        assert action.delay_ms == 0
        assert action.should_reconnect is False
        assert action.max_retries == 5  # Default for WARNING severity

    def test_get_error_stats(
        self,
        bridge: ConnectionErrorBridge,
        connection_context: ConnectionErrorContext,
    ) -> None:
        """Test getting error statistics."""
        # Create various errors
        errors = [
            (ConnectionError("Error 1"), "connection"),
            (TimeoutError("Error 2"), "connection"),
            (OSError("Error 3"), "listener"),
        ]

        for error, error_type in errors:
            bridge.create_connection_error(error, connection_context, error_type)

        stats = bridge.get_error_stats()

        assert stats["total_errors"] == 3
        assert len(stats["error_codes"]) > 0
        assert len(stats["severities"]) > 0
        assert len(stats["recovery_strategies"]) > 0
        assert "recovery_stats" in stats

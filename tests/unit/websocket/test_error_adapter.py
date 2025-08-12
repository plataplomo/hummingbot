"""Comprehensive compatibility adapter tests for WebSocket error system.

Tests the WebSocketErrorAdapter that bridges between the new typed WebSocket
error system and the legacy APIError system for backward compatibility.
"""

from __future__ import annotations

from datetime import UTC, datetime

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.api_error_codes import APIErrorCode
from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_error_adapter import WebSocketErrorAdapter
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_exceptions import (
    WebSocketAuthenticationError,
    WebSocketConnectionError,
    WebSocketRateLimitError,
    WebSocketSubscriptionError,
    WebSocketValidationError,
)
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class TestWebSocketErrorAdapter:
    """Test WebSocket error adapter functionality."""

    def test_adapter_basic_conversion(self) -> None:
        """Test basic conversion from WebSocket error to APIError."""
        ws_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST,
            message="Connection lost to exchange",
        )

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        assert isinstance(api_error, APIError)
        assert api_error.code == APIErrorCode.NETWORK_ISSUE.value
        assert "Connection lost to exchange" in api_error.message
        assert api_error.http_status == 503  # Service unavailable

    def test_connection_error_mapping(self) -> None:
        """Test connection error code mappings."""
        connection_mappings = [
            (WebSocketErrorCode.CONNECTION_CLOSED, APIErrorCode.NETWORK_ISSUE, 503),
            (WebSocketErrorCode.CONNECTION_LOST, APIErrorCode.NETWORK_ISSUE, 503),
            (WebSocketErrorCode.CONNECTION_REFUSED, APIErrorCode.SERVICE_UNAVAILABLE, 503),
            (WebSocketErrorCode.CONNECTION_TIMEOUT, APIErrorCode.TIMEOUT, 503),
            (WebSocketErrorCode.CONNECTION_RESET, APIErrorCode.NETWORK_ISSUE, 503),
        ]

        for ws_code, expected_api_code, expected_status in connection_mappings:
            ws_error = ErrorTestFactory.create_test_error(code=ws_code)
            api_error = WebSocketErrorAdapter.to_api_error(ws_error)

            assert api_error.code == expected_api_code.value, f"Failed for {ws_code.name}"
            assert api_error.http_status == expected_status, f"Failed status for {ws_code.name}"

    def test_authentication_error_mapping(self) -> None:
        """Test authentication error code mappings."""
        auth_mappings = [
            (WebSocketErrorCode.AUTH_REQUIRED, APIErrorCode.AUTHENTICATION_FAILED, 401),
            (WebSocketErrorCode.AUTH_FAILED, APIErrorCode.AUTHENTICATION_FAILED, 401),
            (WebSocketErrorCode.AUTH_EXPIRED, APIErrorCode.AUTHENTICATION_FAILED, 401),
            (WebSocketErrorCode.AUTH_REVOKED, APIErrorCode.AUTHENTICATION_FAILED, 403),
            (WebSocketErrorCode.IP_BANNED, APIErrorCode.IP_BAN_SUSPECTED, 403),
            (WebSocketErrorCode.ACCOUNT_SUSPENDED, APIErrorCode.ACCOUNT_SUSPENDED, 403),
        ]

        for ws_code, expected_api_code, expected_status in auth_mappings:
            ws_error = ErrorTestFactory.create_test_error(code=ws_code)
            api_error = WebSocketErrorAdapter.to_api_error(ws_error)

            assert api_error.code == expected_api_code.value, f"Failed for {ws_code.name}"
            assert api_error.http_status == expected_status, f"Failed status for {ws_code.name}"

    def test_stream_error_mapping(self) -> None:
        """Test stream error code mappings."""
        stream_mappings = [
            (WebSocketErrorCode.STREAM_INTERRUPTED, APIErrorCode.SERVER_ERROR),
            (WebSocketErrorCode.STREAM_CORRUPTED, APIErrorCode.SERVER_ERROR),
            (WebSocketErrorCode.SEQUENCE_GAP, APIErrorCode.NETWORK_ISSUE),
            (WebSocketErrorCode.SEQUENCE_OUT_OF_ORDER, APIErrorCode.SERVER_ERROR),
        ]

        for ws_code, expected_api_code in stream_mappings:
            ws_error = ErrorTestFactory.create_test_error(code=ws_code)
            api_error = WebSocketErrorAdapter.to_api_error(ws_error)

            assert api_error.code == expected_api_code.value, f"Failed for {ws_code.name}"
            assert api_error.http_status == 500, "Stream errors should be 500"

    def test_rate_limit_mapping(self) -> None:
        """Test rate limit error mapping."""
        ws_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.RATE_LIMITED,
            message="Too many requests",
        )

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        assert api_error.code == APIErrorCode.RATE_LIMITED.value
        assert api_error.http_status == 503
        assert "Too many requests" in api_error.message

    def test_context_preservation(self) -> None:
        """Test that WebSocket context is preserved in metadata."""
        context = ErrorTestFactory.create_test_context(
            connection_id="test-conn-123",
            exchange="hyperliquid",
            channel="trades",
            topic="BTC-USDC",
            sequence_number=1000,
        )

        ws_error = WebSocketStreamError(
            message="Test error",
            code=WebSocketErrorCode.STREAM_INTERRUPTED,
            context=context,
        )

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        # Check metadata preservation
        assert api_error.metadata is not None
        assert api_error.metadata.get("connection_id") == "test-conn-123"
        assert api_error.metadata.get("exchange") == "hyperliquid"
        assert api_error.metadata.get("channel") == "trades"
        assert api_error.metadata.get("topic") == "BTC-USDC"
        assert api_error.metadata.get("sequence_number") == 1000
        assert api_error.metadata.get("ws_error_code") == "STREAM_INTERRUPTED"

    def test_severity_mapping(self) -> None:
        """Test severity level mapping."""
        # Critical WebSocket error
        critical_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.STREAM_CORRUPTED,
        )
        critical_error.severity = ErrorSeverity.CRITICAL

        api_error = WebSocketErrorAdapter.to_api_error(critical_error)
        assert api_error.metadata.get("ws_severity") == "CRITICAL"

        # Warning WebSocket error
        warning_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.SEQUENCE_GAP,
        )
        warning_error.severity = ErrorSeverity.WARNING

        api_error = WebSocketErrorAdapter.to_api_error(warning_error)
        assert api_error.metadata.get("ws_severity") == "WARNING"

    def test_recovery_strategy_in_metadata(self) -> None:
        """Test that recovery strategy is included in metadata."""
        ws_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST,
        )
        ws_error.recovery_strategy = WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        assert api_error.metadata is not None
        assert api_error.metadata.get("ws_recovery_strategy") == "EXPONENTIAL_BACKOFF"

    def test_retryability_mapping(self) -> None:
        """Test that retryability is correctly mapped."""
        # Retryable error
        retryable_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_TIMEOUT,
        )
        api_error = WebSocketErrorAdapter.to_api_error(retryable_error)
        assert api_error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE is True

        # Non-retryable error
        non_retryable_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.AUTH_REVOKED,
        )
        api_error = WebSocketErrorAdapter.to_api_error(non_retryable_error)
        assert api_error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE is False

    def test_specific_exception_types(self) -> None:
        """Test conversion of specific WebSocket exception types."""
        # Connection error
        conn_error = WebSocketConnectionError(
            message="Connection failed",
            code=WebSocketErrorCode.CONNECTION_REFUSED,
            context=ErrorTestFactory.create_test_context(),
        )
        api_error = WebSocketErrorAdapter.to_api_error(conn_error)
        assert api_error.code == APIErrorCode.SERVICE_UNAVAILABLE.value

        # Authentication error
        auth_error = WebSocketAuthenticationError(
            message="Auth failed",
            code=WebSocketErrorCode.AUTH_FAILED,
            context=ErrorTestFactory.create_test_context(),
        )
        api_error = WebSocketErrorAdapter.to_api_error(auth_error)
        assert api_error.code == APIErrorCode.AUTHENTICATION_FAILED.value

        # Rate limit error
        rate_error = WebSocketRateLimitError(
            context=ErrorTestFactory.create_test_context(),
            limit_type="messages",
            retry_after_ms=5000,
        )
        api_error = WebSocketErrorAdapter.to_api_error(rate_error)
        assert api_error.code == APIErrorCode.RATE_LIMITED.value

        # Subscription error
        sub_error = WebSocketSubscriptionError(
            message="Subscription failed",
            code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
            context=ErrorTestFactory.create_test_context(),
        )
        api_error = WebSocketErrorAdapter.to_api_error(sub_error)
        assert api_error.code == APIErrorCode.INVALID_REQUEST.value

        # Validation error
        val_error = WebSocketValidationError(
            message="Validation failed",
            code=WebSocketErrorCode.INVALID_MESSAGE_FORMAT,
            context=ErrorTestFactory.create_test_context(),
            field="message_type",
        )
        api_error = WebSocketErrorAdapter.to_api_error(val_error)
        assert api_error.code == APIErrorCode.INVALID_REQUEST.value
        assert api_error.metadata.get("validation_field") == "message_type"

    def test_error_chain_preservation(self) -> None:
        """Test that error chain is preserved in metadata."""
        context = ErrorTestFactory.create_test_context()

        # Add error chain
        context.add_to_error_chain(ValueError("First error"))
        context.add_to_error_chain(TypeError("Second error"))

        ws_error = WebSocketStreamError(
            message="Error with chain",
            code=WebSocketErrorCode.STREAM_INTERRUPTED,
            context=context,
        )

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        assert api_error.metadata.get("error_chain") is not None
        error_chain = api_error.metadata["error_chain"]
        assert len(error_chain) == 2
        assert error_chain[0]["class"] == "ValueError"
        assert error_chain[0]["message"] == "First error"
        assert error_chain[1]["class"] == "TypeError"
        assert error_chain[1]["message"] == "Second error"

    def test_legacy_monitoring_data(self) -> None:
        """Test legacy monitoring data extraction."""
        ws_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST,
            message="Lost connection",
        )

        legacy_data = WebSocketErrorAdapter.get_legacy_monitoring_data(ws_error)

        assert isinstance(legacy_data, dict)
        assert legacy_data["error_type"] == "websocket"
        assert legacy_data["error_code"] == WebSocketErrorCode.CONNECTION_LOST.value
        assert legacy_data["error_message"] == "Lost connection"
        assert legacy_data["exchange"] == ws_error.context.exchange
        assert legacy_data["timestamp_ms"] == ws_error.context.error_timestamp_ms
        assert "severity" in legacy_data
        assert "is_retryable" in legacy_data

    def test_unmapped_error_code_handling(self) -> None:
        """Test handling of unmapped error codes."""
        # Create an error with a code that might not have explicit mapping
        ws_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.INTERNAL_ERROR,
            message="Internal error",
        )

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        # Should default to SERVER_ERROR
        assert api_error.code == APIErrorCode.SERVER_ERROR.value
        assert api_error.http_status == 500
        assert "Internal error" in api_error.message

    def test_timestamp_preservation(self) -> None:
        """Test that timestamps are preserved correctly."""
        now = datetime.now(UTC)
        context = ErrorTestFactory.create_test_context(
            error_timestamp_ms=int(now.timestamp() * 1000),
        )

        ws_error = WebSocketStreamError(
            message="Test",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=context,
        )

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        # Check timestamp in metadata
        assert api_error.metadata.get("error_timestamp_ms") == context.error_timestamp_ms
        assert api_error.metadata.get("timestamp_iso") is not None

    def test_adapter_idempotency(self) -> None:
        """Test that converting the same error multiple times yields consistent results."""
        ws_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.RATE_LIMITED,
        )

        api_error1 = WebSocketErrorAdapter.to_api_error(ws_error)
        api_error2 = WebSocketErrorAdapter.to_api_error(ws_error)

        # Should produce identical APIError instances
        assert api_error1.code == api_error2.code
        assert api_error1.message == api_error2.message
        assert api_error1.http_status == api_error2.http_status
        assert api_error1.is_retryable == api_error2.is_retryable

    def test_metadata_completeness(self) -> None:
        """Test that all relevant metadata is included."""
        context = ErrorTestFactory.create_test_context(
            connection_id="full-conn",
            exchange="hyperliquid",
            channel="orderbook",
            topic="ETH-USDC",
            sequence_number=5000,
            expected_sequence=5005,
            active_subscriptions=10,
            raw_message_size=2048,
        )

        ws_error = WebSocketStreamError(
            message="Full metadata test",
            code=WebSocketErrorCode.SEQUENCE_GAP,
            context=context,
        )

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)
        metadata = api_error.metadata

        # Check all fields are present
        assert metadata.get("connection_id") == "full-conn"
        assert metadata.get("exchange") == "hyperliquid"
        assert metadata.get("channel") == "orderbook"
        assert metadata.get("topic") == "ETH-USDC"
        assert metadata.get("sequence_number") == 5000
        assert metadata.get("expected_sequence") == 5005
        assert metadata.get("sequence_gap") == 5
        assert metadata.get("active_subscriptions") == 10
        assert metadata.get("raw_message_size") == 2048
        assert metadata.get("ws_error_code") == "SEQUENCE_GAP"
        assert metadata.get("ws_category") == "STREAM"

    def test_adapter_with_cause(self) -> None:
        """Test adapter handling of errors with causes."""
        cause = ValueError("Original cause")
        ws_error = WebSocketStreamError(
            message="Error with cause",
            code=WebSocketErrorCode.INVALID_MESSAGE_FORMAT,
            context=ErrorTestFactory.create_test_context(),
            cause=cause,
        )

        api_error = WebSocketErrorAdapter.to_api_error(ws_error)

        # Cause should be in metadata
        assert api_error.metadata.get("cause_type") == "ValueError"
        assert api_error.metadata.get("cause_message") == "Original cause"

    def test_exchange_specific_mappings(self) -> None:
        """Test exchange-specific error mappings."""
        exchanges = ["hyperliquid", "backpack", "binance"]

        for exchange in exchanges:
            context = ErrorTestFactory.create_test_context(exchange=exchange)
            ws_error = WebSocketStreamError(
                message=f"Error from {exchange}",
                code=WebSocketErrorCode.EXCHANGE_ERROR,
                context=context,
            )

            api_error = WebSocketErrorAdapter.to_api_error(ws_error)

            assert api_error.metadata.get("exchange") == exchange
            assert api_error.code == APIErrorCode.SERVER_ERROR.value

    def test_security_error_mapping(self) -> None:
        """Test security-related error mappings."""
        security_mappings = [
            (WebSocketErrorCode.SECURITY_VIOLATION, APIErrorCode.PERMISSION_DENIED, 403),
            (WebSocketErrorCode.INVALID_SIGNATURE, APIErrorCode.AUTHENTICATION_FAILED, 401),
            (WebSocketErrorCode.REPLAY_ATTACK_DETECTED, APIErrorCode.PERMISSION_DENIED, 403),
            (WebSocketErrorCode.INJECTION_DETECTED, APIErrorCode.INVALID_REQUEST, 400),
        ]

        for ws_code, expected_api_code, expected_status in security_mappings:
            ws_error = ErrorTestFactory.create_test_error(code=ws_code)
            api_error = WebSocketErrorAdapter.to_api_error(ws_error)

            assert api_error.code == expected_api_code.value, f"Failed for {ws_code.name}"
            assert api_error.http_status == expected_status, f"Failed status for {ws_code.name}"

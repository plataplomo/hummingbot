"""Comprehensive unit tests for WebSocket error foundation components.

Tests all components created in Steps 1-9 of the refactor plan.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common.error_foundation import (
    ErrorChain,
    ErrorSeverity,
    ErrorTimestampMixin,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.enums.websocket.error_codes import WebSocketErrorCode
from cyberdelta.apis.exceptions.websocket import (
    WebSocketAuthenticationError,
    WebSocketConnectionError,
    WebSocketSecurityError,
    WebSocketSequenceError,
    WebSocketSubscriptionError,
    WebSocketSubscriptionLimitError,
    WebSocketValidationError,
)
from cyberdelta.apis.exceptions.websocket.stream_error import WebSocketStreamError
from cyberdelta.apis.models.websocket.error_context import StreamErrorContext
from cyberdelta.apis.models.websocket.stream_log import WebSocketStreamLogData
from cyberdelta.apis.websocket.error_context.validation import StreamErrorContextValidator
from cyberdelta.config.models.websocket_error_config import (
    WebSocketErrorAlertingConfig,
    WebSocketErrorConfig,
    WebSocketErrorLoggingConfig,
    WebSocketErrorMetricsConfig,
    WebSocketErrorRecoveryConfig,
)
from cyberdelta.enums import ExchangeName


# ============================================================================
# Test Error Foundation (Step 1)
# ============================================================================


class TestErrorSeverity:
    """Test ErrorSeverity enum."""

    def test_severity_ordering(self) -> None:
        """Test that severity levels are properly ordered."""
        assert ErrorSeverity.DEBUG < ErrorSeverity.INFO
        assert ErrorSeverity.INFO < ErrorSeverity.WARNING
        assert ErrorSeverity.WARNING < ErrorSeverity.ERROR
        assert ErrorSeverity.ERROR < ErrorSeverity.CRITICAL
        assert ErrorSeverity.CRITICAL < ErrorSeverity.FATAL

    def test_severity_values(self) -> None:
        """Test severity numeric values."""
        assert ErrorSeverity.DEBUG.value == 10
        assert ErrorSeverity.INFO.value == 20
        assert ErrorSeverity.WARNING.value == 30
        assert ErrorSeverity.ERROR.value == 40
        assert ErrorSeverity.CRITICAL.value == 50
        assert ErrorSeverity.FATAL.value == 60


class TestWebSocketRecoveryStrategy:
    """Test WebSocketRecoveryStrategy enum."""

    def test_recovery_strategy_categories(self) -> None:
        """Test recovery strategy value ranges."""
        # No recovery
        assert WebSocketRecoveryStrategy.NONE.value == 0

        # Simple retry strategies (100-199)
        assert 100 <= WebSocketRecoveryStrategy.IMMEDIATE_RETRY.value < 200
        assert 100 <= WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF.value < 200
        assert 100 <= WebSocketRecoveryStrategy.LINEAR_BACKOFF.value < 200

        # Connection strategies (200-299)
        assert 200 <= WebSocketRecoveryStrategy.RECONNECT_SAME.value < 300
        assert 200 <= WebSocketRecoveryStrategy.RECONNECT_DIFFERENT.value < 300
        assert 200 <= WebSocketRecoveryStrategy.FULL_RECONNECT.value < 300

        # Subscription strategies (300-399)
        assert 300 <= WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE.value < 400
        assert 300 <= WebSocketRecoveryStrategy.RESUBSCRIBE_ALL.value < 400
        assert 300 <= WebSocketRecoveryStrategy.RESUBSCRIBE_SELECTIVE.value < 400

        # Advanced strategies (400+)
        assert WebSocketRecoveryStrategy.CIRCUIT_BREAKER.value >= 400
        assert WebSocketRecoveryStrategy.FALLBACK_EXCHANGE.value >= 400
        assert WebSocketRecoveryStrategy.DEGRADE_SERVICE.value >= 400


class TestErrorTimestampMixin:
    """Test ErrorTimestampMixin functionality."""

    def test_timestamp_initialization(self) -> None:
        """Test that timestamp is set on initialization."""

        class TestError(ErrorTimestampMixin, Exception):
            def __init__(self) -> None:
                super().__init__()

        error = TestError()
        assert isinstance(error.timestamp, datetime)
        assert error.timestamp.tzinfo is not None
        assert isinstance(error.timestamp_ms, int)
        assert error.timestamp_ms > 0

    def test_age_calculation(self) -> None:
        """Test age calculation."""

        class TestError(ErrorTimestampMixin, Exception):
            def __init__(self) -> None:
                super().__init__()

        error = TestError()
        age = error.age_seconds()
        assert age >= 0
        assert age < 1  # Should be very small for a just-created error


class TestErrorChain:
    """Test ErrorChain support."""

    def test_from_exception(self) -> None:
        """Test creating error chain from exception."""
        exc = ValueError("Test error")
        chain = ErrorChain.from_exception(exc)

        assert chain.error_class == "ValueError"
        assert chain.error_message == "Test error"
        assert chain.error_code is None
        assert chain.timestamp_ms > 0

    def test_from_exception_with_code(self) -> None:
        """Test creating error chain from exception with code."""

        class CodedError(Exception):
            def __init__(self, message: str, code: int) -> None:
                super().__init__(message)
                self.code = code

        exc = CodedError("Test error", 123)
        chain = ErrorChain.from_exception(exc)

        assert chain.error_class == "CodedError"
        assert chain.error_message == "Test error"
        assert chain.error_code == 123


# ============================================================================
# Test WebSocket Error Codes (Step 2)
# ============================================================================


class TestWebSocketErrorCode:
    """Test WebSocketErrorCode enum."""

    def test_error_code_ranges(self) -> None:
        """Test that error codes are in expected ranges."""
        # Connection errors (1000-1099)
        assert 1000 <= WebSocketErrorCode.CONNECTION_CLOSED.value < 1100
        assert 1000 <= WebSocketErrorCode.CONNECTION_LOST.value < 1100

        # Authentication errors (1100-1199)
        assert 1100 <= WebSocketErrorCode.AUTH_FAILED.value < 1200
        assert 1100 <= WebSocketErrorCode.RATE_LIMITED.value < 1200

        # Stream errors (1200-1299)
        assert 1200 <= WebSocketErrorCode.STREAM_INTERRUPTED.value < 1300
        assert 1200 <= WebSocketErrorCode.STREAM_CORRUPTED.value < 1300

        # Subscription errors (1300-1399)
        assert 1300 <= WebSocketErrorCode.SUBSCRIPTION_FAILED.value < 1400
        assert 1300 <= WebSocketErrorCode.SUBSCRIPTION_LIMIT_EXCEEDED.value < 1400

    def test_get_category(self) -> None:
        """Test error code category determination."""
        assert WebSocketErrorCode.CONNECTION_CLOSED.get_category() == "CONNECTION"
        assert WebSocketErrorCode.STREAM_INTERRUPTED.get_category() == "STREAM"
        assert WebSocketErrorCode.SUBSCRIPTION_FAILED.get_category() == "SUBSCRIPTION"
        assert WebSocketErrorCode.AUTH_FAILED.get_category() == "AUTHENTICATION"
        assert (
            WebSocketErrorCode.RATE_LIMITED.get_category() == "AUTHENTICATION"
        )  # RATE_LIMITED is in auth range
        assert (
            WebSocketErrorCode.PROTOCOL_ERROR.get_category() == "CONNECTION"
        )  # PROTOCOL_ERROR is in connection range
        assert WebSocketErrorCode.SECURITY_VIOLATION.get_category() == "SECURITY"

    def test_is_retryable(self) -> None:
        """Test retryability determination."""
        # Retryable errors
        assert WebSocketErrorCode.CONNECTION_LOST.is_retryable() is True
        assert WebSocketErrorCode.STREAM_INTERRUPTED.is_retryable() is True
        assert WebSocketErrorCode.RATE_LIMITED.is_retryable() is True

        # Non-retryable errors
        assert WebSocketErrorCode.AUTH_REVOKED.is_retryable() is False
        assert WebSocketErrorCode.IP_BANNED.is_retryable() is False
        assert WebSocketErrorCode.SECURITY_VIOLATION.is_retryable() is False

    def test_is_critical(self) -> None:
        """Test criticality determination."""
        # Critical errors
        assert WebSocketErrorCode.STREAM_CORRUPTED.is_critical() is True
        assert WebSocketErrorCode.AUTH_REVOKED.is_critical() is True
        assert WebSocketErrorCode.IP_BANNED.is_critical() is True
        assert WebSocketErrorCode.SECURITY_VIOLATION.is_critical() is True

        # Non-critical errors
        assert WebSocketErrorCode.CONNECTION_LOST.is_critical() is False
        assert WebSocketErrorCode.SEQUENCE_GAP.is_critical() is False
        assert WebSocketErrorCode.RATE_LIMITED.is_critical() is False

    def test_get_suggested_action(self) -> None:
        """Test suggested action generation."""
        action = WebSocketErrorCode.CONNECTION_LOST.get_suggested_action()
        assert "reconnect" in action.lower()

        action = WebSocketErrorCode.AUTH_EXPIRED.get_suggested_action()
        assert "refresh" in action.lower() or "new" in action.lower()

        action = WebSocketErrorCode.RATE_LIMITED.get_suggested_action()
        assert "backoff" in action.lower() or "retry" in action.lower()


# ============================================================================
# Test Stream Error Context (Step 3)
# ============================================================================


class TestStreamErrorContext:
    """Test StreamErrorContext model."""

    @pytest.fixture
    def valid_context(self) -> StreamErrorContext:
        """Create a valid error context.

        Returns:
            StreamErrorContext: Valid test context
        """
        return StreamErrorContext(
            connection_id="test-connection-123",
            exchange=ExchangeName.HYPERLIQUID,
            channel="trades",
            topic="BTC-USDC",
            sequence_number=100,
            expected_sequence=99,
            last_received_sequence=98,
        )

    def test_context_creation(self, valid_context: StreamErrorContext) -> None:
        """Test creating valid context."""
        assert valid_context.connection_id == "test-connection-123"
        assert valid_context.exchange == "hyperliquid"
        assert valid_context.channel == "trades"
        assert valid_context.topic == "BTC-USDC"

    def test_context_validation(self) -> None:
        """Test context field validation."""
        # Empty connection_id should fail
        with pytest.raises(ValidationError):
            StreamErrorContext(
                connection_id="",
                exchange=ExchangeName.HYPERLIQUID,
            )

        # Invalid exchange type should fail (testing Pydantic validation)
        # Using dict to bypass type checking for validation test
        with pytest.raises(ValidationError):
            StreamErrorContext.model_validate({
                "connection_id": "test-123",
                "exchange": "",  # Invalid exchange value
            })

        # Negative sequence numbers should fail
        with pytest.raises(ValidationError):
            StreamErrorContext(
                connection_id="test-123",
                exchange=ExchangeName.HYPERLIQUID,
                sequence_number=-1,
            )

    def test_sequence_gap_detection(self, valid_context: StreamErrorContext) -> None:
        """Test sequence gap detection."""
        assert valid_context.has_sequence_gap() is True
        assert valid_context.get_sequence_gap_size() == 1

        # No gap
        valid_context.sequence_number = 99
        assert valid_context.has_sequence_gap() is False
        assert valid_context.get_sequence_gap_size() is None

    def test_connection_duration(self, valid_context: StreamErrorContext) -> None:
        """Test connection duration calculation."""
        now = int(datetime.now(UTC).timestamp() * 1000)
        valid_context.connection_started_ms = now - 60000  # 60 seconds ago
        valid_context.error_timestamp_ms = now

        duration = valid_context.get_connection_duration_ms()
        assert duration is not None
        assert 59000 <= duration <= 61000  # Allow some time variance

    def test_stale_connection_detection(self, valid_context: StreamErrorContext) -> None:
        """Test stale connection detection."""
        now = int(datetime.now(UTC).timestamp() * 1000)
        valid_context.error_timestamp_ms = now
        valid_context.last_message_received_ms = now - 40000  # 40 seconds ago

        assert valid_context.is_stale_connection(30000) is True
        assert valid_context.is_stale_connection(50000) is False

    def test_error_chain_addition(self, valid_context: StreamErrorContext) -> None:
        """Test adding errors to chain."""
        exc = ValueError("Test error")
        valid_context.add_to_error_chain(exc)

        assert len(valid_context.error_chain) == 1
        assert valid_context.error_chain[0].error_class == "ValueError"
        assert valid_context.error_chain[0].error_message == "Test error"

    def test_context_summary(self, valid_context: StreamErrorContext) -> None:
        """Test context summary generation."""
        summary = valid_context.get_summary()
        assert "hyperliquid" in summary
        assert "test-con..." in summary  # Truncated ID
        assert "trades" in summary
        assert "BTC-USDC" in summary
        assert "100" in summary  # Sequence number


# ============================================================================
# Test WebSocket Stream Log Data (Step 4)
# ============================================================================


class TestWebSocketStreamLogData:
    """Test WebSocketStreamLogData model."""

    @pytest.fixture
    def sample_context(self) -> StreamErrorContext:
        """Create sample context.

        Returns:
            StreamErrorContext: Sample test context
        """
        return StreamErrorContext(
            connection_id="test-123",
            exchange=ExchangeName.BACKPACK,
            channel="orderbook",
            topic="ETH-USDC",
        )

    def test_from_stream_error(self, sample_context: StreamErrorContext) -> None:
        """Test creating log data from stream error components."""
        log_data = WebSocketStreamLogData.from_stream_error(
            error_code=WebSocketErrorCode.CONNECTION_LOST,
            message="Connection lost",
            context=sample_context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
        )

        assert log_data.error_code == WebSocketErrorCode.CONNECTION_LOST
        assert log_data.message == "Connection lost"
        assert log_data.connection_id == "test-123"
        assert log_data.exchange == "backpack"
        assert log_data.severity == ErrorSeverity.ERROR
        assert log_data.recovery_strategy == WebSocketRecoveryStrategy.RECONNECT_SAME

    def test_log_level_determination(self) -> None:
        """Test log level determination from severity."""
        log_data = WebSocketStreamLogData(
            error_code=WebSocketErrorCode.UNKNOWN_ERROR,
            error_code_name="UNKNOWN_ERROR",
            error_category="UNKNOWN",
            message="Test",
            severity=ErrorSeverity.DEBUG,
            is_critical=False,
            is_retryable=False,
            recovery_strategy=WebSocketRecoveryStrategy.NONE,
            suggested_action="None",
            connection_id="test",
            exchange="test",
            timestamp=datetime.now(UTC),
            timestamp_ms=int(datetime.now(UTC).timestamp() * 1000),
        )

        assert log_data.get_log_level() == "debug"

        log_data.severity = ErrorSeverity.WARNING
        assert log_data.get_log_level() == "warning"

        log_data.severity = ErrorSeverity.CRITICAL
        assert log_data.get_log_level() == "critical"

    def test_metrics_dict_generation(self, sample_context: StreamErrorContext) -> None:
        """Test metrics dictionary generation."""
        log_data = WebSocketStreamLogData.from_stream_error(
            error_code=WebSocketErrorCode.SEQUENCE_GAP,
            message="Sequence gap detected",
            context=sample_context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        )

        metrics = log_data.to_metrics_dict()
        assert metrics["error_code"] == WebSocketErrorCode.SEQUENCE_GAP.value
        assert metrics["error_category"] == "STREAM"
        assert metrics["severity"] == ErrorSeverity.WARNING.value
        assert metrics["exchange"] == "backpack"


# ============================================================================
# Test WebSocket Stream Error (Step 5)
# ============================================================================


class TestWebSocketStreamError:
    """Test WebSocketStreamError class."""

    @pytest.fixture
    def sample_context(self) -> StreamErrorContext:
        """Create sample context.

        Returns:
            StreamErrorContext: Sample test context
        """
        return StreamErrorContext(
            connection_id="test-456",
            exchange=ExchangeName.HYPERLIQUID,
            reconnect_count=2,
        )

    def test_error_creation(self, sample_context: StreamErrorContext) -> None:
        """Test creating WebSocket stream error."""
        error = WebSocketStreamError(
            message="Test error",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=sample_context,
        )

        assert error.message == "Test error"
        assert error.code == WebSocketErrorCode.CONNECTION_LOST
        assert error.context == sample_context
        assert error.severity == ErrorSeverity.WARNING  # Auto-determined
        assert error.recovery_strategy == WebSocketRecoveryStrategy.RECONNECT_SAME

    def test_severity_determination(self, sample_context: StreamErrorContext) -> None:
        """Test automatic severity determination."""
        # Critical error
        error = WebSocketStreamError(
            message="Security violation",
            code=WebSocketErrorCode.SECURITY_VIOLATION,
            context=sample_context,
        )
        assert error.severity == ErrorSeverity.CRITICAL

        # Warning level
        error = WebSocketStreamError(
            message="Sequence gap",
            code=WebSocketErrorCode.SEQUENCE_GAP,
            context=sample_context,
        )
        assert error.severity == ErrorSeverity.WARNING

    def test_recovery_strategy_determination(self, sample_context: StreamErrorContext) -> None:
        """Test automatic recovery strategy determination."""
        # No recovery for security
        error = WebSocketStreamError(
            message="Security issue",
            code=WebSocketErrorCode.SECURITY_VIOLATION,
            context=sample_context,
        )
        assert error.recovery_strategy == WebSocketRecoveryStrategy.NONE

        # Exponential backoff for rate limiting
        error = WebSocketStreamError(
            message="Rate limited",
            code=WebSocketErrorCode.RATE_LIMITED,
            context=sample_context,
        )
        assert error.recovery_strategy == WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF

        # Different reconnect after multiple failures
        sample_context.reconnect_count = 3
        error = WebSocketStreamError(
            message="Connection lost",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=sample_context,
        )
        assert error.recovery_strategy == WebSocketRecoveryStrategy.RECONNECT_DIFFERENT

    def test_retry_delay_calculation(self, sample_context: StreamErrorContext) -> None:
        """Test retry delay calculation."""
        # Immediate retry
        error = WebSocketStreamError(
            message="Test",
            code=WebSocketErrorCode.SEQUENCE_GAP,
            context=sample_context,
        )
        assert error.get_retry_delay_ms() == 0

        # Exponential backoff
        sample_context.metadata.retry_count = 2
        sample_context.metadata.backoff_ms = 1000
        error = WebSocketStreamError(
            message="Rate limited",
            code=WebSocketErrorCode.RATE_LIMITED,
            context=sample_context,
        )
        delay = error.get_retry_delay_ms()
        assert delay == 4000  # 1000 * 2^2

    def test_error_chaining(self, sample_context: StreamErrorContext) -> None:
        """Test error chaining."""
        cause = ValueError("Original error")
        error = WebSocketStreamError(
            message="Wrapped error",
            code=WebSocketErrorCode.UNKNOWN_ERROR,
            context=sample_context,
            cause=cause,
        )

        assert error.cause == cause
        assert error.__cause__ == cause
        assert len(error.context.error_chain) == 1
        assert error.context.error_chain[0].error_class == "ValueError"


# ============================================================================
# Test Error Context Validator (Step 6)
# ============================================================================


class TestStreamErrorContextValidator:
    """Test StreamErrorContextValidator."""

    def test_validate_connection_id(self) -> None:
        """Test connection ID validation."""
        # Valid IDs
        assert (
            StreamErrorContextValidator.validate_connection_id("abc123-def456") == "abc123-def456"
        )
        assert (
            StreamErrorContextValidator.validate_connection_id("test_connection_123")
            == "test_connection_123"
        )

        # Invalid IDs
        with pytest.raises(ValueError, match="non-empty"):
            StreamErrorContextValidator.validate_connection_id("")

        with pytest.raises(ValueError, match="format"):
            StreamErrorContextValidator.validate_connection_id("a")  # Too short

    def test_validate_exchange(self) -> None:
        """Test exchange name validation."""
        # Valid exchanges
        assert (
            StreamErrorContextValidator.validate_exchange(ExchangeName.HYPERLIQUID)
            == ExchangeName.HYPERLIQUID
        )
        assert (
            StreamErrorContextValidator.validate_exchange(ExchangeName.BACKPACK)
            == ExchangeName.BACKPACK
        )

        # Note: Invalid exchange validation is handled by Pydantic's type enforcement
        # The validate_exchange method expects an ExchangeName enum, not strings

    def test_validate_sequence_consistency(self) -> None:
        """Test sequence consistency validation."""
        # Valid sequences
        StreamErrorContextValidator.validate_sequence_consistency(100, 101, 99)

        # Excessive gap
        with pytest.raises(ValueError, match="Excessive sequence gap"):
            StreamErrorContextValidator.validate_sequence_consistency(100, 30100, 99)

        # Sequence regression
        with pytest.raises(ValueError, match="Sequence regression"):
            StreamErrorContextValidator.validate_sequence_consistency(50, 100, 100)

    def test_sanitize_context(self) -> None:
        """Test context sanitization."""
        # Create invalid context using model_construct to bypass validation
        context = StreamErrorContext.model_construct(
            connection_id="!invalid!",  # Will be sanitized
            exchange="INVALID_EXCHANGE",  # Will be sanitized
            active_subscriptions=-5,  # Will be sanitized
        )

        sanitized = StreamErrorContextValidator.sanitize_context(context)
        assert sanitized.connection_id == "unknown-connection"
        assert sanitized.exchange == "unknown"
        assert sanitized.active_subscriptions == 0


# ============================================================================
# Test WebSocket-Specific Exceptions (Step 7)
# ============================================================================


class TestWebSocketExceptions:
    """Test WebSocket-specific exception classes."""

    @pytest.fixture
    def sample_context(self) -> StreamErrorContext:
        """Create sample context.

        Returns:
            StreamErrorContext: Sample test context
        """
        return StreamErrorContext(
            connection_id="test-789",
            exchange=ExchangeName.BACKPACK,
        )

    def test_connection_error(self, sample_context: StreamErrorContext) -> None:
        """Test WebSocketConnectionError."""
        error = WebSocketConnectionError(
            message="Connection failed",
            context=sample_context,
        )
        assert error.code == WebSocketErrorCode.CONNECTION_LOST
        assert error.recovery_strategy == WebSocketRecoveryStrategy.RECONNECT_SAME

    # WebSocketConnectionClosedError removed - unused in production

    def test_authentication_error(self, sample_context: StreamErrorContext) -> None:
        """Test WebSocketAuthenticationError."""
        # Temporary auth failure
        error = WebSocketAuthenticationError(
            message="Auth failed",
            context=sample_context,
        )
        assert error.recovery_strategy == WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF
        assert error.severity == ErrorSeverity.ERROR

        # Permanent auth failure
        error = WebSocketAuthenticationError(
            message="Account banned",
            context=sample_context,
        )
        assert error.recovery_strategy == WebSocketRecoveryStrategy.NONE
        assert error.severity == ErrorSeverity.CRITICAL

    def test_subscription_limit_error(self, sample_context: StreamErrorContext) -> None:
        """Test WebSocketSubscriptionLimitError."""
        error = WebSocketSubscriptionLimitError(
            context=sample_context,
            limit=100,
            current=101,
            channel="trades",
        )
        assert "101/100" in error.message
        assert "trades" in error.message
        assert error.context.extra_context["subscription_limit"] == 100

    def test_validation_error(self, sample_context: StreamErrorContext) -> None:
        """Test WebSocketValidationError."""
        error = WebSocketValidationError(
            message="Invalid field",
            context=sample_context,
            field="price",
            value="not_a_number",
        )
        assert error.context.extra_context["validation_field"] == "price"
        assert error.context.extra_context["invalid_value"] == "not_a_number"

    def test_sequence_error(self, sample_context: StreamErrorContext) -> None:
        """Test WebSocketSequenceError."""
        # Update context with sequence info
        sample_context.expected_sequence = 100
        sample_context.sequence_number = 105
        error = WebSocketSequenceError(
            message="5 messages missing",
            context=sample_context,
            code=WebSocketErrorCode.SEQUENCE_GAP,
        )
        assert "5 messages missing" in error.message
        assert error.code == WebSocketErrorCode.SEQUENCE_GAP
        assert error.context.expected_sequence == 100
        assert error.context.sequence_number == 105

    # WebSocketRateLimitError removed - unused in production

    def test_security_error(self, sample_context: StreamErrorContext) -> None:
        """Test WebSocketSecurityError."""
        error = WebSocketSecurityError(
            message="Security violation detected",
            context=sample_context,
            security_type="suspicious_activity",
        )
        assert error.severity == ErrorSeverity.CRITICAL
        assert error.recovery_strategy == WebSocketRecoveryStrategy.NONE
        assert error.context.extra_context["security_type"] == "suspicious_activity"


# ============================================================================
# Test WebSocket Error Configuration (Step 9)
# ============================================================================


class TestWebSocketErrorConfig:
    """Test WebSocket error configuration."""

    def test_recovery_config_defaults(self) -> None:
        """Test recovery configuration defaults."""
        config = WebSocketErrorRecoveryConfig()
        assert config.max_recovery_attempts == 3
        assert config.initial_backoff_ms == 1000
        assert config.max_backoff_ms == 60000
        assert config.backoff_multiplier == 2.0
        assert config.jitter_enabled is True
        assert config.circuit_breaker_enabled is True

    def test_recovery_config_validation(self) -> None:
        """Test recovery configuration validation."""
        # Valid config
        config = WebSocketErrorRecoveryConfig(
            max_recovery_attempts=5,
            initial_backoff_ms=500,
            sequence_gap_recovery_method="full_resync",
        )
        assert config.max_recovery_attempts == 5
        assert config.sequence_gap_recovery_method == "full_resync"

        # Invalid recovery method
        with pytest.raises(ValidationError):
            WebSocketErrorRecoveryConfig(sequence_gap_recovery_method="invalid_method")

    def test_metrics_config(self) -> None:
        """Test metrics configuration."""
        config = WebSocketErrorMetricsConfig()
        assert config.enable_metrics_collection is True
        assert config.metrics_buffer_size == 1000
        assert config.track_error_rates is True
        assert config.track_recovery_times is True
        assert config.track_stack_traces is False  # Off by default

    def test_alerting_config(self) -> None:
        """Test alerting configuration."""
        config = WebSocketErrorAlertingConfig()
        assert config.enable_alerting is True
        assert config.critical_error_threshold == 1
        assert config.error_rate_alert_threshold == 0.1
        assert config.alert_cooldown_ms == 300000
        assert config.log_alerts is True
        assert config.webhook_alerts is False

    def test_logging_config_validation(self) -> None:
        """Test logging configuration validation."""
        # Valid severity
        config = WebSocketErrorLoggingConfig(min_severity_to_log="ERROR")
        assert config.min_severity_to_log == "ERROR"

        # Invalid severity
        with pytest.raises(ValidationError):
            WebSocketErrorLoggingConfig(min_severity_to_log="INVALID")

    def test_complete_config(self) -> None:
        """Test complete WebSocket error configuration."""
        config = WebSocketErrorConfig()
        assert config.enabled is True
        assert config.validate_contexts is True
        assert config.async_error_handling is True

        # Sub-configs are initialized
        assert isinstance(config.recovery, WebSocketErrorRecoveryConfig)
        assert isinstance(config.metrics, WebSocketErrorMetricsConfig)
        assert isinstance(config.alerting, WebSocketErrorAlertingConfig)
        assert isinstance(config.logging, WebSocketErrorLoggingConfig)

    def test_exchange_overrides(self) -> None:
        """Test exchange-specific configuration overrides."""
        config = WebSocketErrorConfig(
            exchange_overrides={
                "hyperliquid": {
                    "recovery": {
                        "max_recovery_attempts": 5,
                        "initial_backoff_ms": 2000,
                    }
                }
            }
        )

        # Get config with overrides
        hl_config = config.get_exchange_config(ExchangeName.HYPERLIQUID)
        assert hl_config.recovery.max_recovery_attempts == 5
        assert hl_config.recovery.initial_backoff_ms == 2000

        # Get config without overrides
        bp_config = config.get_exchange_config(ExchangeName.BACKPACK)
        assert bp_config.recovery.max_recovery_attempts == 3  # Default
        assert bp_config.recovery.initial_backoff_ms == 1000  # Default


# ============================================================================
# Integration Tests
# ============================================================================


class TestFoundationIntegration:
    """Integration tests for all foundation components."""

    def test_complete_error_flow(self) -> None:
        """Test complete error flow from creation to adaptation."""
        # Create context
        context = StreamErrorContext(
            connection_id="integration-test",
            exchange=ExchangeName.BACKPACK,
            channel="orderbook",
            topic="SOL-USDC",
            sequence_number=1000,
            expected_sequence=999,
            reconnect_count=1,
        )

        # Create WebSocket error
        ws_error = WebSocketStreamError(
            message="Integration test error",
            code=WebSocketErrorCode.SEQUENCE_GAP,
            context=context,
        )

        # Verify automatic determinations
        assert ws_error.severity == ErrorSeverity.WARNING
        assert ws_error.recovery_strategy == WebSocketRecoveryStrategy.IMMEDIATE_RETRY
        assert ws_error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE
        assert ws_error.is_critical is False

        # Convert to log data
        log_data = ws_error.to_log_data()
        assert log_data.connection_id == "integration-test"
        assert log_data.sequence_gap == 1

        # Verify error properties without adapter
        assert ws_error.code == WebSocketErrorCode.SEQUENCE_GAP
        assert ws_error.context.has_sequence_gap() is True
        assert ws_error.context.get_sequence_gap_size() == 1

    def test_error_with_configuration(self) -> None:
        """Test error handling with configuration."""
        # Create config
        config = WebSocketErrorConfig(
            recovery=WebSocketErrorRecoveryConfig(
                max_recovery_attempts=5,
                initial_backoff_ms=2000,
            ),
            metrics=WebSocketErrorMetricsConfig(
                enable_metrics_collection=True,
                track_stack_traces=True,
            ),
        )

        # Create error
        context = StreamErrorContext(
            connection_id="config-test",
            exchange=ExchangeName.HYPERLIQUID,
        )
        context.metadata.retry_count = 2
        context.metadata.backoff_ms = config.recovery.initial_backoff_ms

        error = WebSocketStreamError(
            message="Config test",
            code=WebSocketErrorCode.RATE_LIMITED,
            context=context,
        )

        # Calculate delay based on config
        delay = error.get_retry_delay_ms()
        expected_delay = 2000 * (2**2)  # 8000ms
        assert delay == min(expected_delay, 60000)

    def test_exception_hierarchy(self) -> None:
        """Test that all exceptions properly inherit from WebSocketStreamError."""
        context = StreamErrorContext(
            connection_id="hierarchy-test",
            exchange=ExchangeName.HYPERLIQUID,
        )

        # Test various exception types
        exceptions = [
            WebSocketConnectionError("Test", context),
            WebSocketAuthenticationError("Test", context),
            WebSocketSubscriptionError("Test", context),
            WebSocketValidationError("Test", context),
            WebSocketConnectionError("Test", context),
            WebSocketSecurityError("Test", context),
        ]

        for exc in exceptions:
            assert isinstance(exc, WebSocketStreamError)
            assert isinstance(exc, Exception)
            assert hasattr(exc, "code")
            assert hasattr(exc, "context")
            assert hasattr(exc, "severity")
            assert hasattr(exc, "recovery_strategy")

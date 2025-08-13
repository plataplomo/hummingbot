"""Test utilities for WebSocket error system testing.

Provides utilities for creating test errors, contexts, and scenarios
to facilitate comprehensive testing of the WebSocket error system.
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field, ValidationError
from pydantic_core import InitErrorDetails

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError


class ErrorTestScenario(BaseModel):
    """Test scenario for error handling."""

    name: str = Field(description="Scenario name")
    error_code: WebSocketErrorCode = Field(description="Error code to test")
    expected_severity: ErrorSeverity = Field(description="Expected severity")
    expected_recovery: WebSocketRecoveryStrategy = Field(description="Expected recovery strategy")
    should_be_retryable: bool = Field(description="Whether error should be retryable")
    should_be_critical: bool = Field(description="Whether error should be critical")
    context_overrides: dict[str, Any] = Field(default_factory=dict, description="Context overrides")


class ErrorTestFactory:
    """Factory for creating test errors and contexts."""

    @staticmethod
    def create_test_context(
        connection_id: str = "test-connection-123",
        exchange: str = "testexchange",
        channel: str | None = "trades",
        topic: str | None = "BTC-USDC",
        sequence_number: int | None = None,
        expected_sequence: int | None = None,
        error_timestamp_ms: int | None = None,
        reconnect_count: int = 0,
        is_authenticated: bool = False,
    ) -> StreamErrorContext:
        """Create a test error context with defaults.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name
            channel: Channel name
            topic: Topic/symbol
            sequence_number: Sequence number
            expected_sequence: Expected sequence number
            error_timestamp_ms: Error timestamp in milliseconds
            reconnect_count: Number of reconnection attempts
            is_authenticated: Whether connection is authenticated

        Returns:
            StreamErrorContext for testing
        """
        if error_timestamp_ms is None:
            error_timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)

        return StreamErrorContext(
            connection_id=connection_id,
            exchange=exchange,
            channel=channel,
            topic=topic,
            sequence_number=sequence_number,
            expected_sequence=expected_sequence,
            error_timestamp_ms=error_timestamp_ms,
            reconnect_count=reconnect_count,
            is_authenticated=is_authenticated,
        )

    @staticmethod
    def create_test_error(
        code: WebSocketErrorCode = WebSocketErrorCode.CONNECTION_LOST,
        message: str | None = None,
        context: StreamErrorContext | None = None,
        severity: ErrorSeverity | None = None,
        recovery_strategy: WebSocketRecoveryStrategy | None = None,
        cause: Exception | None = None,
    ) -> WebSocketStreamError:
        """Create a test WebSocket error.

        Args:
            code: Error code
            message: Error message (generated if not provided)
            context: Error context (created if not provided)
            severity: Error severity
            recovery_strategy: Recovery strategy
            cause: Underlying exception

        Returns:
            WebSocketStreamError for testing
        """
        if message is None:
            message = f"Test error: {code.name}"

        if context is None:
            context = ErrorTestFactory.create_test_context()

        return WebSocketStreamError(
            message=message,
            code=code,
            context=context,
            severity=severity,
            recovery_strategy=recovery_strategy,
            cause=cause,
        )

    @staticmethod
    def create_validation_error(field_errors: dict[str, str] | None = None) -> ValidationError:
        """Create a test validation error.

        Args:
            field_errors: Field errors to include

        Returns:
            ValidationError for testing
        """
        if field_errors is None:
            field_errors = {"test_field": "Invalid value"}

        # Create a model that will fail validation
        class TestModel(BaseModel):
            test_field: str = Field(pattern="^valid$")

        try:
            TestModel(test_field="invalid")
        except ValidationError as e:
            return e

        # Fallback if validation doesn't fail (shouldn't happen)
        # Create a validation error manually since the expected ValidationError didn't occur

        error_details: InitErrorDetails = {
            "type": "value_error",
            "loc": ("test_field",),
            "input": "invalid",
        }
        return ValidationError.from_exception_data("TestModel", [error_details])

    @staticmethod
    def create_random_error() -> WebSocketStreamError:
        """Create a random error for stress testing.

        Returns:
            Random WebSocketStreamError
        """
        all_codes = list(WebSocketErrorCode)
        code = secrets.choice(all_codes)

        context = ErrorTestFactory.create_test_context(
            connection_id=f"random-{secrets.randbits(14) % 9000 + 1000}",
            exchange=secrets.choice(["hyperliquid", "backpack", "binance"]),
            channel=secrets.choice(["trades", "orderbook", "account", None]),
            sequence_number=secrets.randbits(20) if secrets.randbits(1) else None,
        )

        return ErrorTestFactory.create_test_error(
            code=code,
            message=f"Random error: {code.name}",
            context=context,
        )


class ErrorScenarioGenerator:
    """Generate comprehensive error test scenarios."""

    @staticmethod
    def get_all_error_code_scenarios() -> list[ErrorTestScenario]:
        """Generate test scenarios for all error codes.

        Returns:
            List of test scenarios covering all error codes
        """
        scenarios: list[ErrorTestScenario] = []

        # Connection errors
        scenarios.extend([
            ErrorTestScenario(
                name="Connection Lost",
                error_code=WebSocketErrorCode.CONNECTION_LOST,
                expected_severity=ErrorSeverity.WARNING,
                expected_recovery=WebSocketRecoveryStrategy.RECONNECT_SAME,
                should_be_retryable=True,
                should_be_critical=False,
            ),
            ErrorTestScenario(
                name="Connection Timeout",
                error_code=WebSocketErrorCode.CONNECTION_TIMEOUT,
                expected_severity=ErrorSeverity.ERROR,
                expected_recovery=WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
                should_be_retryable=True,
                should_be_critical=False,
            ),
            ErrorTestScenario(
                name="Protocol Error",
                error_code=WebSocketErrorCode.PROTOCOL_ERROR,
                expected_severity=ErrorSeverity.ERROR,
                expected_recovery=WebSocketRecoveryStrategy.FULL_RECONNECT,
                should_be_retryable=True,  # FULL_RECONNECT is a form of retry
                should_be_critical=True,
            ),
        ])

        # Authentication errors
        scenarios.extend([
            ErrorTestScenario(
                name="Auth Failed",
                error_code=WebSocketErrorCode.AUTH_FAILED,
                expected_severity=ErrorSeverity.ERROR,
                expected_recovery=WebSocketRecoveryStrategy.NONE,
                should_be_retryable=False,
                should_be_critical=False,
            ),
            ErrorTestScenario(
                name="IP Banned",
                error_code=WebSocketErrorCode.IP_BANNED,
                expected_severity=ErrorSeverity.CRITICAL,
                expected_recovery=WebSocketRecoveryStrategy.NONE,
                should_be_retryable=False,
                should_be_critical=True,
            ),
        ])

        # Stream errors
        scenarios.extend([
            ErrorTestScenario(
                name="Sequence Gap",
                error_code=WebSocketErrorCode.SEQUENCE_GAP,
                expected_severity=ErrorSeverity.WARNING,
                expected_recovery=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
                should_be_retryable=True,
                should_be_critical=False,
            ),
            ErrorTestScenario(
                name="Stream Corrupted",
                error_code=WebSocketErrorCode.STREAM_CORRUPTED,
                expected_severity=ErrorSeverity.CRITICAL,
                expected_recovery=WebSocketRecoveryStrategy.FULL_RECONNECT,
                should_be_retryable=False,
                should_be_critical=True,
            ),
        ])

        # Rate limiting
        scenarios.append(
            ErrorTestScenario(
                name="Rate Limited",
                error_code=WebSocketErrorCode.RATE_LIMITED,
                expected_severity=ErrorSeverity.WARNING,
                expected_recovery=WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
                should_be_retryable=True,
                should_be_critical=False,
            )
        )

        return scenarios

    @staticmethod
    def get_recovery_strategy_scenarios() -> list[
        tuple[WebSocketRecoveryStrategy, list[WebSocketErrorCode]]
    ]:
        """Get scenarios for each recovery strategy.

        Returns:
            List of recovery strategies with applicable error codes
        """
        return [
            (
                WebSocketRecoveryStrategy.NONE,
                [
                    WebSocketErrorCode.AUTH_FAILED,
                    WebSocketErrorCode.IP_BANNED,
                    WebSocketErrorCode.ACCOUNT_SUSPENDED,
                ],
            ),
            (
                WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
                [
                    WebSocketErrorCode.SEQUENCE_GAP,
                    WebSocketErrorCode.SEQUENCE_OUT_OF_ORDER,
                ],
            ),
            (
                WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
                [
                    WebSocketErrorCode.RATE_LIMITED,
                    WebSocketErrorCode.CONNECTION_TIMEOUT,
                ],
            ),
            (
                WebSocketRecoveryStrategy.RECONNECT_SAME,
                [
                    WebSocketErrorCode.CONNECTION_LOST,
                    WebSocketErrorCode.HEARTBEAT_TIMEOUT,
                ],
            ),
            (
                WebSocketRecoveryStrategy.FULL_RECONNECT,
                [
                    WebSocketErrorCode.PROTOCOL_ERROR,
                    WebSocketErrorCode.STREAM_CORRUPTED,
                ],
            ),
            (
                WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
                [
                    WebSocketErrorCode.SUBSCRIPTION_FAILED,
                    WebSocketErrorCode.CHANNEL_CLOSED,
                ],
            ),
        ]

    @staticmethod
    def get_critical_error_scenarios() -> list[WebSocketErrorCode]:
        """Get all critical error codes.

        Returns:
            List of critical error codes
        """
        return [
            WebSocketErrorCode.STREAM_CORRUPTED,
            WebSocketErrorCode.SECURITY_VIOLATION,
            WebSocketErrorCode.IP_BANNED,
            WebSocketErrorCode.ACCOUNT_SUSPENDED,
            WebSocketErrorCode.AUTH_REVOKED,
            WebSocketErrorCode.INJECTION_DETECTED,
            WebSocketErrorCode.REPLAY_ATTACK_DETECTED,
            WebSocketErrorCode.PROTOCOL_ERROR,
            WebSocketErrorCode.HANDSHAKE_FAILED,
            WebSocketErrorCode.MEMORY_LIMIT_EXCEEDED,
            WebSocketErrorCode.RESOURCE_EXHAUSTED,
        ]

    @staticmethod
    def get_retryable_error_scenarios() -> list[WebSocketErrorCode]:
        """Get all retryable error codes.

        Returns:
            List of retryable error codes
        """
        return [
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.CONNECTION_TIMEOUT,
            WebSocketErrorCode.CONNECTION_RESET,
            WebSocketErrorCode.STREAM_INTERRUPTED,
            WebSocketErrorCode.SEQUENCE_GAP,
            WebSocketErrorCode.SEQUENCE_OUT_OF_ORDER,
            WebSocketErrorCode.RATE_LIMITED,
            WebSocketErrorCode.RECOVERY_IN_PROGRESS,
            WebSocketErrorCode.EXCHANGE_OVERLOADED,
            WebSocketErrorCode.EXCHANGE_MAINTENANCE,
        ]


class ErrorAssertions:
    """Assertion helpers for error testing."""

    @staticmethod
    def assert_error_properties(
        error: WebSocketStreamError,
        expected_code: WebSocketErrorCode,
        expected_severity: ErrorSeverity | None = None,
        expected_recovery: WebSocketRecoveryStrategy | None = None,
    ) -> None:
        """Assert error has expected properties.

        Args:
            error: Error to check
            expected_code: Expected error code
            expected_severity: Expected severity (if provided)
            expected_recovery: Expected recovery strategy (if provided)
        """
        assert error.code == expected_code, f"Expected code {expected_code}, got {error.code}"

        if expected_severity is not None:
            assert error.severity == expected_severity, (
                f"Expected severity {expected_severity}, got {error.severity}"
            )

        if expected_recovery is not None:
            assert error.recovery_strategy == expected_recovery, (
                f"Expected recovery {expected_recovery}, got {error.recovery_strategy}"
            )

    @staticmethod
    def assert_context_valid(context: StreamErrorContext) -> None:
        """Assert context is valid.

        Args:
            context: Context to validate
        """
        assert context.connection_id, "Connection ID required"
        assert context.exchange, "Exchange required"
        assert context.error_timestamp_ms > 0, "Timestamp required"

        # Check sequence consistency if applicable
        if context.sequence_number is not None and context.expected_sequence is not None:
            gap = context.get_sequence_gap_size()
            if gap is not None:
                assert gap >= 0, "Sequence gap cannot be negative"

    @staticmethod
    def assert_error_chain_valid(error: WebSocketStreamError) -> None:
        """Assert error chain is valid.

        Args:
            error: Error with potential chain
        """
        if error.cause:
            assert isinstance(error.cause, Exception), "Cause must be an exception"

        if error.context.error_chain:
            for chain_error in error.context.error_chain:
                assert chain_error.error_class, "Chain error must have class"
                assert chain_error.error_message, "Chain error must have message"
                assert chain_error.timestamp_ms > 0, "Chain error must have timestamp"

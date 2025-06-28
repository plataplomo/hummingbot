"""Security tests for response validation utilities.

This module tests security scenarios for the NO DECORATORS API sanitization solution,
ensuring that validation utilities properly handle malicious inputs, edge cases,
and potential attack vectors.
"""

import threading
import time
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
    ensure_string_response,
    validate_required_fields,
    validate_response_not_empty,
)


class TestSecurityValidationScenarios:
    """Test security scenarios for response validation utilities."""

    def test_ensure_dict_response_null_injection(self) -> None:
        """Test that null responses are properly rejected with security logging."""
        with pytest.raises(APIError) as exc_info:
            ensure_dict_response(None, "test_context", 200)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received" in str(exc_info.value)
        assert exc_info.value.http_status == 200

    def test_ensure_dict_response_type_confusion_attack(self) -> None:
        """Test that type confusion attacks are prevented."""
        # Test with list instead of dict
        with pytest.raises(APIError) as exc_info:
            ensure_dict_response([{"malicious": "data"}], "balance", 200)

        assert "expected dict, got list" in str(exc_info.value)
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

        # Test with string instead of dict
        with pytest.raises(APIError) as exc_info:
            ensure_dict_response("malicious string", "order", 200)

        assert "expected dict, got str" in str(exc_info.value)

        # Test with integer instead of dict
        with pytest.raises(APIError) as exc_info:
            ensure_dict_response(cast("Any", 12345), "ticker", 200)

        assert "expected dict, got int" in str(exc_info.value)

    def test_ensure_list_response_type_confusion_attack(self) -> None:
        """Test list validation against type confusion attacks."""
        # Test with dict instead of list
        with pytest.raises(APIError) as exc_info:
            ensure_list_response({"not": "a list"}, "positions", 200)

        assert "expected list, got dict" in str(exc_info.value)
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

        # Test with string instead of list
        with pytest.raises(APIError) as exc_info:
            ensure_list_response("not a list", "trades", 200)

        assert "expected list, got str" in str(exc_info.value)

    def test_ensure_string_response_dos_protection(self) -> None:
        """Test DoS protection for large string responses."""
        # Create a string larger than 1MB
        large_string = "x" * (1_000_001)

        # Should not raise but should log warning
        with patch("cyberdelta.apis.utils.response_validation.logger") as mock_logger:
            result = ensure_string_response(large_string, "large_response", 200)

            # Verify the string is returned (not truncated)
            assert result == large_string

            # Verify security warning was logged with structured logging format
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args
            # Check event name (first positional arg)
            assert call_args[0][0] == "response_validation_large_string"
            # Check keyword arguments
            assert call_args[1]["action"] == "validate_string_response"
            assert call_args[1]["message"] == "Large string response detected - potential DoS risk"
            assert call_args[1]["context"] == "large_response"
            assert call_args[1]["string_length"] == 1_000_001
            assert call_args[1]["security_alert"] is True

    def test_validate_required_fields_injection_attack(self) -> None:
        """Test required fields validation against injection attacks."""
        malicious_response = {
            "asset": "BTC",
            "__proto__": {"isAdmin": True},  # Prototype pollution attempt
            "quantity": "100",
        }

        # Should validate only specified fields, ignoring malicious extras
        validate_required_fields(
            malicious_response,
            ["asset", "quantity"],
            "balance",
            200,
        )  # Should not raise

        # But should fail if required field is missing
        with pytest.raises(APIError) as exc_info:
            validate_required_fields(
                malicious_response,
                ["asset", "quantity", "timestamp"],
                "balance",
                200,
            )

        assert "Missing required fields" in str(exc_info.value)
        assert "timestamp" in str(exc_info.value)

    def test_validate_response_not_empty_edge_cases(self) -> None:
        """Test empty response validation edge cases."""
        # Empty dict should raise
        with pytest.raises(APIError) as exc_info:
            validate_response_not_empty({}, "empty_dict", 200)

        assert "Empty response" in str(exc_info.value)

        # Empty list should raise
        with pytest.raises(APIError) as exc_info:
            validate_response_not_empty([], "empty_list", 200)

        assert "Empty response" in str(exc_info.value)

        # Non-empty should not raise
        validate_response_not_empty({"data": "value"}, "valid_dict", 200)
        validate_response_not_empty([1, 2, 3], "valid_list", 200)

    def test_nested_attack_vectors(self) -> None:
        """Test validation against nested attack vectors."""
        # Deeply nested structure that could cause stack overflow
        nested_dict: dict[str, Any] = {"level": 0}
        current: dict[str, Any] = nested_dict
        for i in range(1000):
            current["next"] = {"level": i + 1}
            current = current["next"]

        # Should handle without crashing
        result = ensure_dict_response(nested_dict, "nested_attack", 200)
        assert result is nested_dict

    def test_unicode_and_encoding_attacks(self) -> None:
        """Test validation against unicode and encoding attacks."""
        # Unicode direction override characters
        malicious_unicode = {
            "asset": "BTC\u202e\u0041\u0054\u0045",  # Right-to-left override
            "quantity": "100",
        }

        # Should pass validation (content validation is mapper's job)
        result = ensure_dict_response(malicious_unicode, "unicode_test", 200)
        assert result is malicious_unicode

        # Null byte injection
        null_byte_dict = {"asset": "BTC\x00INJECTED", "quantity": "100"}

        result = ensure_dict_response(null_byte_dict, "null_byte_test", 200)
        assert result is null_byte_dict

    @patch("cyberdelta.apis.utils.response_validation.logger")
    def test_security_logging_consistency(self, mock_logger: MagicMock) -> None:
        """Test that security events are consistently logged."""
        # Test null response logging with structured format
        with pytest.raises(APIError):
            ensure_dict_response(None, "test_null", 400)

        mock_logger.error.assert_called()
        call_args = mock_logger.error.call_args
        # Check event name (first positional arg)
        assert call_args[0][0] == "response_validation_null_response"
        # Check keyword arguments
        assert call_args[1]["action"] == "validate_dict_response"
        assert call_args[1]["message"] == "Null response received - security violation"
        assert call_args[1]["context"] == "test_null"
        assert call_args[1]["security_alert"] is True

        # Test type mismatch logging (old string format)
        mock_logger.reset_mock()
        with pytest.raises(APIError):
            ensure_dict_response("wrong_type", "test_type", 500)

        mock_logger.error.assert_called()
        error_call = mock_logger.error.call_args[0][0]
        assert "SECURITY: Type mismatch" in error_call
        assert "test_type" in error_call

    def test_error_message_information_leakage(self) -> None:
        """Test that error messages don't leak sensitive information."""
        # Ensure error messages are generic enough not to reveal internals
        with pytest.raises(APIError) as exc_info:
            ensure_dict_response(None, "sensitive_operation", 403)

        error_message = str(exc_info.value)
        # Should not contain implementation details
        assert "ParsedJsonResponse" not in error_message
        assert "cyberdelta" not in error_message
        assert "validation.py" not in error_message

        # Should contain user-friendly message
        assert "No data received" in error_message
        assert "sensitive_operation" in error_message

    def test_concurrent_validation_safety(self) -> None:
        """Test that validation functions are thread-safe."""
        results = []
        errors = []

        def validate_concurrently(value: dict[str, Any] | str, expected_error: bool) -> None:
            try:
                if expected_error:
                    ensure_dict_response(value, f"thread_{threading.current_thread().name}", 200)
                else:
                    result = ensure_dict_response(
                        value,
                        f"thread_{threading.current_thread().name}",
                        200,
                    )
                    results.append(result)
            except APIError as e:
                errors.append(e)

        # Create multiple threads with different inputs
        threads = []
        for i in range(10):
            if i % 2 == 0:
                # Valid dict
                thread = threading.Thread(target=validate_concurrently, args=({"thread": i}, False))
            else:
                # Invalid input
                thread = threading.Thread(target=validate_concurrently, args=("invalid", True))
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify results
        assert len(results) == 5  # 5 valid dicts
        assert len(errors) == 5  # 5 errors from invalid inputs

        # Check that each error has correct type
        for error in errors:
            assert error.code == APIErrorCode.INVALID_RESPONSE.value
            assert "expected dict, got str" in str(error)

    def test_malformed_json_like_structures(self) -> None:
        """Test validation against malformed JSON-like structures."""
        # Circular reference (though Python dicts can't truly have this)
        circular_dict: dict[str, Any] = {"a": 1}
        circular_dict["self"] = circular_dict  # Creates a reference, not true circular

        # Should handle without infinite loop
        result = ensure_dict_response(circular_dict, "circular_test", 200)
        assert result is circular_dict

        # Very large dict
        large_dict = {str(i): i for i in range(10000)}
        large_result = ensure_dict_response(large_dict, "large_dict_test", 200)
        assert len(large_result) == 10000

    def test_special_python_objects(self) -> None:
        """Test validation against special Python objects."""
        # Test with None values in dict
        dict_with_none = {"asset": "BTC", "quantity": None}
        result = ensure_dict_response(dict_with_none, "none_value_test", 200)
        assert result is dict_with_none

        # Test with boolean values
        dict_with_bool = {"success": True, "active": False}
        result = ensure_dict_response(dict_with_bool, "bool_test", 200)
        assert result is dict_with_bool

        # Test with mixed types in list
        mixed_list = [1, "two", 3.0, None, True, {"nested": "dict"}]
        mixed_result = ensure_list_response(mixed_list, "mixed_list_test", 200)
        assert mixed_result is mixed_list


class TestValidationPerformance:
    """Test performance characteristics of validation utilities."""

    def test_large_response_performance(self) -> None:
        """Test that validation handles large responses efficiently."""
        # Create a large list
        large_list = [{"id": i, "data": f"item_{i}"} for i in range(100000)]

        start_time = time.time()
        result = ensure_list_response(large_list, "performance_test", 200)
        end_time = time.time()

        # Should complete quickly (under 100ms)
        assert (end_time - start_time) < 0.1
        assert result is large_list

        # Create a large dict
        large_dict = {str(i): {"data": f"value_{i}"} for i in range(50000)}

        start_time = time.time()
        dict_result = ensure_dict_response(large_dict, "performance_test", 200)
        end_time = time.time()

        # Should complete quickly
        assert (end_time - start_time) < 0.1
        assert dict_result is large_dict


class TestValidationErrorHandling:
    """Test error handling in validation utilities."""

    def test_error_http_status_propagation(self) -> None:
        """Test that HTTP status codes are properly propagated in errors."""
        test_cases = [
            (400, "Bad Request"),
            (401, "Unauthorized"),
            (403, "Forbidden"),
            (404, "Not Found"),
            (429, "Too Many Requests"),
            (500, "Internal Server Error"),
            (503, "Service Unavailable"),
        ]

        for status_code, description in test_cases:
            with pytest.raises(APIError) as exc_info:
                ensure_dict_response(None, description, status_code)

            assert exc_info.value.http_status == status_code
            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    def test_context_preservation_in_errors(self) -> None:
        """Test that context is preserved in error messages."""
        contexts = [
            "ticker (BTC-USD)",
            "order placement for user 12345",
            "balance update",
            "position liquidation",
            "funding rate calculation",
        ]

        for context in contexts:
            with pytest.raises(APIError) as exc_info:
                ensure_dict_response("invalid", context, 200)

            assert context in str(exc_info.value)

    def test_validate_required_fields_edge_cases(self) -> None:
        """Test edge cases for required fields validation."""
        # Empty required fields list
        validate_required_fields({"any": "data"}, [], "empty_required", 200)

        # Field with empty string value (should pass - existence check only)
        validate_required_fields(
            {"asset": "", "quantity": "0"},
            ["asset", "quantity"],
            "empty_values",
            200,
        )

        # Field with None value (should pass - field exists)
        validate_required_fields(
            {"asset": "BTC", "quantity": None},
            ["asset", "quantity"],
            "none_value",
            200,
        )

        # Nested field check (not supported, would fail)
        with pytest.raises(APIError):
            validate_required_fields(
                {"data": {"asset": "BTC"}},
                ["data.asset"],
                "nested_field",
                200,
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

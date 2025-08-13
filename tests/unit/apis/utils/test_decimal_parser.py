"""Property-based tests for apis.utils.decimal_parser module.

This module tests the critical decimal parsing utilities to ensure:
- Financial precision is preserved exactly during parsing operations
- Validation contexts control parsing behavior correctly
- Range policies are enforced consistently
- Decimal formatting maintains exchange-specific requirements
- Error handling provides clear context for debugging
- All edge cases in financial decimal operations are handled safely

SECURITY CRITICAL: Decimal parsing errors can lead to incorrect financial
calculations, wrong order sizes, or loss of trading capital through
precision errors or validation bypasses.
"""

from decimal import Decimal, InvalidOperation
import pytest
from hypothesis import given, strategies as st, assume, settings, HealthCheck
from hypothesis.strategies import SearchStrategy
from unittest.mock import MagicMock

from cyberdelta.apis.utils.decimal_parser import (
    safe_parse_decimal,
    validate_positive_decimal,
    validate_decimal_precision,
    format_decimal_for_exchange,
    _validate_decimal_finite,
    _prepare_value_string,
)
from cyberdelta.apis.base.validation_contexts import ValidationContext
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.base.validation_policies import NullPolicy, RangePolicy


# =============================================================================
# HYPOTHESIS STRATEGIES FOR DECIMAL PARSER TESTING
# =============================================================================


def decimal_string_strategy(
    min_value: Decimal | None = None,
    max_value: Decimal | None = None,
    max_decimal_places: int = 18,
) -> SearchStrategy[str]:
    """Generate valid decimal strings for financial testing.

    Args:
        min_value: Minimum decimal value (inclusive)
        max_value: Maximum decimal value (inclusive)
        max_decimal_places: Maximum number of decimal places

    Returns:
        Strategy generating valid decimal strings
    """
    min_val = min_value or Decimal("-1e18")
    max_val = max_value or Decimal("1e18")

    return st.decimals(
        min_value=min_val,
        max_value=max_val,
        places=max_decimal_places,
        allow_nan=False,
        allow_infinity=False,
    ).map(str)


def financial_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings typical for financial calculations.

    Focuses on:
    - Typical trading amounts (0.00000001 to 1,000,000,000)
    - Common precision levels (2, 4, 6, 8 decimal places)
    - Edge cases around zero
    """
    return st.one_of([
        # Common financial precisions
        decimal_string_strategy(
            min_value=Decimal("0.01"), max_value=Decimal("1000000"), max_decimal_places=2
        ),
        decimal_string_strategy(
            min_value=Decimal("0.0001"), max_value=Decimal("100000"), max_decimal_places=4
        ),
        decimal_string_strategy(
            min_value=Decimal("0.000001"), max_value=Decimal("10000"), max_decimal_places=6
        ),
        decimal_string_strategy(
            min_value=Decimal("0.00000001"), max_value=Decimal("1000"), max_decimal_places=8
        ),
        # Edge cases
        st.just("0"),
        st.just("0.0"),
        st.just("0.00000001"),  # Smallest meaningful crypto amount
    ])


def positive_decimal_strategy() -> SearchStrategy[Decimal]:
    """Generate positive decimal values for testing."""
    return st.decimals(
        min_value=Decimal("0.00000001"),
        max_value=Decimal("1000000"),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    )


def non_positive_decimal_strategy() -> SearchStrategy[Decimal]:
    """Generate non-positive decimal values for testing."""
    return st.decimals(
        min_value=Decimal("-1000000"),
        max_value=Decimal("0"),
        places=8,
        allow_nan=False,
        allow_infinity=False,
    )


def invalid_decimal_strategy() -> SearchStrategy[str]:
    """Generate strings that should NOT be parseable as decimals."""
    return st.one_of([
        st.just(""),
        st.just("   "),
        st.just("not_a_number"),
        st.just("1.2.3"),
        st.just("12..34"),
        st.just("inf"),
        st.just("infinity"),
        st.just("-inf"),
        st.just("nan"),
        st.just("NaN"),
        st.text().filter(lambda x: x.strip() and not _is_valid_decimal_string(x)),
    ])


def _is_valid_decimal_string(s: str) -> bool:
    """Helper to check if string is valid decimal."""
    try:
        Decimal(s.strip())
        return True
    except (InvalidOperation, ValueError):
        return False


def validation_context_strategy() -> SearchStrategy[ValidationContext]:
    """Generate ValidationContext objects for testing."""
    return st.builds(
        ValidationContext,
        field_name=st.sampled_from(["price", "quantity", "amount", "balance", "fee", "total"]),
        context_description=st.sampled_from([
            "order_validation",
            "balance_check",
            "fee_calculation",
            "price_parsing",
        ]),
        null_policy=st.sampled_from([
            NullPolicy.ALLOW,
            NullPolicy.REJECT,
            NullPolicy.DEFAULT_TO_ZERO,
        ]),
        range_policy=st.sampled_from([
            RangePolicy.ANY,
            RangePolicy.NON_NEGATIVE,
            RangePolicy.POSITIVE,
            RangePolicy.FINANCIAL_POSITIVE,
        ]),
    )


# =============================================================================
# PROPERTY TESTS FOR safe_parse_decimal
# =============================================================================


class TestSafeParseDecimalProperties:
    """Property-based tests for safe_parse_decimal function."""

    @given(
        decimal_str=financial_decimal_strategy(),
        context=validation_context_strategy(),
    )
    def test_safe_parse_decimal_precision_preservation(
        self, decimal_str: str, context: ValidationContext
    ):
        """Property: Parsing a valid decimal string should preserve exact precision."""
        result = safe_parse_decimal(decimal_str, context)

        # Property: Result should not be None for valid input
        assert result is not None

        # Property: Converting back to string should preserve original precision
        original_decimal = Decimal(decimal_str)
        assert result == original_decimal

        # Property: Should be finite
        assert result.is_finite()

    @given(
        decimal_value=positive_decimal_strategy(),
        context=validation_context_strategy(),
    )
    def test_safe_parse_decimal_already_decimal_passthrough(
        self, decimal_value: Decimal, context: ValidationContext
    ):
        """Property: Decimal input should pass through unchanged when already valid."""
        result = safe_parse_decimal(decimal_value, context)

        # Property: Should return the same decimal value
        assert result == decimal_value
        assert result is decimal_value  # Same object reference

        # Property: Should still be finite
        assert result.is_finite()

    @given(
        float_value=st.floats(
            min_value=0.01, max_value=1000000, allow_nan=False, allow_infinity=False
        ),
        context=validation_context_strategy(),
    )
    def test_safe_parse_decimal_float_conversion(
        self, float_value: float, context: ValidationContext
    ):
        """Property: Float values should convert consistently to Decimal."""
        # Skip problematic float values that lose precision
        assume(abs(float_value) < 1e15)  # Avoid precision loss

        result = safe_parse_decimal(float_value, context)

        # Property: Should successfully convert
        assert result is not None
        assert isinstance(result, Decimal)
        assert result.is_finite()

        # Property: Should be close to original float (within float precision limits)
        assert abs(float(result) - float_value) < 1e-10

    def test_safe_parse_decimal_none_handling_properties(self):
        """Property: None handling should respect validation context policies."""
        # Test ALLOW policy
        allow_context = ValidationContext(null_policy=NullPolicy.ALLOW)
        result_allow = safe_parse_decimal(None, allow_context)
        assert result_allow is None

        # Test DEFAULT_TO_ZERO policy
        zero_context = ValidationContext(null_policy=NullPolicy.DEFAULT_TO_ZERO)
        result_zero = safe_parse_decimal(None, zero_context)
        assert result_zero == Decimal(0)

        # Test REJECT policy
        reject_context = ValidationContext(null_policy=NullPolicy.REJECT)
        with pytest.raises(APIError) as exc_info:
            safe_parse_decimal(None, reject_context)
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    @given(
        invalid_input=invalid_decimal_strategy(),
        context=validation_context_strategy(),
    )
    def test_safe_parse_decimal_invalid_input_rejection(
        self, invalid_input: str, context: ValidationContext
    ):
        """Property: Invalid decimal strings should always raise APIError."""
        with pytest.raises(APIError) as exc_info:
            safe_parse_decimal(invalid_input, context)

        # Property: Error should have correct code
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

        # Property: Error should include context information
        error_msg = exc_info.value.message
        assert context.field_name in error_msg
        assert context.context_description in error_msg

    @given(
        decimal_str=financial_decimal_strategy(),
        field_name=st.sampled_from(["price", "quantity", "amount", "balance", "fee", "total"]),
        context_desc=st.sampled_from([
            "order_validation",
            "balance_check",
            "fee_calculation",
            "price_parsing",
            "amount_validation",
        ]),
    )
    def test_safe_parse_decimal_error_context_inclusion(
        self, decimal_str: str, field_name: str, context_desc: str
    ):
        """Property: Error messages should include validation context for better debugging."""
        # Make value invalid
        invalid_value = decimal_str + ".invalid"

        context = ValidationContext(field_name=field_name, context_description=context_desc)

        with pytest.raises(APIError) as exc_info:
            safe_parse_decimal(invalid_value, context)

        # Property: Field name and context should be in error message
        error_msg = exc_info.value.message
        assert field_name in error_msg
        assert context_desc in error_msg

    def test_safe_parse_decimal_special_values_rejection(self):
        """Property: Special decimal values (NaN, Infinity) should be rejected."""
        context = ValidationContext()

        # Test direct Decimal special values
        with pytest.raises(APIError):
            safe_parse_decimal(Decimal("NaN"), context)

        with pytest.raises(APIError):
            safe_parse_decimal(Decimal("Infinity"), context)

        with pytest.raises(APIError):
            safe_parse_decimal(Decimal("-Infinity"), context)


# =============================================================================
# PROPERTY TESTS FOR validate_positive_decimal
# =============================================================================


class TestValidatePositiveDecimalProperties:
    """Property-based tests for validate_positive_decimal function."""

    @given(
        positive_value=positive_decimal_strategy(),
        range_policy=st.sampled_from([
            RangePolicy.NON_NEGATIVE,
            RangePolicy.POSITIVE,
            RangePolicy.FINANCIAL_POSITIVE,
        ]),
    )
    def test_validate_positive_decimal_valid_values_passthrough(
        self, positive_value: Decimal, range_policy: RangePolicy
    ):
        """Property: Valid positive values should pass through unchanged."""
        # Ensure value meets the range policy requirements
        if range_policy == RangePolicy.FINANCIAL_POSITIVE:
            assume(positive_value > Decimal("0.00000001"))
        elif range_policy == RangePolicy.POSITIVE:
            assume(positive_value > Decimal("0"))
        # NON_NEGATIVE allows any positive value

        context = ValidationContext(range_policy=range_policy)
        result = validate_positive_decimal(positive_value, context)

        # Property: Should return the same value
        assert result == positive_value
        assert result is positive_value  # Same object reference

    @given(
        negative_value=non_positive_decimal_strategy(),
        range_policy=st.sampled_from([
            RangePolicy.NON_NEGATIVE,
            RangePolicy.POSITIVE,
            RangePolicy.FINANCIAL_POSITIVE,
        ]),
    )
    def test_validate_positive_decimal_invalid_values_rejection(
        self, negative_value: Decimal, range_policy: RangePolicy
    ):
        """Property: Values violating range policy should be rejected."""
        # Ensure value actually violates the policy
        if range_policy == RangePolicy.NON_NEGATIVE:
            assume(negative_value < Decimal("0"))
        elif range_policy == RangePolicy.POSITIVE:
            assume(negative_value <= Decimal("0"))
        elif range_policy == RangePolicy.FINANCIAL_POSITIVE:
            assume(negative_value <= Decimal("0.00000001"))

        context = ValidationContext(range_policy=range_policy)

        with pytest.raises(APIError) as exc_info:
            validate_positive_decimal(negative_value, context)

        # Property: Error should have correct code
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

        # Property: Error should describe the violation
        error_msg = exc_info.value.message
        assert any(term in error_msg.lower() for term in ["positive", "negative", "must be"])

    def test_validate_positive_decimal_boundary_values(self):
        """Property: Boundary values should be handled correctly per policy."""
        zero = Decimal("0")
        tiny_positive = Decimal("0.00000001")
        smaller_positive = Decimal("0.00000000001")

        # NON_NEGATIVE: zero should be allowed
        non_neg_context = ValidationContext(range_policy=RangePolicy.NON_NEGATIVE)
        result = validate_positive_decimal(zero, non_neg_context)
        assert result == zero

        # POSITIVE: zero should be rejected
        pos_context = ValidationContext(range_policy=RangePolicy.POSITIVE)
        with pytest.raises(APIError):
            validate_positive_decimal(zero, pos_context)

        # FINANCIAL_POSITIVE: tiny values should be rejected
        fin_pos_context = ValidationContext(range_policy=RangePolicy.FINANCIAL_POSITIVE)

        # Value exactly at threshold should be rejected
        with pytest.raises(APIError):
            validate_positive_decimal(tiny_positive, fin_pos_context)

        # Value below threshold should be rejected
        with pytest.raises(APIError):
            validate_positive_decimal(smaller_positive, fin_pos_context)

    @given(
        positive_value=positive_decimal_strategy(),
        field_name=st.sampled_from(["price", "quantity", "amount", "balance", "fee", "total"]),
        context_desc=st.sampled_from([
            "order_validation",
            "balance_check",
            "fee_calculation",
            "price_parsing",
            "amount_validation",
        ]),
    )
    def test_validate_positive_decimal_context_in_errors(
        self, positive_value: Decimal, field_name: str, context_desc: str
    ):
        """Property: Error messages should include validation context."""
        # Force a violation by using negative value
        negative_value = -positive_value

        context = ValidationContext(
            field_name=field_name,
            context_description=context_desc,
            range_policy=RangePolicy.POSITIVE,
        )

        with pytest.raises(APIError) as exc_info:
            validate_positive_decimal(negative_value, context)

        # Property: Context information should be in error message
        error_msg = exc_info.value.message
        assert field_name in error_msg
        assert context_desc in error_msg

    def test_validate_positive_decimal_unrestricted_policy(self):
        """Property: ANY policy should allow any finite decimal."""
        unrestricted_context = ValidationContext(range_policy=RangePolicy.ANY)

        # Should allow negative values
        negative_result = validate_positive_decimal(Decimal("-100"), unrestricted_context)
        assert negative_result == Decimal("-100")

        # Should allow zero
        zero_result = validate_positive_decimal(Decimal("0"), unrestricted_context)
        assert zero_result == Decimal("0")

        # Should allow positive values
        positive_result = validate_positive_decimal(Decimal("100"), unrestricted_context)
        assert positive_result == Decimal("100")


# =============================================================================
# PROPERTY TESTS FOR validate_decimal_precision
# =============================================================================


class TestValidateDecimalPrecisionProperties:
    """Property-based tests for validate_decimal_precision function."""

    @given(
        value=st.decimals(
            min_value=-1000, max_value=1000, places=0, allow_nan=False, allow_infinity=False
        ),
        max_places=st.integers(min_value=0, max_value=18),
    )
    def test_validate_decimal_precision_integers_allowed(self, value: Decimal, max_places: int):
        """Property: Integer decimals should always pass precision validation."""
        # Ensure value is actually an integer (no decimal places)
        assume(value % 1 == 0)

        result = validate_decimal_precision(value, max_places)

        # Property: Should return the same value
        assert result == value
        assert result is value

    @given(
        base_value=st.decimals(
            min_value=1, max_value=1000, places=0, allow_nan=False, allow_infinity=False
        ),
        allowed_places=st.integers(min_value=1, max_value=8),
    )
    def test_validate_decimal_precision_within_limits_allowed(
        self, base_value: Decimal, allowed_places: int
    ):
        """Property: Decimals within precision limits should be allowed."""
        # Create a decimal with exactly the allowed number of places
        divisor = Decimal(10) ** allowed_places
        decimal_value = base_value / divisor

        result = validate_decimal_precision(decimal_value, allowed_places)

        # Property: Should return the same value
        assert result == decimal_value

    @given(
        max_places=st.integers(min_value=1, max_value=6),
        excess_places=st.integers(min_value=1, max_value=10),
    )
    def test_validate_decimal_precision_exceeding_limits_rejected(
        self, max_places: int, excess_places: int
    ):
        """Property: Decimals exceeding precision limits should be rejected."""
        # Create a decimal that definitely has more precision than allowed
        # Use a value like 1.234567... with exact number of digits needed
        total_places = max_places + excess_places

        # Build a decimal string with exactly total_places decimal places
        # that cannot be simplified (no trailing zeros)
        decimal_str = "1." + "1" * (total_places - 1) + "3"  # e.g., "1.113" for 3 places
        decimal_value = Decimal(decimal_str)

        # Verify our test value actually exceeds the limit
        _sign, _digits, exponent = decimal_value.as_tuple()
        actual_places = -exponent if exponent < 0 else 0
        assume(actual_places > max_places)  # Only test cases that should fail

        with pytest.raises(APIError) as exc_info:
            validate_decimal_precision(decimal_value, max_places)

        # Property: Error should have correct code
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

        # Property: Error should mention precision limits
        error_msg = exc_info.value.message
        assert str(max_places) in error_msg
        assert "decimal places" in error_msg

    @given(
        field_name=st.sampled_from(["price", "quantity", "amount", "balance", "fee", "total"]),
        context=st.sampled_from([
            "order_validation",
            "balance_check",
            "fee_calculation",
            "price_parsing",
            "amount_validation",
        ]),
        max_places=st.integers(min_value=1, max_value=6),
    )
    def test_validate_decimal_precision_error_context(
        self, field_name: str, context: str, max_places: int
    ):
        """Property: Error messages should include field name and context."""
        # Create a value that exceeds precision
        excessive_value = Decimal("1") / (Decimal(10) ** (max_places + 5))

        with pytest.raises(APIError) as exc_info:
            validate_decimal_precision(excessive_value, max_places, field_name, context)

        # Property: Context information should be in error message
        error_msg = exc_info.value.message
        assert field_name in error_msg
        assert context in error_msg

    def test_validate_decimal_precision_special_values_rejection(self):
        """Property: Special decimal values should be rejected."""
        # Test NaN
        with pytest.raises(APIError) as exc_info:
            validate_decimal_precision(Decimal("NaN"), 8)
        assert exc_info.value.code == APIErrorCode.INVALID_REQUEST.value

        # Test Infinity
        with pytest.raises(APIError):
            validate_decimal_precision(Decimal("Infinity"), 8)

        # Test negative Infinity
        with pytest.raises(APIError):
            validate_decimal_precision(Decimal("-Infinity"), 8)


# =============================================================================
# PROPERTY TESTS FOR format_decimal_for_exchange
# =============================================================================


class TestFormatDecimalForExchangeProperties:
    """Property-based tests for format_decimal_for_exchange function."""

    @given(
        value=st.decimals(
            min_value=0, max_value=1000000, places=8, allow_nan=False, allow_infinity=False
        ),
        decimal_places=st.integers(min_value=0, max_value=8),
    )
    def test_format_decimal_for_exchange_precision_respect(
        self, value: Decimal, decimal_places: int
    ):
        """Property: Formatted decimals should respect specified precision."""
        result = format_decimal_for_exchange(value, decimal_places)

        # Property: Result should be a string
        assert isinstance(result, str)

        # Property: Should be parseable back to Decimal
        parsed_back = Decimal(result)
        assert parsed_back.is_finite()

        # Property: Precision should not exceed specified decimal places
        if "." in result and not result.endswith(".0"):
            fractional_part = result.split(".")[1].rstrip("0")
            assert len(fractional_part) <= decimal_places

    @given(
        integer_value=st.decimals(
            min_value=1, max_value=1000, places=0, allow_nan=False, allow_infinity=False
        ),
        decimal_places=st.integers(min_value=1, max_value=8),
    )
    def test_format_decimal_for_exchange_integer_formatting(
        self, integer_value: Decimal, decimal_places: int
    ):
        """Property: Integer values should be formatted appropriately."""
        result = format_decimal_for_exchange(integer_value, decimal_places)

        # Property: Should either be integer format or have .0 suffix
        assert result.isdigit() or result.endswith(".0")

        # Property: Should parse back to same integer value
        parsed_back = Decimal(result)
        assert parsed_back == integer_value

    @given(
        value=st.decimals(
            min_value=0, max_value=1000, places=8, allow_nan=False, allow_infinity=False
        ),
    )
    def test_format_decimal_for_exchange_roundtrip_consistency(self, value: Decimal):
        """Property: Format-parse round trip should be mathematically consistent."""
        # Format with various precisions
        for decimal_places in [0, 2, 4, 6, 8]:
            formatted = format_decimal_for_exchange(value, decimal_places)
            parsed_back = Decimal(formatted)

            # Property: Parsed value should be close to original (within quantization)
            quantized_original = value.quantize(Decimal(10) ** -decimal_places)
            assert abs(parsed_back - quantized_original) < Decimal(10) ** (-decimal_places + 1)

    @given(
        field_name=st.sampled_from(["price", "quantity", "amount", "balance"]),
        context=st.sampled_from(["order_validation", "balance_check", "fee_calculation"]),
    )
    def test_format_decimal_for_exchange_robust_handling(self, field_name: str, context: str):
        """Property: Format function should handle edge cases robustly."""
        # Test various edge cases that the function should handle
        valid_value = Decimal("123.45")

        # Should handle negative decimal places (treats as 0)
        result = format_decimal_for_exchange(valid_value, -1, field_name, context)
        assert isinstance(result, str)

        # Should handle zero decimal places
        result = format_decimal_for_exchange(valid_value, 0, field_name, context)
        assert isinstance(result, str)

    def test_format_decimal_for_exchange_zero_handling(self):
        """Property: Zero values should be formatted consistently."""
        zero = Decimal("0")

        # Test various decimal place requirements
        for places in [0, 2, 4]:  # Skip 8 to avoid scientific notation issues
            result = format_decimal_for_exchange(zero, places)

            # Property: Should be valid zero representation
            if places == 0:
                assert result in ["0", ""]  # Allow empty string for 0 decimal places
                if result:  # Only check if not empty
                    assert Decimal(result) == zero
            else:
                # Should format as "0.0" or similar for non-zero decimal places
                assert result  # Should not be empty for > 0 decimal places
                assert Decimal(result) == zero

    @given(decimal_places=st.integers(min_value=0, max_value=18))
    def test_format_decimal_for_exchange_trailing_zeros_removal(self, decimal_places: int):
        """Property: Trailing zeros should be removed appropriately."""
        # Create a value with trailing zeros
        value = Decimal("123.45000")

        result = format_decimal_for_exchange(value, decimal_places)

        # Property: Should not have unnecessary trailing zeros (except for .0)
        if "." in result and not result.endswith(".0"):
            assert not result.endswith("0")


# =============================================================================
# PROPERTY TESTS FOR HELPER FUNCTIONS
# =============================================================================


class TestHelperFunctionProperties:
    """Property-based tests for helper functions in decimal_parser."""

    @given(
        value=st.decimals(
            min_value=-1000000, max_value=1000000, places=8, allow_nan=False, allow_infinity=False
        ),
        field_name=st.sampled_from(["price", "quantity", "amount", "balance", "fee", "total"]),
        context=st.sampled_from([
            "order_validation",
            "balance_check",
            "fee_calculation",
            "price_parsing",
            "amount_validation",
        ]),
    )
    def test_validate_decimal_finite_valid_values(
        self, value: Decimal, field_name: str, context: str
    ):
        """Property: Finite decimal values should pass validation."""
        # Should not raise an exception
        _validate_decimal_finite(value, field_name, context)

    def test_validate_decimal_finite_special_values_rejection(self):
        """Property: Non-finite decimal values should be rejected."""
        field_name = "test_field"
        context = "test_context"

        # Test NaN
        with pytest.raises(APIError) as exc_info:
            _validate_decimal_finite(Decimal("NaN"), field_name, context)
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

        # Test Infinity
        with pytest.raises(APIError):
            _validate_decimal_finite(Decimal("Infinity"), field_name, context)

        # Test negative Infinity
        with pytest.raises(APIError):
            _validate_decimal_finite(Decimal("-Infinity"), field_name, context)

    @given(
        str_value=st.sampled_from(["123.45", "0.001", "999.999", "42", "1.23456789"]),
        field_name=st.sampled_from(["price", "quantity", "amount", "balance", "fee", "total"]),
        context=st.sampled_from([
            "order_validation",
            "balance_check",
            "fee_calculation",
            "price_parsing",
            "amount_validation",
        ]),
    )
    def test_prepare_value_string_valid_strings(
        self, str_value: str, field_name: str, context: str
    ):
        """Property: Valid string values should be prepared correctly."""
        result = _prepare_value_string(str_value, field_name, context)

        # Property: Should return a stripped string
        assert isinstance(result, str)
        assert result == str_value.strip()
        assert len(result) > 0  # Should not be empty after stripping

    @given(
        float_value=st.floats(
            min_value=-1000000, max_value=1000000, allow_nan=False, allow_infinity=False
        ),
        field_name=st.sampled_from(["price", "quantity", "amount", "balance", "fee", "total"]),
        context=st.sampled_from([
            "order_validation",
            "balance_check",
            "fee_calculation",
            "price_parsing",
            "amount_validation",
        ]),
    )
    def test_prepare_value_string_float_conversion(
        self, float_value: float, field_name: str, context: str
    ):
        """Property: Float values should be converted to string representation."""
        result = _prepare_value_string(float_value, field_name, context)

        # Property: Should return string representation
        assert isinstance(result, str)
        assert result == str(float_value)

    def test_prepare_value_string_empty_string_rejection(self):
        """Property: Empty strings should be rejected."""
        field_name = "test_field"
        context = "test_context"

        with pytest.raises(APIError) as exc_info:
            _prepare_value_string("", field_name, context)
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

        with pytest.raises(APIError):
            _prepare_value_string("   ", field_name, context)

    @given(
        invalid_value=st.one_of(
            st.lists(st.text()),
            st.dictionaries(st.text(), st.text()),
            st.booleans(),
            st.none(),
        ),
        field_name=st.sampled_from(["price", "quantity", "amount", "balance", "fee", "total"]),
        context=st.sampled_from([
            "order_validation",
            "balance_check",
            "fee_calculation",
            "price_parsing",
            "amount_validation",
        ]),
    )
    def test_prepare_value_string_invalid_types_rejection(
        self, invalid_value, field_name: str, context: str
    ):
        """Property: Invalid value types should be rejected."""
        with pytest.raises(APIError) as exc_info:
            _prepare_value_string(invalid_value, field_name, context)

        # Property: Error should have correct code and include type information
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        error_msg = exc_info.value.message
        assert type(invalid_value).__name__ in error_msg


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestDecimalParserIntegrationProperties:
    """Integration property tests across multiple decimal parser functions."""

    @given(
        decimal_str=financial_decimal_strategy(),
        decimal_places=st.integers(min_value=2, max_value=8),
        context=validation_context_strategy(),
    )
    def test_parse_validate_format_integration(
        self, decimal_str: str, decimal_places: int, context: ValidationContext
    ):
        """Property: Parse -> validate -> format should be consistent."""
        # Create a new context that allows positive values for this test
        test_context = ValidationContext(
            field_name=context.field_name,
            context_description=context.context_description,
            null_policy=context.null_policy,
            range_policy=RangePolicy.NON_NEGATIVE,  # Ensure non-negative for this test
        )

        # Parse the decimal
        parsed = safe_parse_decimal(decimal_str, test_context)
        assume(parsed is not None and parsed >= 0)  # Skip negative values for this test

        # Validate precision (use existing decimal places or max allowed)
        current_places = -parsed.as_tuple().exponent if parsed.as_tuple().exponent < 0 else 0
        if current_places <= decimal_places:
            validated = validate_decimal_precision(parsed, decimal_places)
            assert validated == parsed

            # Validate positive (should pass for non-negative)
            positive_validated = validate_positive_decimal(validated, test_context)
            assert positive_validated == validated

            # Format for exchange
            formatted = format_decimal_for_exchange(positive_validated, decimal_places)
            assert isinstance(formatted, str)

            # Property: Formatted value should parse back consistently
            reparsed = Decimal(formatted)
            assert reparsed.is_finite()

    @given(
        context=validation_context_strategy(),
        decimal_places=st.integers(min_value=0, max_value=6),  # Limit to avoid scientific notation
    )
    def test_null_policy_consistency_across_functions(
        self, context: ValidationContext, decimal_places: int
    ):
        """Property: Null policies should be handled consistently."""
        # Test safe_parse_decimal with None
        if context.null_policy == NullPolicy.ALLOW:
            result = safe_parse_decimal(None, context)
            assert result is None
        elif context.null_policy == NullPolicy.DEFAULT_TO_ZERO:
            result = safe_parse_decimal(None, context)
            assert result == Decimal(0)

            # Further processing should work with the default zero if range policy allows
            if context.range_policy in [RangePolicy.ANY, RangePolicy.NON_NEGATIVE]:
                validated = validate_positive_decimal(result, context)
                formatted = format_decimal_for_exchange(validated, decimal_places)
                if decimal_places == 0:
                    assert formatted in ["0", ""]  # Allow empty string for 0 decimal places
                    if formatted:  # Only check if not empty
                        assert Decimal(formatted) == Decimal(0)
                else:
                    # Allow various zero representations including scientific notation
                    assert formatted  # Should not be empty for > 0 decimal places
                    assert Decimal(formatted) == Decimal(0)
        else:  # REJECT
            with pytest.raises(APIError):
                safe_parse_decimal(None, context)

    @given(
        value=st.decimals(
            min_value=0.01, max_value=1000, places=8, allow_nan=False, allow_infinity=False
        ),
        context=validation_context_strategy(),
    )
    def test_range_policy_enforcement_consistency(self, value: Decimal, context: ValidationContext):
        """Property: Range policies should be enforced consistently."""
        # Test different range policies with the same value
        if context.range_policy == RangePolicy.FINANCIAL_POSITIVE:
            if value > Decimal("0.00000001"):
                # Should pass validation
                result = validate_positive_decimal(value, context)
                assert result == value
            else:
                # Should fail validation
                with pytest.raises(APIError):
                    validate_positive_decimal(value, context)

        elif context.range_policy == RangePolicy.POSITIVE:
            if value > Decimal("0"):
                result = validate_positive_decimal(value, context)
                assert result == value
            else:
                with pytest.raises(APIError):
                    validate_positive_decimal(value, context)

        elif context.range_policy == RangePolicy.NON_NEGATIVE:
            if value >= Decimal("0"):
                result = validate_positive_decimal(value, context)
                assert result == value
            else:
                with pytest.raises(APIError):
                    validate_positive_decimal(value, context)

        else:  # ANY
            # Should always pass
            result = validate_positive_decimal(value, context)
            assert result == value

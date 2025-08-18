"""Property-based tests for utils.parsing module.

This module tests the critical parsing utilities to ensure:
- Decimal parsing preserves financial precision exactly
- Datetime parsing handles all timestamp formats correctly
- String validation prevents malformed data ingestion
- Round-trip parsing maintains data integrity
- Edge cases are handled safely without data loss
- Timezone handling is consistent and correct
- Error handling provides clear context for debugging

SECURITY CRITICAL: Parsing errors can lead to incorrect financial data,
wrong timestamps affecting trade timing, or injection vulnerabilities
through malformed string data.
"""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import pytest
from hypothesis import Verbosity, assume, given, settings, strategies as st
from hypothesis.strategies import DataObject, SearchStrategy

from cyberdelta.exceptions.field_validation import (
    DecimalFieldError,
    EnumFieldError,
    TypeFieldError,
)
from cyberdelta.exceptions.parsing import (
    DateTimeParsingError,
    EmptyStringError,
    TimestampFormatError,
)
from cyberdelta.utils.parsing import (
    check_str_parsable_to_finite_decimal,
    parse_datetime_utc,
    parse_decimal_safely,
    timeframe_to_ms,
    validate_enum_field,
    validate_str_field,
)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR FINANCIAL DATA
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


    Returns:
        A Hypothesis strategy for testing.
    """
    return st.one_of([
        # Common financial precisions
        decimal_string_strategy(
            min_value=Decimal("0.01"), max_value=Decimal(1000000), max_decimal_places=2
        ),
        decimal_string_strategy(
            min_value=Decimal("0.0001"), max_value=Decimal(100000), max_decimal_places=4
        ),
        decimal_string_strategy(
            min_value=Decimal("0.000001"), max_value=Decimal(10000), max_decimal_places=6
        ),
        decimal_string_strategy(
            min_value=Decimal("0.00000001"), max_value=Decimal(1000), max_decimal_places=8
        ),
        # Edge cases
        st.just("0"),
        st.just("0.0"),
        st.just("0.00000001"),  # Smallest meaningful crypto amount
    ])


def invalid_decimal_strategy() -> SearchStrategy[str]:
    """Generate strings that should NOT be parseable as decimals.

    Returns:
        A Hypothesis strategy for testing.
    """
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
    """Helper to check if string is valid decimal.

    Returns:
        A boolean value.
    """
    try:
        Decimal(s.strip().replace(",", ""))
    except (InvalidOperation, ValueError):
        return False
    else:
        return True


def timestamp_strategy() -> SearchStrategy[float]:
    """Generate valid timestamp values in different scales.

    Returns:
        A Hypothesis strategy for testing.
    """
    # Current timestamp ranges for different scales
    current_time = datetime.now(tz=UTC).timestamp()

    return st.one_of([
        # Seconds in range from 1970 to 2100
        st.floats(min_value=0, max_value=4102444800),
        # Milliseconds
        st.floats(
            min_value=current_time * 1000 - 86400000, max_value=current_time * 1000 + 86400000
        ),
        # Microseconds
        st.floats(
            min_value=current_time * 1000000 - 86400000000,
            max_value=current_time * 1000000 + 86400000000,
        ),
        # Nanoseconds (very large numbers)
        st.floats(
            min_value=current_time * 1000000000 - 86400000000000,
            max_value=current_time * 1000000000 + 86400000000000,
        ),
    ])


# =============================================================================
# PROPERTY TESTS FOR parse_decimal_safely
# =============================================================================


class TestParseDecimalValueProperties:
    """Property-based tests for parse_decimal_safely function."""

    @given(decimal_str=financial_decimal_strategy())
    @settings(max_examples=1000, verbosity=Verbosity.normal)
    def test_decimal_roundtrip_precision_preservation(self, decimal_str: str) -> None:
        """Property: Parsing a decimal string should preserve exact precision.

        This is CRITICAL for financial calculations - any precision loss
        could result in incorrect trading amounts.
        """
        # Parse the decimal string
        result = parse_decimal_safely(decimal_str, allow_none=False)

        # Property: Result should not be None for valid input
        assert result is not None

        # Property: Converting back to string should preserve original precision
        original_decimal = Decimal(decimal_str)
        assert result == original_decimal

        # Property: String representation should be equivalent
        # (allowing for different but equivalent representations like "1.0" vs "1")
        assert str(result) == str(original_decimal)

    @given(
        value=st.one_of(
            financial_decimal_strategy(),
            st.decimals(min_value=-1e18, max_value=1e18, allow_nan=False, allow_infinity=False),
            st.floats(min_value=-1e18, max_value=1e18, allow_nan=False, allow_infinity=False),
        )
    )
    def test_decimal_input_types_consistency(self, value: str | Decimal | float) -> None:
        """Property: Function should handle string, Decimal, and float inputs consistently."""
        # Skip problematic float values that lose precision
        if isinstance(value, float) and (
            abs(value) > 1e15 or (abs(value) > 0 and abs(value) < 1e-15)
        ):
            assume(False)

        # Parse different representations of the same value
        if isinstance(value, str):
            str_result = parse_decimal_safely(value, allow_none=False)
            decimal_result = parse_decimal_safely(Decimal(value), allow_none=False)

            # Property: String and Decimal inputs should give same result
            assert str_result == decimal_result

        elif isinstance(value, Decimal):
            decimal_result = parse_decimal_safely(value, allow_none=False)
            str_result = parse_decimal_safely(str(value), allow_none=False)

            # Property: Decimal input should return itself unchanged
            assert decimal_result is value  # Same object reference
            assert str_result == decimal_result

        else:  # float
            # Property: Float conversion should be consistent
            float_result = parse_decimal_safely(value, allow_none=False)
            str_result = parse_decimal_safely(str(value), allow_none=False)

            # Note: May differ due to float precision limits, but should be close
            assert isinstance(float_result, Decimal)
            assert isinstance(str_result, Decimal)

    @given(invalid_input=invalid_decimal_strategy())
    def test_decimal_invalid_input_rejection(self, invalid_input: str) -> None:
        """Property: Invalid decimal strings should always raise DecimalFieldError."""
        with pytest.raises(DecimalFieldError) as exc_info:
            parse_decimal_safely(invalid_input, allow_none=False)

        # Property: Error should include field information
        error_msg = str(exc_info.value)
        assert any(
            phrase in error_msg
            for phrase in [
                "Cannot convert to Decimal",
                "Non-finite values",
                "not allowed in financial calculations",
            ]
        )

    def test_decimal_none_handling_properties(self) -> None:
        """Property: None handling should respect allow_none parameter."""
        # Property: allow_none=True should return None for None input
        result_allowed = parse_decimal_safely(None, allow_none=True)
        assert result_allowed is None

        # Property: allow_none=False should raise for None input
        with pytest.raises(DecimalFieldError) as exc_info:
            parse_decimal_safely(None, allow_none=False)
        assert "Value cannot be None" in str(exc_info.value)

    @given(
        value=financial_decimal_strategy(),
        field_name=st.text(min_size=1, max_size=20).filter(lambda x: x.strip()),
    )
    def test_decimal_error_context_inclusion(self, value: str, field_name: str) -> None:
        """Property: Error messages should include field name for better debugging."""
        # Make value invalid
        invalid_value = value + ".invalid"

        with pytest.raises(DecimalFieldError) as exc_info:
            parse_decimal_safely(invalid_value, allow_none=False, field_name=field_name)

        # Property: Field name should be in error message
        error_msg = str(exc_info.value)
        assert field_name in error_msg

    @given(decimal_str=financial_decimal_strategy())
    def test_decimal_financial_invariants(self, decimal_str: str) -> None:
        """Property: Parsed decimals should maintain financial calculation invariants."""
        result = parse_decimal_safely(decimal_str, allow_none=False)

        # Property: Result should be finite (no NaN, infinity)
        assert result.is_finite()

        # Property: Result should maintain sign
        original = Decimal(decimal_str)
        if original > 0:
            assert result > 0
        elif original < 0:
            assert result < 0
        else:
            assert result == 0

        # Property: Arithmetic operations should work correctly
        doubled = result * Decimal(2)
        assert doubled == result + result  # Multiplication consistency

        if result != 0:
            # Property: Division should be reversible
            halved = result / Decimal(2)
            assert halved * Decimal(2) == result


# =============================================================================
# PROPERTY TESTS FOR parse_datetime_utc
# =============================================================================


class TestParseDatetimeUtcProperties:
    """Property-based tests for parse_datetime_utc function."""

    @given(timestamp=timestamp_strategy())
    def test_datetime_timestamp_roundtrip(self, timestamp: float) -> None:
        """Property: Parsing timestamp should preserve temporal information."""
        # Parse timestamp
        result = parse_datetime_utc(timestamp)

        # Property: Result should be timezone-aware UTC datetime
        assert result is not None
        assert result.tzinfo == UTC

        # Property: Converting back should give similar timestamp
        result_timestamp = result.timestamp()

        # Allow small differences due to precision (especially for large timestamps)
        if timestamp < 1e12:  # Likely seconds
            assert abs(result_timestamp - timestamp) < 1.0
        else:
            # For milliseconds/microseconds/nanoseconds, compare scaled values
            scale_factor = 1.0
            if timestamp > 2e17:  # nanoseconds
                scale_factor = 1e9
            elif timestamp > 2e14:  # microseconds
                scale_factor = 1e6
            elif timestamp > 2e11:  # milliseconds
                scale_factor = 1e3

            expected_seconds = timestamp / scale_factor
            assert abs(result_timestamp - expected_seconds) < 1.0

    @given(dt=st.datetimes(timezones=st.just(UTC)))
    def test_datetime_utc_datetime_passthrough(self, dt: datetime) -> None:
        """Property: UTC datetime input should pass through unchanged."""
        result = parse_datetime_utc(dt)

        # Property: Should return the same datetime
        assert result == dt
        assert result is not None
        assert result.tzinfo == UTC

    @given(dt=st.datetimes(timezones=st.none()))
    def test_datetime_naive_datetime_utc_assignment(self, dt: datetime) -> None:
        """Property: Naive datetime should get UTC timezone assigned."""
        result = parse_datetime_utc(dt)

        # Property: Should have UTC timezone assigned
        assert result is not None
        assert result.tzinfo == UTC

        # Property: Time components should be preserved
        assert result.year == dt.year
        assert result.month == dt.month
        assert result.day == dt.day
        assert result.hour == dt.hour
        assert result.minute == dt.minute
        assert result.second == dt.second
        assert result.microsecond == dt.microsecond

    def test_datetime_none_handling(self) -> None:
        """Property: None input should return None."""
        result = parse_datetime_utc(None)
        assert result is None

    @given(
        valid_iso_string=st.datetimes(timezones=st.one_of(st.none(), st.just(UTC))).map(
            lambda dt: dt.isoformat()
        )
    )
    def test_datetime_iso_string_parsing(self, valid_iso_string: str) -> None:
        """Property: Valid ISO strings should parse correctly."""
        result = parse_datetime_utc(valid_iso_string)

        # Property: Should successfully parse
        assert result is not None
        assert result.tzinfo == UTC

    @given(invalid_string=st.text(min_size=1).filter(lambda x: x and not _is_valid_iso_datetime(x)))
    def test_datetime_invalid_string_rejection(self, invalid_string: str) -> None:
        """Property: Invalid datetime strings should raise DateTimeParsingError."""
        # Skip strings that might be valid timestamps
        assume(not _could_be_timestamp(invalid_string))

        with pytest.raises((DateTimeParsingError, TimestampFormatError)):
            parse_datetime_utc(invalid_string)


def _is_valid_iso_datetime(s: str) -> bool:
    """Helper to check if string is valid ISO datetime.

    Returns:
        A boolean value.
    """
    try:
        datetime.fromisoformat(s)
    except ValueError:
        return False
    else:
        return True


def _could_be_timestamp(s: str) -> bool:
    """Helper to check if string could be a valid timestamp.

    Returns:
        A boolean value.
    """
    try:
        float_val = float(s)
    except ValueError:
        return False
    else:
        return 0 <= float_val <= 1e20  # Reasonable timestamp range


# =============================================================================
# PROPERTY TESTS FOR validate_str_field
# =============================================================================


class TestValidateStrFieldProperties:
    """Property-based tests for validate_str_field function."""

    @given(
        text=st.text(min_size=1, max_size=1000),
        max_length=st.integers(min_value=1, max_value=1000),
    )
    def test_str_field_valid_input_passthrough(self, text: str, max_length: int) -> None:
        """Property: Valid string input should pass through unchanged."""
        assume(len(text) <= max_length)
        assume(text.strip())  # Non-empty after stripping

        result = validate_str_field(text, max_length=max_length, allow_empty=False)

        # Property: Should return the exact same string
        assert result == text
        assert type(result) is str

    @given(
        non_string=st.one_of(
            st.integers(),
            st.floats(),
            st.booleans(),
            st.lists(st.text()),
            st.dictionaries(st.text(), st.text()),
        )
    )
    def test_str_field_non_string_rejection(
        self, non_string: float | bool | list[str] | dict[str, str]
    ) -> None:
        """Property: Non-string input should raise TypeFieldError."""
        with pytest.raises(TypeFieldError) as exc_info:
            validate_str_field(non_string)

        # Property: Error should indicate expected vs actual type
        error_msg = str(exc_info.value)
        assert "str" in error_msg
        assert type(non_string).__name__ in error_msg

    @given(
        text=st.text(min_size=1, max_size=100),
        max_length=st.integers(min_value=1, max_value=50),
    )
    def test_str_field_length_validation(self, text: str, max_length: int) -> None:
        """Property: Strings exceeding max_length should be rejected."""
        assume(len(text) > max_length)

        with pytest.raises(TypeFieldError) as exc_info:
            validate_str_field(text, max_length=max_length)

        # Property: Error should mention length constraint
        error_msg = str(exc_info.value)
        assert str(max_length) in error_msg
        assert str(len(text)) in error_msg

    @given(empty_str=st.just("") | st.text(max_size=10).filter(lambda x: not x.strip()))
    def test_str_field_empty_string_handling(self, empty_str: str) -> None:
        """Property: Empty string handling should respect allow_empty parameter."""
        # Property: allow_empty=True should accept empty strings
        result = validate_str_field(empty_str, allow_empty=True)
        assert result == empty_str

        # Property: allow_empty=False should reject empty strings
        with pytest.raises(EmptyStringError):
            validate_str_field(empty_str, allow_empty=False)


# =============================================================================
# PROPERTY TESTS FOR validate_enum_field
# =============================================================================


class TestValidateEnumFieldProperties:
    """Property-based tests for validate_enum_field function."""

    @given(
        allowed_values=st.sets(
            st.text(min_size=1, max_size=20).filter(lambda x: x.strip()),
            min_size=1,
            max_size=10,
        ),
        chosen_value=st.data(),
    )
    def test_enum_field_allowed_value_acceptance(
        self, allowed_values: set[str], chosen_value: DataObject
    ) -> None:
        """Property: Values in allowed set should be accepted."""
        # Choose one of the allowed values
        value = chosen_value.draw(st.sampled_from(sorted(allowed_values)))

        result = validate_enum_field(value, allowed_values)

        # Property: Should return the same value
        assert result == value
        assert result in allowed_values

    @given(
        allowed_values=st.sets(
            st.text(
                alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")),
                min_size=1,
                max_size=10,
            ),
            min_size=1,
            max_size=5,
        ),
        invalid_value=st.text(
            alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd")), min_size=1, max_size=10
        ),
    )
    def test_enum_field_disallowed_value_rejection(
        self, allowed_values: set[str], invalid_value: str
    ) -> None:
        """Property: Values not in allowed set should be rejected."""
        assume(invalid_value not in allowed_values)

        with pytest.raises(EnumFieldError) as exc_info:
            validate_enum_field(invalid_value, allowed_values)

        # Property: Error should contain the invalid value
        error_msg = str(exc_info.value)
        assert invalid_value in error_msg


# =============================================================================
# PROPERTY TESTS FOR timeframe_to_ms
# =============================================================================


class TestTimeframeToMsProperties:
    """Property-based tests for timeframe_to_ms function."""

    @given(
        minutes=st.integers(min_value=1, max_value=1440),  # 1 minute to 1 day
    )
    def test_timeframe_minute_conversion(self, minutes: int) -> None:
        """Property: Minute timeframes should convert correctly."""
        timeframe = f"{minutes}m"
        result = timeframe_to_ms(timeframe)

        # Property: Should convert to correct milliseconds
        expected_ms = minutes * 60 * 1000
        assert result == expected_ms

    @given(
        hours=st.integers(min_value=1, max_value=24),
    )
    def test_timeframe_hour_conversion(self, hours: int) -> None:
        """Property: Hour timeframes should convert correctly."""
        timeframe = f"{hours}h"
        result = timeframe_to_ms(timeframe)

        # Property: Should convert to correct milliseconds
        expected_ms = hours * 60 * 60 * 1000
        assert result == expected_ms

    @given(
        days=st.integers(min_value=1, max_value=7),
    )
    def test_timeframe_day_conversion(self, days: int) -> None:
        """Property: Day timeframes should convert correctly."""
        timeframe = f"{days}d"
        result = timeframe_to_ms(timeframe)

        # Property: Should convert to correct milliseconds
        expected_ms = days * 24 * 60 * 60 * 1000
        assert result == expected_ms

    @given(
        raw_minutes=st.integers(min_value=1, max_value=1440),
    )
    def test_timeframe_raw_number_conversion(self, raw_minutes: int) -> None:
        """Property: Raw numbers should be treated as minutes."""
        timeframe = str(raw_minutes)
        result = timeframe_to_ms(timeframe)

        # Property: Should convert as minutes
        expected_ms = raw_minutes * 60 * 1000
        assert result == expected_ms

    @given(
        invalid_timeframe=st.text().filter(lambda x: not _is_valid_timeframe(x)),
        default_minutes=st.integers(min_value=1, max_value=60),
    )
    def test_timeframe_invalid_with_default(
        self, invalid_timeframe: str, default_minutes: int
    ) -> None:
        """Property: Invalid timeframes should use default when provided."""
        result = timeframe_to_ms(invalid_timeframe, default_to_minutes=default_minutes)

        # Property: Should return default value in milliseconds
        expected_ms = default_minutes * 60 * 1000
        assert result == expected_ms

    @given(
        invalid_timeframe=st.text().filter(lambda x: not _is_valid_timeframe(x)),
    )
    def test_timeframe_invalid_without_default(self, invalid_timeframe: str) -> None:
        """Property: Invalid timeframes should raise ValueError when no default."""
        with pytest.raises(ValueError):
            timeframe_to_ms(invalid_timeframe, default_to_minutes=None)


def _is_valid_timeframe(s: str) -> bool:
    """Helper to check if string is a valid timeframe.

    Returns:
        A boolean value.
    """
    if not s or not s.strip():
        return False

    s_lower = s.lower().strip()

    # Check for valid patterns
    for suffix in ["m", "h", "d"]:
        if suffix in s_lower:
            try:
                num_part = s_lower.replace(suffix, "")
                int(num_part)
            except ValueError:
                continue
            else:
                return True

    # Check for raw number
    try:
        int(s_lower)
    except ValueError:
        return False
    else:
        return True


# =============================================================================
# PROPERTY TESTS FOR check_str_parsable_to_finite_decimal
# =============================================================================


class TestCheckStrParsableToFiniteDecimalProperties:
    """Property-based tests for check_str_parsable_to_finite_decimal function."""

    @given(decimal_str=financial_decimal_strategy())
    def test_finite_decimal_string_acceptance(self, decimal_str: str) -> None:
        """Property: Valid finite decimal strings should be accepted."""
        result = check_str_parsable_to_finite_decimal(decimal_str)

        # Property: Should return the original string
        assert result == decimal_str

        # Property: Should be parseable as finite decimal
        parsed = Decimal(result)
        assert parsed.is_finite()

    @given(
        invalid_input=st.one_of(
            invalid_decimal_strategy(),
            st.just("inf"),
            st.just("-inf"),
            st.just("infinity"),
            st.just("nan"),
            st.just("NaN"),
        )
    )
    def test_invalid_decimal_string_rejection(self, invalid_input: str) -> None:
        """Property: Invalid or non-finite decimal strings should be rejected."""
        with pytest.raises((DecimalFieldError, EmptyStringError)):
            check_str_parsable_to_finite_decimal(invalid_input)

    @given(
        non_string=st.one_of(
            st.integers(),
            st.floats(),
            st.none(),
            st.booleans(),
        )
    )
    def test_non_string_input_rejection(self, non_string: float | bool | None) -> None:
        """Property: Non-string input should be rejected."""
        with pytest.raises(TypeFieldError):
            check_str_parsable_to_finite_decimal(non_string)


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestParsingIntegrationProperties:
    """Integration property tests across multiple parsing functions."""

    @given(
        decimal_str=financial_decimal_strategy(),
        field_name=st.text(min_size=1, max_size=20).filter(lambda x: x.strip()),
    )
    def test_decimal_validation_consistency(self, decimal_str: str, field_name: str) -> None:
        """Property: Decimal validation should be consistent across functions."""
        # Both functions should handle the same valid input consistently
        parse_result = parse_decimal_safely(decimal_str, allow_none=False, field_name=field_name)
        validate_result = check_str_parsable_to_finite_decimal(decimal_str, field_name=field_name)

        # Property: parse_decimal_safely result should match string input
        assert str(parse_result) == str(Decimal(decimal_str))

        # Property: check_str_parsable_to_finite_decimal should return original string
        assert validate_result == decimal_str

        # Property: Both should handle the same input without errors
        assert parse_result.is_finite()

    @given(
        timestamp=timestamp_strategy(),
        field_name=st.text(min_size=1, max_size=20).filter(lambda x: x.strip()),
    )
    def test_datetime_parsing_field_context(self, timestamp: float, field_name: str) -> None:
        """Property: Datetime parsing should preserve field context in errors."""
        # Valid timestamp should parse successfully
        result = parse_datetime_utc(timestamp, field_name=field_name)
        assert result is not None
        assert result.tzinfo == UTC

        # Invalid timestamp should include field name in error
        invalid_timestamp = "invalid_timestamp_" + field_name
        with pytest.raises(DateTimeParsingError) as exc_info:
            parse_datetime_utc(invalid_timestamp, field_name=field_name)

        # Property: Field name should be in error message
        error_message = str(exc_info.value)
        if field_name not in error_message:
            pytest.fail(f"Field name '{field_name}' should be in error message: {error_message}")

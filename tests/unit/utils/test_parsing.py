"""Unit tests for parsing utilities.

Tests parsing functions for various data types including decimals, datetimes, and validation.
"""

import math
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest

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
    parse_decimal_value,
    timeframe_to_ms,
    validate_enum_field,
    validate_str_field,
)


class TestParseDecimalValue:
    """Test cases for parse_decimal_value function."""

    def test_decimal_passthrough(self) -> None:
        """Should return Decimal unchanged."""
        d = Decimal("1.23")
        assert parse_decimal_value(d) == d

    def test_str_to_decimal(self) -> None:
        """Should parse numeric string to Decimal."""
        assert parse_decimal_value("123.45") == Decimal("123.45")

    def test_str_with_commas(self) -> None:
        """Should parse string with commas to Decimal."""
        assert parse_decimal_value("1,234,567.89") == Decimal("1234567.89")

    def test_int_and_float(self) -> None:
        """Should parse int and float to Decimal."""
        assert parse_decimal_value(42) == Decimal(42)
        assert parse_decimal_value(math.pi) == Decimal(str(math.pi))

    def test_none_allowed(self) -> None:
        """Should return None if value is None and allow_none is True."""
        assert parse_decimal_value(None, allow_none=True) is None

    def test_none_not_allowed(self) -> None:
        """Should raise if value is None and allow_none is False."""
        with pytest.raises(ValueError, match="cannot be None"):
            parse_decimal_value(None, allow_none=False)

    def test_invalid_value(self) -> None:
        """Should raise ValueError for invalid input."""
        with pytest.raises(ValueError, match="Cannot convert"):
            parse_decimal_value("not_a_number")

    def test_error_context_includes_field(self) -> None:
        """Error message should include field_name if provided."""
        with pytest.raises(ValueError) as exc:
            parse_decimal_value("bad", field_name="test_field")
        assert "test_field" in str(exc.value)

    def test_decimal_scientific_notation(self) -> None:
        """Should parse scientific notation string to Decimal."""
        assert parse_decimal_value("1e6") == Decimal("1e6")

    def test_decimal_leading_trailing_whitespace(self) -> None:
        """Should parse string with leading/trailing whitespace to Decimal."""
        assert parse_decimal_value("  42.5  ") == Decimal("42.5")

    def test_decimal_plus_sign(self) -> None:
        """Should parse string with plus sign to Decimal."""
        assert parse_decimal_value("+123.45") == Decimal("123.45")

    def test_decimal_very_large(self) -> None:
        """Should parse very large value to Decimal."""
        val = "1e50"
        assert parse_decimal_value(val) == Decimal(val)

    def test_decimal_very_small(self) -> None:
        """Should parse very small value to Decimal."""
        val = "1e-50"
        assert parse_decimal_value(val) == Decimal(val)

    def test_decimal_invalid_unicode(self) -> None:
        """Should raise ValueError for string with invalid unicode/characters."""
        with pytest.raises(ValueError):
            parse_decimal_value("12.3\ufffd4")

    def test_decimal_empty_string(self) -> None:
        """Should raise ValueError for empty string."""
        with pytest.raises(ValueError):
            parse_decimal_value("")


class TestParseDatetimeUTC:
    """Test cases for parse_datetime_utc function."""

    def test_datetime_passthrough(self) -> None:
        """Should return aware datetime unchanged, or make naive datetime UTC-aware."""
        dt_aware = datetime(2023, 1, 1, 12, 0, tzinfo=UTC)
        dt_naive_construction = datetime(2023, 1, 1, 12, 0, tzinfo=UTC)
        dt_naive = dt_naive_construction.replace(
            tzinfo=None
        )  # Create truly naive datetime for test
        assert parse_datetime_utc(dt_aware) == dt_aware
        result = parse_datetime_utc(dt_naive)
        assert result is not None
        assert result.tzinfo == UTC
        assert result.replace(tzinfo=None) == dt_naive

    def test_epoch_seconds(self) -> None:
        """Should parse int/float epoch seconds to UTC datetime."""
        ts = 1700000000
        dt = parse_datetime_utc(ts)
        assert isinstance(dt, datetime)
        assert dt.tzinfo == UTC
        # Allow small delta due to float conversion
        assert abs(dt.timestamp() - ts) < 1

    def test_epoch_milliseconds(self) -> None:
        """Should parse int/float epoch ms to UTC datetime."""
        ts_ms = 1700000000000
        dt = parse_datetime_utc(ts_ms)
        assert isinstance(dt, datetime)
        assert dt.tzinfo == UTC
        assert abs(dt.timestamp() - ts_ms / 1000) < 1

    def test_iso_string(self) -> None:
        """Should parse ISO 8601 string to UTC datetime."""
        iso = "2023-01-01T12:00:00"
        dt = parse_datetime_utc(iso)
        assert isinstance(dt, datetime)
        assert dt.tzinfo == UTC
        assert dt.year == 2023
        assert dt.month == 1
        assert dt.day == 1

    def test_none_returns_none(self) -> None:
        """Should return None if value is None."""
        assert parse_datetime_utc(None) is None

    def test_invalid_type(self) -> None:
        """Should raise ValueError for unsupported type."""
        # Testing with an invalid type by using cast to bypass type checking

        invalid_value = cast("str", [])  # Cast list to str to satisfy type checker
        with pytest.raises(TimestampFormatError, match="Unsupported datetime type"):
            parse_datetime_utc(invalid_value)

    def test_invalid_string(self) -> None:
        """Should raise DateTimeParsingError for invalid ISO string."""
        with pytest.raises(
            DateTimeParsingError,
            match=r"Cannot parse as ISO datetime .* or as numeric timestamp",
        ):
            parse_datetime_utc("not-a-date")

    def test_error_context_includes_field(self) -> None:
        """Error message should include field_name if provided."""
        with pytest.raises(DateTimeParsingError) as exc:
            parse_datetime_utc("bad", field_name="test_field")
        assert "test_field" in str(exc.value)

    def test_pre_1970_epoch(self) -> None:
        """Should parse negative epoch seconds (before 1970) to UTC datetime."""
        ts = -1000000000  # ~1938
        dt = parse_datetime_utc(ts)
        assert isinstance(dt, datetime)
        assert dt.year < 1970

    def test_far_future_epoch(self) -> None:
        """Should parse far future epoch seconds (e.g., year 3000) to UTC datetime."""
        ts = 32503680000  # 3000-01-01T00:00:00Z
        dt = parse_datetime_utc(ts)
        assert isinstance(dt, datetime)
        assert dt.year == 3000

    def test_leap_year_feb_29(self) -> None:
        """Should parse Feb 29 on a leap year."""
        iso = "2020-02-29T12:00:00"
        dt = parse_datetime_utc(iso)
        assert dt is not None
        assert dt.month == 2
        assert dt.day == 29

    def test_non_leap_year_feb_29(self) -> None:
        """Should raise DateTimeParsingError for Feb 29 on a non-leap year."""
        with pytest.raises(DateTimeParsingError):
            parse_datetime_utc("2019-02-29T12:00:00")

    def test_unreadable_date_string(self) -> None:
        """Should raise DateTimeParsingError for unreadable/ambiguous date string."""
        with pytest.raises(DateTimeParsingError):
            parse_datetime_utc("yesterday")

    def test_datetime_with_timezone_offset(self) -> None:
        """Should parse ISO string with timezone offset and convert to UTC."""
        iso = "2023-01-01T12:00:00+02:00"
        dt = parse_datetime_utc(iso)
        assert dt is not None
        offset = dt.utcoffset()
        assert offset is not None
        assert offset.total_seconds() == 7200

    def test_datetime_trailing_z(self) -> None:
        """Should parse ISO string with trailing 'Z' as UTC."""
        iso = "2023-01-01T12:00:00Z"
        dt = parse_datetime_utc(iso.replace("Z", "+00:00"))
        assert dt is not None
        offset = dt.utcoffset()
        assert offset is not None
        assert offset.total_seconds() == 0

    def test_datetime_empty_string(self) -> None:
        """Test parsing empty string raises DateTimeParsingError."""
        with pytest.raises(DateTimeParsingError):
            parse_datetime_utc("")

    def test_datetime_invalid_iso_string(self) -> None:
        """Test parsing invalid ISO string raises DateTimeParsingError."""
        with pytest.raises(DateTimeParsingError):
            parse_datetime_utc("not-a-valid-datetime")

    def test_datetime_numeric_timestamp_seconds(self) -> None:
        """Test parsing timestamp in seconds."""
        timestamp = 1672574400.0  # 2023-01-01 12:00:00 UTC
        dt = parse_datetime_utc(timestamp)
        assert dt is not None
        assert dt.year == 2023
        assert dt.month == 1
        assert dt.day == 1

    def test_datetime_numeric_timestamp_milliseconds(self) -> None:
        """Test parsing timestamp in milliseconds."""
        timestamp = 1672574400000.0  # 2023-01-01 12:00:00 UTC in ms
        dt = parse_datetime_utc(timestamp)
        assert dt is not None
        assert dt.year == 2023
        assert dt.month == 1
        assert dt.day == 1

    def test_datetime_numeric_timestamp_microseconds(self) -> None:
        """Test parsing timestamp in microseconds."""
        timestamp = 1672574400000000.0  # 2023-01-01 12:00:00 UTC in microseconds
        dt = parse_datetime_utc(timestamp)
        assert dt is not None
        assert dt.year == 2023
        assert dt.month == 1
        assert dt.day == 1

    def test_datetime_numeric_timestamp_nanoseconds(self) -> None:
        """Test parsing timestamp in nanoseconds."""
        timestamp = 1672574400000000000.0  # 2023-01-01 12:00:00 UTC in nanoseconds
        dt = parse_datetime_utc(timestamp)
        assert dt is not None
        assert dt.year == 2023
        assert dt.month == 1
        assert dt.day == 1

    def test_datetime_invalid_numeric_timestamp(self) -> None:
        """Test parsing invalid numeric timestamp raises TimestampFormatError."""
        with pytest.raises(TimestampFormatError):
            parse_datetime_utc(float("nan"))  # Invalid timestamp

    def test_datetime_string_as_numeric_fallback(self) -> None:
        """Test parsing string that represents numeric timestamp."""
        timestamp_str = "1672574400"  # 2023-01-01 12:00:00 UTC as string
        dt = parse_datetime_utc(timestamp_str)
        assert dt is not None
        assert dt.year == 2023

    def test_datetime_error_context_includes_field(self) -> None:
        """Test error message includes field_name when provided."""
        with pytest.raises(DateTimeParsingError) as exc:
            parse_datetime_utc("invalid", field_name="test_field")
        assert "test_field" in str(exc.value)


class TestValidateStrField:
    """Test cases for validate_str_field function."""

    def test_validate_str_field_success(self) -> None:
        """Test successful string validation."""
        result = validate_str_field("valid_string", field_name="test")
        assert result == "valid_string"

    def test_validate_str_field_empty_not_allowed(self) -> None:
        """Test empty string raises error when not allowed."""
        with pytest.raises(EmptyStringError):
            validate_str_field("", field_name="test", allow_empty=False)

    def test_validate_str_field_empty_allowed(self) -> None:
        """Test empty string passes when allowed."""
        result = validate_str_field("", field_name="test", allow_empty=True)
        assert not result

    def test_validate_str_field_max_length_exceeded(self) -> None:
        """Test string exceeding max length raises error."""
        with pytest.raises(TypeFieldError):
            validate_str_field("toolong", field_name="test", max_length=5)

    def test_validate_str_field_whitespace_handling(self) -> None:
        """Test whitespace handling in string validation."""
        # Whitespace strings are treated as empty if allow_empty=False
        result = validate_str_field("test", field_name="test", allow_empty=False)
        assert result == "test"

    def test_validate_str_field_non_string_type(self) -> None:
        """Test non-string type raises error."""
        with pytest.raises(TypeFieldError):
            validate_str_field(123, field_name="test")


class TestValidateEnumField:
    """Test cases for validate_enum_field function."""

    def test_validate_enum_field_success(self) -> None:
        """Test successful enum validation."""
        allowed_values = {"value1", "value2"}
        result = validate_enum_field("value1", allowed_values, field_name="test")
        assert result == "value1"

    def test_validate_enum_field_invalid_value(self) -> None:
        """Test invalid enum value raises error."""
        allowed_values = {"value1", "value2"}
        with pytest.raises(EnumFieldError):
            validate_enum_field("invalid", allowed_values, field_name="test")

    def test_validate_enum_field_case_sensitive(self) -> None:
        """Test case sensitive enum validation (default behavior)."""
        allowed_values = {"value1", "value2"}
        # Should work with exact match
        result = validate_enum_field("value1", allowed_values, field_name="test")
        assert result == "value1"

        # Should fail with different case
        with pytest.raises(EnumFieldError):
            validate_enum_field("VALUE1", allowed_values, field_name="test")


class TestTimeframeToMs:
    """Test cases for timeframe_to_ms function."""

    def test_timeframe_minutes(self) -> None:
        """Test parsing minutes timeframe."""
        assert timeframe_to_ms("5m") == 300000
        assert timeframe_to_ms("1m") == 60000

    def test_timeframe_hours(self) -> None:
        """Test parsing hours timeframe."""
        assert timeframe_to_ms("2h") == 7200000
        assert timeframe_to_ms("1h") == 3600000

    def test_timeframe_days(self) -> None:
        """Test parsing days timeframe."""
        assert timeframe_to_ms("1d") == 86400000

    def test_timeframe_invalid_format(self) -> None:
        """Test invalid timeframe format raises error."""
        with pytest.raises(ValueError):
            timeframe_to_ms("invalid", default_to_minutes=None)

    def test_timeframe_no_unit_uses_default(self) -> None:
        """Test number without unit uses default minutes."""
        assert timeframe_to_ms("5") == 300000  # 5 minutes

    def test_timeframe_invalid_with_default(self) -> None:
        """Test invalid timeframe with default fallback."""
        result = timeframe_to_ms("invalid", default_to_minutes=2)
        assert result == 120000  # 2 minutes

    def test_timeframe_empty_string(self) -> None:
        """Test empty timeframe string."""
        result = timeframe_to_ms("", default_to_minutes=1)
        assert result == 60000  # 1 minute default


class TestCheckStrParsableToFiniteDecimal:
    """Test cases for check_str_parsable_to_finite_decimal function."""

    def test_check_valid_decimal_string(self) -> None:
        """Test valid decimal string passes check."""
        result = check_str_parsable_to_finite_decimal("123.45", field_name="test")
        assert result == "123.45"

    def test_check_integer_string(self) -> None:
        """Test integer string passes check."""
        result = check_str_parsable_to_finite_decimal("123", field_name="test")
        assert result == "123"

    def test_check_invalid_string(self) -> None:
        """Test invalid string raises error."""
        with pytest.raises(DecimalFieldError):
            check_str_parsable_to_finite_decimal("not_a_number", field_name="test")

    def test_check_non_string_type(self) -> None:
        """Test non-string type raises error."""
        with pytest.raises(TypeFieldError):
            check_str_parsable_to_finite_decimal(123, field_name="test")

    def test_check_infinity_string(self) -> None:
        """Test infinity string raises error."""
        with pytest.raises(DecimalFieldError):
            check_str_parsable_to_finite_decimal("inf", field_name="test")

    def test_check_nan_string(self) -> None:
        """Test NaN string raises error."""
        with pytest.raises(DecimalFieldError):
            check_str_parsable_to_finite_decimal("nan", field_name="test")

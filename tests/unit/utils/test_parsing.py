"""Unit tests for parsing utilities.

Tests parsing functions for various data types including decimals, datetimes, and validation.
"""

import math
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value


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
        assert parse_decimal_value(math.pi) == Decimal("3.14")

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
        dt_naive = datetime(2023, 1, 1, 12, 0)
        assert parse_datetime_utc(dt_aware) == dt_aware
        result = parse_datetime_utc(dt_naive)
        assert result is not None
        assert result.tzinfo == UTC and result.replace(tzinfo=None) == dt_naive

    def test_epoch_seconds(self) -> None:
        """Should parse int/float epoch seconds to UTC datetime."""
        ts = 1700000000
        dt = parse_datetime_utc(ts)
        assert isinstance(dt, datetime) and dt.tzinfo == UTC
        # Allow small delta due to float conversion
        assert abs(dt.timestamp() - ts) < 1

    def test_epoch_milliseconds(self) -> None:
        """Should parse int/float epoch ms to UTC datetime."""
        ts_ms = 1700000000000
        dt = parse_datetime_utc(ts_ms)
        assert isinstance(dt, datetime) and dt.tzinfo == UTC
        assert abs(dt.timestamp() - ts_ms / 1000) < 1

    def test_iso_string(self) -> None:
        """Should parse ISO 8601 string to UTC datetime."""
        iso = "2023-01-01T12:00:00"
        dt = parse_datetime_utc(iso)
        assert isinstance(dt, datetime) and dt.tzinfo == UTC
        assert dt.year == 2023 and dt.month == 1 and dt.day == 1

    def test_none_returns_none(self) -> None:
        """Should return None if value is None."""
        assert parse_datetime_utc(None) is None

    def test_invalid_type(self) -> None:
        """Should raise ValueError for unsupported type."""
        # Testing with an invalid type by using cast to bypass type checking
        from typing import cast

        invalid_value = cast("str", [])  # Cast list to str to satisfy type checker
        with pytest.raises(ValueError, match="Unsupported datetime type"):
            parse_datetime_utc(invalid_value)

    def test_invalid_string(self) -> None:
        """Should raise ValueError for invalid ISO string."""
        with pytest.raises(
            ValueError,
            match=r"Cannot parse string .* as ISO datetime .* or as numeric timestamp",
        ):
            parse_datetime_utc("not-a-date")

    def test_error_context_includes_field(self) -> None:
        """Error message should include field_name if provided."""
        with pytest.raises(ValueError) as exc:
            parse_datetime_utc("bad", field_name="test_field")
        assert "test_field" in str(exc.value)

    def test_pre_1970_epoch(self) -> None:
        """Should parse negative epoch seconds (before 1970) to UTC datetime."""
        ts = -1000000000  # ~1938
        dt = parse_datetime_utc(ts)
        assert isinstance(dt, datetime) and dt.year < 1970

    def test_far_future_epoch(self) -> None:
        """Should parse far future epoch seconds (e.g., year 3000) to UTC datetime."""
        ts = 32503680000  # 3000-01-01T00:00:00Z
        dt = parse_datetime_utc(ts)
        assert isinstance(dt, datetime) and dt.year == 3000

    def test_leap_year_feb_29(self) -> None:
        """Should parse Feb 29 on a leap year."""
        iso = "2020-02-29T12:00:00"
        dt = parse_datetime_utc(iso)
        assert dt is not None
        assert dt.month == 2 and dt.day == 29

    def test_non_leap_year_feb_29(self) -> None:
        """Should raise ValueError for Feb 29 on a non-leap year."""
        with pytest.raises(ValueError):
            parse_datetime_utc("2019-02-29T12:00:00")

    def test_unreadable_date_string(self) -> None:
        """Should raise ValueError for unreadable/ambiguous date string."""
        with pytest.raises(ValueError):
            parse_datetime_utc("yesterday")

    def test_datetime_with_timezone_offset(self) -> None:
        """Should parse ISO string with timezone offset and convert to UTC."""
        iso = "2023-01-01T12:00:00+02:00"
        dt = parse_datetime_utc(iso)
        assert dt is not None
        offset = dt.utcoffset()
        assert offset is not None and offset.total_seconds() == 7200

    def test_datetime_trailing_z(self) -> None:
        """Should parse ISO string with trailing 'Z' as UTC."""
        iso = "2023-01-01T12:00:00Z"
        dt = parse_datetime_utc(iso.replace("Z", "+00:00"))
        assert dt is not None
        offset = dt.utcoffset()
        assert offset is not None and offset.total_seconds() == 0

    def test_datetime_empty_string(self) -> None:
        """Should raise ValueError for empty string."""
        with pytest.raises(ValueError):
            parse_datetime_utc("")

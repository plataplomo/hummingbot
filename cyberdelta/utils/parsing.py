"""
cyberdelta.utils.parsing
-----------------------
Utility functions for robust, consistent parsing of datetimes and decimals across all core models.
"""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


def parse_datetime_utc(value: Any) -> datetime | None:
    """
    Parses various inputs into a timezone-aware UTC datetime object.
    Accepts:
        - datetime (returns as UTC-aware)
        - int/float (epoch seconds or ms, auto-detects ms)
        - str (ISO 8601)
        - None (returns None)
    Raises ValueError on invalid input.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        try:
            timestamp = value / 1000.0 if value > 1e11 else float(value)
            return datetime.fromtimestamp(timestamp, tz=UTC)
        except (TypeError, ValueError, OSError) as e:
            raise ValueError(f"Invalid timestamp value '{value}': {e}") from e
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except ValueError as e:
            raise ValueError(f"Cannot parse ISO datetime string '{value}': {e}") from e
    raise ValueError(f"Unsupported datetime type: {type(value)}")


def parse_decimal_value(value: Any, allow_none: bool = True) -> Decimal | None:
    """
    Safely convert various inputs to Decimal.
    Accepts:
        - Decimal (returns as is)
        - str/int/float (converts, strips commas from str)
        - None (returns None if allow_none, else raises)
    Raises ValueError on invalid input.
    """
    if value is None:
        if allow_none:
            return None
        else:
            raise ValueError("Value cannot be None")
    if isinstance(value, Decimal):
        return value
    try:
        str_val = str(value).replace(",", "") if isinstance(value, str) else str(value)
        return Decimal(str_val)
    except (InvalidOperation, ValueError, TypeError) as e:
        raise ValueError(f"Cannot convert '{value}' to Decimal: {e}") from e

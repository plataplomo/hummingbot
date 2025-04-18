"""
cyberdelta.utils.parsing
-----------------------
Utility functions for robust, consistent parsing of datetimes and decimals across all core models.

All parsing errors will include the field name in their messages if provided,
greatly improving error traceability.
"""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation


def parse_datetime_utc(
    value: datetime | int | float | str | None, field_name: str = ""
) -> datetime | None:
    """
    Parses various inputs into a timezone-aware UTC datetime object.
    Accepts:
        - datetime (returns as UTC-aware)
        - int/float (epoch seconds or ms, auto-detects ms)
        - str (ISO 8601)
        - None (returns None)
    Args:
        value: The value to parse as a datetime.
        field_name: (Optional) The name of the field being parsed. If provided, it will be
            included in any error messages for better traceability.
    Returns:
        A timezone-aware UTC datetime object, or None if value is None and allowed.
    Raises:
        ValueError: If the value cannot be parsed as a datetime. The error message will include
            the field name if provided.
    """
    prefix = f"{field_name}: " if field_name else ""
    if value is None:
        return None
    elif isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    elif isinstance(value, int | float):
        try:
            timestamp = value / 1000.0 if value > 1e11 else float(value)
            return datetime.fromtimestamp(timestamp, tz=UTC)
        except (TypeError, ValueError, OSError) as e:
            raise ValueError(f"{prefix}Invalid timestamp value '{value}': {e}") from e
    # ---
    # NOTE: The following type narrowing is canonical and type-safe in Python.
    # Pylance/Pyright may incorrectly flag this as unnecessary due to static type inference,
    # but at runtime, explicit type checks are required for robust parsing.
    # The 'pyright: ignore[reportUnnecessaryIsInstance]' directive silences this false positive.
    # Mypy does not warn on this line, so this is the most cross-tool compatible solution.
    # ---
    elif isinstance(value, str):  # pyright: ignore[reportUnnecessaryIsInstance]
        try:
            dt = datetime.fromisoformat(value)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except ValueError as e:
            raise ValueError(f"{prefix}Cannot parse ISO datetime string '{value}': {e}") from e
    else:
        raise ValueError(f"{prefix}Unsupported datetime type: {type(value)}")


def parse_decimal_value(
    value: Decimal | str | int | float | None,
    allow_none: bool = True,
    field_name: str = "",
) -> Decimal | None:
    """
    Safely convert various inputs to Decimal, with robust error context.
    Accepts:
        - Decimal (returns as is)
        - str/int/float (converts, strips commas from str)
        - None (returns None if allow_none, else raises)
    Args:
        value: The value to parse as a Decimal.
        allow_none: If True, None is allowed and will return None. If False, None will raise.
        field_name: (Optional) The name of the field being parsed. If provided, it will be
            included in any error messages for better traceability.
    Returns:
        A Decimal object, or None if value is None and allowed.
    Raises:
        ValueError: If the value cannot be parsed as a Decimal. The error message will include
            the field name if provided.
    """
    prefix = f"{field_name}: " if field_name else ""
    if value is None:
        if allow_none:
            return None
        else:
            raise ValueError(f"{prefix}Value cannot be None")
    if isinstance(value, Decimal):
        return value
    try:
        str_val = str(value).replace(",", "")
        return Decimal(str_val)
    except (InvalidOperation, ValueError, TypeError) as e:
        raise ValueError(f"{prefix}Cannot convert '{value}' to Decimal: {e}") from e

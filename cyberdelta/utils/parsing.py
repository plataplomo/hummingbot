"""cyberdelta.utils.parsing.

Utility functions for robust, consistent parsing of datetimes and decimals across all core models.

All parsing errors will include the field name in their messages if provided,
greatly improving error traceability.
"""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation


# Note: Removed logger import to avoid circular import with config.structlog_config


def parse_datetime_utc(
    value: datetime | float | str | None,
    field_name: str = "",
) -> datetime | None:
    """Parse various inputs into a timezone-aware UTC datetime object.

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
    if isinstance(value, datetime):
        return _ensure_utc_timezone(value)
    if isinstance(value, int | float):
        return _parse_numeric_timestamp(value, prefix)
    if isinstance(value, str):  # pyright: ignore[reportUnnecessaryIsInstance]
        return _parse_string_datetime(value, prefix)
    raise ValueError(f"{prefix}Unsupported datetime type: {type(value)}")


def _ensure_utc_timezone(dt: datetime) -> datetime:
    """Ensure datetime has UTC timezone."""
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _parse_numeric_timestamp(value: float, prefix: str) -> datetime:
    """Parse numeric timestamp, auto-detecting scale (ns, us, ms, s)."""
    try:
        timestamp_s = _determine_timestamp_scale(value)
        return datetime.fromtimestamp(timestamp_s, tz=UTC)
    except (TypeError, ValueError, OSError) as e:
        raise ValueError(f"{prefix}Invalid timestamp value '{value}': {e}") from e


def _determine_timestamp_scale(value: float) -> float:
    """Determine the scale of a timestamp and convert to seconds."""
    # Determine scale: ns, us, ms, or s
    if value > 2e17:  # Heuristic: likely nanoseconds (e.g., current date ~1.7e18)
        return value / 1e9
    if value > 2e14:  # Heuristic: likely microseconds (e.g., current date ~1.7e15)
        return value / 1e6
    if value > 2e11:  # Heuristic: likely milliseconds (e.g., current date ~1.7e12)
        return value / 1e3
    # Heuristic: likely seconds (e.g., current date ~1.7e9)
    return float(value)


def _parse_string_datetime(value: str, prefix: str) -> datetime:
    """Parse string as ISO datetime or numeric timestamp."""
    # ---
    # NOTE: The following type narrowing is canonical and type-safe in Python.
    # Pylance/Pyright may incorrectly flag this as unnecessary due to static type inference,
    # but at runtime, explicit type checks are required for robust parsing.
    # The 'pyright: ignore[reportUnnecessaryIsInstance]' directive silences this false positive.
    # Mypy does not warn on this line, so this is the most cross-tool compatible solution.
    # ---
    try:
        dt = datetime.fromisoformat(value)
        return _ensure_utc_timezone(dt)
    except ValueError as e_iso:
        return _parse_string_as_numeric_timestamp(value, prefix, e_iso)


def _parse_string_as_numeric_timestamp(value: str, prefix: str, iso_error: ValueError) -> datetime:
    """Try to parse string as numeric timestamp if ISO parsing failed."""
    try:
        float_val = float(value)
        # Reuse the timestamp scale logic
        timestamp_s = _determine_timestamp_scale(float_val)
        return datetime.fromtimestamp(timestamp_s, tz=UTC)
    except (ValueError, TypeError, OSError) as e_num:
        raise ValueError(
            f"{prefix}Cannot parse string '{value}' as ISO datetime ({iso_error}) "
            f"or as numeric timestamp ({e_num})",
        ) from e_num


def parse_decimal_value(
    value: Decimal | str | float | None,
    allow_none: bool = True,
    field_name: str = "",
) -> Decimal | None:
    """Safely convert various inputs to Decimal, with robust error context.

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
        raise ValueError(f"{prefix}Value cannot be None")
    if isinstance(value, Decimal):
        return value
    try:
        str_val = str(value).strip().replace(",", "")
        return Decimal(str_val)
    except (InvalidOperation, TypeError) as e:
        raise ValueError(f"{prefix}Cannot convert '{value}' to Decimal: {e}") from e


def validate_str_field(
    value: object,
    field_name: str = "",
    max_length: int | None = None,
    allow_empty: bool = False,
) -> str:
    """Validate that a value is a string, optionally non-empty, within max_length, and valid UTF-8.

    Args:
        value: The value to validate.
        field_name: Name of the field for error messages.
        max_length: Maximum allowed length (if any).
        allow_empty: If True, allow empty/whitespace-only strings.

    Returns:
        The validated string value.

    Raises:
        ValueError: If validation fails.

    """
    prefix = f"{field_name}: " if field_name else ""
    if not isinstance(value, str):
        raise ValueError(f"{prefix}Expected string, got {type(value).__name__}")
    if not allow_empty and not value.strip():
        raise ValueError(f"Field {field_name}: String cannot be empty")
    if max_length is not None and len(value) > max_length:
        raise ValueError(f"{prefix}String value too long (max {max_length} chars)")
    try:
        value.encode("utf-8", "strict")
    except UnicodeEncodeError as e:
        raise ValueError(f"{prefix}Invalid UTF-8 sequence: {e}") from e
    return value


def validate_enum_field(
    value: object,
    allowed: set[str],
    field_name: str = "",
    max_length: int | None = 32,
) -> str:
    """Validate that a value is a string, a member of the allowed set, and valid UTF-8.

    Args:
        value: The value to validate.
        allowed: Set of allowed string values.
        field_name: Name of the field for error messages.
        max_length: Maximum allowed length for the string representation.

    Returns:
        The validated string value.

    Raises:
        ValueError: If validation fails.

    """
    s = validate_str_field(value, field_name=field_name, max_length=max_length, allow_empty=False)

    prefix = f"{field_name}: " if field_name else ""
    if s not in allowed:
        allowed_sorted_list = sorted(allowed)
        raise ValueError(f"{prefix}Invalid value '{s}'. Expected one of {allowed_sorted_list}")
    return s


def timeframe_to_ms(tf_str: str, default_to_minutes: int | None = 1) -> int:
    """Convert a timeframe string (e.g., "1m", "5m", "1h", "1d") to milliseconds.

    Args:
        tf_str: The timeframe string to parse.
        default_to_minutes: The default duration in minutes to return if parsing fails.
                            If None, raises ValueError on parse failure.

    Returns:
        The timeframe duration in milliseconds.

    Raises:
        ValueError: If tf_str is unparseable and default_to_minutes is None.

    """
    tf_str_lower = tf_str.lower().strip()
    if not tf_str_lower:
        message = "Timeframe string cannot be empty."
        if default_to_minutes is None:
            raise ValueError(message)
        # Warning: message. Defaulting to default_to_minutes minute(s).
        return default_to_minutes * 60 * 1000

    try:
        if "m" in tf_str_lower:
            return int(tf_str_lower.replace("m", "")) * 60 * 1000
        if "h" in tf_str_lower:
            return int(tf_str_lower.replace("h", "")) * 60 * 60 * 1000
        if "d" in tf_str_lower:
            return int(tf_str_lower.replace("d", "")) * 24 * 60 * 60 * 1000
        # Attempt to parse as raw minutes if no suffix
        return int(tf_str_lower) * 60 * 1000
    except ValueError as e:
        message = f"Could not parse timeframe string '{tf_str}' as integer or known unit: {e}"
        if default_to_minutes is None:
            raise ValueError(message) from e
        # Warning: message. Defaulting to default_to_minutes minute(s).
        return default_to_minutes * 60 * 1000


def check_str_parsable_to_finite_decimal(value: object, field_name: str = "") -> str:
    """Validate that a value is a string, is parsable to a finite Decimal.

    Returns the original string if valid, otherwise raises ValueError.
    This is intended for use with Pydantic's AfterValidator on a string field.

    Args:
        value: The value to validate.
        field_name: Name of the field for error messages.

    Returns:
        The validated string value.

    Raises:
        ValueError: If validation fails.

    """
    # First, validate it's a proper string (non-empty, UTF-8, etc. as per validate_str_field)
    # Assuming basic string validation (e.g. non-empty) is desired for such fields.
    # Adjust allow_empty based on typical requirements for numeric strings.
    validated_str = validate_str_field(value, field_name=field_name, allow_empty=False)

    # Then, try to parse it as a Decimal and check finiteness
    try:
        parsed_decimal = parse_decimal_value(validated_str, allow_none=False, field_name=field_name)
        # parse_decimal_value raises if allow_none=False and input is None,
        # or if it can't convert. So parsed_decimal here should not be None.
        if parsed_decimal is None:  # Should not happen due to allow_none=False
            raise ValueError(
                f"Field {field_name or 'value'}: parsing unexpectedly returned None "
                f"for '{validated_str}'.",
            )
        if not parsed_decimal.is_finite():
            raise ValueError(
                f"Field {field_name or 'value'}: parsed decimal '{validated_str}' is not finite.",
            )
    except ValueError as e:  # Catch errors from validate_str_field or parse_decimal_value
        # Re-raise to ensure the message includes field_name if passed down.
        # If parse_decimal_value or validate_str_field already prefixed, this might duplicate.
        # However, ensuring the check for finiteness is clear.
        if field_name and field_name not in str(e):
            raise ValueError(f"Field {field_name}: {e}") from e
        raise  # Re-raise if field_name already in message or no field_name

    return validated_str  # Return the original string if all checks pass

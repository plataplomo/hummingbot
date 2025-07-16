"""DateTime parsing utilities for API responses.

This module provides safe datetime parsing utilities for handling
various timestamp formats from exchange APIs.
"""

from datetime import UTC, datetime
from typing import NoReturn

from cyberdelta.apis.base.validation_context_domain import ValidationContext
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


def safe_parse_timestamp(
    value: str | float | datetime | None,
    validation_context: ValidationContext | None = None,
) -> datetime | None:
    """Safely parse a timestamp value to UTC datetime.

    Handles various timestamp formats including:
    - Unix timestamps (seconds or milliseconds)
    - ISO format strings
    - Already-parsed datetime objects

    Args:
        value: Timestamp value to parse
        validation_context: Validation context with null and timestamp policies

    Returns:
        Parsed datetime in UTC or None if allowed

    Raises:
        APIError: If parsing fails
    """
    # Use default context if none provided
    if validation_context is None:
        validation_context = ValidationContext()

    # Handle None values according to context policy
    if value is None:
        if validation_context.null_policy.value == "allow":
            return None
        raise APIError(
            message=(
                f"Cannot parse None as timestamp for {validation_context.field_name} "
                f"in {validation_context.context_description}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    # Handle already-parsed datetime objects
    if isinstance(value, datetime):
        # Ensure timezone awareness - convert to UTC if naive
        if value.tzinfo is None:
            logger.warning(
                "naive_datetime_converted",
                field_name=validation_context.field_name,
                context=validation_context.context_description,
                message="Converting naive datetime to UTC",
            )
            return value.replace(tzinfo=UTC)
        # Convert to UTC if timezone-aware
        return value.astimezone(UTC)

    # Handle numeric timestamps (Unix time)
    if isinstance(value, (int, float)):
        return _parse_unix_timestamp(
            value, validation_context.field_name, validation_context.context_description
        )

    # Handle string timestamps - at this point value must be str
    return _parse_string_timestamp(
        value, validation_context.field_name, validation_context.context_description
    )


def format_timestamp_for_api(
    dt: datetime,
    format_type: str = "unix_ms",
    validation_context: ValidationContext | None = None,
) -> str | int:
    """Format a datetime for API submission.

    Args:
        dt: Datetime to format
        format_type: Format type ("unix_s", "unix_ms", "iso", "iso_date")
        validation_context: Validation context for error messages

    Returns:
        Formatted timestamp

    Raises:
        APIError: If formatting fails
    """
    # Use default context if none provided
    if validation_context is None:
        validation_context = ValidationContext()

    try:
        # Ensure we're working with UTC
        dt_utc = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)

        if format_type == "unix_s":
            return int(dt_utc.timestamp())
        if format_type == "unix_ms":
            return int(dt_utc.timestamp() * 1000)
        if format_type == "iso":
            return dt_utc.isoformat()
        if format_type == "iso_date":
            return dt_utc.date().isoformat()
        _raise_unknown_format_error(format_type)

    except (ValueError, OSError) as e:
        logger.exception(
            "timestamp_format_failed",
            field_name=validation_context.field_name,
            context=validation_context.context_description,
            format_type=format_type,
            error=str(e),
        )
        raise APIError(
            message=(
                f"Failed to format timestamp {validation_context.field_name} "
                f"in {validation_context.context_description}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        ) from e


def validate_timestamp_range(
    dt: datetime,
    min_date: datetime | None = None,
    max_date: datetime | None = None,
    validation_context: ValidationContext | None = None,
) -> datetime:
    """Validate that a timestamp falls within acceptable range.

    Args:
        dt: Datetime to validate
        min_date: Minimum allowed date (optional)
        max_date: Maximum allowed date (optional)
        validation_context: Validation context for error messages

    Returns:
        Validated datetime

    Raises:
        APIError: If timestamp is out of range
    """
    # Use default context if none provided
    if validation_context is None:
        validation_context = ValidationContext()

    if min_date and dt < min_date:
        raise APIError(
            message=(
                f"{validation_context.field_name} too early "
                f"in {validation_context.context_description}: {dt} < {min_date}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    if max_date and dt > max_date:
        raise APIError(
            message=(
                f"{validation_context.field_name} too late "
                f"in {validation_context.context_description}: {dt} > {max_date}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    return dt


def _parse_unix_timestamp(
    value: float,
    field_name: str,
    context: str,
) -> datetime:
    """Parse Unix timestamp (auto-detecting seconds vs milliseconds)."""
    try:
        # Auto-detect format based on magnitude
        # Timestamps after year 3000 are likely milliseconds
        year_3000_timestamp = 32503680000  # Year 3000 in seconds
        timestamp_seconds = value / 1000 if value > year_3000_timestamp else value

        # Validate reasonable range (after 1970, before year 3000)
        if timestamp_seconds < 0 or timestamp_seconds > year_3000_timestamp:
            _raise_timestamp_range_error(timestamp_seconds)

        dt = datetime.fromtimestamp(timestamp_seconds, tz=UTC)

        logger.debug(
            "unix_timestamp_parsed",
            field_name=field_name,
            context=context,
            original_value=value,
            parsed_datetime=dt.isoformat(),
        )

    except (ValueError, OSError, OverflowError) as e:
        logger.exception(
            "unix_timestamp_parse_failed",
            field_name=field_name,
            context=context,
            value=value,
            error=str(e),
        )
        raise APIError(
            message=f"Invalid Unix timestamp for {field_name} in {context}: {value}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        ) from e
    else:
        return dt


def _parse_string_timestamp(
    value: str,
    field_name: str,
    context: str,
) -> datetime:
    """Parse string timestamp using common formats."""
    value = value.strip()
    if not value:
        raise APIError(
            message=f"Empty timestamp string for {field_name} in {context}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    # Common ISO formats to try
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO with microseconds and Z
        "%Y-%m-%dT%H:%M:%SZ",  # ISO with Z
        "%Y-%m-%dT%H:%M:%S.%f",  # ISO with microseconds
        "%Y-%m-%dT%H:%M:%S",  # ISO basic
        "%Y-%m-%d %H:%M:%S.%f",  # Space-separated with microseconds
        "%Y-%m-%d %H:%M:%S",  # Space-separated basic
        "%Y-%m-%d",  # Date only
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt).replace(tzinfo=UTC)
            # Directly assign UTC timezone to avoid naive datetime

            logger.debug(
                "string_timestamp_parsed",
                field_name=field_name,
                context=context,
                original_value=value,
                format_used=fmt,
                parsed_datetime=dt.isoformat(),
            )

            return dt.astimezone(UTC)

        except ValueError:
            continue

    # If all formats failed, try parsing as Unix timestamp
    try:
        unix_value = float(value)
        return _parse_unix_timestamp(unix_value, field_name, context)
    except ValueError:
        pass

    logger.error(
        "string_timestamp_parse_failed",
        field_name=field_name,
        context=context,
        value=value,
        formats_tried=formats,
    )
    raise APIError(
        message=f"Unable to parse timestamp string for {field_name} in {context}: {value}",
        code=APIErrorCode.INVALID_RESPONSE.value,
    )


def _raise_unknown_format_error(format_type: str) -> NoReturn:
    """Raise ValueError for unknown format type."""
    msg = f"Unknown format type: {format_type}"
    raise ValueError(msg)


def _raise_timestamp_range_error(timestamp_seconds: float) -> NoReturn:
    """Raise ValueError for timestamp out of range."""
    msg = f"Timestamp out of reasonable range: {timestamp_seconds}"
    raise ValueError(msg)

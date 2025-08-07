"""Structured logging configuration for CyberDeltaEngine."""

from __future__ import annotations

import logging
import re
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog
from structlog.contextvars import merge_contextvars
from structlog.processors import CallsiteParameter, CallsiteParameterAdder
from structlog.typing import EventDict, Processor


if TYPE_CHECKING:
    from cyberdelta.config.models.app_config import AppSettings

# Type alias for JSON-serializable values
type JSONSerializable = str | int | float | bool | dict[str, Any] | list[Any] | None


# Add TRACE level below DEBUG
TRACE_LEVEL = 5
logging.addLevelName(TRACE_LEVEL, "TRACE")


# Note: We don't add trace method to logging.Logger due to type checking issues.
# The TraceLevelLogger wrapper provides trace functionality instead.


def add_timestamp(_: object, __: str, event_dict: EventDict) -> EventDict:
    """Add ISO format timestamp to log events.

    Args:
        _: Logger instance (unused)
        __: Event name (unused)
        event_dict: The event dictionary to modify

    Returns:
        EventDict: Modified event dictionary with timestamp added
    """
    event_dict["timestamp"] = datetime.now(UTC).isoformat()
    return event_dict


def serialize_decimals(_: object, __: str, event_dict: EventDict) -> EventDict:
    """Convert Decimal values to strings for proper serialization.

    This preserves the exact precision of Decimal values in logs without
    converting them to floats.

    Args:
        _: Logger instance (unused)
        __: Event name (unused)
        event_dict: The event dictionary to process

    Returns:
        EventDict: Modified event dictionary with Decimals converted to strings
    """

    def convert_value(
        value: JSONSerializable | Decimal | tuple[Any, ...],
    ) -> JSONSerializable | tuple[Any, ...]:
        """Recursively convert Decimal values to strings.

        Args:
            value: Any value from the event dictionary

        Returns:
            The value with Decimals converted to strings
        """
        if isinstance(value, Decimal):
            # Convert to string to preserve exact precision
            return str(value)
        if isinstance(value, dict):
            return {k: convert_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [convert_value(v) for v in value]
        if isinstance(value, tuple):
            return tuple(convert_value(v) for v in value)
        return value

    return {key: convert_value(value) for key, value in event_dict.items()}


def censor_sensitive_data(_: object, __: str, event_dict: EventDict) -> EventDict:
    """Remove or mask sensitive data from logs.

    Args:
        _: Logger instance (unused)
        __: Event name (unused)
        event_dict: The event dictionary to censor

    Returns:
        EventDict: Modified event dictionary with sensitive data masked
    """
    sensitive_keys = {
        "api_key",
        "secret",
        "password",
        "private_key",
        "seed_phrase",
        "auth_token",
        "signature",
    }

    for key in list(event_dict.keys()):
        if any(sensitive in key.lower() for sensitive in sensitive_keys):
            event_dict[key] = "***REDACTED***"

    return event_dict


def strip_ansi_codes(_: object, __: str, event_dict: EventDict) -> EventDict:
    """Strip ANSI color codes from all string values in event dict.

    Args:
        _: Logger instance (unused)
        __: Event name (unused)
        event_dict: The event dictionary to process

    Returns:
        EventDict: Modified event dictionary with ANSI codes stripped
    """
    ansi_pattern = re.compile(r"\x1b\[[0-9;]*m")

    def strip_value(
        value: str | dict[str, Any] | list[Any] | float | bool | None,
    ) -> str | dict[str, Any] | list[Any] | int | float | bool | None:
        if isinstance(value, str):
            return ansi_pattern.sub("", value)
        if isinstance(value, dict):
            return {k: strip_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [strip_value(v) for v in value]
        return value

    return {key: strip_value(value) for key, value in event_dict.items()}


def setup_structlog(app_settings: AppSettings) -> None:
    """Configure structlog for the application.

    Args:
        app_settings: Application configuration containing logging settings
    """
    # Determine log level
    log_level = getattr(logging, app_settings.general.log_level.upper(), logging.INFO)

    # Configure standard logging first
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
        force=True,  # Force reconfiguration
    )

    # Setup file logging if configured
    if app_settings.general.log_file:
        setup_file_logging(app_settings.general.log_file, log_level)

    # Base processors
    base_processors: list[Processor] = [
        # Add timestamp
        add_timestamp,
        # Merge context variables
        merge_contextvars,
        # Add callsite parameters
        CallsiteParameterAdder(
            parameters=[
                CallsiteParameter.FILENAME,
                CallsiteParameter.LINENO,
                CallsiteParameter.FUNC_NAME,
            ],
            additional_ignores=["structlog", "logging"],
        ),
        # Serialize Decimal values to preserve precision
        serialize_decimals,
        # Censor sensitive data
        censor_sensitive_data,
        # Process positional arguments
        structlog.stdlib.PositionalArgumentsFormatter(),
        # Process stack info
        structlog.processors.StackInfoRenderer(),
        # Process exceptions
        structlog.processors.format_exc_info,
        # Add log level and logger name
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
    ]

    # Configure structlog for console output only
    # File output is handled by stdlib logging handlers with ProcessorFormatter
    structlog.configure(
        processors=[
            *base_processors,
            # Serialize Decimals one more time before console rendering
            serialize_decimals,
            # Render as colored console output
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def setup_file_logging(log_file: str, level: int) -> None:
    """Setup file logging handler with JSON output.

    Args:
        log_file: Path to log file
        level: Logging level
    """
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove any existing file handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            root_logger.removeHandler(handler)

    # Create file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(level)

    # Create processor formatter for clean JSON output
    # Use separate processors that strip ANSI codes before JSON rendering
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            # Extract from structlog's context and remove meta
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            # Serialize Decimals before JSON rendering
            serialize_decimals,
            # Strip ANSI codes before JSON rendering
            strip_ansi_codes,
            # Render as clean JSON
            structlog.processors.JSONRenderer(),
        ],
        foreign_pre_chain=[
            # For non-structlog logs (shouldn't happen but just in case)
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            add_timestamp,
            serialize_decimals,
            censor_sensitive_data,
            strip_ansi_codes,
        ],
    )

    file_handler.setFormatter(formatter)

    # Add to root logger
    root_logger.addHandler(file_handler)


class TraceLevelLogger:
    """Wrapper to add trace level support to structlog BoundLogger."""

    def __init__(self, logger: structlog.BoundLogger) -> None:
        """Initialize the trace logger wrapper.

        Args:
            logger: The structlog BoundLogger to wrap
        """
        self._logger = logger

    def debug(self, event: str | None = None, **kwargs: object) -> None:
        """Log at DEBUG level."""
        self._logger.debug(event, **kwargs)

    def info(self, event: str | None = None, **kwargs: object) -> None:
        """Log at INFO level."""
        self._logger.info(event, **kwargs)

    def warning(self, event: str | None = None, **kwargs: object) -> None:
        """Log at WARNING level."""
        self._logger.warning(event, **kwargs)

    def error(self, event: str | None = None, **kwargs: object) -> None:
        """Log at ERROR level."""
        self._logger.error(event, **kwargs)

    def critical(self, event: str | None = None, **kwargs: object) -> None:
        """Log at CRITICAL level."""
        self._logger.critical(event, **kwargs)

    def exception(self, event: str | None = None, **kwargs: object) -> None:
        """Log an exception with traceback.

        Note: This method should only be called from within exception handlers
        where sys.exc_info() will return valid exception information.
        """
        # Only add exc_info if we're actually in an exception context
        exc_type, _exc_value, _exc_traceback = sys.exc_info()
        if exc_type is not None:
            # We're in an exception context, include traceback
            kwargs["exc_info"] = True
            self._logger.error(event, **kwargs)
        else:
            # No active exception, log as regular error
            self._logger.error(event, **kwargs)

    def trace(self, event: str, **kwargs: object) -> None:
        """Log at TRACE level (below DEBUG)."""
        # Use debug with special marker since structlog doesn't support custom levels
        self._logger.debug(
            "trace_level_log",
            trace_event=event,
            level="TRACE",
            message=f"[TRACE] {event}",
            **kwargs,
        )

    def bind(self, **kwargs: object) -> TraceLevelLogger:
        """Bind context to logger.

        Args:
            **kwargs: Context variables to bind to the logger

        Returns:
            TraceLevelLogger: New logger instance with bound context
        """
        return TraceLevelLogger(self._logger.bind(**kwargs))

    def unbind(self, *keys: str) -> TraceLevelLogger:
        """Unbind context from logger.

        Args:
            *keys: Context keys to unbind from the logger

        Returns:
            TraceLevelLogger: New logger instance with context unbound
        """
        return TraceLevelLogger(self._logger.unbind(*keys))

    def try_unbind(self, *keys: str) -> TraceLevelLogger:
        """Try to unbind context from logger.

        Args:
            *keys: Context keys to try to unbind from the logger

        Returns:
            TraceLevelLogger: New logger instance with context unbound if keys existed
        """
        return TraceLevelLogger(self._logger.try_unbind(*keys))

    def __getattr__(self, name: str) -> object:
        """Delegate any other attribute access to the wrapped logger.

        Args:
            name: The attribute name

        Returns:
            The attribute from the wrapped logger
        """
        return getattr(self._logger, name)


def get_logger(name: str | None = None, **context: object) -> TraceLevelLogger:
    """Get a configured structlog logger with trace support.

    Args:
        name: Logger name (defaults to module name)
        **context: Additional context to bind to logger

    Returns:
        Configured structlog logger with context and trace support
    """
    logger: structlog.BoundLogger = structlog.get_logger(name)
    if context:
        logger = logger.bind(**context)
    return TraceLevelLogger(logger)

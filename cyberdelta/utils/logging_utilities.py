"""Logging utilities for reducing log spam and improving performance.

This module provides utilities for sampling, aggregation, and error suppression
to reduce log volume while maintaining operational visibility.
"""

import secrets
import time
from collections import defaultdict
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class LoggerProtocol(Protocol):
    """Protocol defining the interface for logger objects."""

    def debug(self, event: str | None = None, **kwargs: object) -> None:
        """Log a debug message."""
        ...

    def info(self, event: str | None = None, **kwargs: object) -> None:
        """Log an info message."""
        ...

    def warning(self, event: str | None = None, **kwargs: object) -> None:
        """Log a warning message."""
        ...

    def error(self, event: str | None = None, **kwargs: object) -> None:
        """Log an error message."""
        ...


class SampledLogger:
    """Logger that samples debug messages to reduce log volume.

    This is useful for high-frequency debug logs that would otherwise
    create massive log spam but are still useful for debugging.
    """

    def __init__(self, logger: LoggerProtocol, sample_rate: float = 0.01) -> None:
        """Initialize sampled logger.

        Args:
            logger: The underlying logger (structlog.BoundLogger or TraceLevelLogger)
            sample_rate: Fraction of messages to log (0.01 = 1%)
        """
        self.logger = logger
        self.sample_rate = sample_rate
        self.counter = 0

    def debug_sampled(self, event: str, **kwargs: object) -> None:
        """Log debug message with sampling."""
        self.counter += 1
        if secrets.randbelow(10000) < int(self.sample_rate * 10000):
            kwargs["sample_count"] = self.counter
            self.logger.debug(f"[SAMPLE] {event}", **kwargs)


class ErrorSuppressor:
    """Suppresses repeated error messages to reduce log spam.

    Logs the first occurrence immediately, then suppresses subsequent
    occurrences for a duration. Includes occurrence count when re-logging.
    """

    def __init__(self, logger: LoggerProtocol, suppress_duration: float = 300) -> None:
        """Initialize error suppressor.

        Args:
            logger: The underlying logger (structlog.BoundLogger or TraceLevelLogger)
            suppress_duration: Seconds to suppress repeated errors (default 5 minutes)
        """
        self.logger = logger
        self.suppress_duration = suppress_duration
        self.errors: dict[str, float] = {}
        self.counts: dict[str, int] = defaultdict(int)

    def log_once(self, error_key: str, level: str, event: str, **kwargs: object) -> None:
        """Log error with suppression for repeated occurrences.

        Args:
            error_key: Unique identifier for this error type
            level: Log level (error, warning, etc.)
            event: Event name for structured logging
            **kwargs: Additional context for the log
        """
        now = time.time()
        self.counts[error_key] += 1

        if error_key not in self.errors or now - self.errors[error_key] > self.suppress_duration:
            # Add occurrence count if this is a repeat
            if self.counts[error_key] > 1:
                kwargs["occurrence_count"] = self.counts[error_key]
                kwargs["suppressed_duration"] = self.suppress_duration

            # Log the error
            log_method = getattr(self.logger, level)
            log_method(event, **kwargs)

            # Update last log time and reset count
            self.errors[error_key] = now
            self.counts[error_key] = 0


class SmartErrorSuppressor:
    """Advanced error suppressor with exponential backoff.

    Provides more sophisticated error suppression with exponential backoff
    for frequently occurring errors, preventing log spam while ensuring
    important errors are eventually logged.
    """

    def __init__(
        self,
        logger: LoggerProtocol,
        initial_suppress: float = 60,
        max_suppress: float = 3600,
    ) -> None:
        """Initialize smart error suppressor.

        Args:
            logger: The underlying logger (structlog.BoundLogger or TraceLevelLogger)
            initial_suppress: Initial suppression duration in seconds (default 1 minute)
            max_suppress: Maximum suppression duration in seconds (default 1 hour)
        """
        self.logger = logger
        self.initial_suppress = initial_suppress
        self.max_suppress = max_suppress
        self.error_states: dict[str, dict[str, Any]] = {}

    def log_with_suppression(
        self,
        error_key: str,
        level: str,
        event: str,
        **kwargs: dict[str, Any],
    ) -> None:
        """Log with smart suppression using exponential backoff.

        Args:
            error_key: Unique identifier for this error type
            level: Log level (error, warning, etc.)
            event: Event name for structured logging
            **kwargs: Additional context for the log
        """
        now = time.time()
        state = self.error_states.get(
            error_key,
            {"last_log": 0, "count": 0, "suppress_until": 0, "backoff_multiplier": 0},
        )

        state["count"] += 1

        if now > state["suppress_until"]:
            # Add occurrence information
            if state["count"] > 1:
                kwargs["occurrence_count"] = state["count"]
                kwargs["since_last_log"] = round(now - state["last_log"], 1)

            # Log the error
            log_method = getattr(self.logger, level)
            log_method(event, **kwargs)

            # Calculate next suppression duration with exponential backoff
            suppress_duration = min(
                self.initial_suppress * (2 ** state["backoff_multiplier"]),
                self.max_suppress,
            )

            # Update state
            state["suppress_until"] = now + suppress_duration
            state["last_log"] = now
            state["count"] = 0
            state["backoff_multiplier"] = min(state["backoff_multiplier"] + 1, 6)

        self.error_states[error_key] = state


class MessageStatsAggregator:
    """Aggregates message statistics and logs summaries periodically.

    Instead of logging every message, this aggregates statistics and
    logs summaries at regular intervals.
    """

    def __init__(self, logger: LoggerProtocol, window_seconds: int = 60) -> None:
        """Initialize message stats aggregator.

        Args:
            logger: The underlying logger (structlog.BoundLogger or TraceLevelLogger)
            window_seconds: Seconds between summary logs (default 1 minute)
        """
        self.logger = logger
        self.window_seconds = window_seconds
        self.stats: dict[str, int] = defaultdict(int)
        self.last_log_time = time.time()
        self.start_time = time.time()

    def record_event(self, event_type: str, count: int = 1) -> None:
        """Record an event for aggregation.

        Args:
            event_type: Type of event to count
            count: Number of occurrences (default 1)
        """
        self.stats[event_type] += count

        # Check if we should log a summary
        current_time = time.time()
        if current_time - self.last_log_time >= self.window_seconds:
            self._log_summary()

    def _log_summary(self) -> None:
        """Log aggregated statistics summary."""
        if not self.stats:
            return

        current_time = time.time()
        window_duration = current_time - self.last_log_time
        total_events = sum(self.stats.values())

        # Sort stats by count (descending) and take top 10
        top_events = dict(sorted(self.stats.items(), key=lambda x: x[1], reverse=True)[:10])

        self.logger.info(
            "message_stats_summary",
            window_seconds=round(window_duration, 1),
            total_events=total_events,
            events_per_second=round(total_events / window_duration, 2),
            top_events=top_events,
            unique_event_types=len(self.stats),
            message=f"Message stats (last {int(window_duration)}s): {total_events} total events",
        )

        # Reset for next window
        self.stats.clear()
        self.last_log_time = current_time

    def force_summary(self) -> None:
        """Force an immediate summary log."""
        if self.stats:
            self._log_summary()

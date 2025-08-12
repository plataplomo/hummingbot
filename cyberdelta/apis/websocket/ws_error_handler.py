"""Base Error Handler for WebSocket Messages.

This module provides centralized error handling for WebSocket message processing,
including error suppression, structured logging, and consistent error responses.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

from cachetools import TTLCache
from pydantic import BaseModel, ConfigDict, Field, ValidationError

# Pure WebSocket error system - only WebSocketStreamError types used
# This handler is being phased out in favor of ws_stream_error_handler.py
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName


class ErrorSuppressionConfig(BaseModel):
    """Configuration for error suppression behavior."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    ttl_seconds: float = Field(
        default=300.0,  # 5 minutes
        gt=0,
        le=3600,  # Max 1 hour
        description="Time to live for error cache entries",
    )
    max_cache_size: int = Field(
        default=1000,
        gt=0,
        le=10000,
        description="Maximum number of cached error entries",
    )
    suppression_threshold: int = Field(
        default=5,
        gt=0,
        le=100,
        description="Number of identical errors before suppression kicks in",
    )
    log_every_n: int = Field(
        default=100,
        gt=0,
        le=1000,
        description="Log every Nth suppressed error for visibility",
    )


class ErrorStats:
    """Tracks error statistics for monitoring."""

    def __init__(self) -> None:
        """Initialize error statistics."""
        self.total_errors = 0
        self.suppressed_errors = 0
        self.validation_errors = 0
        self.routing_errors = 0
        self.processing_errors = 0
        self.unknown_errors = 0
        self._start_time = time.time()

    def to_dict(self) -> dict[str, Any]:
        """Convert stats to dictionary.

        Returns:
            Dictionary of error statistics.

        """
        uptime = time.time() - self._start_time
        return {
            "total_errors": self.total_errors,
            "suppressed_errors": self.suppressed_errors,
            "validation_errors": self.validation_errors,
            "routing_errors": self.routing_errors,
            "processing_errors": self.processing_errors,
            "unknown_errors": self.unknown_errors,
            "errors_per_minute": (self.total_errors / uptime) * 60 if uptime > 0 else 0,
            "uptime_seconds": uptime,
        }


class BaseErrorHandler:
    """Centralized error handling for WebSocket messages."""

    def __init__(
        self,
        exchange_name: ExchangeName,
        suppression_config: ErrorSuppressionConfig | None = None,
    ) -> None:
        """Initialize the error handler.

        Args:
            exchange_name: Name of the exchange for logging context.
            suppression_config: Configuration for error suppression.

        """
        self.exchange_name = exchange_name.value
        self.suppression_config = suppression_config or ErrorSuppressionConfig()

        # Error suppression cache
        self._error_cache: TTLCache[str, int] = TTLCache(
            maxsize=self.suppression_config.max_cache_size,
            ttl=self.suppression_config.ttl_seconds,
        )
        self._suppressed_counts: dict[str, int] = {}

        # Statistics
        self.stats = ErrorStats()

        # Logger
        self.logger = get_logger(f"WebSocketErrorHandler.{exchange_name}")

    def _generate_error_key(
        self,
        error: Exception,
        payload: dict[str, Any] | None = None,
    ) -> str:
        """Generate a unique key for error deduplication.

        Args:
            error: The error that occurred.
            payload: Optional payload that caused the error.

        Returns:
            Unique error key for caching.

        """
        # Create a hash of error type, message, and payload structure
        key_parts = [
            type(error).__name__,
            str(error),
        ]

        if payload is not None:
            # Add payload structure (keys only) to the hash
            key_parts.append("|".join(sorted(payload.keys())))

        key_str = "|".join(key_parts)
        return hashlib.sha256(key_str.encode()).hexdigest()

    def _should_suppress(self, error_key: str) -> bool:
        """Check if an error should be suppressed.

        Args:
            error_key: Unique error identifier.

        Returns:
            True if error should be suppressed.

        """
        # Get current count
        current_count = self._error_cache.get(error_key, 0)

        # Increment count
        self._error_cache[error_key] = current_count + 1

        # Check if we should suppress
        if current_count >= self.suppression_config.suppression_threshold:
            self._suppressed_counts[error_key] = self._suppressed_counts.get(error_key, 0) + 1

            # Log every Nth suppressed error
            return self._suppressed_counts[error_key] % self.suppression_config.log_every_n != 0

        return False

    def _safe_truncate(
        self,
        data: object,
        max_length: int = 200,
    ) -> str:
        """Safely truncate data for logging.

        Args:
            data: Data to truncate.
            max_length: Maximum string length.

        Returns:
            Truncated string representation.

        """
        try:
            str_data = str(data)
        except (TypeError, ValueError, AttributeError):
            return "<unprintable>"
        else:
            if len(str_data) > max_length:
                return str_data[:max_length] + "..."
            return str_data

    async def handle_validation_error(
        self,
        error: ValidationError,
        payload: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> None:
        """Handle Pydantic validation errors.

        Args:
            error: The validation error.
            payload: The payload that failed validation.
            context: Additional context for logging.

        """
        self.stats.total_errors += 1
        self.stats.validation_errors += 1

        error_key = self._generate_error_key(error, payload)

        if self._should_suppress(error_key):
            self.stats.suppressed_errors += 1
            return

        # Extract validation details
        error_details = [
            {
                "field": ".".join(str(loc) for loc in err["loc"]),
                "type": err["type"],
                "message": err["msg"],
            }
            for err in error.errors()
        ]

        # Remove 'exchange' from context to avoid conflicts
        context_copy = (context or {}).copy()
        context_copy.pop("exchange", None)

        self.logger.error(
            "websocket_validation_error",
            exchange=self.exchange_name,
            error_type=type(error).__name__,
            error_message=str(error),
            error_details=error_details,
            payload_sample=self._safe_truncate(payload),
            suppressed_count=self._suppressed_counts.get(error_key, 0),
            **context_copy,
        )

    async def handle_unroutable_message(
        self,
        message: dict[str, Any],
        reason: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Handle messages that cannot be routed.

        Args:
            message: The unroutable message.
            reason: Optional reason why the message couldn't be routed.
            context: Optional additional context.

        """
        self.stats.total_errors += 1
        self.stats.routing_errors += 1

        # Generate key based on message structure
        error_key = self._generate_error_key(
            ValueError("Unroutable message"),
            message,
        )

        if self._should_suppress(error_key):
            self.stats.suppressed_errors += 1
            return

        log_kwargs = {
            "exchange": self.exchange_name,
            "message_keys": list(message.keys()),
            "message_type": type(message).__name__,
            "message_sample": self._safe_truncate(message),
            "suppressed_count": self._suppressed_counts.get(error_key, 0),
        }

        if reason:
            log_kwargs["reason"] = reason
        if context:
            log_kwargs.update(context)

        self.logger.warning("websocket_unroutable_message", **log_kwargs)

    async def handle_unknown_topic(
        self,
        topic: str,
        message: dict[str, Any],
    ) -> None:
        """Handle messages with unknown topics/channels.

        Args:
            topic: The unknown topic.
            message: The full message.

        """
        self.stats.total_errors += 1
        self.stats.routing_errors += 1

        error_key = f"unknown_topic:{topic}"

        if self._should_suppress(error_key):
            self.stats.suppressed_errors += 1
            return

        self.logger.warning(
            "websocket_unknown_topic",
            exchange=self.exchange_name,
            topic=topic,
            message_sample=self._safe_truncate(message),
            suppressed_count=self._suppressed_counts.get(error_key, 0),
        )

    async def handle_processing_error(
        self,
        error: Exception,
        payload: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> None:
        """Handle general processing errors.

        Args:
            error: The processing error.
            payload: The payload being processed.
            context: Additional context.

        """
        self.stats.total_errors += 1
        self.stats.processing_errors += 1

        error_key = self._generate_error_key(error, payload)

        if self._should_suppress(error_key):
            self.stats.suppressed_errors += 1
            return

        # Remove 'exchange' from context to avoid conflicts
        context_copy = (context or {}).copy()
        context_copy.pop("exchange", None)

        self.logger.error(
            "websocket_processing_error",
            exchange=self.exchange_name,
            error_type=type(error).__name__,
            error_message=str(error),
            payload_sample=self._safe_truncate(payload),
            suppressed_count=self._suppressed_counts.get(error_key, 0),
            **context_copy,
        )

    async def handle_routing_error(
        self,
        error: Exception,
        message: dict[str, Any],
    ) -> None:
        """Handle errors during message routing.

        Args:
            error: The routing error.
            message: The message being routed.

        """
        self.stats.total_errors += 1
        self.stats.routing_errors += 1

        error_key = self._generate_error_key(error, message)

        if self._should_suppress(error_key):
            self.stats.suppressed_errors += 1
            return

        self.logger.error(
            "websocket_routing_error",
            exchange=self.exchange_name,
            error_type=type(error).__name__,
            error_message=str(error),
            message_sample=self._safe_truncate(message),
            suppressed_count=self._suppressed_counts.get(error_key, 0),
        )

    # Pure typed error system - validation errors handled as WebSocketStreamError
    # Use the new typed error system from ws_stream_error_handler.py instead

    def get_stats(self) -> dict[str, Any]:
        """Get error handling statistics.

        Returns:
            Dictionary of statistics.

        """
        return {
            "exchange": self.exchange_name,
            "error_stats": self.stats.to_dict(),
            "cache_info": {
                "size": len(self._error_cache),
                "max_size": self.suppression_config.max_cache_size,
                "ttl_seconds": self.suppression_config.ttl_seconds,
            },
            "suppression_info": {
                "threshold": self.suppression_config.suppression_threshold,
                "active_suppressions": len(self._suppressed_counts),
            },
        }

    def reset_stats(self) -> None:
        """Reset error statistics."""
        self.stats = ErrorStats()
        self._suppressed_counts.clear()
        self.logger.info(
            "error_stats_reset",
            exchange=self.exchange_name,
        )

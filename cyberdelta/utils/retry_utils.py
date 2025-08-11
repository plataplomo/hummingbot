"""Retry utilities for event system.

Simple helper to reduce duplication of Retrying configuration.
"""

from collections.abc import Callable
from typing import Any

from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cyberdelta.config.models.event_system_config import EventRetryConfig
from cyberdelta.config.structlog_config import get_logger


def create_retryer(
    retry_config: EventRetryConfig,
    *,
    multiplier_factor: float = 1.0,
    attempts_factor: float = 1.0,
    retry_on: type[Exception] | tuple[type[Exception], ...] = ConnectionError,
    before_sleep: Callable[[RetryCallState], Any] | None = None,
    logger_name: str | None = None,
) -> AsyncRetrying:
    """Create a configured Retrying instance from config.

    Args:
        retry_config: EventRetryConfig with retry parameters
        multiplier_factor: Factor to multiply delays by (0.5 for faster, 2.0 for slower)
        attempts_factor: Factor to multiply max_attempts by (0.5 for half, 2.0 for double)
        retry_on: Exception types to retry on
        before_sleep: Optional callback before sleep (called before retry delay)
        logger_name: Optional logger name for automatic before_sleep logging

    Returns:
        Configured AsyncRetrying instance

    Example:
        retryer = create_retryer(
            self.config.retry_config,
            logger_name="my_handler"  # Automatically adds retry logging
        )
        result = await retryer(self._async_operation, arg1, arg2)
    """
    # Build retryer with base config
    retryer = AsyncRetrying(
        stop=stop_after_attempt(max(1, int(retry_config.max_attempts * attempts_factor))),
        wait=wait_exponential(
            multiplier=retry_config.exponential_base * multiplier_factor,
            min=retry_config.initial_delay_sec * multiplier_factor,
            max=retry_config.max_delay_sec * (multiplier_factor if multiplier_factor <= 1 else 1.0),
        ),
        retry=retry_if_exception_type(retry_on),
        reraise=True,
    )

    # Add before_sleep callback if provided
    if before_sleep is not None:
        retryer.before_sleep = before_sleep
    elif logger_name is not None:
        # Create custom logging callback for structlog
        logger = get_logger(logger_name)

        def log_retry(retry_state: RetryCallState) -> None:
            """Log retry attempts using structlog."""
            logger.warning(
                "retrying_operation",
                attempt=retry_state.attempt_number,
                wait_seconds=(retry_state.next_action and retry_state.next_action.sleep) or 0,
            )

        retryer.before_sleep = log_retry

    return retryer

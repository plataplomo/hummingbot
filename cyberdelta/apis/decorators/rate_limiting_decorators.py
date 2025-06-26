"""Enhanced Rate Limiting and Retry Decorators - CyberDeltaEngine.

Class-based decorators for rate limiting and retry logic that integrate
with the existing infrastructure.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from functools import wraps
from typing import ParamSpec, TypeVar, cast

from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


class RateLimited:
    """Enhanced rate limiting decorator with better state management.

    Integrates with existing RateLimiter infrastructure.
    """

    def __init__(
        self,
        calls_per_minute: int = 60,
        burst_size: int | None = None,
    ) -> None:
        """Initialize rate limiter."""
        self.calls_per_minute = calls_per_minute
        self.burst_size = burst_size or calls_per_minute
        self._rate_limiter = TokenBucketRateLimiterRuntime(
            rate=calls_per_minute / 60.0,
            bucket_size=self.burst_size,
        )

    def __call__(self, func: Callable[P, T]) -> Callable[P, T]:
        """Apply rate limiting."""
        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                # Always wait for rate limit
                await self._rate_limiter.acquire()
                async_func = cast("Callable[P, Awaitable[T]]", func)
                return await async_func(*args, **kwargs)

            return cast("Callable[P, T]", async_wrapper)
        raise TypeError("RateLimited decorator can only be applied to async functions")


class RetryOnFailure:
    """Enhanced retry decorator with exponential backoff.

    Matches current retry patterns in HttpClient.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        retry_on: tuple[type[Exception], ...] = (Exception,),
    ) -> None:
        """Initialize retry settings."""
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.retry_on = retry_on

    def __call__(self, func: Callable[P, T]) -> Callable[P, T]:
        """Apply retry logic."""
        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                last_exception: Exception | None = None
                delay = self.initial_delay
                async_func = cast("Callable[P, Awaitable[T]]", func)

                for attempt in range(self.max_attempts):
                    try:
                        return await async_func(*args, **kwargs)
                    except self.retry_on as e:
                        last_exception = e
                        if attempt < self.max_attempts - 1:
                            logger.warning(
                                f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                                f"Retrying in {delay}s...",
                            )
                            await asyncio.sleep(delay)
                            delay = min(delay * self.exponential_base, self.max_delay)
                        else:
                            logger.error(
                                f"All {self.max_attempts} attempts failed for {func.__name__}",
                            )

                if last_exception is not None:
                    raise last_exception
                raise RuntimeError("No exception captured")

            return cast("Callable[P, T]", async_wrapper)
        raise TypeError("RetryOnFailure decorator can only be applied to async functions")


class CircuitBreaker:
    """Circuit breaker pattern for API resilience.

    Prevents cascading failures by temporarily disabling calls to failing services.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type[Exception] = Exception,
    ) -> None:
        """Initialize circuit breaker."""
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self._failure_count = 0
        self._last_failure_time: datetime | None = None
        self._state: str = "closed"  # closed, open, half-open
        self._lock = asyncio.Lock()

    def __call__(self, func: Callable[P, T]) -> Callable[P, T]:
        """Apply circuit breaker logic."""
        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                async with self._lock:
                    # Check if circuit should be reset
                    if self._state == "open" and self._last_failure_time:
                        elapsed = (datetime.now(UTC) - self._last_failure_time).total_seconds()
                        if elapsed >= self.recovery_timeout:
                            self._state = "half-open"
                            self._failure_count = 0

                    # If circuit is open, fail fast
                    if self._state == "open":
                        raise APIError(
                            message=f"Circuit breaker is open for {func.__name__}",
                            code=APIErrorCode.SERVICE_UNAVAILABLE.value,
                        )

                try:
                    async_func = cast("Callable[P, Awaitable[T]]", func)
                    result = await async_func(*args, **kwargs)

                    # Success - reset failure count if in half-open state
                    async with self._lock:
                        if self._state == "half-open":
                            self._state = "closed"
                            self._failure_count = 0

                    return result

                except self.expected_exception:
                    async with self._lock:
                        self._failure_count += 1
                        self._last_failure_time = datetime.now(UTC)

                        if self._failure_count >= self.failure_threshold:
                            self._state = "open"
                            logger.error(
                                f"Circuit breaker opened for {func.__name__} after "
                                f"{self._failure_count} failures",
                            )

                        raise

            return cast("Callable[P, T]", async_wrapper)
        raise TypeError("CircuitBreaker decorator can only be applied to async functions")


class Timeout:
    """Timeout decorator for async operations.

    Ensures operations complete within specified time limits.
    """

    def __init__(self, seconds: float) -> None:
        """Initialize timeout decorator."""
        self.seconds = seconds

    def __call__(self, func: Callable[P, T]) -> Callable[P, T]:
        """Apply timeout."""
        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                try:
                    async_func = cast("Callable[P, Awaitable[T]]", func)
                    return await asyncio.wait_for(async_func(*args, **kwargs), timeout=self.seconds)
                except TimeoutError as e:
                    raise APIError(
                        message=f"Operation {func.__name__} timed out after {self.seconds}s",
                        code=APIErrorCode.TIMEOUT.value,
                    ) from e

            return cast("Callable[P, T]", async_wrapper)
        raise TypeError("Timeout decorator can only be applied to async functions")


# Legacy function interfaces for backward compatibility
def rate_limited(
    calls_per_minute: int = 60,
    burst_size: int | None = None,
) -> RateLimited:
    """Legacy function interface - use RateLimited decorator instead."""
    return RateLimited(
        calls_per_minute=calls_per_minute,
        burst_size=burst_size,
    )


def retry_on_failure(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    retry_on: tuple[type[Exception], ...] = (Exception,),
) -> RetryOnFailure:
    """Legacy function interface - use RetryOnFailure decorator instead."""
    return RetryOnFailure(
        max_attempts=max_attempts,
        initial_delay=initial_delay,
        max_delay=max_delay,
        exponential_base=exponential_base,
        retry_on=retry_on,
    )

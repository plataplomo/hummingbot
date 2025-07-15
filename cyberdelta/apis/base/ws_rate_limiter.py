"""WebSocket rate limiting system.

This module provides comprehensive rate limiting for WebSocket operations,
supporting global, per-connection, and per-message-type limits with
token bucket and sliding window algorithms.
"""

from __future__ import annotations

import time
from collections import deque
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, ValidationInfo, field_validator


class BurstSizeTooLargeError(ValueError):
    """Raised when burst size is too large for the configured rate."""

    def __init__(self, burst_size: int, rate: float) -> None:
        """Initialize with burst size and rate values."""
        super().__init__(f"Burst size {burst_size} too large for rate {rate}/s")


class UnsupportedAlgorithmError(ValueError):
    """Raised when an unsupported rate limiting algorithm is specified."""

    def __init__(self, algorithm: str) -> None:
        """Initialize with algorithm name."""
        super().__init__(f"Unsupported algorithm: {algorithm}")


class RateLimitType(StrEnum):
    """Types of rate limits."""

    GLOBAL = "global"
    PER_CONNECTION = "per_connection"
    PER_MESSAGE_TYPE = "per_message_type"
    PER_USER = "per_user"


class RateLimitAlgorithm(StrEnum):
    """Rate limiting algorithms."""

    TOKEN_BUCKET = "token_bucket"  # noqa: S105
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


class RateLimitConfig(BaseModel):
    """Configuration for a rate limit rule."""

    limit_type: RateLimitType
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET
    requests_per_second: float = Field(gt=0, description="Maximum requests per second")
    burst_size: int = Field(gt=0, description="Maximum burst size")
    window_size_seconds: int = Field(default=60, gt=0, description="Window size for sliding window")
    message_types: list[str] = Field(
        default_factory=list, description="Message types this limit applies to"
    )
    enabled: bool = True

    @field_validator("burst_size")
    @classmethod
    def burst_size_must_be_reasonable(cls, v: int, info: ValidationInfo) -> int:
        """Validate burst size is reasonable compared to rate."""
        data = info.data
        if "requests_per_second" in data:
            max_burst = int(data["requests_per_second"] * 10)  # Max 10 seconds worth
            if v > max_burst:
                rate = data["requests_per_second"]
                raise BurstSizeTooLargeError(v, rate)
        return v


class RateLimitResult(BaseModel):
    """Result of a rate limit check."""

    allowed: bool
    limit_type: RateLimitType
    remaining: int = Field(ge=0, description="Remaining requests in current window")
    reset_time: datetime = Field(description="When the limit resets")
    retry_after_seconds: float | None = Field(
        default=None, description="Seconds to wait before retry"
    )
    current_rate: float = Field(ge=0, description="Current request rate")


class TokenBucket:
    """Token bucket rate limiter implementation."""

    def __init__(self, requests_per_second: float, burst_size: int) -> None:
        """Initialize token bucket.

        Args:
            requests_per_second: Rate at which tokens are added
            burst_size: Maximum number of tokens in bucket
        """
        self.requests_per_second = requests_per_second
        self.burst_size = burst_size
        self.tokens = float(burst_size)
        self.last_update = time.time()

    def is_allowed(self) -> tuple[bool, float]:
        """Check if request is allowed and return remaining tokens.

        Returns:
            Tuple of (is_allowed, remaining_tokens)
        """
        current_time = time.time()

        # Add tokens based on elapsed time
        elapsed = current_time - self.last_update
        self.tokens = min(self.burst_size, self.tokens + (elapsed * self.requests_per_second))
        self.last_update = current_time

        # Check if we have tokens available
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True, self.tokens
        return False, self.tokens

    def time_until_available(self) -> float:
        """Get seconds until next token is available."""
        if self.tokens >= 1.0:
            return 0.0
        return (1.0 - self.tokens) / self.requests_per_second

    def current_rate(self) -> float:
        """Get current request rate (same as configured rate for token bucket)."""
        return self.requests_per_second


class SlidingWindowCounter:
    """Sliding window rate limiter implementation."""

    def __init__(self, requests_per_second: float, window_size_seconds: int) -> None:
        """Initialize sliding window counter.

        Args:
            requests_per_second: Maximum requests per second
            window_size_seconds: Size of the sliding window
        """
        self.requests_per_second = requests_per_second
        self.window_size_seconds = window_size_seconds
        self.max_requests = int(requests_per_second * window_size_seconds)
        self.requests: deque[float] = deque()

    def is_allowed(self) -> tuple[bool, int]:
        """Check if request is allowed and return remaining requests.

        Returns:
            Tuple of (is_allowed, remaining_requests)
        """
        current_time = time.time()
        cutoff_time = current_time - self.window_size_seconds

        # Remove old requests outside the window
        while self.requests and self.requests[0] <= cutoff_time:
            self.requests.popleft()

        # Check if we're within limits
        if len(self.requests) < self.max_requests:
            self.requests.append(current_time)
            return True, self.max_requests - len(self.requests)
        return False, 0

    def time_until_available(self) -> float:
        """Get seconds until oldest request expires."""
        if len(self.requests) < self.max_requests:
            return 0.0
        if not self.requests:
            return 0.0
        return max(0.0, self.requests[0] + self.window_size_seconds - time.time())

    def current_rate(self) -> float:
        """Get current request rate per second."""
        if not self.requests:
            return 0.0
        return len(self.requests) / self.window_size_seconds


class WebSocketRateLimiter:
    """Comprehensive WebSocket rate limiter."""

    def __init__(self, configs: list[RateLimitConfig]) -> None:
        """Initialize rate limiter with configurations.

        Args:
            configs: List of rate limiting configurations
        """
        self.configs = {config.limit_type: config for config in configs if config.enabled}
        self.limiters: dict[str, TokenBucket | SlidingWindowCounter] = {}
        self.global_limiter: TokenBucket | SlidingWindowCounter | None = None

        # Initialize global limiter if configured
        if RateLimitType.GLOBAL in self.configs:
            self.global_limiter = self._create_limiter(self.configs[RateLimitType.GLOBAL])

    def _create_limiter(self, config: RateLimitConfig) -> TokenBucket | SlidingWindowCounter:
        """Create a limiter instance based on configuration.

        Args:
            config: Rate limit configuration

        Returns:
            Configured limiter instance
        """
        if config.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
            return TokenBucket(config.requests_per_second, config.burst_size)
        if config.algorithm == RateLimitAlgorithm.SLIDING_WINDOW:
            return SlidingWindowCounter(config.requests_per_second, config.window_size_seconds)
        raise UnsupportedAlgorithmError(config.algorithm)

    def _get_limiter_key(
        self,
        limit_type: RateLimitType,
        connection_id: str,
        message_type: str | None = None,
        user_id: str | None = None,
    ) -> str:
        """Generate key for limiter lookup.

        Args:
            limit_type: Type of rate limit
            connection_id: Connection identifier
            message_type: Optional message type
            user_id: Optional user identifier

        Returns:
            Limiter key string
        """
        if limit_type == RateLimitType.PER_CONNECTION:
            return f"conn:{connection_id}"
        if limit_type == RateLimitType.PER_MESSAGE_TYPE:
            return f"msg:{message_type or 'unknown'}"
        if limit_type == RateLimitType.PER_USER:
            return f"user:{user_id or 'anonymous'}"
        return "global"

    def check_rate_limit(
        self, connection_id: str, message_type: str | None = None, user_id: str | None = None
    ) -> RateLimitResult:
        """Check if request is within rate limits.

        Args:
            connection_id: Connection identifier
            message_type: Optional message type
            user_id: Optional user identifier

        Returns:
            Rate limit check result
        """
        current_time = datetime.now(UTC)

        # Check global limit first
        if self.global_limiter and RateLimitType.GLOBAL in self.configs:
            allowed, remaining = self.global_limiter.is_allowed()
            if not allowed:
                retry_after = self.global_limiter.time_until_available()
                current_rate = self.global_limiter.current_rate()
                return RateLimitResult(
                    allowed=False,
                    limit_type=RateLimitType.GLOBAL,
                    remaining=int(remaining),
                    reset_time=current_time,
                    retry_after_seconds=retry_after,
                    current_rate=current_rate,
                )

        # Check other configured limits
        for limit_type, config in self.configs.items():
            if limit_type == RateLimitType.GLOBAL:
                continue  # Already checked

            # Skip if message type doesn't match filter
            if (
                limit_type == RateLimitType.PER_MESSAGE_TYPE
                and config.message_types
                and message_type not in config.message_types
            ):
                continue

            limiter_key = self._get_limiter_key(limit_type, connection_id, message_type, user_id)

            # Get or create limiter
            if limiter_key not in self.limiters:
                self.limiters[limiter_key] = self._create_limiter(config)

            limiter = self.limiters[limiter_key]
            allowed, remaining = limiter.is_allowed()

            if not allowed:
                retry_after = limiter.time_until_available()
                current_rate = limiter.current_rate()
                return RateLimitResult(
                    allowed=False,
                    limit_type=limit_type,
                    remaining=int(remaining),
                    reset_time=current_time,
                    retry_after_seconds=retry_after,
                    current_rate=current_rate,
                )

        # All limits passed
        return RateLimitResult(
            allowed=True,
            limit_type=RateLimitType.GLOBAL,  # Default
            remaining=999,  # Placeholder
            reset_time=current_time,
            current_rate=0.0,
        )

    def get_stats(self) -> dict[str, Any]:
        """Get rate limiter statistics.

        Returns:
            Dictionary of statistics
        """
        stats: dict[str, Any] = {
            "total_limiters": len(self.limiters),
            "global_limiter_enabled": self.global_limiter is not None,
            "configured_limits": list(self.configs.keys()),
            "limiter_details": {},
        }

        # Add details for each limiter
        for key, limiter in self.limiters.items():
            limiter_stats: dict[str, Any] = {
                "type": type(limiter).__name__,
            }

            if isinstance(limiter, TokenBucket):
                limiter_stats.update({
                    "current_tokens": str(limiter.tokens),
                    "burst_size": str(limiter.burst_size),
                    "requests_per_second": str(limiter.requests_per_second),
                })
            else:
                # Must be SlidingWindowCounter due to _create_limiter implementation
                limiter_stats.update({
                    "current_requests": str(len(limiter.requests)),
                    "max_requests": str(limiter.max_requests),
                    "current_rate": str(limiter.current_rate()),
                    "window_size": str(limiter.window_size_seconds),
                })

            stats["limiter_details"][key] = limiter_stats

        return stats

    def reset_limiter(
        self, connection_id: str, message_type: str | None = None, user_id: str | None = None
    ) -> None:
        """Reset specific limiters.

        Args:
            connection_id: Connection identifier
            message_type: Optional message type
            user_id: Optional user identifier
        """
        for limit_type in self.configs:
            if limit_type == RateLimitType.GLOBAL:
                continue

            limiter_key = self._get_limiter_key(limit_type, connection_id, message_type, user_id)
            if limiter_key in self.limiters:
                del self.limiters[limiter_key]

    def reset_all(self) -> None:
        """Reset all limiters."""
        self.limiters.clear()
        if self.global_limiter:
            config = self.configs[RateLimitType.GLOBAL]
            self.global_limiter = self._create_limiter(config)


class RateLimitError(Exception):
    """Exception raised when rate limit is exceeded."""

    def __init__(self, result: RateLimitResult, message: str | None = None) -> None:
        """Initialize rate limit error.

        Args:
            result: Rate limit check result
            message: Optional error message
        """
        self.result = result
        super().__init__(message or f"Rate limit exceeded for {result.limit_type}")


class RateLimitMiddleware:
    """Middleware for applying rate limits to WebSocket operations."""

    def __init__(self, rate_limiter: WebSocketRateLimiter) -> None:
        """Initialize rate limit middleware.

        Args:
            rate_limiter: Rate limiter instance
        """
        self.rate_limiter = rate_limiter

    async def check_rate_limit(
        self,
        connection_id: str,
        message_type: str | None = None,
        user_id: str | None = None,
        raise_on_limit: bool = True,
    ) -> RateLimitResult:
        """Check rate limit and optionally raise exception.

        Args:
            connection_id: Connection identifier
            message_type: Optional message type
            user_id: Optional user identifier
            raise_on_limit: Whether to raise exception on limit exceeded

        Returns:
            Rate limit check result

        Raises:
            RateLimitError: If rate limit exceeded and raise_on_limit is True
        """
        result = self.rate_limiter.check_rate_limit(connection_id, message_type, user_id)

        if not result.allowed and raise_on_limit:
            raise RateLimitError(result)

        return result

"""Token bucket rate limiter implementation for API requests."""

from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class TokenBucketRateLimiterRuntime:
    """Runtime/business logic for a token bucket rate limiter.

    This is NOT a Pydantic model. It manages async state and provides the acquire() method.
    Use from_pydantic() and to_pydantic() to bridge with the config/state model.
    """

    def __init__(
        self,
        rate: float,
        bucket_size: int,
        tokens: float | None = None,
        last_refill: float | None = None,
    ) -> None:
        """Initialize the token bucket rate limiter.

        Args:
            rate: Token refill rate (tokens per second)
            bucket_size: Maximum number of tokens the bucket can hold
            tokens: Initial number of tokens (defaults to bucket_size)
            last_refill: Last refill timestamp (defaults to current time)

        """
        self.rate = rate
        self.bucket_size = bucket_size
        self.tokens = float(tokens) if tokens is not None else float(bucket_size)
        self.last_refill = last_refill if last_refill is not None else time.monotonic()
        self.lock = asyncio.Lock()
        self.is_ip_banned_until: float | None = None

    async def acquire(self, tokens_to_consume: int = 1) -> float:
        """Acquire tokens from the bucket.

        Args:
            tokens_to_consume: Number of tokens to consume (default: 1)

        Returns:
            Time waited in seconds (0.0 if no wait was needed)

        """
        wait_time = 0.0
        async with self.lock:
            # Check if IP ban is active
            if self.is_ip_banned_until is not None:
                now_mono = time.monotonic()
                if now_mono < self.is_ip_banned_until:
                    wait_time_for_ban = self.is_ip_banned_until - now_mono
                    # Release lock while sleeping
                    self.lock.release()
                    try:
                        logger.warning(
                            f"IP ban active for rate limiter. Waiting {wait_time_for_ban:.2f}s.",
                        )
                        await asyncio.sleep(wait_time_for_ban)
                        wait_time += wait_time_for_ban
                    finally:
                        await self.lock.acquire()
                    # After waiting, clear the ban state and proceed to token acquisition
                    self.is_ip_banned_until = None
                else:  # Ban duration has passed
                    self.is_ip_banned_until = None

            # Proceed with normal token acquisition
            now = time.monotonic()
            elapsed = now - self.last_refill
            new_tokens = elapsed * self.rate
            if new_tokens > 0:
                self.tokens = min(self.bucket_size, self.tokens + new_tokens)
                self.last_refill = now
            if self.tokens < tokens_to_consume:
                token_wait_time = (tokens_to_consume - self.tokens) / self.rate
                self.lock.release()
                try:
                    await asyncio.sleep(token_wait_time)
                    wait_time += token_wait_time
                finally:
                    await self.lock.acquire()
                now = time.monotonic()
                elapsed = now - self.last_refill
                new_tokens = elapsed * self.rate
                self.tokens = min(self.bucket_size, self.tokens + new_tokens)
                self.last_refill = now
            self.tokens -= tokens_to_consume
            return wait_time

    async def trigger_ip_ban(self, duration_seconds: float) -> None:
        """Trigger an IP ban for the specified duration.

        Args:
            duration_seconds: Duration of the IP ban in seconds.

        """
        async with self.lock:
            self.is_ip_banned_until = time.monotonic() + duration_seconds
            logger.critical(f"Rate limiter IP BAN triggered for {duration_seconds:.1f}s.")

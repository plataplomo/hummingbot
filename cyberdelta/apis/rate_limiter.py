"""Token bucket rate limiter implementation for API requests."""

from __future__ import annotations

import asyncio
import time

from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


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

        while True:
            sleep_time = 0.0

            async with self.lock:
                # Check if IP ban is active
                if self.is_ip_banned_until is not None:
                    now_mono = time.monotonic()
                    if now_mono < self.is_ip_banned_until:
                        sleep_time = self.is_ip_banned_until - now_mono
                        logger.warning(
                            "rate_limiter_ip_ban_waiting",
                            wait_time_seconds=sleep_time,
                            action="waiting_for_ip_ban_expiry",
                            message=(f"IP ban active for rate limiter. Waiting {sleep_time:.2f}s."),
                        )
                    else:
                        # Ban duration has passed
                        self.is_ip_banned_until = None

                # If no IP ban wait needed, proceed with token acquisition
                if sleep_time == 0.0:
                    # Refill tokens based on elapsed time
                    now = time.monotonic()
                    elapsed = now - self.last_refill
                    new_tokens = elapsed * self.rate
                    if new_tokens > 0:
                        # For normal token refill, cap at bucket size
                        # But during acquisition, we need to allow checking the full refilled amount
                        potential_tokens = self.tokens + new_tokens
                        self.last_refill = now
                    else:
                        potential_tokens = self.tokens

                    # Check if we have enough tokens (including newly refilled ones)
                    if potential_tokens >= tokens_to_consume:
                        # Update actual tokens, but cap at bucket size AFTER consuming
                        self.tokens = min(self.bucket_size, potential_tokens - tokens_to_consume)
                        # Clear IP ban if it was set and we successfully got tokens
                        if self.is_ip_banned_until is not None:
                            self.is_ip_banned_until = None
                        return wait_time
                    # Update tokens to capped amount for next iteration
                    self.tokens = min(self.bucket_size, potential_tokens)
                    # Calculate wait time for tokens
                    sleep_time = (tokens_to_consume - potential_tokens) / self.rate

            # Sleep outside the lock (whether for IP ban or token wait)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
                wait_time += sleep_time
                # Loop back to reacquire lock and recheck state

    async def trigger_ip_ban(self, duration_seconds: float) -> None:
        """Trigger an IP ban for the specified duration.

        Args:
            duration_seconds: Duration of the IP ban in seconds.

        """
        async with self.lock:
            self.is_ip_banned_until = time.monotonic() + duration_seconds
            logger.critical(
                "rate_limiter_ip_ban_triggered",
                action="trigger_ip_ban",
                duration_seconds=duration_seconds,
                message=f"Rate limiter IP BAN triggered for {duration_seconds:.1f}s.",
            )

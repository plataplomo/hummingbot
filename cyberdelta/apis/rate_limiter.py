from __future__ import annotations

import asyncio
import time

from cyberdelta.apis.models.api import RateLimiterConfig as RateLimiterConfigModel


class TokenBucketRateLimiterRuntime:
    """
    Runtime/business logic for a token bucket rate limiter.
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
        self.rate = rate
        self.bucket_size = bucket_size
        self.tokens = float(tokens) if tokens is not None else float(bucket_size)
        self.last_refill = last_refill if last_refill is not None else time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> float:
        wait_time = 0.0
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            new_tokens = elapsed * self.rate
            if new_tokens > 0:
                self.tokens = min(self.bucket_size, self.tokens + new_tokens)
                self.last_refill = now
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate
                self.lock.release()
                try:
                    await asyncio.sleep(wait_time)
                finally:
                    await self.lock.acquire()
                now = time.monotonic()
                elapsed = now - self.last_refill
                new_tokens = elapsed * self.rate
                self.tokens = min(self.bucket_size, self.tokens + new_tokens)
                self.last_refill = now
            self.tokens -= 1
            return wait_time

    @classmethod
    def from_pydantic(cls, model: RateLimiterConfigModel) -> TokenBucketRateLimiterRuntime:
        return cls(
            rate=model.rate,
            bucket_size=model.bucket_size,
            tokens=model.tokens,
            last_refill=model.last_refill,
        )

    def to_pydantic(self) -> RateLimiterConfigModel:
        return RateLimiterConfigModel(
            rate=self.rate,
            bucket_size=self.bucket_size,
            tokens=self.tokens,
            last_refill=self.last_refill,
        )

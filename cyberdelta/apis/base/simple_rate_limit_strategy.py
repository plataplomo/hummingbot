"""
cyberdelta.apis.base.simple_rate_limit_strategy
---------------------------------------------
Simple token bucket strategy implementation for exchanges with basic rate limiting.

This strategy uses a single TokenBucketRateLimiterRuntime instance and consumes
tokens based on request weight. It's suitable for exchanges like Backpack that
have straightforward rate limiting without complex IP weight calculations.
"""

from __future__ import annotations

from typing import Any

from cyberdelta.apis.base.rate_limit_strategy_interface import RateLimitStrategy
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime


class SimpleTokenBucketStrategy(RateLimitStrategy):
    """
    Simple rate limiting strategy using a single token bucket.

    This strategy is appropriate for exchanges with basic rate limiting where
    each request consumes a configurable number of tokens from a single bucket.
    """

    def __init__(
        self, limiter: TokenBucketRateLimiterRuntime, default_request_weight: int = 1
    ) -> None:
        """
        Initialize the simple token bucket strategy.

        Args:
            limiter: The token bucket rate limiter instance to use.
            default_request_weight: Default number of tokens to consume per request.
        """
        self.limiter = limiter
        self.default_request_weight = default_request_weight

    async def prepare_and_acquire(self, request_context: dict[str, Any]) -> None:
        """
        Acquire tokens from the bucket based on request weight.

        Args:
            request_context: Dict containing request details. Uses 'request_weight'
                           if present, otherwise falls back to default_request_weight.

        Returns:
            None - this strategy does not modify the request payload.
        """
        cost = request_context.get("request_weight", self.default_request_weight)
        if cost > 0:  # Only acquire if cost is positive
            await self.limiter.acquire(tokens_to_consume=cost)
        return None  # Does not modify data payload

    async def handle_exchange_retry_after(
        self, duration_seconds: float, request_context: dict[str, Any]
    ) -> None:
        """
        Handle exchange-advised retry-after delays.
        
        The simple token bucket strategy does not react to retry-after directives,
        as it maintains a constant rate limit. Subclasses can override this method
        to implement exchange-specific behavior.

        Args:
            duration_seconds: The exchange-advised delay in seconds (ignored).
            request_context: Context of the request that was rate-limited (ignored).
        """
        pass  # No-op for simple strategy

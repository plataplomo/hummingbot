"""Simple token bucket rate limiting strategy for exchanges with basic rate limiting.

---------------------------------------------
Simple token bucket strategy implementation for exchanges with basic rate limiting.

This strategy uses a single TokenBucketRateLimiterRuntime instance and consumes
tokens based on request weight. It's suitable for exchanges like Backpack that
have straightforward rate limiting without complex IP weight calculations.
"""

from __future__ import annotations

from cyberdelta.apis.base.rate_limit_models import RateLimitRequestContext
from cyberdelta.apis.base.rate_limit_strategy_interface import RateLimitStrategy
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime


class SimpleTokenBucketStrategy(RateLimitStrategy):
    """Simple rate limiting strategy using a single token bucket.

    This strategy is appropriate for exchanges with basic rate limiting where
    each request consumes a configurable number of tokens from a single bucket.
    """

    def __init__(
        self,
        limiter: TokenBucketRateLimiterRuntime,
        default_request_weight: int = 1,
    ) -> None:
        """Initialize the simple token bucket strategy.

        Args:
            limiter: The token bucket rate limiter instance to use.
            default_request_weight: Default number of tokens to consume per request.

        """
        self.limiter = limiter
        self.default_request_weight = default_request_weight

    async def prepare_and_acquire(self, request_context: RateLimitRequestContext) -> None:
        """Acquire tokens from the bucket based on request weight.

        Args:
            request_context: RateLimitRequestContext containing request details.

        """
        cost = request_context.request_weight
        if cost > 0:  # Only acquire if cost is positive
            await self.limiter.acquire(tokens_to_consume=cost)

    async def handle_exchange_retry_after(
        self,
        duration_seconds: float,
        request_context: RateLimitRequestContext,
    ) -> None:
        """Reacts to an exchange-advised retry_after directive.

        For this simple strategy, it means temporarily pausing its limiter.

        Args:
            duration_seconds: The exchange-advised delay in seconds.
            request_context: RateLimitRequestContext with request details.

        """
        if hasattr(self, "limiter") and hasattr(self.limiter, "trigger_ip_ban"):
            # Log the action being taken by this specific strategy
            from cyberdelta.config.structlog_config import get_logger

            logger = get_logger(__name__)
            logger.info(
                "rate_limit_retry_after_received",
                strategy="SimpleTokenBucketStrategy",
                exchange_name=request_context.exchange_name,
                duration_seconds=duration_seconds,
                message=(
                    f"SimpleTokenBucketStrategy for {request_context.exchange_name}: "
                    f"Received exchange-advised retry_after of {duration_seconds:.2f}s. "
                    f"Triggering temporary pause on its limiter."
                ),
            )
            await self.limiter.trigger_ip_ban(duration_seconds)
        else:
            # This case implies incorrect setup or that the limiter doesn't support banning
            pass

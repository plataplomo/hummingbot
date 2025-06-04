"""cyberdelta.apis.backpack.bp_rate_limit_strategy.

---------------------------------------------

Backpack-specific rate limiting strategy that extends SimpleTokenBucketStrategy
to handle exchange-advised retry delays.

This strategy inherits all the basic token bucket functionality from SimpleTokenBucketStrategy
and adds the ability to react to explicit retry-after directives from Backpack's error messages.
"""

from typing import Any

from cyberdelta.apis.base.simple_rate_limit_strategy import SimpleTokenBucketStrategy
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
from cyberdelta.config.logging_config import get_logger

logger = get_logger(__name__)


class BackpackRateLimitStrategy(SimpleTokenBucketStrategy):
    """Backpack-specific rate limiting strategy.

    Extends SimpleTokenBucketStrategy to handle exchange-advised retry-after delays
    by triggering temporary pauses on the underlying token bucket limiter.
    """

    def __init__(
        self,
        limiter: TokenBucketRateLimiterRuntime,
        default_request_weight: int = 1,
    ) -> None:
        """Initialize the Backpack rate limit strategy.

        Args:
            limiter: The token bucket rate limiter instance to use.
            default_request_weight: Default number of tokens to consume per request.

        """
        super().__init__(limiter, default_request_weight)

    async def handle_exchange_retry_after(
        self,
        duration_seconds: float,
        request_context: dict[str, Any],
    ) -> None:
        """React to an explicit retry-after directive from Backpack.

        When Backpack returns a rate limit error with a retry-after duration,
        this method triggers a temporary pause on the underlying limiter to
        ensure subsequent requests honor the exchange's requested delay.

        Args:
            duration_seconds: The exchange-advised delay in seconds.
            request_context: Context of the request that was rate-limited,
                           containing details like 'exchange_name', 'method',
                           'endpoint', 'endpoint_group'.

        """
        exchange_name = request_context.get("exchange_name", "N/A")
        logger.info(
            f"BackpackRateLimitStrategy: Received exchange-advised retry_after of "
            f"{duration_seconds:.2f}s. Triggering temporary pause on limiter for "
            f"exchange: {exchange_name}.",
        )

        # Trigger a temporary "ban" on the limiter for the specified duration.
        # This will cause subsequent calls to limiter.acquire() to wait until
        # the ban duration expires.
        await self.limiter.trigger_ip_ban(duration_seconds)

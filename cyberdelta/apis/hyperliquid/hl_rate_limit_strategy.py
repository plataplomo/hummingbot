"""cyberdelta.apis.hyperliquid.hl_rate_limit_strategy.

------------------------------------------------
Hyperliquid-specific rate limiting strategy implementation.

This strategy manages dual rate limiters:
1. IP weight limiter - tracks total IP weight consumption per minute
2. Address action limiter - safety net for address-based actions per minute
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cyberdelta.apis.base.rate_limit_strategy_interface import RateLimitStrategy
from cyberdelta.apis.exceptions.configuration import HyperliquidRateLimitConfigError
from cyberdelta.apis.hyperliquid.hl_request_weighter import HyperliquidRequestWeighter
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.base import RequiredParameterError


if TYPE_CHECKING:
    from cyberdelta.apis.base.rate_limit_models import RateLimitRequestContext
    from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig


logger = get_logger(__name__)


class HyperliquidRateLimitStrategy(RateLimitStrategy):
    """Rate limiting strategy for Hyperliquid exchange.

    Implements Hyperliquid's dual rate limiting system:
    - IP weight-based limiting for all requests
    - Address action count limiting as a safety net for /exchange actions
    """

    def __init__(self, hl_exchange_config: ExchangeSpecificConfig) -> None:
        """Initialize the Hyperliquid rate limit strategy.

        Args:
            hl_exchange_config: Hyperliquid-specific exchange configuration
                               containing rate limit parameters.

        Raises:
            HyperliquidRateLimitConfigError: If required configuration fields are missing
            RequiredParameterError: If required parameters are None after validation
        """
        # Validate we have required Hyperliquid configuration
        missing_fields: list[str] = []
        if not hl_exchange_config.ip_weight_limit_per_minute:
            missing_fields.append("ip_weight_limit_per_minute")
        if not hl_exchange_config.address_action_safety_net:
            missing_fields.append("address_action_safety_net")

        if missing_fields:
            raise HyperliquidRateLimitConfigError(missing_fields)

        # Initialize request weighter utility
        self._request_weighter = HyperliquidRequestWeighter(hl_exchange_config)

        # IP Weight Limiter
        ip_rate_rpm = hl_exchange_config.ip_weight_limit_per_minute
        if ip_rate_rpm is None:  # Already checked above, but mypy needs this
            raise RequiredParameterError(
                parameter="ip_weight_limit_per_minute",
                context="HyperliquidRateLimitStrategy",
                exchange=ExchangeName.HYPERLIQUID.value,
            )
        ip_rate_rps = ip_rate_rpm / 60.0
        ip_bucket = max(1, int(ip_rate_rps * 2))  # 2-second bucket
        self._ip_weight_limiter = TokenBucketRateLimiterRuntime(
            rate=ip_rate_rps,
            bucket_size=ip_bucket,
        )
        logger.info(
            "hyperliquid_ip_weight_limiter_initialized",
            rate=ip_rate_rps,
            bucket_size=ip_bucket,
            message="Hyperliquid IP weight limiter initialized",
        )

        # Address Action Count Limiter (Safety Net)
        aa_config = hl_exchange_config.address_action_safety_net
        if aa_config is None:  # Already checked above, but mypy needs this
            raise RequiredParameterError(
                parameter="address_action_safety_net",
                context="HyperliquidRateLimitStrategy",
                exchange=ExchangeName.HYPERLIQUID.value,
            )
        aa_rate_rpm = aa_config.rate_per_minute
        aa_rate_rps = aa_rate_rpm / 60.0
        aa_bucket = max(1, int(aa_rate_rps * 2))  # 2-second bucket
        self._address_action_limiter = TokenBucketRateLimiterRuntime(
            rate=aa_rate_rps,
            bucket_size=aa_bucket,
        )
        logger.info(
            "hyperliquid_address_action_limiter_initialized",
            rate=aa_rate_rps,
            bucket_size=aa_bucket,
            message="Hyperliquid address action limiter initialized",
        )

    async def prepare_and_acquire(self, request_context: RateLimitRequestContext) -> None:
        """Acquire necessary rate limit permissions for a Hyperliquid request.

        This method:
        1. Calculates IP weight cost using the request weighter
        2. Calculates address action count (for /exchange endpoint)
        3. Acquires tokens from the appropriate limiter(s)

        Args:
            request_context: RateLimitRequestContext containing request details
        """
        endpoint = request_context.endpoint
        action_payload = request_context.action_payload
        method = request_context.method

        # Calculate costs using the weighter
        ip_cost = self._request_weighter.get_ip_weight(endpoint, action_payload)
        address_action_cost = self._request_weighter.get_address_action_count(
            endpoint,
            action_payload,
        )

        logger.debug(
            "hyperliquid_rate_limit_costs_calculated",
            method=method,
            endpoint=endpoint,
            ip_cost=ip_cost,
            address_action_cost=address_action_cost,
            message="Hyperliquid rate limit costs calculated",
        )

        # Acquire IP weight tokens if needed
        if ip_cost > 0:
            await self._ip_weight_limiter.acquire(tokens_to_consume=ip_cost)
            logger.debug(
                "acquired_ip_weight_tokens",
                action="acquire_tokens",
                ip_cost=ip_cost,
                message="Acquired IP weight tokens",
            )

        # Acquire address action tokens if needed (only for /exchange)
        if address_action_cost > 0:
            await self._address_action_limiter.acquire(tokens_to_consume=address_action_cost)
            logger.debug(
                "acquired_address_action_tokens",
                action="acquire_tokens",
                address_action_cost=address_action_cost,
                message="Acquired address action tokens",
            )

        # Hyperliquid doesn't modify the payload for rate limiting

    async def trigger_ip_ban_on_main_pool(self, duration_seconds: float) -> None:
        """Trigger an IP ban on the main IP weight limiter.

        This method is called when the exchange returns an IP ban error,
        causing all subsequent requests to wait for the ban duration.

        Args:
            duration_seconds: Duration of the IP ban in seconds.

        """
        await self._ip_weight_limiter.trigger_ip_ban(duration_seconds)

    async def handle_exchange_retry_after(
        self,
        duration_seconds: float,
        request_context: RateLimitRequestContext,
    ) -> None:
        """Handles exchange-advised retry_after directives.

        Hyperliquid's primary rate limit feedback mechanism is an IP ban (403 error),
        which is handled by trigger_ip_ban_on_main_pool. If Hyperliquid were to
        provide explicit retry-after durations in other rate limit error messages,
        this method could be used to trigger a similar ban on the appropriate limiter pool.
        For now, this implementation will call trigger_ip_ban_on_main_pool, assuming any
        explicit retry-after from HL implies a general backoff is needed.

        Args:
            duration_seconds: The exchange-advised delay in seconds.
            request_context: Context of the request that was rate-limited.

        """
        logger.info(
            "hyperliquid_exchange_retry_after_received",
            duration_seconds=duration_seconds,
            exchange_name=request_context.exchange_name,
            message="Received exchange-advised retry_after, applying temporary IP ban",
        )
        await self.trigger_ip_ban_on_main_pool(duration_seconds)
        # If more granular control based on request_context (e.g., endpoint_group) is needed
        # for different limiter pools within HyperliquidRateLimitStrategy,
        # that logic would be added here.

"""
cyberdelta.apis.hyperliquid.hl_rate_limit_strategy
------------------------------------------------
Hyperliquid-specific rate limiting strategy implementation.

This strategy manages dual rate limiters:
1. IP weight limiter - tracks total IP weight consumption per minute
2. Address action limiter - safety net for address-based actions per minute
"""

from __future__ import annotations

import logging
from typing import Any

from cyberdelta.apis.base.rate_limit_strategy_interface import RateLimitStrategy
from cyberdelta.apis.hyperliquid.hl_request_weighter import HyperliquidRequestWeighter
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
from cyberdelta.config.config_models import ExchangeSpecificConfig

logger = logging.getLogger(__name__)


class HyperliquidRateLimitStrategy(RateLimitStrategy):
    """
    Rate limiting strategy for Hyperliquid exchange.

    Implements Hyperliquid's dual rate limiting system:
    - IP weight-based limiting for all requests
    - Address action count limiting as a safety net for /exchange actions
    """

    def __init__(self, hl_exchange_config: ExchangeSpecificConfig) -> None:
        """
        Initialize the Hyperliquid rate limit strategy.

        Args:
            hl_exchange_config: Hyperliquid-specific exchange configuration
                               containing rate limit parameters.
        """
        # Validate we have required Hyperliquid configuration
        if not all(
            [
                hl_exchange_config.ip_weight_limit_per_minute,
                hl_exchange_config.address_action_safety_net,
            ]
        ):
            raise ValueError(
                "HyperliquidRateLimitStrategy requires ip_weight_limit_per_minute "
                "and address_action_safety_net configuration"
            )

        # Initialize request weighter utility
        self._request_weighter = HyperliquidRequestWeighter(hl_exchange_config)

        # IP Weight Limiter
        ip_rate_rpm = hl_exchange_config.ip_weight_limit_per_minute
        if ip_rate_rpm is None:  # Already checked above, but mypy needs this
            raise ValueError("ip_weight_limit_per_minute is required")
        ip_rate_rps = ip_rate_rpm / 60.0
        ip_bucket = max(1, int(ip_rate_rps * 2))  # 2-second bucket
        self._ip_weight_limiter = TokenBucketRateLimiterRuntime(
            rate=ip_rate_rps, bucket_size=ip_bucket
        )
        logger.info(
            f"Hyperliquid IP weight limiter initialized: "
            f"rate={ip_rate_rps:.2f} weights/sec, bucket={ip_bucket} weights"
        )

        # Address Action Count Limiter (Safety Net)
        aa_config = hl_exchange_config.address_action_safety_net
        if aa_config is None:  # Already checked above, but mypy needs this
            raise ValueError("address_action_safety_net is required")
        aa_rate_rpm = aa_config.rate_per_minute
        aa_rate_rps = aa_rate_rpm / 60.0
        aa_bucket = max(1, int(aa_rate_rps * 2))  # 2-second bucket
        self._address_action_limiter = TokenBucketRateLimiterRuntime(
            rate=aa_rate_rps, bucket_size=aa_bucket
        )
        logger.info(
            f"Hyperliquid address action limiter initialized: "
            f"rate={aa_rate_rps:.2f} actions/sec, bucket={aa_bucket} actions"
        )

    async def prepare_and_acquire(self, request_context: dict[str, Any]) -> None:
        """
        Acquire necessary rate limit permissions for a Hyperliquid request.

        This method:
        1. Calculates IP weight cost using the request weighter
        2. Calculates address action count (for /exchange endpoint)
        3. Acquires tokens from the appropriate limiter(s)

        Args:
            request_context: Dict containing request details including:
                - endpoint: The API endpoint path
                - action_payload: The request payload (optional)
                - method: HTTP method (for logging)
                - exchange_name: Exchange name (for logging)

        Returns:
            None - Hyperliquid doesn't modify request payloads for rate limiting.

        Raises:
            APIError: If rate limit acquisition fails or times out.
        """
        endpoint = request_context.get("endpoint", "")
        action_payload = request_context.get("action_payload")
        method = request_context.get("method", "")

        # Calculate costs using the weighter
        ip_cost = self._request_weighter.get_ip_weight(endpoint, action_payload)
        address_action_cost = self._request_weighter.get_address_action_count(
            endpoint, action_payload
        )

        logger.debug(
            f"Hyperliquid rate limit costs for {method} {endpoint}: "
            f"ip_weight={ip_cost}, address_actions={address_action_cost}"
        )

        # Acquire IP weight tokens if needed
        if ip_cost > 0:
            await self._ip_weight_limiter.acquire(tokens_to_consume=ip_cost)
            logger.debug(f"Acquired {ip_cost} IP weight tokens")

        # Acquire address action tokens if needed (only for /exchange)
        if address_action_cost > 0:
            await self._address_action_limiter.acquire(tokens_to_consume=address_action_cost)
            logger.debug(f"Acquired {address_action_cost} address action tokens")

        # Hyperliquid doesn't modify the payload for rate limiting
        return None

    async def trigger_ip_ban_on_main_pool(self, duration_seconds: float) -> None:
        """
        Trigger an IP ban on the main IP weight limiter.

        This method is called when the exchange returns an IP ban error,
        causing all subsequent requests to wait for the ban duration.

        Args:
            duration_seconds: Duration of the IP ban in seconds.
        """
        await self._ip_weight_limiter.trigger_ip_ban(duration_seconds)

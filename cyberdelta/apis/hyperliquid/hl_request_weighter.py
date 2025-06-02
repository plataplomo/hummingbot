"""
cyberdelta.apis.hyperliquid.hl_request_weighter
----------------------------------------------
Utility class for calculating IP weights and address action counts for Hyperliquid requests.

This class encapsulates the logic for determining how many IP weight tokens and
address action tokens a specific Hyperliquid API request will consume, based on
the endpoint and payload.
"""

from __future__ import annotations

import logging
from typing import Any, cast

from cyberdelta.config.config_models import ExchangeSpecificConfig

logger = logging.getLogger(__name__)


class HyperliquidRequestWeighter:
    """
    Calculates IP weights and address action counts for Hyperliquid API requests.

    This utility is used by HyperliquidRateLimitStrategy to determine the cost
    of each request in terms of both IP weight limits and address-based action limits.
    """

    def __init__(self, hl_exchange_config: ExchangeSpecificConfig) -> None:
        """
        Initialize the request weighter with Hyperliquid-specific configuration.

        Args:
            hl_exchange_config: The Hyperliquid exchange configuration containing
                              IP weight mappings and other rate limit parameters.
        """
        self.hl_exchange_config = hl_exchange_config

        # Validate that we have the required Hyperliquid-specific fields
        if not all(
            [
                self.hl_exchange_config.info_request_type_ip_weights is not None,
                self.hl_exchange_config.default_info_weight is not None,
                self.hl_exchange_config.exchange_action_base_ip_weight is not None,
            ]
        ):
            raise ValueError(
                "HyperliquidRequestWeighter requires Hyperliquid-specific "
                "rate limit configuration fields"
            )

    def get_ip_weight(self, endpoint: str, action_payload: dict[str, Any] | None) -> int:
        """
        Calculate the IP weight cost for a given request.

        Args:
            endpoint: The API endpoint path (e.g., "/info", "/exchange")
            action_payload: The request payload (can be None for GET requests)

        Returns:
            The IP weight cost of the request.
        """
        # Handle /exchange endpoint with batch formula
        if endpoint == "/exchange":
            batch_length = 1  # Default for single action
            if action_payload and "actions" in action_payload:
                actions = action_payload["actions"]
                if isinstance(actions, list):
                    batch_length = len(cast(list[Any], actions)) if actions else 1  # type: ignore [redundant-cast]

            # Formula: base_weight + (batch_length // 40)
            base_weight = self.hl_exchange_config.exchange_action_base_ip_weight or 1
            ip_weight = base_weight + (batch_length // 40)

            logger.debug(
                f"Hyperliquid /exchange request: batch_length={batch_length}, ip_weight={ip_weight}"
            )
            return ip_weight

        # Handle /info endpoint with type-specific weights
        elif endpoint == "/info":
            api_type = None
            if action_payload:
                api_type = action_payload.get("type")

            if api_type and self.hl_exchange_config.info_request_type_ip_weights:
                # Look up specific weight for this info type
                ip_weight = self.hl_exchange_config.info_request_type_ip_weights.get(
                    api_type, self.hl_exchange_config.default_info_weight or 20
                )
                logger.debug(f"Hyperliquid /info request: type={api_type}, ip_weight={ip_weight}")
            else:
                # Use default weight for unknown or missing types
                ip_weight = self.hl_exchange_config.default_info_weight or 20
                logger.debug(
                    f"Hyperliquid /info request: unknown type, using default ip_weight={ip_weight}"
                )

            return ip_weight

        # Handle other endpoints with default weight
        else:
            ip_weight = self.hl_exchange_config.default_info_weight or 20
            logger.warning(
                f"Hyperliquid unknown endpoint '{endpoint}', using default ip_weight={ip_weight}"
            )
            return ip_weight

    def get_address_action_count(self, endpoint: str, action_payload: dict[str, Any] | None) -> int:
        """
        Calculate the address action count for a given request.

        Only /exchange endpoint actions count towards the address-based action limit.

        Args:
            endpoint: The API endpoint path (e.g., "/info", "/exchange")
            action_payload: The request payload (can be None for GET requests)

        Returns:
            The number of address-based actions in the request.
        """
        # Only /exchange endpoint contributes to address action count
        if endpoint == "/exchange":
            action_count = 1  # Default for single action
            if action_payload and "actions" in action_payload:
                actions = action_payload["actions"]
                if isinstance(actions, list):
                    action_count = len(cast(list[Any], actions)) if actions else 1  # type: ignore [redundant-cast]

            logger.debug(f"Hyperliquid /exchange request: address_action_count={action_count}")
            return action_count

        # All other endpoints have zero address action count
        return 0

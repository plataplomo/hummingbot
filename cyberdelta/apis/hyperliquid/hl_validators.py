"""Hyperliquid-specific validators.

This module contains validation logic specific to Hyperliquid exchange,
keeping exchange-specific code within the exchange package.
"""

from __future__ import annotations

from cyberdelta.apis.common.base_types import InvalidChannelError


class HyperliquidValidators:
    """Hyperliquid-specific validation utilities."""

    @staticmethod
    def validate_hyperliquid_channel(channel: str) -> str:
        """Validate Hyperliquid channel format.

        Args:
            channel: Channel name (e.g., "l2Book", "trades", "userEvents").

        Returns:
            The validated channel string.

        Raises:
            InvalidChannelError: If channel format is invalid.

        """
        # Known Hyperliquid channels
        valid_channels = {
            "l2Book",
            "trades",
            "userEvents",
            "allMids",
            "notification",
            "webData2",
            "subscriptionResponse",
            "fills",
            "orders",
            "candle",
        }

        if channel not in valid_channels:
            raise InvalidChannelError(channel, valid_channels)

        return channel

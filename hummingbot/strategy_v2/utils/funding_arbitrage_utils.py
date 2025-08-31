"""
Shared utilities for funding arbitrage strategies
Used by both direct strategy and controller implementations
"""
from decimal import Decimal
from typing import ClassVar


class FundingArbitrageConstants:
    """Constants for funding arbitrage strategies"""

    # Exchange quote currency mapping
    QUOTE_MARKETS_MAP: ClassVar[dict[str, str]] = {
        "hyperliquid_perpetual": "USD",
        "binance_perpetual": "USDT",
        "backpack_perpetual": "USDC",
    }

    # Funding payment intervals (in seconds)
    FUNDING_PAYMENT_INTERVAL_MAP: ClassVar[dict[str, int]] = {
        "binance_perpetual": 60 * 60 * 8,  # 8 hours
        "hyperliquid_perpetual": 60 * 60 * 1,  # 1 hour
        "backpack_perpetual": 60 * 60 * 8,  # 8 hours
    }

    # Default profitability calculation interval
    FUNDING_PROFITABILITY_INTERVAL: ClassVar[int] = 60 * 60 * 24  # 24 hours

    @classmethod
    def get_trading_pair_for_connector(cls, token: str, connector: str) -> str:
        """Get the trading pair format for a specific connector"""
        quote = cls.QUOTE_MARKETS_MAP.get(connector, "USDT")
        return f"{token}-{quote}"

    @classmethod
    def get_normalized_funding_rate_per_second(
        cls,
        funding_rate: Decimal,
        connector_name: str
    ) -> Decimal:
        """Normalize funding rate to per-second basis"""
        payment_interval = cls.FUNDING_PAYMENT_INTERVAL_MAP.get(
            connector_name,
            60 * 60 * 8  # Default to 8 hours
        )
        return funding_rate / payment_interval

    @classmethod
    def calculate_funding_diff_profitability(
        cls,
        rate_long: Decimal,
        rate_short: Decimal,
        interval: int | None = None
    ) -> Decimal:
        """
        Calculate funding rate difference profitability.
        Positive value means profitable to LONG on first exchange and SHORT on second.
        """
        if interval is None:
            interval = cls.FUNDING_PROFITABILITY_INTERVAL

        return (rate_short - rate_long) * interval

    @classmethod
    def is_perpetual_exchange(cls, exchange: str) -> bool:
        """Check if exchange is a perpetual/futures market"""
        return "perpetual" in exchange.lower() or "futures" in exchange.lower()

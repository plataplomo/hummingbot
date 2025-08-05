"""Portfolio state model for unified view across all exchanges.

This module provides the PortfolioState model which represents the single
source of truth for the entire portfolio, aggregating balances and positions
from all exchanges.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import DerivativePosition, SpotBalance


class PortfolioState(BaseModel):
    """Unified portfolio state aggregating all balances and positions across ALL exchanges.

    This is the single source of truth for the entire portfolio, combining:
    - Spot balances from all exchanges (Hyperliquid, Backpack, etc.)
    - Derivative positions from all exchanges
    - Calculated total equity across all exchanges in USD

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - All monetary values use Decimal type
    - Exchange identification uses ExchangeName enum
    - Keys use format "{exchange}:{asset}" for uniqueness
    """

    # Using Dict with string keys as we need "{exchange}:{asset}" format
    balances: dict[str, SpotBalance] = Field(
        description='Spot balances keyed by "{exchange}:{asset}" e.g. "hyperliquid:USDC"'
    )

    positions: dict[str, DerivativePosition] = Field(
        description='Derivative positions keyed by "{exchange}:{symbol}" e.g. "backpack:BTC_USD"'
    )

    timestamp: datetime = Field(description="UTC timestamp of this portfolio state snapshot")

    # Optional calculated field - must be explicitly set
    total_equity_usd: Decimal | None = Field(
        default=None,
        description="Sum of all balances + position values in USD (must be calculated)",
    )

    def get_exchange_balances(self, exchange: ExchangeName) -> dict[str, SpotBalance]:
        """Get all balances for a specific exchange.

        Args:
            exchange: Exchange to filter by (uses ExchangeName enum)

        Returns:
            Dictionary of balances for the specified exchange
        """
        prefix = f"{exchange.value}:"
        return {key: balance for key, balance in self.balances.items() if key.startswith(prefix)}

    def get_exchange_positions(self, exchange: ExchangeName) -> dict[str, DerivativePosition]:
        """Get all positions for a specific exchange.

        Args:
            exchange: Exchange to filter by (uses ExchangeName enum)

        Returns:
            Dictionary of positions for the specified exchange
        """
        prefix = f"{exchange.value}:"
        return {key: position for key, position in self.positions.items() if key.startswith(prefix)}

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True

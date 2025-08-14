"""Fee calculator protocol for exchange-specific fee calculations."""

from typing import Any, Protocol

from cyberdelta.models import Fill
from cyberdelta.models.financial import FeeResult


class FeeCalculatorProtocol(Protocol):
    """Protocol for fee calculation implementations.

    This protocol defines the interface for different fee calculation strategies
    across exchanges. Each exchange has unique fee structures, discount models,
    and calculation methods that require specific implementations.

    Use Cases:
    - Exchange-specific fee structures (Binance, Hyperliquid, Backpack)
    - VIP tier discounts and maker/taker rate differences
    - Token-based fee discounts (e.g., BNB on Binance)
    - Referral rebate calculations
    - Testing with predictable mock fee calculations
    """

    def calculate_fee(
        self,
        fill: Fill,
        exchange_config: dict[str, Any],
    ) -> FeeResult:
        """Calculate fees for a fill based on exchange-specific rules.

        Args:
            fill: The fill to calculate fees for
            exchange_config: Exchange-specific configuration including
                           fee rates, VIP tiers, discount tokens, etc.

        Returns:
            FeeResult with calculated fee amount, currency, and calculation details

        Raises:
            ValueError: If fill data is invalid or incomplete
            ConfigurationError: If exchange_config is missing required settings
        """
        ...

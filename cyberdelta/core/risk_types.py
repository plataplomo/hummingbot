"""Risk management type definitions and protocols."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from cyberdelta.validation.funding_data import ArbitrageOpportunity

# Define ZERO and ONE constants for clarity
ZERO = Decimal(0)
ONE = Decimal(1)


class CircuitBreakerSystemProtocol(Protocol):
    """Protocol for circuit breaker system implementation."""

    def check_state(self) -> Any:
        """Check the state of the circuit breaker.

        Returns:
            CircuitBreakerState-like object with is_tripped and reason attributes.
        """
        ...

    def check_exchange(self, exchange: str) -> Any:
        """Check the state of a specific exchange circuit breaker.

        Args:
            exchange: The exchange to check.

        Returns:
            ExchangeCircuitBreakerState-like object with breaker_state attribute.
        """
        ...

    def check_symbol(self, symbol: Any, exchange: str) -> Any:
        """Check the state of a specific symbol circuit breaker.

        Args:
            symbol: The symbol to check.
            exchange: The exchange to check.

        Returns:
            SymbolCircuitBreakerState-like object with breaker_state attribute.
        """
        ...


class FundingRateValidatorProtocol(Protocol):
    """Protocol for funding rate validator implementation."""

    def get_symbol_metrics(self, exchange: str, symbol: Any) -> dict[str, Any] | None:
        """Get metrics for a symbol on an exchange.

        Args:
            exchange: The exchange name.
            symbol: The symbol object.

        Returns:
            Dictionary with metrics including 'rmse', 'bias', 'last_update', etc.
            Returns None if no metrics available.
        """
        ...


@dataclass
class SizedOpportunity:
    """An arbitrage opportunity with calculated position sizes and risk metrics."""

    opportunity: ArbitrageOpportunity
    long_size: Decimal  # Size for the long position (in quote currency, e.g., USD)
    short_size: Decimal  # Size for the short position (in quote currency, e.g., USD)
    allocation_percentage: Decimal  # Percentage of portfolio allocated to this opportunity
    expected_profit: Decimal  # Expected profit in quote currency
    expected_return: Decimal  # Expected return as a percentage
    risk_adjusted_return: Decimal  # Risk-adjusted return (e.g., Sharpe ratio)

    def __post_init__(self) -> None:
        """Validate the sized opportunity after initialization."""
        # Ensure all values are Decimals
        self.long_size = Decimal(str(self.long_size))
        self.short_size = Decimal(str(self.short_size))
        self.allocation_percentage = Decimal(str(self.allocation_percentage))
        self.expected_profit = Decimal(str(self.expected_profit))
        self.expected_return = Decimal(str(self.expected_return))
        self.risk_adjusted_return = Decimal(str(self.risk_adjusted_return))

        # Validate non-negative sizes
        if self.long_size < 0 or self.short_size < 0:
            msg = "Position sizes must be non-negative"
            raise ValueError(msg)

        # Validate allocation percentage is between 0 and 1
        if not (0 <= self.allocation_percentage <= 1):
            msg = f"Allocation percentage must be between 0 and 1, got {self.allocation_percentage}"
            raise ValueError(msg)
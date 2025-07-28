"""Specific result models for symbol operations.

These models provide type-safe, well-named results for complex symbol operations
while keeping the core transformation methods simple and type-safe.
"""

from typing import Any

from pydantic import BaseModel, Field, computed_field

from cyberdelta.core.symbols.models import InternalSymbol


def _create_empty_success_list() -> list[tuple[str, InternalSymbol]]:
    """Factory function for successful transforms list.
    
    Returns:
        Empty list for successful transforms
    """
    return []


def _create_empty_failure_list() -> list[tuple[str, str]]:
    """Factory function for failed transforms list.
    
    Returns:
        Empty list for failed transforms
    """
    return []


class SymbolBatchTransformResult(BaseModel):
    """Result of transforming multiple exchange symbols to internal format.

    This model provides a type-safe container for batch transformation operations,
    clearly separating successful transformations from failures with detailed
    error information.
    """

    successful_transforms: list[tuple[str, InternalSymbol]] = Field(
        default_factory=_create_empty_success_list,
        description="Successfully transformed (exchange_symbol, internal_symbol) pairs",
    )
    failed_transforms: list[tuple[str, str]] = Field(
        default_factory=_create_empty_failure_list,
        description="Failed (exchange_symbol, error_message) pairs",
    )

    @property
    @computed_field
    def success_count(self) -> int:
        """Number of successful transformations.
        
        Returns:
            Count of successful transformations
        """
        return len(self.successful_transforms)

    @property
    @computed_field
    def failure_count(self) -> int:
        """Number of failed transformations.
        
        Returns:
            Count of failed transformations
        """
        return len(self.failed_transforms)

    @property
    @computed_field
    def total_count(self) -> int:
        """Total number of transformation attempts.
        
        Returns:
            Total count of all transformation attempts
        """
        return self.success_count + self.failure_count

    @property
    @computed_field
    def success_rate(self) -> float:
        """Success rate as percentage (0-100).
        
        Returns:
            Percentage of successful transformations
        """
        if self.total_count == 0:
            return 0.0
        return (self.success_count / self.total_count) * 100.0

    @property
    @computed_field
    def has_failures(self) -> bool:
        """Whether any transformations failed.
        
        Returns:
            True if there are any failed transformations
        """
        return self.failure_count > 0

    @property
    @computed_field
    def all_successful(self) -> bool:
        """Whether all transformations succeeded.
        
        Returns:
            True if all transformations were successful
        """
        return self.failure_count == 0 and self.success_count > 0


class SymbolArbitrageCompatibility(BaseModel):
    """Result of checking symbol compatibility across exchanges for arbitrage.

    This model provides detailed information about whether a symbol can be traded
    across multiple exchanges for arbitrage opportunities, including per-exchange
    availability and any compatibility warnings.
    """

    is_arbitrage_compatible: bool = Field(
        description="Whether symbol can be traded across all requested exchanges"
    )
    exchange_availability: dict[str, dict[str, Any]] = Field(
        default_factory=dict, description="Per-exchange availability and symbol mapping"
    )
    compatibility_warnings: list[str] = Field(
        default_factory=list, description="Any warnings about cross-exchange compatibility"
    )

    @property
    @computed_field
    def available_exchanges(self) -> list[str]:
        """List of exchanges where the symbol is available.
        
        Returns:
            List of exchange names where symbol is available
        """
        return [
            exchange_name
            for exchange_name, info in self.exchange_availability.items()
            if info.get("available", False)
        ]

    @property
    @computed_field
    def unavailable_exchanges(self) -> list[str]:
        """List of exchanges where the symbol is not available.
        
        Returns:
            List of exchange names where symbol is not available
        """
        return [
            exchange_name
            for exchange_name, info in self.exchange_availability.items()
            if not info.get("available", False)
        ]

    @property
    @computed_field
    def exchange_symbols(self) -> dict[str, str]:
        """Mapping of exchange names to their symbol representations.
        
        Returns:
            Dictionary mapping exchange names to symbol strings
        """
        return {
            exchange_name: info.get("symbol", "")
            for exchange_name, info in self.exchange_availability.items()
            if info.get("available", False) and info.get("symbol")
        }

    def get_exchange_error(self, exchange_name: str) -> str | None:
        """Get error message for a specific exchange, if any.
        
        Returns:
            Error message string or None if no error
        """
        exchange_info = self.exchange_availability.get(exchange_name, {})
        return exchange_info.get("error") if not exchange_info.get("available", False) else None

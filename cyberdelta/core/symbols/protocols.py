"""Symbol system protocols for type safety and testing.

These protocols define the contracts for symbol storage and transformation,
enabling easy mocking, testing, and future extensibility without complex inheritance.
"""

from typing import Protocol

from cyberdelta.core.symbols.models import InternalSymbol, UnifiedSymbol


class SymbolStoreProtocol(Protocol):
    """Protocol for symbol storage - enables easy mocking and future storage backends."""

    def store(self, symbol: UnifiedSymbol) -> None:
        """Store a unified symbol with cross-exchange mappings."""
        ...

    def get_by_internal(self, internal_symbol: str) -> UnifiedSymbol | None:
        """Retrieve symbol by internal canonical representation."""
        ...

    def get_by_exchange(self, exchange_symbol: str, exchange_name: str) -> UnifiedSymbol | None:
        """Retrieve symbol by exchange-specific representation."""
        ...

    def get_all(self) -> list[UnifiedSymbol]:
        """Get all stored symbols for system-wide operations."""
        ...

    def clear(self) -> None:
        """Clear all stored symbol data."""
        ...


class SymbolTransformerProtocol(Protocol):
    """Protocol for exchange symbol transformations."""

    def internal_to_exchange(self, internal: InternalSymbol) -> str:
        """Transform internal symbol to exchange-specific format."""
        ...

    def exchange_to_internal(self, exchange_symbol: str) -> InternalSymbol:
        """Transform exchange symbol to internal format."""
        ...

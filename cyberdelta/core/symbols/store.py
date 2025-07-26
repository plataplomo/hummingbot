"""Pure storage layer for symbols - implements SymbolStoreProtocol.

This module provides thread-safe symbol storage with fast lookup indices
for symbol operations. Focused on storage concerns only.
"""

from threading import RLock

from cyberdelta.core.symbols.models import UnifiedSymbol
from cyberdelta.core.symbols.protocols import SymbolStoreProtocol


class SymbolStore(SymbolStoreProtocol):
    """Pure storage layer for symbols - implements SymbolStoreProtocol for type safety."""

    def __init__(self) -> None:
        """Initialize symbol store with thread-safe storage and lookup indices."""
        # Core symbol storage
        self._symbols: dict[str, UnifiedSymbol] = {}
        # Fast lookup indices for symbol operations
        # internal -> {exchange -> exchange_symbol}
        self._internal_to_exchange: dict[str, dict[str, str]] = {}
        # exchange -> {exchange_symbol -> internal}
        self._exchange_to_internal: dict[str, dict[str, str]] = {}
        # Thread-safe for concurrent operations
        self._lock = RLock()

    def store(self, symbol: UnifiedSymbol) -> None:
        """Store unified symbol with bidirectional lookup indices for fast retrieval."""
        with self._lock:
            internal_value = symbol.internal.value
            self._symbols[internal_value] = symbol

            # Build optimized lookup indices for performance
            self._internal_to_exchange[internal_value] = {}
            for exchange_name, exchange_symbol in symbol.exchange_mappings.items():
                self._internal_to_exchange[internal_value][exchange_name] = exchange_symbol.value

                if exchange_name not in self._exchange_to_internal:
                    self._exchange_to_internal[exchange_name] = {}
                self._exchange_to_internal[exchange_name][exchange_symbol.value] = internal_value

    def get_by_internal(self, internal_symbol: str) -> UnifiedSymbol | None:
        """Retrieve symbol by internal canonical representation."""
        with self._lock:
            return self._symbols.get(internal_symbol)

    def get_by_exchange(self, exchange_symbol: str, exchange_name: str) -> UnifiedSymbol | None:
        """Retrieve symbol by exchange-specific representation."""
        with self._lock:
            internal = self._exchange_to_internal.get(exchange_name, {}).get(exchange_symbol)
            return self._symbols.get(internal) if internal else None

    def get_all(self) -> list[UnifiedSymbol]:
        """Get all stored symbols for system operations."""
        with self._lock:
            return list(self._symbols.values())

    def clear(self) -> None:
        """Clear all stored symbol data."""
        with self._lock:
            self._symbols.clear()
            self._internal_to_exchange.clear()
            self._exchange_to_internal.clear()

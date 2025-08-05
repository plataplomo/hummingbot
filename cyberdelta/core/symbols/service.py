"""Symbol Service - Clean Architecture."""

from typing import Any

from cyberdelta.enums.exchange_names import ExchangeName

from .models import Symbol, SymbolComponents
from .protocols import ExchangeHandler


class SymbolService:
    """Central service for symbol operations with injected dependencies."""

    def __init__(
        self,
        handlers: dict[ExchangeName, ExchangeHandler[Any]],
        equivalence_map: dict[str, list[Symbol]] | None = None,
    ) -> None:
        """Initialize symbol service with handlers and optional equivalence map."""
        self.handlers = handlers
        self._equivalence_map: dict[str, list[Symbol]] = equivalence_map or {}
        self._canonical_cache: dict[tuple[str, ExchangeName], tuple[str, SymbolComponents]] = {}

    def create_symbol(
        self,
        value: str,
        exchange: ExchangeName,
        asset_index: int | None = None,
        symbol_id: int | None = None,
    ) -> Symbol:
        """Create symbol using appropriate handler.

        Returns:
            Symbol: Created symbol instance.

        Raises:
            ValueError: If no handler is registered for the exchange.
        """
        handler = self.handlers.get(exchange)
        if not handler:
            msg = f"No handler registered for exchange {exchange}"
            raise ValueError(msg)

        symbol = handler.create_symbol(value, asset_index=asset_index, symbol_id=symbol_id)

        # Pre-compute and cache components
        components = handler.parse_components(value)
        symbol.set_components(components)

        return symbol

    def parse_components(self, symbol: Symbol) -> SymbolComponents:
        """Parse symbol components using exchange handler.

        Returns:
            SymbolComponents: Parsed symbol components.

        Raises:
            ValueError: If no handler is registered for the exchange.
        """
        handler = self.handlers.get(symbol.exchange)
        if not handler:
            msg = f"No handler registered for exchange {symbol.exchange}"
            raise ValueError(msg)
        return handler.parse_components(symbol.value)

    def convert_symbol(
        self,
        symbol: Symbol,
        target_exchange: ExchangeName,
    ) -> Symbol:
        """Convert symbol to another exchange.

        Returns:
            Symbol: Converted symbol for the target exchange.

        Raises:
            ValueError: If no handler is registered for either exchange.
        """
        if symbol.exchange == target_exchange:
            return symbol

        # Get canonical representation
        canonical, components = self._get_canonical_with_components(symbol)

        # Convert to target format
        target_handler = self.handlers.get(target_exchange)
        if not target_handler:
            msg = f"No handler registered for exchange {target_exchange}"
            raise ValueError(msg)

        target_value = target_handler.from_canonical(canonical, components)
        return target_handler.create_symbol(target_value)

    def get_canonical(self, symbol: Symbol) -> str:
        """Get canonical representation.

        Returns:
            str: Canonical representation of the symbol.
        """
        canonical, _ = self._get_canonical_with_components(symbol)
        return canonical

    def _get_canonical_with_components(self, symbol: Symbol) -> tuple[str, SymbolComponents]:
        """Get canonical representation with components (cached).

        Returns:
            tuple[str, SymbolComponents]: Canonical format and components.

        Raises:
            ValueError: If no handler is registered for the exchange.
        """
        cache_key = (symbol.value, symbol.exchange)
        if cache_key in self._canonical_cache:
            return self._canonical_cache[cache_key]

        handler = self.handlers.get(symbol.exchange)
        if not handler:
            msg = f"No handler registered for exchange {symbol.exchange}"
            raise ValueError(msg)
        result = handler.to_canonical(symbol.value)

        self._canonical_cache[cache_key] = result
        return result

    def register_symbol(self, symbol: Symbol) -> None:
        """Register a symbol and update equivalence mappings."""
        canonical = self.get_canonical(symbol)
        if canonical not in self._equivalence_map:
            self._equivalence_map[canonical] = []

        # Check if already registered
        for existing in self._equivalence_map[canonical]:
            if existing.value == symbol.value and existing.exchange == symbol.exchange:
                return

        self._equivalence_map[canonical].append(symbol)

    def get_equivalent_symbols(self, symbol: Symbol) -> list[Symbol]:
        """Get all symbols equivalent to the given symbol.

        Returns:
            list[Symbol]: List of equivalent symbols.
        """
        canonical = self.get_canonical(symbol)
        return self._equivalence_map.get(canonical, [])

    def find_symbol(self, value: str, exchange: ExchangeName) -> Symbol | None:
        """Find a registered symbol by value and exchange.

        Returns:
            Symbol | None: Found symbol or None if not found.
        """
        for symbols in self._equivalence_map.values():
            for symbol in symbols:
                if symbol.value == value and symbol.exchange == exchange:
                    return symbol
        return None

    def are_equivalent(self, symbol1: Symbol, symbol2: Symbol) -> bool:
        """Check if two symbols represent the same instrument.

        Returns:
            bool: True if symbols are equivalent, False otherwise.
        """
        return self.get_canonical(symbol1) == self.get_canonical(symbol2)

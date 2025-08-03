"""Scalable symbol registry for N exchanges."""

from functools import lru_cache
from typing import Any, Protocol

from cyberdelta.enums.exchange_names import ExchangeName

from .models import Symbol
from .protocols import ExchangeHandler


class SymbolFactory(Protocol):
    """Protocol for symbol factories."""

    def __call__(
        self, value: str, asset_index: int | None = None, symbol_id: int | None = None
    ) -> Symbol:
        """Create a symbol with the given value and metadata."""
        ...


class SymbolRegistry:
    """Central registry for symbol creation - scalable to N exchanges."""

    def __init__(self) -> None:
        """Initialize the registry."""
        self._handlers: dict[ExchangeName, ExchangeHandler[Any]] = {}
        self._factories: dict[ExchangeName, SymbolFactory] = {}
        self._initialized = False

    def register_handler(self, exchange: ExchangeName, handler: ExchangeHandler[Any]) -> None:
        """Register an exchange handler."""
        self._handlers[exchange] = handler

        # Create and cache factory for this exchange
        self._factories[exchange] = self._create_factory(exchange, handler)

    def _create_factory(
        self, exchange: ExchangeName, handler: ExchangeHandler[Any]
    ) -> SymbolFactory:
        """Create a factory function for an exchange.
        
        Returns:
            SymbolFactory: Factory function for creating symbols.
        """

        def _factory(
            value: str, asset_index: int | None = None, symbol_id: int | None = None
        ) -> Symbol:
            # Create symbol using handler
            symbol = handler.create_symbol(value, asset_index=asset_index, symbol_id=symbol_id)

            # Pre-compute and cache components
            components = handler.parse_components(value)
            symbol.set_components(components)

            return symbol

        # Set name and doc before caching
        _factory.__name__ = f"{exchange.value}_symbol_factory"
        _factory.__doc__ = f"Create {exchange.value} symbol"

        # Apply cache after setting attributes
        return lru_cache(maxsize=1000)(_factory)

    def get_factory(self, exchange: ExchangeName) -> SymbolFactory:
        """Get factory for an exchange.
        
        Returns:
            SymbolFactory: Factory for the specified exchange.
            
        Raises:
            ValueError: If exchange is not registered.
        """
        if exchange not in self._factories:
            msg = f"No factory registered for {exchange}"
            raise ValueError(msg)
        return self._factories[exchange]

    def create_symbol(
        self,
        value: str,
        exchange: ExchangeName,
        asset_index: int | None = None,
        symbol_id: int | None = None,
    ) -> Symbol:
        """Create symbol for any registered exchange.
        
        Returns:
            Symbol: Created symbol instance.
        """
        factory = self.get_factory(exchange)
        return factory(value, asset_index=asset_index, symbol_id=symbol_id)

    def __getattr__(self, name: str) -> SymbolFactory:
        """Dynamic attribute access for exchange factories.

        Allows: registry.hyperliquid("BTC-PERP")
        
        Returns:
            SymbolFactory: Factory for the requested exchange.
            
        Raises:
            AttributeError: If exchange is not found.
        """
        # Convert attribute name to exchange enum
        try:
            exchange = ExchangeName(name.lower())
            return self.get_factory(exchange)
        except (ValueError, KeyError) as e:
            msg = f"No factory for exchange: {name}"
            raise AttributeError(msg) from e

    def initialize_with_handlers(self, handlers: dict[ExchangeName, ExchangeHandler[Any]]) -> None:
        """Initialize the registry with handlers."""
        if not self._initialized:
            for exchange, handler in handlers.items():
                self.register_handler(exchange, handler)
            self._initialized = True

    def get_handlers(self) -> dict[ExchangeName, ExchangeHandler[Any]]:
        """Get the registered handlers.
        
        Returns:
            dict[ExchangeName, ExchangeHandler[Any]]: Registered exchange handlers.
        """
        return self._handlers


# Global registry instance
_registry = SymbolRegistry()


def get_registry() -> SymbolRegistry:
    """Get the global symbol registry.
    
    Returns:
        SymbolRegistry: The global symbol registry instance.
    """
    return _registry

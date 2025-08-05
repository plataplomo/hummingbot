"""Clean public API for symbol creation."""

from functools import lru_cache

from cyberdelta.enums.exchange_names import ExchangeName

from .factory import create_symbol_service
from .models import Symbol
from .registry import SymbolFactory, get_registry
from .service import SymbolService


# Direct symbol creation
def symbol(
    value: str, exchange: ExchangeName, asset_index: int | None = None, symbol_id: int | None = None
) -> Symbol:
    """Create symbol for any exchange.

    Examples:
        >>> symbol("BTC-PERP", ExchangeName.HYPERLIQUID)
        >>> symbol("BTC_USD_PERP", ExchangeName.BACKPACK, symbol_id=12345)

    Returns:
        Symbol: Created symbol instance.
    """
    return get_registry().create_symbol(
        value, exchange, asset_index=asset_index, symbol_id=symbol_id
    )


# Exchange namespaces for cleaner imports
class Exchanges:
    """Namespace for exchange-specific symbol creation.

    Provides dynamic access to all registered exchanges:
        >>> exchanges.hyperliquid("BTC-PERP")
        >>> exchanges.backpack("BTC_USD_PERP", symbol_id=12345)
    """

    def __getattr__(self, name: str) -> SymbolFactory:
        """Get factory for the given exchange name.

        Returns:
            SymbolFactory: Factory for creating symbols on the specified exchange.
        """
        registry = get_registry()
        return registry.__getattr__(name)


# Singleton instance
exchanges = Exchanges()


# Get symbol service for advanced operations
@lru_cache(maxsize=1)
def get_symbol_service() -> SymbolService:
    """Get symbol service for advanced operations like equivalence.

    Returns:
        SymbolService: Service instance for symbol operations.
    """
    registry = get_registry()
    return create_symbol_service(handlers=registry.get_handlers())

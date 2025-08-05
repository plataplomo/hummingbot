"""Global Symbol Service Access Pattern."""

from functools import lru_cache

from cyberdelta.enums.exchange_names import ExchangeName

from .factory import create_symbol_service
from .models import Symbol
from .service import SymbolService


class _GlobalSymbolService:
    """Container for global symbol service instance."""

    _instance: SymbolService | None = None

    @classmethod
    def get(cls) -> SymbolService:
        """Get or create the global symbol service.

        Returns:
            SymbolService: The global symbol service instance.
        """
        if cls._instance is None:
            cls._instance = create_symbol_service()
        return cls._instance


def get_symbol_service() -> SymbolService:
    """Get or create the global symbol service.

    Returns:
        SymbolService: The global symbol service instance.
    """
    return _GlobalSymbolService.get()


# Convenience factory functions
@lru_cache(maxsize=1000)
def bp_symbol(value: str, symbol_id: int | None = None) -> Symbol:
    """Create a Backpack symbol (cached).

    Returns:
        Symbol: Backpack symbol instance.
    """
    return get_symbol_service().create_symbol(value, ExchangeName.BACKPACK, symbol_id=symbol_id)


@lru_cache(maxsize=1000)
def hl_symbol(value: str, asset_index: int | None = None) -> Symbol:
    """Create a Hyperliquid symbol (cached).

    Returns:
        Symbol: Hyperliquid symbol instance.
    """
    return get_symbol_service().create_symbol(
        value, ExchangeName.HYPERLIQUID, asset_index=asset_index
    )

"""Global Symbol Service Access Pattern."""

from functools import lru_cache

from cyberdelta.config import get_app_settings
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.base import ConfigurationNotInitializedError

from .factory import create_symbol_service
from .models import Symbol
from .service import SymbolService


class _GlobalSymbolService:
    """Container for global symbol service instance.

    This class implements a configuration-aware singleton pattern with lazy initialization.
    The service is only created when first accessed, not at import time, which prevents
    configuration initialization issues during testing.
    """

    _instance: SymbolService | None = None
    _initialized: bool = False

    @classmethod
    def get(cls) -> SymbolService:
        """Get or create the global symbol service.

        This method is safe to call at any time, including module level.
        Actual initialization is deferred until first real use.

        Returns:
            SymbolService: The global symbol service instance.

        Raises:
            RuntimeError: If symbol service initialization fails.
        """
        if not cls._initialized:
            cls._initialize()
        if cls._instance is None:
            msg = "Symbol service initialization failed"
            raise RuntimeError(msg)
        return cls._instance

    @classmethod
    def _initialize(cls) -> None:
        """Initialize the symbol service when actually needed.

        This method handles both production (with config) and test (without config)
        environments gracefully.
        """
        if cls._initialized:
            return

        cls._instance = cls._create_instance()
        cls._initialized = True

    @classmethod
    def _create_instance(cls) -> SymbolService:
        """Create the symbol service instance.

        Returns:
            SymbolService: A configured or minimal service instance.
        """
        try:
            # Create base service
            service = create_symbol_service()

            # Try to load configuration and register symbols
            app_settings = get_app_settings()

            # Register all configured symbols
            symbol_groups = getattr(app_settings, "symbol_groups", None)
            if symbol_groups is None:
                # Fallback to unified_symbols for compatibility
                symbol_groups = getattr(app_settings, "unified_symbols", [])

            for symbol_group in symbol_groups:
                for mapping in symbol_group.mappings:
                    try:
                        exchange = ExchangeName(mapping.exchange.lower())
                    except ValueError:
                        continue

                    # Extract metadata values
                    asset_index = mapping.metadata.asset_index
                    symbol_id = (
                        int(mapping.metadata.symbol_id)
                        if mapping.metadata.symbol_id is not None
                        else None
                    )

                    symbol = service.create_symbol(
                        mapping.value, exchange, asset_index=asset_index, symbol_id=symbol_id
                    )
                    service.register_symbol(symbol)

        except (AttributeError, ImportError, ConfigurationNotInitializedError):
            # Config not available during testing - return minimal service
            return create_symbol_service()
        else:
            # Configuration loaded successfully
            return service

    @classmethod
    def reset(cls) -> None:
        """Reset the global instance for testing.

        This method should only be used in test fixtures to ensure
        clean state between tests.
        """
        cls._instance = None
        cls._initialized = False


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

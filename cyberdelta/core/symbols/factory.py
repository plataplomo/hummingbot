"""Symbol Service Factory."""

from typing import Any

from cyberdelta.config import get_app_settings
from cyberdelta.enums.exchange_names import ExchangeName

from .handlers import DEFAULT_HANDLERS
from .protocols import ExchangeHandler
from .service import SymbolService


def create_symbol_service(
    handlers: dict[ExchangeName, ExchangeHandler[Any]] | None = None,
) -> SymbolService:
    """Create configured symbol service with all dependencies.
    
    Returns:
        SymbolService: Configured symbol service instance.
    """
    # Use provided handlers or defaults
    if handlers is None:
        handlers = DEFAULT_HANDLERS

    # Create service
    service = SymbolService(handlers)

    # Load symbols from config if available
    try:
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
    except (AttributeError, ImportError):
        # Config not available during testing
        pass

    return service

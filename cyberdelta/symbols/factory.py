"""Symbol Service Factory."""

from typing import Any

from cyberdelta.enums.exchange_names import ExchangeName

from .handlers import DEFAULT_HANDLERS
from .protocols import ExchangeHandler
from .service import SymbolService


def create_symbol_service(
    handlers: dict[ExchangeName, ExchangeHandler[Any]] | None = None,
) -> SymbolService:
    """Create symbol service with handlers.

    This is a pure factory function that only creates the service with handlers.
    Configuration loading is handled in global_service.py to avoid circular dependencies.

    Args:
        handlers: Exchange handlers to use, defaults to DEFAULT_HANDLERS

    Returns:
        SymbolService: Symbol service instance with handlers.
    """
    # Use provided handlers or defaults
    if handlers is None:
        handlers = DEFAULT_HANDLERS

    # Create and return service (no configuration loading here)
    return SymbolService(handlers)

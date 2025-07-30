"""Symbol System - Clean Architecture."""

# New registry-based API
from .api import exchanges, symbol
from .common import symbols
from .config_loader import load_symbols_from_config
from .factory import create_symbol_service
from .global_service import bp_symbol, get_symbol_service, hl_symbol

# Initialize the registry with default handlers
from .handlers import DEFAULT_HANDLERS
from .models import (
    BackpackMetadata,
    HyperliquidMetadata,
    Symbol,
    SymbolComponents,
    SymbolMetadata,
)
from .protocols import ExchangeHandler
from .registry import get_registry
from .service import SymbolService


_registry = get_registry()
_registry.initialize_with_handlers(DEFAULT_HANDLERS)


__all__ = [
    "BackpackMetadata",
    "ExchangeHandler",
    "HyperliquidMetadata",
    "Symbol",
    "SymbolComponents",
    "SymbolMetadata",
    "SymbolService",
    "bp_symbol",
    "create_symbol_service",
    "exchanges",
    "get_registry",
    "get_symbol_service",
    "hl_symbol",
    "load_symbols_from_config",
    "symbol",
    "symbols",
]

"""Unified Symbol Service - Compatibility wrapper for the new symbol system.

This module provides a compatibility layer to bridge the old symbol service
interface with the new symbol system.
"""

from __future__ import annotations

from typing import Any

from cyberdelta.core.symbols import Symbol, SymbolService, get_symbol_service
from cyberdelta.enums.exchange_names import ExchangeName


class SymbolNotFoundError(Exception):
    """Symbol not found in registry."""

    pass


# For backward compatibility, create aliases
InternalSymbol = Symbol
ExchangeSymbol = Symbol
UnifiedSymbol = Symbol


class UnifiedSymbolService:
    """Compatibility wrapper for the new symbol system.

    This class provides a bridge between the old UnifiedSymbolService interface
    and the new symbol system. It wraps the global symbol service to maintain
    backward compatibility with existing tests and code.
    """

    def __init__(self) -> None:
        """Initialize the unified symbol service with the global symbol service."""
        self._service = get_symbol_service()

    @property
    def service(self) -> SymbolService:
        """Get the underlying symbol service for compatibility."""
        return self._service

    def get_symbol(self, value: str, exchange: ExchangeName) -> Symbol:
        """Get or create a symbol.

        Args:
            value: Symbol value (e.g., "BTC-PERP")
            exchange: Exchange name

        Returns:
            Symbol object
        """
        return self._service.create_symbol(value, exchange)

    def create_internal_symbol(
        self, value: str, base_asset: str, quote_asset: str | None = None, market_type: str = "PERP"
    ) -> Symbol:
        """Create an internal symbol (backward compatibility).

        In the new system, all symbols are unified, so this just creates
        a regular symbol for Hyperliquid (default internal exchange).
        """
        # For internal symbols, default to Hyperliquid
        return self._service.create_symbol(value, ExchangeName.HYPERLIQUID)

    def get_exchange_symbol(self, internal_symbol: Symbol, exchange: ExchangeName) -> Symbol:
        """Get exchange-specific symbol (backward compatibility).

        In the new system, we create a new symbol for the target exchange.
        """
        # Extract the base value and create for target exchange
        return self._service.create_symbol(internal_symbol.value, exchange)

    def get_unified_symbol(
        self, base_asset: str, quote_asset: str | None = None, market_type: str = "PERP"
    ) -> Symbol:
        """Get unified symbol (backward compatibility).

        In the new system, all symbols are unified, so we just create
        a symbol with the appropriate format.
        """
        if market_type == "PERP":
            value = f"{base_asset}-PERP"
        else:
            value = f"{base_asset}-{quote_asset or 'USDC'}"

        # Default to Hyperliquid for unified symbols
        return self._service.create_symbol(value, ExchangeName.HYPERLIQUID)

    def normalize_symbol(self, symbol: str, exchange: ExchangeName) -> str:
        """Normalize symbol string for exchange (backward compatibility).

        Args:
            symbol: Symbol string
            exchange: Target exchange

        Returns:
            Normalized symbol string
        """
        # Create symbol and get its value (which is already normalized)
        symbol_obj = self._service.create_symbol(symbol, exchange)
        return symbol_obj.value

    def denormalize_symbol(self, symbol: str, exchange: ExchangeName) -> str:
        """Denormalize symbol string from exchange format (backward compatibility).

        In the new system, symbols are already in their canonical form.
        """
        return symbol

    def validate_symbol(self, symbol: str, exchange: ExchangeName) -> bool:
        """Validate if symbol is valid for exchange.

        Args:
            symbol: Symbol string
            exchange: Exchange name

        Returns:
            True if valid, False otherwise
        """
        try:
            self._service.create_symbol(symbol, exchange)
            return True
        except Exception:
            return False

"""Unified Symbol Service - Direct interface to the new symbol system.

This module provides the primary interface to the new unified symbol system,
completely bypassing all legacy compatibility layers for optimal performance.
"""

from __future__ import annotations

from typing import Any

from cyberdelta.core.symbols.exceptions import SymbolNotFoundError
from cyberdelta.core.symbols.models import ExchangeSymbol, InternalSymbol, UnifiedSymbol
from cyberdelta.core.symbols.service import SymbolService
from cyberdelta.enums.exchange_names import ExchangeName


class UnifiedSymbolService:
    """Direct interface to the new symbol system - NO COMPATIBILITY LAYER.

    This service provides clean, type-safe access to the unified symbol system
    without any legacy compatibility overhead. All methods use typed enums
    and return typed symbol objects rather than strings.

    Features:
    - Zero backwards compatibility - clean break from old system
    - Direct registry access for optimal performance
    - Type-safe symbol operations
    - Thread-safe through underlying registry
    """

    def __init__(self) -> None:
        """Initialize the unified symbol service."""
        self.service = SymbolService()

    def get_exchange_symbol_value(self, internal_symbol: str, exchange_id: ExchangeName) -> str:
        """Get exchange symbol string value from internal symbol.

        Args:
            internal_symbol: Internal symbol name (e.g., "BTC")
            exchange_id: Exchange identifier enum

        Returns:
            Exchange-specific symbol string (e.g., "BTC_PERP")
        """
        exchange_symbol = self.service.get_exchange_symbol(internal_symbol, exchange_id.value)
        return exchange_symbol.value

    def get_internal_symbol_value(self, exchange_symbol: str, exchange_id: ExchangeName) -> str:
        """Get internal symbol string value from exchange symbol.

        Args:
            exchange_symbol: Exchange-specific symbol (e.g., "BTC_PERP")
            exchange_id: Exchange identifier enum

        Returns:
            Internal symbol string (e.g., "BTC")
        """
        internal_symbol = self.service.get_internal_symbol(exchange_symbol, exchange_id.value)
        return internal_symbol.value

    def get_exchange_symbol(
        self, internal_symbol: str, exchange_id: ExchangeName
    ) -> ExchangeSymbol:
        """Get exchange symbol object from internal symbol.

        Args:
            internal_symbol: Internal symbol name
            exchange_id: Exchange identifier enum

        Returns:
            ExchangeSymbol object with full metadata
        """
        return self.service.get_exchange_symbol(internal_symbol, exchange_id.value)

    def get_internal_symbol(
        self, exchange_symbol: str, exchange_id: ExchangeName
    ) -> InternalSymbol:
        """Get internal symbol object from exchange symbol.

        Args:
            exchange_symbol: Exchange-specific symbol
            exchange_id: Exchange identifier enum

        Returns:
            InternalSymbol object with full metadata
        """
        return self.service.get_internal_symbol(exchange_symbol, exchange_id.value)

    def get_unified_symbol(self, internal_symbol: str) -> UnifiedSymbol:
        """Get complete unified symbol with all exchange mappings.

        Args:
            internal_symbol: Internal symbol name

        Returns:
            UnifiedSymbol with all exchange mappings

        Raises:
            SymbolNotFoundError: If symbol not found
        """
        unified_symbol = self.service.store.get_by_internal(internal_symbol)
        if not unified_symbol:
            raise SymbolNotFoundError(
                symbol=internal_symbol,
                context="unified_symbol_lookup",
                details={"internal_symbol": internal_symbol},
            )
        return unified_symbol

    def get_trading_specifications(self, internal_symbol: str) -> dict[str, Any]:
        """Get trading specifications for a symbol.

        Args:
            internal_symbol: Internal symbol name

        Returns:
            Dictionary with trading specifications:
            - tick_size: Minimum price increment
            - min_order_size: Minimum order size
            - max_order_size: Maximum order size
            - lot_size: Order size increment
            - supported_exchanges: List of exchanges supporting this symbol
        """
        unified = self.get_unified_symbol(internal_symbol)
        return {
            "tick_size": unified.tick_size,
            "min_order_size": unified.min_order_size,
            "max_order_size": unified.max_order_size,
            "lot_size": unified.lot_size,
            "is_tradeable": unified.is_tradeable,
            "is_active": unified.is_active,
            "supported_exchanges": list(unified.exchange_mappings.keys()),
        }

    def is_symbol_supported(self, internal_symbol: str, exchange_id: ExchangeName) -> bool:
        """Check if symbol is supported on exchange.

        Args:
            internal_symbol: Internal symbol name
            exchange_id: Exchange identifier enum

        Returns:
            True if symbol is supported on exchange, False otherwise
        """
        try:
            self.service.get_exchange_symbol(internal_symbol, exchange_id.value)
        except SymbolNotFoundError:
            return False
        else:
            return True

    def get_all_internal_symbols(self) -> list[str]:
        """Get all internal symbol values.

        Returns:
            List of all internal symbol strings
        """
        symbols = self.service.get_all_symbols()
        return [symbol.internal.value for symbol in symbols]

    def get_symbols_for_exchange(self, exchange_id: ExchangeName) -> list[str]:
        """Get all internal symbols supported on exchange.

        Args:
            exchange_id: Exchange identifier enum

        Returns:
            List of internal symbol strings supported on exchange
        """
        # Filter symbols that support the requested exchange
        all_symbols = self.service.get_all_symbols()
        symbols = [s for s in all_symbols if exchange_id.value in s.exchange_mappings]
        return [symbol.internal.value for symbol in symbols]

    def get_base_asset(self, internal_symbol: str) -> str:
        """Get base asset for a symbol.

        Args:
            internal_symbol: Internal symbol name

        Returns:
            Base asset name (e.g., "BTC" for "BTC_PERP")
        """
        unified = self.get_unified_symbol(internal_symbol)
        return unified.internal.base_asset

    def get_quote_asset(self, internal_symbol: str) -> str | None:
        """Get quote asset for a symbol.

        Args:
            internal_symbol: Internal symbol name

        Returns:
            Quote asset name if available (e.g., "USDC" for "BTC_USDC"), None for perpetuals
        """
        unified = self.get_unified_symbol(internal_symbol)
        return unified.internal.quote_asset

    def get_asset_index(self, internal_symbol: str, exchange_id: ExchangeName) -> int | None:
        """Get asset index for Hyperliquid spot symbols.

        Args:
            internal_symbol: Internal symbol name
            exchange_id: Exchange identifier (should be HYPERLIQUID)

        Returns:
            Asset index if available, None otherwise
        """
        exchange_symbol = self.service.get_exchange_symbol(internal_symbol, exchange_id.value)
        return exchange_symbol.asset_index

    def get_symbol_by_index(self, asset_index: int, exchange_id: ExchangeName) -> str:
        """Get symbol by asset index (Hyperliquid spot symbols).

        Args:
            asset_index: Asset index number
            exchange_id: Exchange identifier (should be HYPERLIQUID)

        Returns:
            Internal symbol name

        Raises:
            SymbolNotFoundError: If no symbol found for index
        """
        # Search all symbols for one with matching asset_index
        all_symbols = self.service.get_all_symbols()
        for unified in all_symbols:
            exchange_symbol = unified.exchange_mappings.get(exchange_id.value)
            if exchange_symbol and exchange_symbol.asset_index == asset_index:
                return unified.internal.value

        raise SymbolNotFoundError(
            symbol=str(asset_index),
            context=f"asset_index_lookup_{exchange_id.value}",
            details={"asset_index": asset_index, "exchange": exchange_id.value},
        )

    def validate_symbol_pair(
        self, internal_symbol: str, long_exchange: ExchangeName, short_exchange: ExchangeName
    ) -> None:
        """Validate symbol is available on both exchanges for arbitrage.

        Args:
            internal_symbol: Internal symbol name
            long_exchange: Exchange for long position
            short_exchange: Exchange for short position

        Raises:
            SymbolNotFoundError: If symbol not available on either exchange
        """
        # Check long exchange
        if not self.is_symbol_supported(internal_symbol, long_exchange):
            raise SymbolNotFoundError(
                symbol=internal_symbol,
                context=f"long_exchange_{long_exchange.value}",
                details={"exchange": long_exchange.value, "position_side": "long"},
            )

        # Check short exchange
        if not self.is_symbol_supported(internal_symbol, short_exchange):
            raise SymbolNotFoundError(
                symbol=internal_symbol,
                context=f"short_exchange_{short_exchange.value}",
                details={"exchange": short_exchange.value, "position_side": "short"},
            )

    def get_registry_stats(self) -> dict[str, Any]:
        """Get symbol registry statistics.

        Returns:
            Dictionary with registry statistics
        """
        # Return basic stats from the new service
        all_symbols = self.service.get_all_symbols()
        supported_exchanges: set[str] = set()
        for symbol in all_symbols:
            supported_exchanges.update(symbol.exchange_mappings.keys())

        return {
            "total_symbols": len(all_symbols),
            "supported_exchanges": sorted(supported_exchanges),
            "exchange_count": len(supported_exchanges),
        }


# Module-level service instance using a class to avoid global statement
class _SymbolServiceSingleton:
    """Singleton holder for symbol service."""

    _instance: UnifiedSymbolService | None = None

    @classmethod
    def get_instance(cls) -> UnifiedSymbolService:
        """Get or create the singleton instance.
        
        Returns:
            The singleton UnifiedSymbolService instance
        """
        if cls._instance is None:
            cls._instance = UnifiedSymbolService()
        return cls._instance

    @classmethod
    def set_instance(cls, service: UnifiedSymbolService) -> None:
        """Set the singleton instance."""
        cls._instance = service


def get_symbol_service() -> UnifiedSymbolService:
    """Get global symbol service instance.

    Returns:
        Global UnifiedSymbolService instance
    """
    return _SymbolServiceSingleton.get_instance()


def initialize_symbol_service() -> UnifiedSymbolService:
    """Initialize and return the global symbol service.

    This should be called during application startup after
    symbols have been loaded into the registry.

    Returns:
        Initialized UnifiedSymbolService instance
    """
    service = UnifiedSymbolService()
    _SymbolServiceSingleton.set_instance(service)
    return service

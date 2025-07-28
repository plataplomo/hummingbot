"""Symbol transformation plugins for exchange-specific formats.

This module provides exchange-specific symbol transformers implementing
the SymbolTransformerProtocol. Each exchange has its own transformer
handling the bidirectional conversion between internal and exchange formats.
"""

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols.models import InternalSymbol, create_internal_symbol
from cyberdelta.core.symbols.protocols import SymbolTransformerProtocol


# Error message constants to satisfy TRY003
class TransformerErrorMessages:
    """Error message constants for symbol transformers."""

    INVALID_HYPERLIQUID_SYMBOL = "Invalid Hyperliquid symbol format"
    INVALID_BACKPACK_SYMBOL = "Invalid Backpack symbol format"


class HyperliquidSymbolTransformer(SymbolTransformerProtocol):
    """Hyperliquid exchange symbol transformations."""

    def internal_to_exchange(self, internal: InternalSymbol) -> str:
        """Transform internal symbol to Hyperliquid format.
        
        Returns:
            Hyperliquid-formatted symbol string (e.g., BTC-PERP or BTC/USDC)
        """
        if internal.market_type == MarketType.PERP:
            return f"{internal.base_asset}-PERP"
        # SPOT
        return f"{internal.base_asset}/{internal.quote_asset}"

    def exchange_to_internal(self, exchange_symbol: str) -> InternalSymbol:
        """Transform Hyperliquid symbol to internal format.
        
        Handles Hyperliquid's symbol formats:
        - Perpetuals: "BTC-PERP" → InternalSymbol(BTC_USD, PERP)
        - Spot: "BTC/USDC" → InternalSymbol(BTC_USDC, SPOT)
        
        Args:
            exchange_symbol: Hyperliquid exchange symbol string
            
        Returns:
            InternalSymbol with extracted base/quote assets and market type
            
        Raises:
            ValueError: If symbol format is not recognized as valid Hyperliquid format
        """
        if "-PERP" in exchange_symbol:
            base = exchange_symbol.replace("-PERP", "")
            return create_internal_symbol(
                value=f"{base}_USD", base_asset=base, quote_asset="USD", market_type=MarketType.PERP
            )
        if "/" in exchange_symbol:
            base, quote = exchange_symbol.split("/", 1)
            return create_internal_symbol(
                value=f"{base}_{quote}",
                base_asset=base,
                quote_asset=quote,
                market_type=MarketType.SPOT,
            )
        raise ValueError(TransformerErrorMessages.INVALID_HYPERLIQUID_SYMBOL)


class BackpackSymbolTransformer(SymbolTransformerProtocol):
    """Backpack exchange symbol transformations."""

    def internal_to_exchange(self, internal: InternalSymbol) -> str:
        """Transform internal symbol to Backpack format.
        
        Returns:
            Backpack-formatted symbol string (e.g., BTC_PERP or BTC_USDC)
        """
        if internal.market_type == MarketType.PERP:
            return f"{internal.base_asset}_PERP"
        # SPOT
        return f"{internal.base_asset}_{internal.quote_asset}"

    def exchange_to_internal(self, exchange_symbol: str) -> InternalSymbol:
        """Transform Backpack symbol to internal format.
        
        Handles Backpack's symbol formats:
        - Perpetuals: "BTC_PERP" → InternalSymbol(BTC_USD, PERP)
        - Spot: "BTC_USDC" → InternalSymbol(BTC_USDC, SPOT)
        
        Args:
            exchange_symbol: Backpack exchange symbol string
            
        Returns:
            InternalSymbol with extracted base/quote assets and market type
            
        Raises:
            ValueError: If symbol format is not recognized as valid Backpack format
        """
        if "_PERP" in exchange_symbol:
            base = exchange_symbol.replace("_PERP", "")
            return create_internal_symbol(
                value=f"{base}_USD", base_asset=base, quote_asset="USD", market_type=MarketType.PERP
            )
        if "_" in exchange_symbol:
            base, quote = exchange_symbol.split("_", 1)
            return create_internal_symbol(
                value=f"{base}_{quote}",
                base_asset=base,
                quote_asset=quote,
                market_type=MarketType.SPOT,
            )
        raise ValueError(TransformerErrorMessages.INVALID_BACKPACK_SYMBOL)


class BinanceSymbolTransformer(SymbolTransformerProtocol):
    """Binance exchange - implement when adding Binance support."""

    def internal_to_exchange(self, internal: InternalSymbol) -> str:
        """Transform internal symbol to Binance format.
        
        Returns:
            Binance-formatted symbol string (e.g., BTCUSDT)
        """
        if internal.market_type == MarketType.PERP:
            return f"{internal.base_asset}USDT"  # Binance futures format
        # SPOT
        return f"{internal.base_asset}{internal.quote_asset}"

    def exchange_to_internal(self, exchange_symbol: str) -> InternalSymbol:
        """Transform Binance symbol to internal format.
        
        Returns:
            InternalSymbol (not implemented yet)
        """
        # Binance-specific parsing logic will be implemented when adding Binance
        raise NotImplementedError("Binance symbol transformer not implemented yet")


# Type-safe symbol transformer registry using SymbolTransformerProtocol
SYMBOL_TRANSFORMERS: dict[str, SymbolTransformerProtocol] = {
    "hyperliquid": HyperliquidSymbolTransformer(),
    "backpack": BackpackSymbolTransformer(),
}

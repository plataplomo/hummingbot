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
        """Transform internal symbol to Hyperliquid format."""
        if internal.market_type == MarketType.PERP:
            return f"{internal.base_asset}-PERP"
        # SPOT
        return f"{internal.base_asset}/{internal.quote_asset}"

    def exchange_to_internal(self, exchange_symbol: str) -> InternalSymbol:
        """Transform Hyperliquid symbol to internal format."""
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
        """Transform internal symbol to Backpack format."""
        if internal.market_type == MarketType.PERP:
            return f"{internal.base_asset}_PERP"
        # SPOT
        return f"{internal.base_asset}_{internal.quote_asset}"

    def exchange_to_internal(self, exchange_symbol: str) -> InternalSymbol:
        """Transform Backpack symbol to internal format."""
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
        """Transform internal symbol to Binance format."""
        if internal.market_type == MarketType.PERP:
            return f"{internal.base_asset}USDT"  # Binance futures format
        # SPOT
        return f"{internal.base_asset}{internal.quote_asset}"

    def exchange_to_internal(self, exchange_symbol: str) -> InternalSymbol:
        """Transform Binance symbol to internal format."""
        # Binance-specific parsing logic will be implemented when adding Binance
        raise NotImplementedError("Binance symbol transformer not implemented yet")


# Type-safe symbol transformer registry using SymbolTransformerProtocol
SYMBOL_TRANSFORMERS: dict[str, SymbolTransformerProtocol] = {
    "hyperliquid": HyperliquidSymbolTransformer(),
    "backpack": BackpackSymbolTransformer(),
}

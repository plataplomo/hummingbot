"""Symbol-aware mixin for Backpack services.

This mixin provides Symbol-aware methods that preserve type safety
while working with the existing string-based infrastructure.
"""

from typing import Any

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols.models import BackpackMetadata, Symbol


class SymbolAwareMixin:
    """Mixin providing Symbol-aware helper methods for Backpack."""

    def get_symbol_id_for_symbol(self, symbol: Symbol) -> int | None:
        """Get symbol ID for a Symbol object.

        This preserves the Symbol type and can leverage metadata if available.

        Args:
            symbol: Symbol object with potential metadata

        Returns:
            Symbol ID or None if not found
        """
        # Type-safe metadata access for Backpack symbols
        if isinstance(symbol.metadata, BackpackMetadata) and symbol.metadata.symbol_id is not None:
            return symbol.metadata.symbol_id

        # Backpack doesn't have a lookup service, return None
        return None

    def validate_symbol_for_order_type(
        self,
        symbol: Symbol,
        order_type: str,
    ) -> None:
        """Validate symbol is compatible with order type.

        Args:
            symbol: Symbol to validate
            order_type: Order type string

        Note:
            May raise ValueError if symbol is incompatible with order type
        """

        def _raise_invalid_order_type() -> None:
            msg = f"Order type {order_type} not valid for spot market symbol {symbol.value}"
            raise ValueError(msg)

        # Type-safe component access
        try:
            # Components are computed on demand via property
            market_type = symbol.market_type
            if market_type == MarketType.SPOT and "FUTURES" in order_type.upper():
                _raise_invalid_order_type()
        except (AttributeError, ValueError):
            # Components not available or not parsed, skip validation
            pass

    def get_symbol_metadata(self, symbol: Symbol) -> dict[str, Any]:
        """Extract all useful metadata from symbol.

        Args:
            symbol: Symbol object

        Returns:
            Dictionary containing symbol metadata
        """
        metadata: dict[str, Any] = {
            "value": symbol.value,
            "exchange": symbol.exchange.value,
        }

        # Type-safe component access
        try:
            metadata["base_asset"] = symbol.base_asset
            metadata["quote_asset"] = symbol.quote_asset
            metadata["market_type"] = symbol.market_type.value
        except (AttributeError, ValueError):
            # Components not available, skip
            pass

        # Type-safe metadata access for Backpack
        if isinstance(symbol.metadata, BackpackMetadata) and symbol.metadata.symbol_id is not None:
            metadata["symbol_id"] = symbol.metadata.symbol_id

        return metadata

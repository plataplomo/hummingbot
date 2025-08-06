"""Symbol-aware mixin for Hyperliquid services.

This mixin provides Symbol-aware methods that preserve type safety
while working with the existing string-based infrastructure.
"""

from typing import TYPE_CHECKING, Any

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.symbols.models import HyperliquidMetadata, Symbol


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class SymbolAwareMixin:
    """Mixin providing Symbol-aware helper methods."""

    async def get_asset_index_for_symbol(
        self,
        symbol: Symbol,
        get_asset_index_callable: "Callable[[str], Awaitable[int | None]]",
    ) -> int | None:
        """Get asset index for a Symbol object.

        This preserves the Symbol type and can leverage metadata if available.

        Args:
            symbol: Symbol object with potential metadata
            get_asset_index_callable: Callable to fetch asset index

        Returns:
            Asset index or None if not found
        """
        # Type-safe metadata access for Hyperliquid symbols
        if (
            isinstance(symbol.metadata, HyperliquidMetadata)
            and symbol.metadata.asset_index is not None
        ):
            return symbol.metadata.asset_index

        # Fallback to lookup using symbol value
        return await get_asset_index_callable(symbol.value)

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
            if market_type == MarketType.SPOT and "PERP" in order_type.upper():
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

        # Type-safe metadata access for Hyperliquid
        if (
            isinstance(symbol.metadata, HyperliquidMetadata)
            and symbol.metadata.asset_index is not None
        ):
            metadata["asset_index"] = symbol.metadata.asset_index

        return metadata

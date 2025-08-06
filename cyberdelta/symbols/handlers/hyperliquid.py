"""Hyperliquid Exchange Handler - Clean Architecture."""

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.symbols.models import BaseSymbol, HyperliquidMetadata, Symbol, SymbolComponents


class HyperliquidHandler:
    """Hyperliquid-specific symbol handling."""

    # Exchange-specific constants
    DEFAULT_PERP_QUOTE = "USD"
    DEFAULT_MARKET_TYPE = MarketType.PERP
    PERP_SUFFIX = "-PERP"
    SYMBOL_SEPARATOR = "-"
    INDEX_PREFIX = "@"

    @property
    def exchange(self) -> ExchangeName:
        """Get the exchange this handler is for."""
        return ExchangeName.HYPERLIQUID

    def parse_components(self, value: str) -> SymbolComponents:
        """Parse Hyperliquid symbol format.

        Returns:
            SymbolComponents: Parsed symbol components.
        """
        # Handle @N format
        if value.startswith(self.INDEX_PREFIX):
            return SymbolComponents(base_asset=value, market_type=self.DEFAULT_MARKET_TYPE)

        # Handle PERP format
        if value.endswith(self.PERP_SUFFIX):
            base = value[: -len(self.PERP_SUFFIX)]
            return SymbolComponents(
                base_asset=base, quote_asset=self.DEFAULT_PERP_QUOTE, market_type=MarketType.PERP
            )

        # Handle spot pairs
        if self.SYMBOL_SEPARATOR in value:
            parts = value.split(self.SYMBOL_SEPARATOR, 1)
            return SymbolComponents(
                base_asset=parts[0], quote_asset=parts[1], market_type=MarketType.SPOT
            )

        return SymbolComponents(base_asset=value, market_type=MarketType.SPOT)

    def format_symbol(self, components: SymbolComponents) -> str:
        """Format components into Hyperliquid symbol.

        Returns:
            str: Formatted Hyperliquid symbol.
        """
        if components.market_type == MarketType.PERP:
            return f"{components.base_asset}{self.PERP_SUFFIX}"
        if components.quote_asset:
            return f"{components.base_asset}{self.SYMBOL_SEPARATOR}{components.quote_asset}"
        return components.base_asset

    def to_canonical(self, value: str) -> tuple[str, SymbolComponents]:
        """Convert to canonical format.

        Returns:
            tuple[str, SymbolComponents]: Canonical format and parsed components.
        """
        components = self.parse_components(value)
        if components.quote_asset:
            canonical = f"{components.base_asset}_{components.quote_asset}"
        else:
            canonical = components.base_asset
        return canonical, components

    def from_canonical(self, canonical: str, components: SymbolComponents) -> str:
        """Convert from canonical to Hyperliquid format.

        Returns:
            str: Hyperliquid formatted symbol.
        """
        return self.format_symbol(components)

    def create_metadata(
        self, asset_index: int | None = None, symbol_id: int | None = None
    ) -> HyperliquidMetadata:
        """Create Hyperliquid metadata.

        Returns:
            HyperliquidMetadata: Metadata instance for Hyperliquid.
        """
        return HyperliquidMetadata(asset_index=asset_index)

    def create_symbol(
        self, value: str, asset_index: int | None = None, symbol_id: int | None = None
    ) -> Symbol:
        """Create Hyperliquid symbol.

        Returns:
            Symbol: Created Hyperliquid symbol.
        """
        metadata = self.create_metadata(asset_index=asset_index)
        symbol = BaseSymbol[HyperliquidMetadata](
            value=value, exchange=self.exchange, metadata=metadata
        )
        # Pre-compute components
        components = self.parse_components(value)
        symbol.set_components(components)
        return symbol

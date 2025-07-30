"""Backpack Exchange Handler - Clean Architecture."""

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols.models import BackpackMetadata, BaseSymbol, Symbol, SymbolComponents
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.symbol_mapping import SymbolMappingErrorMessages, SymbolMappingFieldError


class BackpackHandler:
    """Backpack-specific symbol handling."""

    # Exchange-specific constants
    DEFAULT_PERP_QUOTE = "USDC"
    DEFAULT_MARKET_TYPE = MarketType.PERP
    PERP_SUFFIX = "_PERP"
    SYMBOL_SEPARATOR = "_"

    @property
    def exchange(self) -> ExchangeName:
        """Get the exchange this handler is for."""
        return ExchangeName.BACKPACK

    def parse_components(self, value: str) -> SymbolComponents:
        """Parse Backpack symbol format."""
        # Handle PERP format
        if value.endswith(self.PERP_SUFFIX):
            base_part = value[: -len(self.PERP_SUFFIX)]
            if self.SYMBOL_SEPARATOR in base_part:
                parts = base_part.split(self.SYMBOL_SEPARATOR, 1)
                return SymbolComponents(
                    base_asset=parts[0], quote_asset=parts[1], market_type=MarketType.PERP
                )
            return SymbolComponents(
                base_asset=base_part,
                quote_asset=self.DEFAULT_PERP_QUOTE,
                market_type=MarketType.PERP,
            )

        # Handle spot pairs
        if self.SYMBOL_SEPARATOR in value:
            parts = value.split(self.SYMBOL_SEPARATOR, 1)
            return SymbolComponents(
                base_asset=parts[0], quote_asset=parts[1], market_type=MarketType.SPOT
            )

        return SymbolComponents(base_asset=value, market_type=MarketType.SPOT)

    def format_symbol(self, components: SymbolComponents) -> str:
        """Format components into Backpack symbol."""
        if components.market_type == MarketType.PERP:
            if components.quote_asset and components.quote_asset != self.DEFAULT_PERP_QUOTE:
                base = components.base_asset
                quote = components.quote_asset
                return f"{base}{self.SYMBOL_SEPARATOR}{quote}{self.PERP_SUFFIX}"
            return f"{components.base_asset}{self.PERP_SUFFIX}"
        if components.quote_asset:
            return f"{components.base_asset}{self.SYMBOL_SEPARATOR}{components.quote_asset}"
        return components.base_asset

    def to_canonical(self, value: str) -> tuple[str, SymbolComponents]:
        """Convert to canonical format."""
        components = self.parse_components(value)
        if components.quote_asset:
            canonical = f"{components.base_asset}_{components.quote_asset}"
        else:
            canonical = components.base_asset
        return canonical, components

    def from_canonical(self, canonical: str, components: SymbolComponents) -> str:
        """Convert from canonical to Backpack format."""
        return self.format_symbol(components)

    def create_metadata(
        self, asset_index: int | None = None, symbol_id: int | None = None
    ) -> BackpackMetadata:
        """Create Backpack metadata."""
        return BackpackMetadata(symbol_id=symbol_id)

    def create_symbol(
        self, value: str, asset_index: int | None = None, symbol_id: int | None = None
    ) -> Symbol:
        """Create Backpack symbol."""
        metadata = self.create_metadata(asset_index=asset_index, symbol_id=symbol_id)
        symbol = BaseSymbol[BackpackMetadata](
            value=value, exchange=self.exchange, metadata=metadata
        )
        # Pre-compute components
        components = self.parse_components(value)
        symbol.set_components(components)
        return symbol

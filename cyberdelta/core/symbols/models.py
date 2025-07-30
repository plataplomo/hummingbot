"""Symbol Models - Clean Architecture with Type Safety."""

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.enums.exchange_names import ExchangeName


# Base metadata model
class SymbolMetadata(BaseModel):
    """Base class for exchange metadata."""

    model_config = ConfigDict(frozen=True)

    @property
    def exchange_type(self) -> ExchangeName:
        """Get the exchange type this metadata is for."""
        raise NotImplementedError


class HyperliquidMetadata(SymbolMetadata):
    """Hyperliquid-specific metadata."""

    asset_index: int | None = None

    @property
    def exchange_type(self) -> ExchangeName:
        """Get the exchange type this metadata is for."""
        return ExchangeName.HYPERLIQUID


class BackpackMetadata(SymbolMetadata):
    """Backpack-specific metadata."""

    symbol_id: int | None = None

    @property
    def exchange_type(self) -> ExchangeName:
        """Get the exchange type this metadata is for."""
        return ExchangeName.BACKPACK


class SymbolComponents(BaseModel):
    """Parsed symbol components - pure data."""

    model_config = ConfigDict(frozen=True)

    base_asset: str
    quote_asset: str | None = None
    market_type: MarketType


class BaseSymbol[TMetadata: SymbolMetadata](BaseModel):
    """Internal generic symbol implementation with full type safety."""

    model_config = ConfigDict(frozen=True)

    value: str = Field(..., min_length=1, max_length=30)
    exchange: ExchangeName
    metadata: TMetadata

    _components: SymbolComponents | None = PrivateAttr(default=None)

    def set_components(self, components: SymbolComponents) -> None:
        """Set the cached components for this symbol.

        This works even on frozen models because private attributes
        are not subject to the frozen constraint in Pydantic.
        """
        self._components = components

    @property
    def base_asset(self) -> str:
        """Get the base asset of the symbol."""
        if not self._components:
            msg = "Components not set. Call service.parse_components() first."
            raise ValueError(msg)
        return self._components.base_asset

    @property
    def quote_asset(self) -> str | None:
        """Get the quote asset of the symbol."""
        if not self._components:
            msg = "Components not set. Call service.parse_components() first."
            raise ValueError(msg)
        return self._components.quote_asset

    @property
    def market_type(self) -> MarketType:
        """Get the market type of the symbol."""
        if not self._components:
            msg = "Components not set. Call service.parse_components() first."
            raise ValueError(msg)
        return self._components.market_type

    def __str__(self) -> str:
        """String representation of the symbol."""
        return self.value

    def __hash__(self) -> int:
        """Hash based on value and exchange."""
        return hash((self.value, self.exchange))


# Public API type alias - explicit union of all supported exchanges
type Symbol = BaseSymbol[HyperliquidMetadata] | BaseSymbol[BackpackMetadata]

# Internal use - export BaseSymbol for use in handlers
__all__ = [
    "BackpackMetadata",
    "BaseSymbol",
    "HyperliquidMetadata",
    "Symbol",
    "SymbolComponents",
    "SymbolMetadata",
]

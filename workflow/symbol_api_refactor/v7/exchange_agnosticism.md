"""Exchange Agnosticism Analysis for V7 Symbol Architecture

This file documents potential exchange agnosticism issues in the V7 implementation
and suggests improvements for better extensibility.
"""

# 🚨 POTENTIAL EXCHANGE AGNOSTICISM ISSUES:

## 1. Hard-coded exchange assumptions in protocols
# Current:
def create_metadata(
    self, asset_index: int | None = None, symbol_id: int | None = None
) -> TMetadata:
    """The protocol assumes only two types of metadata fields."""
    # What if a new exchange needs contract_address or listing_date?


## 2. Factory functions are exchange-specific
# Current:
def bp_symbol(value: str, symbol_id: int | None = None) -> Symbol[Any]:
    """Backpack-specific factory function."""
    
def hl_symbol(value: str, asset_index: int | None = None) -> Symbol[Any]:
    """Hyperliquid-specific factory function."""
# Adding a new exchange means modifying global_service.py


## 3. Market type assumptions
# Current:
market_type: MarketType = MarketType.PERP  # Default in SymbolComponents
# We assume PERP as default, but what if an exchange primarily trades SPOT or has OPTIONS?


## 4. Quote asset assumptions
# Both handlers assume USD as quote for perps:
# Hyperliquid
quote_asset="USD"
# Backpack  
quote_asset="USD"
# What if an exchange uses USDT or EUR as the perp quote?


## 5. Symbol format assumptions
# The fallback parsing in Symbol model is exchange-specific:
if "_" in self.value:
    return self.value.split("_")[0]
if "-" in self.value:
    return self.value.split("-")[0]
# This assumes underscore or dash separators. What about ":" or "/" or no separator?


# 💡 SUGGESTIONS FOR BETTER EXCHANGE AGNOSTICISM:

## 1. Make metadata creation exchange-specific
from typing import Protocol, Any
from abc import abstractmethod

class ExchangeHandler(Protocol[TMetadata]):
    @abstractmethod
    def create_metadata_from_config(self, config: dict[str, Any]) -> TMetadata:
        """Create metadata from configuration dict - each handler defines its needs."""
        ...
    
    @abstractmethod
    def create_default_metadata(self) -> TMetadata:
        """Create metadata with default values for this exchange."""
        ...


## 2. Metadata builder pattern
from typing import Generic, TypeVar

TMetadata = TypeVar('TMetadata', bound='SymbolMetadata')

class MetadataBuilder(Generic[TMetadata]):
    """Builder pattern for constructing exchange-specific metadata."""
    
    def build(self) -> TMetadata:
        """Build the metadata instance."""
        raise NotImplementedError

class HyperliquidMetadataBuilder(MetadataBuilder[HyperliquidMetadata]):
    def __init__(self):
        self._asset_index: int | None = None
    
    def with_asset_index(self, asset_index: int) -> 'HyperliquidMetadataBuilder':
        self._asset_index = asset_index
        return self
    
    def build(self) -> HyperliquidMetadata:
        return HyperliquidMetadata(asset_index=self._asset_index)


## 3. Symbol factory with explicit exchange methods
class SymbolFactory:
    """Factory with explicit methods per exchange."""
    
    def create_hyperliquid_symbol(
        self, 
        value: str,
        asset_index: int | None = None
    ) -> Symbol[HyperliquidMetadata]:
        """Create Hyperliquid symbol with explicit parameters."""
        handler = self.handlers[ExchangeName.HYPERLIQUID]
        return handler.create_symbol(value, asset_index=asset_index)
    
    def create_backpack_symbol(
        self,
        value: str,
        symbol_id: int
    ) -> Symbol[BackpackMetadata]:
        """Create Backpack symbol with explicit parameters."""
        handler = self.handlers[ExchangeName.BACKPACK]
        return handler.create_symbol(value, symbol_id=symbol_id)


## 4. Remove hardcoded defaults
class SymbolComponents(BaseModel):
    base_asset: str
    quote_asset: str | None = None
    market_type: MarketType | None = None  # No default


## 5. Handler-specific configuration
class HyperliquidHandler:
    DEFAULT_PERP_QUOTE = "USD"  # Configurable
    
class BackpackHandler:
    DEFAULT_PERP_QUOTE = "USDC"  # Different default


## 6. Remove fallback parsing from Symbol
@property
def base_asset(self) -> str:
    """Get the base asset of the symbol."""
    if not self._components:
        raise ValueError("Components not set. Call set_components() first.")
    return self._components.base_asset


## 7. Plugin registration system
# In factory.py
HANDLER_REGISTRY: dict[ExchangeName, type[ExchangeHandler]] = {}

def register_handler(
    exchange: ExchangeName, 
    handler_class: type[ExchangeHandler]
) -> None:
    """Register a handler for an exchange."""
    HANDLER_REGISTRY[exchange] = handler_class


# 📋 SUMMARY:
"""
While the current V7 implementation is much better than the 3-model system, 
it still has some exchange-specific assumptions baked in:

- Metadata fields in protocol signatures (asset_index, symbol_id)
- Hardcoded quote assets (USD for both exchanges)
- Default market types (PERP as default)
- Symbol parsing fallbacks (assumes _ or - separators)
- Exchange-specific factory functions (bp_symbol, hl_symbol)

For true exchange agnosticism without using **kwargs, we could:

1. Use configuration dicts with explicit parsing in handlers
2. Implement builder pattern for metadata construction
3. Create explicit factory methods per exchange
4. Move ALL defaults to handler classes
5. Remove symbol parsing from the core Symbol model
6. Use a plugin registration system for new exchanges

The key insight: Each exchange should be fully self-contained in its handler,
with the core system making NO assumptions about exchange-specific details.
"""
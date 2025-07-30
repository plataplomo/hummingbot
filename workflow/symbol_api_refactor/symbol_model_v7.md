# Symbol Model V7 - Protocol-Based Enhanced ExchangeSymbol

## Improvements Over V6
- **Better exchange agnosticism** through generic metadata
- **Stronger type safety** with TypedDict and generics
- **Clean behavior separation** using protocols

## Core Design: Data Model + Behavior Protocols

### 1. Exchange-Agnostic Metadata

```python
from typing import Protocol, TypeVar, Generic, TypedDict
from pydantic import BaseModel, Field, PrivateAttr, ConfigDict
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.core.enums.enums import MarketType

# Type-safe exchange metadata using TypedDict
class HyperliquidMetadata(TypedDict, total=False):
    """Hyperliquid-specific metadata."""
    asset_index: int
    universe: str  # "PERP" or "SPOT"
    
class BackpackMetadata(TypedDict, total=False):
    """Backpack-specific metadata."""
    symbol_id: int
    symbol_type: str  # "FUTURES" or "SPOT"

class BinanceMetadata(TypedDict, total=False):
    """Binance-specific metadata."""
    symbol_status: str
    base_precision: int
    quote_precision: int

# Generic metadata type
TMetadata = TypeVar('TMetadata', bound=dict)

class SymbolComponents(BaseModel):
    """Parsed symbol components - pure data."""
    base_asset: str
    quote_asset: str | None = None
    market_type: MarketType = MarketType.PERP
    
    model_config = ConfigDict(frozen=True)
```

### 2. Enhanced ExchangeSymbol with Generic Metadata

```python
class ExchangeSymbol(BaseModel, Generic[TMetadata]):
    """Exchange symbol with type-safe metadata and lazy component parsing.
    
    This is a pure data model - all behavior is delegated to protocols.
    """
    
    model_config = ConfigDict(frozen=True)
    
    # Core fields
    value: str = Field(..., min_length=1, max_length=30)
    exchange_id: ExchangeName
    
    # Type-safe exchange metadata
    metadata: TMetadata = Field(default_factory=dict)
    
    # Lazy-loaded components
    _components: SymbolComponents | None = PrivateAttr(default=None)
    _parser: 'SymbolComponentParser | None' = PrivateAttr(default=None)
    
    @property
    def base_asset(self) -> str:
        """Get base asset, parsing if needed."""
        return self.components.base_asset
    
    @property
    def quote_asset(self) -> str | None:
        """Get quote asset if this is a pair."""
        return self.components.quote_asset
    
    @property
    def market_type(self) -> MarketType:
        """Get market type."""
        return self.components.market_type
    
    @property
    def components(self) -> SymbolComponents:
        """Get parsed components, computing if needed."""
        if self._components is None:
            if self._parser is None:
                # Get parser from registry
                self._parser = get_parser_for_exchange(self.exchange_id)
            self._components = self._parser.parse(self.value, self.exchange_id)
        return self._components
    
    def with_parser(self, parser: 'SymbolComponentParser') -> 'ExchangeSymbol[TMetadata]':
        """Create new instance with specific parser (for testing)."""
        new = self.model_copy()
        object.__setattr__(new, '_parser', parser)
        return new
    
    # String compatibility
    def __str__(self) -> str:
        """String representation for backward compatibility."""
        return self.value
    
    # Type narrowing helpers
    def is_hyperliquid(self) -> TypeGuard['ExchangeSymbol[HyperliquidMetadata]']:
        """Type guard for Hyperliquid symbols."""
        return self.exchange_id == ExchangeName.HYPERLIQUID
    
    def is_backpack(self) -> TypeGuard['ExchangeSymbol[BackpackMetadata]']:
        """Type guard for Backpack symbols."""
        return self.exchange_id == ExchangeName.BACKPACK
```

### 3. Behavior Protocols

```python
class SymbolComponentParser(Protocol):
    """Protocol for parsing symbol components."""
    
    def parse(self, value: str, exchange: ExchangeName) -> SymbolComponents:
        """Parse symbol value into components."""
        ...
    
    def supports_exchange(self, exchange: ExchangeName) -> bool:
        """Check if parser supports this exchange."""
        ...

class SymbolConverter(Protocol):
    """Protocol for converting symbols between exchanges."""
    
    def convert(
        self, 
        symbol: ExchangeSymbol[Any], 
        target: ExchangeName
    ) -> ExchangeSymbol[Any]:
        """Convert symbol to target exchange format."""
        ...
    
    def to_canonical(self, symbol: ExchangeSymbol[Any]) -> str:
        """Convert to canonical format (e.g., BTC_USD)."""
        ...

class SymbolValidator(Protocol):
    """Protocol for validating symbol formats."""
    
    def validate_format(self, value: str, exchange: ExchangeName) -> list[str]:
        """Validate symbol format, return list of errors."""
        ...
    
    def is_tradeable(self, symbol: ExchangeSymbol[Any]) -> bool:
        """Check if symbol is tradeable on its exchange."""
        ...
```

### 4. Implementation Classes

```python
class TransformerBasedParser:
    """Parser implementation using existing transformers."""
    
    def __init__(self, transformers: dict[str, SymbolTransformerProtocol]):
        self.transformers = transformers
    
    def parse(self, value: str, exchange: ExchangeName) -> SymbolComponents:
        transformer = self.transformers.get(exchange.value)
        if transformer:
            try:
                internal = transformer.exchange_to_internal(value)
                return SymbolComponents(
                    base_asset=internal.base_asset,
                    quote_asset=internal.quote_asset,
                    market_type=internal.market_type
                )
            except Exception:
                pass
        
        # Fallback parsing
        return self._parse_fallback(value)
    
    def _parse_fallback(self, value: str) -> SymbolComponents:
        """Exchange-agnostic fallback parsing."""
        # Check for perpetual markers
        perp_markers = ["-PERP", "_PERP", "PERP", "-USD", "_USD"]
        for marker in perp_markers:
            if marker in value:
                base = value.replace(marker, "")
                return SymbolComponents(
                    base_asset=base,
                    quote_asset="USD",
                    market_type=MarketType.PERP
                )
        
        # Check for pair separators
        for sep in ["_", "-", "/"]:
            if sep in value:
                parts = value.split(sep, 1)
                if len(parts) == 2 and parts[0] and parts[1]:
                    return SymbolComponents(
                        base_asset=parts[0],
                        quote_asset=parts[1],
                        market_type=MarketType.SPOT
                    )
        
        # Single asset
        return SymbolComponents(
            base_asset=value,
            quote_asset=None,
            market_type=MarketType.SPOT
        )

class TransformerBasedConverter:
    """Converter implementation using transformers."""
    
    def __init__(self, transformers: dict[str, SymbolTransformerProtocol]):
        self.transformers = transformers
    
    def convert(
        self, 
        symbol: ExchangeSymbol[Any], 
        target: ExchangeName
    ) -> ExchangeSymbol[Any]:
        if target == symbol.exchange_id:
            return symbol
        
        # Get canonical form first
        canonical = self.to_canonical(symbol)
        
        # Convert to target format
        target_transformer = self.transformers.get(target.value)
        if target_transformer:
            # Create InternalSymbol for transformer
            internal = create_internal_symbol(
                value=canonical,
                base_asset=symbol.base_asset,
                quote_asset=symbol.quote_asset,
                market_type=symbol.market_type
            )
            target_value = target_transformer.internal_to_exchange(internal)
            
            # Create new symbol with appropriate metadata type
            metadata = self._get_default_metadata(target)
            return ExchangeSymbol(
                value=target_value,
                exchange_id=target,
                metadata=metadata
            )
        
        # Fallback: use same value
        return ExchangeSymbol(
            value=symbol.value,
            exchange_id=target,
            metadata={}
        )
    
    def to_canonical(self, symbol: ExchangeSymbol[Any]) -> str:
        """Get canonical format."""
        if symbol.quote_asset:
            return f"{symbol.base_asset}_{symbol.quote_asset}"
        return symbol.base_asset
```

### 5. Registry and Factory

```python
# Global registries
_PARSERS: dict[ExchangeName, SymbolComponentParser] = {}
_CONVERTERS: dict[str, SymbolConverter] = {}

def register_parser(exchange: ExchangeName, parser: SymbolComponentParser) -> None:
    """Register a parser for an exchange."""
    _PARSERS[exchange] = parser

def get_parser_for_exchange(exchange: ExchangeName) -> SymbolComponentParser:
    """Get parser for exchange, with fallback."""
    return _PARSERS.get(exchange, _default_parser)

# Initialize with transformer-based implementations
_default_parser = TransformerBasedParser(SYMBOL_TRANSFORMERS)
_default_converter = TransformerBasedConverter(SYMBOL_TRANSFORMERS)

# Register for all exchanges
for exchange in ExchangeName:
    register_parser(exchange, _default_parser)
```

### 6. Type-Safe Factory Functions

```python
def create_hyperliquid_symbol(
    value: str,
    asset_index: int | None = None,
) -> ExchangeSymbol[HyperliquidMetadata]:
    """Create Hyperliquid symbol with proper metadata."""
    metadata: HyperliquidMetadata = {}
    if asset_index is not None:
        metadata["asset_index"] = asset_index
    
    return ExchangeSymbol[HyperliquidMetadata](
        value=value,
        exchange_id=ExchangeName.HYPERLIQUID,
        metadata=metadata
    )

def create_backpack_symbol(
    value: str,
    symbol_id: int | None = None,
) -> ExchangeSymbol[BackpackMetadata]:
    """Create Backpack symbol with proper metadata."""
    metadata: BackpackMetadata = {}
    if symbol_id is not None:
        metadata["symbol_id"] = symbol_id
    
    return ExchangeSymbol[BackpackMetadata](
        value=value,
        exchange_id=ExchangeName.BACKPACK,
        metadata=metadata
    )
```

## Usage Examples

```python
# Type-safe symbol creation
btc_hl = create_hyperliquid_symbol("BTC-PERP", asset_index=0)
btc_bp = create_backpack_symbol("BTC_USDC_PERP", symbol_id=123)

# Component access (lazy parsed)
print(btc_hl.base_asset)  # "BTC"
print(btc_hl.quote_asset)  # "USD"
print(btc_hl.market_type)  # MarketType.PERP

# Type-safe metadata access
if btc_hl.is_hyperliquid():
    # Type checker knows metadata is HyperliquidMetadata
    index = btc_hl.metadata.get("asset_index")

# String compatibility
positions[str(btc_hl)] = position  # Works via __str__

# Conversion using protocol
converter = get_default_converter()
btc_binance = converter.convert(btc_hl, ExchangeName.BINANCE)

# Custom parser for testing
test_parser = MockParser({"BTC-PERP": SymbolComponents(...)})
test_symbol = btc_hl.with_parser(test_parser)
```

## Benefits Over V6

1. **True Exchange Agnosticism**
   - Generic metadata type `TMetadata`
   - No hardcoded exchange-specific fields
   - Easy to add new exchanges

2. **Better Type Safety**
   - TypedDict for metadata structure
   - Type guards for narrowing
   - Generic types preserve metadata type

3. **Clean Behavior Separation**
   - All parsing logic in `SymbolComponentParser`
   - All conversion logic in `SymbolConverter`
   - Data model has no behavior beyond properties

4. **Testability**
   - Can inject custom parsers
   - Protocol-based mocking
   - No global state in model

5. **Extensibility**
   - Register custom parsers per exchange
   - Override default implementations
   - Add new behavior protocols

## Migration Path

1. **Phase 1**: Implement enhanced ExchangeSymbol with protocols
2. **Phase 2**: Update existing code to use factory functions
3. **Phase 3**: Migrate metadata access to typed dictionaries
4. **Phase 4**: Replace string manipulation with component properties
5. **Phase 5**: Deprecate InternalSymbol/UnifiedSymbol

This design provides maximum flexibility while maintaining type safety and keeping behavior separate from data.
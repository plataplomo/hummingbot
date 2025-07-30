# Symbol Model V7 Fixed - Protocol-Based Enhanced ExchangeSymbol

## Improvements Over V6
- **Better exchange agnosticism** through base class inheritance for metadata
- **Stronger type safety** with Pydantic throughout
- **Clean behavior separation** using protocols
- **Scalable design** - new exchanges just extend SymbolMetadata base class

## Core Design: Data Model + Behavior Protocols

### 1. Exchange-Agnostic Metadata (Pydantic Models)

```python
from typing import Protocol
from pydantic import BaseModel, Field, PrivateAttr, ConfigDict
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.core.enums.enums import MarketType

# Base metadata model for type safety
class SymbolMetadata(BaseModel):
    """Base class for exchange metadata - ensures type safety."""
    model_config = ConfigDict(frozen=True)

class HyperliquidMetadata(SymbolMetadata):
    """Hyperliquid-specific metadata."""
    asset_index: int | None = None
    universe: str | None = None  # "PERP" or "SPOT"
    
class BackpackMetadata(SymbolMetadata):
    """Backpack-specific metadata."""
    symbol_id: int | None = None
    symbol_type: str | None = None  # "FUTURES" or "SPOT"

class SymbolComponents(BaseModel):
    """Parsed symbol components - pure data."""
    model_config = ConfigDict(frozen=True)
    
    base_asset: str
    quote_asset: str | None = None
    market_type: MarketType = MarketType.PERP
```

### 2. Enhanced ExchangeSymbol with Pydantic Metadata

```python
class ExchangeSymbol(BaseSymbol):
    """Exchange symbol with exchange-agnostic metadata and lazy component parsing.
    
    This is a pure data model - all behavior is delegated to protocols.
    """
    
    model_config = ConfigDict(frozen=True)
    
    # Core fields
    value: str = Field(..., min_length=1, max_length=30)
    exchange_id: ExchangeName
    
    # Exchange-agnostic metadata using base class
    metadata: SymbolMetadata | None = None
    
    # Parsed components stored directly (not lazy)
    _base_asset: str | None = PrivateAttr(default=None)
    _quote_asset: str | None = PrivateAttr(default=None)
    _market_type: MarketType | None = PrivateAttr(default=None)
    
    @property
    def base_asset(self) -> str:
        """Get base asset, parsing if needed."""
        if self._base_asset is None:
            self._parse_components()
        return self._base_asset
    
    @property
    def quote_asset(self) -> str | None:
        """Get quote asset if this is a pair."""
        if self._quote_asset is None:
            self._parse_components()
        return self._quote_asset
    
    @property
    def market_type(self) -> MarketType:
        """Get market type."""
        if self._market_type is None:
            self._parse_components()
        return self._market_type
    
    def _parse_components(self) -> None:
        """Parse components using the global parser."""
        parser = get_parser_for_exchange(self.exchange_id)
        components = parser.parse(self.value, self.exchange_id)
        self._base_asset = components.base_asset
        self._quote_asset = components.quote_asset
        self._market_type = components.market_type
    
    # String compatibility
    def __str__(self) -> str:
        """String representation for backward compatibility."""
        return self.value
    
    # Exchange-specific property accessors
    @property
    def asset_index(self) -> int | None:
        """Get Hyperliquid asset index if available."""
        if isinstance(self.metadata, HyperliquidMetadata):
            return self.metadata.asset_index
        return None
    
    @property
    def symbol_id(self) -> int | None:
        """Get Backpack symbol ID if available."""
        if isinstance(self.metadata, BackpackMetadata):
            return self.metadata.symbol_id
        return None
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
        symbol: ExchangeSymbol, 
        target: ExchangeName
    ) -> ExchangeSymbol:
        """Convert symbol to target exchange format."""
        ...
    
    def to_canonical(self, symbol: ExchangeSymbol) -> str:
        """Convert to canonical format (e.g., BTC_USD)."""
        ...

class SymbolValidator(Protocol):
    """Protocol for validating symbol formats."""
    
    def validate_format(self, value: str, exchange: ExchangeName) -> list[str]:
        """Validate symbol format, return list of errors."""
        ...
    
    def is_tradeable(self, symbol: ExchangeSymbol) -> bool:
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
        symbol: ExchangeSymbol, 
        target: ExchangeName
    ) -> ExchangeSymbol:
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
            
            # Create new symbol with appropriate metadata
            metadata = self._get_metadata_for_exchange(target)
            return ExchangeSymbol(
                value=target_value,
                exchange_id=target,
                metadata=metadata
            )
        
        # Fallback: use same value
        return ExchangeSymbol(
            value=symbol.value,
            exchange_id=target,
            metadata=None
        )
    
    def to_canonical(self, symbol: ExchangeSymbol) -> str:
        """Get canonical format."""
        if symbol.quote_asset:
            return f"{symbol.base_asset}_{symbol.quote_asset}"
        return symbol.base_asset
    
    def _get_metadata_for_exchange(self, exchange: ExchangeName) -> SymbolMetadata | None:
        """Get default metadata for exchange."""
        if exchange == ExchangeName.HYPERLIQUID:
            return HyperliquidMetadata()
        elif exchange == ExchangeName.BACKPACK:
            return BackpackMetadata()
        return None
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

# Register for supported exchanges only
for exchange in [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]:
    register_parser(exchange, _default_parser)
```

### 6. Factory Functions

```python
def create_hyperliquid_symbol(
    value: str,
    asset_index: int | None = None,
    universe: str | None = None,
) -> ExchangeSymbol:
    """Create Hyperliquid symbol with proper metadata."""
    metadata = HyperliquidMetadata(
        asset_index=asset_index,
        universe=universe
    ) if (asset_index is not None or universe is not None) else None
    
    return ExchangeSymbol(
        value=value,
        exchange_id=ExchangeName.HYPERLIQUID,
        metadata=metadata
    )

def create_backpack_symbol(
    value: str,
    symbol_id: int | None = None,
    symbol_type: str | None = None,
) -> ExchangeSymbol:
    """Create Backpack symbol with proper metadata."""
    metadata = BackpackMetadata(
        symbol_id=symbol_id,
        symbol_type=symbol_type
    ) if (symbol_id is not None or symbol_type is not None) else None
    
    return ExchangeSymbol(
        value=value,
        exchange_id=ExchangeName.BACKPACK,
        metadata=metadata
    )

def create_exchange_symbol(
    value: str,
    exchange_id: ExchangeName,
    asset_index: int | None = None,
    symbol_id: int | None = None,
) -> ExchangeSymbol:
    """Create exchange symbol backward compatible with current usage."""
    if exchange_id == ExchangeName.HYPERLIQUID:
        return create_hyperliquid_symbol(value, asset_index)
    elif exchange_id == ExchangeName.BACKPACK:
        return create_backpack_symbol(value, symbol_id)
    else:
        return ExchangeSymbol(value=value, exchange_id=exchange_id)
```

## Usage Examples

```python
# Create symbols
btc_hl = create_hyperliquid_symbol("BTC-PERP", asset_index=0)
btc_bp = create_backpack_symbol("BTC_USDC_PERP", symbol_id=123)

# Component access (parsed on first access)
print(btc_hl.base_asset)  # "BTC"
print(btc_hl.quote_asset)  # "USD"
print(btc_hl.market_type)  # MarketType.PERP

# Metadata access through properties
print(btc_hl.asset_index)  # 0
print(btc_bp.symbol_id)  # 123

# String compatibility
positions[str(btc_hl)] = position  # Works via __str__

# Conversion using protocol
converter = _default_converter
btc_bp_converted = converter.convert(btc_hl, ExchangeName.BACKPACK)

# For testing, can mock the parser in registry
mock_parser = MockParser({"BTC-PERP": SymbolComponents(...)})
register_parser(ExchangeName.HYPERLIQUID, mock_parser)

# Backward compatible with existing code
symbol = create_exchange_symbol(
    value="BTC-PERP",
    exchange_id=ExchangeName.HYPERLIQUID,
    asset_index=0
)
```

## Benefits

1. **Pure Pydantic** - No TypedDict, all Pydantic models
2. **No Binance** - Only Hyperliquid and Backpack as in codebase
3. **Exchange Agnostic** - Metadata as Pydantic models, not hardcoded fields
4. **Clean Separation** - Protocols for behavior, models for data
5. **Backward Compatible** - Works with existing ExchangeSymbol usage
6. **Type Safe** - Full Pydantic validation and type checking

## What This Solves

- Portfolio tracker can use `symbol.quote_asset` instead of `symbol.split("_")[-1]`
- ExchangeSymbol has exchange-agnostic metadata through base class inheritance
- Clean behavior separation through protocols
- Maintains compatibility with existing Week 1&2 refactor
- Scales to new exchanges by extending SymbolMetadata base class

## How It Scales

When adding a new exchange:

```python
# Just extend the base class
class NewExchangeMetadata(SymbolMetadata):
    """New exchange specific metadata."""
    custom_field: str | None = None
    another_field: int | None = None

# Update factory function
def create_new_exchange_symbol(value: str, custom_field: str | None = None) -> ExchangeSymbol:
    metadata = NewExchangeMetadata(custom_field=custom_field) if custom_field else None
    return ExchangeSymbol(value=value, exchange_id=ExchangeName.NEW_EXCHANGE, metadata=metadata)
```

No need to modify union types or core ExchangeSymbol class!
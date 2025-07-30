# Symbol Model V7 Clean - Pure Architecture

## Design Principles
- **No backward compatibility** - Clean slate design
- **Pure data models** - No behavior in models
- **Dependency injection** - No global state
- **Type safety first** - Generics and protocols

## Core Design: Decoupled Data Model + Injected Behavior

### 1. Exchange Metadata with Type Safety

```python
from typing import Protocol, TypeVar, Generic
from pydantic import BaseModel, Field, ConfigDict, PrivateAttr
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.core.enums.enums import MarketType

# Type variable for metadata
TMetadata = TypeVar('TMetadata', bound='SymbolMetadata')

# Base metadata model
class SymbolMetadata(BaseModel):
    """Base class for exchange metadata."""
    model_config = ConfigDict(frozen=True)

    @property
    def exchange_type(self) -> type[ExchangeName]:
        """Get the exchange type this metadata is for."""
        raise NotImplementedError

class HyperliquidMetadata(SymbolMetadata):
    """Hyperliquid-specific metadata."""
    # Only @N symbols have asset_index, so it's truly optional
    asset_index: int | None = None

    @property
    def exchange_type(self) -> type[ExchangeName]:
        return ExchangeName.HYPERLIQUID

class BackpackMetadata(SymbolMetadata):
    """Backpack-specific metadata."""
    # Required for WebSocket operations
    symbol_id: int

    @property
    def exchange_type(self) -> type[ExchangeName]:
        return ExchangeName.BACKPACK

class SymbolComponents(BaseModel):
    """Parsed symbol components - pure data."""
    model_config = ConfigDict(frozen=True)

    base_asset: str
    quote_asset: str | None = None
    market_type: MarketType = MarketType.PERP
```

### 2. Pure Symbol Data Model

```python
class Symbol(BaseModel, Generic[TMetadata]):
    """Pure symbol data model with no behavior.

    Clean design with no BaseSymbol inheritance or behavior methods.
    """

    model_config = ConfigDict(frozen=True)

    # Core fields only
    value: str = Field(..., min_length=1, max_length=30)
    exchange: ExchangeName

    # Exchange-specific data in typed metadata
    metadata: TMetadata

    # Cached components - computed once
    _components: SymbolComponents | None = PrivateAttr(default=None)

    @property
    def base_asset(self) -> str:
        """Get base asset from cached components."""
        if self._components:
            return self._components.base_asset
        # Fallback: parse from value (simple cases)
        if '_' in self.value:
            return self.value.split('_')[0]
        elif '-' in self.value:
            return self.value.split('-')[0]
        return self.value

    @property
    def quote_asset(self) -> str | None:
        """Get quote asset from cached components."""
        if self._components:
            return self._components.quote_asset
        return None

    @property
    def market_type(self) -> MarketType:
        """Get market type from cached components."""
        if self._components:
            return self._components.market_type
        return MarketType.PERP  # Default

    def __str__(self) -> str:
        """String representation."""
        return self.value

    def __hash__(self) -> int:
        """Hash for dict keys."""
        return hash((self.value, self.exchange))

```

### 3. Behavior Protocols

```python
class ExchangeHandler(Protocol[TMetadata]):
    """Protocol for exchange-specific symbol handling."""

    @property
    def exchange(self) -> ExchangeName:
        """The exchange this handler is for."""
        ...

    def parse_components(self, value: str) -> SymbolComponents:
        """Parse symbol value into components using exchange rules."""
        ...

    def format_symbol(self, components: SymbolComponents) -> str:
        """Format components into exchange-specific symbol value."""
        ...

    def to_canonical(self, value: str) -> tuple[str, SymbolComponents]:
        """Convert to canonical format and return components."""
        ...

    def from_canonical(self, canonical: str, components: SymbolComponents) -> str:
        """Convert from canonical format to exchange format."""
        ...

    def create_metadata(self, **kwargs) -> TMetadata:
        """Create exchange-specific metadata."""
        ...

    def create_symbol(self, value: str, **metadata_kwargs) -> Symbol[TMetadata]:
        """Create symbol with proper metadata."""
        ...
```

### 4. Service Layer

```python
class SymbolService:
    """Central service for symbol operations with injected dependencies."""

    def __init__(
        self,
        handlers: dict[ExchangeName, ExchangeHandler[Any]],
        equivalence_map: dict[str, list[Symbol[Any]]] | None = None
    ):
        self.handlers = handlers
        # Map canonical representations to equivalent symbols
        self._equivalence_map: dict[str, list[Symbol[Any]]] = equivalence_map or {}
        # Cache for canonical representations
        self._canonical_cache: dict[tuple[str, ExchangeName], tuple[str, SymbolComponents]] = {}

    def create_symbol(
        self,
        value: str,
        exchange: ExchangeName,
        **metadata_kwargs
    ) -> Symbol[Any]:
        """Create symbol using appropriate handler."""
        handler = self.handlers.get(exchange)
        if not handler:
            raise ValueError(f"No handler registered for exchange {exchange}")

        symbol = handler.create_symbol(value, **metadata_kwargs)

        # Pre-compute and cache components
        components = handler.parse_components(value)
        symbol._components = components

        return symbol

    def parse_components(self, symbol: Symbol[Any]) -> SymbolComponents:
        """Parse symbol components using exchange handler."""
        handler = self.handlers.get(symbol.exchange)
        if not handler:
            raise ValueError(f"No handler registered for exchange {symbol.exchange}")
        return handler.parse_components(symbol.value)

    def convert_symbol(
        self,
        symbol: Symbol[Any],
        target_exchange: ExchangeName
    ) -> Symbol[Any]:
        """Convert symbol to another exchange."""
        if symbol.exchange == target_exchange:
            return symbol

        # Get canonical representation
        canonical, components = self._get_canonical_with_components(symbol)

        # Convert to target format
        target_handler = self.handlers.get(target_exchange)
        if not target_handler:
            raise ValueError(f"No handler registered for exchange {target_exchange}")

        target_value = target_handler.from_canonical(canonical, components)
        return target_handler.create_symbol(target_value)

    def get_canonical(self, symbol: Symbol[Any]) -> str:
        """Get canonical representation."""
        canonical, _ = self._get_canonical_with_components(symbol)
        return canonical

    def _get_canonical_with_components(self, symbol: Symbol[Any]) -> tuple[str, SymbolComponents]:
        """Get canonical representation with components (cached)."""
        cache_key = (symbol.value, symbol.exchange)
        if cache_key in self._canonical_cache:
            return self._canonical_cache[cache_key]

        handler = self.handlers.get(symbol.exchange)
        if not handler:
            raise ValueError(f"No handler registered for exchange {symbol.exchange}")
        result = handler.to_canonical(symbol.value)

        self._canonical_cache[cache_key] = result
        return result

    def register_symbol(self, symbol: Symbol[Any]) -> None:
        """Register a symbol and update equivalence mappings."""
        canonical = self.get_canonical(symbol)
        if canonical not in self._equivalence_map:
            self._equivalence_map[canonical] = []

        # Check if this exact symbol already exists
        for existing in self._equivalence_map[canonical]:
            if existing.value == symbol.value and existing.exchange == symbol.exchange:
                return  # Already registered

        self._equivalence_map[canonical].append(symbol)

    def get_equivalent_symbols(self, symbol: Symbol[Any]) -> list[Symbol[Any]]:
        """Get all symbols equivalent to the given symbol."""
        canonical = self.get_canonical(symbol)
        return self._equivalence_map.get(canonical, [])

    def find_symbol(self, value: str, exchange: ExchangeName) -> Symbol[Any] | None:
        """Find a registered symbol by value and exchange."""
        # Search through all registered symbols
        for symbols in self._equivalence_map.values():
            for symbol in symbols:
                if symbol.value == value and symbol.exchange == exchange:
                    return symbol
        return None

    def are_equivalent(self, symbol1: Symbol[Any], symbol2: Symbol[Any]) -> bool:
        """Check if two symbols represent the same instrument."""
        return self.get_canonical(symbol1) == self.get_canonical(symbol2)
```

### 5. Implementation Classes

```python
class HyperliquidHandler:
    """Hyperliquid-specific symbol handling."""

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.HYPERLIQUID

    def parse_components(self, value: str) -> SymbolComponents:
        """Parse Hyperliquid symbol format."""
        # Handle @N format
        if value.startswith('@'):
            # This is an index-based symbol, need context to resolve
            return SymbolComponents(base_asset=value)

        # Handle PERP format
        if value.endswith('-PERP'):
            base = value[:-5]  # Remove '-PERP'
            return SymbolComponents(
                base_asset=base,
                quote_asset='USD',
                market_type=MarketType.PERP
            )

        # Handle spot pairs
        if '-' in value:
            parts = value.split('-', 1)
            return SymbolComponents(
                base_asset=parts[0],
                quote_asset=parts[1],
                market_type=MarketType.SPOT
            )

        # Single asset
        return SymbolComponents(base_asset=value)

    def format_symbol(self, components: SymbolComponents) -> str:
        """Format components into Hyperliquid symbol."""
        if components.market_type == MarketType.PERP:
            return f"{components.base_asset}-PERP"
        elif components.quote_asset:
            return f"{components.base_asset}-{components.quote_asset}"
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
        """Convert from canonical to Hyperliquid format."""
        return self.format_symbol(components)

    def create_metadata(self, **kwargs) -> HyperliquidMetadata:
        """Create Hyperliquid metadata."""
        return HyperliquidMetadata(
            asset_index=kwargs.get('asset_index')
        )

    def create_symbol(self, value: str, **metadata_kwargs) -> Symbol[HyperliquidMetadata]:
        """Create Hyperliquid symbol."""
        # Pass value to metadata creation for inference
        metadata_kwargs['value'] = value
        return Symbol[HyperliquidMetadata](
            value=value,
            exchange=self.exchange,
            metadata=self.create_metadata(**metadata_kwargs)
        )

class BackpackHandler:
    """Backpack-specific symbol handling."""

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.BACKPACK

    def parse_components(self, value: str) -> SymbolComponents:
        """Parse Backpack symbol format."""
        # Handle PERP format
        if value.endswith('_PERP'):
            # Remove _PERP and parse base
            base_part = value[:-5]
            if '_' in base_part:
                parts = base_part.split('_', 1)
                return SymbolComponents(
                    base_asset=parts[0],
                    quote_asset=parts[1],
                    market_type=MarketType.PERP
                )
            return SymbolComponents(
                base_asset=base_part,
                quote_asset='USD',
                market_type=MarketType.PERP
            )

        # Handle spot pairs
        if '_' in value:
            parts = value.split('_', 1)
            return SymbolComponents(
                base_asset=parts[0],
                quote_asset=parts[1],
                market_type=MarketType.SPOT
            )

        # Single asset
        return SymbolComponents(base_asset=value)

    def format_symbol(self, components: SymbolComponents) -> str:
        """Format components into Backpack symbol."""
        if components.market_type == MarketType.PERP:
            if components.quote_asset and components.quote_asset != 'USD':
                return f"{components.base_asset}_{components.quote_asset}_PERP"
            return f"{components.base_asset}_PERP"
        elif components.quote_asset:
            return f"{components.base_asset}_{components.quote_asset}"
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

    def create_metadata(self, **kwargs) -> BackpackMetadata:
        """Create Backpack metadata."""
        # symbol_id is required
        symbol_id = kwargs.get('symbol_id')
        if symbol_id is None:
            raise ValueError("symbol_id is required for Backpack symbols")

        return BackpackMetadata(
            symbol_id=symbol_id
        )

    def create_symbol(self, value: str, **metadata_kwargs) -> Symbol[BackpackMetadata]:
        """Create Backpack symbol."""
        # Pass value to metadata creation for inference
        metadata_kwargs['value'] = value
        return Symbol[BackpackMetadata](
            value=value,
            exchange=self.exchange,
            metadata=self.create_metadata(**metadata_kwargs)
        )

```

### 6. Dependency Injection Setup

```python
def create_symbol_service(handlers: dict[ExchangeName, ExchangeHandler[Any]] | None = None) -> SymbolService:
    """Create configured symbol service with all dependencies.

    Args:
        handlers: Optional dict of exchange handlers. If not provided, uses defaults.
    """
    # Use provided handlers or create defaults
    if handlers is None:
        handlers = {
            ExchangeName.HYPERLIQUID: HyperliquidHandler(),
            ExchangeName.BACKPACK: BackpackHandler(),
        }

    # Create service
    service = SymbolService(handlers)

    # Load symbols from config if available
    try:
        from cyberdelta.config import get_app_settings
        app_settings = get_app_settings()

        # Register all configured symbols
        for unified_config in app_settings.unified_symbols:
            # Create symbols for each exchange
            for exchange_str, exchange_config in unified_config.exchange_mappings.items():
                try:
                    exchange = ExchangeName(exchange_str.lower())
                except ValueError:
                    # Unknown exchange - skip
                    continue

                # Build metadata kwargs from config
                metadata_kwargs = {}

                # Add all config fields as metadata (exchange-agnostic)
                if hasattr(exchange_config, 'asset_index') and exchange_config.asset_index is not None:
                    metadata_kwargs['asset_index'] = exchange_config.asset_index
                if hasattr(exchange_config, 'symbol_id') and exchange_config.symbol_id is not None:
                    metadata_kwargs['symbol_id'] = int(exchange_config.symbol_id)
                if hasattr(exchange_config, 'universe') and exchange_config.universe is not None:
                    metadata_kwargs['universe'] = exchange_config.universe
                if hasattr(exchange_config, 'symbol_type') and exchange_config.symbol_type is not None:
                    metadata_kwargs['symbol_type'] = exchange_config.symbol_type

                symbol = service.create_symbol(
                    exchange_config.value,
                    exchange,
                    **metadata_kwargs
                )
                service.register_symbol(symbol)
    except Exception:
        # Config not available during testing or initialization
        pass

    return service
```

## Usage Examples

```python
# Create service with dependencies
service = create_symbol_service()

# Create symbols via service
btc_hl = service.create_symbol(
    "BTC-PERP",
    ExchangeName.HYPERLIQUID,
    asset_index=0
)

btc_bp = service.create_symbol(
    "BTC_USDC_PERP",
    ExchangeName.BACKPACK,
    symbol_id=123
)

# Register symbols to establish equivalence
service.register_symbol(btc_hl)
service.register_symbol(btc_bp)

# Direct access to components - no service needed!
print(btc_hl.base_asset)  # "BTC"
print(btc_hl.quote_asset)  # "USD"
print(btc_hl.market_type)  # MarketType.PERP

# Or use service for fresh parsing if needed
components = service.parse_components(btc_hl)
print(components.base_asset)  # "BTC"

# Convert between exchanges
btc_bp_converted = service.convert_symbol(btc_hl, ExchangeName.BACKPACK)
print(btc_bp_converted.value)  # "BTC_PERP"

# Get canonical representation
canonical = service.get_canonical(btc_hl)
print(canonical)  # "BTC_USD"

# Check equivalence
print(service.are_equivalent(btc_hl, btc_bp))  # True

# Get all equivalent symbols
equivalents = service.get_equivalent_symbols(btc_hl)
for symbol in equivalents:
    print(f"{symbol.exchange}: {symbol.value}")
# Output:
# HYPERLIQUID: BTC-PERP
# BACKPACK: BTC_USDC_PERP

# Find a specific symbol
found = service.find_symbol("BTC-PERP", ExchangeName.HYPERLIQUID)
if found:
    print(f"Found: {found.value} on {found.exchange}")

# Type-safe metadata access - always present
print(btc_hl.metadata.asset_index)  # Type safe! (may be None for non-@N symbols)

# Use in domain models
from typing import Any

class Ticker(BaseModel):
    """Domain model using symbols."""
    symbol: Symbol[Any]  # Any exchange
    price: Decimal

    def get_base_asset(self) -> str:
        """Get base asset directly from symbol."""
        return self.symbol.base_asset

# Adding a new exchange
class NewExchangeMetadata(SymbolMetadata):
    """New exchange metadata."""
    custom_field: str | None = None

class NewExchangeHandler:
    """Handler for new exchange."""

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.NEW_EXCHANGE

    def parse_components(self, value: str) -> SymbolComponents:
        # Exchange-specific parsing logic
        ...

    def format_symbol(self, components: SymbolComponents) -> str:
        # Exchange-specific formatting logic
        ...

    # ... implement other methods

# Register new exchange
handlers = {
    ExchangeName.HYPERLIQUID: HyperliquidHandler(),
    ExchangeName.BACKPACK: BackpackHandler(),
    ExchangeName.NEW_EXCHANGE: NewExchangeHandler(),
}
service = create_symbol_service(handlers)
```

## Clean Architecture Summary

### 1. **Pure Data Models**
- `Symbol` is just data (value, exchange, metadata)
- No behavior methods or parsing logic
- Clean separation of concerns

### 2. **Dependency Injection**
- All behavior injected via `SymbolService`
- No global state or registries
- Easy to test with mock dependencies

### 3. **Type Safety**
- Generic `Symbol[TMetadata]` preserves metadata type
- Factories return properly typed symbols
- No type information loss

### 4. **True Exchange Agnosticism**
- No exchange-specific fields in core model
- All exchange data in typed metadata
- New exchanges just need new factory + metadata class

### 5. **Clean Usage Pattern**
- Create service once with dependencies
- Use service for all operations
- Domain models receive service as parameter

### 6. **Cross-Exchange Equivalence**
- Service maintains equivalence mappings
- Symbols are registered to establish relationships
- Can query equivalent symbols across exchanges
- Canonical representation enables cross-exchange matching

## How This Solves the Canonical Problem

Instead of having separate InternalSymbol and UnifiedSymbol models, the service layer maintains the knowledge of which symbols are equivalent:

1. **Canonical Representation**: Each symbol can be converted to a canonical form (e.g., "BTC_USD")
2. **Equivalence Mapping**: The service tracks which symbols map to the same canonical form
3. **Cross-Exchange Knowledge**: When you need to know that "BTC-PERP" on Hyperliquid equals "BTC_USDC_PERP" on Backpack, the service provides this through:
   - `are_equivalent()` - Check if two symbols represent the same instrument
   - `get_equivalent_symbols()` - Get all equivalent symbols
   - `convert_symbol()` - Convert a symbol to another exchange's format

This approach:
- Keeps the single Symbol model clean and simple
- Maintains cross-exchange knowledge in the service layer
- Supports configuration loading to establish equivalence mappings
- Avoids the complexity of multiple model types
- Provides all the functionality of the 3-model system

## True Exchange Agnosticism

The improved V7 achieves exchange agnosticism through:

1. **Exchange Handlers**: Each exchange implements its own handler with:
   - Parsing logic (how to extract components from its format)
   - Formatting logic (how to create its format from components)
   - Canonical conversion (to/from canonical representation)
   - Metadata creation (exchange-specific fields)

2. **No Hardcoded Logic**: The service doesn't know about:
   - Specific symbol formats (like `-PERP` or `_USD`)
   - Exchange-specific metadata fields
   - Parsing rules for different exchanges

3. **Plugin Architecture**: Adding a new exchange only requires:
   - Creating a metadata class (if needed)
   - Implementing the ExchangeHandler protocol
   - Registering the handler with the service

4. **Config Agnosticism**: The config loader:
   - Uses `hasattr` to check for fields dynamically
   - Doesn't hardcode exchange-specific logic
   - Works with any metadata fields present in config

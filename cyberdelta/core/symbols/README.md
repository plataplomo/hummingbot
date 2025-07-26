# CyberDelta Symbol System - Domain-Driven Architecture

## Overview

The CyberDelta Symbol System is a comprehensive, type-safe symbol management system following Domain-Driven Design (DDD) principles. It provides unified symbol handling across multiple cryptocurrency exchanges with rich domain models, protocol-based architecture, and extensive validation.

## Architecture

### Core Principles

1. **Domain-First Design**: Rich domain objects with business logic encapsulation
2. **Type Safety**: Full Pydantic validation and type hints throughout
3. **Exchange Agnostic**: Clean separation between internal and exchange-specific representations
4. **Protocol-Based**: Dependency injection and testability through protocols
5. **Performance Optimized**: Thread-safe caching with O(1) lookups

### Domain Models

#### InternalSymbol
Canonical internal representation of trading symbols.

```python
from cyberdelta.core.symbols import create_internal_symbol
from cyberdelta.core.enums.enums import MarketType

symbol = create_internal_symbol(
    value="BTC_USD",
    base_asset="BTC", 
    quote_asset="USD",
    market_type=MarketType.PERP
)

# Computed properties
print(symbol.canonical_name)  # "BTC/USD Perpetual"
print(symbol.is_pair)         # True
```

#### ExchangeSymbol
Exchange-specific symbol representation with metadata.

```python
from cyberdelta.core.symbols import create_exchange_symbol
from cyberdelta.enums.exchange_names import ExchangeName

symbol = create_exchange_symbol(
    value="BTC-PERP",
    exchange_id=ExchangeName.HYPERLIQUID,
    internal_symbol=internal_symbol,
    asset_index=None  # For Hyperliquid spot symbols
)

# Exchange-specific metadata
print(symbol.is_indexed)      # False (no asset_index)
print(symbol.exchange_id)     # ExchangeName.HYPERLIQUID
```

#### UnifiedSymbol
Complete symbol representation combining internal and exchange mappings.

```python
from cyberdelta.core.symbols.models import UnifiedSymbol
from decimal import Decimal

unified = UnifiedSymbol(
    internal=internal_symbol,
    exchange_mappings={
        "hyperliquid": hyperliquid_symbol,
        "backpack": backpack_symbol
    },
    tick_size=Decimal("0.01"),
    min_order_size=Decimal("0.001"),
    is_active=True,
    is_tradeable=True
)
```

### Service Layer

#### SymbolService
Main orchestration service for all symbol operations.

```python
from cyberdelta.core.symbols import SymbolService

service = SymbolService()

# Transform exchange symbol to internal format
internal = service.get_internal_symbol("BTC-PERP", "hyperliquid")

# Transform internal symbol to exchange format  
exchange = service.get_exchange_symbol("BTC_USD", "hyperliquid")

# Batch operations with detailed results
result = service.batch_transform_symbols(
    ["BTC-PERP", "ETH-PERP"], 
    "hyperliquid"
)
print(f"Success rate: {result.success_rate}%")

# Arbitrage compatibility validation
compatibility = service.validate_arbitrage_compatibility(
    "BTC_USD", 
    ["hyperliquid", "backpack"]
)
print(f"Compatible: {compatibility.is_arbitrage_compatible}")
```

### Storage Layer

#### SymbolStore
Thread-safe storage with optimized lookups.

```python
from cyberdelta.core.symbols import SymbolStore

store = SymbolStore()

# Register unified symbol
store.store(unified_symbol)

# Fast O(1) lookups
by_internal = store.get_by_internal("BTC_USD")
by_exchange = store.get_by_exchange("BTC-PERP", "hyperliquid")
```

### Transformation Layer

#### Exchange Transformers
Plugin system for exchange-specific symbol transformations.

```python
from cyberdelta.core.symbols.transformers import HyperliquidSymbolTransformer

transformer = HyperliquidSymbolTransformer()

# Internal to exchange
exchange_value = transformer.internal_to_exchange(internal_symbol)

# Exchange to internal 
internal_symbol = transformer.exchange_to_internal("BTC-PERP")
```

## Usage Examples

### Basic Symbol Operations

```python
from cyberdelta.core.symbols import (
    SymbolService, 
    create_internal_symbol,
    MarketType
)

# Initialize service
service = SymbolService()

# Create and register a symbol
internal = create_internal_symbol(
    value="BTC_USD",
    base_asset="BTC",
    quote_asset="USD", 
    market_type=MarketType.PERP
)

# Get exchange representations
hyperliquid_symbol = service.get_exchange_symbol("BTC_USD", "hyperliquid")
backpack_symbol = service.get_exchange_symbol("BTC_USD", "backpack")

print(f"Hyperliquid: {hyperliquid_symbol.value}")  # "BTC-PERP"
print(f"Backpack: {backpack_symbol.value}")       # "BTC_PERP"
```

### Arbitrage Operations

```python
from cyberdelta.core.symbols.helpers import get_domain_helpers

helpers = get_domain_helpers()

# Validate arbitrage pair
is_valid, errors = helpers.validate_arbitrage_pair(
    "BTC_USD", 
    "hyperliquid", 
    "backpack"
)

if is_valid:
    # Get symbols for arbitrage
    long_symbol = helpers.resolve_for_exchange("BTC_USD", "hyperliquid")
    short_symbol = helpers.resolve_for_exchange("BTC_USD", "backpack") 
    
    print(f"Long: {long_symbol.value} on {long_symbol.exchange_id.value}")
    print(f"Short: {short_symbol.value} on {short_symbol.exchange_id.value}")
```

### API Integration

```python
from cyberdelta.apis.common.domain_formatters import format_symbol_for_api

# Format symbol for exchange API
api_format = format_symbol_for_api(exchange_symbol)

# Exchange-specific formatting
if exchange_symbol.exchange_id == ExchangeName.HYPERLIQUID:
    # Returns: {"symbol": "BTC-PERP"}
    # Or: {"symbol": "BTC/USDC", "assetIndex": 0} for spot
    pass
elif exchange_symbol.exchange_id == ExchangeName.BACKPACK:
    # Returns: {"symbol": "BTC_PERP", "symbolId": 1001}
    pass
```

### Validation

```python
from cyberdelta.core.symbols.validators import (
    validate_domain_object,
    get_validation_errors,
    validate_arbitrage_pair
)

# Validate domain objects
is_valid = validate_domain_object(symbol)
errors = get_validation_errors(symbol)

# Validate arbitrage pairs
is_compatible = validate_arbitrage_pair(long_symbol, short_symbol)
```

### Logging and Observability

```python
from cyberdelta.core.symbols.logging_helpers import (
    log_symbol_context,
    create_operation_logger
)

# Rich structured logging
log_symbol_context(
    "Symbol resolved successfully",
    symbol=exchange_symbol,
    level="info",
    operation="arbitrage_setup"
)

# Component-specific logger
logger = create_operation_logger("execution_handler")
logger.log_symbol_resolution(
    operation="exchange_to_internal",
    symbol_input="BTC-PERP",
    exchange="hyperliquid",
    result=internal_symbol,
    duration_ms=12.5
)
```

## Testing

### Test Factories

```python
from tests.factories.symbol_factories import (
    InternalSymbolFactory,
    ExchangeSymbolFactory,
    UnifiedSymbolFactory,
    ArbitrageSymbolFactory,
    create_btc_symbols
)

# Create test symbols
btc_internal = InternalSymbolFactory.create_btc_usd_perp()
btc_hyperliquid = ExchangeSymbolFactory.create_hyperliquid_btc_perp()
btc_unified = UnifiedSymbolFactory.create_btc_usd_perp_unified()

# Create arbitrage pairs
long_symbol, short_symbol = ArbitrageSymbolFactory.create_btc_arbitrage_pair()

# Complete symbol sets
internal, hl_exchange, bp_exchange, unified = create_btc_symbols()
```

### Test Examples

```python
import pytest
from tests.factories.symbol_factories import create_test_symbol_service_data

def test_symbol_service_operations():
    service = SymbolService()
    
    # Load test data
    test_symbols = create_test_symbol_service_data()
    for symbol in test_symbols:
        service.register_symbol(symbol)
    
    # Test transformations
    internal = service.get_internal_symbol("BTC-PERP", "hyperliquid")
    assert internal.value == "BTC_USD"
    assert internal.base_asset == "BTC"
    assert internal.market_type == MarketType.PERP
    
    exchange = service.get_exchange_symbol("BTC_USD", "backpack")
    assert exchange.value == "BTC_PERP"
    assert exchange.exchange_id == ExchangeName.BACKPACK
```

## Migration from Legacy System

### Domain Model Migration

The system is designed to migrate from string-based symbol handling to rich domain objects:

```python
# Legacy approach (deprecated)
symbol_mapper = SymbolServiceAdapter()  # Will be deleted
exchange_symbol = symbol_mapper.get_exchange_symbol("BTC_USD", "hyperliquid")

# New domain approach
helpers = get_domain_helpers()
exchange_symbol = helpers.resolve_for_exchange("BTC_USD", "hyperliquid")

# Now you have full domain object with metadata
print(exchange_symbol.exchange_id)     # ExchangeName.HYPERLIQUID
print(exchange_symbol.internal_symbol) # Full InternalSymbol object
print(exchange_symbol.is_indexed)     # Computed property
```

### Migration Utilities

```python
from cyberdelta.core.symbols.migration_utils import (
    LegacyBridge,
    safe_domain_call,
    migrate_string_to_domain
)

# Temporary compatibility bridge
bridge = LegacyBridge(helpers)
exchange_value = bridge.get_exchange_symbol_value("BTC_USD", "hyperliquid")

# Safe migration helpers
result = safe_domain_call(
    helpers.resolve_for_exchange,
    "BTC_USD", 
    "hyperliquid",
    fallback=None
)
```

## Performance Characteristics

### Lookups
- **Internal → Exchange**: O(1) for registered symbols, O(1) transformation fallback
- **Exchange → Internal**: O(1) for registered symbols, O(1) transformation fallback  
- **Symbol Registration**: O(1) with three indices maintained
- **Batch Operations**: Linear with early termination on errors

### Memory Usage
- **Symbol Storage**: ~1KB per UnifiedSymbol with full metadata
- **Index Overhead**: ~200 bytes per symbol across three indices
- **Transformer Cache**: Stateless, no memory overhead

### Threading
- **Thread Safety**: Full thread safety with RLock protection
- **Concurrency**: Read operations are lock-free after initialization
- **Scalability**: Supports high-frequency trading workloads

## Configuration

### Symbol Loading

```python
from cyberdelta.core.symbols.config_loader import load_symbols_from_config

# Load from configuration
symbols = load_symbols_from_config()
for symbol in symbols:
    service.register_symbol(symbol)
```

### Exchange Configuration

```python
# Supported exchanges are auto-detected from transformers
supported = service.get_supported_exchanges()
print(supported)  # ["hyperliquid", "backpack", "binance"]
```

## Error Handling

### Exception Types

```python
from cyberdelta.core.symbols.exceptions import (
    SymbolError,
    SymbolNotFoundError,
    SymbolValidationError
)

try:
    symbol = service.get_exchange_symbol("INVALID", "hyperliquid")
except SymbolNotFoundError as e:
    print(f"Symbol not found: {e}")
    print(f"Details: {e.details}")
except SymbolValidationError as e:
    print(f"Validation failed: {e}")
    print(f"Expected format: {e.expected_format}")
```

### Rich Error Context

All operations provide detailed error context:

```python
# Detailed error messages with domain context
# Example: "Failed to transform symbol 'BTC-INVALID' from hyperliquid to internal format. 
#          Error: Invalid symbol format. Transformer: HyperliquidSymbolTransformer"
```

## Extending the System

### Adding New Exchanges

1. **Create Transformer**:
```python
from cyberdelta.core.symbols.protocols import SymbolTransformerProtocol

class NewExchangeTransformer(SymbolTransformerProtocol):
    def internal_to_exchange(self, internal: InternalSymbol) -> str:
        # Implementation
        pass
    
    def exchange_to_internal(self, exchange_symbol: str) -> InternalSymbol:
        # Implementation  
        pass
```

2. **Register Transformer**:
```python
from cyberdelta.core.symbols.transformers import SYMBOL_TRANSFORMERS

SYMBOL_TRANSFORMERS["newexchange"] = NewExchangeTransformer()
```

3. **Add API Formatter**:
```python
from cyberdelta.apis.common.domain_formatters import ExchangeApiFormatter

class NewExchangeApiFormatter(ExchangeApiFormatter):
    @staticmethod
    def format_symbol_for_api(exchange_symbol: ExchangeSymbol) -> dict:
        # Implementation
        pass
```

### Custom Validation Rules

```python
from cyberdelta.core.symbols.validators import DomainObjectValidator

class CustomValidator(DomainObjectValidator):
    @staticmethod
    def validate_custom_symbol(symbol: ExchangeSymbol) -> List[str]:
        errors = []
        # Custom validation logic
        return errors
```

## Best Practices

### Domain Object Usage

1. **Always use domain objects** instead of strings for symbol operations
2. **Validate domain objects** before critical operations
3. **Use factories** for consistent test object creation
4. **Leverage computed properties** for business logic

### Performance Optimization

1. **Register symbols** at startup for O(1) lookups
2. **Use batch operations** for multiple symbols
3. **Cache domain helpers** instead of creating new instances
4. **Prefer unified symbols** for multi-exchange operations

### Error Handling

1. **Catch specific exceptions** (SymbolNotFoundError, SymbolValidationError)
2. **Use safe domain calls** for fallback behavior
3. **Log with domain context** for better debugging
4. **Validate arbitrage pairs** before execution

### Testing

1. **Use symbol factories** for consistent test data
2. **Test domain object validation** explicitly
3. **Mock at the protocol level** for unit tests
4. **Test error scenarios** with invalid symbols

## Troubleshooting

### Common Issues

**Symbol not found errors**:
- Check if symbol is registered in the service
- Verify exchange name spelling ("hyperliquid" not "Hyperliquid")
- Ensure transformer exists for the exchange

**Validation failures**:
- Check symbol format against exchange conventions
- Verify market type consistency across exchanges
- Ensure required metadata (asset_index, symbol_id) is present

**Performance issues**:
- Register symbols at startup instead of lazy loading
- Use batch operations for multiple symbols
- Check for proper index usage in lookups

### Debug Logging

Enable debug logging to see detailed symbol operations:

```python
import logging
logging.getLogger("cyberdelta.core.symbols").setLevel(logging.DEBUG)
```

### Health Checks

```python
# Verify system health
service = SymbolService()
print(f"Supported exchanges: {service.get_supported_exchanges()}")
print(f"Registered symbols: {len(service.get_all_symbols())}")

# Test basic operations
try:
    internal = service.get_internal_symbol("BTC-PERP", "hyperliquid")
    exchange = service.get_exchange_symbol("BTC_USD", "hyperliquid")
    print("✓ Symbol transformations working")
except Exception as e:
    print(f"✗ Symbol transformations failed: {e}")
```

---

This symbol system provides a robust, type-safe foundation for cryptocurrency trading operations with excellent performance, maintainability, and extensibility characteristics.
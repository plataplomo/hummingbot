# Step 5: Testing and Usage Examples

## Overview
Create comprehensive tests for architecture and provide clear usage examples. This ensures the new system works correctly and developers understand how to use it.

## Implementation

### 5.1 Create Test Directory Structure
```bash
mkdir -p tests/unit/core/symbols/new
```

### 5.2 Test Models
**File**: `tests/unit/core/symbols/new/test_models.py`

```python
"""Tests for Symbol Models."""

import pytest
from cyberdelta.core.symbols import (
    Symbol,
    HyperliquidMetadata,
    BackpackMetadata,
    SymbolComponents,
)
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.core.enums.enums import MarketType

class TestSymbolModel:
    def test_symbol_creation_hyperliquid(self):
        metadata = HyperliquidMetadata(asset_index=1)
        symbol = Symbol[HyperliquidMetadata](
            value="BTC-PERP",
            exchange=ExchangeName.HYPERLIQUID,
            metadata=metadata
        )

        assert symbol.value == "BTC-PERP"
        assert symbol.exchange == ExchangeName.HYPERLIQUID
        assert symbol.metadata.asset_index == 1

    def test_symbol_creation_backpack(self):
        metadata = BackpackMetadata(symbol_id=123)
        symbol = Symbol[BackpackMetadata](
            value="BTC_PERP",
            exchange=ExchangeName.BACKPACK,
            metadata=metadata
        )

        assert symbol.value == "BTC_PERP"
        assert symbol.exchange == ExchangeName.BACKPACK
        assert symbol.metadata.symbol_id == 123

    def test_symbol_immutability(self):
        metadata = BackpackMetadata(symbol_id=123)
        symbol = Symbol[BackpackMetadata](
            value="BTC_PERP",
            exchange=ExchangeName.BACKPACK,
            metadata=metadata
        )

        # Should not be able to modify
        with pytest.raises(Exception):
            symbol.value = "ETH_PERP"

    def test_symbol_properties_with_components(self):
        metadata = HyperliquidMetadata()
        symbol = Symbol[HyperliquidMetadata](
            value="BTC-PERP",
            exchange=ExchangeName.HYPERLIQUID,
            metadata=metadata
        )

        # Set cached components
        symbol._components = SymbolComponents(
            base_asset="BTC",
            quote_asset="USD",
            market_type=MarketType.PERP
        )

        assert symbol.base_asset == "BTC"
        assert symbol.quote_asset == "USD"
        assert symbol.market_type == MarketType.PERP

    def test_symbol_hash_and_equality(self):
        metadata1 = HyperliquidMetadata()
        symbol1 = Symbol[HyperliquidMetadata](
            value="BTC-PERP",
            exchange=ExchangeName.HYPERLIQUID,
            metadata=metadata1
        )

        metadata2 = HyperliquidMetadata()
        symbol2 = Symbol[HyperliquidMetadata](
            value="BTC-PERP",
            exchange=ExchangeName.HYPERLIQUID,
            metadata=metadata2
        )

        # Same value and exchange = same hash
        assert hash(symbol1) == hash(symbol2)

        # Can be used as dict keys
        symbol_dict = {symbol1: "data"}
        assert symbol2 in symbol_dict
```

### 5.3 Test Exchange Handlers
**File**: `tests/unit/core/symbols/new/test_handlers.py`

```python
"""Tests for Exchange Handlers."""

import pytest
from cyberdelta.core.symbols.handlers import HyperliquidHandler, BackpackHandler
from cyberdelta.core.enums.enums import MarketType
from cyberdelta.enums.exchange_names import ExchangeName

class TestHyperliquidHandler:
    def setup_method(self):
        self.handler = HyperliquidHandler()

    def test_parse_perp_symbol(self):
        components = self.handler.parse_components("BTC-PERP")
        assert components.base_asset == "BTC"
        assert components.quote_asset == "USD"
        assert components.market_type == MarketType.PERP

    def test_parse_spot_symbol(self):
        components = self.handler.parse_components("BTC-USDC")
        assert components.base_asset == "BTC"
        assert components.quote_asset == "USDC"
        assert components.market_type == MarketType.SPOT

    def test_parse_index_symbol(self):
        components = self.handler.parse_components("@1")
        assert components.base_asset == "@1"
        assert components.quote_asset is None

    def test_canonical_conversion(self):
        canonical, components = self.handler.to_canonical("BTC-PERP")
        assert canonical == "BTC_USD"

        # Round trip
        hl_format = self.handler.from_canonical(canonical, components)
        assert hl_format == "BTC-PERP"

    def test_create_symbol(self):
        symbol = self.handler.create_symbol("BTC-PERP", asset_index=0)
        assert symbol.value == "BTC-PERP"
        assert symbol.exchange == ExchangeName.HYPERLIQUID
        assert symbol.metadata.asset_index == 0
        assert symbol.base_asset == "BTC"  # Cached components

class TestBackpackHandler:
    def setup_method(self):
        self.handler = BackpackHandler()

    def test_parse_perp_symbol(self):
        components = self.handler.parse_components("BTC_PERP")
        assert components.base_asset == "BTC"
        assert components.quote_asset == "USD"
        assert components.market_type == MarketType.PERP

    def test_parse_perp_with_quote(self):
        components = self.handler.parse_components("BTC_USDC_PERP")
        assert components.base_asset == "BTC"
        assert components.quote_asset == "USDC"
        assert components.market_type == MarketType.PERP

    def test_create_symbol_requires_id(self):
        with pytest.raises(ValueError, match="symbol_id is required"):
            self.handler.create_symbol("BTC_PERP")

    def test_create_symbol_with_id(self):
        symbol = self.handler.create_symbol("BTC_PERP", symbol_id=1)
        assert symbol.metadata.symbol_id == 1
        assert symbol.base_asset == "BTC"
```

### 5.4 Test Symbol Service
**File**: `tests/unit/core/symbols/new/test_service.py`

```python
"""Tests for Symbol Service."""

import pytest
from cyberdelta.core.symbols import (
    SymbolService,
    HyperliquidHandler,
    BackpackHandler,
)
from cyberdelta.enums.exchange_names import ExchangeName

class TestSymbolService:
    def setup_method(self):
        handlers = {
            ExchangeName.HYPERLIQUID: HyperliquidHandler(),
            ExchangeName.BACKPACK: BackpackHandler(),
        }
        self.service = SymbolService(handlers)

    def test_create_symbol(self):
        symbol = self.service.create_symbol(
            "BTC-PERP",
            ExchangeName.HYPERLIQUID,
            asset_index=0
        )
        assert symbol.value == "BTC-PERP"
        assert symbol.exchange == ExchangeName.HYPERLIQUID
        assert symbol.base_asset == "BTC"  # Components cached

    def test_equivalence_checking(self):
        # Create equivalent symbols
        btc_hl = self.service.create_symbol(
            "BTC-PERP",
            ExchangeName.HYPERLIQUID
        )
        btc_bp = self.service.create_symbol(
            "BTC_PERP",
            ExchangeName.BACKPACK,
            symbol_id=1
        )

        # Register them
        self.service.register_symbol(btc_hl)
        self.service.register_symbol(btc_bp)

        # Check equivalence
        assert self.service.are_equivalent(btc_hl, btc_bp)

        # Get all equivalent symbols
        equivalents = self.service.get_equivalent_symbols(btc_hl)
        assert len(equivalents) == 2
        assert btc_bp in equivalents

    def test_symbol_conversion(self):
        btc_hl = self.service.create_symbol(
            "BTC-PERP",
            ExchangeName.HYPERLIQUID
        )

        # Convert to Backpack format
        btc_bp = self.service.convert_symbol(btc_hl, ExchangeName.BACKPACK)
        assert btc_bp.value == "BTC_PERP"
        assert btc_bp.exchange == ExchangeName.BACKPACK

    def test_find_symbol(self):
        symbol = self.service.create_symbol(
            "ETH-PERP",
            ExchangeName.HYPERLIQUID
        )
        self.service.register_symbol(symbol)

        found = self.service.find_symbol("ETH-PERP", ExchangeName.HYPERLIQUID)
        assert found is not None
        assert found.value == "ETH-PERP"
```

### 5.5 Create Usage Examples
**File**: `cyberdelta/core/symbols/examples.py`

```python
"""Symbol System Usage Examples."""

from cyberdelta.core.symbols import (
    bp_symbol,
    hl_symbol,
    get_symbol_service,
    Symbol,
)
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.core.enums.enums import MarketType

def basic_usage():
    """Basic symbol creation and usage."""
    # Create symbols using factory functions
    btc_bp = bp_symbol("BTC_PERP", symbol_id=1)
    eth_bp = bp_symbol("ETH_USDC", symbol_id=20)

    # Direct property access
    print(f"Base: {btc_bp.base_asset}")      # "BTC"
    print(f"Quote: {btc_bp.quote_asset}")    # "USD"
    print(f"Type: {btc_bp.market_type}")     # MarketType.PERP

    # Type-safe metadata access
    print(f"Symbol ID: {btc_bp.metadata.symbol_id}")  # 1

def cross_exchange_operations():
    """Working with symbols across exchanges."""
    service = get_symbol_service()

    # Create equivalent symbols
    btc_bp = bp_symbol("BTC_PERP", symbol_id=1)
    btc_hl = hl_symbol("BTC-PERP", asset_index=0)

    # Register for equivalence tracking
    service.register_symbol(btc_bp)
    service.register_symbol(btc_hl)

    # Check if they're the same instrument
    if service.are_equivalent(btc_bp, btc_hl):
        print("These represent the same instrument!")

    # Convert between exchanges
    eth_hl = hl_symbol("ETH-PERP")
    eth_bp = service.convert_symbol(eth_hl, ExchangeName.BACKPACK)
    print(f"Converted: {eth_bp.value}")  # "ETH_PERP"

def using_in_domain_models():
    """Using symbols in domain models."""
    from typing import Any
    from decimal import Decimal
    from pydantic import BaseModel

    class Ticker(BaseModel):
        """Domain model using symbols."""
        symbol: Symbol[Any]
        bid_price: Decimal
        ask_price: Decimal

        @property
        def spread(self) -> Decimal:
            return self.ask_price - self.bid_price

        @property
        def display_name(self) -> str:
            """Get display name using symbol properties."""
            if self.symbol.quote_asset:
                return f"{self.symbol.base_asset}/{self.symbol.quote_asset}"
            return self.symbol.base_asset

    # Usage
    ticker = Ticker(
        symbol=bp_symbol("BTC_PERP", symbol_id=1),
        bid_price=Decimal("50000.00"),
        ask_price=Decimal("50001.00")
    )

    print(f"Ticker: {ticker.display_name}")  # "BTC/USD"
    print(f"Spread: {ticker.spread}")        # 1.00

def batch_operations():
    """Working with multiple symbols."""
    service = get_symbol_service()

    # Define symbols to create
    symbols_to_create = [
        ("BTC_PERP", 1),
        ("ETH_PERP", 2),
        ("SOL_PERP", 3),
    ]

    # Create and register
    for value, symbol_id in symbols_to_create:
        symbol = bp_symbol(value, symbol_id=symbol_id)
        service.register_symbol(symbol)

    # Find specific symbol
    btc = service.find_symbol("BTC_PERP", ExchangeName.BACKPACK)
    if btc:
        print(f"Found: {btc.value} with ID {btc.metadata.symbol_id}")

if __name__ == "__main__":
    basic_usage()
    cross_exchange_operations()
    using_in_domain_models()
    batch_operations()
```

## Testing Strategy

### Unit Tests
1. Test each component in isolation
2. Test all edge cases
3. Test error conditions
4. Test type safety

### Integration Tests
1. Test full workflow
2. Test config loading
3. Test cross-exchange operations
4. Test with real data

### Performance Tests
1. Test symbol creation performance
2. Test lookup performance
3. Test conversion performance

## Documentation
1. Update main README
2. Add docstrings to all public APIs
3. Create migration guide
4. Add type hints everywhere

## Success Criteria
- [ ] All tests pass
- [ ] Examples run correctly
- [ ] Documentation complete
- [ ] Performance acceptable
- [ ] Type checking passes

## Summary
The new symbol system is now complete with:
- Clean architecture
- Type-safe metadata
- Exchange-agnostic design
- Global access patterns
- Comprehensive tests

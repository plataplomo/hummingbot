# Step 3: Replace Service Layer

## Overview
Replace the current SymbolService and SymbolStore with the service that uses handlers for all operations. This provides equivalence checking, conversions, and canonical representations.

## Current State
- **service.py**: Complex service with store dependency, uses transformers
- **store.py**: UnifiedSymbol storage with indices
- **Issues**: Tied to 3-model architecture, complex storage logic

## Implementation

### 3.1 Replace service.py
**File**: `cyberdelta/core/symbols/service.py`

Replace entire file with:
```python
"""Symbol Service - Clean Architecture."""

from typing import Any
from cyberdelta.enums.exchange_names import ExchangeName
from .models import Symbol, SymbolComponents
from .protocols import ExchangeHandler

class SymbolService:
    """Central service for symbol operations with injected dependencies."""

    def __init__(
        self,
        handlers: dict[ExchangeName, ExchangeHandler[Any]],
        equivalence_map: dict[str, list[Symbol[Any]]] | None = None
    ):
        self.handlers = handlers
        self._equivalence_map: dict[str, list[Symbol[Any]]] = equivalence_map or {}
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

        # Check if already registered
        for existing in self._equivalence_map[canonical]:
            if existing.value == symbol.value and existing.exchange == symbol.exchange:
                return

        self._equivalence_map[canonical].append(symbol)

    def get_equivalent_symbols(self, symbol: Symbol[Any]) -> list[Symbol[Any]]:
        """Get all symbols equivalent to the given symbol."""
        canonical = self.get_canonical(symbol)
        return self._equivalence_map.get(canonical, [])

    def find_symbol(self, value: str, exchange: ExchangeName) -> Symbol[Any] | None:
        """Find a registered symbol by value and exchange."""
        for symbols in self._equivalence_map.values():
            for symbol in symbols:
                if symbol.value == value and symbol.exchange == exchange:
                    return symbol
        return None

    def are_equivalent(self, symbol1: Symbol[Any], symbol2: Symbol[Any]) -> bool:
        """Check if two symbols represent the same instrument."""
        return self.get_canonical(symbol1) == self.get_canonical(symbol2)
```

### 3.2 Create Factory Function
**File**: `cyberdelta/core/symbols/factory.py`

```python
"""Symbol Service Factory."""

from typing import Any
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.config import get_app_settings
from .service import SymbolService
from .protocols import ExchangeHandler
from .handlers import DEFAULT_HANDLERS

def create_symbol_service(
    handlers: dict[ExchangeName, ExchangeHandler[Any]] | None = None
) -> SymbolService:
    """Create configured symbol service with all dependencies."""
    # Use provided handlers or defaults
    if handlers is None:
        handlers = DEFAULT_HANDLERS

    # Create service
    service = SymbolService(handlers)

    # Load symbols from config if available
    try:
        app_settings = get_app_settings()

        # Register all configured symbols
        for unified_config in app_settings.unified_symbols:
            for exchange_str, exchange_config in unified_config.exchange_mappings.items():
                try:
                    exchange = ExchangeName(exchange_str.lower())
                except ValueError:
                    continue

                # Build metadata kwargs
                metadata_kwargs = {}

                if hasattr(exchange_config, 'asset_index') and exchange_config.asset_index is not None:
                    metadata_kwargs['asset_index'] = exchange_config.asset_index
                if hasattr(exchange_config, 'symbol_id') and exchange_config.symbol_id is not None:
                    metadata_kwargs['symbol_id'] = int(exchange_config.symbol_id)

                symbol = service.create_symbol(
                    exchange_config.value,
                    exchange,
                    **metadata_kwargs
                )
                service.register_symbol(symbol)
    except Exception:
        # Config not available during testing
        pass

    return service
```

### 3.3 Create Global Access Pattern
**File**: `cyberdelta/core/symbols/global_service.py`

```python
"""Global Symbol Service Access Pattern."""

from functools import lru_cache
from typing import Any
from cyberdelta.enums.exchange_names import ExchangeName
from .factory import create_symbol_service
from .service import SymbolService
from .models import Symbol

# Global service instance
_symbol_service: SymbolService | None = None

def get_symbol_service() -> SymbolService:
    """Get or create the global symbol service."""
    global _symbol_service
    if _symbol_service is None:
        _symbol_service = create_symbol_service()
    return _symbol_service

# Convenience factory functions
@lru_cache(maxsize=1000)
def bp_symbol(value: str, **kwargs) -> Symbol[Any]:
    """Create a Backpack symbol (cached)."""
    return get_symbol_service().create_symbol(
        value, ExchangeName.BACKPACK, **kwargs
    )

@lru_cache(maxsize=1000)
def hl_symbol(value: str, **kwargs) -> Symbol[Any]:
    """Create a Hyperliquid symbol (cached)."""
    return get_symbol_service().create_symbol(
        value, ExchangeName.HYPERLIQUID, **kwargs
    )
```

### 3.4 Delete store.py
Remove store.py completely - equivalence tracking is now in the service.

### 3.5 Delete helpers.py
Remove helpers.py - not needed with new patterns.

## Testing
1. Test service creation with handlers
2. Test symbol creation through service
3. Test equivalence checking
4. Test symbol conversion
5. Test global access patterns

## Success Criteria
- [ ] Old service/store deleted
- [ ] New service implements all functionality
- [ ] Factory function works
- [ ] Global access pattern available
- [ ] Config loading works

## Next: Step 4
Update config_loader.py to work with new architecture.

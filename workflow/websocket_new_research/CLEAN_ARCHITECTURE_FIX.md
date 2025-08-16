# Clean Architecture Fix for WebSocket Circular Dependency

## Problem Analysis

The circular dependency occurs because:
```
websocket (infrastructure) → exchange models (implementation) → ExchangeAPI (base) → websocket
```

Two files in the WebSocket infrastructure layer violate the Dependency Rule:
- `ws_type_adapters.py` - imports exchange-specific models
- `ws_discriminated_unions.py` - imports exchange-specific envelopes

## Key Constraints
1. **No lazy loading** - Maintains type safety and predictability
2. **No type safety loss** - All types must be fully typed
3. **Simple for v0.0.1** - No overengineered plugin systems
4. **All exchanges have WebSocket** - No need for ultra-flexible abstraction

## Solution: Reorganize Module Structure

### The Clean Fix: Move Type Adapters to Integration Layer

Create a new "integration" layer that sits between infrastructure and implementations, allowed to import from both:

```
apis/
├── base/                    # Layer 1: Base abstractions
│   └── exchange_api.py
├── websocket/              # Layer 2: Infrastructure
│   ├── ws_protocols.py     # Only protocols, no implementations
│   └── ws_context.py       # Generic WebSocket handling
├── integration/            # Layer 3: Integration (NEW)
│   ├── __init__.py
│   ├── ws_type_adapters.py      # Moved here - can import from all layers
│   └── ws_discriminated_unions.py # Moved here - can import from all layers
├── hyperliquid/            # Layer 4: Implementations
│   └── hl_api.py
└── backpack/               # Layer 4: Implementations
    └── bp_api.py
```

### Why This Works

1. **Clean Dependencies**:
   - Base → Nothing
   - Infrastructure → Base
   - Integration → Base, Infrastructure, Implementations
   - Implementations → Base, Infrastructure

2. **No Circular Dependencies**:
   - Integration layer can import from implementations (exchange models)
   - Implementations don't import from integration
   - Infrastructure doesn't import from implementations

3. **Type Safety Maintained**:
   - All imports are explicit and at module level
   - Full type checking preserved
   - No dynamic imports or lazy loading

### Implementation Steps

#### Step 1: Create Integration Layer
```python
# cyberdelta/apis/integration/__init__.py
"""Integration layer for cross-cutting concerns between infrastructure and implementations."""

from .ws_type_adapters import WebSocketTypeAdapters
from .ws_discriminated_unions import (
    WebSocketEnvelopeUnion,
    validate_envelope_ultra_fast,
)

__all__ = [
    "WebSocketTypeAdapters",
    "WebSocketEnvelopeUnion",
    "validate_envelope_ultra_fast",
]
```

#### Step 2: Move Files
```bash
# Create integration directory
mkdir -p cyberdelta/apis/integration

# Move the problematic files
mv cyberdelta/apis/websocket/ws_type_adapters.py cyberdelta/apis/integration/
mv cyberdelta/apis/websocket/ws_discriminated_unions.py cyberdelta/apis/integration/
```

#### Step 3: Update Imports in Moved Files
```python
# cyberdelta/apis/integration/ws_type_adapters.py
# No changes needed - can still import from exchange models

# cyberdelta/apis/integration/ws_discriminated_unions.py
# No changes needed - can still import from exchange models
```

#### Step 4: Update WebSocket __init__.py
```python
# cyberdelta/apis/websocket/__init__.py
# Remove the import that causes circular dependency:
# from .ws_type_adapters import WebSocketTypeAdapters  # REMOVE THIS LINE

# Keep only infrastructure components
from .ws_context import WebSocketMessageContext
from .ws_protocols import WebSocketContextProtocol
# ... other infrastructure imports

__all__ = [
    "WebSocketMessageContext",
    "WebSocketContextProtocol",
    # Remove "WebSocketTypeAdapters" from exports
]
```

#### Step 5: Update Usage Points
When type adapters are needed, import from integration layer:
```python
# Instead of:
from cyberdelta.apis.websocket import WebSocketTypeAdapters

# Use:
from cyberdelta.apis.integration import WebSocketTypeAdapters
```

### Alternative: Exchange-Owned Adapters (Even Simpler)

If the type adapters aren't used yet (grep shows no usage), an even simpler approach:

#### Move adapters into each exchange module:
```python
# cyberdelta/apis/hyperliquid/hl_type_adapters.py
"""Hyperliquid-specific type adapters."""
from pydantic import TypeAdapter
from .models.hl_ws_discriminated_envelope import DiscriminatedHyperliquidEnvelope

class HyperliquidTypeAdapters:
    """Type adapters for Hyperliquid WebSocket messages."""
    envelope_adapter = TypeAdapter(DiscriminatedHyperliquidEnvelope)
    user_event_adapter = TypeAdapter(DiscriminatedHyperliquidUserEvent)
```

```python
# cyberdelta/apis/backpack/bp_type_adapters.py
"""Backpack-specific type adapters."""
from pydantic import TypeAdapter
from .models.bp_ws_discriminated_envelope import DiscriminatedBackpackEnvelope

class BackpackTypeAdapters:
    """Type adapters for Backpack WebSocket messages."""
    envelope_adapter = TypeAdapter(DiscriminatedBackpackEnvelope)
```

Then create a simple aggregator when needed:
```python
# cyberdelta/apis/integration/ws_adapter_aggregator.py
"""Aggregates type adapters from all exchanges."""
from cyberdelta.apis.hyperliquid.hl_type_adapters import HyperliquidTypeAdapters
from cyberdelta.apis.backpack.bp_type_adapters import BackpackTypeAdapters

class WebSocketAdapterAggregator:
    """Aggregates adapters from all exchanges."""
    hyperliquid = HyperliquidTypeAdapters
    backpack = BackpackTypeAdapters
```

### Benefits of This Approach

1. **No Circular Dependencies**: Clean layer separation
2. **Type Safety**: All imports explicit and typed
3. **Simple**: Just moving files, no complex refactoring
4. **Maintainable**: Clear where cross-cutting code lives
5. **Scalable**: Can add more exchanges easily
6. **No Magic**: No lazy loading, no dynamic imports
7. **Fast**: Same performance as current implementation

### Migration Path

1. **Phase 1** (30 minutes):
   - Create integration directory
   - Move the two problematic files
   - Update imports in websocket/__init__.py
   - Test that main.py runs

2. **Phase 2** (Optional, 30 minutes):
   - If type adapters aren't used, consider exchange-owned approach
   - Move adapters to respective exchange modules
   - Remove unused code

3. **Phase 3** (When needed):
   - Add actual usage of type adapters
   - Import from integration layer or exchange modules

### Why This is Better Than Complex Solutions

1. **Appropriate for v0.0.1**: Simple file moves, no new abstractions
2. **Maintains all type safety**: No dynamic imports or protocols
3. **Follows clean architecture**: Proper dependency direction
4. **Easy to understand**: Integration layer is a common pattern
5. **Reversible**: Can evolve to plugin system later if needed

### Summary

The cleanest fix for v0.0.1 is to:
1. Create an `integration` layer between infrastructure and implementations
2. Move `ws_type_adapters.py` and `ws_discriminated_unions.py` there
3. Update imports accordingly

This maintains type safety, avoids lazy loading, and fixes the circular dependency with minimal changes.

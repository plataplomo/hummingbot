# Circular Import Solution - Protocol & Registry Pattern

## Problem
The circular import was caused by:
```
common/types.py → backpack/bp_ws_context → backpack/__init__.py → BackpackAPI → common/ → circular!
```

The fundamental issue was that `common/types.py` needed to import concrete context classes from exchange packages, but those packages had auto-imports that imported from common.

## Solution: Protocol-Based Typing with Registry

### 1. **Protocol Definition** (`common/ws_protocols.py`)
- Created `WebSocketContextProtocol` that defines the interface for all WebSocket contexts
- Uses Python's Protocol for structural typing - no concrete imports needed
- Allows type checking without creating circular dependencies

### 2. **Registry Pattern** (`common/ws_context_registry.py`)
- Created `WebSocketContextRegistry` to manage context types dynamically
- Supports lazy registration to avoid import-time dependencies
- Exchange-specific contexts register themselves when needed

### 3. **Updated Types** (`common/types.py`)
- Changed from concrete union type to Protocol-based type:
  ```python
  # Before (causes circular import):
  WebSocketContextUnion = BackpackMessageContext | HyperliquidMessageContext
  
  # After (no circular import):
  WebSocketContextUnion = WebSocketContextProtocol
  ```

### 4. **Type-Safe Processor** (`common/ws_typed_processor.py`)
- Updated to use registry for creating contexts
- No longer imports concrete context classes
- Maintains full type safety through Protocol

### 5. **Fixed Import Paths**
- `ws_processor.py`: Import `WebSocketContextUnion` from `common.types`
- `ws_transformer.py`: Use Protocol attributes instead of isinstance checks
- `common/__init__.py`: Export `MessageHandler` and `WebSocketContextUnion`

## Benefits

1. **No Circular Imports**: Protocol-based typing breaks the dependency cycle
2. **Type Safety**: Full type checking still works through Protocol
3. **Extensibility**: New exchanges can register their contexts without modifying core code
4. **Lazy Loading**: Contexts are only imported when actually needed
5. **Clean Architecture**: Clear separation between interface (Protocol) and implementation

## Key Insight

The Protocol pattern is perfect for this use case because:
- WebSocket contexts all share the same interface (attributes like `validated_envelope`, `symbol`, etc.)
- We need type safety without concrete dependencies
- Python's structural typing through Protocol matches our needs exactly

This solution provides a clean, architectural breakthrough that resolves the circular import while maintaining full type safety and extensibility.
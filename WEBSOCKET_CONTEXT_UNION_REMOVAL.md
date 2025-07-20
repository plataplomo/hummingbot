# WebSocketContextUnion Removal

## Why We Removed It

`WebSocketContextUnion` was originally a Union type:
```python
# Original (caused circular imports)
WebSocketContextUnion = BackpackMessageContext | HyperliquidMessageContext
```

When we switched to Protocol-based design to fix circular imports, it became:
```python
# Confusing alias
WebSocketContextUnion = WebSocketContextProtocol
```

This was confusing because:
1. The name "Union" no longer represented what it actually was (a Protocol)
2. It was just an unnecessary alias adding no value
3. It made the code less clear about what type was actually being used

## What We Changed

### 1. Removed the Alias
- Deleted `WebSocketContextUnion` from `common/types.py`
- Removed it from `common/__init__.py` exports

### 2. Updated All Usage
Replaced all occurrences of `WebSocketContextUnion` with `WebSocketContextProtocol`:
- 6 files updated
- 37 total replacements
- Imports changed from `common.types` to `websocket.ws_protocols`

### 3. Cleaner Code
Before:
```python
from cyberdelta.apis.common.types import WebSocketContextUnion

def process(context: WebSocketContextUnion) -> None:
    # Confusing - is this a Union or Protocol?
```

After:
```python
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol

def process(context: WebSocketContextProtocol) -> None:
    # Clear - this is a Protocol
```

## Benefits

1. **Clarity**: The type name now accurately reflects what it is
2. **Simplicity**: No unnecessary aliases
3. **Direct imports**: Import the Protocol directly from where it's defined
4. **No backwards compatibility debt**: Clean slate for future development

## Migration Guide

If you have code using `WebSocketContextUnion`:

```python
# Old
from cyberdelta.apis.common.types import WebSocketContextUnion

# New
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
```

Then replace all uses of `WebSocketContextUnion` with `WebSocketContextProtocol`.

## Summary

This cleanup removes a confusing remnant from our circular import fix journey. The codebase is now cleaner and more maintainable with accurate type names that reflect their actual implementation.
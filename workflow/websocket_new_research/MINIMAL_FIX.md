# Minimal Fix for WebSocket Circular Dependency

## The Absolute Simplest Solution

Since grep shows `WebSocketTypeAdapters` methods are **not actually used anywhere** in the codebase yet, the minimal fix is:

### Option A: Remove Unused Code (5 minutes)

1. **Remove from websocket/__init__.py**:
```python
# cyberdelta/apis/websocket/__init__.py
# REMOVE these lines:
# from .ws_type_adapters import WebSocketTypeAdapters
# Remove "WebSocketTypeAdapters" from __all__
```

2. **Delete or rename the problematic files**:
```bash
# Either delete them:
rm cyberdelta/apis/websocket/ws_type_adapters.py
rm cyberdelta/apis/websocket/ws_discriminated_unions.py

# Or rename them to mark as unused:
mv cyberdelta/apis/websocket/ws_type_adapters.py cyberdelta/apis/websocket/ws_type_adapters.py.unused
mv cyberdelta/apis/websocket/ws_discriminated_unions.py cyberdelta/apis/websocket/ws_discriminated_unions.py.unused
```

3. **Done!** The circular dependency is gone.

### Option B: Keep Files but Break Import Chain (2 minutes)

If you want to keep the files for future use:

1. **Remove from websocket/__init__.py**:
```python
# cyberdelta/apis/websocket/__init__.py
# COMMENT OUT:
# from .ws_type_adapters import WebSocketTypeAdapters  # TODO: Re-enable when needed
```

2. **Add a comment to the files**:
```python
# cyberdelta/apis/websocket/ws_type_adapters.py
"""
NOTICE: This file is currently not imported to avoid circular dependencies.
When needed, import directly:
  from cyberdelta.apis.websocket.ws_type_adapters import WebSocketTypeAdapters
Or move to integration layer as described in CLEAN_ARCHITECTURE_FIX.md
"""
```

### Why This Works

1. **Code not used**: No functionality is lost
2. **Immediate fix**: Unblocks development NOW
3. **Type safe**: No dynamic imports or type loss
4. **Reversible**: Can add back when actually needed
5. **YAGNI**: Don't keep unused code that causes problems

### When You Actually Need Type Adapters

When you need them (probably when implementing WebSocket message handling), choose one of:

1. **Integration Layer Approach** (from CLEAN_ARCHITECTURE_FIX.md):
   - Move to `apis/integration/` directory
   - Import from there when needed

2. **Exchange-Owned Approach**:
   - Each exchange creates its own type adapters
   - No central file with all exchange imports

3. **Just-In-Time Creation**:
   - Create type adapters where they're used
   - Don't pre-create them centrally

### Verification

After removing the imports:
```bash
# This should work:
python main.py --help

# Verify no circular imports:
python -c "from cyberdelta.apis.base.exchange_api import ExchangeAPI; print('Success!')"
```

## Decision Matrix

| Solution | Time | Risk | Type Safety | Complexity |
|----------|------|------|-------------|------------|
| **Remove unused code** | 5 min | None | ✅ Full | Trivial |
| **Comment out imports** | 2 min | None | ✅ Full | Trivial |
| **Integration layer** | 30 min | Low | ✅ Full | Simple |
| **Exchange-owned** | 1 hour | Low | ✅ Full | Moderate |
| **Plugin system** | Days | Medium | ✅ Full | Complex |

## Recommendation

For v0.0.1, since the code isn't used:
1. **Remove the imports from websocket/__init__.py** (2 minutes)
2. **Continue development**
3. **When you need type adapters, implement the integration layer approach**

This follows the principle: "The best code is no code" - don't maintain unused code that causes problems.

## One-Line Fix

The absolute minimal fix that solves your problem RIGHT NOW:

```bash
# Remove the problematic import
sed -i '/from \.ws_type_adapters import WebSocketTypeAdapters/d' cyberdelta/apis/websocket/__init__.py
sed -i '/"WebSocketTypeAdapters"/d' cyberdelta/apis/websocket/__init__.py
```

Done! The circular dependency is gone.

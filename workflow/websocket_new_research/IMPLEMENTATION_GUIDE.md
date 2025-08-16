# WebSocket Circular Dependency - Implementation Guide

## Quick Fix Implementation (2 minutes)

### Step 1: Remove the Problematic Import

Edit `/cyberdelta/apis/websocket/__init__.py`:

```python
# BEFORE:
from .ws_type_adapters import WebSocketTypeAdapters

__all__ = [
    "ExchangeName",
    "WebSocketContextFactory",
    "WebSocketContextProtocol",
    "WebSocketContextRegistry",
    "WebSocketMessageContext",
    "WebSocketRateLimiter",
    "WebSocketRegistryFactory",
    "WebSocketTypeAdapters",  # Remove this
]

# AFTER:
# Line removed

__all__ = [
    "ExchangeName",
    "WebSocketContextFactory",
    "WebSocketContextProtocol",
    "WebSocketContextRegistry",
    "WebSocketMessageContext",
    "WebSocketRateLimiter",
    "WebSocketRegistryFactory",
    # "WebSocketTypeAdapters" removed
]
```

### Step 2: Verify the Fix

```bash
# Test that main.py works
python main.py --help

# Test that imports work
python -c "from cyberdelta.apis.base.exchange_api import ExchangeAPI; print('✅ No circular dependency!')"

# Run type checkers to ensure no issues
mypy cyberdelta/apis/
ruff check cyberdelta/apis/
pyright cyberdelta/apis/
```

### Step 3: Document the Change

Add a comment to the unused files:

```python
# cyberdelta/apis/websocket/ws_type_adapters.py
"""
⚠️ NOTICE: This file is currently not imported to avoid circular dependencies.

The WebSocket type adapters are not yet used in the codebase.
When needed, consider:
1. Moving to apis/integration/ layer
2. Creating exchange-specific adapters
3. Importing directly where needed

See: workflow/websocket_new_research/CLEAN_ARCHITECTURE_FIX.md
"""
```

## Future Implementation (When Type Adapters Are Needed)

### Option A: Integration Layer (Recommended)

When you need the type adapters:

1. **Create integration layer**:
```bash
mkdir -p cyberdelta/apis/integration
```

2. **Move files**:
```bash
mv cyberdelta/apis/websocket/ws_type_adapters.py cyberdelta/apis/integration/
mv cyberdelta/apis/websocket/ws_discriminated_unions.py cyberdelta/apis/integration/
```

3. **Create integration __init__.py**:
```python
# cyberdelta/apis/integration/__init__.py
"""Integration layer for cross-cutting concerns."""

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

4. **Use from integration layer**:
```python
# When you need type adapters
from cyberdelta.apis.integration import WebSocketTypeAdapters

# Validate a message
validated = WebSocketTypeAdapters.validate_envelope_python(data)
```

### Option B: Exchange-Specific Adapters

1. **Create in each exchange**:
```python
# cyberdelta/apis/hyperliquid/hl_type_adapters.py
from pydantic import TypeAdapter
from .models.hl_ws_discriminated_envelope import DiscriminatedHyperliquidEnvelope

class HyperliquidTypeAdapters:
    envelope = TypeAdapter(DiscriminatedHyperliquidEnvelope)

    @classmethod
    def validate(cls, data: dict):
        return cls.envelope.validate_python(data)
```

2. **Use directly**:
```python
from cyberdelta.apis.hyperliquid.hl_type_adapters import HyperliquidTypeAdapters

validated = HyperliquidTypeAdapters.validate(data)
```

## Testing the Solution

### Unit Test for Circular Dependencies
```python
# tests/test_no_circular_imports.py
import pytest
import sys
import importlib

def test_no_circular_dependency():
    """Ensure no circular imports in the API layer."""
    # Clear any cached imports
    modules_to_test = [
        'cyberdelta.apis.base.exchange_api',
        'cyberdelta.apis.websocket',
        'cyberdelta.apis.hyperliquid',
        'cyberdelta.apis.backpack',
    ]

    for module in modules_to_test:
        if module in sys.modules:
            del sys.modules[module]

    # Try importing - should not raise ImportError
    try:
        import cyberdelta.apis.base.exchange_api
        import cyberdelta.apis.websocket
        assert True, "No circular import detected"
    except ImportError as e:
        pytest.fail(f"Circular import detected: {e}")
```

### Performance Test (When Implemented)
```python
# tests/test_ws_adapter_performance.py
import time
from cyberdelta.apis.integration import WebSocketTypeAdapters

def test_adapter_performance():
    """Ensure type adapters are performant."""
    sample_data = {
        "channel": "userEvents",
        "data": {...}  # Sample data
    }

    start = time.perf_counter()
    for _ in range(10000):
        WebSocketTypeAdapters.validate_envelope_python(sample_data)
    elapsed = time.perf_counter() - start

    # Should validate 10k messages in under 1 second
    assert elapsed < 1.0, f"Validation too slow: {elapsed:.2f}s"
```

## Rollback Plan

If something goes wrong:

1. **Restore the import** (not recommended):
```python
# cyberdelta/apis/websocket/__init__.py
from .ws_type_adapters import WebSocketTypeAdapters  # Re-add if needed
```

2. **Use the workaround** (current state):
- Keep __init__.py files empty in exchange modules
- Use direct imports

## Monitoring

After implementation:

1. **Check for import errors**:
```bash
# Should show no errors
python -c "import cyberdelta.apis"
```

2. **Run existing tests**:
```bash
pytest tests/
```

3. **Check type safety**:
```bash
mypy --strict cyberdelta/
```

## FAQ

**Q: Why not fix it "properly" now?**
A: The code isn't used. Implementing unused abstractions is premature optimization.

**Q: What if we need it tomorrow?**
A: The integration layer approach takes 30 minutes to implement.

**Q: Is this technical debt?**
A: No, keeping unused code that causes problems is technical debt. Removing it reduces debt.

**Q: Will this break anything?**
A: No, grep confirms the code isn't used anywhere.

**Q: What about type safety?**
A: Fully maintained. No dynamic imports or lazy loading.

## Summary

1. **Now**: Remove the import (2 minutes) ✅
2. **Later**: Implement integration layer when needed
3. **Never**: Keep unused code that causes problems

This approach:
- Fixes the immediate problem
- Maintains type safety
- Follows YAGNI principle
- Keeps codebase clean
- Documents future path

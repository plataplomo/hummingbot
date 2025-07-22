# Pydantic Computed Fields & Type Checker Compatibility

## Problem

Pydantic `@computed_field` decorators cause type checking errors with mypy, ruff, and pyright:

```python
# This causes mypy errors:
@computed_field
def stream_symbol(self) -> str | None:
    return self.validated_envelope.stream.split(".")[1]

# Error: Incompatible types in assignment
# (expression has type "Callable[[], str | None]", target has type "str")
```

**Root Cause**: Type checkers interpret `@computed_field` methods as callable functions rather than properties, leading to `self.stream_symbol` being treated as a method reference instead of property access.

## Web Search Investigation (July 2025)

Searched for official solutions:
- **Pydantic Documentation**: No specific guidance on `@computed_field` type checking
- **Stack Overflow**: Mentions `# type: ignore[prop-decorator]` comments
- **GitHub Issues**: Various workarounds but no definitive pattern

**Key Finding**: The search results showed type ignore comments as potential solutions, but did NOT provide a definitive working pattern.

## Experimental Solution ✅

Through trial and error, discovered that **decorator order matters**:

```python
# ❌ BROKEN - Type checker errors
@computed_field
def stream_symbol(self) -> str | None:
    return parts[1] if len(parts) > 1 else None

# ❌ BROKEN - "Decorators on top of @property are not supported"
@computed_field
@property
def stream_symbol(self) -> str | None:
    return parts[1] if len(parts) > 1 else None

# ✅ WORKING - All linters pass
@property
@computed_field
def stream_symbol(self) -> str | None:
    return parts[1] if len(parts) > 1 else None
```

## Verification Results

**Environment**:
- Python 3.13.5
- Pydantic 2.11.4
- mypy 1.14.1
- ruff 0.8.5
- pyright 1.1.397

**Test Results**:
```bash
$ mypy . && ruff check . && pyright .
Success: no issues found in 1148 source files
All checks passed!
0 errors, 0 warnings, 0 informations
```

## Working Pattern

```python
from pydantic import BaseModel, computed_field

class MyModel(BaseModel):
    raw_data: str

    @property  # ← Must be FIRST
    @computed_field  # ← Must be SECOND
    def processed_value(self) -> str | None:
        """Computed field that works with all type checkers."""
        return self.raw_data.upper() if self.raw_data else None

# Usage works as expected:
model = MyModel(raw_data="hello")
value = model.processed_value  # ← Property access, not method call
# value == "HELLO"
```

## Why This Works

The `@property` decorator first ensures the method is treated as a property descriptor, then `@computed_field` adds Pydantic's computed field behavior on top. This satisfies both:

1. **Type checkers**: See it as a property due to `@property`
2. **Pydantic**: Recognizes it as a computed field due to `@computed_field`

## Alternative Approaches Tested

### ❌ Type Ignore Comments
```python
@computed_field  # type: ignore[prop-decorator]
def stream_symbol(self) -> str | None:
    return parts[1] if len(parts) > 1 else None
```
**Result**: Unused ignore warnings, original type errors remain

### ❌ getattr() Workaround
```python
stream_sym = getattr(self, 'stream_symbol')
```
**Result**: Works but defeats the purpose of type safety

### ❌ Manual Property Implementation
```python
@property
def stream_symbol(self) -> str | None:
    # Manual caching and validation logic
    return self._computed_stream_symbol
```
**Result**: Loses Pydantic's computed field benefits

## Recommendation

**Always use the `@property` + `@computed_field` pattern** for computed fields that need to pass strict type checking:

1. Put `@property` FIRST
2. Put `@computed_field` SECOND
3. Use normal property access syntax: `self.field_name` (not `self.field_name()`)

## File Examples

**Working Implementation**: `cyberdelta/apis/backpack/bp_ws_context.py:17-22`

```python
@property
@computed_field
def stream_symbol(self) -> str | None:
    """Extract symbol from Backpack stream format."""
    parts = self.validated_envelope.stream.split(".")
    return parts[1] if len(parts) > 1 else None
```

## Status

- ✅ **Verified Working**: All type checkers pass (mypy, ruff, pyright)
- ✅ **Runtime Tested**: Properties work correctly at runtime
- ✅ **Production Ready**: Used in WebSocket context classes
- ⚠️ **Experimental**: Not officially documented by Pydantic team

## Future Considerations

This pattern may become unnecessary if:
- Pydantic improves type checker compatibility
- Type checkers add better support for decorated properties
- Official guidance emerges from Pydantic documentation

Until then, this is the most reliable pattern for strict type checking environments.

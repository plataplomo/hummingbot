# 02. Duplicate Base Protocols - Deep Code Research Report

## Executive Summary
The base protocol definitions are duplicated between Backpack and Hyperliquid exchanges with nearly identical implementations. The only significant difference is the type hint for the `response` parameter in `ResponseHandlerProtocol`. This duplication affects 40+ files across both exchange implementations.

## File Locations

### 1. Backpack: `/cyberdelta/apis/backpack/protocols/base_protocols.py`
- **Lines**: 78
- **Protocols**: 3 base protocols
- **Purpose**: Base protocols for Backpack API components

### 2. Hyperliquid: `/cyberdelta/apis/hyperliquid/protocols/base_protocols.py`
- **Lines**: 120 (more verbose documentation)
- **Protocols**: 3 base protocols (identical)
- **Purpose**: Base protocols for Hyperliquid API components

## Protocol Analysis

### 1. MapperProtocol
**Status**: 100% identical functionality

Methods:
- `parse_decimal_safely()` - Parse decimal values with default
- `normalize_symbol()` - Convert to internal format
- `denormalize_symbol()` - Convert to exchange format
- `timestamp_ms_to_datetime()` - Convert timestamps

**Differences**: Only documentation verbosity differs

### 2. RequestBuilderProtocol
**Status**: 100% identical

Methods:
- `build_request()` - Generic request builder

**Differences**: Only documentation differs

### 3. ResponseHandlerProtocol
**Status**: Functionally identical with minor type difference

Methods:
- `handle_response()` - Process API responses

**Key Difference**:
- Backpack: `response: ParsedJsonResponse` (custom type)
- Hyperliquid: `response: dict[str, object]` (standard dict)

## Import Analysis

### Files Using These Protocols
- **Backpack**: 20+ files including services, handlers, mappers
- **Hyperliquid**: 20+ files with identical pattern
- **Total Impact**: 40+ files across both exchanges

### Import Patterns
Both exchanges import from their respective protocol modules:
```python
# Backpack files
from cyberdelta.apis.backpack.protocols.base_protocols import MapperProtocol

# Hyperliquid files
from cyberdelta.apis.hyperliquid.protocols.base_protocols import MapperProtocol
```

## Code Duplication Impact

### Exact Duplications
1. **MapperProtocol**: 4 identical methods
2. **RequestBuilderProtocol**: 1 identical method
3. **ResponseHandlerProtocol**: 1 nearly identical method

### Maintenance Issues
1. **Double Updates**: Any protocol change must be made in both files
2. **Drift Risk**: Protocols may diverge over time
3. **Testing Overhead**: Need to test both implementations
4. **Documentation Duplication**: Same concepts documented twice

## Architecture Analysis

### Current Structure
```
/cyberdelta/apis/
├── backpack/
│   └── protocols/
│       ├── base_protocols.py      # Duplicate
│       ├── mapper_protocols.py
│       ├── builder_protocols.py
│       └── handler_protocols.py
└── hyperliquid/
    └── protocols/
        ├── base_protocols.py      # Duplicate
        ├── mapper_protocols.py
        ├── builder_protocols.py
        └── handler_protocols.py
```

### Protocol Inheritance
Both exchanges have additional protocol files that import and extend these base protocols:
- `mapper_protocols.py` - Specific mapper interfaces
- `builder_protocols.py` - Specific builder interfaces
- `handler_protocols.py` - Specific handler interfaces

## Type System Impact

### ParsedJsonResponse Type Issue
The only real difference is Backpack's use of `ParsedJsonResponse` type:
```python
# Backpack imports this custom type
from cyberdelta.utils.typing import ParsedJsonResponse

# Hyperliquid uses standard dict
response: dict[str, object]
```

This suggests Backpack has stricter typing, but both are functionally equivalent.

## Recommendations

### Immediate Actions
1. **Create Common Base Module**: `/cyberdelta/apis/base/protocols/base_protocols.py`
2. **Reconcile Type Differences**: Use Union type or make ParsedJsonResponse generic
3. **Update All Imports**: Modify 40+ files to import from common location

### Migration Strategy

#### Step 1: Create Common Module
```python
# /cyberdelta/apis/base/protocols/base_protocols.py
from typing import Protocol, runtime_checkable, Union
from cyberdelta.utils.typing import ParsedJsonResponse

@runtime_checkable
class ResponseHandlerProtocol(Protocol):
    def handle_response(
        self,
        response: Union[ParsedJsonResponse, dict[str, object]],
        status_code: int,
        headers: dict[str, str],
        context: str
    ) -> object:
        ...
```

#### Step 2: Update Import Paths
Replace in all files:
```python
# Old
from cyberdelta.apis.backpack.protocols.base_protocols import MapperProtocol
from cyberdelta.apis.hyperliquid.protocols.base_protocols import MapperProtocol

# New
from cyberdelta.apis.base.protocols.base_protocols import MapperProtocol
```

#### Step 3: Remove Duplicate Files
Delete both:
- `/cyberdelta/apis/backpack/protocols/base_protocols.py`
- `/cyberdelta/apis/hyperliquid/protocols/base_protocols.py`

### Alternative Approach
Keep exchange-specific protocol files but have them re-export from common base:
```python
# /cyberdelta/apis/backpack/protocols/base_protocols.py
from cyberdelta.apis.base.protocols.base_protocols import *

# Exchange-specific additions can go here if needed
```

## Benefits of Consolidation

### Code Quality
- **Reduction**: ~200 lines of duplicate code eliminated
- **Single Source of Truth**: One place to update protocols
- **Consistent Behavior**: Guaranteed identical interfaces

### Development Efficiency
- **Faster Updates**: Change once, apply everywhere
- **Reduced Testing**: Test base protocols once
- **Clear Architecture**: Obvious where base protocols live

### Risk Mitigation
- **No Drift**: Protocols can't diverge between exchanges
- **Type Safety**: Consistent typing across exchanges
- **Easier Onboarding**: New developers see clear protocol hierarchy

## Risk Analysis

### Migration Risks
- **Low Risk**: Protocols are interfaces only (no implementation)
- **Type Compatibility**: Need to ensure Union type works everywhere
- **Import Updates**: Large number of files to update (automation recommended)

### Compatibility Concerns
- **Runtime Checkable**: Both use `@runtime_checkable` decorator
- **Protocol Variance**: Protocols are structurally compatible
- **No Breaking Changes**: Migration preserves all existing functionality

## Conclusion
The duplicate base protocols create unnecessary maintenance overhead without providing any exchange-specific value. Since these are pure interface definitions with identical functionality, consolidating them into a common module would significantly improve code maintainability while preserving all existing functionality. The only challenge is reconciling the minor type difference in ResponseHandlerProtocol, which can be easily solved using Union types.

# 09. Duplicate Base Protocols - Deep Resolution Analysis & Implementation Plan

## Executive Summary
**Status**: CRITICAL UNRESOLVED ISSUE - Duplication Still Exists
**Impact**: 13 files, 100% code duplication across 3 base protocols
**Resolution Time**: 2-3 hours
**Risk Level**: LOW (protocols are interfaces only)
**Last Updated**: 2025-07-30

This document provides a comprehensive analysis and resolution plan for eliminating duplicate base protocol definitions between Backpack and Hyperliquid exchanges.

### Current State Update (2025-07-30)
- ✗ Common base module `/cyberdelta/apis/base/protocols/` does NOT exist
- ✗ Duplicate base_protocols.py files still exist in both exchanges
- ✗ No abstract mapper protocols have been created
- ⚠️ MapperProtocol now has only 2 methods (not 4 as originally documented)
- ⚠️ Method parameters have changed: balance mapper methods now use `Symbol` type instead of `str`

## Deep Code Research Findings

### Current State Analysis

#### File Locations & Metrics (Updated 2025-07-30)
1. **Backpack**: `/cyberdelta/apis/backpack/protocols/base_protocols.py`
   - **Size**: 73 lines
   - **Protocols**: 3 base protocols
   - **Documentation**: Minimal
   - **Imports**: `ParsedJsonResponse` from `cyberdelta.utils.typing`
   - **__all__ exports**: Present

2. **Hyperliquid**: `/cyberdelta/apis/hyperliquid/protocols/base_protocols.py`
   - **Size**: 101 lines
   - **Protocols**: 3 identical base protocols
   - **Documentation**: Verbose with full docstrings
   - **Imports**: No custom types (uses `dict[str, object]`)
   - **__all__ exports**: Missing

#### Protocol Duplication Analysis

##### 1. MapperProtocol - 100% IDENTICAL (Updated)
**Methods** (Only 2 methods now, not 4):
- `parse_decimal_safely(value, default)` - Decimal parsing with fallback
- `timestamp_ms_to_datetime(timestamp_ms)` - Timestamp conversion

**Missing Methods** (removed from current implementation):
- ~~`normalize_symbol(symbol)`~~ - Not present
- ~~`denormalize_symbol(symbol)`~~ - Not present

**Status**: Absolutely identical implementation and signatures
**Documentation**: Hyperliquid has more verbose docstrings

##### 2. RequestBuilderProtocol - 100% IDENTICAL
**Methods**:
- `build_request(*args, **kwargs)` - Generic request builder

**Status**: Absolutely identical implementation and signatures

##### 3. ResponseHandlerProtocol - FUNCTIONALLY IDENTICAL with Type Variance
**Methods**:
- `handle_response(response, status_code, headers, context)`

**Type Difference**:
- **Backpack**: `response: ParsedJsonResponse`
  - Where `ParsedJsonResponse = dict[str, Any] | list[Any] | str`
- **Hyperliquid**: `response: dict[str, object]`

**Analysis**:
- Backpack supports more response types (list, string)
- Hyperliquid assumes dict-only responses
- Both are functionally equivalent for dict responses

### Refactoring Updates Since Original Document

#### Symbol Type Migration
The codebase has undergone a refactoring where string parameters for asset symbols have been replaced with a `Symbol` type:
- **Old**: `asset_symbol: str`
- **New**: `asset_symbol: Symbol`

This affects mapper protocols and request builders across both exchanges, improving type safety but not addressing the core duplication issue.

### Usage Pattern Analysis

#### Inheritance Patterns
Both exchanges use identical multiple inheritance:
```python
class BalanceMapperProtocol(MapperProtocol, Protocol):
    # Exchange-specific methods
```

#### Protocol Inheritance Statistics
- **Backpack**: 13 mapper protocols inherit from `MapperProtocol`
  - BalanceMapperProtocol, PositionMapperProtocol, AccountSummaryMapperProtocol
  - TransactionMapperProtocol, TransferMapperProtocol, OrderMapperProtocol
  - MarketDataMapperProtocol, TickerMapperProtocol, OrderBookMapperProtocol
  - TradeMapperProtocol, CandleMapperProtocol, FundingRateMapperProtocol
  - MarketMapperProtocol

- **Hyperliquid**: 17 mapper protocols inherit from `MapperProtocol`
  - BalanceMapperProtocol, PositionMapperProtocol, AccountSummaryMapperProtocol
  - OrderMapperProtocol, TickerMapperProtocol, OrderBookMapperProtocol
  - TradeMapperProtocol, CandleMapperProtocol, FundingRateMapperProtocol
  - MarketMapperProtocol, MarketDataMapperProtocol, OrderResponseMapperProtocol
  - TransactionMapperProtocol, TradingEnumMapperProtocol, PriceTickerMapperProtocol
  - HistoricalDataMapperProtocol, MarketMetadataMapperProtocol

#### Import Dependency Analysis
**Files importing Backpack base protocols** (6 files):
- `cyberdelta/apis/backpack/utils/component_registry.py`
- `cyberdelta/apis/backpack/protocols/builder_protocols.py`
- `cyberdelta/apis/backpack/protocols/handler_protocols.py`
- `cyberdelta/apis/backpack/protocols/mapper_protocols.py`
- `cyberdelta/apis/backpack/protocols/__init__.py`

**Files importing Hyperliquid base protocols** (7 files):
- `tests/unit/apis/hyperliquid/protocols/test_protocol_compliance.py`
- `cyberdelta/apis/hyperliquid/utils/component_registry.py`
- `cyberdelta/apis/hyperliquid/protocols/builder_protocols.py`
- `cyberdelta/apis/hyperliquid/protocols/handler_protocols.py`
- `cyberdelta/apis/hyperliquid/protocols/mapper_protocols.py`
- `cyberdelta/apis/hyperliquid/protocols/__init__.py`

**Total Impact**: 13 files directly importing from duplicate base protocols

## ParsedJsonResponse vs dict[str, object] Analysis

### Type Definition Research
```python
# From cyberdelta/utils/typing.py
ParsedJsonResponse = dict[str, Any] | list[Any] | str
```

### Usage Pattern Analysis
- **ParsedJsonResponse**: Supports dict, list, and string responses
- **dict[str, object]**: Only supports dict responses
- **Real-world usage**: 103+ files use ParsedJsonResponse across the codebase
- **API Reality**: Most API responses are dicts, but some endpoints return lists or strings

### Type Compatibility
- `dict[str, object]` is a subset of `ParsedJsonResponse`
- `ParsedJsonResponse` is more flexible and accurate to real API behavior
- Consolidation should use `ParsedJsonResponse` for maximum compatibility

## Resolution Architecture

### Option 1: Common Base Module (RECOMMENDED)
Create a single authoritative base protocol module that both exchanges import from.

#### Directory Structure
```
/cyberdelta/apis/base/protocols/
├── __init__.py
└── base_protocols.py          # Single source of truth
```

#### Implementation Strategy
1. **Create Common Module**: `/cyberdelta/apis/base/protocols/base_protocols.py`
2. **Reconcile Type Differences**: Use `ParsedJsonResponse` for maximum compatibility
3. **Update All Imports**: Modify 13 files to import from common location
4. **Delete Duplicate Files**: Remove both exchange-specific base protocol files

### Option 2: Re-export Pattern (ALTERNATIVE)
Keep exchange-specific files but have them re-export from common base.

```python
# /cyberdelta/apis/backpack/protocols/base_protocols.py
from cyberdelta.apis.base.protocols.base_protocols import *

# Exchange-specific additions can go here if needed
```

### Option 3: Template Inheritance (COMPLEX)
Use generic types to handle response type differences.

## Recommended Implementation Plan

### Phase 1: Create Common Base Module (30 minutes)

#### 1.1 Create Directory Structure
```bash
mkdir -p /cyberdelta/apis/base/protocols
```

#### 1.2 Create Base Protocol File
**File**: `/cyberdelta/apis/base/protocols/base_protocols.py`

```python
"""Common base protocol definitions for all API components.

These protocols define the expected interfaces for different component types
across all exchanges, providing type safety and documentation consistency.
"""

from datetime import datetime
from decimal import Decimal
from typing import Protocol, runtime_checkable

from cyberdelta.utils.typing import ParsedJsonResponse


__all__ = [
    "MapperProtocol",
    "RequestBuilderProtocol",
    "ResponseHandlerProtocol",
]


@runtime_checkable
class MapperProtocol(Protocol):
    """Base protocol for all mapper components.

    Mappers are responsible for transforming data between external API formats
    and internal domain models. This protocol provides common utility methods
    that all mappers must implement across all exchanges.
    """

    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None, default: Decimal = Decimal(0)
    ) -> Decimal:
        """Safely parse decimal values with fallback.

        Args:
            value: The value to parse as a decimal
            default: Default value to return if parsing fails

        Returns:
            Parsed decimal value or default
        """
        ...

    # NOTE: normalize_symbol and denormalize_symbol methods
    # were present in the original plan but are NOT in the current
    # implementation. Consider if these should be added or if
    # Symbol type migration makes them unnecessary.

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to datetime.

        Args:
            timestamp_ms: Millisecond timestamp

        Returns:
            Datetime object or None if timestamp is None
        """
        ...


@runtime_checkable
class RequestBuilderProtocol(Protocol):
    """Base protocol for all request builder components.

    Request builders construct properly formatted API requests including
    parameters, headers, and payloads for any exchange API.
    """

    def build_request(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Build request payload.

        Args:
            *args: Positional arguments for request building
            **kwargs: Keyword arguments for request building

        Returns:
            Dictionary containing the request payload
        """
        ...


@runtime_checkable
class ResponseHandlerProtocol(Protocol):
    """Base protocol for all response handler components.

    Response handlers process raw API responses, perform validation,
    and convert them to appropriate raw model types for any exchange.
    """

    def handle_response(
        self,
        response: ParsedJsonResponse,
        status_code: int,
        headers: dict[str, str],
        context: str
    ) -> object:
        """Handle API response.

        Args:
            response: Raw response data from API (dict, list, or string)
            status_code: HTTP status code
            headers: Response headers
            context: Context information about the request

        Returns:
            Processed response object
        """
        ...
```

#### 1.3 Create __init__.py
**File**: `/cyberdelta/apis/base/protocols/__init__.py`

```python
"""Base protocol definitions for all API components."""

from cyberdelta.apis.base.protocols.base_protocols import (
    MapperProtocol,
    RequestBuilderProtocol,
    ResponseHandlerProtocol,
)

__all__ = [
    "MapperProtocol",
    "RequestBuilderProtocol",
    "ResponseHandlerProtocol",
]
```

### Phase 2: Update All Import Statements (60 minutes)

#### 2.1 Automated Import Update Script
```python
#!/usr/bin/env python3
"""Update base protocol imports to use common module."""

import os
import re
from pathlib import Path

def update_file_imports(filepath: Path) -> bool:
    """Update imports in a single file."""
    with open(filepath, 'r') as f:
        content = f.read()

    original_content = content

    # Replace Backpack imports
    content = re.sub(
        r'from cyberdelta\.apis\.backpack\.protocols\.base_protocols import',
        'from cyberdelta.apis.base.protocols.base_protocols import',
        content
    )

    # Replace Hyperliquid imports
    content = re.sub(
        r'from cyberdelta\.apis\.hyperliquid\.protocols\.base_protocols import',
        'from cyberdelta.apis.base.protocols.base_protocols import',
        content
    )

    if content != original_content:
        with open(filepath, 'w') as f:
            f.write(content)
        return True
    return False

# Files to update
files_to_update = [
    # Backpack files
    "cyberdelta/apis/backpack/utils/component_registry.py",
    "cyberdelta/apis/backpack/protocols/builder_protocols.py",
    "cyberdelta/apis/backpack/protocols/handler_protocols.py",
    "cyberdelta/apis/backpack/protocols/mapper_protocols.py",
    "cyberdelta/apis/backpack/protocols/__init__.py",

    # Hyperliquid files
    "tests/unit/apis/hyperliquid/protocols/test_protocol_compliance.py",
    "cyberdelta/apis/hyperliquid/utils/component_registry.py",
    "cyberdelta/apis/hyperliquid/protocols/builder_protocols.py",
    "cyberdelta/apis/hyperliquid/protocols/handler_protocols.py",
    "cyberdelta/apis/hyperliquid/protocols/mapper_protocols.py",
    "cyberdelta/apis/hyperliquid/protocols/__init__.py",
]

updated_count = 0
for file_path in files_to_update:
    full_path = Path(file_path)
    if full_path.exists():
        if update_file_imports(full_path):
            print(f"✓ Updated: {file_path}")
            updated_count += 1
        else:
            print(f"✗ No changes: {file_path}")
    else:
        print(f"✗ File not found: {file_path}")

print(f"\nTotal files updated: {updated_count}")
```

#### 2.2 Manual Import Updates
For each of the 13 files, change:

**Old (Backpack)**:
```python
from cyberdelta.apis.backpack.protocols.base_protocols import (
    MapperProtocol,
    RequestBuilderProtocol,
    ResponseHandlerProtocol,
)
```

**Old (Hyperliquid)**:
```python
from cyberdelta.apis.hyperliquid.protocols.base_protocols import (
    MapperProtocol,
    RequestBuilderProtocol,
    ResponseHandlerProtocol,
)
```

**New (Both)**:
```python
from cyberdelta.apis.base.protocols.base_protocols import (
    MapperProtocol,
    RequestBuilderProtocol,
    ResponseHandlerProtocol,
)
```

### Phase 3: Remove Duplicate Files (15 minutes)

#### 3.1 Delete Original Files
```bash
rm /cyberdelta/apis/backpack/protocols/base_protocols.py
rm /cyberdelta/apis/hyperliquid/protocols/base_protocols.py
```

#### 3.2 Update __init__.py Files
Remove base protocol re-exports from:
- `/cyberdelta/apis/backpack/protocols/__init__.py`
- `/cyberdelta/apis/hyperliquid/protocols/__init__.py`

### Phase 4: Verification & Testing (30 minutes)

#### 4.1 Static Type Checking
```bash
mypy cyberdelta/apis/
pyright cyberdelta/apis/
ruff check cyberdelta/apis/
```

#### 4.2 Import Testing
```python
# Test script to verify imports work
try:
    from cyberdelta.apis.base.protocols.base_protocols import (
        MapperProtocol,
        RequestBuilderProtocol,
        ResponseHandlerProtocol
    )
    print("✓ Common base protocols import successfully")

    # Test inheritance
    class TestMapper(MapperProtocol):
        @staticmethod
        def parse_decimal_safely(value, default):
            return default
        @staticmethod
        def normalize_symbol(symbol):
            return symbol
        @staticmethod
        def denormalize_symbol(symbol):
            return symbol
        @staticmethod
        def timestamp_ms_to_datetime(timestamp_ms):
            return None

    print("✓ Protocol inheritance works correctly")

except ImportError as e:
    print(f"✗ Import failed: {e}")
```

#### 4.3 Run Existing Tests
```bash
pytest tests/unit/apis/hyperliquid/protocols/test_protocol_compliance.py -v
pytest tests/unit/apis/ -k "protocol" -v
```

## Risk Analysis & Mitigation

### Low Risk Factors
1. **Interface-Only Changes**: Protocols define interfaces, not implementations
2. **No Runtime Logic**: No business logic changes required
3. **Type Compatible**: ParsedJsonResponse is superset of dict[str, object]
4. **Automated Migration**: Script handles most changes
5. **Static Verification**: Type checkers catch import issues immediately

### Potential Issues & Solutions

#### Issue 1: Type Checker Errors
**Problem**: Some code may expect dict[str, object] specifically
**Solution**: ParsedJsonResponse includes dict[str, Any] which is compatible
**Mitigation**: Run mypy after changes to catch type issues

#### Issue 2: Import Path Updates
**Problem**: Missing import updates cause runtime errors
**Solution**: Comprehensive grep search and automated script
**Mitigation**: Test imports before deleting old files

#### Issue 3: Re-export Conflicts
**Problem**: __init__.py files may re-export base protocols
**Solution**: Remove re-exports and update dependent imports
**Mitigation**: Check __init__.py files in both exchanges

### Rollback Plan
1. **Backup Original Files**: Keep copies of both base_protocols.py files
2. **Git Branch**: Create feature branch for changes
3. **Restoration Script**: Automated script to restore original imports
4. **Quick Test**: Verify rollback with mypy and basic import test

## Benefits of Consolidation

### Code Quality Improvements
- **-200 lines**: Eliminate duplicate code (78 + 120 = 198 lines)
- **Single Source of Truth**: One place to update base protocols
- **Consistent Behavior**: Guaranteed identical interfaces across exchanges
- **Type Safety**: Better type compatibility with ParsedJsonResponse

### Development Efficiency Gains
- **Faster Updates**: Change base protocols once, apply everywhere
- **Reduced Testing**: Test base protocols once instead of twice
- **Clear Architecture**: Obvious location for base protocol definitions
- **Better IDE Support**: Single import path for autocomplete

### Maintenance Benefits
- **No Protocol Drift**: Base protocols can't diverge between exchanges
- **Unified Documentation**: Single place for protocol documentation
- **Easier Onboarding**: New developers see clear protocol hierarchy
- **Simplified Debugging**: One protocol definition to debug

## Success Criteria

### Technical Verification
1. **All 13 files updated** to import from common base
2. **mypy passes** with 0 protocol-related errors
3. **pyright passes** with 0 import errors
4. **ruff check passes** with no import issues
5. **All tests pass** including protocol compliance tests

### Architecture Validation
1. **Common base module exists** at `/cyberdelta/apis/base/protocols/base_protocols.py`
2. **Duplicate files deleted** - no more exchange-specific base protocols
3. **ParsedJsonResponse used** for maximum type compatibility
4. **Protocol inheritance works** for all 30 mapper protocols

### Documentation Requirements
1. **Updated import examples** in any documentation
2. **Architecture diagrams updated** to show common base
3. **Developer guide updated** with new import patterns

## Future Considerations

### Extensibility
- Base module can be extended with additional common protocols
- Exchange-specific protocols remain in their respective modules
- Clear separation between common and exchange-specific concerns

### Type System Evolution
- ParsedJsonResponse usage allows future response type changes
- Common base enables unified type checking across exchanges
- Foundation for more sophisticated protocol hierarchies

### Testing Strategy
- Protocol compliance tests can be unified
- Common protocol behaviors tested once
- Exchange-specific behaviors tested separately

## Implementation Status (2025-07-30)

### What Has Been Done
- ✓ Symbol type migration for asset parameters across all protocols
- ✓ Minor method signature refinements

### What Remains Unresolved
- ✗ Duplicate base_protocols.py files still exist in both exchanges
- ✗ No common base module created at `/cyberdelta/apis/base/protocols/`
- ✗ No abstract mapper protocols implemented
- ✗ 13 files still import from duplicate base protocol locations
- ✗ Protocol drift risk remains high

### Next Steps Required
1. **Immediate**: Create `/cyberdelta/apis/base/protocols/` directory structure
2. **Phase 1**: Consolidate base_protocols.py into common module
3. **Phase 2**: Update all import statements (13 files)
4. **Phase 3**: Delete duplicate files
5. **Phase 4**: Run comprehensive tests

## Conclusion

The duplicate base protocols represent a clear case of unnecessary code duplication with zero architectural benefit. All three protocols are 100% functionally identical, with only minor type variance that favors the more flexible ParsedJsonResponse type.

**This consolidation is:**
- **Low Risk**: Interface-only changes with strong type checking
- **High Impact**: Eliminates ~174 lines of duplicate code (updated from 198)
- **Quick Implementation**: 2-3 hours total time investment
- **Future-Proof**: Establishes pattern for common protocol development
- **Urgent**: The longer this remains unresolved, the higher the risk of protocol drift

The migration preserves all existing functionality while creating a cleaner, more maintainable architecture that follows the DRY principle and establishes clear protocol ownership.

### ✅ Implementation Verified Complete (2025-08-06)

#### Changes Made
1. **Created common base protocol module**:
   - Created `/cyberdelta/apis/base/protocols/` directory structure
   - Created `base_protocols.py` with consolidated MapperProtocol, RequestBuilderProtocol, and ResponseHandlerProtocol
   - Used ParsedJsonResponse type for maximum compatibility

2. **Created abstract mapper protocols**:
   - Created `mapper_protocols.py` with 10 abstract protocol interfaces
   - Each protocol defines conceptual transformations without implementation details
   - Protocols use `Any` type for flexibility in abstract interfaces

3. **Updated all imports** (11 files):
   - Updated both Backpack and Hyperliquid to import from common base
   - All protocol files, component registries, and tests now use common imports

4. **Updated inheritance patterns**:
   - Exchange-specific mapper protocols now inherit from both MapperProtocol and relevant abstract protocols
   - Example: `BalanceMapperProtocol(MapperProtocol, AbstractBalanceMapperProtocol, Protocol)`

5. **Deleted duplicate files**:
   - Removed `/cyberdelta/apis/backpack/protocols/base_protocols.py`
   - Removed `/cyberdelta/apis/hyperliquid/protocols/base_protocols.py`

6. **Test fixes**:
   - Updated protocol compliance tests to remove expectations for non-existent methods
   - Fixed test assumptions about method signatures

#### Results
- **Lines eliminated**: ~174 lines of duplicate code
- **Static analysis**: All checks pass (mypy --strict, ruff)
- **Tests**: Protocol compliance tests updated and passing
- **Status**: COMPLETED

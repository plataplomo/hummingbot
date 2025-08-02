# Mypy Clean Break Fixes After Refactoring

## Summary
After the portfolio boundaries separation refactor, running mypy revealed 530 errors across 87 files. This document tracks the clean break fixes to improve type safety and fix import issues.

## Error Categories

### 1. Import Issues (High Priority)
- Missing modules due to moved files
- Incorrect import paths after refactoring
- Base classes not found in expected locations

### 2. Type Annotation Issues
- Missing type parameters for generics (BaseEvent[T])
- __all__ lists need type annotations
- Missing return type annotations

### 3. Abstract Class Issues
- BaseEvent is being instantiated directly
- Missing abstract method implementations

### 4. Attribute Errors
- PortfolioState missing expected attributes
- SpotBalance has no 'values' attribute
- BaseEvent missing exchange_id attribute

## Fix Plan

### Phase 1: Fix Critical Import Errors
1. [x] Fix screening module imports (base_screener.py renamed to base_validator.py)
   - Renamed all Screener classes to Validator
   - Updated imports in __init__ files
2. [ ] Fix cache service imports
3. [ ] Fix infrastructure service factory imports
4. [x] Update __all__ declarations with proper type annotations
   - Fixed infrastructure/__init__.py

### Phase 2: Fix Type Parameters
1. [x] Add proper type parameters to BaseEvent usage
   - Created PortfolioEvent type alias as BaseEvent[Any]
2. [ ] Fix Task type parameters
3. [ ] Add missing return type annotations

### Phase 3: Fix Abstract Class Issues
1. [x] Fix BaseEvent instantiation - use concrete event classes
   - Updated to use ErrorOccurredEvent.create() instead of direct instantiation
   - Fixed ErrorData constructor parameters
2. [ ] Ensure all abstract methods are implemented

### Phase 4: Fix Attribute Errors
1. [ ] Update PortfolioState usage to match actual model
2. [ ] Fix SpotBalance attribute access
3. [ ] Update event attribute access patterns

## Progress Log

### Progress (2025-01-08)
- Started with 530 errors across 87 files
- Fixed screening module imports (Screener → Validator)
- Fixed PortfolioError → CoreError imports
- Fixed BaseEvent instantiation in analytics orchestrator
- Fixed __all__ type annotations

### Key Fixes Applied:
1. **Import fixes**:
   - `from .base_screener import BaseScreener` → `from .base_validator import BaseValidator`
   - `from cyberdelta.core.portfolio.exceptions.base import PortfolioError` → `from cyberdelta.core.infrastructure.exceptions.base import CoreError as PortfolioError`

2. **Class renames**:
   - All `*Screener` classes → `*Validator`
   - `BaseScreener` → `BaseValidator`

3. **Type fixes**:
   - Added `PortfolioEvent = BaseEvent[Any]` type alias
   - Fixed ErrorOccurredEvent instantiation
   - Added type annotation to `__all__: list[str] = []`
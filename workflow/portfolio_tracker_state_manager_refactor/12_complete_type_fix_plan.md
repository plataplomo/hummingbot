# 20-Step Plan to Complete Portfolio Type Refactor

## Overview
We created Pydantic models but didn't update the code to use them. This plan will systematically replace all dict usage with proper models.

## Phase 1: Foundation Fixes (Steps 1-4)

### Step 1: Fix All TypeVar Bounds
- Add bounds to TypeVars in state_types.py, validation_types.py
- Change `T = TypeVar("T")` to `T = TypeVar("T", bound=BaseModel)`
- This will eliminate many Unknown type inferences

### Step 2: Fix Serialization Service
- Update serialization.py to use TypedDict for JSON parsing
- Add proper type narrowing to eliminate Unknown propagation
- Create SerializedPortfolioData TypedDict for type safety

### Step 3: Create Missing Domain Models
- Create MetricsData model for metrics
- Create ErrorContext model for error handling
- Create ValidationContext model for validation data
- Place in portfolio_types/domain_models.py

### Step 4: Replace Lambda Factories
- Find all `Field(default_factory=lambda: {})` patterns
- Replace with `Field(default_factory=dict)`
- This fixes pyright inference issues

## Phase 2: Protocol Updates (Steps 5-8)

### Step 5: Update StateManagerProtocol
- Change `update_from_orchestrator(dict[str, Any])` to use `PortfolioUpdate`
- Change `get_portfolio_snapshot()` to return `PortfolioSnapshot`
- Update all dict returns to use appropriate models

### Step 6: Update Service Protocols
- Update PersistenceServiceProtocol to use models
- Update ValidationServiceProtocol to use ValidationResult
- Update all dict[str, Any] to specific models

### Step 7: Update Manager Protocols
- BalanceManagerProtocol should use BalanceUpdate model
- PositionManagerProtocol should use PositionUpdate model
- OrderManagerProtocol should use OrderUpdate model

### Step 8: Create Update Models
- Create BalanceUpdate, PositionUpdate, OrderUpdate models
- These replace dict parameters in manager interfaces

## Phase 3: Implementation Updates (Steps 9-12)

### Step 9: Update Portfolio State Manager
- Replace all dict parameters with models
- Update process_trade to use TradeUpdate model
- Update all return types to use models

### Step 10: Update Service Implementations
- Update all services to implement new protocols
- Replace dict usage with model usage
- Fix all protocol override errors

### Step 11: Fix Validation Middleware
- Replace dynamic type extraction with typed methods
- Create extract_trades(), extract_positions() with proper types
- Use overloads for type safety

### Step 12: Update Calculators
- Replace dict returns with calculation result models
- Use CalculationResult models consistently
- Fix all dict[str, Any] in calculator interfaces

## Phase 4: State Management (Steps 13-16)

### Step 13: Fix State Container
- Add proper bounds to generic type
- Replace dict[str, object] with typed models
- Fix PrivateAttr usage

### Step 14: Fix State Snapshot
- Update metadata to use MetadataModel
- Replace dict comparisons with model comparisons
- Fix all dict[str, Any] usage

### Step 15: Update Persistence
- Update save/load to use models
- Create PersistedState model
- Fix serialization to preserve types

### Step 16: Update Event System
- Replace event data dicts with event models
- Create typed event dispatchers
- Fix all event handler signatures

## Phase 5: Final Integration (Steps 17-20)

### Step 17: Update All Managers
- TradeManager should use models throughout
- Update all manager interfaces
- Fix all dict usage in managers

### Step 18: Update Examples
- Update all examples to use models
- Fix all dict construction to model construction
- Ensure examples demonstrate proper usage

### Step 19: Fix Remaining Import Issues
- Consolidate model imports
- Fix circular dependencies
- Remove unnecessary TYPE_CHECKING blocks

### Step 20: Final Verification
- Run mypy, ruff, pyright
- Fix any remaining type errors
- Document model usage patterns

## Expected Outcomes

### Before:
- 43 pyright errors
- Models created but not used
- Dict[str, Any] everywhere

### After:
- 0 pyright errors
- All models properly used
- Type-safe throughout

## Key Changes Summary

1. **No more dict parameters** - All functions use models
2. **No more dict returns** - All returns use models
3. **Proper type bounds** - All generics properly bounded
4. **Type-safe serialization** - No Unknown from JSON
5. **Complete model adoption** - Models used everywhere

## Implementation Order

1. Start with foundation (Steps 1-4) - fixes core issues
2. Update protocols (Steps 5-8) - defines new interfaces
3. Update implementations (Steps 9-12) - implements interfaces
4. Fix state management (Steps 13-16) - core functionality
5. Final integration (Steps 17-20) - complete adoption

This systematic approach will finally complete the refactor and eliminate all type issues.

# Validation System - Breaking Changes

> **🚨 CRITICAL: BREAKING CHANGES - Complete API Rewrite**
>
> This document outlines the breaking changes introduced by the validation system modernization. All legacy validators have been **completely removed** in favor of a unified validation architecture.

## Breaking Changes Overview

### Complete Removal of Legacy Validators

**REMOVED** - All legacy validator classes:
- ❌ `OrderValidator`  
- ❌ `RiskValidator`
- ❌ `PortfolioValidator`
- ❌ `ExchangeValidator` 
- ❌ `MarketValidator`
- ❌ `OrderModificationValidator`

**ADDED** - Single unified validation service:
- ✅ `UnifiedValidationService` - Consolidates all validation logic

### API Changes

#### Import Changes

```python
# ❌ REMOVED - Legacy imports
from cyberdelta.domain.trading.validation import (
    OrderValidator, 
    RiskValidator, 
    PortfolioValidator,
    ExchangeValidator,
    MarketValidator,
    OrderModificationValidator
)

# ✅ NEW - Unified import  
from cyberdelta.domain.trading.validation import UnifiedValidationService
from cyberdelta.enums import TradingState
```

#### API Method Changes

```python
# ❌ REMOVED - Legacy API
validator = OrderValidator(config)
violations = await validator.validate_order(order, portfolio_state, market_snapshot)
if violations:
    # Handle violations list

# ✅ NEW - Unified API
validator = UnifiedValidationService(config)
result = await validator.validate_order(
    order=order,
    portfolio_state=portfolio_state,
    market_snapshot=market_snapshot,
    trading_state=TradingState.ACTIVE,
    is_reconciling=False,
    is_reduce_only=False
)
if not result.is_valid:
    # Handle result.violations list
```

#### ValidationResult Changes

```python
# ❌ REMOVED - Simple list return
violations: list[str] = await validator.validate_order(...)

# ✅ NEW - Enhanced ValidationResult object
result: ValidationResult = await validator.validate_order(...)

# Available properties:
result.is_valid              # bool - overall validation status
result.violations            # list[str] - detailed violation messages  
result.validation_type       # str - type of validation performed
result.timestamp            # datetime - when validation occurred
result.category             # ValidationCategory - primary failure category
```

### Configuration Changes

#### Validation Rule Configuration

```python
# ✅ NEW - Rule-based configuration (future feature)
validation:
  rules:
    price_precision:
      enabled: true
    quantity_precision: 
      enabled: true
    balance_validation:
      enabled: true
    max_position:
      enabled: true
    max_exposure:
      enabled: true
    market_status:
      enabled: true
    trading_hours:
      enabled: true
    market_liquidity:
      enabled: true

# Performance tuning
validation:
  fail_fast_on_precision: true
  log_level: "INFO"
```

### Error Handling Updates

#### Exception Types

```python
# ❌ REMOVED - Validator-specific exceptions
from cyberdelta.exceptions import OrderValidationError, RiskValidationError

# ✅ NEW - Unified validation exceptions
from cyberdelta.models.validation import ValidationResult
# Use ValidationResult.is_valid and ValidationResult.violations
```

#### Error Message Format

```python
# ❌ REMOVED - Simple string violations
violations = [
    "Insufficient balance",
    "Price precision error"
]

# ✅ NEW - Enhanced violation messages with categories
violations = [
    "Balance validation failed: Insufficient USDC balance for order value $1000.00",
    "Price precision error: Price 123.456 not aligned to tick size 0.01 for BTC-USDC"
]
```

### Dependencies and Service Registration

#### Dependency Injection Updates

```python
# ❌ REMOVED - Multiple validator registrations
container.register(OrderValidator, config)
container.register(RiskValidator, config)  
container.register(PortfolioValidator, config)
# ... etc

# ✅ NEW - Single service registration
container.register(UnifiedValidationService, config)
```

#### Service Constructor Updates

```python
# ❌ REMOVED - Multiple validator dependencies
class TradingService:
    def __init__(
        self,
        order_validator: OrderValidator,
        risk_validator: RiskValidator,
        portfolio_validator: PortfolioValidator,
        # ... other validators
    ):
        pass

# ✅ NEW - Single validator dependency
class TradingService:
    def __init__(
        self,
        validator: UnifiedValidationService,
        # ... other dependencies
    ):
        pass
```

## Migration Guide

### Step 1: Update Imports

Replace all legacy validator imports:

```bash
# Find and replace across codebase
grep -r "from.*validation.*import.*Validator" . --include="*.py"
```

### Step 2: Update Service Constructors

Replace multiple validator parameters with single UnifiedValidationService:

```python
# Before
def __init__(self, order_validator: OrderValidator, risk_validator: RiskValidator):
    self._order_validator = order_validator
    self._risk_validator = risk_validator

# After  
def __init__(self, validator: UnifiedValidationService):
    self._validator = validator
```

### Step 3: Update Validation Calls

Replace legacy validation calls with new API:

```python
# Before
violations = await self._order_validator.validate_order(order, portfolio, market)
if violations:
    raise OrderValidationError(violations)

# After
result = await self._validator.validate_order(
    order=order,
    portfolio_state=portfolio,
    market_snapshot=market,
    trading_state=TradingState.ACTIVE,
    is_reconciling=False,
    is_reduce_only=False
)
if not result.is_valid:
    raise OrderValidationError(result.violations)
```

### Step 4: Update Error Handling

Replace simple violation lists with ValidationResult:

```python
# Before
try:
    violations = await validator.validate_order(order)
    if violations:
        log_violations(violations)
except Exception as e:
    handle_validation_error(e)

# After
try:
    result = await validator.validate_order(order=order, ...)
    if not result.is_valid:
        log_violations(result.violations, result.validation_type)
except Exception as e:
    handle_validation_error(e)
```

### Step 5: Update Configuration

Remove legacy validator configurations and add unified settings:

```yaml
# Remove legacy sections
# order_validation: ...
# risk_validation: ...
# portfolio_validation: ...

# Add unified configuration
validation:
  fail_fast_on_precision: true
  log_validation_details: true
```

## Architecture Benefits

### Performance Improvements

- **Single validation pass** - No duplicate checks across validators
- **Category-based execution** - Fail-fast on critical precision errors
- **Optimized rule execution** - Rules executed in logical order
- **Reduced memory overhead** - Single service instead of 6 validators

### Type Safety Enhancements

- **Protocol-based rules** - Consistent ValidationRule interface
- **Typed validation context** - Comprehensive context data structure
- **Enum-driven categories** - Strongly typed validation categories
- **Result objects** - Structured validation results vs. simple lists

### Maintainability Improvements

- **Single responsibility** - One service for all validation
- **Extensible architecture** - Easy to add new validation rules
- **Centralized configuration** - Single point for validation settings
- **Comprehensive testing** - Unified test strategy

### Future Features Enabled

- **Dynamic rule configuration** - Runtime enable/disable rules
- **A/B testing framework** - Test different validation configurations
- **Performance monitoring** - Built-in metrics and timing
- **Custom rule plugins** - External validation rule development

## Rollback Procedures

> **⚠️ WARNING: No rollback possible due to breaking changes approach**

This modernization adopts a breaking changes approach for clean architecture. There is no backwards compatibility or rollback mechanism. 

### If Issues Occur:

1. **Fix forward** - Address issues in the unified validation service
2. **Emergency hotfix** - Create temporary fixes in the unified service
3. **Comprehensive testing** - Use the extensive test suite to validate fixes

### Recommended Deployment Strategy:

1. **Staging environment** - Full testing in staging first
2. **Gradual rollout** - Deploy to limited production environments  
3. **Monitoring** - Intensive monitoring during initial deployment
4. **Quick response** - Development team on standby for issues

## Support and Documentation

### Additional Documentation

- `DEVELOPER_GUIDE.md` - Guide for extending the validation system
- `VALIDATION_CONSOLIDATION_SUMMARY.md` - Complete project summary
- `workflow/trading_logic_cleanup/week1_validation_consolidation_plan_additional.md` - Implementation roadmap

### Rule Development

- All validation rules implement the `ValidationRule` protocol
- Rules are organized by `ValidationCategory` enum values
- Execution order: `PRECISION → LIMITS → BALANCE → RISK → MARKET → STATE`
- Easy to add new rules by implementing the protocol

### Monitoring and Debugging

- Enhanced logging with structured data
- Validation timing metrics
- Category-specific error reporting
- Comprehensive violation messages

---

## Summary

This breaking changes implementation completely modernizes the validation architecture by:

- **Removing** 6 legacy validators and their complex interactions
- **Replacing** with a single, unified validation service
- **Enhancing** type safety through protocols and structured results
- **Improving** performance through optimized execution order
- **Enabling** future features through extensible architecture

The result is a cleaner, more maintainable, and more performant validation system that serves as a foundation for future trading engine development.
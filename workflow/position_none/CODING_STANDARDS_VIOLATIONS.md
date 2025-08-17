# CODING_STANDARDS.md Violations in PositionManager

## Direct Quotes from CODING_STANDARDS.md

### On Fallbacks
> "Never do any fallbacks, unless explicitly asked so"
> "Every fallback coded in is a possible security leak coded in: bad idea"

**Violation**: Line 46 - Creates default calculator when None passed

---

### On Default Values
> "NO DEFAULT VALUES FOR CRITICAL OPERATIONS"
> "Defaults hide missing configuration and create different behavior in different environments"

**Violations**: 
- Line 96 - `exchange: ExchangeName | None = None`
- Line 221 - `exchange: ExchangeName | None = None`

---

### On Optional Types
> "Don't code in `| None` just because you think it looks as a cool fallback, there always have to be a good business logic reason for optionality"

**Violations**:
- Line 46 - Optional calculator is convenience, not business requirement
- Line 114 - Returning None for "no PnL" when Decimal(0) is correct
- Line 367 - Using None as sentinel for deletion

---

### On Assumptions
> "NO ASSUMPTIONS - EVER"
> "Assumptions create hidden dependencies that break when reality differs from your mental model"

**Violation**: Line 61-64 - Assumes creating default calculator is safe

---

### On Fail Fast Philosophy
> "Fail Fast Philosophy"
> "Never hide failures"

**Violation**: Line 77 - Returning None hides the fact that position doesn't exist

---

## Specific Violations Mapped to Code

### 1. Fallback Creation (CRITICAL)
```python
# Line 61-64
if pnl_calculator is None:
    # Create default calculator for backward compatibility
    # In production, should always inject the calculator
    self._pnl_calculator = MarkToMarketCalculator(config, fee_calculator=None)
```
**Violation**: "Never do any fallbacks, unless explicitly asked so"
- Creates hidden configuration
- Different behavior when None vs explicit calculator
- "backward compatibility" = technical debt

### 2. Default Parameters for Critical Operations
```python
# Line 96
async def get_all_positions(self, exchange: ExchangeName | None = None)

# Line 221  
async def get_total_exposure(self, exchange: ExchangeName | None = None)
```
**Violation**: "NO DEFAULT VALUES FOR CRITICAL OPERATIONS"
- Position retrieval is critical
- Exposure calculation is critical for risk management

### 3. Hiding Missing Data
```python
# Line 77
-> DerivativePosition | None  # Returns None if not found
```
**Violation**: "Fail Fast Philosophy"
- Should throw PositionNotFoundError
- Forces defensive programming throughout codebase

### 4. Using None Where Zero is Correct
```python
# Line 114
-> Decimal | None  # Returns None when no PnL realized
```
**Violation**: "Don't code in `| None` just because you think it looks as a cool fallback"
- Decimal(0) is the correct representation
- Forces None checks in financial calculations

### 5. Implicit Behavior Through Sentinels
```python
# Line 367
new_position: DerivativePosition | None  # None means delete
```
**Violation**: Single method doing multiple operations based on None
- Not explicit about intent
- Violates Single Responsibility Principle

## Financial Impact Assessment

### Risk Level: CRITICAL 🔴

These violations occur in:
1. **Position Management** - Core to all trading operations
2. **PnL Calculations** - Direct financial impact
3. **Risk Exposure Calculations** - Critical for risk management

### Potential Consequences:
1. **Hidden Failures**: None returns can propagate causing NoneType errors in production
2. **Incorrect Calculations**: None in financial calculations could be treated as 0
3. **Audit Issues**: Fallback behaviors not explicitly documented
4. **Testing Gaps**: Optional parameters create multiple code paths

## Compliance Score: 2/10 ❌

The PositionManager significantly violates core principles:
- ❌ No fallbacks rule
- ❌ No default values for critical operations  
- ❌ Fail fast philosophy
- ❌ Explicit over implicit
- ❌ Single responsibility

## Required Actions

1. **IMMEDIATE**: Remove fallback calculator creation
2. **HIGH PRIORITY**: Replace None returns with exceptions
3. **HIGH PRIORITY**: Split multi-purpose methods
4. **MEDIUM PRIORITY**: Create proper result types

## Code Review Checklist

Before any PR touching PositionManager:
- [ ] Zero `| None` usage
- [ ] No default parameters for methods
- [ ] No fallback creation
- [ ] Exceptions for error cases
- [ ] Single purpose methods
- [ ] Proper result types instead of tuples with None

## Enforcement

Add to CI/CD pipeline:
```bash
# Check for None fallbacks in PositionManager
grep -n "| None" cyberdelta/domain/portfolio/position_manager.py && exit 1

# Check for default None parameters
grep -n "= None" cyberdelta/domain/portfolio/position_manager.py && exit 1

# Check for None returns
grep -n "return None" cyberdelta/domain/portfolio/position_manager.py && exit 1
```

## Bottom Line

**The current implementation prioritizes convenience over safety**, directly contradicting the project's philosophy that **"In trading systems, every assumption is a future bug, every hardcoded value is a future crisis, and every implicit behavior is a future investigation."**

These are not minor style issues - they are **fundamental violations** of the project's core safety principles in **critical financial code paths**.
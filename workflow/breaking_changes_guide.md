# Breaking Changes Migration Guide

## What Would Need to Change for a Clean Break

### 1. Configuration Files
```yaml
# OLD config.yaml
risk:
  use_simple_sizing_path: true  # ❌ REMOVED
  simple_sizing_method: "fixed_fraction"  # ❌ REMOVED
  simple_fixed_fraction: 0.1  # ❌ REMOVED

# NEW config.yaml  
risk:
  sizing:
    method: "simple"  # ✅ REQUIRED
    parameters:
      fraction: 0.1
```

### 2. Risk Manager Creation
```python
# OLD main.py
risk_manager = RiskManager(
    config,
    portfolio_state_manager,
    circuit_breaker,  # ❌ REMOVED - use factory
    funding_validator,  # ❌ REMOVED - use factory
    risk_factory  # Was optional
)

# NEW main.py
risk_manager = RiskManager(
    config,
    portfolio_state_manager,
    risk_factory  # ✅ REQUIRED
)
```

### 3. Return Type Changes
```python
# OLD - Returns SizedOpportunity
sized_opp = await risk_manager.size_opportunity(opportunity)
if sized_opp:
    long_size = sized_opp.long_size
    short_size = sized_opp.short_size

# NEW - Returns SizingResult
result = await risk_manager.size_opportunity(opportunity)
if result and result.success:
    size = result.position_size
    metrics = result.risk_metrics
```

### 4. Strategy/ExecutionHandler Updates
```python
# OLD funding_rate_arbitrage.py
sized_opportunity = await self.risk_manager.size_opportunity(opportunity)
if not sized_opportunity:
    return None

signal = TradeSignal(
    long_size=sized_opportunity.long_size,
    short_size=sized_opportunity.short_size,
    expected_profit=sized_opportunity.expected_profit
)

# NEW funding_rate_arbitrage.py  
sizing_result = await self.risk_manager.size_opportunity(opportunity)
if not sizing_result or not sizing_result.success:
    return None

signal = TradeSignal(
    long_size=sizing_result.position_size,
    short_size=sizing_result.position_size,  # Delta neutral
    expected_profit=sizing_result.expected_profit_usd
)
```

### 5. Test Updates
```python
# OLD test
mock_risk_manager.size_opportunity.return_value = SizedOpportunity(
    opportunity=opp,
    long_size=Decimal("100"),
    short_size=Decimal("100"),
    allocation_percentage=Decimal("0.1"),
    expected_profit=Decimal("10"),
    expected_return=Decimal("0.01"),
    risk_adjusted_return=Decimal("0.5")
)

# NEW test
mock_risk_manager.size_opportunity.return_value = SizingResult(
    success=True,
    position_size=Decimal("100"),
    message="Sized successfully",
    risk_metrics={"sharpe_ratio": 0.5},
    expected_profit_usd=Decimal("10")
)
```

### 6. Remove Legacy Types
```python
# DELETE cyberdelta/core/risk_types.py SizedOpportunity class
# UPDATE all imports to use SizingResult instead
```

### 7. API Contract Changes

**Before**: Mixed concerns, legacy fields
```python
class SizedOpportunity:
    opportunity: ArbitrageOpportunity  # Original opportunity
    long_size: Decimal
    short_size: Decimal  
    allocation_percentage: Decimal
    expected_profit: Decimal
    expected_return: Decimal
    risk_adjusted_return: Decimal
```

**After**: Clean separation
```python
class SizingResult:
    success: bool
    position_size: Decimal
    message: str
    risk_metrics: dict[str, Any]
    sizing_details: dict[str, Any]
    expected_profit_usd: Decimal | None
    kelly_fraction: Decimal | None
```

## Migration Effort Required

### Files to Update (~25-30 files)
1. **Core System** (5 files)
   - main.py
   - execution_handler.py
   - signal_generator.py
   - engine.py
   - strategy_manager.py

2. **Strategies** (3 files)
   - funding_rate_arbitrage.py
   - Any future strategies
   - strategy base class

3. **Tests** (15-20 files)
   - All risk manager tests
   - Integration tests
   - Strategy tests
   - Execution handler tests

4. **Configuration** (2-3 files)
   - config.yaml templates
   - Config validation
   - Documentation

### Benefits of Breaking Changes

1. **Cleaner API**: No confusion about which fields to use
2. **Better Types**: SizingResult is more expressive than SizedOpportunity
3. **Simpler Code**: No backwards compatibility checks
4. **Modern Patterns**: Direct use of factory pattern
5. **Performance**: No conversion overhead

### Risks of Breaking Changes

1. **Downtime**: System can't run until all changes complete
2. **Testing**: All tests need updates simultaneously  
3. **Rollback**: Can't easily rollback if issues arise
4. **Documentation**: All docs become outdated
5. **User Impact**: Anyone using the API needs to update

## Recommendation

For a production system, the backwards-compatible approach is better because:
- Zero downtime migration
- Gradual deprecation possible
- Can migrate component by component
- Rollback is trivial
- Users get time to adapt

For a greenfield or early-stage project, breaking changes are better because:
- Cleaner codebase
- No technical debt
- Better developer experience
- Simpler mental model
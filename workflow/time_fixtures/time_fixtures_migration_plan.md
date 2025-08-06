# Time Fixtures Migration Plan: unittest.mock to pytest-freezer

**Created**: June 2025
**Last Updated**: August 2025
**Status**: ⚠️ **INFRASTRUCTURE COMPLETE, MIGRATION STALLED** - Phase 1 Done, Phase 2+ Incomplete
**Priority**: 🔴 **HIGH** - Only 2.8% Actual Adoption

## Executive Summary

This document outlines the migration plan for transitioning from ad-hoc `unittest.mock` datetime patches to the standardized pytest-freezer approach across the CyberDeltaEngine test suite. **REALITY CHECK**: Infrastructure successfully built but migration stalled - only 8 out of 289 test files (2.8%) actually use the new fixtures despite comprehensive tooling being available.

## Current State (August 2025)

### Actual unittest.mock Usage
Based on comprehensive codebase analysis:

1. **Direct Module Patching** (1 file found)
   - Only `test_bp_market_data_mapper_robustness.py` contains a patch within frozen_time context

2. **Context Manager Patterns** (1 file found)
   ```python
   with patch("module.datetime") as mock_datetime:
       mock_datetime.now.return_value = fixed_time
   ```
   - Found in `test_bp_market_data_mapper_robustness.py`

3. **Direct datetime.now(UTC) Usage** (~50+ files)
   - Most tests use `datetime.now(UTC)` directly without any mocking
   - No time control for deterministic testing

4. **Actual Fixture Adoption** (8 files only)
   - `test_bp_auth.py`
   - `test_hl_auth_sign_l1_action.py`
   - `test_hl_raw_ws_events.py`
   - `test_hl_cache_performance_benchmark.py`
   - `test_bp_market_data_mapper_core.py`
   - `test_bp_market_data_mapper_robustness.py`
   - `test_bp_spot_candles.py`
   - `test_bp_perp_candles.py`

## ⚠️ ACTUAL MIGRATION STATUS

### Real Migration Progress
- **Phase 1**: ✅ 100% complete - Infrastructure built successfully
- **Phase 2**: ❌ ~7% complete - Only 8 out of 106 files migrated
- **Phase 3-4**: ❌ Not started - Complex migrations pending

### Current Reality
1. **✅ Migration tooling exists** - 4 scripts totaling ~1000 lines created
2. **❌ No governance active** - No pre-commit hooks or CI enforcement found
3. **❌ Migration abandoned** - Work stopped after infrastructure phase
4. **⚠️ Metrics misleading** - Script reports 88.89% but actual usage is 2.8%

### What Actually Exists
1. **✅ Complete infrastructure** - `tests/fixtures/time_fixtures.py` (231 lines)
2. **✅ Migration scripts** - AST-based tooling available but unused
3. **❌ Minimal adoption** - 8 files using fixtures out of 289 total
4. **❌ No enforcement** - Tests continue using uncontrolled datetime

## Migration Strategy

### Phase 1: Foundation (Week 1) ✅ COMPLETED
- [x] Create centralized `time_fixtures.py` - **EXISTS AND WORKS**
- [x] Implement `FreezerProtocol` for type safety - **IMPLEMENTED**
- [x] Add comprehensive fixtures - **ALL CREATED**:
  - `frozen_time` - Basic time freezing (used by 8 files)
  - `mock_time_factory` - Factory for complex scenarios (rarely used)
  - `mock_time_patch` - For time.time() mocking (minimal usage)
  - `market_time_simulation` - Market hours simulation (1 usage found)
  - `rate_limit_timer` - Rate limiting tests (minimal usage)
- [x] Update conftest.py to expose fixtures - **ACCESSIBLE BUT UNUSED**

**INFRASTRUCTURE STATUS: 100% COMPLETE, 2.8% ADOPTED**

### Phase 2: Low-Risk Migrations (Weeks 2-3) ❌ **STALLED**
**STATUS**: **Only 8 out of 106 time-dependent files migrated**
**ADOPTION RATE**: **7.5%** - Migration abandoned after initial attempts

#### ACTUAL MIGRATION STATUS:
1. **Mapper Tests** - 3 files migrated, ~35+ remaining
2. **Auth Tests** - 2 files migrated, ~8+ remaining
3. **Integration Tests** - 2 candle tests migrated, many remaining
4. **Performance Tests** - 1 benchmark migrated

**PATTERN USED IN THE 8 MIGRATED FILES**:
```python
async def test_expiration_cleanup(
    signal_queue: PrioritySignalQueue,
    frozen_time: FreezerProtocol,
) -> None:
    frozen_time.move_to("2024-01-01 00:00:00+00:00")
    # This pattern works but only 8 files use it
```

#### 2.1 Simple @patch Decorators (Remaining ~34 files)
**Target**: Tests with simple `@patch("module.datetime")` patterns
**Approach**:
1. Replace decorator with fixture parameter
2. Use `frozen_time.move_to()` instead of mock configuration

**Before**:
```python
@patch("cyberdelta.apis.backpack.mappers.bp_market_data_mapper.datetime")
async def test_transform_raw_ticker(mock_datetime):
    mock_datetime.now.return_value = datetime(2024, 1, 1, tzinfo=UTC)
    # test code
```

**After**:
```python
async def test_transform_raw_ticker(frozen_time):
    frozen_time.move_to("2024-01-01 00:00:00+00:00")
    # test code
```

#### 2.2 Context Manager Patterns
**Target**: Tests using `with patch(...)` patterns
**Approach**:
1. Replace context manager with fixture usage
2. Move time setting to beginning of test

**Before**:
```python
async def test_something():
    with patch("module.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_time
        # test code
```

**After**:
```python
async def test_something(frozen_time):
    frozen_time.move_to(fixed_time)
    # test code
```

### Phase 3: Complex Migrations (Weeks 4-5)

#### 3.1 Multiple Module Patches
**Target**: Tests patching multiple datetime modules
**Approach**:
1. Use pytest-freezer which patches globally
2. Remove redundant patches

**Before**:
```python
with (
    patch("module1.datetime") as mock_dt1,
    patch("module2.datetime") as mock_dt2,
):
    mock_dt1.now.return_value = mock_dt2.now.return_value = fixed_time
```

**After**:
```python
async def test_something(frozen_time):
    frozen_time.move_to(fixed_time)
    # Both modules now use frozen time automatically
```

#### 3.2 Dynamic Time Progression
**Target**: Tests that advance time during execution
**Approach**:
1. Use `frozen_time.move_to()` for each time change
2. Or use `rate_limit_timer` fixture for incremental advances

**Before**:
```python
times = [datetime(2024, 1, 1, h) for h in range(24)]
mock_datetime.now.side_effect = times
```

**After**:
```python
async def test_something(frozen_time):
    for h in range(24):
        frozen_time.move_to(f"2024-01-01 {h:02d}:00:00+00:00")
        # test code for this hour
```

### Phase 4: Special Cases (Week 6)

#### 4.1 time.time() Patches
**Target**: Auth and rate limiting tests using `time.time()`
**Approach**:
1. Use `mock_time_patch` fixture
2. Or convert to datetime-based approach

**Before**:
```python
@patch("time.time", return_value=1678886400.0)
async def test_auth(mock_time):
    # test code
```

**After**:
```python
async def test_auth(mock_time_patch):
    mock_time_patch.return_value = 1678886400.0
    # test code
```

#### 4.2 Complex Side Effects
**Target**: Tests with complex time behavior
**Approach**:
1. Use `mock_time_factory` for custom behavior
2. Or implement custom fixtures as needed

## File Priority List

### High Priority (Core Business Logic)
1. `tests/unit/apis/backpack/mappers/test_bp_market_data_mapper_*.py`
2. `tests/unit/apis/backpack/mappers/test_bp_account_data_mapper_*.py`
3. `tests/unit/core/test_signal_queue.py`
4. `tests/unit/core/test_portfolio_tracker.py`

### Medium Priority (Service Layer)
1. `tests/unit/apis/backpack/services/test_bp_*_service_*.py`
2. `tests/unit/apis/hyperliquid/services/test_hl_*_service_*.py`
3. `tests/integration/test_synchronized_order_submission.py`

### Low Priority (Edge Cases)
1. Tests with complex mocking that works correctly
2. Tests that will be refactored anyway
3. Deprecated test files

## Migration Checklist

For each file being migrated:

- [ ] Identify all datetime/time mocking patterns
- [ ] Choose appropriate fixture(s) from time_fixtures.py
- [ ] Remove unittest.mock imports for datetime
- [ ] Replace mock setup with fixture usage
- [ ] Verify time-dependent assertions still pass
- [ ] Run test in isolation
- [ ] Run test suite to check for interference
- [ ] Update any test documentation

## Validation Criteria

### Success Metrics
1. **Test Stability**: All migrated tests pass consistently
2. **Code Reduction**: Less boilerplate in each test file
3. **Type Safety**: FreezerProtocol provides better IDE support
4. **Performance**: No significant test slowdown

### Rollback Plan
1. Git history preserves original implementations
2. Can selectively revert files if issues arise
3. Both approaches can coexist during migration

## Risk Mitigation

### Potential Issues
1. **Global Patching**: pytest-freezer patches globally, might affect unrelated code
   - *Mitigation*: Careful test isolation, use fixtures only where needed

2. **Different Behavior**: Mock behavior might differ from freezer
   - *Mitigation*: Thorough testing of each migration

3. **Learning Curve**: Developers need to learn new patterns
   - *Mitigation*: Documentation and examples

## Post-Migration Tasks

1. **Documentation Update**
   - Update test writing guidelines
   - Add examples to developer documentation
   - Create troubleshooting guide

2. **CI/CD Updates**
   - Ensure pytest-freezer is in all environments
   - Update any test running scripts

3. **Code Review Guidelines**
   - Add checks for new tests using old patterns
   - Encourage fixture usage in reviews

## Example Migrations

### Example 1: Simple Timestamp Test
```python
# Before
@patch("cyberdelta.apis.backpack.mappers.bp_market_data_mapper.datetime")
async def test_none_timestamp_fallback(mock_datetime):
    mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    result = transform_timestamp(None)
    assert result == datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

# After
async def test_none_timestamp_fallback(frozen_time):
    frozen_time.move_to("2024-01-01 12:00:00+00:00")
    result = transform_timestamp(None)
    assert result == datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
```

### Example 2: Rate Limiting Test
```python
# Before
async def test_rate_limit():
    start = time.time()
    with patch("time.time") as mock_time:
        mock_time.side_effect = [start, start + 0.1, start + 0.2]
        # test code

# After
async def test_rate_limit(rate_limit_timer):
    # test first request
    rate_limit_timer(advance_seconds=0.1)
    # test second request
    rate_limit_timer(advance_seconds=0.1)
    # test third request
```

## Timeline - ACTUAL vs PLANNED

### What Actually Happened:
- **Week 1**: ✅ Foundation (COMPLETED - Infrastructure built)
- **Weeks 2-3**: ❌ Simple migrations (8/106 files migrated - ABANDONED)
- **Weeks 4-5**: ❌ Complex migrations (NOT STARTED)
- **Week 6**: ❌ Special cases (NOT STARTED)
- **Week 7+**: ❌ Work stopped, no further progress

### Current State (August 2025):
- Infrastructure: ✅ Complete and working
- Migration Scripts: ✅ Available (4 scripts, ~1000 lines)
- Actual Migration: ❌ 8 out of 289 files (2.8%)
- Governance: ❌ No enforcement or hooks active
- Metrics: ⚠️ Misleading (88.89% reported vs 2.8% actual)

### Realistic Next Steps:
- **Week 1**: 🔴 Acknowledge actual state, reset expectations
- **Week 2**: 🎯 Pick 10 high-value files and migrate manually
- **Week 3**: 🎯 Set up actual CI enforcement
- **Week 4**: 📊 Create honest metrics dashboard
- **Month 2**: 🚀 Target 25% real adoption
- **Month 3**: 🏆 Aim for 50% adoption

## Conclusion - Infrastructure Success, Migration Failed

The **technical foundation is excellent** but the **migration execution failed** with only 2.8% adoption (8 out of 289 test files) despite comprehensive infrastructure and tooling.

**ACTUAL ACHIEVEMENTS**:
- **Technical Success**: Infrastructure works well for the 8 files using it
- **Migration Failure**: 98+ files still use uncontrolled datetime

**CURRENT REALITY**:
1. ✅ **Automation tooling created** - 4 scripts exist but show migration incomplete
2. ❌ **No governance active** - No pre-commit hooks or CI enforcement found
3. ⚠️ **Metrics misleading** - Scripts report 88.89% but actual adoption is 2.8%
4. ❌ **Migration abandoned** - Work stopped after infrastructure phase

**RECOMMENDATION**: The infrastructure is solid and the tooling exists. What's needed is actual execution of the migration work. The project should either:
1. Accept the current state and document it honestly
2. Commit resources to actually migrate the remaining 98+ files
3. Simplify the approach to encourage organic adoption

The gap between documentation claims and reality needs to be addressed.

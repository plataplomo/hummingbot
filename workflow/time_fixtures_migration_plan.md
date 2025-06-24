# Time Fixtures Migration Plan: unittest.mock to pytest-freezer

**Created**: June 2025  
**Status**: Implementation Ready  
**Priority**: Medium  

## Executive Summary

This document outlines the migration plan for transitioning from ad-hoc `unittest.mock` datetime patches to the standardized pytest-freezer approach across the CyberDeltaEngine test suite. The goal is to improve test maintainability, reduce code duplication, and establish consistent time mocking patterns.

## Current State

### Inventory of unittest.mock Usage
Based on analysis, the following patterns are currently in use:

1. **Direct Module Patching** (~40 files)
   ```python
   @patch("cyberdelta.apis.backpack.mappers.bp_market_data_mapper.datetime")
   ```

2. **Context Manager Patterns** (~25 files)
   ```python
   with patch("module.datetime") as mock_datetime:
       mock_datetime.now.return_value = fixed_time
   ```

3. **Complex MagicMock Configurations** (~15 files)
   ```python
   mock_datetime = MagicMock()
   mock_datetime.now.return_value = fixed_datetime
   mock_datetime.utcnow.return_value = fixed_datetime.replace(tzinfo=None)
   ```

4. **time.time() Patches** (~10 files)
   ```python
   @patch("time.time", return_value=1678886400.0)
   ```

## Migration Strategy

### Phase 1: Foundation (Week 1) ✅ COMPLETED
- [x] Create centralized `time_fixtures.py`
- [x] Implement `FreezerProtocol` for type safety
- [x] Add comprehensive fixtures:
  - `frozen_time` - Basic time freezing
  - `mock_time_factory` - Factory for complex scenarios
  - `mock_time_patch` - For time.time() mocking
  - `market_time_simulation` - Market hours simulation
  - `rate_limit_timer` - Rate limiting tests
- [x] Update conftest.py to expose fixtures

### Phase 2: Low-Risk Migrations (Weeks 2-3)

#### 2.1 Simple @patch Decorators
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

## Timeline

- **Week 1**: ✅ Foundation (COMPLETED)
- **Weeks 2-3**: Simple migrations (30-40 files)
- **Weeks 4-5**: Complex migrations (20-30 files)
- **Week 6**: Special cases and cleanup
- **Week 7**: Documentation and review
- **Week 8**: Buffer for issues and refinements

## Conclusion

This migration will standardize time mocking across the CyberDeltaEngine test suite, improving maintainability and reducing technical debt. The phased approach minimizes risk while allowing for continuous validation of the migration process.
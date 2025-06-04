# Unit Test Refactoring Report

## Executive Summary

This report identifies tests in the `/tests/unit/` directory that should be moved to `/tests/integration/` and tests that access protected members that should be refactored.

## 1. Integration Tests Currently in Unit Test Folders

### 1.1 File I/O Operations

#### `/tests/unit/test_config_security.py`
- **Lines**: 15, 34, 192-193, 315
- **Issues**: Uses `tempfile.TemporaryDirectory()` and real file operations with `open()`
- **Pattern**: Creates temporary directories and writes/reads configuration files
- **Recommendation**: Move to `/tests/integration/config/`

#### `/tests/unit/test_simplified_visualizer.py`
- **Lines**: 3, 78
- **Issues**: Uses `tempfile.mkdtemp()` for creating temporary directories
- **Pattern**: Tests visualization output to actual files
- **Recommendation**: Move to `/tests/integration/visualization/`

### 1.2 Tests with Sleep/Timing Dependencies

#### `/tests/unit/core/test_signal_queue.py`
- **Pattern**: Uses `asyncio.sleep` or timing-dependent logic
- **Recommendation**: Move to `/tests/integration/core/`

#### `/tests/unit/apis/connectivity/test_http_client.py`
- **Pattern**: Tests HTTP client with timing concerns
- **Recommendation**: Keep in unit tests but ensure proper mocking

#### `/tests/unit/apis/test_rate_limiter.py`
- **Pattern**: Tests rate limiting with sleep operations
- **Recommendation**: Move timing-dependent tests to `/tests/integration/apis/`

### 1.3 Multi-Component Tests

#### `/tests/unit/core/test_portfolio_tracker.py`
- **Lines**: Multiple instances of testing with API clients and multiple exchanges
- **Issues**: Tests interactions between PortfolioTracker, ExchangeAPI clients, and price conversions
- **Pattern**: Uses multiple mock API clients and tests cross-component behavior
- **Recommendation**: Move complex multi-exchange tests to `/tests/integration/core/`

#### `/tests/unit/test_execution_handler.py`
- **Pattern**: Tests ExecutionHandler with multiple components (API clients, orders, risk management)
- **Recommendation**: Move complex execution workflow tests to `/tests/integration/core/`

#### `/tests/unit/test_strategy_manager.py`
- **Pattern**: Tests StrategyManager with multiple strategies and components
- **Recommendation**: Move to `/tests/integration/core/`

### 1.4 WebSocket Tests with Real Connections

While most WebSocket tests appear to use mocks, any tests that:
- Connect to actual WebSocket endpoints
- Use real WebSocket libraries without mocking
Should be moved to `/tests/integration/apis/connectivity/`

## 2. Tests Accessing Protected Members

### 2.1 `/tests/unit/test_execution_handler.py`
- **Lines**: 319, 341, 390, 409, 502, 584, 1137, 1223-1224
- **Protected Methods Accessed**:
  - `_place_order_with_retry()` - lines 319, 341
  - `_get_order_status()` - lines 390, 409
  - `_compensate_position()` - lines 502, 584, 1137
  - `_add_to_history()` - lines 1223-1224
- **Recommendation**: 
  - Create public test helper methods or use dependency injection
  - Consider making these methods public if they're part of the testing interface

### 2.2 `/tests/unit/conftest.py`
- **Lines**: 42, 46, 49, 52, 66, 109, 112, 115, 118
- **Protected Attributes**: `_data`, `_raise_for_status_called`, `_request`
- **Context**: Mock response class implementation
- **Recommendation**: This is acceptable as it's test infrastructure

### 2.3 `/tests/unit/apis/base/test_exchange_api.py`
- **Lines**: 84, 86, 88
- **Protected Attributes**: `_http_client`, `_ws_manager`, `_rate_limiter_service`
- **Recommendation**: Use public properties or dependency injection

### 2.4 `/tests/unit/core/test_symbol_mapper.py`
- **Lines**: 93-96, 222-223
- **Protected Attributes**: `_exchange_to_internal`, `_internal_to_exchange`
- **Recommendation**: Add public methods to verify mapping state

### 2.5 `/tests/unit/core/test_portfolio_tracker.py`
- **Line**: 883 (comment references `_fetch_exchange_balances`)
- **Recommendation**: Test through public API instead

### 2.6 `/tests/unit/apis/connectivity/test_http_client.py`
- **Lines**: 859, 871 (comments reference `_parse_and_validate_response`)
- **Recommendation**: Test behavior through public methods

## 3. Recommended Actions

### 3.1 Immediate Actions
1. Create directory structure: `/tests/integration/` with subdirectories matching unit test structure
2. Move identified integration tests to appropriate directories
3. Update imports in moved test files

### 3.2 Refactoring Protected Member Access
1. For `test_execution_handler.py`:
   - Create a test-specific subclass that exposes protected methods
   - Or add a testing mode that makes these methods accessible
   
2. For `test_symbol_mapper.py`:
   - Add public methods like `has_exchange_mapping()` and `has_internal_mapping()`
   
3. For `test_exchange_api.py`:
   - Use constructor injection or public properties

### 3.3 Test Organization
```
tests/
├── unit/           # Pure unit tests with mocks
├── integration/    # Tests with I/O, timing, or multiple real components
│   ├── apis/
│   ├── config/
│   ├── core/
│   └── visualization/
└── e2e/           # End-to-end tests (future)
```

### 3.4 CI/CD Considerations
- Run unit tests on every commit (fast)
- Run integration tests on PR creation/update (slower)
- Add test markers: `@pytest.mark.unit`, `@pytest.mark.integration`

## 4. Summary Statistics

- **Files with file I/O operations**: 2
- **Files with timing dependencies**: 5
- **Files with multi-component tests**: 7
- **Files accessing protected members**: 6
- **Total files needing attention**: ~15-20

## 5. Priority Order

1. **High Priority**: Move file I/O tests (`test_config_security.py`, `test_simplified_visualizer.py`)
2. **Medium Priority**: Refactor protected member access in core tests
3. **Low Priority**: Review and categorize WebSocket tests
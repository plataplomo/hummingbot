# Detailed Test Analysis - Line by Line

## 1. Integration Tests in Unit Test Directory

### 1.1 File System Operations

#### `/tests/unit/test_config_security.py`
```python
# Line 34-36: Creates real temporary directory
with tempfile.TemporaryDirectory() as temp_dir_name:
    config_path = os.path.join(temp_dir_name, "config.yaml")
    with open(config_path, "w") as f:

# Lines 192-193: Multiple temporary directories
tempfile.TemporaryDirectory() as temp_dir_name,
tempfile.TemporaryDirectory() as home_dir_name,
```
**Issue**: Real file system operations, not mocked
**Solution**: Move to `/tests/integration/config/test_config_security.py`

#### `/tests/unit/test_simplified_visualizer.py`
```python
# Line 78: Creates real temporary directory
test_dir = tempfile.mkdtemp()
```
**Issue**: Creates actual directories for output
**Solution**: Move to `/tests/integration/visualization/test_simplified_visualizer.py`

### 1.2 Multi-Component Integration Tests

#### `/tests/unit/core/test_portfolio_tracker.py`
```python
# Lines 159-165: Registers multiple API clients
tracker = PortfolioTracker(config, pt_config)
# Register API clients
for exchange_id, client in api_clients.items():
    tracker.register_api_client(exchange_id, client)

# Lines 333-409: Tests full initialization flow with multiple exchanges
await portfolio_tracker.initialize()
# Verify that balances are updated
assert "USDC" in portfolio_tracker.balances["hyperliquid"]
assert "USDC" in portfolio_tracker.balances["backpack"]
```
**Issue**: Tests coordination between multiple components
**Solution**: Move complex multi-exchange tests to `/tests/integration/core/test_portfolio_tracker_integration.py`

## 2. Protected Member Access Patterns

### 2.1 Direct Protected Method Testing

#### `/tests/unit/test_execution_handler.py`
```python
# Line 319: Testing protected method directly
result_order = await execution_handler._place_order_with_retry(
    execution=execution,
    exchange_id="hyperliquid",

# Line 390: Another protected method
result_status = await execution_handler._get_order_status(
    execution=execution, exchange_id="hyperliquid", order_id="HL-Status"
)

# Lines 1223-1224: Protected method for history
execution_handler._add_to_history(exec1)
execution_handler._add_to_history(exec2)
```

**Refactoring Options**:

1. **Option A - Test Helper Class**:
```python
class TestableExecutionHandler(ExecutionHandler):
    """Test subclass that exposes protected methods."""

    async def test_place_order_with_retry(self, *args, **kwargs):
        return await self._place_order_with_retry(*args, **kwargs)

    async def test_get_order_status(self, *args, **kwargs):
        return await self._get_order_status(*args, **kwargs)
```

2. **Option B - Friend Testing Pattern**:
```python
# In production code
class ExecutionHandler:
    def __init__(self, testing_mode=False):
        self._testing_mode = testing_mode

    @property
    def test_interface(self):
        if not self._testing_mode:
            raise RuntimeError("Test interface only available in testing mode")
        return self._TestInterface(self)

    class _TestInterface:
        def __init__(self, handler):
            self._handler = handler

        async def place_order_with_retry(self, *args, **kwargs):
            return await self._handler._place_order_with_retry(*args, **kwargs)
```

### 2.2 Protected Attribute Access

#### `/tests/unit/core/test_symbol_mapper.py`
```python
# Lines 93-96: Accessing internal mapping dictionaries
assert "valid_exchange" in mapper._exchange_to_internal
assert "missing_symbols" not in mapper._exchange_to_internal
assert "invalid_symbols_type" not in mapper._exchange_to_internal
assert "ETH" not in mapper._internal_to_exchange
```

**Refactoring Solution**:
```python
# Add public inspection methods to SymbolMapper
class SymbolMapper:
    def has_exchange(self, exchange: str) -> bool:
        """Check if exchange is registered."""
        return exchange in self._exchange_to_internal

    def has_symbol_mapping(self, exchange: str, symbol: str) -> bool:
        """Check if a specific symbol mapping exists."""
        return (exchange in self._exchange_to_internal and
                symbol in self._exchange_to_internal[exchange])

    def get_mapped_exchanges(self) -> list[str]:
        """Get list of all mapped exchanges."""
        return list(self._exchange_to_internal.keys())
```

## 3. Network/External Service Tests

### 3.1 HTTP Client Tests
#### `/tests/unit/apis/connectivity/test_http_client.py`
While this file patches aiohttp properly, it contains complex session management tests that might benefit from integration testing:

```python
# Lines 86-100: Complex session lifecycle testing
async def test_internal_session_creation_reuse_and_closure(
    self,
    MockAiohttpSession: MagicMock,
    http_client_instance: HttpClient,
) -> None:
    """Test internal session is created on first request, reused, and closed correctly."""
```

**Recommendation**: Keep mocked tests in unit, but add integration tests for real HTTP scenarios

### 3.2 WebSocket Tests
Many files reference WebSocket functionality but most appear to use mocks appropriately. However, any test that:
- Creates actual WebSocket connections
- Tests reconnection logic with real timing
- Validates actual WebSocket protocol behavior

Should be moved to integration tests.

## 4. Timing-Sensitive Tests

### 4.1 Rate Limiter Tests
#### `/tests/unit/apis/test_rate_limiter.py`
Tests that validate actual rate limiting behavior with time.sleep or asyncio.sleep should be integration tests.

### 4.2 Signal Queue Tests
#### `/tests/unit/core/test_signal_queue.py`
If this file contains tests that depend on actual timing for queue expiration or cleanup, those specific tests should be moved.

## 5. Recommended Test Structure

```python
# For protected member access - use a test utilities module
# tests/test_utils/testable_classes.py
from cyberdelta.core.execution_handler import ExecutionHandler

class TestableExecutionHandler(ExecutionHandler):
    """ExecutionHandler with exposed internals for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Expose internal methods with test_ prefix
        self.test_place_order_with_retry = self._place_order_with_retry
        self.test_get_order_status = self._get_order_status
        self.test_compensate_position = self._compensate_position
        self.test_add_to_history = self._add_to_history

# In test files
from tests.test_utils.testable_classes import TestableExecutionHandler

async def test_order_retry_logic():
    handler = TestableExecutionHandler(...)
    result = await handler.test_place_order_with_retry(...)
    # assertions
```

## 6. Migration Checklist

- [ ] Create `/tests/integration/` directory structure
- [ ] Move `test_config_security.py` → `/tests/integration/config/`
- [ ] Move `test_simplified_visualizer.py` → `/tests/integration/visualization/`
- [ ] Extract integration tests from `test_portfolio_tracker.py` → `/tests/integration/core/`
- [ ] Create `tests/test_utils/` for testable class wrappers
- [ ] Refactor protected member access in remaining unit tests
- [ ] Add pytest markers for test categorization
- [ ] Update CI/CD pipeline to run tests separately

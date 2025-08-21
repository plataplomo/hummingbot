# Hummingbot Connector Testing Strategy

## Research Findings

Based on my analysis of the Hummingbot test environment, here's a comprehensive overview of their testing approach:

## 1. Test Structure

### Directory Organization
```
test/
├── hummingbot/
│   ├── connector/
│   │   ├── exchange/
│   │   │   ├── backpack/
│   │   │   │   ├── test_backpack_auth.py
│   │   │   │   └── test_backpack_utils.py
│   │   │   └── [other_exchanges]/
│   │   └── derivative/
│   │       ├── backpack_perpetual/
│   │       │   └── __init__.py (currently empty)
│   │       └── [other_derivatives]/
│   └── [other_components]/
└── isolated_asyncio_wrapper_test_case.py
```

### Test Framework
- **Primary Framework**: `pytest` (configured in `pyproject.toml`)
- **Base Classes**: Custom test case wrappers for async support
- **Coverage**: Uses `coverage.py` for code coverage reporting
- **Mocking**: `unittest.mock`, `aioresponses`, and custom `NetworkMockingAssistant`

## 2. Testing Levels

### Unit Tests (Primary Focus)
Hummingbot heavily relies on unit tests with mocked external dependencies:

**Characteristics:**
- Mock all external API calls using `aioresponses` and `NetworkMockingAssistant`
- Test individual methods in isolation
- Focus on business logic validation
- Async-first testing patterns

**Example Pattern (from Binance Perpetual tests):**
```python
class BinancePerpetualDerivativeUnitTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)
        self.exchange = BinancePerpetualDerivative(...)

    def test_specific_functionality(self):
        # Mock API responses
        # Test exchange methods
        # Assert expected behavior
```

### Integration Tests (Limited)
- **NO dedicated integration test directory found**
- Tests are primarily unit tests with extensive mocking
- Real API integration appears to be tested manually or in separate environments

### End-to-End Tests
- **NOT found in the codebase**
- Likely performed manually or in staging environments

## 3. Test Support Infrastructure

### Key Testing Utilities

1. **`IsolatedAsyncioWrapperTestCase`**
   - Custom wrapper around `unittest.IsolatedAsyncioTestCase`
   - Manages event loop isolation per test
   - Provides async utilities like `run_async_with_timeout`

2. **`NetworkMockingAssistant`**
   - Sophisticated mocking helper for HTTP and WebSocket connections
   - Queues responses for sequential API calls
   - Tracks sent messages for verification
   - Supports both JSON and text message formats

3. **`AbstractExchangeConnectorTests`**
   - Base test class for exchange connectors
   - Provides common test patterns and assertions
   - Located in `hummingbot/connector/test_support/exchange_connector_test.py`

4. **`PerpetualDerivativeTests`**
   - Base test class specifically for perpetual derivatives
   - Extends `ExchangeConnectorTests`
   - Additional tests for positions, funding, leverage

## 4. Testing Patterns

### Common Test Categories for Connectors

1. **Authentication Tests** (`test_*_auth.py`)
   - Signature generation
   - Header construction
   - WebSocket authentication messages
   - Error handling for invalid credentials

2. **Utils Tests** (`test_*_utils.py`)
   - Trading pair conversions
   - Symbol formatting
   - Data validation
   - Helper function behavior

3. **Order Book Data Source Tests** (`test_*_api_order_book_data_source.py`)
   - Snapshot fetching
   - Diff processing
   - WebSocket stream handling
   - Trade data parsing

4. **User Stream Data Source Tests** (`test_*_user_stream_data_source.py`)
   - Private WebSocket authentication
   - Order updates
   - Balance updates
   - Position updates (for derivatives)

5. **Main Connector Tests** (`test_*_exchange.py` or `test_*_derivative.py`)
   - Order placement
   - Order cancellation
   - Balance queries
   - Trading rules
   - Error handling
   - In-flight order tracking

6. **Web Utils Tests** (`test_*_web_utils.py`)
   - URL construction
   - REST request building
   - Rate limiting
   - Error mapping

## 5. Testing Best Practices Observed

1. **Isolation**: Each test runs in isolated event loop
2. **Mocking**: External dependencies heavily mocked
3. **Async Testing**: Full support for async/await patterns
4. **Event Verification**: Use of `EventLogger` to verify events
5. **Time Control**: Mock time for consistent testing
6. **Response Queuing**: Sequential API responses via queues

## 6. Test Execution

### Running Tests
```bash
# Run all tests with coverage
make test

# Run specific test file
pytest test/hummingbot/connector/exchange/backpack/test_backpack_auth.py

# Run with coverage report
make run_coverage

# Generate HTML coverage report
make report_coverage
```

### Test Configuration
- Configured via `pyproject.toml`
- Asyncio fixture scope set to "function"
- Excluded directories in Makefile (e.g., mock, certain exchanges)

## 7. Recommended Testing Approach for Backpack Connector

### Phase 1: Complete Unit Test Coverage (Priority)

#### Spot Connector Tests Needed:
1. **`test_backpack_exchange.py`** (Main connector tests)
   - Order lifecycle (create, fill, cancel)
   - Balance management
   - Trading rules application
   - Error scenarios
   - Rate limiting

2. **`test_backpack_api_order_book_data_source.py`**
   - Order book snapshots
   - WebSocket depth updates
   - Trade stream processing

3. **`test_backpack_user_stream_data_source.py`**
   - Authentication flow
   - Private channel subscriptions
   - Event parsing and routing

4. **`test_backpack_web_utils.py`**
   - REST helper functions
   - URL builders
   - Error response handling

#### Derivatives Connector Tests Needed:
1. **`test_backpack_perpetual_derivative.py`**
   - All spot tests plus:
   - Position management
   - Funding rate updates
   - Leverage operations
   - Liquidation warnings
   - Position actions (OPEN/CLOSE)

2. **`test_backpack_perpetual_api_order_book_data_source.py`**
   - Similar to spot but with perpetual-specific channels

3. **`test_backpack_perpetual_user_stream_data_source.py`**
   - Position updates
   - Funding payments
   - Liquidation events

4. **`test_backpack_perpetual_auth.py`**
   - Can likely reuse spot auth tests

5. **`test_backpack_perpetual_utils.py`**
   - Perpetual-specific utilities
   - Symbol conversions for perps

6. **`test_backpack_perpetual_web_utils.py`**
   - Perpetual REST endpoints
   - WebSocket URLs

### Phase 2: Integration Testing (Optional/Future)

While Hummingbot doesn't have explicit integration tests, we could create:

1. **Mock Server Tests**
   - Create a mock Backpack server
   - Test full workflows with realistic delays
   - Simulate network issues and recoveries

2. **Testnet Integration** (if Backpack provides testnet)
   - Real API calls to testnet
   - Limited order amounts
   - Full workflow validation

### Phase 3: Manual Testing Protocol

1. **Paper Trading Mode**
   - Use Hummingbot's paper trading
   - Validate all strategies work

2. **Testnet Trading** (if available)
   - Small real trades
   - Monitor for issues

3. **Production Testing**
   - Start with minimal amounts
   - Gradual scaling
   - Monitor logs carefully

## 8. Test Implementation Template

Here's a template based on Hummingbot patterns:

```python
import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

from aioresponses import aioresponses

from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.common import OrderType, TradeType


class BackpackExchangeUnitTest(IsolatedAsyncioWrapperTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDC"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.symbol = f"{cls.base_asset}_{cls.quote_asset}"

    def setUp(self):
        super().setUp()
        self.mocking_assistant = NetworkMockingAssistant(self.local_event_loop)

        self.exchange = BackpackExchange(
            backpack_api_key="test_api_key",
            backpack_api_secret="test_secret",
            trading_pairs=[self.trading_pair]
        )

    def test_order_creation(self):
        # Test implementation
        pass
```

## 9. Coverage Goals

### Minimum Coverage Requirements:
- **Line Coverage**: 80%+
- **Branch Coverage**: 70%+
- **Critical Path Coverage**: 100% (order operations, auth, balance management)

### Priority Areas:
1. Authentication and signature generation
2. Order placement and cancellation
3. Balance updates
4. Error handling
5. WebSocket reconnection
6. Rate limiting

## 10. Continuous Testing Strategy

1. **Pre-commit Hooks**: Run flake8, mypy
2. **PR Requirements**: All tests must pass
3. **Coverage Reports**: Monitor coverage trends
4. **Performance Tests**: Monitor latency and throughput
5. **Regression Tests**: Add tests for any bugs found

## Conclusion

Hummingbot uses a comprehensive unit testing approach with sophisticated mocking infrastructure. The focus is on:
- **Unit tests over integration tests**
- **Mocking all external dependencies**
- **Async-first testing patterns**
- **Event-driven verification**

For the Backpack connector, we should follow this established pattern, creating comprehensive unit tests that mock all Backpack API interactions while thoroughly testing the business logic.

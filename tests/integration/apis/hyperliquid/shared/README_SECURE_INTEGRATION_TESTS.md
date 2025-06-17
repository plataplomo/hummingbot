# Secure Hyperliquid Integration Tests

This directory contains comprehensive, security-compliant integration tests for the Hyperliquid API components and WebSocket functionality.

## 🛡️ Security Compliance

These tests strictly adhere to the **TESTING_SECURITY_RULES.md** to prevent catastrophic financial losses:

### ✅ Financial Safety Features

- **No hardcoded values**: All prices, quantities, and constraints come from real exchange APIs
- **Fail-fast error handling**: Critical operations fail tests immediately if they fail in reality
- **Decimal precision**: All financial calculations use `Decimal` type to prevent float precision errors
- **Real market data**: Tests use live exchange data, not mocked or fake values
- **Timezone-aware operations**: All datetime operations include explicit timezone information

### ✅ Trading Safety Features

- **Dynamic symbol retrieval**: Uses `symbol_helpers.py` to get available symbols from exchange
- **Market constraint validation**: Respects real tick sizes, step sizes, and minimum quantities
- **Order lifecycle testing**: Validates complete order placement → cancellation workflows
- **Race condition prevention**: Uses proper polling instead of fixed delays
- **Data freshness validation**: Ensures market data is recent enough for trading decisions

## 📁 Test Files

### `test_hl_api_integration.py`
**Comprehensive API component integration tests**

Tests the integration between different API services:
- **Trading ↔ Account Service Integration**: Validates that order operations correctly affect account summaries
- **Market Data ↔ Trading Integration**: Ensures trading service respects market constraints from market data service
- **Error Handling Consistency**: Validates that errors are mapped consistently across all services
- **Data Model Consistency**: Ensures the same data is represented consistently across different services
- **Concurrent Operations**: Tests that concurrent API calls maintain data integrity

**Key Test Methods:**
- `test_trading_account_service_integration()`: Full order placement → account update → cancellation workflow
- `test_market_data_trading_integration()`: Market constraint validation and order acceptance/rejection
- `test_error_handling_consistency_across_services()`: Error mapping consistency validation
- `test_data_model_consistency_across_pipeline()`: Symbol and financial data consistency checks
- `test_concurrent_service_operations()`: Concurrent API operation validation

### `test_hl_api_ws_integration.py`
**Comprehensive WebSocket integration tests**

Tests WebSocket functionality and integration with internal models:
- **Real-time Market Data**: WebSocket ticker streaming and model transformation
- **Trading Event Streaming**: Order status updates via WebSocket
- **Data Freshness Validation**: Ensures WebSocket data is fresh enough for trading
- **Error Handling & Reconnection**: WebSocket failure recovery and retry logic
- **Performance Requirements**: Validates WebSocket operations meet trading performance needs

**Key Test Methods:**
- `test_websocket_market_data_integration()`: Ticker streaming and model validation
- `test_websocket_trading_events_integration()`: Order event streaming validation
- `test_websocket_data_freshness_validation()`: Timestamp and freshness checks
- `test_websocket_error_handling_and_reconnection()`: Connection failure recovery
- `test_websocket_performance_and_latency()`: Performance requirements validation

## 🔧 Supporting Infrastructure

### `symbol_helpers.py`
Dynamic symbol retrieval functions that eliminate hardcoded symbols:
- `get_available_symbols()`: Get all available symbols from exchange
- `get_test_symbol()`: Get specific symbol by index for testing
- `get_major_crypto_symbol()`: Find symbol for major cryptocurrencies (BTC, ETH, etc.)
- `validate_symbol_format()`: Exchange-specific symbol format validation

### `test_helpers.py`
Enhanced with race condition prevention and polling utilities:
- `wait_for_order_placement()`: Poll until order appears in open orders
- `wait_for_order_cancellation()`: Poll until order cancellation is reflected
- `eventually_assert()`: Generic polling assertion helper
- Adaptive polling intervals for optimal performance

## 🚦 Test Execution

### Prerequisites
1. **Real API credentials**: Tests require valid Hyperliquid testnet credentials
2. **Sufficient balance**: Account must have balance for order placement tests
3. **Network connectivity**: Tests make real API calls to Hyperliquid

### Running Tests
```bash
# Run all secure integration tests
pytest tests/integration/apis/hyperliquid/shared/test_hl_api_integration.py -v

# Run WebSocket integration tests
pytest tests/integration/apis/hyperliquid/websockets/test_hl_api_ws_integration.py -v

# Run with specific markers
pytest -m "integration and not websocket" -v  # API tests only
pytest -m "websocket" -v                      # WebSocket tests only
```

### VCR Cassettes
- **API Integration Tests**: Use VCR to record/replay HTTP requests for reproducibility
- **WebSocket Tests**: Cannot use VCR - use real WebSocket connections with timeouts

## 🔍 Test Validation

Each test validates:

### Financial Data Integrity
- All monetary values are `Decimal` type (never `float`)
- Prices respect exchange tick sizes
- Quantities respect exchange step sizes and minimums
- No hardcoded financial values anywhere

### Model Consistency
- Data models maintain consistent field types across operations
- Symbol representations are consistent across services
- Timestamp handling is timezone-aware throughout

### Error Handling
- Critical failures cause test failures (no silent ignoring)
- Business logic errors vs system errors are properly distinguished
- Network errors are handled separately from API errors

### Performance Requirements
- API operations complete within reasonable timeframes
- WebSocket message processing meets latency requirements
- Concurrent operations maintain data integrity

## 🚨 Critical Failure Scenarios

These tests will **FAIL** if:

1. **Hardcoded values detected**: Any use of arbitrary financial values
2. **Precision loss**: Float arithmetic in financial calculations
3. **Stale data**: Market data older than acceptable thresholds
4. **Silent failures**: Critical operations failing without test failure
5. **Race conditions**: Operations assuming immediate consistency
6. **Timezone issues**: Naive datetime objects in financial operations

## 🛠️ Maintenance

### Adding New Tests
1. **Follow security rules**: Consult `TESTING_SECURITY_RULES.md` first
2. **Use dynamic data**: Never hardcode financial values
3. **Validate precision**: Ensure all financial data uses `Decimal`
4. **Test error cases**: Validate both success and failure scenarios
5. **Document thoroughly**: Explain what each test validates and why

### Updating Tests
1. **Preserve security**: Maintain fail-fast error handling
2. **Update symbols**: Use `symbol_helpers.py` for any symbol references
3. **Validate changes**: Ensure updates don't introduce security violations
4. **Test thoroughly**: Run full test suite after changes

## 📚 Related Documentation

- `../../../TESTING_SECURITY_RULES.md`: Complete security rules and examples
- `symbol_helpers.py`: Dynamic symbol retrieval documentation
- `test_helpers.py`: Enhanced test utilities with polling and validation

## 💡 Best Practices

1. **Always use real data**: Get prices, constraints, and symbols from exchange APIs
2. **Fail fast**: If a critical operation fails, fail the test immediately
3. **Validate precision**: Check that financial data maintains Decimal precision
4. **Test error paths**: Ensure error handling works correctly
5. **Use polling**: Never assume operations complete immediately
6. **Validate freshness**: Ensure data is recent enough for trading decisions

These tests serve as the foundation for safe, reliable trading operations by ensuring all API components work together correctly with real market data and proper error handling.
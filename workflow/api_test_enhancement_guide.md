# API Client Test Enhancement Guide

## Overview

This document provides actionable guidance for implementing comprehensive edge case and failure scenario testing for API client classes, building on the patterns established in the enhanced `HyperliquidMarketDataService` tests.

## ✅ IMPLEMENTATION STATUS

### Completed Enhancements

1. **✅ HyperliquidMarketDataService Tests** - `tests/unit/apis/hyperliquid/services/test_hl_market_data_service.py`
   - Added 25+ comprehensive edge case and failure scenario tests
   - Covers unexpected data structures, APIError propagation, mapper failures, boundary conditions
   - All tests passing with clean linter status

2. **✅ BackpackAPI Tests** - `tests/unit/apis/backpack/test_bp_api.py`
   - Added comprehensive error handling tests for API layer
   - Tests service error propagation, validation failures, authentication issues
   - Covers batch operation failures and edge cases
   - All tests passing with clean linter status

3. **✅ HyperliquidAPI Tests** - `tests/unit/apis/hyperliquid/test_hl_api.py`
   - Added comprehensive error handling tests for API layer
   - Tests data retrieval failures, trading operation errors, input validation
   - Covers Hyperliquid-specific scenarios and service integration
   - All tests passing with clean linter status

4. **🔄 WebSocketManager Tests** - `tests/unit/apis/connectivity/test_ws_manager.py`
   - Added basic comprehensive error handling tests
   - Covers connection failures, message handling errors, resource management
   - Some advanced tests require refactoring due to protected member access

### Test Coverage Improvements

The enhancements have significantly improved test coverage in the following areas:

#### I. Data Structure Validation
- **Unexpected Response Structures**: Tests how APIs handle when services return structurally different data
- **Empty Successful Responses**: Validates behavior when APIs return 200 OK with empty payloads
- **Malformed Data**: Tests handling of invalid JSON, missing fields, wrong types

#### II. Error Propagation Chains
- **Service Layer Errors**: Validates that APIErrors from services are properly propagated
- **HTTP Client Errors**: Tests handling of network timeouts, connection failures, SSL errors
- **Authentication Failures**: Covers signature validation, expired tokens, permission issues

#### III. Boundary Conditions
- **Input Validation**: Tests with None inputs, empty strings, invalid ranges
- **Resource Limits**: Validates behavior at API rate limits, connection limits
- **Edge Case Values**: Tests with extreme values, boundary conditions

#### IV. Failure Isolation
- **Service Independence**: Ensures errors in one service don't affect others
- **Exception Handling**: Validates that unexpected exceptions are properly wrapped
- **Resource Cleanup**: Tests proper cleanup during failures

## Core Testing Philosophy

1. **Test the Orchestration, Not the Implementation**: Focus on how API classes coordinate their dependencies rather than the internal logic of those dependencies.
2. **Mock All Collaborators**: Use comprehensive mocking of services, mappers, HTTP clients, and authenticators.
3. **Test Error Propagation**: Verify that errors from dependencies are properly caught, wrapped, and propagated.
4. **Validate Boundary Conditions**: Test edge cases like None inputs, empty responses, and invalid ranges.

## Implementation Patterns

### Pattern 1: Service Error Propagation Testing

```python
@pytest.mark.asyncio
async def test_get_balances_service_validation_error(
    self, api_with_di: Callable[..., APIClass], mock_service: MagicMock
) -> None:
    """Test API handles service ValidationError gracefully."""
    api = api_with_di()
    
    # Mock service to raise APIError wrapping ValidationError
    mock_service.get_balances.side_effect = APIError(
        message="Invalid response structure",
        code=APIErrorCode.INVALID_RESPONSE.value,
        original_exception=ValidationError.from_exception_data(
            title="BalanceModel", line_errors=[]
        ),
    )
    
    with pytest.raises(APIError) as exc_info:
        await api.get_balances()
    
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "Invalid response structure" in exc_info.value.message
    mock_service.get_balances.assert_called_once()
    
    await api.close()
```

### Pattern 2: Boundary Condition Testing

```python
@pytest.mark.asyncio
async def test_get_ticker_none_symbol_input(
    self, api_with_di: Callable[..., APIClass]
) -> None:
    """Test API behavior with None symbol input."""
    api = api_with_di()
    
    # Should handle None input gracefully
    with pytest.raises((APIError, TypeError, ValueError)):
        await api.get_ticker(None)  # type: ignore[arg-type]
    
    await api.close()
```

### Pattern 3: Multiple Error Scenario Testing

```python
@pytest.mark.asyncio
async def test_multiple_service_error_isolation(
    self, api_with_di: Callable[..., APIClass], 
    mock_account_service: MagicMock,
    mock_trading_service: MagicMock,
    mock_market_data_service: MagicMock
) -> None:
    """Test that errors in one service don't affect others."""
    api = api_with_di()
    
    # Configure different behaviors for different services
    mock_account_service.get_balances.side_effect = APIError(
        message="Account service error",
        code=APIErrorCode.RATE_LIMITED.value,
    )
    mock_trading_service.get_open_orders.return_value = []  # Success
    mock_market_data_service.get_ticker.return_value = None  # Success
    
    # Account service should fail
    with pytest.raises(APIError):
        await api.get_balances()
    
    # Other services should still work
    orders = await api.get_open_orders()
    assert orders == []
    
    ticker = await api.get_ticker("BTC")
    assert ticker is None
    
    await api.close()
```

## Test Organization Guidelines

### File Structure
- Group tests by functionality (data retrieval, trading actions, WebSocket, initialization)
- Use descriptive class names: `TestAPIClassComprehensiveErrorHandling`
- Organize test methods with clear section comments

### Test Method Naming
- Use descriptive names that indicate the scenario being tested
- Include the expected outcome in the name
- Examples:
  - `test_get_balances_service_validation_error`
  - `test_place_order_insufficient_funds_propagation`
  - `test_get_ticker_empty_successful_response`

### Documentation
- Each test should have a clear docstring explaining the scenario
- Use comments to explain complex setup or assertions
- Document the expected behavior and why it's important

## WebSocket Testing Considerations

WebSocket testing requires special attention due to:
- Asynchronous message handling
- Connection state management
- Reconnection logic
- Message parsing and routing

### Key WebSocket Test Scenarios
1. **Connection Failures**: Timeout, refused, SSL errors
2. **Message Handler Exceptions**: Ensure they don't crash connections
3. **Malformed Messages**: JSON parsing errors, unexpected formats
4. **Send Operation Failures**: When not connected, transmission errors
5. **Resource Management**: Proper cleanup, double-close safety

## Authentication Service Testing

Authentication services require testing of:
- Signature generation and validation
- Token expiration handling
- Permission and scope validation
- Rate limiting and retry logic

## Trading Service Testing

Trading services need comprehensive testing of:
- Order placement edge cases
- Cancellation scenarios
- Position management
- Risk validation
- Market condition handling

## Next Steps

1. **Complete WebSocket Manager Tests**: Refactor to avoid protected member access
2. **Add Authentication Service Tests**: Implement comprehensive auth failure scenarios
3. **Enhance Trading Service Tests**: Add more complex trading scenarios
4. **Integration Test Expansion**: Add end-to-end failure scenario tests
5. **Performance Test Addition**: Add load and stress testing scenarios

## Maintenance

- Review and update tests when API interfaces change
- Add new test scenarios as edge cases are discovered in production
- Ensure test performance remains acceptable as coverage grows
- Regular review of test effectiveness and coverage gaps

## I. API Class Public Interface Testing Patterns

### A. Data Retrieval Methods (`get_ticker`, `get_balances`, etc.)

```python
# Pattern 1: Service Returns Unexpected Data Structure
@pytest.mark.asyncio
async def test_get_ticker_service_validation_error(
    api_instance: BackpackAPI,
    mock_market_data_service: MagicMock,
) -> None:
    """Test API handles service ValidationError gracefully."""
    symbol = "BTC"
    
    # Mock service to raise APIError wrapping ValidationError
    mock_market_data_service.get_ticker.side_effect = APIError(
        message="Invalid ticker response structure",
        code=APIErrorCode.INVALID_RESPONSE.value,
        original_exception=ValidationError.from_exception_data(
            title="TickerModel", line_errors=[]
        ),
    )
    
    with pytest.raises(APIError) as exc_info:
        await api_instance.get_ticker(symbol)
    
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    mock_market_data_service.get_ticker.assert_called_once_with(symbol)

# Pattern 2: Service Returns None/Empty but Successful
@pytest.mark.asyncio
async def test_get_balances_empty_successful_response(
    api_instance: BackpackAPI,
    mock_account_service: MagicMock,
) -> None:
    """Test API handles empty but successful balance response."""
    # Mock service to return empty list
    mock_account_service.get_balances.return_value = []
    
    result = await api_instance.get_balances()
    
    assert result == []
    mock_account_service.get_balances.assert_called_once()

# Pattern 3: Various APIError Code Propagation
@pytest.mark.asyncio
async def test_get_ticker_rate_limited_propagation(
    api_instance: BackpackAPI,
    mock_market_data_service: MagicMock,
) -> None:
    """Test API propagates RATE_LIMITED from service correctly."""
    symbol = "ETH"
    
    mock_market_data_service.get_ticker.side_effect = APIError(
        message="Rate limit exceeded",
        code=APIErrorCode.RATE_LIMITED.value,
        http_status=429,
    )
    
    with pytest.raises(APIError) as exc_info:
        await api_instance.get_ticker(symbol)
    
    assert exc_info.value.code == APIErrorCode.RATE_LIMITED.value
    assert exc_info.value.http_status == 429
```

### B. Authenticated Action Methods (`place_order`, `cancel_order`, etc.)

```python
# Pattern 4: Authenticator Failure During Request Preparation
@pytest.mark.asyncio
async def test_place_order_authenticator_failure(
    api_instance: BackpackAPI,
    mock_authenticator: MagicMock,
    mock_trading_service: MagicMock,
) -> None:
    """Test place_order handles authenticator failures correctly."""
    order_request = OrderRequest(
        symbol="BTC",
        side=OrderSide.BUY,
        quantity=Decimal("0.1"),
        price=Decimal("50000"),
    )
    
    # Mock authenticator to fail during request preparation
    mock_authenticator.prepare_request.side_effect = APIError(
        message="Invalid private key for signing",
        code=APIErrorCode.AUTHENTICATION_FAILED.value,
    )
    
    with pytest.raises(APIError) as exc_info:
        await api_instance.place_order(order_request)
    
    assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
    # Trading service should not be called if authentication fails
    mock_trading_service.place_order.assert_not_called()

# Pattern 5: Service Unexpected Exception Wrapping
@pytest.mark.asyncio
async def test_cancel_order_service_unexpected_exception(
    api_instance: BackpackAPI,
    mock_trading_service: MagicMock,
) -> None:
    """Test API wraps unexpected service exceptions correctly."""
    order_id = "order_123"
    
    # Mock service to raise unexpected exception
    mock_trading_service.cancel_order.side_effect = RuntimeError(
        "Unexpected service failure"
    )
    
    with pytest.raises(APIError) as exc_info:
        await api_instance.cancel_order(order_id)
    
    # API should wrap unexpected exceptions
    assert exc_info.value.code in [APIErrorCode.UNKNOWN.value, APIErrorCode.EXCHANGE_SPECIFIC.value]
    assert "RuntimeError" in str(exc_info.value.original_exception)
```

### C. Input Validation and Boundary Testing

```python
# Pattern 6: None Input Handling
@pytest.mark.asyncio
async def test_get_ticker_none_symbol_input(
    api_instance: BackpackAPI,
) -> None:
    """Test API handles None symbol input gracefully."""
    with pytest.raises((APIError, TypeError, ValueError)):
        await api_instance.get_ticker(None)  # type: ignore[arg-type]

# Pattern 7: Empty String Input Handling
@pytest.mark.asyncio
async def test_get_order_book_empty_symbol(
    api_instance: BackpackAPI,
    mock_market_data_service: MagicMock,
) -> None:
    """Test API handles empty symbol gracefully."""
    symbol = ""
    
    # Service might return None for empty symbol
    mock_market_data_service.get_order_book.return_value = None
    
    result = await api_instance.get_order_book(symbol)
    assert result is None
    mock_market_data_service.get_order_book.assert_called_once_with(symbol)
```

## II. WebSocket Interaction Testing Patterns

### A. Subscription Management Testing

```python
# Pattern 8: Invalid Topic Subscription
@pytest.mark.asyncio
async def test_subscribe_invalid_topic(
    api_instance: BackpackAPI,
    mock_ws_manager: MagicMock,
) -> None:
    """Test subscription to invalid/unsupported topic."""
    invalid_topic = "invalid_channel"
    mock_handler = MagicMock()
    
    # Mock topic validation to return None (invalid topic)
    with patch.object(api_instance, '_construct_subscription_payload', return_value=None):
        result = api_instance.subscribe(invalid_topic, mock_handler)
    
    assert result is False  # or raises exception, depending on design
    mock_ws_manager.send_json.assert_not_called()

# Pattern 9: WebSocket Manager Not Connected
@pytest.mark.asyncio
async def test_subscribe_ws_not_connected(
    api_instance: BackpackAPI,
    mock_ws_manager: MagicMock,
) -> None:
    """Test subscription when WebSocket is not connected."""
    topic = "depth@BTC"
    mock_handler = MagicMock()
    
    # Mock WebSocket manager as not connected
    mock_ws_manager.is_connected = False
    
    # API should handle gracefully - either queue for later or log warning
    with patch.object(api_instance, '_construct_subscription_payload', 
                     return_value={"channel": "depth", "symbol": "BTC"}):
        result = api_instance.subscribe(topic, mock_handler)
    
    # Depending on design: might return False, log warning, or queue subscription
    mock_ws_manager.send_json.assert_not_called()

# Pattern 10: WebSocket Send Failure
@pytest.mark.asyncio
async def test_subscribe_ws_send_failure(
    api_instance: BackpackAPI,
    mock_ws_manager: MagicMock,
) -> None:
    """Test subscription when WebSocket send fails."""
    topic = "trades@ETH"
    mock_handler = MagicMock()
    subscription_payload = {"channel": "trades", "symbol": "ETH"}
    
    mock_ws_manager.is_connected = True
    mock_ws_manager.send_json.return_value = False  # Send failed
    
    with patch.object(api_instance, '_construct_subscription_payload', 
                     return_value=subscription_payload):
        result = api_instance.subscribe(topic, mock_handler)
    
    assert result is False
    mock_ws_manager.send_json.assert_called_once_with(subscription_payload)
```

### B. WebSocket Message Processing Testing

```python
# Pattern 11: Malformed WebSocket Message Structure
@pytest.mark.asyncio
async def test_ws_message_malformed_structure(
    api_instance: BackpackAPI,
    mock_ws_message_handler: MagicMock,
) -> None:
    """Test handling of malformed WebSocket message structure."""
    # Mock message missing critical keys
    malformed_message = {
        "data": {"price": "50000"},
        # Missing 'channel' or 'topic' key
    }
    
    # Simulate message processing (this depends on your actual WS message routing)
    with patch.object(api_instance, '_route_websocket_message') as mock_router:
        api_instance._handle_websocket_message(malformed_message)
    
    # Should handle gracefully without calling handler
    mock_router.assert_not_called()

# Pattern 12: WS Message Handler Validation Error
@pytest.mark.asyncio
async def test_ws_message_handler_validation_error(
    api_instance: BackpackAPI,
    mock_ws_message_handler: MagicMock,
) -> None:
    """Test WebSocket message with invalid data payload."""
    message = {
        "channel": "depth",
        "data": {
            "bids": "invalid_should_be_list",  # Wrong type
            "asks": None,  # Missing data
        }
    }
    
    # Mock handler to raise APIError for validation failure
    mock_ws_message_handler.handle_depth_payload.side_effect = APIError(
        message="Invalid depth data structure",
        code=APIErrorCode.INVALID_RESPONSE.value,
    )
    
    # API should log error and not crash
    with patch.object(api_instance, '_registered_handlers', {"depth": [MagicMock()]}):
        api_instance._handle_websocket_message(message)
    
    # Handler should have been called and failed gracefully
    mock_ws_message_handler.handle_depth_payload.assert_called_once()

# Pattern 13: Application Handler Exception
@pytest.mark.asyncio
async def test_ws_application_handler_exception(
    api_instance: BackpackAPI,
) -> None:
    """Test WebSocket processing continues when app handler crashes."""
    def failing_handler(data: Any) -> None:
        raise RuntimeError("Application handler crashed")
    
    # Register failing handler
    api_instance.subscribe("trades@BTC", failing_handler)
    
    valid_message = {
        "channel": "trades",
        "data": [{"price": "50000", "quantity": "0.1"}]
    }
    
    # Mock successful message parsing
    with patch.object(api_instance, '_ws_message_handler') as mock_handler:
        parsed_trade = MagicMock()  # Valid parsed trade
        mock_handler.handle_trades_payload.return_value = [parsed_trade]
        
        # Should not raise exception even though app handler crashes
        api_instance._handle_websocket_message(valid_message)
    
    # Message handler should have been called
    mock_handler.handle_trades_payload.assert_called_once()
```

### C. WebSocket Connection Lifecycle Testing

```python
# Pattern 14: Resubscription Failure on Reconnect
@pytest.mark.asyncio
async def test_resubscribe_partial_failure(
    api_instance: BackpackAPI,
    mock_ws_manager: MagicMock,
) -> None:
    """Test resubscription when some subscriptions fail on reconnect."""
    # Setup initial subscriptions
    handler1 = MagicMock()
    handler2 = MagicMock()
    api_instance.subscribe("depth@BTC", handler1)
    api_instance.subscribe("trades@ETH", handler2)
    
    # Mock subscription payload construction to fail for one topic
    def construct_side_effect(topic: str) -> dict[str, Any] | None:
        if "depth@BTC" in topic:
            return {"channel": "depth", "symbol": "BTC"}
        elif "trades@ETH" in topic:
            raise ValueError("Invalid topic format")
        return None
    
    with patch.object(api_instance, '_construct_subscription_payload', 
                     side_effect=construct_side_effect):
        mock_ws_manager.send_json.side_effect = [True, False]  # First succeeds, second fails
        
        # Trigger resubscription (simulate reconnection)
        api_instance._on_ws_connected()
    
    # Should have attempted both subscriptions
    assert mock_ws_manager.send_json.call_count == 1  # Only successful one called
```

## III. Configuration and Initialization Testing

```python
# Pattern 15: Component Initialization Failure
@pytest.mark.asyncio
async def test_api_init_service_failure(
    mock_config: MagicMock,
) -> None:
    """Test API initialization when service initialization fails."""
    # Mock config that would cause service init to fail
    mock_config.market_data_service_config = None  # Missing required config
    
    with pytest.raises(APIError) as exc_info:
        BackpackAPI(config=mock_config)
    
    assert exc_info.value.code == APIErrorCode.CONFIGURATION_ERROR.value

# Pattern 16: Missing Critical Configuration
@pytest.mark.asyncio
async def test_api_init_missing_auth_config(
    mock_config: MagicMock,
) -> None:
    """Test API initialization with missing authentication configuration."""
    mock_config.authentication = None  # Missing auth config
    
    # Should either initialize with warnings or fail gracefully
    with pytest.raises((APIError, ValueError)):
        BackpackAPI(config=mock_config)
```

## IV. Integration and End-to-End Error Chain Testing

```python
# Pattern 17: Full Error Chain Validation
@pytest.mark.asyncio
async def test_place_order_complete_error_chain(
    api_instance: BackpackAPI,
    mock_authenticator: MagicMock,
    mock_trading_service: MagicMock,
    mock_order_validator: MagicMock,
) -> None:
    """Test error handling through complete place_order call chain."""
    order_request = OrderRequest(
        symbol="BTC",
        side=OrderSide.BUY,
        quantity=Decimal("0.1"),
        price=Decimal("50000"),
    )
    
    # Test Scenario 1: Validator fails
    mock_order_validator.validate.side_effect = APIError(
        message="Invalid order size",
        code=APIErrorCode.INVALID_REQUEST.value,
    )
    
    with pytest.raises(APIError) as exc_info:
        await api_instance.place_order(order_request)
    
    assert exc_info.value.code == APIErrorCode.INVALID_REQUEST.value
    # Later components should not be called
    mock_authenticator.prepare_request.assert_not_called()
    mock_trading_service.place_order.assert_not_called()

# Pattern 18: Complex Multi-Step Operation Failure
@pytest.mark.asyncio
async def test_batch_operation_partial_failure(
    api_instance: BackpackAPI,
    mock_trading_service: MagicMock,
) -> None:
    """Test batch operation with some successes and some failures."""
    order_requests = [
        OrderRequest(symbol="BTC", side=OrderSide.BUY, quantity=Decimal("0.1"), price=Decimal("50000")),
        OrderRequest(symbol="ETH", side=OrderSide.SELL, quantity=Decimal("1.0"), price=Decimal("3000")),
        OrderRequest(symbol="SOL", side=OrderSide.BUY, quantity=Decimal("10"), price=Decimal("100")),
    ]
    
    # Mock service to succeed for first, fail for second, succeed for third
    def place_order_side_effect(request: OrderRequest) -> Order:
        if request.symbol == "ETH":
            raise APIError(
                message="Insufficient balance",
                code=APIErrorCode.INSUFFICIENT_BALANCE.value,
            )
        return MagicMock(spec=Order, symbol=request.symbol)
    
    mock_trading_service.place_order.side_effect = place_order_side_effect
    
    # API should handle partial failures gracefully
    results = await api_instance.place_batch_orders(order_requests)
    
    # Verify results contain both successes and failures
    assert len(results) == 3
    assert results[0].success is True  # BTC succeeded
    assert results[1].success is False  # ETH failed
    assert results[1].error.code == APIErrorCode.INSUFFICIENT_BALANCE.value
    assert results[2].success is True  # SOL succeeded
```

## V. Testing Implementation Guidelines

### A. Test Organization
- Group tests by functionality (data retrieval, trading actions, WebSocket, initialization)
- Use descriptive test names that indicate the failure scenario being tested
- Include docstrings explaining the specific edge case or failure mode

### B. Mock Configuration Best Practices
- Use `spec` parameter in MagicMock to maintain type safety
- Configure mocks to return realistic data structures
- Use `side_effect` for complex error scenarios
- Verify mock interactions to ensure proper orchestration

### C. Assertion Patterns
- Always verify the correct error code and message
- Check that collaborators were called (or not called) as expected
- Validate error chaining and original exception preservation
- Test both positive and negative scenarios for each method

### D. Coverage Goals
- Test each public method with multiple failure scenarios
- Cover all APIError codes that might be encountered
- Test boundary conditions and edge cases
- Validate error propagation through the entire call chain

## VI. Running Enhanced Tests

```bash
# Run specific test categories
.venv/bin/pytest tests/unit/apis/backpack/test_bp_api.py::TestBackpackAPI::test_*_error_* -v

# Run with coverage to identify gaps
.venv/bin/pytest tests/unit/apis/ --cov=cyberdelta.apis --cov-report=html

# Run only edge case and failure scenario tests
.venv/bin/pytest tests/unit/apis/ -k "error or failure or malformed or invalid" -v
```

This comprehensive guide provides the foundation for implementing robust, production-ready test suites that will catch edge cases and ensure your API classes handle failures gracefully. 
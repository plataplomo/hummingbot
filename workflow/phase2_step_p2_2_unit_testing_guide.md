# Phase 2, Step P2.2: WebSocket Logic Routers - Unit Testing Guide

## Overview

This document provides comprehensive guidance for implementing unit tests for the newly created `BackpackWsMessageRouter` and `HyperliquidWsMessageRouter` classes, as well as updating existing WebSocket integration tests.

## Part 1: New Unit Tests for WebSocket Routers

### 1.1 BackpackWsMessageRouter Tests (`test_bp_ws_message_router.py`)

**Location**: `tests/unit/apis/backpack/test_bp_ws_message_router.py`

**Key Testing Areas:**

#### Initialization Testing
- Verify router initializes with all required dependencies
- Test that logger is properly configured
- Validate internal state setup

#### Subscription Payload Construction Testing
```python
def test_construct_subscription_payload_basic_topic(self, router):
    """Test subscription payload construction for basic topics."""
    result = router.construct_subscription_payload("depth.SOL_USDC")
    
    expected = {
        "op": "subscribe",
        "channel": "depth.SOL_USDC",
        "args": {},
    }
    assert result == expected
```

**Test Cases:**
- Basic topics: `depth.SOL_USDC`, `ticker.BTC_USDC`
- Event types: `fills`, `orders`, `positionUpdate`
- Edge cases: empty strings, None values

#### Message Routing Testing

**Core Routing Logic:**
- Test each supported topic type (`depth`, `ticker`, `fills`, `orders`, `positionUpdate`)
- Verify correct raw handler method calls
- Verify correct mapper method calls
- Verify application handler invocation

**Example Test Pattern:**
```python
@pytest.mark.asyncio
async def test_route_message_depth_topic(
    self, router, mock_raw_ws_handler, mock_market_data_mapper, mock_app_handler
):
    """Test routing depth messages."""
    message = {
        "topic": "depth.SOL_USDC",
        "data": {"bids": [], "asks": []},
    }
    ws_handlers = {"depth.SOL_USDC": mock_app_handler}

    await router.route_message(message, ws_handlers)

    # Verify pipeline: raw handler -> mapper -> app handler
    mock_raw_ws_handler.handle_depth_payload.assert_called_once_with({"bids": [], "asks": []})
    mock_market_data_mapper.transform_ws_depth_event_to_internal.assert_called_once_with(
        "SOL_USDC", {"mock": "depth_data"}
    )
    mock_app_handler.assert_called_once()
```

**Error Path Testing:**
- `APIError` from raw message handler
- `TransformationError` from mappers
- `Exception` in application handler
- Invalid message formats (missing topic, missing data)
- No registered handler for topic

**Edge Case Testing:**
- Base topic fallback (`depth.ETH_USDC` -> `depth` handler)
- Symbol extraction from topics
- Unknown topics (should pass raw data)
- Message format variations (topic vs type field)

#### Mock Strategy

**Dependencies to Mock:**
```python
@pytest.fixture
def mock_market_data_mapper(self) -> Mock:
    mapper = Mock(spec=BackpackMarketDataMapper)
    mapper.transform_ws_depth_event_to_internal = Mock(return_value=Mock())
    mapper.transform_ws_ticker_event_to_internal = Mock(return_value=Mock())
    return mapper

@pytest.fixture
def mock_raw_ws_handler(self) -> Mock:
    handler = Mock(spec=BackpackWsRawMessageHandler)
    handler.handle_depth_payload = Mock(return_value={"mock": "depth_data"})
    handler.handle_ticker_payload = Mock(return_value={"mock": "ticker_data"})
    # ... other handlers
    return handler
```

### 1.2 HyperliquidWsMessageRouter Tests (`test_hl_ws_message_router.py`)

**Location**: `tests/unit/apis/hyperliquid/test_hl_ws_message_router.py`

**Key Testing Areas:**

#### Subscription Payload Construction Testing

**Hyperliquid-Specific Patterns:**
```python
def test_construct_subscription_payload_l2book(self, router):
    """Test subscription payload construction for l2Book."""
    result = router.construct_subscription_payload("l2Book:SOL", None)
    
    expected = {
        "method": "subscribe",
        "subscription": {"type": "l2Book", "coin": "SOL"},
    }
    assert result == expected
```

**Test Cases:**
- Market data: `l2Book:SOL`, `trades:BTC`, `candle:ETH:1m`
- User data: `userEvents` (requires wallet address)
- Control: `allMids`
- Edge cases: invalid topics, missing wallet for userEvents

#### Message Routing Testing

**Channel-Based Routing:**
- `l2Book` channel with coin extraction
- `trades` channel with trade list processing
- `userEvents` channel with event type discrimination
- `allMids` channel with direct processing
- Control channels: `pong`, `subscriptionResponse`

**UserEvents Sub-Routing:**
```python
@pytest.mark.asyncio
async def test_route_message_user_events_fill(
    self, router, mock_raw_ws_handler, mock_account_data_mapper, mock_app_handler
):
    """Test routing userEvents fill messages."""
    message = {
        "channel": "userEvents",
        "data": [{"type": "fill", "fillData": {"coin": "SOL", "px": "100"}}],
    }
    ws_handlers = {"userEvents": mock_app_handler}

    await router.route_message(message, ws_handlers)

    mock_raw_ws_handler.handle_user_fill_event_payload.assert_called_once()
    mock_account_data_mapper.transform_ws_fill_event_to_internal.assert_called_once()
    mock_app_handler.assert_called_once()
```

**Complex Error Handling:**
- `ValidationError` in userEvents processing (should continue with other events)
- Invalid data types (non-dict for l2Book, non-list for trades/userEvents)
- Missing required fields in event data

#### Mock Strategy for Hyperliquid

**Complex Raw Handler Mocking:**
```python
@pytest.fixture
def mock_raw_ws_handler(self) -> Mock:
    handler = Mock(spec=HyperliquidWsRawMessageHandler)
    handler.handle_l2book_payload = Mock(return_value=Mock())
    handler.handle_public_trades_payload = Mock(return_value=[Mock()])
    handler.handle_user_fill_event_payload = Mock(return_value=Mock())
    handler.handle_user_order_update_wrapper_payload = Mock(return_value=Mock(data=Mock()))
    handler.handle_user_order_event_payload = Mock(return_value=Mock())
    handler.handle_user_position_update_event_payload = Mock(return_value=Mock())
    handler.handle_all_mids_payload = Mock(return_value=Mock(model_dump=Mock(return_value={})))
    return handler
```

## Part 2: Updates to Existing WebSocket Tests

### 2.1 Backpack API WebSocket Tests (`test_bp_api_ws.py`)

**Current State**: Tests directly test WebSocket message handling in `BackpackAPI`

**Required Updates:**

#### Mock Router Integration
```python
@pytest.fixture
def mock_bp_ws_router(self) -> Mock:
    """Mock the BackpackWsMessageRouter."""
    router = Mock(spec=BackpackWsMessageRouter)
    router.construct_subscription_payload = Mock(return_value={"op": "subscribe", "channel": "test"})
    router.route_message = AsyncMock()
    return router

@pytest.fixture
def api_with_mocked_router(self, mock_bp_ws_router, ...):
    """Create API instance with mocked router."""
    api = BackpackAPI(...)
    api._bp_ws_router = mock_bp_ws_router
    return api
```

#### Test Focus Shift
- **Before**: Test detailed routing logic within API
- **After**: Test that API correctly delegates to router

**Example Updated Test:**
```python
@pytest.mark.asyncio
async def test_handle_websocket_message_delegates_to_router(
    self, api_with_mocked_router, mock_bp_ws_router
):
    """Test that WebSocket message handling delegates to router."""
    message = {"topic": "depth.SOL_USDC", "data": {"bids": [], "asks": []}}
    
    await api_with_mocked_router._handle_websocket_message(message)
    
    mock_bp_ws_router.route_message.assert_called_once_with(
        message, api_with_mocked_router._ws_handlers
    )
```

#### Integration Test Scope
- WebSocket connection management
- Subscription/unsubscription flow
- Handler registration/deregistration
- Connection error handling
- Reconnection logic

### 2.2 Hyperliquid API WebSocket Tests (`test_hl_api_ws.py`)

**Similar Updates Required:**

#### Mock Router Integration
```python
@pytest.fixture
def mock_hl_ws_router(self) -> Mock:
    """Mock the HyperliquidWsMessageRouter."""
    router = Mock(spec=HyperliquidWsMessageRouter)
    router.construct_subscription_payload = Mock(return_value={"method": "subscribe"})
    router.route_message = AsyncMock()
    return router
```

#### Test Focus Areas
- Delegation to router for message processing
- Wallet address passing for userEvents subscriptions
- Connection lifecycle management
- Error handling and recovery

## Part 3: Testing Best Practices

### 3.1 Mock Design Principles

**Isolation**: Each router test should mock all dependencies to test routing logic in isolation

**Realistic Returns**: Mock return values should match expected types from real implementations

**Error Simulation**: Use `side_effect` to simulate various error conditions

**Call Verification**: Always verify that mocked methods are called with expected arguments

### 3.2 Test Coverage Goals

**Router Unit Tests:**
- 100% line coverage for routing logic
- All error paths tested
- All supported message types tested
- Edge cases and invalid inputs tested

**API Integration Tests:**
- Delegation to router verified
- WebSocket lifecycle management tested
- Error propagation tested

### 3.3 Test Organization

**File Structure:**
```
tests/unit/apis/backpack/
├── test_bp_ws_message_router.py      # New router tests
├── test_bp_api_ws.py                 # Updated API WebSocket tests
└── ...

tests/unit/apis/hyperliquid/
├── test_hl_ws_message_router.py      # New router tests
├── test_hl_api_ws.py                 # Updated API WebSocket tests
└── ...
```

**Test Class Organization:**
- One test class per router class
- Logical grouping of test methods by functionality
- Clear, descriptive test method names
- Comprehensive docstrings

### 3.4 Async Testing Patterns

**Use `pytest.mark.asyncio`** for all async test methods

**Mock Async Dependencies** with `AsyncMock` for application handlers

**Test Async Error Handling** to ensure exceptions don't propagate unexpectedly

## Part 4: Implementation Priority

### Phase 1: Router Unit Tests
1. Create `test_bp_ws_message_router.py` with comprehensive coverage
2. Create `test_hl_ws_message_router.py` with comprehensive coverage
3. Run tests to verify router isolation and functionality

### Phase 2: API Test Updates
1. Update `test_bp_api_ws.py` to mock router and test delegation
2. Update `test_hl_api_ws.py` to mock router and test delegation
3. Verify integration test coverage remains comprehensive

### Phase 3: Validation
1. Run full test suite to ensure no regressions
2. Verify test coverage metrics meet project standards
3. Review test quality and maintainability

## Conclusion

This testing strategy ensures that:
- Router logic is thoroughly tested in isolation
- API integration with routers is verified
- Error handling is comprehensive
- Test maintenance burden is minimized
- Code coverage goals are met

The separation of router unit tests from API integration tests provides better test isolation, faster test execution, and clearer failure diagnosis when issues arise. 
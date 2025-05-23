# Step 3: Align Test Expectations with Code's Validation Logic

## Goal
Ensure tests assert errors raised by the correct layer of code, matching the actual validation and error propagation flow.

## Problem Analysis
Tests often expect `APIError` from service layers when the API client itself validates inputs and raises `ValueError`/`TypeError` before the service is called. This creates a mismatch between test expectations and actual code behavior.

## Validation Layer Hierarchy

```
User Input → API Client Validation → Service Call → Response Processing
    ↓              ↓                    ↓              ↓
ValueError/    ValueError/          APIError        APIError
TypeError      TypeError            (wrapped)       (wrapped)
(direct)       (direct)
```

## Specific Test Fixes

### 3.1 Cancel Order Tests - Missing Symbol Parameter

**Files**: 
- `tests/unit/apis/backpack/test_bp_api.py`
- `tests/unit/apis/hyperliquid/test_hl_api.py`

**Problem**: Tests call `api.cancel_order(order_id)` without required `symbol` parameter, causing immediate `ValueError` instead of testing service error propagation.

**Fix**:
```python
# BEFORE (Fails at API client validation)
def test_cancel_order_insufficient_balance_propagation(self, mock_trading_service):
    mock_trading_service.cancel_order.side_effect = APIError(
        message="Insufficient balance", 
        code=APIErrorCode.INSUFFICIENT_FUNDS.value
    )
    
    with pytest.raises(APIError) as exc_info:
        # This raises ValueError for missing symbol before service is called
        await api.cancel_order("order123")

# AFTER (Properly tests service error propagation)
def test_cancel_order_insufficient_balance_propagation(self, mock_trading_service):
    mock_trading_service.cancel_order.side_effect = APIError(
        message="Insufficient balance", 
        code=APIErrorCode.INSUFFICIENT_FUNDS.value
    )
    
    with pytest.raises(APIError) as exc_info:
        # Now tests actual service error propagation
        await api.cancel_order("order123", symbol="BTC-PERP")
    
    assert exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value
```

### 3.2 Get Order Tests - Invalid Order ID Format

**Files**: `tests/unit/apis/hyperliquid/test_hl_api.py`

**Problem**: Test uses non-integer-parseable `order_id` string, causing `ValueError` from `int()` conversion instead of testing `ORDER_NOT_FOUND` propagation.

**Fix**:
```python
# BEFORE (Fails at int() conversion)
def test_get_order_order_not_found_propagation(self, mock_trading_service):
    mock_trading_service.get_order.side_effect = APIError(
        message="Order not found",
        code=APIErrorCode.ORDER_NOT_FOUND.value
    )
    
    with pytest.raises(APIError) as exc_info:
        # "12345" -> int("12345") fails in some contexts
        await api.get_order("12345", symbol="BTC-PERP")

# AFTER (Uses valid integer format)
def test_get_order_order_not_found_propagation(self, mock_trading_service):
    mock_trading_service.get_order.side_effect = APIError(
        message="Order not found",
        code=APIErrorCode.ORDER_NOT_FOUND.value
    )
    
    with pytest.raises(APIError) as exc_info:
        # Use clearly integer-parseable string
        await api.get_order("123456789", symbol="BTC-PERP")
    
    assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
```

### 3.3 Get Ticker Tests - None Symbol Input

**Files**: 
- `tests/unit/apis/backpack/test_bp_api.py`
- `tests/unit/apis/hyperliquid/test_hl_api.py`

**Problem**: Tests expect specific error types when `symbol=None` is passed, but the actual validation layer behavior is unclear.

**Analysis Options**:

**Option A**: API client validates and raises `TypeError` directly
```python
def test_get_ticker_none_symbol_input(self, mock_market_data_service):
    # Expect direct TypeError from API client validation
    with pytest.raises(TypeError) as exc_info:
        await api.get_ticker(symbol=None)
    
    assert "symbol cannot be None" in str(exc_info.value)
    # Service should not be called
    mock_market_data_service.get_ticker.assert_not_called()
```

**Option B**: API client passes None to service, service validates and wraps
```python
def test_get_ticker_none_symbol_input(self, mock_market_data_service):
    # Mock service to raise TypeError when symbol=None
    mock_market_data_service.get_ticker.side_effect = TypeError("symbol cannot be None")
    
    # Expect API client to wrap service TypeError as APIError (per Step 2)
    with pytest.raises(APIError) as exc_info:
        await api.get_ticker(symbol=None)
    
    assert exc_info.value.code == APIErrorCode.INVALID_REQUEST.value
    assert isinstance(exc_info.value.original_exception, TypeError)
```

**Recommendation**: Implement Option A - API client validates non-None symbol before service call.

### 3.4 Test Categories by Validation Layer

| Test Category | Expected Exception | Validation Layer | Fix Required |
|---------------|-------------------|------------------|--------------|
| Missing required params | `ValueError` | API Client | Add required params to test |
| Invalid param types | `TypeError` | API Client | Use valid types or expect `TypeError` |
| Invalid param values | `ValueError` | API Client | Use valid values or expect `ValueError` |
| Service business logic | `APIError` | Service | Ensure params pass API client validation |
| Network/HTTP errors | `APIError` | Service | Mock service to raise appropriate errors |
| Response parsing errors | `APIError` | Response Handler | Mock malformed responses |

### 3.5 Implementation Strategy

1. **Audit test parameters**: Ensure all test inputs pass API client validation layer
2. **Categorize test intentions**: Distinguish between API client validation tests vs service error propagation tests
3. **Fix parameter issues**: Add missing required parameters, use valid formats
4. **Separate test concerns**: Create separate tests for API client validation vs service error propagation

## Validation Criteria
- Tests fail at intended validation layer
- No tests fail due to missing required parameters
- Service error propagation tests actually reach the service layer
- API client validation tests target the correct validation logic 
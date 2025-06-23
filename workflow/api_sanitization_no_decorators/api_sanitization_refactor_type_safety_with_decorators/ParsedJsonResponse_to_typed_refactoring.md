# ParsedJsonResponse to Typed Raw Models - Comprehensive Refactoring Guide

## Executive Summary

This document provides a detailed, practical refactoring plan to replace `ParsedJsonResponse` with direct typed Pydantic Raw model responses in the CyberDeltaEngine APIs. The goal is simple: eliminate the untyped bottleneck and establish complete type safety from HTTP boundary to domain models.

**Core Objective**: Transform `HttpClient → ParsedJsonResponse → ResponseHandler → Raw Model` into `HttpClient → Raw Model` directly.

## Current Architecture Analysis

### 1. Current ParsedJsonResponse Definition and Usage

**Location**: `/workspaces/CyberDeltaEngine/cyberdelta/apis/connectivity/http_client.py`

```python
# Current problematic definition
ParsedJsonResponse = dict[str, Any] | list[Any] | str

# Used in HttpClient.request() return type
async def request(
    self,
    method: str,
    endpoint_path: str,
    # ... other params
) -> tuple[
    ParsedJsonResponse | str | None,  # ← UNTYPED BOTTLENECK
    int,
    ProcessedResponseHeaders,
    CIMultiDictProxy[str],
]:
```

**The Problem**: `ParsedJsonResponse` is a 3-way union that erases all type information, requiring manual validation at every usage point.

### 2. Current Flow Pattern

```python
# Step 1: HttpClient returns untyped response
raw_response, status_code, headers, _ = await self._http_client_requester(...)
# raw_response is ParsedJsonResponse | str | None

# Step 2: Service calls ResponseHandler for validation
raw_ticker = self._response_handler.handle_get_ticker_response(
    raw_response, symbol, status_code, headers
)
# raw_ticker is now BackpackRawTicker

# Step 3: Service calls Mapper for transformation
internal_ticker = self._mapper.transform_raw_ticker_to_internal(raw_ticker)
# internal_ticker is now Ticker (domain model)
```

### 3. Existing Raw Model Catalog

#### **Backpack Raw Models** (`/workspaces/CyberDeltaEngine/cyberdelta/apis/backpack/models/`)

**Account & Balance Models:**
- `BackpackRawAccount` - Complete account information
- `BackpackRawBalance` - Asset balance details (available, locked, staked)
- `BackpackRawAccountSummary` - Comprehensive account summary
- `BackpackRawCollateralResponse` - Collateral information

**Market Data Models:**
- `BackpackRawTicker` - 24hr ticker statistics
- `BackpackRawMarket` - Market metadata and trading rules
- `BackpackRawKline` - OHLCV candlestick data
- `BackpackRawTrade` (public trades)

**Trading Models:**
- `BackpackRawOrder` - Order information
- `BackpackRawPosition` - Derivative position data
- `BackpackRawFill` - User fill/execution data

**Operational Models:**
- `BackpackRawWithdrawalResponse` - Withdrawal confirmations
- `BackpackRawFundingRate` - Funding rate information
- Various limit/restriction models

**Status**: ✅ **COMPLETE** - All major endpoints have corresponding Raw models

#### **Hyperliquid Raw Models** (`/workspaces/CyberDeltaEngine/cyberdelta/apis/hyperliquid/models/`)

**User State & Account Models:**
- `HyperliquidRawUserStateResponse` - Complete user state
- `HyperliquidRawClearinghouseState` - Clearinghouse state details
- `HyperliquidRawAssetPosition` - Position details per asset
- `HyperliquidRawMarginSummary` - Margin calculations

**Market Data Models:**
- `HyperliquidRawL2Book` - L2 order book snapshot
- `HyperliquidRawAllMids` - All symbol mid prices
- `HyperliquidRawCandleSnapshot` - OHLCV candle data
- `HyperliquidRawMetaAndAssetCtxsResponse` - Market metadata

**Trading Models:**
- `HyperliquidRawOpenOrdersResponse` - Open orders list
- `HyperliquidRawHistoricalOrderResponse` - Order history
- `HyperliquidRawExchangeResponse` - Order/cancel confirmations
- `HyperliquidRawUserFillsResponse` - User fill history

**Status**: ✅ **COMPLETE** - All major endpoints have corresponding Raw models

### 4. Current Response Handler Analysis

#### **Backpack Response Handler Pattern**
**Location**: `/workspaces/CyberDeltaEngine/cyberdelta/apis/backpack/bp_response_handler.py`

```python
@staticmethod
def handle_get_ticker_response(
    raw_response_content: RawJsonResponse,  # Alias for ParsedJsonResponse
    symbol: str,
    status_code: int,
    headers: Mapping[str, str],
) -> BackpackRawTicker:
    """Validate the raw response for the Get Ticker endpoint."""
    context = f"ticker ({symbol}) - Status: {status_code}"
    
    # Manual type validation
    if not isinstance(raw_response_content, dict):
        raise APIError(f"Unexpected {context} response format...")
    
    # Pydantic validation
    try:
        return BackpackRawTicker.model_validate(raw_response_content)
    except ValidationError as e:
        raise BackpackResponseHandler._handle_validation_error(...) from e
```

**Pattern Characteristics:**
- **46 handler methods** covering all Backpack endpoints
- Identical pattern: type check → Pydantic validate → return Raw model
- Context-aware error messages
- Standardized error handling via `_handle_validation_error`

#### **Hyperliquid Response Handler Pattern**
**Location**: `/workspaces/CyberDeltaEngine/cyberdelta/apis/hyperliquid/hl_response_handler.py`

```python
@staticmethod
def handle_info_user_state_response(
    raw_response_content: ParsedJsonResponse,
    user_address: str,
) -> HyperliquidRawUserStateResponse:
    """Validates the /info response for user_state."""
    context = f"info (user state for {user_address})"
    
    # Manual type validation
    if not isinstance(raw_response_content, dict):
        raise APIError(f"Unexpected {context} response format...")
    
    # Pydantic validation
    try:
        return HyperliquidRawUserStateResponse.model_validate(raw_response_content)
    except ValidationError as e:
        raise HyperliquidResponseHandler._handle_validation_error(...) from e
```

**Pattern Characteristics:**
- **26 handler methods** covering all Hyperliquid endpoints
- Identical pattern to Backpack but with Hyperliquid-specific context
- Special handling for edge cases (empty responses, complex structures)

### 5. Current Service Layer Integration

#### **Service Method Example** (`/workspaces/CyberDeltaEngine/cyberdelta/apis/backpack/services/bp_market_data_service.py`)

```python
async def get_ticker(self, symbol: str) -> Ticker:
    """Retrieve the latest ticker information for a specific symbol."""
    try:
        # 1. Build request parameters
        params = self._request_builder.build_get_ticker_params(symbol=symbol)
        
        # 2. Execute HTTP request → get ParsedJsonResponse
        response_tuple = await self._http_client_requester(
            method="GET",
            endpoint="/api/v1/ticker",
            params=params.model_dump(),
            is_signed=False,
        )
        raw_data, status_code, headers = response_tuple
        
        # 3. Validate ParsedJsonResponse → Raw Model
        raw_ticker_model: BackpackRawTicker = self._response_handler.handle_get_ticker_response(
            raw_data, symbol, status_code, headers
        )
        
        # 4. Transform Raw Model → Domain Model
        internal_ticker = self._mapper.transform_raw_ticker_to_internal(
            raw_ticker_model, symbol_override=symbol
        )
        
        return internal_ticker
        
    except APIError:
        raise  # Re-raise APIErrors
    except TransformationError as e:
        # Handle transformation errors
        raise APIError(...) from e
```

**Service Layer Problems:**
1. **Repetitive Validation**: Every service method manually calls ResponseHandler
2. **Type Safety Gap**: `raw_data` is untyped between HttpClient and ResponseHandler
3. **Error Handling Duplication**: Similar try/catch patterns in every method
4. **Mixed Concerns**: Services handle both HTTP and validation concerns

## Proposed Refactoring Strategy

### Goal: Direct Typed Responses

Transform the current 4-layer pattern:
```
HttpClient → ParsedJsonResponse → ResponseHandler → Raw Model → Mapper → Domain Model
```

Into a clean 3-layer pattern:
```
HttpClient → Raw Model → Mapper → Domain Model
```

### Implementation Approach: Modified HttpClient

**Strategy**: Add optional generic type parameter to existing HttpClient, making it backward-compatible while enabling typed responses.

#### **Step 1: Enhanced HttpClient Interface**

```python
# File: cyberdelta/apis/connectivity/http_client.py

from typing import TypeVar, Type, overload
from pydantic import BaseModel

T = TypeVar('T', bound=BaseModel)

class HttpClient:
    """Enhanced HTTP client with optional typed responses."""
    
    # Keep existing method for backward compatibility
    async def request(
        self,
        method: str,
        endpoint_path: str,
        authenticator: IAuthenticator | None = None,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        is_signed: bool = False,
        request_timeout: float | None = None,
        serialize_none_as_null: bool = False,
    ) -> tuple[
        ParsedJsonResponse | str | None,
        int,
        ProcessedResponseHeaders,
        CIMultiDictProxy[str],
    ]:
        """Original method - maintains backward compatibility."""
        # Existing implementation unchanged
        pass
    
    # NEW: Add typed request method
    @overload
    async def typed_request(
        self,
        method: str,
        endpoint_path: str,
        response_model: Type[T],
        *,
        authenticator: IAuthenticator | None = None,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        is_signed: bool = False,
        request_timeout: float | None = None,
        serialize_none_as_null: bool = False,
    ) -> tuple[T, int, ProcessedResponseHeaders, CIMultiDictProxy[str]]:
        ...
    
    @overload
    async def typed_request(
        self,
        method: str,
        endpoint_path: str,
        response_model: Type[list[T]],
        *,
        authenticator: IAuthenticator | None = None,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        is_signed: bool = False,
        request_timeout: float | None = None,
        serialize_none_as_null: bool = False,
    ) -> tuple[list[T], int, ProcessedResponseHeaders, CIMultiDictProxy[str]]:
        ...
    
    async def typed_request(
        self,
        method: str,
        endpoint_path: str,
        response_model: Type[T] | Type[list[T]],
        **kwargs: Any,
    ) -> tuple[T | list[T], int, ProcessedResponseHeaders, CIMultiDictProxy[str]]:
        """Make HTTP request with automatic Pydantic validation."""
        
        # 1. Execute standard HTTP request
        raw_response, status_code, headers, raw_headers = await self.request(
            method=method,
            endpoint_path=endpoint_path,
            authenticator=kwargs.get('authenticator'),
            params=kwargs.get('params'),
            data=kwargs.get('data'),
            headers=kwargs.get('headers'),
            is_signed=kwargs.get('is_signed', False),
            request_timeout=kwargs.get('request_timeout'),
            serialize_none_as_null=kwargs.get('serialize_none_as_null', False),
        )
        
        # 2. Validate response structure
        if raw_response is None:
            raise APIError(
                message=f"No data received from {method} {endpoint_path}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )
        
        # 3. Handle list responses
        if hasattr(response_model, '__origin__') and response_model.__origin__ is list:
            if not isinstance(raw_response, list):
                raise APIError(
                    message=f"Expected list response from {endpoint_path}, got {type(raw_response).__name__}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            
            # Validate each item in the list
            item_model = response_model.__args__[0]
            validated_items = []
            for i, item in enumerate(raw_response):
                try:
                    validated_items.append(item_model.model_validate(item))
                except ValidationError as e:
                    raise APIError(
                        message=f"List item {i} validation failed for {endpoint_path}: {e}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                        http_status=status_code,
                    ) from e
            
            return validated_items, status_code, headers, raw_headers
        
        # 4. Handle single model responses
        else:
            if not isinstance(raw_response, dict):
                raise APIError(
                    message=f"Expected dict response from {endpoint_path}, got {type(raw_response).__name__}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            
            try:
                validated_model = response_model.model_validate(raw_response)
                return validated_model, status_code, headers, raw_headers
            except ValidationError as e:
                raise APIError(
                    message=f"Response validation failed for {endpoint_path}: {e}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                ) from e
```

#### **Step 2: Service Layer Integration**

```python
# File: cyberdelta/apis/backpack/services/bp_market_data_service.py

class BackpackMarketDataService:
    """Market data service with optional typed responses."""
    
    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        http_client: HttpClient,  # Add direct HttpClient reference
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,  # Keep for legacy methods
        mapper: BackpackMarketDataMapper,
        exchange_name: str,
    ) -> None:
        self._http_client_requester = http_client_requester  # Legacy
        self._http_client = http_client  # New typed client
        self._request_builder = request_builder
        self._response_handler = response_handler  # Legacy
        self._mapper = mapper
        self._exchange_name = exchange_name
    
    # NEW: Typed method using direct Raw model response
    async def get_ticker_typed(self, symbol: str) -> Ticker:
        """Get ticker using typed HTTP client - MUCH SIMPLER."""
        try:
            # 1. Build request parameters
            params = self._request_builder.build_get_ticker_params(symbol=symbol)
            
            # 2. Execute typed HTTP request → get BackpackRawTicker directly
            raw_ticker, status_code, headers, _ = await self._http_client.typed_request(
                method="GET",
                endpoint_path="/api/v1/ticker",
                response_model=BackpackRawTicker,
                params=params.model_dump(),
                is_signed=False,
            )
            
            # 3. Transform Raw Model → Domain Model (validation already done!)
            internal_ticker = self._mapper.transform_raw_ticker_to_internal(
                raw_ticker, symbol_override=symbol
            )
            
            return internal_ticker
            
        except APIError:
            raise  # HttpClient already provides proper APIError
        except TransformationError as e:
            # Only handle transformation errors
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to transform exchange data",
                original_exception=e,
                http_status=status_code,
            ) from e
    
    # LEGACY: Keep existing method during transition
    async def get_ticker(self, symbol: str) -> Ticker:
        """Original method - unchanged during migration."""
        # Existing implementation unchanged
        pass
```

### Migration Plan: 4-Phase Approach

#### **Phase 1: Foundation (Week 1)**
- **Add `typed_request` method to HttpClient**
- **Keep existing `request` method unchanged**
- **Add comprehensive unit tests for typed client**
- **No breaking changes - purely additive**

**Files to modify:**
```
cyberdelta/apis/connectivity/http_client.py
tests/apis/connectivity/test_http_client.py (add typed tests)
```

#### **Phase 2: Pilot Implementation (Week 2)**
- **Choose 3-5 simple endpoints for pilot**
- **Add typed methods alongside existing methods**
- **Update factory classes to inject HttpClient reference**
- **Integration testing with live APIs**

**Pilot endpoints (Backpack):**
```
GET /api/v1/ticker → BackpackRawTicker (simple)
GET /api/v1/capital → dict[str, BackpackRawBalance] (dict response)
GET /api/v1/orders → list[BackpackRawOrder] (list response)
```

**Files to modify:**
```
cyberdelta/apis/backpack/services/bp_market_data_service.py (add get_ticker_typed)
cyberdelta/apis/backpack/services/bp_account_service.py (add get_balances_typed)
cyberdelta/apis/backpack/bp_api_components_factory.py (inject HttpClient)
```

#### **Phase 3: Full Migration (Week 3-4)**
- **Migrate all Backpack service methods**
- **Migrate all Hyperliquid service methods**
- **Update factory classes and API constructors**
- **Comprehensive integration testing**

**Migration order by complexity:**
1. Market data services (simple single-object responses)
2. Account services (dict/list responses)
3. Trading services (complex responses with edge cases)

#### **Phase 4: Cleanup (Week 5)**
- **Deprecate old methods (add deprecation warnings)**
- **Update all callers to use typed methods**
- **Remove ResponseHandler classes**
- **Remove legacy `request` method**
- **Update ParsedJsonResponse definition (or remove entirely)**

### Response Type Mapping Strategy

#### **Single Object Responses**
```python
# Direct model mapping
GET /api/v1/ticker → BackpackRawTicker
GET /info (user_state) → HyperliquidRawUserStateResponse
POST /exchange (place_order) → HyperliquidRawExchangeResponse
```

#### **Dictionary Responses** 
```python
# Balance endpoints return asset → balance mapping
GET /api/v1/capital → dict[str, BackpackRawBalance]

# Implementation in typed_request:
# Validate outer dict structure, then validate each value
validated_balances = {
    asset: BackpackRawBalance.model_validate(balance_data)
    for asset, balance_data in raw_response.items()
}
```

#### **List Responses**
```python
# Array endpoints
GET /api/v1/orders → list[BackpackRawOrder]
GET /api/v1/position → list[BackpackRawPosition]

# Implementation in typed_request:
# Validate list structure, then validate each item
validated_orders = [
    BackpackRawOrder.model_validate(order_data)
    for order_data in raw_response
]
```

#### **Complex Response Handling**

**Hyperliquid Meta Response** (complex nested structure):
```python
# Current ResponseHandler does preprocessing
def handle_info_meta_response(
    raw_response_content: ParsedJsonResponse,
) -> HyperliquidRawMetaAndAssetCtxsResponse:
    # Complex validation and preprocessing
    if not isinstance(raw_response_content, dict):
        raise APIError(...)
    
    # Handle nested structures, missing fields, etc.
    return HyperliquidRawMetaAndAssetCtxsResponse.model_validate(raw_response_content)

# In typed_request approach:
# Move preprocessing logic into Raw model field validators
class HyperliquidRawMetaAndAssetCtxsResponse(BaseModel):
    @field_validator('asset_ctxs', mode='before')
    @classmethod
    def validate_asset_ctxs(cls, v: Any) -> list[dict[str, Any]]:
        # Move preprocessing logic here
        if not isinstance(v, list):
            raise ValueError("Expected list for asset_ctxs")
        return v
```

### Error Handling Strategy

#### **Current Error Pattern** (in ResponseHandlers)
```python
try:
    return BackpackRawTicker.model_validate(raw_response_content)
except ValidationError as e:
    raise BackpackResponseHandler._handle_validation_error(
        e, context, raw_response_content, status_code
    ) from e
```

#### **New Error Pattern** (in typed_request)
```python
try:
    validated_model = response_model.model_validate(raw_response)
    return validated_model, status_code, headers, raw_headers
except ValidationError as e:
    raise APIError(
        message=f"Response validation failed for {endpoint_path}: {e}",
        code=APIErrorCode.INVALID_RESPONSE.value,
        http_status=status_code,
        validation_errors=e.errors(),  # Preserve detailed validation errors
    ) from e
```

**Benefits:**
- **Centralized error handling** in one place (HttpClient)
- **Consistent error format** across all endpoints
- **Rich error context** with endpoint and validation details
- **Preserved error chains** for debugging

### Testing Strategy

#### **Unit Tests for Typed HttpClient**
```python
# test_http_client_typed.py

async def test_typed_request_single_model():
    """Test typed_request with single model response."""
    client = HttpClient(...)
    
    # Mock successful response
    mock_response = {"symbol": "BTC_USDC", "lastPrice": "45000.00"}
    
    with patch.object(client, 'request') as mock_request:
        mock_request.return_value = (mock_response, 200, {}, {})
        
        result, status, headers, _ = await client.typed_request(
            method="GET",
            endpoint_path="/api/v1/ticker",
            response_model=BackpackRawTicker,
            params={"symbol": "BTC_USDC"},
        )
        
        assert isinstance(result, BackpackRawTicker)
        assert result.symbol == "BTC_USDC"
        assert result.last_price == "45000.00"

async def test_typed_request_list_response():
    """Test typed_request with list response."""
    client = HttpClient(...)
    
    # Mock list response
    mock_response = [
        {"symbol": "BTC_USDC", "side": "Buy", "quantity": "1.0"},
        {"symbol": "ETH_USDC", "side": "Sell", "quantity": "5.0"},
    ]
    
    with patch.object(client, 'request') as mock_request:
        mock_request.return_value = (mock_response, 200, {}, {})
        
        result, status, headers, _ = await client.typed_request(
            method="GET",
            endpoint_path="/api/v1/orders",
            response_model=list[BackpackRawOrder],
        )
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(order, BackpackRawOrder) for order in result)

async def test_typed_request_validation_error():
    """Test typed_request with invalid response structure."""
    client = HttpClient(...)
    
    # Mock invalid response
    mock_response = {"invalid": "structure"}
    
    with patch.object(client, 'request') as mock_request:
        mock_request.return_value = (mock_response, 200, {}, {})
        
        with pytest.raises(APIError) as exc_info:
            await client.typed_request(
                method="GET",
                endpoint_path="/api/v1/ticker",
                response_model=BackpackRawTicker,
            )
        
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "validation failed" in exc_info.value.message.lower()
```

#### **Integration Tests**
```python
# test_service_typed_integration.py

async def test_service_typed_method_with_live_api():
    """Test service method using typed HttpClient with live API."""
    service = BackpackMarketDataService(...)
    
    # Test actual API call
    ticker = await service.get_ticker_typed("BTC_USDC")
    
    # Verify domain model
    assert isinstance(ticker, Ticker)
    assert ticker.symbol == "BTC_USDC"
    assert ticker.price > Decimal("0")
    assert ticker.volume >= Decimal("0")
```

### Performance Considerations

#### **Current Performance** (4 layers)
```
HttpClient.request() → JSON parsing → ParsedJsonResponse
→ ResponseHandler validation → Raw Model creation
→ Mapper transformation → Domain Model
```

#### **New Performance** (3 layers)
```
HttpClient.typed_request() → JSON parsing + Pydantic validation → Raw Model
→ Mapper transformation → Domain Model
```

**Performance Benefits:**
- **Eliminate duplicate validation**: No separate ResponseHandler validation step
- **Reduce object creation**: Fewer intermediate objects
- **Better memory usage**: Direct model creation without intermediate ParsedJsonResponse

**Performance Risks:**
- **Pydantic validation overhead**: More complex validation in HttpClient
- **Error handling overhead**: Rich error objects with validation details

**Mitigation Strategies:**
- **Benchmark validation performance**: Compare old vs new validation speed
- **Profile memory usage**: Ensure no memory leaks in error paths
- **Optimize error objects**: Create validation errors only when needed

### Boundaries and Dependencies

#### **What Changes**
- **HttpClient**: Add `typed_request` method
- **Service layers**: Add typed methods using `typed_request`
- **Factory classes**: Inject HttpClient references
- **ResponseHandlers**: Eventually deprecated and removed

#### **What Stays the Same**
- **Raw Pydantic models**: No changes needed
- **Mapper classes**: No changes needed  
- **Domain models**: No changes needed
- **Request builders**: No changes needed
- **Authentication**: No changes needed
- **Rate limiting**: No changes needed

#### **Backward Compatibility**
- **Phase 1-3**: Both old and new methods coexist
- **Phase 4**: Deprecation warnings before removal
- **Migration window**: 2-3 months for all callers to migrate

### Potential Problems and Mitigation

#### **Problem 1: Complex Response Preprocessing**
**Issue**: Some ResponseHandlers do complex preprocessing before Pydantic validation

**Example**: Hyperliquid meta responses require field restructuring

**Mitigation**: Move preprocessing logic into Raw model field validators
```python
class HyperliquidRawMetaAndAssetCtxsResponse(BaseModel):
    @field_validator('complex_field', mode='before')
    @classmethod
    def preprocess_complex_field(cls, v: Any) -> Any:
        # Move ResponseHandler preprocessing logic here
        if isinstance(v, list) and len(v) > 0:
            # Restructure as needed
            return restructure_data(v)
        return v
```

#### **Problem 2: Error Message Quality**
**Issue**: Current ResponseHandlers provide rich, context-aware error messages

**Mitigation**: Enhance typed_request error handling with context builders
```python
async def typed_request(
    self,
    method: str,
    endpoint_path: str,
    response_model: Type[T],
    context_builder: Callable[..., str] | None = None,
    **kwargs: Any,
) -> tuple[T, int, ProcessedResponseHeaders, CIMultiDictProxy[str]]:
    # Build rich context for errors
    if context_builder:
        context = context_builder(method, endpoint_path, kwargs)
    else:
        context = f"{method} {endpoint_path}"
    
    try:
        validated_model = response_model.model_validate(raw_response)
        return validated_model, status_code, headers, raw_headers
    except ValidationError as e:
        raise APIError(
            message=f"Response validation failed for {context}: {e}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
            validation_errors=e.errors(),
        ) from e
```

#### **Problem 3: Type Safety with Generic Responses**
**Issue**: Some endpoints return different structures based on parameters

**Example**: Hyperliquid `/info` endpoint returns different models based on `type` parameter

**Mitigation**: Use union types or overloaded methods
```python
# Option 1: Union types for dynamic responses
InfoResponse = HyperliquidRawUserStateResponse | HyperliquidRawMetaResponse | HyperliquidRawL2BookResponse

# Option 2: Overloaded methods for each response type
@overload
async def get_info_user_state(self, user_address: str) -> HyperliquidRawUserStateResponse: ...

@overload  
async def get_info_meta(self) -> HyperliquidRawMetaResponse: ...

# Implementation chooses appropriate response_model based on parameters
```

#### **Problem 4: Testing Complexity**
**Issue**: More complex type testing with generic methods

**Mitigation**: Comprehensive test matrix
```python
# Test matrix for each endpoint:
# - Valid single model response
# - Valid list response  
# - Valid dict response
# - Invalid structure (wrong type)
# - Invalid content (validation errors)
# - Network errors
# - Empty responses
# - Edge cases (empty lists, null values)
```

#### **Problem 5: Migration Coordination**
**Issue**: Large codebase with many service callers

**Mitigation**: Gradual migration strategy
```python
# Phase 1: Add typed methods alongside existing ones
async def get_ticker(self) -> Ticker:  # Legacy
    """Original method - unchanged"""
    pass

async def get_ticker_typed(self) -> Ticker:  # New
    """Typed method using new HttpClient"""
    pass

# Phase 2: Add deprecation warnings to legacy methods
async def get_ticker(self) -> Ticker:
    """Original method - DEPRECATED, use get_ticker_typed()"""
    warnings.warn(
        "get_ticker() is deprecated, use get_ticker_typed()",
        DeprecationWarning,
        stacklevel=2
    )
    # existing implementation

# Phase 3: Update all callers
# Phase 4: Remove legacy methods
```

## Implementation Timeline

### **Week 1: Foundation**
- [ ] Implement `typed_request` method in HttpClient
- [ ] Add comprehensive unit tests for typed client
- [ ] Verify backward compatibility (all existing tests pass)

### **Week 2: Pilot Implementation**  
- [ ] Choose 5 simple endpoints for pilot
- [ ] Implement typed service methods for pilot endpoints
- [ ] Update factory classes to inject HttpClient references
- [ ] Integration testing with live APIs

### **Week 3-4: Full Migration**
- [ ] Migrate all Backpack service methods to typed approach
- [ ] Migrate all Hyperliquid service methods to typed approach  
- [ ] Handle complex response preprocessing cases
- [ ] Comprehensive testing of all migrated methods

### **Week 5: Cleanup and Optimization**
- [ ] Add deprecation warnings to legacy methods
- [ ] Performance benchmarking and optimization
- [ ] Documentation updates
- [ ] Final integration testing

### **Month 2: Deprecation Period**
- [ ] Monitor usage of deprecated methods
- [ ] Update all callers to use typed methods
- [ ] Remove ResponseHandler classes
- [ ] Remove legacy methods and ParsedJsonResponse

## Success Metrics

### **Type Safety Metrics**
- **100% typed responses**: All API responses validated with specific Pydantic models
- **Zero ParsedJsonResponse usage**: Complete elimination of untyped responses
- **Mypy validation**: Zero type errors in static analysis

### **Code Quality Metrics**  
- **60% reduction in service method lines**: Eliminate ResponseHandler boilerplate
- **Eliminated duplicate validation**: Single validation point in HttpClient
- **Improved error messages**: Consistent, rich error context

### **Performance Metrics**
- **Reduced object creation**: Fewer intermediate objects in response pipeline
- **Validation performance**: Equal or better validation speed than current approach
- **Memory usage**: No increase in baseline memory consumption

### **Reliability Metrics**
- **Zero validation bypass**: Impossible to skip Pydantic validation
- **100% test coverage**: All typed methods have comprehensive tests
- **Backward compatibility**: Existing functionality unchanged during migration

## Conclusion

The ParsedJsonResponse to typed Raw models refactoring provides significant benefits:

1. **Complete Type Safety**: Direct Raw model responses eliminate the untyped bottleneck
2. **Simplified Architecture**: Remove unnecessary ResponseHandler layer
3. **Better Developer Experience**: IDE autocomplete and compile-time error checking
4. **Improved Performance**: Eliminate duplicate validation steps
5. **Enhanced Maintainability**: Centralized validation logic in HttpClient

The 4-phase migration approach ensures:
- **Zero breaking changes** during implementation
- **Thorough testing** at each phase
- **Gradual adoption** with deprecation period
- **Complete backward compatibility** until final cleanup

This refactoring establishes the foundation for future decorator-based enhancements while providing immediate benefits in type safety and code clarity.

The comprehensive Raw model coverage in both Backpack and Hyperliquid means implementation can proceed immediately without additional model creation. The existing architecture already supports this pattern - we're simply removing the ParsedJsonResponse bottleneck that prevents full type safety.

**Result**: A clean, typed API layer where `HttpClient → Raw Model → Domain Model` provides end-to-end type safety from HTTP boundary to business logic.
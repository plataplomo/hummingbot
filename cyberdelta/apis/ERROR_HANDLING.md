# CyberDeltaEngine API Error Handling Architecture

This document outlines the error handling patterns and principles used across all exchange API implementations in CyberDeltaEngine. These patterns ensure consistent, predictable error behavior and maintainable code.

## Table of Contents
1. [Overview](#overview)
2. [Error Types](#error-types)
3. [Layer Responsibilities](#layer-responsibilities)
4. [Error Flow](#error-flow)
5. [Implementation Patterns](#implementation-patterns)
6. [Best Practices](#best-practices)

## Overview

The error handling architecture follows a layered approach where each layer has specific responsibilities. The key principle is that **all errors eventually become `APIError`** instances with proper context, ensuring consistent error handling across the application.

### Core Design Principles

1. **Service Layer is the Guardian**: Catches ALL exceptions and ensures only `APIError` propagates up
2. **Fail Fast on Validation**: Invalid data structures should be caught immediately
3. **Preserve Context**: Always include original exception, HTTP status, and exchange messages
4. **Graceful Degradation**: When possible, return partial data rather than failing completely
5. **Clear Error Boundaries**: Each layer has specific error types it can raise

## Error Types

### 1. `APIError` (Primary Error Type)
- **Location**: `cyberdelta.apis.models.api_error`
- **Purpose**: Standardized error representation for all API operations
- **Fields**:
  - `code`: Standardized error code (from `APIErrorCode` enum)
  - `message`: Human-readable error description
  - `http_status`: HTTP status code (if applicable)
  - `exchange_code`: Exchange-specific error code
  - `exchange_message`: Raw error message from exchange
  - `metadata`: Additional context information
  - `original_exception`: The underlying exception that caused this error
  - `retry_after`: Seconds to wait before retry (for rate limits)

### 2. `TransformationError`
- **Location**: `cyberdelta.apis.models.api_error`
- **Purpose**: Indicates data transformation/mapping failures
- **Used By**: Mapper classes
- **Caught By**: Service layer (wrapped as `APIError`)

### 3. `ValidationError` (Pydantic)
- **Location**: `pydantic`
- **Purpose**: Model validation failures
- **Used By**: Pydantic models, Response Handlers
- **Caught By**: Service layer (wrapped as `APIError`)

### 4. Standard Python Exceptions
- **Types**: `ValueError`, `TypeError`, `KeyError`, etc.
- **Purpose**: Input validation, type errors, missing data
- **Handling**: Context-dependent (see patterns below)

## Layer Responsibilities

### 1. Service Layer (`*_service.py`)

**Primary Responsibility**: Error orchestration and standardization

```python
async def service_method(self, args: ServiceArgs) -> DomainModel:
    # 1. Input Validation (raises ValueError)
    if not args.symbol:
        raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")
    
    try:
        # 2. Core Logic
        raw_data = await self._make_request(args)
        validated_data = self._response_handler.handle_response(raw_data)
        return self._mapper.transform_to_internal(validated_data)
        
    except APIError:
        # 3. Re-raise API errors as-is
        raise
        
    except TransformationError as e:
        # 4. Wrap transformation errors
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=e,
            http_status=status_code,
            exchange_message=raw_response_content,
        ) from e
        
    except ValidationError as e:
        # 5. Wrap validation errors
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=e,
        ) from e
        
    except (ValueError, TypeError) as e:
        # 6. Distinguish input validation from internal errors
        error_msg = str(e)
        if current_method in error_msg and "symbol" in error_msg:
            raise  # Re-raise input validation errors
        else:
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e,
            ) from e
            
    except Exception as e:
        # 7. Catch-all for unexpected errors
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e,
        ) from e
```

### 2. Response Handler (`*_response_handler.py`)

**Primary Responsibility**: Response structure validation

```python
@staticmethod
def handle_get_ticker_response(
    raw_response_content: RawJsonResponse,
    symbol: str,
    status_code: int,
    headers: Mapping[str, str],
) -> BackpackRawTicker:
    context = f"ticker ({symbol}) - Status: {status_code}"
    
    # 1. Validate response structure
    if not isinstance(raw_response_content, dict):
        raise APIError(
            message=f"Unexpected {context} response format: expected dict, "
                   f"got {type(raw_response_content).__name__}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )
    
    # 2. Validate with Pydantic model
    try:
        return BackpackRawTicker.model_validate(raw_response_content)
    except ValidationError as e:
        raise BackpackResponseHandler._handle_validation_error(
            e, context, raw_response_content
        ) from e
```

### 3. Error Mapper (`*_error_mapper.py`)

**Primary Responsibility**: Error interpretation (never raises errors)

```python
def map_exchange_error(
    self,
    status_code: int,
    error_body: str | None,
    error_data: dict[str, Any] | None = None,
    request_path: str | None = None,
    original_exception: Exception | None = None,
) -> APIError:
    # 1. Parse structured error data
    if error_data:
        try:
            raw_error = ExchangeRawApiError.model_validate(error_data)
            code = self._map_error_code(raw_error.code)
        except ValidationError:
            code = APIErrorCode.EXCHANGE_SPECIFIC
    
    # 2. Fallback to string matching
    if code == APIErrorCode.EXCHANGE_SPECIFIC and error_body:
        code = self._map_string_error(error_body)
    
    # 3. Final fallback to HTTP status
    if code == APIErrorCode.EXCHANGE_SPECIFIC:
        code = self._map_http_status(status_code)
    
    # 4. Return standardized error (never raise)
    return APIError(
        message=effective_message,
        code=code.value,
        http_status=status_code,
        exchange_code=exchange_code,
        exchange_message=error_body,
        metadata=metadata,
        original_exception=original_exception,
        retry_after=retry_after,
    )
```

### 4. Request Builder (`*_request_builder.py`)

**Primary Responsibility**: Request formatting (no error handling)

```python
@staticmethod
def build_place_order_payload(
    symbol: str,
    side: OrderSide,
    order_type: OrderType,
    quantity: Decimal,
    # ... other params
) -> ExchangeRawOrderRequest:
    # No validation - assumes pre-validated inputs
    # Direct transformation only
    request_data = {
        "symbol": format_symbol(symbol),
        "side": side.value,
        "quantity": str(quantity),
        # ...
    }
    return ExchangeRawOrderRequest(**request_data)
```

### 5. Mappers (`mappers/*_mapper.py`)

**Primary Responsibility**: Data transformation with domain validation

```python
@staticmethod
def transform_raw_balance_to_internal(
    asset_symbol: str,
    raw: ExchangeRawBalance,
) -> SpotBalance:
    try:
        # 1. Parse and validate data
        available = parse_decimal_value(raw.available, allow_none=False)
        if available is None:
            raise TransformationError(
                f"Available quantity missing for {asset_symbol}"
            )
        
        # 2. Create domain model
        return SpotBalance(
            asset=asset_symbol,
            available_quantity=available,
            # ...
        )
        
    except Exception as e:
        # 3. Always wrap as TransformationError
        raise TransformationError(
            f"Failed to transform balance: {e}"
        ) from e
```

## Error Flow

### Successful Request Flow
```
Service → RequestBuilder → HTTP Client → Response
   ↓                                         ↓
   ←  ←  ←  ←  ←  Mapper  ← ResponseHandler ←
```

### Error Response Flow
```
Service → RequestBuilder → HTTP Client → Error Response
   ↓                                         ↓
   ←  ←  ←  ←  ←  ←  ←  ←  ErrorMapper  ←  ←
```

### Error Propagation
```
ResponseHandler: ValidationError/APIError
       ↓
Service: Catches, wraps as APIError
       ↓
Mapper: TransformationError
       ↓
Service: Catches, wraps as APIError
       ↓
Caller: Only sees APIError
```

## Implementation Patterns

### 1. Input Validation Pattern
```python
# At the start of service methods
if not symbol:
    raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")

if limit is not None and limit <= 0:
    raise ValueError(f"[{current_method}] 'limit' must be positive when provided.")
```

### 2. Special Case Handling Pattern
```python
# Handle 404 as None instead of error
except APIError as e:
    if e.http_status == 404:
        logger.info(f"Resource not found, returning None")
        return None
    raise
```

### 3. Graceful Degradation Pattern
```python
# Continue with partial data on non-critical failures
try:
    enhanced_data = await self._fetch_enhanced_data()
except APIError as e:
    logger.warning(f"Enhanced data unavailable: {e}. Using basic data.")
    enhanced_data = None
```

### 4. Context Preservation Pattern
```python
# Always include context in errors
raise APIError(
    code=APIErrorCode.INVALID_RESPONSE.value,
    message="Failed to process order data",
    http_status=status_code,
    exchange_message=raw_response_content,
    original_exception=e,
    metadata={
        "symbol": symbol,
        "order_id": order_id,
        "request_path": endpoint_path,
    }
)
```

### 5. Error Code Mapping Pattern
```python
# In error mapper
code_map = {
    "INSUFFICIENT_BALANCE": APIErrorCode.INSUFFICIENT_FUNDS,
    "RATE_LIMIT": APIErrorCode.RATE_LIMITED,
    "INVALID_API_KEY": APIErrorCode.AUTHENTICATION_FAILED,
    # ... comprehensive mappings
}
mapped_code = code_map.get(exchange_code, APIErrorCode.EXCHANGE_SPECIFIC)
```

## Best Practices

### 1. Always Preserve Original Context
- Include original exception
- Preserve HTTP status codes
- Keep exchange-specific error messages
- Add relevant metadata (symbol, order_id, etc.)

### 2. Use Appropriate Error Codes
- Map to specific `APIErrorCode` values when possible
- Use `EXCHANGE_SPECIFIC` only as last resort
- Document any new error patterns discovered

### 3. Log at Appropriate Levels
- `ERROR`: Unexpected failures, data corruption
- `WARNING`: Recoverable issues, fallback scenarios
- `INFO`: Expected failures (404, order not found)
- `DEBUG`: Detailed context for troubleshooting

### 4. Handle Rate Limits Specially
- Extract `retry_after` information when available
- Use regex patterns to parse from error messages
- Provide reasonable defaults when not specified

### 5. Validate Early, Transform Late
- Validate structure in Response Handler
- Validate business logic in Service
- Transform data in Mappers
- Keep Request Builder validation-free

### 6. Document Exchange-Specific Behaviors
- Note any unique error patterns
- Document special status codes
- Explain any workarounds needed

### 7. Test Error Paths
- Unit test each error scenario
- Test with malformed responses
- Verify error context preservation
- Ensure proper error type propagation

## Example: Complete Error Handling Flow

```python
# 1. Service Method
async def get_order(self, order_id: str, symbol: str) -> Order:
    # Input validation
    if not order_id:
        raise ValueError("[get_order] 'order_id' is required")
    
    try:
        # Make request
        response = await self._http_client.get(f"/orders/{order_id}")
        
        # Validate response
        raw_order = self._response_handler.handle_get_order_response(
            response.data, order_id, response.status_code
        )
        
        # Transform to internal model
        return self._mapper.transform_raw_order_to_internal(raw_order)
        
    except APIError as e:
        # Special handling for 404
        if e.http_status == 404:
            logger.info(f"Order {order_id} not found")
            return None
        raise
        
    except TransformationError as e:
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message=f"Failed to process order {order_id}",
            original_exception=e,
            metadata={"order_id": order_id, "symbol": symbol}
        ) from e
```

This architecture ensures that:
- Errors are handled consistently across all exchanges
- Context is never lost during error propagation
- Each layer has clear responsibilities
- The system degrades gracefully when possible
- Debugging is straightforward with proper error context
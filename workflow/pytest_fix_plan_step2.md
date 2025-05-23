# Step 2: Ensure Consistent Exception Wrapping in Services/API Clients

## Goal
All exceptions from dependencies (builders, mappers, HTTP client, lower-level services) must be consistently wrapped into `APIError` by calling service or API client methods.

## Problem Analysis
Tests expect `APIError` but receive primitive exceptions (`ValueError`, `RuntimeError`, `TypeError`) when dependencies fail. This indicates inconsistent exception handling boundaries.

## Implementation Strategy

### 2.1 Service Method Exception Wrapping
**Target**: Service methods calling request builders, mappers, or HTTP clients.

**Pattern**:
```python
async def service_method(self, param: str) -> ResponseType:
    """Service method with comprehensive exception wrapping."""
    try:
        # Call to dependency (builder, mapper, HTTP client)
        payload = self._request_builder.build_request(param)
        response = await self._http_client.post(url, data=payload)
        return self._response_handler.handle_response(response)
    except (ValueError, TypeError) as e:
        # Input validation or type errors
        raise APIError(
            message=f"Invalid input for {service_method.__name__}: {e}",
            code=APIErrorCode.INVALID_REQUEST.value,
            original_exception=e
        ) from e
    except (ConnectionError, TimeoutError) as e:
        # Network-related errors
        raise APIError(
            message=f"Network error in {service_method.__name__}: {e}",
            code=APIErrorCode.NETWORK_ERROR.value,
            original_exception=e
        ) from e
    except Exception as e:
        # Catch-all for unexpected errors
        raise APIError(
            message=f"Unexpected error in {service_method.__name__}: {e}",
            code=APIErrorCode.UNKNOWN.value,
            original_exception=e
        ) from e
```

### 2.2 API Client Method Exception Wrapping
**Target**: API client methods calling service methods.

**Pattern**:
```python
async def api_method(self, param: str) -> ProcessedType:
    """API client method with service call exception wrapping."""
    # Input validation (can raise ValueError/TypeError directly)
    if not param or not isinstance(param, str):
        raise ValueError(f"Invalid param: {param}")
    
    try:
        # Call to service (may raise APIError or other exceptions)
        raw_result = await self.service.method(param)
        return self._mapper.to_internal(raw_result)
    except APIError:
        # Re-raise APIError from service unchanged
        raise
    except (ValueError, TypeError) as e:
        # Mapper or processing errors
        raise APIError(
            message=f"Processing error in {api_method.__name__}: {e}",
            code=APIErrorCode.PROCESSING_ERROR.value,
            original_exception=e
        ) from e
    except Exception as e:
        # Unexpected errors from service or mapper
        raise APIError(
            message=f"Unexpected error in {api_method.__name__}: {e}",
            code=APIErrorCode.UNKNOWN.value,
            original_exception=e
        ) from e
```

### 2.3 Files Requiring Updates

**Service Files**:
- `cyberdelta/apis/backpack/services/bp_market_data_service.py`
- `cyberdelta/apis/backpack/services/bp_trading_service.py`
- `cyberdelta/apis/backpack/services/bp_account_service.py`
- `cyberdelta/apis/hyperliquid/services/hl_market_data_service.py`
- `cyberdelta/apis/hyperliquid/services/hl_trading_service.py`
- `cyberdelta/apis/hyperliquid/services/hl_account_service.py`

**API Client Files**:
- `cyberdelta/apis/backpack/bp_api.py`
- `cyberdelta/apis/hyperliquid/hl_api.py`

### 2.4 Exception Mapping Strategy

| Original Exception | APIErrorCode | Use Case |
|-------------------|--------------|----------|
| `ValueError`, `TypeError` | `INVALID_REQUEST` | Input validation failures |
| `ConnectionError`, `TimeoutError` | `NETWORK_ERROR` | HTTP/WebSocket issues |
| `ValidationError` (Pydantic) | `INVALID_RESPONSE` | Response parsing failures |
| `KeyError`, `AttributeError` | `PROCESSING_ERROR` | Data processing issues |
| Other `Exception` | `UNKNOWN` | Catch-all |

### 2.5 Implementation Phases

1. **Phase 1**: Update service methods (high impact)
2. **Phase 2**: Update API client methods (medium impact)  
3. **Phase 3**: Verify exception boundary consistency (validation)

## Validation Criteria
- All service method calls wrapped in try-catch
- All dependency calls protected with appropriate exception mapping
- Tests expecting `APIError` now pass
- Original exception preserved in `APIError.original_exception` 
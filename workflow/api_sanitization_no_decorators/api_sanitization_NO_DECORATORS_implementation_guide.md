# NO DECORATORS Implementation Guide

## Quick Start: What to Do Right Now

### Step 1: Understand the Core Principle

**ParsedJsonResponse is CORRECT** - Don't try to eliminate it. Instead:
- Fix validation timing (validate immediately after HTTP response)
- Fix validation bypass (use `model_validate` not direct instantiation)
- Fix error consistency (use centralized utilities)

### Step 2: Use Existing Tools

We have one key utility that exists, and one that needs to be created:

1. **`/cyberdelta/utils/secure_transformation.py`** - ✅ Already exists!
   - `secure_transform()` - Use for ALL model creation in mappers
   - `secure_transform_with_audit()` - Use for financial operations

2. **`/cyberdelta/apis/utils/response_validation.py`** - ❌ Needs to be created!
   - `ensure_dict_response()` - Validates dict responses
   - `ensure_list_response()` - Validates list responses
   - `validate_required_fields()` - Checks required fields

### Step 2.5: Create Response Validation Utilities

First, create the response validation file:

```python
# /cyberdelta/apis/utils/response_validation.py
"""Centralized response validation utilities for type-safe API handling."""

from typing import Any, TypeVar
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger

logger = get_logger(__name__)
T = TypeVar('T')


def ensure_dict_response(
    response: ParsedJsonResponse | None,
    context: str,
    status_code: int,
) -> dict[str, Any]:
    """Validate response is a dict with consistent error handling."""
    if response is None:
        logger.error(f"SECURITY: Null response for {context}")
        raise APIError(
            message=f"No data received for {context}, status: {status_code}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    if not isinstance(response, dict):
        logger.error(f"SECURITY: Type mismatch for {context}")
        raise APIError(
            message=f"Expected dict for {context}, got {type(response).__name__}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    return response


def ensure_list_response(
    response: ParsedJsonResponse | None,
    context: str,
    status_code: int,
) -> list[Any]:
    """Validate response is a list with consistent error handling."""
    if response is None:
        logger.error(f"SECURITY: Null response for {context}")
        raise APIError(
            message=f"No data received for {context}, status: {status_code}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    if not isinstance(response, list):
        logger.error(f"SECURITY: Type mismatch for {context}")
        raise APIError(
            message=f"Expected list for {context}, got {type(response).__name__}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    return response
```

### Step 3: Fix Mappers (CRITICAL SECURITY FIX)

#### Pattern for ALL mapper methods:

```python
# ❌ WRONG - Direct instantiation (bypasses validation)
return SpotBalance(
    asset=asset,
    exchange=ExchangeName.BACKPACK.value,
    total_quantity=total,
    available_quantity=available,
)

# ✅ CORRECT - Using secure_transform
from cyberdelta.utils.secure_transformation import secure_transform

balance_data = {
    "asset": asset,
    "exchange": ExchangeName.BACKPACK.value,
    "total_quantity": str(total),  # Convert Decimal to string
    "available_quantity": str(available),
    "timestamp": datetime.now(UTC).isoformat(),
}

return secure_transform(
    data=balance_data,
    model_class=SpotBalance,
    context="balance_transformation",
    source_exchange="backpack"
)
```

### Step 4: Enhance Services (Reduce Boilerplate)

#### Pattern for service methods:

```python
# ❌ OLD WAY - Manual validation
async def get_ticker(self, symbol: str) -> Ticker:
    raw_data, status_code, _ = await self._http_client_requester(...)

    if raw_data is None:
        raise APIError(...)  # Manual error

    if not isinstance(raw_data, dict):
        raise APIError(...)  # More manual validation

    raw_ticker = self._response_handler.handle_get_ticker_response(...)
    return self._mapper.transform_raw_ticker_to_internal(raw_ticker)

# ✅ NEW WAY - Centralized validation
from cyberdelta.apis.utils.response_validation import ensure_dict_response

async def get_ticker(self, symbol: str) -> Ticker:
    raw_data, status_code, _ = await self._http_client_requester(...)

    # One line replaces all manual validation
    validated_data = ensure_dict_response(
        raw_data,
        f"ticker ({symbol})",
        status_code
    )

    raw_ticker = self._response_handler.handle_get_ticker_response(...)
    return self._mapper.transform_raw_ticker_to_internal(raw_ticker)
```

### Step 5: Update Response Handlers

Since services now validate, response handlers can be simpler:

```python
# ❌ OLD - Duplicate validation
def handle_get_ticker_response(self, raw_response_content: RawJsonResponse, ...):
    if not isinstance(raw_response_content, dict):
        raise APIError(...)  # This is now redundant

    return BackpackRawTicker.model_validate(raw_response_content)

# ✅ NEW - Trust validated input
def handle_get_ticker_response(self, raw_response_content: dict[str, Any], ...):
    # Input already validated by service layer
    try:
        return BackpackRawTicker.model_validate(raw_response_content)
    except ValidationError as e:
        # Just handle Pydantic validation errors
        logger.error(f"SECURITY: Model validation failed: {e}")
        raise APIError(...)
```

## Common Patterns

### Pattern 1: Single Object Response
```python
# In service
validated_data = ensure_dict_response(raw_data, "balance", status_code)

# In mapper
return secure_transform(
    data={"asset": asset, "quantity": str(quantity)},
    model_class=Balance,
    context="balance_update",
    source_exchange="backpack"
)
```

### Pattern 2: List Response
```python
# In service
validated_list = ensure_list_response(raw_data, "positions", status_code)

# In mapper
return [
    secure_transform(
        data=position_dict,
        model_class=Position,
        context=f"position_{idx}",
        source_exchange="hyperliquid"
    )
    for idx, position_dict in enumerate(validated_list)
]
```

### Pattern 3: Optional Response
```python
# In service
if raw_data is None:
    return None  # Valid case

validated_data = ensure_dict_response(raw_data, "order", status_code)
```

## Testing

### Test Validation Security
```python
def test_mapper_prevents_validation_bypass():
    """Ensure mappers use secure_transform."""
    malicious_data = {
        "asset": "BTC",
        "total_quantity": "-100",  # Negative attack
    }

    with pytest.raises(TransformationError):
        mapper.transform_balance(malicious_data)
```

### Test Service Validation
```python
def test_service_validates_response_type():
    """Ensure services validate response types."""
    # Mock returns wrong type
    mock_http_client.return_value = ("not_a_dict", 200, {})

    with pytest.raises(APIError) as exc:
        await service.get_ticker("BTC")

    assert "expected dict" in str(exc.value)
```

## Checklist

### Week 1 (Critical Security)
- [ ] Read existing utility: `secure_transformation.py`
- [ ] CREATE the response validation utility: `/cyberdelta/apis/utils/response_validation.py`
- [ ] Fix 5 mapper methods to use `secure_transform`
- [ ] Run tests to ensure no breakage
- [ ] Continue fixing remaining mappers

### Week 2 (Service Enhancement)
- [ ] Update 5 service methods to use `ensure_dict_response`
- [ ] Update corresponding response handlers
- [ ] Measure code reduction (target: 40%)

### Week 3 (Type Safety)
- [ ] Add TypeGuards to `typing.py`
- [ ] Update services to use TypeGuards
- [ ] Document patterns for team

## Key Takeaways

1. **Don't fight ParsedJsonResponse** - It's architecturally correct
2. **Use existing utilities** - `secure_transform` and validation helpers
3. **Fix the real problems** - Validation bypass and inconsistent errors
4. **Keep it simple** - No decorators, just functions
5. **Incremental adoption** - Fix critical security first, enhance later

## Need Help?

- **Security issues**: Fix mappers with `secure_transform` IMMEDIATELY
- **Boilerplate reduction**: Use `ensure_dict_response` in services
- **Type safety**: Add TypeGuards for better IDE support
- **Questions**: The solution is simpler than decorators - just use the utilities!

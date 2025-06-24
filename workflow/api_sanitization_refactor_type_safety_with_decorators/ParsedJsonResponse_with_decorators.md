# Best-of-the-Best: Advanced Decorator-Based Type Safety

Here's the ultimate decorator-based solution that combines maximum type safety, minimal implementation complexity, and exceptional developer experience:

## **Core Decorator Framework**

```python
# cyberdelta/apis/decorators/typed_responses.py
from functools import wraps
from typing import TypeVar, Type, get_type_hints, overload, cast, Any
from collections.abc import Callable, Awaitable
import inspect

T = TypeVar('T', bound=BaseModel)
R = TypeVar('R')

class TypedResponseError(Exception):
    """Typed response validation error."""
    pass

def typed_api_method(
    *,
    response_model: Type[T] | None = None,
    list_of: Type[T] | None = None,
    allow_none: bool = False,
    context_builder: Callable[..., str] | None = None
):
    """
    Ultimate decorator for type-safe API methods.

    Args:
        response_model: Expected single object response type
        list_of: Expected list item type (for array responses)
        allow_none: Whether None responses are valid
        context_builder: Custom function to build error context
    """
    def decorator(func: Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, Any]]]):
        # Extract parameter names for smart context building
        sig = inspect.signature(func)
        param_names = list(sig.parameters.keys())[1:]  # Skip 'self'

        @wraps(func)
        async def wrapper(self, *args, **kwargs) -> T | list[T] | None:
            # Call original method
            raw_data, status_code, headers = await func(self, *args, **kwargs)

            # Build context for errors
            if context_builder:
                context = context_builder(self, *args, **kwargs)
            else:
                # Smart context from method name and parameters
                method_name = func.__name__.replace('_raw', '').replace('get_', '')
                params = []
                for i, param_name in enumerate(param_names[:len(args)]):
                    if i < len(args) and args[i] is not None:
                        params.append(f"{param_name}={args[i]}")
                param_str = f"({', '.join(params)})" if params else ""
                context = f"{method_name}{param_str}"

            # Handle None responses
            if raw_data is None:
                if allow_none:
                    return None
                raise APIError(
                    message=f"No data received for {context}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code
                )

            # Validate and transform based on expected type
            if list_of is not None:
                # Array response validation
                if not isinstance(raw_data, list):
                    raise APIError(
                        message=f"Expected list for {context}, got {type(raw_data).__name__}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                        http_status=status_code
                    )

                try:
                    return [list_of.model_validate(item) for item in raw_data]
                except ValidationError as e:
                    raise APIError(
                        message=f"List validation failed for {context}: {e}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                        http_status=status_code,
                        validation_errors=e.errors()
                    ) from e

            elif response_model is not None:
                # Object response validation
                if not isinstance(raw_data, dict):
                    raise APIError(
                        message=f"Expected object for {context}, got {type(raw_data).__name__}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                        http_status=status_code
                    )

                try:
                    return response_model.model_validate(raw_data)
                except ValidationError as e:
                    raise APIError(
                        message=f"Object validation failed for {context}: {e}",
                        code=APIErrorCode.INVALID_RESPONSE.value,
                        http_status=status_code,
                        validation_errors=e.errors()
                    ) from e

            else:
                # No type specified - return raw data (for backwards compatibility)
                return raw_data

        return wrapper
    return decorator
```

## **Advanced Decorator Variants**

```python
# Convenience decorators for common patterns
def dict_response(model: Type[T], allow_none: bool = False):
    """Decorator for single object responses."""
    return typed_api_method(response_model=model, allow_none=allow_none)

def list_response(item_model: Type[T], allow_none: bool = False):
    """Decorator for array responses."""
    return typed_api_method(list_of=item_model, allow_none=allow_none)

def optional_response(model: Type[T]):
    """Decorator for responses that may be None."""
    return typed_api_method(response_model=model, allow_none=True)

# Advanced decorator with custom validation
def validated_response(
    validator: Callable[[ParsedJsonResponse, str], T],
    context_builder: Callable[..., str] | None = None
):
    """Decorator with custom validation logic."""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            raw_data, status_code, headers = await func(self, *args, **kwargs)

            if context_builder:
                context = context_builder(self, *args, **kwargs)
            else:
                context = f"{func.__name__}({', '.join(map(str, args))})"

            try:
                return validator(raw_data, context)
            except Exception as e:
                raise APIError(
                    message=f"Custom validation failed for {context}: {e}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code
                ) from e

        return wrapper
    return decorator
```

## **Smart Auto-Detection Decorator**

```python
def auto_typed(func: Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, Any]]]):
    """
    Ultra-smart decorator that auto-detects expected return type from type hints.
    """
    # Get return type hint from function
    type_hints = get_type_hints(func)
    return_hint = type_hints.get('return')

    if return_hint is None:
        # No type hint - return raw data
        return func

    # Parse the return type hint
    if hasattr(return_hint, '__origin__'):
        if return_hint.__origin__ is Union:
            # Handle Optional[T] -> Union[T, None]
            args = return_hint.__args__
            if len(args) == 2 and type(None) in args:
                # Optional type
                model_type = next(arg for arg in args if arg is not type(None))
                if hasattr(model_type, '__origin__') and model_type.__origin__ is list:
                    # Optional[list[T]]
                    item_type = model_type.__args__[0]
                    return typed_api_method(list_of=item_type, allow_none=True)(func)
                else:
                    # Optional[T]
                    return typed_api_method(response_model=model_type, allow_none=True)(func)
        elif return_hint.__origin__ is list:
            # list[T]
            item_type = return_hint.__args__[0]
            return typed_api_method(list_of=item_type)(func)

    # Single model type
    if isinstance(return_hint, type) and issubclass(return_hint, BaseModel):
        return typed_api_method(response_model=return_hint)(func)

    # Fallback - no decoration
    return func
```

## **Service Layer Usage - Multiple Styles**

```python
class BackpackMarketDataService:
    """Showcase of different decorator approaches."""

    # Style 1: Explicit decorator with specific model
    @dict_response(BackpackRawTicker, allow_none=True)
    async def get_ticker_raw(self, symbol: str) -> BackpackRawTicker | None:
        """Get raw ticker data with automatic validation."""
        return await self._http_client_requester(
            method="GET",
            endpoint="/api/v1/ticker",
            params={"symbol": symbol},
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )

    # Style 2: Auto-detection from type hints
    @auto_typed
    async def get_order_book_raw(self, symbol: str, depth: int = 20) -> BackpackRawOrderBook | None:
        """Auto-detects BackpackRawOrderBook from return type hint."""
        return await self._http_client_requester(
            method="GET",
            endpoint="/api/v1/depth",
            params={"symbol": symbol, "limit": depth},
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )

    # Style 3: List responses
    @list_response(BackpackRawTrade)
    async def get_recent_trades_raw(self, symbol: str, limit: int = 100) -> list[BackpackRawTrade]:
        """Get recent trades as validated list."""
        return await self._http_client_requester(
            method="GET",
            endpoint="/api/v1/trades",
            params={"symbol": symbol, "limit": limit},
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )

    # Style 4: Custom context and validation
    @typed_api_method(
        response_model=BackpackRawBalance,
        context_builder=lambda self, symbol: f"balance_for_{symbol}"
    )
    async def get_balance_raw(self, symbol: str) -> BackpackRawBalance:
        """Custom context for better error messages."""
        return await self._http_client_requester(
            method="GET",
            endpoint="/api/v1/account",
            params={"asset": symbol},
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )

    # Style 5: Complex custom validation
    @validated_response(
        validator=lambda data, ctx: {
            asset: BackpackRawBalance.model_validate(balance)
            for asset, balance in (data or {}).items()
        },
        context_builder=lambda self: "all_balances"
    )
    async def get_all_balances_raw(self) -> dict[str, BackpackRawBalance]:
        """Custom validation for dict of balances."""
        return await self._http_client_requester(
            method="GET",
            endpoint="/api/v1/capital",
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )

    # Public methods that use the decorated raw methods
    async def get_ticker(self, symbol: str) -> Ticker | None:
        """Clean public API using decorated raw method."""
        raw_ticker = await self.get_ticker_raw(symbol)
        return self._market_data_mapper.transform_raw_ticker_to_internal(raw_ticker) if raw_ticker else None

    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBook | None:
        """Clean public API - 2 lines total!"""
        raw_book = await self.get_order_book_raw(symbol, depth)
        return self._market_data_mapper.transform_raw_order_book_to_internal(raw_book) if raw_book else None
```

## **Advanced Features**

### **1. Decorator Composition**

```python
# Combine multiple decorators for advanced functionality
@retry_on_failure(max_attempts=3)
@rate_limited(calls_per_minute=60)
@dict_response(BackpackRawTicker, allow_none=True)
@cache_result(ttl_seconds=30)
async def get_ticker_raw(self, symbol: str) -> BackpackRawTicker | None:
    """Fully decorated method with retry, rate limiting, validation, and caching."""
    return await self._http_client_requester(...)
```

### **2. Validation Pipeline Decorator**

```python
def validation_pipeline(*validators: Callable[[Any], Any]):
    """Chain multiple validation steps."""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            raw_data, status_code, headers = await func(self, *args, **kwargs)

            result = raw_data
            for validator in validators:
                result = validator(result)
            return result

        return wrapper
    return decorator

# Usage
@validation_pipeline(
    lambda data: ensure_dict_response(data, "ticker", 200),
    lambda data: BackpackRawTicker.model_validate(data),
    lambda ticker: ticker if ticker.last_price != "0" else None
)
async def get_ticker_raw(self, symbol: str) -> BackpackRawTicker | None:
    return await self._http_client_requester(...)
```

### **3. Type-Safe Response Mapping**

```python
def mapped_response(raw_model: Type[T], mapper_method: str):
    """Decorator that automatically maps raw response to domain model."""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # Use dict_response decorator internally
            decorated_func = dict_response(raw_model)(func)
            raw_result = await decorated_func(self, *args, **kwargs)

            if raw_result is None:
                return None

            # Get mapper and call the specified method
            mapper = getattr(self, '_market_data_mapper', None)
            if mapper and hasattr(mapper, mapper_method):
                return getattr(mapper, mapper_method)(raw_result)

            return raw_result

        return wrapper
    return decorator

# Usage - single decorator does everything!
@mapped_response(BackpackRawTicker, 'transform_raw_ticker_to_internal')
async def get_ticker(self, symbol: str) -> Ticker | None:
    """One decorator handles validation AND mapping!"""
    return await self._http_client_requester(
        method="GET", endpoint="/api/v1/ticker", params={"symbol": symbol}
    )
```

## **Why This Is The Ultimate Solution**

### **Benefits:**
1. **Zero Boilerplate**: Service methods become 1-3 lines
2. **Maximum Type Safety**: Full compile-time guarantees + runtime validation
3. **Multiple Styles**: Choose the approach that fits each use case
4. **Auto-Detection**: Smart decorators read type hints automatically
5. **Composable**: Decorators can be combined and layered
6. **Backwards Compatible**: Non-decorated methods continue working
7. **Rich Error Context**: Automatic, smart error message generation
8. **IDE Support**: Full autocomplete and type checking

### **Implementation Time:**
- **Core Framework**: 2-3 days
- **All Advanced Features**: 1 week total
- **Service Migration**: As-needed, method by method

### **Code Reduction:**
- **Before**: 8-12 lines per service method
- **After**: 1-3 lines per service method
- **Reduction**: 70-80% less code

### **Developer Experience:**
```python
# Before (current approach)
async def get_ticker(self, symbol: str) -> Ticker | None:
    raw_response_content, status_code, headers = await self._http_client_requester(
        method="GET", endpoint="/api/v1/ticker", params={"symbol": symbol},
        is_signed=False, endpoint_group="public", request_weight=1,
    )
    if raw_response_content is None:
        return None
    raw_ticker = self._response_handler.handle_get_ticker_response(
        raw_response_content, symbol, status_code, headers
    )
    return self._market_data_mapper.transform_raw_ticker_to_internal(raw_ticker)

# After (decorator approach)
@mapped_response(BackpackRawTicker, 'transform_raw_ticker_to_internal')
async def get_ticker(self, symbol: str) -> Ticker | None:
    return await self._http_client_requester(
        method="GET", endpoint="/api/v1/ticker", params={"symbol": symbol}
    )
```

This decorator-based approach provides the **maximum benefit with minimum complexity** - it's truly the best-of-the-best solution!

## **Implementation Roadmap**

### **Phase 1: Core Framework (3 days)**
1. Implement `typed_api_method` decorator
2. Add convenience decorators (`dict_response`, `list_response`, etc.)
3. Create comprehensive unit tests

### **Phase 2: Auto-Detection (2 days)**
1. Implement `auto_typed` decorator with type hint parsing
2. Add support for complex type hints (Optional, Union, etc.)
3. Test with existing Raw models

### **Phase 3: Advanced Features (2-3 days)**
1. Implement decorator composition support
2. Add validation pipeline decorator
3. Create `mapped_response` decorator

### **Phase 4: Service Migration (Ongoing)**
1. Start with one service class as pilot
2. Migrate methods one by one as needed
3. Maintain full backwards compatibility

### **Migration Strategy**

1. **Coexistence**: New decorated methods alongside existing ones
2. **Gradual Adoption**: Teams choose when to migrate each method
3. **Zero Breaking Changes**: All existing code continues working
4. **Performance**: Decorators add minimal overhead
5. **Testing**: Each decorated method can be tested independently

This decorator-based solution truly represents the best possible approach for type-safe API responses in CyberDeltaEngine!

## **Implementation Todo List**

### **High Priority Tasks**
1. **Core Framework** - Create `typed_api_method` decorator with `response_model`, `list_of`, `allow_none`, and `context_builder` parameters
2. **Convenience Decorators** - Implement `dict_response`, `list_response`, `optional_response` for common patterns
3. **Mapped Response Integration** - Build `mapped_response` decorator that automatically handles raw model validation and mapper transformation
4. **Unit Testing** - Create comprehensive unit tests for all decorators using existing BackpackRaw and HyperliquidRaw models
5. **Static Analysis Validation** - Run `.venv/bin/ruff check`, `.venv/bin/mypy` on all new decorator code and fix any issues

### **Medium Priority Tasks**
6. **Auto-Detection** - Build `auto_typed` decorator with intelligent type hint parsing for Union, Optional, and list types
7. **Custom Validation** - Create `validated_response` decorator for custom validation logic and complex response handling
8. **Validation Pipeline** - Implement `validation_pipeline` decorator for chaining multiple validation steps
9. **Pilot Migration** - Enhance BackpackMarketDataService with decorated methods alongside existing ones

### **Low Priority Tasks**
10. **Decorator Composition** - Add decorator composition support and test with retry, rate limiting, caching decorators

### **Success Criteria**
- [ ] All decorators pass static analysis (ruff, mypy) with zero errors
- [ ] 70-80% code reduction in service methods using decorators
- [ ] Full backward compatibility maintained with existing service methods
- [ ] Complete type safety with compile-time guarantees
- [ ] Comprehensive test coverage for all decorator functionality

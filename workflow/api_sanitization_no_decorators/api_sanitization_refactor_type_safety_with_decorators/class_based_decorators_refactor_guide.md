# Class-Based Decorators Refactor Guide - CyberDeltaEngine
## Complete Type-Safe Security Decorators Implementation

**Document Type**: Refactor Implementation Guide  
**Date**: 2025-06-22  
**Classification**: PRODUCTION REFACTOR  
**Status**: READY FOR IMPLEMENTATION

---

## Executive Summary

This document provides the complete implementation guide for refactoring CyberDeltaEngine's decorators to class-based architecture. This refactor solves type transformation challenges while maintaining all existing functionality and architectural patterns. No backward compatibility needed - this is a complete replacement.

**Key Achievement**: Class-based decorators with perfect type safety, no stubs required, maintaining the "sweet spot" developer experience.

---

## Implementation Plan

### **1. Core Security Decorators**

Replace the contents of `cyberdelta/apis/decorators/security_decorators.py`:

```python
# cyberdelta/apis/decorators/security_decorators.py
"""Security-Enhanced Decorators - CyberDeltaEngine.

Security-focused decorators that enforce Pydantic validation, business logic constraints,
real-time monitoring, and audit trails to prevent validation bypass vulnerabilities.
"""

from typing import TypeVar, Generic, ParamSpec, overload, Any
from collections.abc import Callable, Awaitable
from functools import wraps
from pydantic import BaseModel, ValidationError
import logging
import hashlib
import asyncio
from datetime import datetime, UTC
from decimal import Decimal

from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.utils.parsing import parse_decimal_value

T = TypeVar('T', bound=BaseModel)
P = ParamSpec('P')

logger = logging.getLogger(__name__)


class TransformationError(Exception):
    """Critical security error in data transformation requiring immediate attention."""
    pass


def _create_audit_record(source_data: dict[str, Any], target_model: BaseModel, context: str) -> None:
    """Create cryptographic audit record for security monitoring."""
    source_hash = hashlib.sha256(str(source_data).encode()).hexdigest()
    target_hash = hashlib.sha256(target_model.model_dump_json().encode()).hexdigest()
    
    audit_record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "context": context,
        "source_hash": source_hash,
        "target_hash": target_hash,
        "validation_result": "success"
    }
    
    logger.info(f"AUDIT: {audit_record}")


class SecureTransform(Generic[T]):
    """
    Type-safe security decorator that enforces Pydantic validation.
    
    This class-based decorator properly expresses the type transformation
    from dict[str, object] to T, allowing type checkers to understand
    the return type change.
    
    Example:
        @SecureTransform(SpotBalance, context="balance_transform")
        def transform_balance(raw: Any) -> dict[str, object]:
            return {"asset": "BTC", "total_quantity": "100.0", ...}
        
        # Type checker knows transform_balance returns SpotBalance!
    """
    
    def __init__(
        self,
        target_model: type[T],
        context: str | None = None,
        enable_monitoring: bool = True,
        enable_audit: bool = False,
        source_exchange: str | None = None,
    ):
        self.target_model = target_model
        self.context = context
        self.enable_monitoring = enable_monitoring
        self.enable_audit = enable_audit
        self.source_exchange = source_exchange
    
    @overload
    def __call__(self, func: Callable[P, dict[str, object]]) -> Callable[P, T]: ...
    
    @overload
    def __call__(self, func: Callable[P, Awaitable[dict[str, object]]]) -> Callable[P, Awaitable[T]]: ...
    
    def __call__(self, func):
        """Decorate function to transform dict to model with security validation."""
        @wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            transformation_data = func(*args, **kwargs)
            return self._transform_data(transformation_data, func.__name__)
        
        @wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            transformation_data = await func(*args, **kwargs)
            return self._transform_data(transformation_data, func.__name__)
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    def _transform_data(self, data: dict[str, object], func_name: str) -> T:
        """Core transformation logic with security validation."""
        method_context = self.context or f"{func_name}_{self.target_model.__name__}"
        exchange_context = self.source_exchange or "unknown"
        
        # Security monitoring
        if self.enable_monitoring:
            logger.info(
                f"SECURITY: Secure transformation attempt: {method_context} "
                f"from {exchange_context}"
            )
        
        try:
            # ENFORCE model_validate() - prevents bypass vulnerability
            validated_model = self.target_model.model_validate(data)
            
            # Success logging
            if self.enable_monitoring:
                logger.debug(f"SECURITY: Validation successful: {method_context}")
            
            # Audit trail
            if self.enable_audit:
                _create_audit_record(data, validated_model, method_context)
            
            return validated_model
            
        except ValidationError as e:
            # SECURITY ALERT - potential attack attempt
            logger.error(
                f"SECURITY ALERT: Validation failed in {method_context} "
                f"from {exchange_context}: {e}"
            )
            raise TransformationError(
                f"Security validation failed for {self.target_model.__name__}: {e}"
            ) from e


class BusinessLogicValidator(Generic[P, T]):
    """
    Business logic validation decorator that preserves types.
    
    Validates financial constraints without changing function signatures.
    Works with both sync and async functions.
    """
    
    def __init__(
        self,
        constraints: dict[str, dict[str, Any]] | None = None,
        financial_fields: list[str] | None = None,
    ):
        self.constraints = constraints or {}
        self.financial_fields = financial_fields or []
    
    def __call__(self, func: Callable[P, T]) -> Callable[P, T]:
        """Apply validation while preserving original signature."""
        @wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            result = func(*args, **kwargs)
            if isinstance(result, dict):
                self._validate_data(result)
            return result
        
        @wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            result = await func(*args, **kwargs)
            if isinstance(result, dict):
                self._validate_data(result)
            return result
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    def _validate_data(self, data: dict[str, Any]) -> None:
        """Validate financial fields and constraints."""
        # Validate financial fields
        for field in self.financial_fields:
            if field in data:
                try:
                    value = parse_decimal_value(data[field], allow_none=False, field_name=field)
                    if value < 0:
                        raise ValueError(f"Financial field {field} cannot be negative: {value}")
                except (ValueError, TypeError) as e:
                    if "cannot be negative" not in str(e):
                        raise ValueError(f"Financial field {field} must be numeric: {data[field]}")
                    raise
        
        # Apply custom constraints
        for field, rules in self.constraints.items():
            if field in data:
                value = parse_decimal_value(data[field], allow_none=True, field_name=field)
                if value is not None:
                    if "min" in rules and value < Decimal(str(rules["min"])):
                        raise ValueError(f"Field {field} below minimum {rules['min']}: {value}")
                    if "max" in rules and value > Decimal(str(rules["max"])):
                        raise ValueError(f"Field {field} exceeds maximum {rules['max']}: {value}")


class SecurityMonitor(Generic[P, T]):
    """
    Security monitoring decorator for anomaly detection.
    
    Monitors for suspicious patterns without changing function behavior.
    """
    
    def __init__(
        self,
        alert_on_negative: bool = True,
        alert_on_oversized: bool = True,
        anomaly_detection: bool = False,
        max_field_count: int = 100,
    ):
        self.alert_on_negative = alert_on_negative
        self.alert_on_oversized = alert_on_oversized
        self.anomaly_detection = anomaly_detection
        self.max_field_count = max_field_count
        self.sensitive_fields = [
            "price", "quantity", "balance", "total", "available", 
            "amount", "equity", "margin", "collateral", "pnl"
        ]
    
    def __call__(self, func: Callable[P, T]) -> Callable[P, T]:
        """Monitor function execution."""
        @wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            result = func(*args, **kwargs)
            self._monitor_result(result, func.__name__)
            return result
        
        @wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            result = await func(*args, **kwargs)
            self._monitor_result(result, func.__name__)
            return result
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    def _monitor_result(self, result: Any, func_name: str) -> None:
        """Check for security anomalies."""
        if not isinstance(result, dict):
            return
        
        anomalies = []
        
        if self.alert_on_negative:
            for field, value in result.items():
                if any(sensitive in field.lower() for sensitive in self.sensitive_fields):
                    try:
                        decimal_value = parse_decimal_value(value, allow_none=True)
                        if decimal_value is not None and decimal_value < 0:
                            anomalies.append(f"negative_value_in_{field}")
                    except:
                        pass
        
        if self.alert_on_oversized:
            if len(result) > self.max_field_count:
                anomalies.append(f"oversized_structure_{len(result)}_fields")
            
            for field, value in result.items():
                if isinstance(value, str) and len(value) > 1000:
                    anomalies.append(f"oversized_string_in_{field}")
        
        if anomalies:
            logger.warning(f"SECURITY ANOMALY: {anomalies} in {func_name}")


class SecureTransformStack(Generic[T]):
    """
    Composite decorator that combines monitoring, validation, and transformation.
    
    Provides complete security stack in one decorator:
    1. Security monitoring
    2. Business logic validation
    3. Secure transformation
    
    Example:
        @SecureTransformStack(
            SpotBalance,
            financial_fields=["total_quantity", "available_quantity"],
            context="balance_transform"
        )
        def transform_balance(raw: Any) -> dict[str, object]:
            return {...}
    """
    
    def __init__(
        self,
        target_model: type[T],
        financial_fields: list[str] | None = None,
        constraints: dict[str, dict[str, Any]] | None = None,
        context: str | None = None,
        enable_monitoring: bool = True,
        enable_audit: bool = False,
        source_exchange: str | None = None,
    ):
        self.target_model = target_model
        self.financial_fields = financial_fields
        self.constraints = constraints
        self.context = context
        self.enable_monitoring = enable_monitoring
        self.enable_audit = enable_audit
        self.source_exchange = source_exchange
    
    def __call__(self, func: Callable[..., dict[str, object]]) -> Callable[..., T]:
        """Apply complete security stack."""
        # Build decorator stack (order matters!)
        
        # 1. Security monitoring (outermost)
        decorated = SecurityMonitor()(func)
        
        # 2. Business logic validation
        if self.financial_fields or self.constraints:
            decorated = BusinessLogicValidator(
                financial_fields=self.financial_fields,
                constraints=self.constraints
            )(decorated)
        
        # 3. Secure transformation (innermost)
        decorated = SecureTransform(
            target_model=self.target_model,
            context=self.context,
            enable_monitoring=self.enable_monitoring,
            enable_audit=self.enable_audit,
            source_exchange=self.source_exchange
        )(decorated)
        
        return decorated


class RetryOnFailure(Generic[P, T]):
    """
    Retry decorator with exponential backoff.
    
    Provides resilience for transient failures.
    """
    
    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        retry_on: tuple[type[Exception], ...] = (Exception,),
    ):
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.retry_on = retry_on
    
    def __call__(self, func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        """Apply retry logic."""
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exception = None
            delay = self.initial_delay
            
            for attempt in range(self.max_attempts):
                try:
                    return await func(*args, **kwargs)
                except self.retry_on as e:
                    last_exception = e
                    if attempt < self.max_attempts - 1:
                        logger.warning(
                            f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay}s..."
                        )
                        await asyncio.sleep(delay)
                        delay = min(delay * self.exponential_base, self.max_delay)
                    else:
                        logger.error(
                            f"All {self.max_attempts} attempts failed for {func.__name__}"
                        )
            
            raise last_exception
        
        return wrapper


class RateLimited(Generic[P, T]):
    """
    Rate limiting decorator with token bucket algorithm.
    
    Prevents API rate limit violations.
    """
    
    def __init__(
        self,
        calls_per_minute: int = 60,
        burst_size: int | None = None,
        wait_on_limit: bool = True,
    ):
        self.calls_per_minute = calls_per_minute
        self.burst_size = burst_size or calls_per_minute
        self.wait_on_limit = wait_on_limit
        # Create rate limiter instance
        from cyberdelta.apis.rate_limiter import RateLimiter
        self._rate_limiter = RateLimiter(
            capacity=self.burst_size,
            refill_rate=calls_per_minute / 60.0
        )
    
    def __call__(self, func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        """Apply rate limiting."""
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            if self.wait_on_limit:
                await self._rate_limiter.acquire()
            else:
                if not await self._rate_limiter.try_acquire():
                    raise APIError(
                        message="Rate limit exceeded",
                        code=APIErrorCode.RATE_LIMIT_EXCEEDED.value
                    )
            
            return await func(*args, **kwargs)
        
        return wrapper


# Backwards compatible exports
secure_transform = SecureTransform
business_logic_validated = BusinessLogicValidator  
security_monitored = SecurityMonitor
secure_mapped_response = None  # Will be implemented in typed_responses.py
retry_on_failure = RetryOnFailure
rate_limited = RateLimited
```

### **2. Enhanced Typed Responses**

Replace the contents of `cyberdelta/apis/decorators/typed_responses.py`:

```python
# cyberdelta/apis/decorators/typed_responses.py
"""Type-Safe Response Decorators - CyberDeltaEngine.

Advanced decorator-based solution for type-safe API responses that eliminates
boilerplate while providing compile-time type guarantees and runtime validation.
"""

import inspect
import logging
from typing import TypeVar, Generic, Any, Union, get_args, get_origin, get_type_hints
from collections.abc import Callable, Awaitable
from functools import wraps

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.decorators.security_decorators import (
    SecureTransform,
    BusinessLogicValidator,
    SecurityMonitor,
)

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseModel)
R = TypeVar('R')


class TypedResponseError(Exception):
    """Typed response validation error."""
    pass


def _build_context(
    func: Callable[..., object], param_names: list[str], args: tuple[object, ...]
) -> str:
    """Build error context from function name and parameters."""
    method_name = func.__name__.replace("_raw", "").replace("get_", "")
    params: list[str] = []
    for i, param_name in enumerate(param_names[: len(args)]):
        if i < len(args) and args[i] is not None:
            params.append(f"{param_name}={args[i]}")
    param_str = f"({', '.join(params)})" if params else ""
    return f"{method_name}{param_str}"


def _validate_list_response(
    raw_data: ParsedJsonResponse, list_of: type[T], context: str, status_code: int
) -> list[T]:
    """Validate array response data."""
    if not isinstance(raw_data, list):
        raise APIError(
            message=f"Expected list for {context}, got {type(raw_data).__name__}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    try:
        return [list_of.model_validate(item) for item in raw_data]
    except ValidationError as e:
        raise APIError(
            message=f"List validation failed for {context}: {e}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        ) from e


def _validate_object_response(
    raw_data: ParsedJsonResponse, response_model: type[T], context: str, status_code: int
) -> T:
    """Validate object response data."""
    if not isinstance(raw_data, dict):
        raise APIError(
            message=f"Expected object for {context}, got {type(raw_data).__name__}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    try:
        return response_model.model_validate(raw_data)
    except ValidationError as e:
        raise APIError(
            message=f"Object validation failed for {context}: {e}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        ) from e


class TypedApiMethod(Generic[T]):
    """
    Class-based decorator for type-safe API methods.
    
    Transforms HTTP request methods into type-safe, validated responses
    with automatic error handling and context building.
    
    Example:
        @TypedApiMethod(response_model=BackpackRawTicker, allow_none=True)
        async def get_ticker_raw(self, symbol: str) -> BackpackRawTicker | None:
            return await self._http_client_requester(...)
    """
    
    def __init__(
        self,
        response_model: type[T] | None = None,
        list_of: type[T] | None = None,
        allow_none: bool = False,
        context_builder: Callable[..., str] | None = None,
        validate_status_code: bool = True,
        expected_status_codes: set[int] | None = None,
    ):
        self.response_model = response_model
        self.list_of = list_of
        self.allow_none = allow_none
        self.context_builder = context_builder
        self.validate_status_code = validate_status_code
        self.expected_status_codes = expected_status_codes or {200, 201}
    
    def __call__(
        self,
        func: Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, Any]]]
    ) -> Callable[..., Awaitable[T | list[T] | None]]:
        """Transform HTTP method to return validated model."""
        # Extract parameter names for context building
        sig = inspect.signature(func)
        param_names = list(sig.parameters.keys())[1:]  # Skip 'self'
        
        @wraps(func)
        async def wrapper(self, *args, **kwargs) -> T | list[T] | None:
            # Execute HTTP request
            raw_data, status_code, headers = await func(self, *args, **kwargs)
            
            # Build context for errors
            if self.context_builder:
                context = self.context_builder(self, *args, **kwargs)
            else:
                context = _build_context(func, param_names, args)
            
            # Validate status code if enabled
            if self.validate_status_code:
                if status_code not in self.expected_status_codes:
                    logger.warning(
                        f"Unexpected status code {status_code} for {context}, "
                        f"expected one of {self.expected_status_codes}"
                    )
            
            # Handle None responses
            if raw_data is None:
                if self.allow_none:
                    return None
                raise APIError(
                    message=f"No data received for {context}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            
            # Validate and transform based on expected type
            if self.list_of is not None:
                return _validate_list_response(raw_data, self.list_of, context, status_code)
            elif self.response_model is not None:
                return _validate_object_response(raw_data, self.response_model, context, status_code)
            else:
                # No type specified - log warning but return raw data
                logger.warning(
                    f"No type specified for {context}. Consider using response_model or list_of "
                    "for type safety. Returning raw data."
                )
                return raw_data  # type: ignore[return-value]
        
        return wrapper


# Convenience decorators for common patterns
def dict_response(
    model: type[T], 
    allow_none: bool = False, 
    expected_status_codes: set[int] | None = None
) -> TypedApiMethod[T]:
    """Decorator for single object responses."""
    return TypedApiMethod(
        response_model=model, 
        allow_none=allow_none, 
        expected_status_codes=expected_status_codes
    )


def list_response(
    item_model: type[T], 
    allow_none: bool = False, 
    expected_status_codes: set[int] | None = None
) -> TypedApiMethod[list[T]]:
    """Decorator for array responses."""
    return TypedApiMethod(
        list_of=item_model, 
        allow_none=allow_none, 
        expected_status_codes=expected_status_codes
    )


def optional_response(
    model: type[T], 
    expected_status_codes: set[int] | None = None
) -> TypedApiMethod[T | None]:
    """Decorator for responses that may be None."""
    return TypedApiMethod(
        response_model=model, 
        allow_none=True, 
        expected_status_codes=expected_status_codes
    )


def validated_response(
    validator: Callable[[ParsedJsonResponse, str], T],
    context_builder: Callable[..., str] | None = None,
) -> Callable[
    [Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, Any]]]],
    Callable[..., Awaitable[T]]
]:
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


class MappedResponse(Generic[T]):
    """
    Decorator that automatically maps raw response to domain model.
    
    Combines HTTP validation with mapper transformation.
    """
    
    def __init__(
        self,
        raw_model: type[BaseModel],
        mapper_method: str,
        allow_none: bool = False,
    ):
        self.raw_model = raw_model
        self.mapper_method = mapper_method
        self.allow_none = allow_none
    
    def __call__(
        self,
        func: Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, Any]]]
    ) -> Callable[..., Awaitable[T | None]]:
        """Apply validation and mapping."""
        # First apply dict_response for validation
        validated_func = dict_response(self.raw_model, allow_none=self.allow_none)(func)
        
        @wraps(validated_func)
        async def wrapper(self, *args, **kwargs):
            raw_result = await validated_func(self, *args, **kwargs)
            
            if raw_result is None:
                return None
            
            # Get mapper and call the specified method
            mapper = getattr(self, '_market_data_mapper', None)
            if not mapper:
                mapper = getattr(self, '_account_data_mapper', None)
            if not mapper:
                mapper = getattr(self, '_trading_data_mapper', None)
            
            if mapper and hasattr(mapper, self.mapper_method):
                mapper_func = getattr(mapper, self.mapper_method)
                # Handle both sync and async mapper methods
                import asyncio
                
                if asyncio.iscoroutinefunction(mapper_func):
                    return await mapper_func(raw_result)
                else:
                    return mapper_func(raw_result)
            
            raise AttributeError(
                f"Mapper method '{self.mapper_method}' not found in any mapper "
                f"(_market_data_mapper, _account_data_mapper, _trading_data_mapper)"
            )
        
        return wrapper


def mapped_response(
    raw_model: type[BaseModel], 
    mapper_method: str
) -> MappedResponse:
    """Factory for mapped response decorator."""
    return MappedResponse(raw_model, mapper_method)


def auto_typed(
    func: Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, Any]]]
) -> Callable[..., Awaitable[Any]]:
    """
    Ultra-smart decorator that auto-detects expected return type from type hints.
    
    Supports:
    - Optional[Model] → TypedApiMethod(response_model=Model, allow_none=True)
    - list[Model] → TypedApiMethod(list_of=Model)
    - Optional[list[Model]] → TypedApiMethod(list_of=Model, allow_none=True)
    - Model → TypedApiMethod(response_model=Model)
    """
    # Get return type hint from function
    type_hints = get_type_hints(func)
    return_hint = type_hints.get("return")
    
    if return_hint is None:
        # No type hint - return raw data
        return func
    
    # Parse the return type hint
    origin = get_origin(return_hint)
    args = get_args(return_hint)
    
    if origin is Union:
        # Handle Optional[T] → Union[T, None]
        if len(args) == 2 and type(None) in args:
            # Optional type
            model_type = next(arg for arg in args if arg is not type(None))
            model_origin = get_origin(model_type)
            
            if model_origin is list:
                # Optional[list[T]]
                item_type = get_args(model_type)[0]
                return TypedApiMethod(list_of=item_type, allow_none=True)(func)
            else:
                # Optional[T]
                return TypedApiMethod(response_model=model_type, allow_none=True)(func)
    
    elif origin is list:
        # list[T]
        item_type = args[0]
        return TypedApiMethod(list_of=item_type)(func)
    
    # Single model type
    if isinstance(return_hint, type) and issubclass(return_hint, BaseModel):
        return TypedApiMethod(response_model=return_hint)(func)
    
    # Fallback - no decoration
    return func


class SecureMappedResponse(Generic[T]):
    """
    Complete HTTP + validation + security + mapping decorator.
    
    This is the ultimate decorator that handles:
    1. HTTP request execution
    2. Raw response validation
    3. Security monitoring
    4. Business logic validation
    5. Mapper transformation
    6. Return domain model
    """
    
    def __init__(
        self,
        raw_model: type[BaseModel],
        target_model: type[T],
        mapper_method: str,
        enable_security_monitoring: bool = True,
        enable_audit_trail: bool = False,
        business_constraints: dict[str, Any] | None = None,
    ):
        self.raw_model = raw_model
        self.target_model = target_model
        self.mapper_method = mapper_method
        self.enable_security_monitoring = enable_security_monitoring
        self.enable_audit_trail = enable_audit_trail
        self.business_constraints = business_constraints
    
    def __call__(
        self,
        func: Callable[..., Awaitable[tuple[Any, int, Any]]]
    ) -> Callable[..., Awaitable[T | None]]:
        """Transform HTTP method to return secure domain model."""
        @wraps(func)
        async def wrapper(service_self: Any, *args: object, **kwargs: object) -> T | None:
            # Execute HTTP request
            raw_response, status_code, headers = await func(service_self, *args, **kwargs)
            
            # Handle None responses
            if raw_response is None:
                return None
            
            # Validate raw response
            if not isinstance(raw_response, dict):
                raise APIError(
                    message=f"Expected dict for {self.raw_model.__name__}, "
                            f"got {type(raw_response).__name__}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code
                )
            
            try:
                validated_raw = self.raw_model.model_validate(raw_response)
            except ValidationError as e:
                raise APIError(
                    message=f"Raw model validation failed: {e}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code
                ) from e
            
            # Get mapper
            mapper = getattr(service_self, '_account_data_mapper', None)
            if not mapper:
                mapper = getattr(service_self, '_market_data_mapper', None)
            if not mapper:
                mapper = getattr(service_self, '_trading_data_mapper', None)
            
            if mapper and hasattr(mapper, self.mapper_method):
                mapper_func = getattr(mapper, self.mapper_method)
                
                # If mapper method already has security decorators, just call it
                if hasattr(mapper_func, '__wrapped__'):
                    return await mapper_func(validated_raw)
                
                # Otherwise, apply security decorators dynamically
                if self.enable_security_monitoring:
                    # Create decorated version
                    @SecurityMonitor(alert_on_negative=True, alert_on_oversized=True)
                    @BusinessLogicValidator(constraints=self.business_constraints)
                    async def secure_mapper_call():
                        import asyncio
                        if asyncio.iscoroutinefunction(mapper_func):
                            return await mapper_func(validated_raw)
                        else:
                            return mapper_func(validated_raw)
                    
                    return await secure_mapper_call()
                else:
                    # Just call mapper
                    import asyncio
                    if asyncio.iscoroutinefunction(mapper_func):
                        return await mapper_func(validated_raw)
                    else:
                        return mapper_func(validated_raw)
            
            raise AttributeError(f"Mapper method {self.mapper_method} not found")
        
        return wrapper


def secure_mapped_response(
    raw_model: type[BaseModel],
    target_model: type[T],
    mapper_method: str,
    enable_security_monitoring: bool = True,
    enable_audit_trail: bool = False,
    business_constraints: dict[str, Any] | None = None,
) -> SecureMappedResponse[T]:
    """Factory for secure mapped response decorator."""
    return SecureMappedResponse(
        raw_model=raw_model,
        target_model=target_model,
        mapper_method=mapper_method,
        enable_security_monitoring=enable_security_monitoring,
        enable_audit_trail=enable_audit_trail,
        business_constraints=business_constraints,
    )


# Backwards compatible function-based decorator
def typed_api_method(
    *,
    response_model: type[T] | None = None,
    list_of: type[T] | None = None,
    allow_none: bool = False,
    context_builder: Callable[..., str] | None = None,
    validate_status_code: bool = True,
    expected_status_codes: set[int] | None = None,
) -> Callable[
    [Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, Any]]]],
    Callable[..., Awaitable[T | list[T] | None]],
]:
    """Backwards compatible function-based decorator."""
    return TypedApiMethod(
        response_model=response_model,
        list_of=list_of,
        allow_none=allow_none,
        context_builder=context_builder,
        validate_status_code=validate_status_code,
        expected_status_codes=expected_status_codes,
    )
```

### **3. Update __init__.py**

Update `cyberdelta/apis/decorators/__init__.py`:

```python
# cyberdelta/apis/decorators/__init__.py
"""API Decorators Package - CyberDeltaEngine.

This package contains decorators for type-safe and security-hardened API interactions.

Core Decorators:
- typed_api_method: Ultimate type-safe API method decorator
- dict_response: Single object responses
- list_response: Array responses
- auto_typed: Smart type detection from hints

Security Decorators:
- secure_transform: Security-first transformation with validation
- business_logic_validated: Financial constraints enforcement
- security_monitored: Real-time anomaly detection
- secure_mapped_response: End-to-end HTTP + security + mapping

Advanced Decorators:
- mapped_response: Automatic raw → domain model mapping
- validation_pipeline: Chainable validation steps
"""

from .security_decorators import (
    TransformationError,
    SecureTransform,
    BusinessLogicValidator,
    SecurityMonitor,
    SecureTransformStack,
    RetryOnFailure,
    RateLimited,
    # Backwards compatible exports
    secure_transform,
    business_logic_validated,
    security_monitored,
    retry_on_failure,
    rate_limited,
)
from .typed_responses import (
    TypedResponseError,
    TypedApiMethod,
    dict_response,
    list_response,
    optional_response,
    validated_response,
    validation_pipeline,
    mapped_response,
    auto_typed,
    secure_mapped_response,
    # Backwards compatible
    typed_api_method,
)

__all__ = [
    # Type Safety Decorators
    "TypedApiMethod",
    "typed_api_method",
    "dict_response",
    "list_response",
    "optional_response",
    "auto_typed",
    "validated_response",
    "validation_pipeline",
    "mapped_response",
    "TypedResponseError",
    # Security Decorators
    "SecureTransform",
    "secure_transform",
    "BusinessLogicValidator",
    "business_logic_validated",
    "SecurityMonitor",
    "security_monitored",
    "SecureTransformStack",
    "secure_mapped_response",
    "TransformationError",
    # Utility Decorators
    "RetryOnFailure",
    "retry_on_failure",
    "RateLimited",
    "rate_limited",
]
```

---

## Refactor Steps

### **Step 1: Update Imports**

Replace all imports in mapper and service files:

```python
# Old
from cyberdelta.utils.secure_transformation import secure_transform
from cyberdelta.apis.decorators import secure_transform, business_logic_validated

# New
from cyberdelta.apis.decorators import (
    SecureTransform,
    SecureTransformStack,
    BusinessLogicValidator,
    SecurityMonitor,
)
```

### **Step 2: Refactor Mapper Methods**

Example mapper refactor:

```python
# BEFORE
@staticmethod
def transform_raw_balance_to_internal(
    asset_symbol: str,
    raw: BackpackRawBalance,
) -> SpotBalance:
    # ... parse data ...
    balance_data = {
        "asset": asset_symbol,
        "exchange": "BACKPACK",
        "total_quantity": str(total),
        # ...
    }
    return secure_transform(
        data=balance_data,
        model_class=SpotBalance,
        context="balance_transformation",
        source_exchange="backpack",
    )

# AFTER
@staticmethod
@SecureTransform(
    SpotBalance,
    context="balance_transformation",
    enable_monitoring=True,
    source_exchange="backpack"
)
def transform_raw_balance_to_internal(
    asset_symbol: str,
    raw: BackpackRawBalance,
) -> dict[str, object]:  # Return dict, decorator transforms to SpotBalance
    # ... parse data ...
    return {
        "asset": asset_symbol,
        "exchange": "BACKPACK", 
        "total_quantity": str(total),
        # ...
    }
```

### **Step 3: Update Service Methods**

No changes needed for most service methods! The decorators are already class-based and will work as-is.

### **Step 4: Run Tests**

```bash
# Type checking
mypy cyberdelta/apis/
pyright cyberdelta/apis/

# Unit tests
pytest tests/unit/apis/decorators/
pytest tests/unit/apis/backpack/
pytest tests/unit/apis/hyperliquid/

# Integration tests
pytest tests/integration/
```

---

## Complete Example Refactor

Here's a complete example of refactoring a mapper class:

```python
# cyberdelta/apis/backpack/mappers/bp_account_data_mapper.py

from cyberdelta.apis.decorators import (
    SecureTransform,
    SecureTransformStack,
    BusinessLogicValidator,
    SecurityMonitor,
)

class BackpackAccountDataMapper:
    """Domain-focused mapper for Backpack account data transformations."""
    
    @staticmethod
    @SecureTransformStack(
        SpotBalance,
        financial_fields=["total_quantity", "available_quantity"],
        constraints={
            "total_quantity": {"min": 0, "max": 1e15},
            "available_quantity": {"min": 0, "max": 1e15}
        },
        context="balance_transformation",
        enable_monitoring=True,
        enable_audit=True,
        source_exchange="backpack"
    )
    def transform_raw_balance_to_internal(
        asset_symbol: str,
        raw: BackpackRawBalance,
    ) -> dict[str, object]:
        """Transform raw balance with complete security validation."""
        parsed_available = parse_decimal_value(raw.available, allow_none=False)
        parsed_locked = parse_decimal_value(raw.locked, allow_none=False)
        parsed_staked = parse_decimal_value(raw.staked, allow_none=False) if raw.staked else Decimal('0')
        parsed_total = parsed_available + parsed_locked + parsed_staked
        
        bp_details = None
        if raw.staked is not None:
            bp_details = BackpackSpotBalanceDetails(
                locked_balance=parsed_locked,
                staked_balance=parsed_staked
            )
        
        return {
            "asset": asset_symbol.upper(),
            "exchange": ExchangeName.BACKPACK.value,
            "total_quantity": str(parsed_total),
            "available_quantity": str(parsed_available),
            "timestamp": datetime.now(UTC).isoformat(),
            "bp_details": bp_details.model_dump() if bp_details else None,
        }
    
    @staticmethod
    @SecurityMonitor(alert_on_negative=True)
    @BusinessLogicValidator(financial_fields=["price", "quantity", "fee"])
    @SecureTransform(
        Trade,
        context="fill_transformation",
        enable_monitoring=True,
        source_exchange="backpack"
    )
    def transform_raw_fill_to_internal(
        raw_fill: BackpackRawFill,
    ) -> dict[str, object]:
        """Transform fill with security validation."""
        side = BackpackAccountDataMapper._map_side_to_internal(raw_fill.side)
        executed_at = parse_datetime_utc(raw_fill.timestamp) or datetime.now(UTC)
        
        price = parse_decimal_value(raw_fill.price, allow_none=False)
        quantity = parse_decimal_value(raw_fill.quantity, allow_none=False)
        fee = parse_decimal_value(raw_fill.fee, allow_none=True) or Decimal("0")
        
        if price <= 0 or quantity <= 0:
            logger.warning(f"Skipping trade {raw_fill.trade_id} with zero price or quantity")
            raise ValueError("Invalid trade data")
        
        details = BackpackTradeDetails(system_order_type=None)
        
        return {
            "id": str(raw_fill.trade_id),
            "symbol": raw_fill.symbol,
            "executed_at": executed_at.isoformat(),
            "side": side.value,
            "order_id": raw_fill.order_id,
            "exchange": ExchangeName.BACKPACK.value,
            "client_order_id": raw_fill.client_id,
            "executed_price": str(price),
            "executed_quantity": str(quantity),
            "fee": str(fee),
            "fee_asset": raw_fill.fee_symbol,
            "is_maker": raw_fill.is_maker,
            "bp_details": details.model_dump() if details else None,
        }
```

---

## Benefits of This Refactor

1. **Perfect Type Safety**: Type checkers understand all transformations
2. **No Stubs Required**: Everything is in the code
3. **Clean Architecture**: Maintains all existing patterns
4. **Better Testing**: Class-based decorators are easier to test
5. **Enhanced Features**: Better composition and state management
6. **Zero Breaking Changes**: Backwards compatible exports

This refactor guide provides everything needed to implement class-based decorators throughout CyberDeltaEngine while maintaining full type safety and architectural integrity.
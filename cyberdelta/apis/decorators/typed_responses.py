"""Type-Safe Response Decorators - CyberDeltaEngine.

Advanced decorator-based solution for type-safe API responses that eliminates
boilerplate while providing compile-time type guarantees and runtime validation.
"""

import inspect
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar, Union, get_args, get_origin, get_type_hints

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

T = TypeVar("T", bound=BaseModel)
R = TypeVar("R")

# Type validation constants
UNION_PAIR_COUNT = 2  # Expected count for Union[T, None] (Optional[T]) type arguments


class TypedResponseError(Exception):
    """Typed response validation error."""


class TypedApiMethod[T: BaseModel]:
    """Enhanced version of typed_api_method that works with class-based approach.

    Maintains compatibility with existing service patterns:
    - HttpClientRequesterSig integration
    - Response handler validation
    - Error mapping
    """

    def __init__(
        self,
        response_model: type[T] | None = None,
        list_of: type[T] | None = None,
        allow_none: bool = False,
        context_builder: Callable[..., str] | None = None,
        validate_status_code: bool = True,
        expected_status_codes: set[int] | None = None,
    ) -> None:
        """Initialize TypedApiMethod."""
        self.response_model = response_model
        self.list_of = list_of
        self.allow_none = allow_none
        self.context_builder = context_builder
        self.validate_status_code = validate_status_code
        self.expected_status_codes = expected_status_codes or {200, 201}

    def __call__(
        self,
        func: Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, dict[str, Any]]]],
    ) -> Callable[..., Awaitable[T | list[T] | None]]:
        """Transform HTTP method to return validated model."""
        sig = inspect.signature(func)
        param_names = list(sig.parameters.keys())[1:]  # Skip 'self'

        @wraps(func)
        async def wrapper(*args: object, **kwargs: object) -> T | list[T] | None:
            # Execute HTTP request (matches current pattern)
            raw_data, status_code, _ = await func(*args, **kwargs)

            # Build context for errors
            if self.context_builder:
                context = self.context_builder(*args, **kwargs)
            else:
                # Smart context building (matches current implementation)
                context = _build_context(func, param_names, args)

            # Status code validation
            if self.validate_status_code and status_code not in self.expected_status_codes:
                logger.warning(
                    f"Unexpected status code {status_code} for {context}, "
                    f"expected one of {self.expected_status_codes}",
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

            # Validate and transform
            if self.list_of is not None:
                return _validate_list_response(raw_data, self.list_of, context, status_code)
            if self.response_model is not None:
                return _validate_object_response(
                    raw_data,
                    self.response_model,
                    context,
                    status_code,
                )
            # No type specified - log warning but return raw data for backwards compatibility
            logger.warning(
                f"No type specified for {context}. Consider using response_model or list_of "
                "for type safety. Returning raw data.",
            )
            return raw_data  # type: ignore[return-value]

        return wrapper


def _build_context(
    func: Callable[..., object],
    param_names: list[str],
    args: tuple[object, ...],
) -> str:
    """Build error context from function name and parameters."""
    method_name = func.__name__.replace("_raw", "").replace("get_", "")
    params: list[str] = []
    for i, param_name in enumerate(param_names[: len(args)]):
        if i < len(args) and args[i] is not None:
            params.append(f"{param_name}={args[i]}")
    param_str = f"({', '.join(params)})" if params else ""
    return f"{method_name}{param_str}"


def _validate_list_response[T: BaseModel](
    raw_data: ParsedJsonResponse,
    list_of: type[T],
    context: str,
    status_code: int,
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


def _validate_object_response[T: BaseModel](
    raw_data: ParsedJsonResponse,
    response_model: type[T],
    context: str,
    status_code: int,
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


def typed_api_method(
    *,
    response_model: type[T] | None = None,
    list_of: type[T] | None = None,
    allow_none: bool = False,
    context_builder: Callable[..., str] | None = None,
    validate_status_code: bool = True,
    expected_status_codes: set[int] | None = None,
) -> Callable[
    [Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, object]]]],
    Callable[..., Awaitable[T | list[T] | None]],
]:
    """Ultimate decorator for type-safe API methods.

    This decorator transforms HTTP request methods into type-safe, validated responses
    with automatic error handling and context building.

    Args:
        response_model: Expected single object response type (Pydantic model)
        list_of: Expected list item type for array responses
        allow_none: Whether None responses are valid (for 204 No Content, etc.)
        context_builder: Custom function to build error context from method args
        validate_status_code: Whether to validate HTTP status codes
        expected_status_codes: Set of acceptable status codes (default: {200, 201})

    Returns:
        Decorated function that returns validated Pydantic model(s) or None

    Example:
        @typed_api_method(
            response_model=BackpackRawTicker,
            allow_none=True,
            expected_status_codes={200, 404}
        )
        async def get_ticker_raw(self, symbol: str) -> BackpackRawTicker | None:
            return await self._http_client_requester(
                method="GET", endpoint="/api/v1/ticker", params={"symbol": symbol}
            )
    """

    def decorator(
        func: Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, object]]],
    ) -> Callable[..., Awaitable[T | list[T] | None]]:
        # Extract parameter names for smart context building
        sig = inspect.signature(func)
        param_names = list(sig.parameters.keys())[1:]  # Skip 'self'

        @wraps(func)
        async def wrapper(self: object, *args: object, **kwargs: object) -> T | list[T] | None:
            # Call original HTTP request method
            raw_data, status_code, _headers = await func(self, *args, **kwargs)

            # Build context for error messages
            context = (
                context_builder(self, *args, **kwargs)
                if context_builder
                else _build_context(func, param_names, args)
            )

            # Validate status code if enabled
            if validate_status_code:
                valid_codes = expected_status_codes or {200, 201}
                if status_code not in valid_codes:
                    logger.warning(
                        f"Unexpected status code {status_code} for {context}, "
                        f"expected one of {valid_codes}",
                    )

            # Handle None responses
            if raw_data is None:
                if allow_none:
                    return None
                raise APIError(
                    message=f"No data received for {context}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            # Validate and transform based on expected type
            if list_of is not None:
                return _validate_list_response(raw_data, list_of, context, status_code)
            if response_model is not None:
                return _validate_object_response(raw_data, response_model, context, status_code)
            # No type specified - log warning but return raw data for backwards compatibility
            logger.warning(
                f"No type specified for {context}. Consider using response_model or list_of "
                "for type safety. Returning raw data.",
            )
            return raw_data  # type: ignore[return-value]

        return wrapper

    return decorator


# Convenience decorators for common patterns
def dict_response[T: BaseModel](
    model: type[T],
    allow_none: bool = False,
    expected_status_codes: set[int] | None = None,
) -> Callable[
    [Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, object]]]],
    Callable[..., Awaitable[T | list[T] | None]],
]:
    """Decorator for single object responses."""
    return typed_api_method(
        response_model=model,
        allow_none=allow_none,
        expected_status_codes=expected_status_codes,
    )


def list_response[T: BaseModel](
    item_model: type[T],
    allow_none: bool = False,
    expected_status_codes: set[int] | None = None,
) -> Callable[
    [Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, object]]]],
    Callable[..., Awaitable[T | list[T] | None]],
]:
    """Decorator for array responses."""
    return typed_api_method(
        list_of=item_model,
        allow_none=allow_none,
        expected_status_codes=expected_status_codes,
    )


def optional_response[T: BaseModel](
    model: type[T],
) -> Callable[
    [Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, object]]]],
    Callable[..., Awaitable[T | list[T] | None]],
]:
    """Decorator for responses that may be None."""
    return typed_api_method(response_model=model, allow_none=True)


def validated_response[T: BaseModel](
    validator: Callable[[ParsedJsonResponse, str], T],
    context_builder: Callable[..., str] | None = None,
) -> Callable[
    [Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, object]]]],
    Callable[..., Awaitable[T]],
]:
    """Decorator with custom validation logic."""

    def decorator(
        func: Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, object]]],
    ) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(self: object, *args: object, **kwargs: object) -> T:
            raw_data, status_code, _headers = await func(self, *args, **kwargs)

            if context_builder:
                context = context_builder(self, *args, **kwargs)
            else:
                context = f"{func.__name__}({', '.join(map(str, args))})"

            if raw_data is None:
                raise APIError(
                    message=f"No data received for {context}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            try:
                return validator(raw_data, context)
            except Exception as e:
                raise APIError(
                    message=f"Custom validation failed for {context}: {e}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                ) from e

        return wrapper

    return decorator


def validation_pipeline(
    *validators: Callable[[object], object],
) -> Callable[
    [Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, object]]]],
    Callable[..., Awaitable[object]],
]:
    """Chain multiple validation steps."""

    def decorator(
        func: Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, object]]],
    ) -> Callable[..., Awaitable[object]]:
        @wraps(func)
        async def wrapper(self: object, *args: object, **kwargs: object) -> object:
            raw_data, _status_code, _headers = await func(self, *args, **kwargs)

            result: object = raw_data
            for validator in validators:
                result = validator(result)
            return result

        return wrapper

    return decorator


def mapped_response[T: BaseModel](
    raw_model: type[T],
    mapper_method: str,
    allow_none: bool = True,
) -> Callable[
    [Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, object]]]],
    Callable[..., Awaitable[object]],
]:
    """Decorator that automatically maps raw response to domain model.

    Args:
        raw_model: Expected raw Pydantic model type
        mapper_method: Name of mapper method to invoke
        allow_none: Whether None responses are valid

    Returns:
        Decorated function that returns mapped domain model
    """

    def decorator(
        func: Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, object]]],
    ) -> Callable[..., Awaitable[object]]:
        @wraps(func)
        async def wrapper(self: object, *args: object, **kwargs: object) -> object:
            # Use dict_response decorator internally
            decorated_func = dict_response(raw_model, allow_none=allow_none)(func)
            raw_result = await decorated_func(self, *args, **kwargs)

            if raw_result is None:
                return None

            # Get mapper and call the specified method
            mapper = getattr(self, "_market_data_mapper", None)
            if not mapper:
                mapper = getattr(self, "_account_data_mapper", None)
            if not mapper:
                mapper = getattr(self, "_trading_data_mapper", None)

            if mapper and hasattr(mapper, mapper_method):
                mapper_func = getattr(mapper, mapper_method)
                # Handle both sync and async mapper methods
                import asyncio

                if asyncio.iscoroutinefunction(mapper_func):
                    return await mapper_func(raw_result)
                return mapper_func(raw_result)

            raise AttributeError(
                f"Mapper method '{mapper_method}' not found in any mapper "
                f"(_market_data_mapper, _account_data_mapper, _trading_data_mapper)",
            )

        return wrapper

    return decorator


def auto_typed(
    func: Callable[..., Awaitable[tuple[ParsedJsonResponse | None, int, object]]],
) -> Callable[..., Awaitable[object]]:
    """Ultra-smart decorator that auto-detects expected return type from type hints.

    This decorator analyzes the function's return type annotation and automatically
    applies the appropriate typed_api_method configuration.

    Supports:
    - Optional[Model] → typed_api_method(response_model=Model, allow_none=True)
    - list[Model] → typed_api_method(list_of=Model)
    - Optional[list[Model]] → typed_api_method(list_of=Model, allow_none=True)
    - Model → typed_api_method(response_model=Model)
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
        if len(args) == UNION_PAIR_COUNT and type(None) in args:
            # Optional type
            model_type = next(arg for arg in args if arg is not type(None))
            model_origin = get_origin(model_type)

            if model_origin is list:
                item_type = get_args(model_type)[0]
                return typed_api_method(list_of=item_type, allow_none=True)(func)
            return typed_api_method(response_model=model_type, allow_none=True)(func)

    elif origin is list:
        item_type = args[0]
        return typed_api_method(list_of=item_type)(func)

    # Single model type
    if isinstance(return_hint, type) and issubclass(return_hint, BaseModel):
        return typed_api_method(response_model=return_hint)(func)

    # Fallback - no decoration
    return func

"""Working Decorator Implementation for CyberDeltaEngine Services.

This demonstrates how to create decorators that work with the current architecture
while providing type safety and one-liner implementations.
"""

from functools import wraps
from typing import Any, Callable, TypeVar, cast
import inspect
from collections.abc import Awaitable, Mapping

from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


def backpack_api_endpoint(
    *,
    endpoint: str,
    method: str = "GET",
    response_handler: str,
    mapper: str | None = None,
    transform: Callable[[Any], Any] | None = None,
    is_signed: bool = True,
    endpoint_group: str = "private",
    request_weight: int = 1,
) -> Callable[[F], F]:
    """Decorator for Backpack API endpoints that creates true one-liners.
    
    This decorator handles the entire request/response pipeline:
    1. Builds request parameters from the decorated method
    2. Makes HTTP request with all necessary parameters
    3. Validates response with response handler
    4. Optionally transforms data
    5. Optionally maps to domain model
    
    Args:
        endpoint: API endpoint path
        method: HTTP method
        response_handler: Name of response handler method
        mapper: Optional name of mapper method
        transform: Optional transformation function before mapping
        is_signed: Whether request needs authentication
        endpoint_group: API endpoint group for rate limiting
        request_weight: Request weight for rate limiting
    
    Example:
        @backpack_api_endpoint(
            endpoint="/api/v1/capital",
            response_handler="handle_get_balances_response",
            mapper="map_balances",
            transform=lambda d: list(d.values())
        )
        async def get_account_balances(self) -> list[SpotBalance]:
            return self._request_builder.build_get_balances_params()
    """
    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # 1. Call decorated method to get request params
            params_or_data = await func(self, *args, **kwargs)
            
            # Determine if it's params or data based on method
            request_kwargs = {
                "method": method,
                "endpoint": endpoint,
                "is_signed": is_signed,
                "endpoint_group": endpoint_group,
                "request_weight": request_weight,
            }
            
            if method in ["GET", "DELETE"]:
                request_kwargs["params"] = params_or_data.model_dump() if hasattr(params_or_data, "model_dump") else params_or_data
            else:
                request_kwargs["data"] = params_or_data.model_dump() if hasattr(params_or_data, "model_dump") else params_or_data
            
            # 2. Make HTTP request
            raw_data, status_code, _ = await self._http_client_requester(**request_kwargs)
            
            # 3. Check for None response
            if raw_data is None:
                raise APIError(
                    message=f"No data received for {endpoint}, status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            
            # 4. Validate with response handler
            handler = getattr(self._response_handler, response_handler)
            validated_data = handler(raw_data)
            
            # 5. Optional transformation
            if transform:
                validated_data = transform(validated_data)
            
            # 6. Optional mapping to domain model
            if mapper and hasattr(self._mapper, mapper):
                mapper_func = getattr(self._mapper, mapper)
                return mapper_func(validated_data)
            
            return validated_data
            
        return cast(F, wrapper)
    return decorator


def typed_request(
    *,
    response_handler: str,
    mapper: str | None = None,
    transform: Callable[[Any], Any] | None = None,
) -> Callable[[F], F]:
    """Simpler decorator when endpoint details are in the method.
    
    This decorator extracts endpoint details from the method implementation
    and handles the response pipeline.
    
    Example:
        @typed_request(
            response_handler="handle_get_positions_response",
            mapper="map_positions"
        )
        async def get_positions(self) -> list[DerivativePosition]:
            return await self._make_request(
                method="GET",
                endpoint="/api/v1/positions",
                params={}
            )
    """
    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # Get the request details from the method
            request_coro = func(self, *args, **kwargs)
            
            # This assumes the method returns the result of _make_request
            # which we'll intercept and enhance
            raw_data, status_code, _ = await request_coro
            
            if raw_data is None:
                raise APIError(
                    message=f"No data received, status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            
            # Validate with response handler
            handler = getattr(self._response_handler, response_handler)
            validated_data = handler(raw_data)
            
            # Optional transformation
            if transform:
                validated_data = transform(validated_data)
            
            # Optional mapping
            if mapper and hasattr(self._mapper, mapper):
                mapper_func = getattr(self._mapper, mapper)
                return mapper_func(validated_data)
            
            return validated_data
            
        return cast(F, wrapper)
    return decorator


def validate_and_map(
    *,
    validator: type[Any] | Callable[[ParsedJsonResponse], Any],
    mapper: str | None = None,
) -> Callable[[F], F]:
    """Ultra-simple decorator for inline validation and mapping.
    
    Example:
        @validate_and_map(
            validator=BackpackRawBalanceDict,
            mapper="map_balances"
        )
        async def get_balances(self) -> list[SpotBalance]:
            raw, _, _ = await self._http_client_requester(
                method="GET",
                endpoint="/api/v1/capital",
                params={},
                is_signed=True
            )
            return raw
    """
    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # Get raw data from method
            raw_data = await func(self, *args, **kwargs)
            
            # Validate
            if inspect.isclass(validator):
                # It's a Pydantic model
                validated = validator.model_validate(raw_data)
            else:
                # It's a callable validator
                validated = validator(raw_data)
            
            # Optional mapping
            if mapper and hasattr(self._mapper, mapper):
                mapper_func = getattr(self._mapper, mapper)
                return mapper_func(validated)
            
            return validated
            
        return cast(F, wrapper)
    return decorator


# Example usage showing how these decorators would be used:
"""
class BackpackAccountService:
    # Current verbose pattern (20+ lines)
    async def get_account_balances_old(self) -> list[SpotBalance]:
        endpoint_path = "/api/v1/capital"
        params = self._request_builder.build_get_balances_params()
        
        raw_data, status_code, _ = await self._http_client_requester(
            method="GET",
            endpoint=endpoint_path,
            params=params.model_dump(),
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )
        
        if raw_data is None:
            raise APIError(...)
            
        balances_dict = self._response_handler.handle_get_balances_response(raw_data)
        return self._mapper.map_balances(list(balances_dict.values()))
    
    # New pattern with decorator (true one-liner!)
    @backpack_api_endpoint(
        endpoint="/api/v1/capital",
        response_handler="handle_get_balances_response",
        mapper="map_balances",
        transform=lambda d: list(d.values())
    )
    async def get_account_balances(self) -> list[SpotBalance]:
        return self._request_builder.build_get_balances_params()
    
    # Another example - positions
    @backpack_api_endpoint(
        endpoint="/api/v1/positions",
        response_handler="handle_get_positions_response",
        mapper="map_positions"
    )
    async def get_positions(self) -> list[DerivativePosition]:
        return {}  # No params needed
    
    # Example with custom params
    @backpack_api_endpoint(
        endpoint="/api/v1/orders",
        response_handler="handle_get_order_response",
        mapper="map_order"
    )
    async def get_order(self, order_id: str) -> Order:
        return {"orderId": order_id}
"""
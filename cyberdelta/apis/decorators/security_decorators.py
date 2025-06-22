"""Security-Enhanced Decorators - CyberDeltaEngine.

Security-focused decorators that enforce Pydantic validation, business logic constraints,
real-time monitoring, and audit trails to prevent validation bypass vulnerabilities.
"""

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from functools import wraps
from typing import TypeVar

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class TransformationError(Exception):
    """Critical security error in data transformation requiring immediate attention."""

    pass


def secure_transform(
    target_model: type[T],
    context: str | None = None,
    enable_monitoring: bool = True,
    enable_audit: bool = False,
    source_exchange: str | None = None,
) -> Callable[[Callable[..., Awaitable[dict[str, object]]]], Callable[..., Awaitable[T]]]:
    """Security-first decorator that enforces Pydantic validation and logging.

    This decorator replaces manual secure_transform utility calls with automatic
    decoration that prevents validation bypass vulnerabilities.

    Args:
        target_model: Pydantic model class for validation
        context: Description for security logging
        enable_monitoring: Enable security event logging
        enable_audit: Enable cryptographic audit trail
        source_exchange: Exchange name for security context

    Returns:
        Decorated function that returns validated Pydantic model instance

    Raises:
        TransformationError: If validation fails (indicates potential attack)

    Example:
        @secure_transform(
            target_model=SpotBalance,
            context="balance_transformation",
            enable_monitoring=True,
            source_exchange="backpack"
        )
        async def transform_raw_balance_to_internal(
            self, raw: BackpackRawBalance
        ) -> dict[str, object]:
            # Build data dict
            return {
                "asset": "BTC",
                "exchange": "backpack",
                "total_quantity": "100.0",
                "available_quantity": "50.0",
                "timestamp": datetime.now(UTC).isoformat(),
            }
            # Decorator automatically calls SpotBalance.model_validate()
    """

    def decorator(func: Callable[..., Awaitable[dict[str, object]]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(self: object, *args: object, **kwargs: object) -> T:
            # Call original mapper logic to build data dict
            transformation_data = await func(self, *args, **kwargs)

            # Build context for security logging
            method_context = context or f"{func.__name__}_{target_model.__name__}"
            exchange_context = source_exchange or getattr(self, "_exchange_name", "unknown")

            # Security logging
            if enable_monitoring:
                logger.info(
                    f"SECURITY: Secure transformation attempt: {method_context} "
                    f"from {exchange_context}"
                )

            try:
                # ENFORCE model_validate() - prevents bypass vulnerability
                validated_model = target_model.model_validate(transformation_data)

                # Success logging
                if enable_monitoring:
                    logger.debug(f"SECURITY: Validation successful: {method_context}")

                # Optional audit trail
                if enable_audit:
                    _create_audit_record(transformation_data, validated_model, method_context)

                return validated_model

            except ValidationError as e:
                # SECURITY ALERT - potential attack attempt
                logger.error(
                    f"SECURITY ALERT: Validation failed in {method_context} "
                    f"from {exchange_context}: {e}"
                )
                raise TransformationError(
                    f"Security validation failed for {target_model.__name__}: {e}"
                ) from e

        return wrapper

    return decorator


def _validate_financial_fields(data: dict[str, object], financial_fields: list[str]) -> None:
    """Helper to validate financial fields are non-negative."""
    for field in financial_fields:
        if field in data:
            try:
                value = float(str(data[field]))
                if value < 0:
                    raise ValueError(f"Financial field {field} cannot be negative: {value}")
            except (ValueError, TypeError) as e:
                if "cannot be negative" not in str(e):
                    raise ValueError(
                        f"Financial field {field} must be numeric: {data[field]}"
                    ) from e
                raise


def _validate_custom_constraints(
    data: dict[str, object], constraints: dict[str, dict[str, object]]
) -> None:
    """Helper to validate custom field constraints."""
    for field, rules in constraints.items():
        if field in data:
            value = data[field]

            if "min" in rules:
                min_val = rules["min"]
                if isinstance(min_val, int | float) and float(str(value)) < min_val:
                    raise ValueError(f"Field {field} below minimum {min_val}: {value}")

            if "max" in rules:
                max_val = rules["max"]
                if isinstance(max_val, int | float) and float(str(value)) > max_val:
                    raise ValueError(f"Field {field} exceeds maximum {max_val}: {value}")


def business_logic_validated(
    constraints: dict[str, dict[str, object]] | None = None,
    financial_fields: list[str] | None = None,
) -> Callable[
    [Callable[..., Awaitable[dict[str, object]]]], Callable[..., Awaitable[dict[str, object]]]
]:
    """Decorator that adds business logic constraints to prevent financial exploitation.

    This decorator validates business rules that Raw API models cannot enforce,
    such as non-negative financial values and reasonable bounds.

    Args:
        constraints: Field-specific validation rules with min/max values
        financial_fields: Fields that must be non-negative (price, quantity, balance, etc.)

    Returns:
        Decorated function with business logic validation applied

    Raises:
        ValueError: If business logic constraints are violated

    Example:
        @business_logic_validated(
            financial_fields=["total_quantity", "available_quantity"],
            constraints={
                "total_quantity": {"min": 0, "max": 1000000000},
                "available_quantity": {"min": 0}
            }
        )
        async def transform_balance(self, raw: BackpackRawBalance) -> Dict[str, Any]:
            return {"total_quantity": "100.0", "available_quantity": "50.0"}
    """

    def decorator(
        func: Callable[..., Awaitable[dict[str, object]]],
    ) -> Callable[..., Awaitable[dict[str, object]]]:
        @wraps(func)
        async def wrapper(self: object, *args: object, **kwargs: object) -> dict[str, object]:
            # Get transformation data
            data = await func(self, *args, **kwargs)

            # Apply business logic validation using helper functions
            if financial_fields:
                _validate_financial_fields(data, financial_fields)

            if constraints:
                _validate_custom_constraints(data, constraints)

            return data

        return wrapper

    return decorator


def _check_negative_values(data: dict[str, object]) -> list[str]:
    """Helper to check for negative values in financial fields."""
    anomalies: list[str] = []
    # Extended list of financial field patterns to check
    financial_patterns = [
        "price",
        "quantity",
        "balance",
        "total",
        "available",
        "amount",
        "volume",
        "fee",
        "commission",
        "value",
        "cost",
        "profit",
        "loss",
        "margin",
        "collateral",
    ]

    for field, value in data.items():
        if isinstance(value, str | int | float):
            try:
                float_val = float(str(value))
                # Check if field name contains any financial pattern
                if float_val < 0 and any(
                    pattern in field.lower() for pattern in financial_patterns
                ):
                    anomalies.append(f"negative_value_in_{field}")
            except (ValueError, TypeError):
                pass
    return anomalies


def _check_oversized_data(data: dict[str, object], max_field_count: int) -> list[str]:
    """Helper to check for oversized data structures."""
    anomalies: list[str] = []

    if len(data) > max_field_count:
        anomalies.append(f"oversized_structure_{len(data)}_fields")

    for field, value in data.items():
        if isinstance(value, str) and len(value) > 1000:
            anomalies.append(f"oversized_string_in_{field}")

    return anomalies


def _validate_http_response(
    raw_response_content: object, raw_model: type[BaseModel], status_code: int
) -> BaseModel:
    """Helper to validate HTTP response as raw model."""
    if raw_response_content is None:
        raise APIError(
            message="No response content received",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    if not isinstance(raw_response_content, dict):
        raise APIError(
            message=(
                f"Expected dict for {raw_model.__name__}, got {type(raw_response_content).__name__}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        )

    try:
        return raw_model.model_validate(raw_response_content)
    except ValidationError as e:
        raise APIError(
            message=f"Raw model validation failed for {raw_model.__name__}: {e}",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=status_code,
        ) from e


def _find_mapper(self: object) -> object:
    """Helper to find appropriate mapper from service instance."""
    mapper = getattr(self, "_market_data_mapper", None)
    if not mapper:
        mapper = getattr(self, "_account_data_mapper", None)
    if not mapper:
        mapper = getattr(self, "_trading_data_mapper", None)
    return mapper


def security_monitored(
    alert_on_negative: bool = True,
    alert_on_oversized: bool = True,
    anomaly_detection: bool = False,
    max_field_count: int = 100,
) -> Callable[
    [Callable[..., Awaitable[dict[str, object]]]], Callable[..., Awaitable[dict[str, object]]]
]:
    """Decorator for real-time security monitoring and anomaly detection.

    This decorator monitors transformation data for suspicious patterns that
    may indicate attack attempts or data corruption.

    Args:
        alert_on_negative: Alert on negative values in financial fields
        alert_on_oversized: Alert on oversized data structures or strings
        anomaly_detection: Enable advanced anomaly pattern detection
        max_field_count: Maximum number of fields allowed in data structure

    Returns:
        Decorated function with security monitoring

    Example:
        @security_monitored(
            alert_on_negative=True,
            alert_on_oversized=True,
            max_field_count=50
        )
        async def transform_data(self, raw) -> Dict[str, Any]:
            return {"price": "100.0", "quantity": "10.0"}
    """

    def decorator(
        func: Callable[..., Awaitable[dict[str, object]]],
    ) -> Callable[..., Awaitable[dict[str, object]]]:
        @wraps(func)
        async def wrapper(self: object, *args: object, **kwargs: object) -> dict[str, object]:
            data = await func(self, *args, **kwargs)

            # Collect anomalies using helper functions
            anomalies: list[str] = []

            if alert_on_negative:
                anomalies.extend(_check_negative_values(data))

            if alert_on_oversized:
                anomalies.extend(_check_oversized_data(data, max_field_count))

            # Log anomalies
            if anomalies:
                logger.warning(f"SECURITY ANOMALY: {anomalies} in {func.__name__}")

            return data

        return wrapper

    return decorator


def secure_mapped_response(
    raw_model: type[BaseModel],
    target_model: type[BaseModel],
    mapper_method: str,
    enable_security_monitoring: bool = True,
    enable_audit_trail: bool = False,
    business_constraints: dict[str, dict[str, object]] | None = None,
    financial_fields: list[str] | None = None,
) -> Callable[
    [Callable[..., Awaitable[tuple[object, int, object]]]],
    Callable[..., Awaitable[BaseModel | None]],
]:
    """Ultimate decorator combining HTTP type safety + security validation + mapping.

    This decorator handles the complete secure flow:
    1. HTTP request with typed response validation
    2. Security monitoring and business logic validation
    3. Secure transformation to target model
    4. Automatic mapper method invocation

    Args:
        raw_model: Expected raw Pydantic model from HTTP response
        target_model: Target domain model for transformation
        mapper_method: Name of mapper method to call
        enable_security_monitoring: Enable security monitoring and validation
        enable_audit_trail: Enable cryptographic audit trail
        business_constraints: Business logic constraints to apply
        financial_fields: Fields that must be non-negative (works with constraints)

    Returns:
        Decorated function that returns validated target model

    Example:
        @secure_mapped_response(
            raw_model=BackpackRawTicker,
            target_model=Ticker,
            mapper_method='transform_raw_ticker_to_internal',
            enable_security_monitoring=True,
            business_constraints={"last_price": {"min": 0}},
            financial_fields=["last_price", "volume"]
        )
        async def get_ticker_secure(self, symbol: str) -> Ticker | None:
            return await self._http_client_requester(
                method="GET", endpoint="/api/v1/ticker", params={"symbol": symbol}
            )
    """

    def decorator(
        func: Callable[..., Awaitable[tuple[object, int, object]]],
    ) -> Callable[..., Awaitable[BaseModel | None]]:
        @wraps(func)
        async def wrapper(self: object, *args: object, **kwargs: object) -> BaseModel | None:
            # Step 1: Execute HTTP request
            response_tuple = await func(self, *args, **kwargs)
            raw_response_content, status_code, _headers = response_tuple

            # Step 2: Validate HTTP response as raw model
            if raw_response_content is None:
                return None

            validated_raw = _validate_http_response(raw_response_content, raw_model, status_code)

            # Step 3: Get mapper and apply security validation
            mapper = _find_mapper(self)
            if not mapper or not hasattr(mapper, mapper_method):
                raise AttributeError(f"Mapper method {mapper_method} not found")

            # Step 4: Execute mapper with security decorators
            mapper_func = getattr(mapper, mapper_method)

            # Apply security validation if enabled
            if enable_security_monitoring:
                # Apply business logic validation to mapper function if constraints
                # or financial fields exist
                if business_constraints or financial_fields:
                    decorated_mapper = business_logic_validated(
                        constraints=business_constraints, financial_fields=financial_fields
                    )

                    # Create a wrapper that returns dict for business logic validation
                    async def dict_wrapper(
                        *map_args: object, **map_kwargs: object
                    ) -> dict[str, object]:
                        result = await mapper_func(*map_args, **map_kwargs)
                        return result.model_dump() if hasattr(result, "model_dump") else {}

                    validated_dict_func = decorated_mapper(dict_wrapper)
                    # Get the dict result and convert back to model
                    dict_result = await validated_dict_func(validated_raw)
                    return target_model.model_validate(dict_result)
                else:
                    result = await mapper_func(validated_raw)
                    if isinstance(result, BaseModel):
                        return result
                    raise TypeError(f"Mapper returned non-BaseModel type: {type(result)}")
            else:
                result = await mapper_func(validated_raw)
                if isinstance(result, BaseModel):
                    return result
                raise TypeError(f"Mapper returned non-BaseModel type: {type(result)}")

        return wrapper

    return decorator


def _create_audit_record(
    source_data: dict[str, object], target_model: BaseModel, context: str
) -> None:
    """Create cryptographic audit record for security monitoring."""
    # Use consistent JSON serialization for hashing
    import json

    source_json = json.dumps(source_data, sort_keys=True, default=str)
    source_hash = hashlib.sha256(source_json.encode()).hexdigest()
    target_hash = hashlib.sha256(target_model.model_dump_json().encode()).hexdigest()

    audit_record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "context": context,
        "source_hash": source_hash,
        "target_hash": target_hash,
        "model_type": target_model.__class__.__name__,
        "validation_result": "success",
    }

    logger.info(f"AUDIT: {audit_record}")


def retry_on_failure(
    max_attempts: int = 3,
    backoff_seconds: float = 1.0,
    backoff_multiplier: float = 2.0,
    retry_on: tuple[type[Exception], ...] | None = None,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorator for retrying failed operations with exponential backoff.

    This decorator provides resilience for transient failures in API calls
    or transformations, with configurable retry logic.

    Args:
        max_attempts: Maximum number of attempts (including initial)
        backoff_seconds: Initial backoff time in seconds
        backoff_multiplier: Multiplier for exponential backoff
        retry_on: Tuple of exception types to retry on (None = all exceptions)

    Returns:
        Decorated function with retry logic

    Example:
        @retry_on_failure(max_attempts=3, retry_on=(APIError, ConnectionError))
        @secure_transform(SpotBalance)
        async def transform_balance(self, raw) -> dict[str, object]:
            return {"asset": "BTC", "total_quantity": "100.0"}
    """

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: object, **kwargs: object) -> T:
            last_exception: Exception | None = None
            current_backoff = backoff_seconds

            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    # Check if we should retry this exception
                    if retry_on is not None and not isinstance(e, retry_on):
                        raise

                    # Don't retry on last attempt
                    if attempt == max_attempts - 1:
                        logger.error(
                            f"Max retry attempts ({max_attempts}) exceeded for {func.__name__}: {e}"
                        )
                        raise

                    # Log retry attempt
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_attempts} failed for {func.__name__}: {e}. "
                        f"Retrying in {current_backoff:.1f}s..."
                    )

                    # Wait before retry
                    await asyncio.sleep(current_backoff)
                    current_backoff *= backoff_multiplier

            # Should never reach here, but handle gracefully
            if last_exception:
                raise last_exception
            raise RuntimeError(f"Unexpected retry logic error in {func.__name__}")

        return wrapper

    return decorator


def rate_limited(
    calls_per_minute: int = 60, burst_size: int | None = None
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorator for rate limiting API calls.

    This decorator implements a token bucket algorithm to prevent
    overwhelming APIs with too many requests.

    Args:
        calls_per_minute: Maximum sustained calls per minute
        burst_size: Maximum burst size (defaults to calls_per_minute / 10)

    Returns:
        Decorated function with rate limiting

    Example:
        @rate_limited(calls_per_minute=120, burst_size=20)
        @dict_response(BackpackRawTicker)
        async def get_ticker_raw(self, symbol: str) -> BackpackRawTicker:
            return await self._http_client_requester(...)
    """
    # Calculate rate and burst
    rate_per_second = calls_per_minute / 60.0
    actual_burst = burst_size or max(1, int(calls_per_minute / 10))

    # Shared state for rate limiting
    tokens: float = float(actual_burst)
    last_update = datetime.now(UTC)
    lock = asyncio.Lock()

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: object, **kwargs: object) -> T:
            nonlocal tokens, last_update

            async with lock:
                # Update tokens based on time passed
                now = datetime.now(UTC)
                elapsed = (now - last_update).total_seconds()
                tokens = min(actual_burst, tokens + elapsed * rate_per_second)
                last_update = now

                # Wait if no tokens available
                if tokens < 1:
                    wait_time = (1 - tokens) / rate_per_second
                    logger.debug(
                        f"Rate limit reached for {func.__name__}, waiting {wait_time:.1f}s"
                    )
                    await asyncio.sleep(wait_time)
                    tokens = 1

                # Consume token
                tokens -= 1

            # Execute function
            return await func(*args, **kwargs)

        return wrapper

    return decorator

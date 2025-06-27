"""Security-Enhanced Decorators - CyberDeltaEngine.

Security-focused decorators that enforce Pydantic validation, business logic constraints,
real-time monitoring, and audit trails to prevent validation bypass vulnerabilities.
"""

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from functools import wraps
from typing import Any, ParamSpec, TypeVar, cast, overload

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.parsing import parse_decimal_value


# Security validation constants
MAX_STRING_LENGTH_SECURITY = 1000  # Maximum allowed string length for security checks

logger = get_logger(__name__)

T = TypeVar("T", bound=BaseModel)
P = ParamSpec("P")
R = TypeVar("R")


class TransformationError(Exception):
    """Critical security error in data transformation requiring immediate attention."""


class SecureTransform[T: BaseModel]:
    """Type-safe security decorator that enforces Pydantic validation.

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
    ) -> None:
        """Initialize SecureTransform decorator."""
        self.target_model = target_model
        self.context = context
        self.enable_monitoring = enable_monitoring
        self.enable_audit = enable_audit
        self.source_exchange = source_exchange

    @overload
    def __call__(self, func: Callable[P, dict[str, object]]) -> Callable[P, T]: ...

    @overload
    def __call__(
        self,
        func: Callable[P, Awaitable[dict[str, object]]],
    ) -> Callable[P, Awaitable[T]]: ...

    def __call__(
        self,
        func: Callable[P, dict[str, object]] | Callable[P, Awaitable[dict[str, object]]],
    ) -> Callable[P, T] | Callable[P, Awaitable[T]]:
        """Support both sync and async functions."""
        if asyncio.iscoroutinefunction(func):
            async_func = cast("Callable[P, Awaitable[dict[str, object]]]", func)

            @wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                transformation_data = await async_func(*args, **kwargs)
                return self._transform_data(transformation_data, func.__name__)

            return cast("Callable[P, Awaitable[T]]", async_wrapper)
        sync_func = cast("Callable[P, dict[str, object]]", func)

        @wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            transformation_data = sync_func(*args, **kwargs)
            return self._transform_data(transformation_data, func.__name__)

        return cast("Callable[P, T]", sync_wrapper)

    def _transform_data(self, data: dict[str, object], func_name: str) -> T:
        """Core transformation logic matching current secure_transform utility."""
        method_context = self.context or f"{func_name}_{self.target_model.__name__}"
        exchange_context = self.source_exchange or "unknown"

        # Security monitoring
        if self.enable_monitoring:
            logger.info(
                f"SECURITY: Secure transformation attempt: "
                f"{method_context} from {exchange_context}",
            )

        try:
            # ENFORCE model_validate() - prevents bypass vulnerability
            validated_model = self.target_model.model_validate(data)

            # Success logging
            if self.enable_monitoring:
                logger.debug(
                    "security_validation_successful",
                    action="validate",
                    method_context=method_context,
                    message=f"SECURITY: Validation successful: {method_context}",
                )

            # Audit trail
            if self.enable_audit:
                _create_audit_record(
                    source_data=data,
                    target_model=validated_model,
                    context=method_context,
                )

            return validated_model

        except ValidationError as e:
            # Maintain existing error pattern
            logger.error(
                f"SECURITY ALERT: Validation failed in {method_context} "
                f"from {exchange_context}: {e}",
            )
            raise TransformationError(
                f"Security validation failed for {self.target_model.__name__}: {e}",
            ) from e


def _validate_financial_fields(data: dict[str, Any], financial_fields: list[str]) -> None:
    """Helper to validate financial fields are non-negative."""
    for field in financial_fields:
        if field in data:
            try:
                raw_value = data[field]
                if not isinstance(raw_value, str | int | float | Decimal):
                    raise ValueError(f"Field {field} must be numeric, got {type(raw_value)}")
                value = parse_decimal_value(raw_value, allow_none=False, field_name=field)
                if value is not None and value < 0:
                    raise ValueError(f"Financial field {field} cannot be negative: {value}")
            except (ValueError, TypeError) as e:
                if "cannot be negative" not in str(e):
                    raise ValueError(
                        f"Financial field {field} must be numeric: {data[field]}",
                    ) from e
                raise


def _validate_custom_constraints(
    data: dict[str, Any],
    constraints: dict[str, dict[str, Any]],
) -> None:
    """Helper to validate custom field constraints."""
    for field, rules in constraints.items():
        if field in data:
            raw_value = data[field]
            if not isinstance(raw_value, str | int | float | Decimal | None):
                continue  # Skip non-numeric values
            value = parse_decimal_value(raw_value, allow_none=True, field_name=field)
            if value is not None:
                if "min" in rules and value < Decimal(str(rules["min"])):
                    raise ValueError(f"Field {field} below minimum {rules['min']}: {value}")
                if "max" in rules and value > Decimal(str(rules["max"])):
                    raise ValueError(f"Field {field} exceeds maximum {rules['max']}: {value}")


class BusinessLogicValidator[**P, R]:
    """Business logic validation that preserves types.

    Integrates with existing parsing utilities and validation patterns.
    """

    def __init__(
        self,
        constraints: dict[str, dict[str, Any]] | None = None,
        financial_fields: list[str] | None = None,
    ) -> None:
        """Initialize BusinessLogicValidator."""
        self.constraints = constraints or {}
        self.financial_fields = financial_fields or []

    @overload
    def __call__(self, func: Callable[P, R]) -> Callable[P, R]: ...

    @overload
    def __call__(self, func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]: ...

    def __call__(
        self,
        func: Callable[P, R] | Callable[P, Awaitable[R]],
    ) -> Callable[P, R] | Callable[P, Awaitable[R]]:
        """Validate while preserving function signature."""
        if asyncio.iscoroutinefunction(func):
            async_func = cast("Callable[P, Awaitable[R]]", func)

            @wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                result: R = await async_func(*args, **kwargs)

                # Perform validation only on dict results
                self._validate_if_dict(result)

                return result

            return cast("Callable[P, Awaitable[R]]", async_wrapper)
        sync_func = cast("Callable[P, R]", func)

        @wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            result: R = sync_func(*args, **kwargs)

            # Perform validation only on dict results
            self._validate_if_dict(result)

            return result

        return cast("Callable[P, R]", sync_wrapper)

    def _validate_if_dict(self, result: object) -> None:
        """Validate result if it's a dict and not a BaseModel."""
        if isinstance(result, dict):
            # Cast to satisfy type checker - we know it's a dict
            dict_result = cast("dict[str, Any]", result)
            try:
                # BaseModel has model_validate, plain dicts don't
                if not hasattr(dict_result, "model_validate"):
                    if self.financial_fields:
                        _validate_financial_fields(dict_result, self.financial_fields)
                    if self.constraints:
                        _validate_custom_constraints(dict_result, self.constraints)
            except AttributeError:
                # If checking attributes fails, skip validation
                pass


def _check_negative_values(data: dict[str, Any]) -> list[str]:
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
        if any(pattern in field.lower() for pattern in financial_patterns):
            try:
                # Cast to appropriate type for parse_decimal_value
                if isinstance(value, str | int | float | Decimal):
                    decimal_value = parse_decimal_value(value, allow_none=True)
                    if decimal_value is not None and decimal_value < 0:
                        anomalies.append(f"negative_value_in_{field}")
            except (ValueError, TypeError, AttributeError):
                # Skip fields that cannot be parsed as decimal
                continue
    return anomalies


def _check_oversized_data(data: dict[str, Any], max_field_count: int) -> list[str]:
    """Helper to check for oversized data structures."""
    anomalies: list[str] = []

    if len(data) > max_field_count:
        anomalies.append(f"oversized_structure_{len(data)}_fields")

    for field, value in data.items():
        if isinstance(value, str) and len(value) > MAX_STRING_LENGTH_SECURITY:
            anomalies.append(f"oversized_string_in_{field}")

    return anomalies


def _validate_http_response(
    raw_response_content: object,
    raw_model: type[BaseModel],
    status_code: int,
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


class SecurityMonitor[**P, R]:
    """Security monitoring decorator that integrates with existing logging patterns."""

    def __init__(
        self,
        alert_on_negative: bool = True,
        alert_on_oversized: bool = True,
        anomaly_detection: bool = False,
        max_field_count: int = 100,
    ) -> None:
        """Initialize SecurityMonitor."""
        self.alert_on_negative = alert_on_negative
        self.alert_on_oversized = alert_on_oversized
        self.anomaly_detection = anomaly_detection
        self.max_field_count = max_field_count
        self.sensitive_fields = [
            "price",
            "quantity",
            "balance",
            "total",
            "available",
            "amount",
            "equity",
            "margin",
            "collateral",
            "pnl",
        ]

    @overload
    def __call__(self, func: Callable[P, R]) -> Callable[P, R]: ...

    @overload
    def __call__(self, func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]: ...

    def __call__(
        self,
        func: Callable[P, R] | Callable[P, Awaitable[R]],
    ) -> Callable[P, R] | Callable[P, Awaitable[R]]:
        """Monitor while preserving types."""
        if asyncio.iscoroutinefunction(func):
            async_func = cast("Callable[P, Awaitable[R]]", func)

            @wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                result = await async_func(*args, **kwargs)
                self._monitor_result(result, func.__name__)
                return result

            return cast("Callable[P, Awaitable[R]]", async_wrapper)
        sync_func = cast("Callable[P, R]", func)

        @wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            result = sync_func(*args, **kwargs)
            self._monitor_result(result, func.__name__)
            return result

        return cast("Callable[P, R]", sync_wrapper)

    def _monitor_result(self, result: object, func_name: str) -> None:
        """Monitor for security anomalies."""
        if not isinstance(result, dict):
            return

        anomalies: list[str] = []

        if self.alert_on_negative:
            anomalies.extend(_check_negative_values(cast("dict[str, Any]", result)))

        if self.alert_on_oversized:
            anomalies.extend(
                _check_oversized_data(cast("dict[str, Any]", result), self.max_field_count),
            )

        if anomalies:
            logger.warning(
                "security_anomaly_detected",
                action="detect_anomaly",
                anomalies=anomalies,
                function_name=func_name,
                message=f"SECURITY ANOMALY: {anomalies} in {func_name}",
            )


def secure_mapped_response(
    raw_model: type[BaseModel],
    target_model: type[BaseModel],
    mapper_method: str,
    enable_security_monitoring: bool = True,
    enable_audit_trail: bool = False,
    business_constraints: dict[str, dict[str, Any]] | None = None,
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
                        constraints=business_constraints,
                        financial_fields=financial_fields,
                    )

                    # Create a wrapper that returns dict for business logic validation
                    async def dict_wrapper(
                        *map_args: object,
                        **map_kwargs: object,
                    ) -> dict[str, object]:
                        result = await mapper_func(*map_args, **map_kwargs)
                        return result.model_dump() if hasattr(result, "model_dump") else {}

                    validated_dict_func = decorated_mapper(dict_wrapper)
                    # Get the dict result and convert back to model
                    dict_result = await validated_dict_func(validated_raw)
                    return target_model.model_validate(dict_result)
                result = await mapper_func(validated_raw)
                if isinstance(result, BaseModel):
                    return result
                raise TypeError(f"Mapper returned non-BaseModel type: {type(result)}")
            result = await mapper_func(validated_raw)
            if isinstance(result, BaseModel):
                return result
            raise TypeError(f"Mapper returned non-BaseModel type: {type(result)}")

        return wrapper

    return decorator


def _create_audit_record(
    source_data: dict[str, Any],
    target_model: BaseModel,
    context: str,
) -> None:
    """Create cryptographic audit record for security monitoring."""
    source_hash = hashlib.sha256(str(source_data).encode()).hexdigest()
    target_hash = hashlib.sha256(target_model.model_dump_json().encode()).hexdigest()

    audit_record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "context": context,
        "source_hash": source_hash,
        "target_hash": target_hash,
        "validation_result": "success",
    }

    logger.info(
        "security_audit_trail",
        action="audit",
        **audit_record,
        message=f"AUDIT: {audit_record}",
    )


class SecureTransformStack[T: BaseModel]:
    """Composite decorator matching current decorator stacking patterns.

    Provides the same functionality as stacking multiple decorators but with
    better type safety and cleaner syntax.
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
    ) -> None:
        """Initialize SecureTransformStack."""
        self.target_model = target_model
        self.financial_fields = financial_fields
        self.constraints = constraints
        self.context = context
        self.enable_monitoring = enable_monitoring
        self.enable_audit = enable_audit
        self.source_exchange = source_exchange

    def __call__(self, func: Callable[..., dict[str, object]]) -> Callable[..., T]:
        """Apply security stack in correct order."""
        # Order matches current decorator stacking patterns

        # Type-safe decorator application
        # 1. Security monitoring (outermost)
        monitor = SecurityMonitor[..., dict[str, object]]()
        monitored_func = monitor(func)

        # 2. Business logic validation
        if self.financial_fields or self.constraints:
            validator = BusinessLogicValidator[..., dict[str, object]](
                financial_fields=self.financial_fields,
                constraints=self.constraints,
            )
            validated_func = validator(monitored_func)
        else:
            validated_func = monitored_func

        # 3. Secure transformation (innermost)
        transformer = SecureTransform(
            target_model=self.target_model,
            context=self.context,
            enable_monitoring=self.enable_monitoring,
            enable_audit=self.enable_audit,
            source_exchange=self.source_exchange,
        )

        # Final transformation
        # The transformer properly handles both sync and async functions
        # and returns the correct type based on the input
        return transformer(validated_func)


# Export legacy function names for minimal disruption during migration
def secure_transform[T: BaseModel](
    target_model: type[T],
    context: str | None = None,
    enable_monitoring: bool = True,
    enable_audit: bool = False,
    source_exchange: str | None = None,
) -> SecureTransform[T]:
    """Legacy function interface - use SecureTransform decorator instead."""
    return SecureTransform(
        target_model=target_model,
        context=context,
        enable_monitoring=enable_monitoring,
        enable_audit=enable_audit,
        source_exchange=source_exchange,
    )


def business_logic_validated(
    constraints: dict[str, dict[str, Any]] | None = None,
    financial_fields: list[str] | None = None,
) -> BusinessLogicValidator[..., Any]:
    """Legacy function interface - use BusinessLogicValidator decorator instead."""
    return BusinessLogicValidator(constraints=constraints, financial_fields=financial_fields)


def security_monitored(
    alert_on_negative: bool = True,
    alert_on_oversized: bool = True,
    anomaly_detection: bool = False,
    max_field_count: int = 100,
) -> SecurityMonitor[..., Any]:
    """Legacy function interface - use SecurityMonitor decorator instead."""
    return SecurityMonitor(
        alert_on_negative=alert_on_negative,
        alert_on_oversized=alert_on_oversized,
        anomaly_detection=anomaly_detection,
        max_field_count=max_field_count,
    )

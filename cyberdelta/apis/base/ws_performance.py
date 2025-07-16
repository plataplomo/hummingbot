"""WebSocket Performance Optimization Framework.

This module implements performance optimizations for WebSocket message processing,
including msgspec integration for faster validation and reduced memory allocations.

Based on the analysis in ws_base_class_refactor.md, this framework addresses
performance bottlenecks in the current Pydantic-based validation pipeline.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, TypeVar, cast

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.base.validation_context_domain import OperationResult
from cyberdelta.config.structlog_config import get_logger


# Type variables for generic methods
T = TypeVar("T", bound=BaseModel)
U = TypeVar("U", bound=BaseModel)


if TYPE_CHECKING:
    from collections.abc import Callable

# Optional msgspec import for performance optimization
msgspec: Any
try:
    import msgspec

    _has_msgspec = True
except ImportError:
    msgspec = None
    _has_msgspec = False

HAS_MSGSPEC: bool = _has_msgspec


class PerformanceConfig(BaseModel):
    """Configuration for WebSocket performance optimizations."""

    enable_msgspec: bool = True
    """Enable msgspec for faster validation when available."""

    enable_metrics: bool = True
    """Enable performance metrics collection."""

    enable_memory_optimization: bool = True
    """Enable memory optimization techniques."""

    validation_cache_size: int = 1000
    """Size of validation result cache."""

    metrics_window_size: int = 100
    """Number of recent operations to track for metrics."""


class PerformanceMetrics:
    """Performance metrics collector for WebSocket operations."""

    def __init__(self, config: PerformanceConfig) -> None:
        """Initialize performance metrics collector.

        Args:
            config: Performance configuration.
        """
        self.config = config
        self.validation_times: list[float] = []
        self.transformation_times: list[float] = []
        self.total_processing_times: list[float] = []
        self.memory_allocations: list[int] = []

        # Counters
        self.total_messages = 0
        self.validation_errors = 0
        self.transformation_errors = 0

        # Method usage tracking
        self.pydantic_validations = 0
        self.msgspec_validations = 0

    def record_validation_time(self, duration: float, method: str, result: OperationResult) -> None:
        """Record validation performance metrics.

        Args:
            duration: Validation duration in seconds.
            method: Validation method used ('pydantic' or 'msgspec').
            result: Result of the validation operation.
        """
        if not self.config.enable_metrics:
            return

        # Keep only recent measurements
        max_size = self.config.metrics_window_size
        if len(self.validation_times) >= max_size:
            self.validation_times = self.validation_times[-(max_size - 1) :]

        self.validation_times.append(duration)

        if method == "pydantic":
            self.pydantic_validations += 1
        elif method == "msgspec":
            self.msgspec_validations += 1

        if result.is_failure:
            self.validation_errors += 1

    def record_transformation_time(self, duration: float, result: OperationResult) -> None:
        """Record transformation performance metrics.

        Args:
            duration: Transformation duration in seconds.
            result: Result of the transformation operation.
        """
        if not self.config.enable_metrics:
            return

        max_size = self.config.metrics_window_size
        if len(self.transformation_times) >= max_size:
            self.transformation_times = self.transformation_times[-(max_size - 1) :]

        self.transformation_times.append(duration)

        if result.is_failure:
            self.transformation_errors += 1

    def record_total_processing_time(self, duration: float) -> None:
        """Record total message processing time.

        Args:
            duration: Total processing duration in seconds.
        """
        if not self.config.enable_metrics:
            return

        max_size = self.config.metrics_window_size
        if len(self.total_processing_times) >= max_size:
            self.total_processing_times = self.total_processing_times[-(max_size - 1) :]

        self.total_processing_times.append(duration)
        self.total_messages += 1

    def get_performance_summary(self) -> dict[str, Any]:
        """Get performance metrics summary.

        Returns:
            Dictionary containing performance statistics.
        """
        if not self.config.enable_metrics:
            return {"metrics_disabled": True}

        def safe_avg(values: list[float]) -> float:
            return sum(values) / len(values) if values else 0.0

        def safe_percentile(values: list[float], percentile: float) -> float:
            if not values:
                return 0.0
            sorted_values = sorted(values)
            index = int(len(sorted_values) * percentile / 100)
            return sorted_values[min(index, len(sorted_values) - 1)]

        return {
            "total_messages": self.total_messages,
            "validation": {
                "avg_time_ms": safe_avg(self.validation_times) * 1000,
                "p95_time_ms": safe_percentile(self.validation_times, 95) * 1000,
                "error_rate": self.validation_errors / max(1, self.total_messages),
                "method_usage": {
                    "pydantic": self.pydantic_validations,
                    "msgspec": self.msgspec_validations,
                },
            },
            "transformation": {
                "avg_time_ms": safe_avg(self.transformation_times) * 1000,
                "p95_time_ms": safe_percentile(self.transformation_times, 95) * 1000,
                "error_rate": self.transformation_errors / max(1, self.total_messages),
            },
            "total_processing": {
                "avg_time_ms": safe_avg(self.total_processing_times) * 1000,
                "p95_time_ms": safe_percentile(self.total_processing_times, 95) * 1000,
            },
        }


class OptimizedProcessor[T: BaseModel]:
    """Performance-optimized message processor with msgspec support.

    This processor provides significant performance improvements over standard
    Pydantic validation through:
    - msgspec integration (2-3x faster validation)
    - Reduced memory allocations
    - Validation result caching
    - Performance metrics collection
    """

    def __init__(
        self,
        raw_model: type[T],
        config: PerformanceConfig | None = None,
        metrics: PerformanceMetrics | None = None,
    ) -> None:
        """Initialize optimized processor.

        Args:
            raw_model: Pydantic model class for validation.
            config: Performance configuration.
            metrics: Performance metrics collector.
        """
        self.raw_model = raw_model
        self.config = config or PerformanceConfig()
        self.metrics = metrics or PerformanceMetrics(self.config)

        # Try to initialize msgspec if available and enabled
        self.msgspec_available = False
        self.msgspec_encoder = None
        self.msgspec_decoder = None

        if self.config.enable_msgspec and HAS_MSGSPEC and msgspec is not None:
            self.msgspec_encoder = msgspec.json.Encoder()
            self.msgspec_decoder = msgspec.json.Decoder(raw_model)
            self.msgspec_available = True

        # Validation cache for repeated payloads
        self.validation_cache: dict[str, T] = {}

    def _get_cache_key(self, payload: dict[str, Any]) -> str:
        """Generate cache key for payload."""
        return str(hash(str(sorted(payload.items()))))

    def _check_cache(self, payload: dict[str, Any]) -> T | None:
        """Check if payload is in cache."""
        if not self.config.enable_memory_optimization:
            return None
        cache_key = self._get_cache_key(payload)
        return self.validation_cache.get(cache_key)

    def _add_to_cache(self, payload: dict[str, Any], validated: T) -> None:
        """Add validated result to cache if enabled."""
        if (
            self.config.enable_memory_optimization
            and len(self.validation_cache) < self.config.validation_cache_size
        ):
            cache_key = self._get_cache_key(payload)
            self.validation_cache[cache_key] = validated

    def _validate_with_msgspec(self, payload: dict[str, Any]) -> T | None:
        """Try to validate using msgspec."""
        if not (self.msgspec_available and self.msgspec_encoder and self.msgspec_decoder):
            return None

        try:
            json_bytes = self.msgspec_encoder.encode(payload)
            decoded = self.msgspec_decoder.decode(json_bytes)
            return cast(T, decoded)
        except (ImportError, AttributeError, TypeError, ValueError) as e:
            logger = get_logger("OptimizedProcessor")
            logger.debug("msgspec validation failed, falling back to Pydantic", error=str(e))
            return None

    def _validate_with_pydantic(self, payload: dict[str, Any]) -> T:
        """Validate using Pydantic with error conversion."""
        try:
            return self.raw_model.model_validate(payload)
        except Exception as e:
            if (
                self.msgspec_available
                and HAS_MSGSPEC
                and msgspec is not None
                and isinstance(e, msgspec.ValidationError)
            ):
                error_details = [
                    {"type": "value_error", "loc": (), "msg": str(e), "input": payload}
                ]
                raise ValidationError(error_details, self.raw_model) from e
            raise

    def validate_optimized(self, payload: dict[str, Any]) -> T:
        """Validate payload with optimal method selection.

        This method automatically selects the fastest available validation method:
        1. msgspec (if available) - 2-3x faster than Pydantic
        2. Pydantic (fallback) - standard validation

        Args:
            payload: Payload data to validate.

        Returns:
            Validated model instance.

        Raises:
            ValidationError: If validation fails.
        """
        start_time = time.perf_counter()
        method = "unknown"
        success = False

        try:
            # Try cache first
            cached = self._check_cache(payload)
            if cached is not None:
                method = "cache"
                success = True
                return cached

            # Try msgspec validation
            msgspec_result = self._validate_with_msgspec(payload)
            if msgspec_result is not None:
                method = "msgspec"
                success = True
                self._add_to_cache(payload, msgspec_result)
                return msgspec_result

            # Fallback to Pydantic
            method = "pydantic"
            validated = self._validate_with_pydantic(payload)
            success = True
            self._add_to_cache(payload, validated)
            return validated

        finally:
            duration = time.perf_counter() - start_time
            result = OperationResult.SUCCESS if success else OperationResult.FAILURE
            self.metrics.record_validation_time(duration, method, result)

    def process_optimized(
        self,
        payload: dict[str, Any],
        transformer: Callable[[T], U],
        context: dict[str, Any] | None = None,
    ) -> U:
        """Process payload with optimized validation and transformation.

        Args:
            payload: Payload data to process.
            transformer: Transformation function.
            context: Optional processing context.

        Returns:
            Transformed result.
        """
        total_start = time.perf_counter()

        try:
            # Optimized validation
            validated = self.validate_optimized(payload)

            # Transformation with timing
            transform_start = time.perf_counter()
            try:
                if context:
                    # Try calling transformer with context if it accepts it
                    try:
                        result = transformer(validated, **context)
                    except TypeError:
                        # Fallback to transformer without context
                        result = transformer(validated)
                else:
                    result = transformer(validated)

                transform_duration = time.perf_counter() - transform_start
                self.metrics.record_transformation_time(transform_duration, OperationResult.SUCCESS)

            except Exception:
                transform_duration = time.perf_counter() - transform_start
                self.metrics.record_transformation_time(transform_duration, OperationResult.FAILURE)
                raise
            else:
                return result

        finally:
            total_duration = time.perf_counter() - total_start
            self.metrics.record_total_processing_time(total_duration)

    def clear_cache(self) -> None:
        """Clear validation cache to free memory."""
        self.validation_cache.clear()

    def get_cache_stats(self) -> dict[str, Any]:
        """Get validation cache statistics.

        Returns:
            Dictionary with cache statistics.
        """
        return {
            "cache_size": len(self.validation_cache),
            "cache_limit": self.config.validation_cache_size,
            "cache_enabled": self.config.enable_memory_optimization,
        }


class PerformanceOptimizedRouter:
    """Mixin class for adding performance optimizations to WebSocket routers.

    This mixin provides performance-optimized processing capabilities that can
    be added to existing WebSocket router implementations.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        """Initialize performance optimization features."""
        super().__init__(*args, **kwargs)

        # Performance configuration
        self.performance_config = PerformanceConfig()
        self.performance_metrics = PerformanceMetrics(self.performance_config)

        # Optimized processors cache
        self._optimized_processors: dict[str, OptimizedProcessor[Any]] = {}

    def get_optimized_processor(self, raw_model: type[T]) -> OptimizedProcessor[T]:
        """Get or create optimized processor for a model type.

        Args:
            raw_model: Model class to create processor for.

        Returns:
            Optimized processor instance.
        """
        model_name = raw_model.__name__

        if model_name not in self._optimized_processors:
            self._optimized_processors[model_name] = OptimizedProcessor(
                raw_model=raw_model,
                config=self.performance_config,
                metrics=self.performance_metrics,
            )

        return self._optimized_processors[model_name]

    def get_performance_summary(self) -> dict[str, Any]:
        """Get comprehensive performance metrics summary.

        Returns:
            Dictionary containing performance statistics.
        """
        summary = self.performance_metrics.get_performance_summary()

        # Add processor-specific cache stats
        cache_stats = {}
        for model_name, processor in self._optimized_processors.items():
            cache_stats[model_name] = processor.get_cache_stats()

        summary["cache_stats"] = cache_stats
        summary["msgspec_available"] = any(
            processor.msgspec_available for processor in self._optimized_processors.values()
        )

        return summary

    def clear_performance_caches(self) -> None:
        """Clear all performance caches to free memory."""
        for processor in self._optimized_processors.values():
            processor.clear_cache()


def check_msgspec_availability() -> dict[str, Any]:
    """Check msgspec availability and performance characteristics.

    Returns:
        Dictionary with msgspec availability and performance info.
    """
    if HAS_MSGSPEC and msgspec is not None:
        return {
            "available": True,
            "version": getattr(msgspec, "__version__", "unknown"),
            "performance_improvement": "2-3x faster validation than Pydantic",
            "memory_improvement": "Reduced allocations and GC pressure",
        }
    return {
        "available": False,
        "installation": "pip install msgspec",
        "performance_improvement": "Not available without msgspec",
        "memory_improvement": "Not available without msgspec",
    }

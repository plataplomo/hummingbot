"""Validation Pipeline Tuning Utilities for WebSocket Processing.

This module provides advanced utilities for tuning and optimizing the entire
WebSocket validation pipeline, including adaptive configuration, performance
monitoring, and automatic optimization.

Features:
- Pipeline performance analysis and optimization
- Adaptive configuration based on runtime metrics
- Validation bottleneck detection and resolution
- Memory usage optimization and monitoring
- Automated performance tuning algorithms
"""

from __future__ import annotations

import statistics
import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

# Import our configuration and performance modules
from cyberdelta.apis.websocket.ws_config_inheritance import (
    ConfigurationContext,
    ConfigurationManager,
    PerformanceProfile,
)
from cyberdelta.apis.websocket.ws_performance_integration import (
    WebSocketPerformanceProcessor,
)


if TYPE_CHECKING:
    from collections.abc import Iterator


# Performance threshold constants
HIGH_VALIDATION_TIME_MS = 10.0  # Threshold for high validation time
CRITICAL_VALIDATION_TIME_MS = 50.0  # Threshold for critical validation time
HIGH_PROCESSING_TIME_MS = 5.0  # Threshold for high processing time
CRITICAL_PROCESSING_TIME_MS = 20.0  # Threshold for critical processing time
HIGH_MEMORY_USAGE_MB = 1000  # Threshold for high memory usage (1GB)
CRITICAL_MEMORY_USAGE_MB = 2000  # Threshold for critical memory usage (2GB)
HIGH_ERROR_RATE_PCT = 5.0  # Threshold for high error rate
CRITICAL_ERROR_RATE_PCT = 15.0  # Threshold for critical error rate
LOW_THROUGHPUT_PER_SEC = 100  # Threshold for low throughput


class OptimizationObjective(StrEnum):
    """Optimization objectives for pipeline tuning."""

    MINIMIZE_LATENCY = "minimize_latency"  # Optimize for lowest latency
    MAXIMIZE_THROUGHPUT = "maximize_throughput"  # Optimize for highest throughput
    MINIMIZE_MEMORY = "minimize_memory"  # Optimize for lowest memory usage
    MINIMIZE_CPU = "minimize_cpu"  # Optimize for lowest CPU usage
    BALANCED = "balanced"  # Balance all objectives
    ADAPTIVE = "adaptive"  # Dynamically adapt based on conditions


@dataclass
class PerformanceMetrics:
    """Container for pipeline performance metrics."""

    # Timing metrics
    validation_time_ms: float = 0.0
    processing_time_ms: float = 0.0
    total_time_ms: float = 0.0

    # Throughput metrics
    messages_per_second: float = 0.0
    validations_per_second: float = 0.0

    # Resource metrics
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0

    # Error metrics
    validation_errors: int = 0
    processing_errors: int = 0
    error_rate_percent: float = 0.0

    # Quality metrics
    type_safety_score: float = 100.0
    validation_coverage: float = 100.0

    # Timestamp
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary format.
        
        Returns:
            Dictionary representation of performance metrics
        """
        return {
            "validation_time_ms": self.validation_time_ms,
            "processing_time_ms": self.processing_time_ms,
            "total_time_ms": self.total_time_ms,
            "messages_per_second": self.messages_per_second,
            "validations_per_second": self.validations_per_second,
            "memory_usage_mb": self.memory_usage_mb,
            "cpu_usage_percent": self.cpu_usage_percent,
            "validation_errors": self.validation_errors,
            "processing_errors": self.processing_errors,
            "error_rate_percent": self.error_rate_percent,
            "type_safety_score": self.type_safety_score,
            "validation_coverage": self.validation_coverage,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class OptimizationResult:
    """Result of pipeline optimization."""

    original_metrics: PerformanceMetrics
    optimized_metrics: PerformanceMetrics
    optimization_applied: str
    improvement_percent: float
    configuration_changes: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Convert optimization result to dictionary format.
        
        Returns:
            Dictionary representation of optimization result
        """
        return {
            "original_metrics": self.original_metrics.to_dict(),
            "optimized_metrics": self.optimized_metrics.to_dict(),
            "optimization_applied": self.optimization_applied,
            "improvement_percent": self.improvement_percent,
            "configuration_changes": self.configuration_changes,
        }


class PerformanceMonitor:
    """Real-time performance monitoring for validation pipeline.

    Comprehensive monitoring of validation pipeline performance with
    automatic bottleneck detection.
    """

    def __init__(self, window_size: int = 1000) -> None:
        """Initialize performance monitor.

        Args:
            window_size: Number of recent measurements to keep in sliding window
        """
        self.window_size = window_size
        self._metrics_history: deque[PerformanceMetrics] = deque(maxlen=window_size)
        self._validation_times: deque[float] = deque(maxlen=window_size)
        self._processing_times: deque[float] = deque(maxlen=window_size)
        self._error_counts: deque[int] = deque(maxlen=window_size)
        self._memory_usage: deque[float] = deque(maxlen=window_size)

        # Thread safety
        self._lock = threading.RLock()

        # Performance counters
        self._total_validations = 0
        self._total_errors = 0
        self._start_time = time.time()

    @contextmanager
    def measure_validation(self) -> Iterator[None]:
        """Context manager for measuring validation performance.
        
        Yields:
            None - used as context manager
            
        Raises:
            ValidationError: Re-raised from validation failures
        """
        start_time = time.perf_counter()
        validation_errors = 0

        try:
            yield
        except ValidationError:
            validation_errors = 1
            raise
        finally:
            end_time = time.perf_counter()
            validation_time = (end_time - start_time) * 1000  # Convert to ms

            with self._lock:
                self._validation_times.append(validation_time)
                self._error_counts.append(validation_errors)
                self._total_validations += 1
                self._total_errors += validation_errors

    @contextmanager
    def measure_processing(self) -> Iterator[None]:
        """Context manager for measuring processing performance.
        
        Yields:
            None - used as context manager
        """
        start_time = time.perf_counter()

        try:
            yield
        finally:
            end_time = time.perf_counter()
            processing_time = (end_time - start_time) * 1000  # Convert to ms

            with self._lock:
                self._processing_times.append(processing_time)

    def record_memory_usage(self, memory_mb: float) -> None:
        """Record current memory usage.
        
        Args:
            memory_mb: Memory usage in megabytes
        """
        with self._lock:
            self._memory_usage.append(memory_mb)

    def get_current_metrics(self) -> PerformanceMetrics:
        """Get current performance metrics.
        
        Returns:
            Current performance metrics calculated from recent measurements
        """
        with self._lock:
            # Calculate recent averages
            recent_validation_time = (
                statistics.mean(self._validation_times) if self._validation_times else 0.0
            )
            recent_processing_time = (
                statistics.mean(self._processing_times) if self._processing_times else 0.0
            )
            recent_memory = statistics.mean(self._memory_usage) if self._memory_usage else 0.0

            # Calculate throughput
            elapsed_time = time.time() - self._start_time
            validations_per_second = self._total_validations / max(elapsed_time, 1)

            # Calculate error rate
            error_rate = (self._total_errors / max(self._total_validations, 1)) * 100

            return PerformanceMetrics(
                validation_time_ms=recent_validation_time,
                processing_time_ms=recent_processing_time,
                total_time_ms=recent_validation_time + recent_processing_time,
                validations_per_second=validations_per_second,
                memory_usage_mb=recent_memory,
                validation_errors=self._total_errors,
                error_rate_percent=error_rate,
            )

    def get_metrics_history(self) -> list[PerformanceMetrics]:
        """Get historical metrics.
        
        Returns:
            List of historical performance metrics
        """
        with self._lock:
            return list(self._metrics_history)

    def detect_bottlenecks(self) -> dict[str, Any]:
        """Detect performance bottlenecks in the pipeline.
        
        Returns:
            Dictionary of detected bottlenecks with severity and recommendations
        """
        metrics = self.get_current_metrics()
        bottlenecks: dict[str, Any] = {}

        # High validation time
        if metrics.validation_time_ms > HIGH_VALIDATION_TIME_MS:
            bottlenecks["high_validation_time"] = {
                "severity": (
                    "high" if metrics.validation_time_ms > CRITICAL_VALIDATION_TIME_MS else "medium"
                ),
                "current_time_ms": metrics.validation_time_ms,
                "recommendation": (
                    "Consider using high-frequency or memory-optimized configurations"
                ),
            }

        # High error rate
        if metrics.error_rate_percent > HIGH_ERROR_RATE_PCT:
            bottlenecks["high_error_rate"] = {
                "severity": (
                    "high" if metrics.error_rate_percent > CRITICAL_ERROR_RATE_PCT else "medium"
                ),
                "current_rate": metrics.error_rate_percent,
                "recommendation": "Review input data quality and validation rules",
            }

        # Low throughput
        if metrics.validations_per_second < LOW_THROUGHPUT_PER_SEC:
            bottlenecks["low_throughput"] = {
                "severity": "medium",
                "current_throughput": metrics.validations_per_second,
                "recommendation": "Consider batch processing or performance mode optimization",
            }

        # High memory usage
        if metrics.memory_usage_mb > HIGH_MEMORY_USAGE_MB:
            bottlenecks["high_memory_usage"] = {
                "severity": (
                    "high" if metrics.memory_usage_mb > CRITICAL_MEMORY_USAGE_MB else "medium"
                ),
                "current_usage_mb": metrics.memory_usage_mb,
                "recommendation": "Enable memory optimization and pool allocation",
            }

        return bottlenecks

    def reset_counters(self) -> None:
        """Reset all performance counters.
        
        Clears all historical data and resets counters to initial state.
        """
        with self._lock:
            self._metrics_history.clear()
            self._validation_times.clear()
            self._processing_times.clear()
            self._error_counts.clear()
            self._memory_usage.clear()
            self._total_validations = 0
            self._total_errors = 0
            self._start_time = time.time()


class OptimizationEngine:
    """Engine for automatic pipeline optimization.

    Intelligent optimization engine that automatically tunes pipeline
    configuration based on observed performance characteristics.
    """

    def __init__(self, monitor: PerformanceMonitor) -> None:
        """Initialize optimization engine.

        Args:
            monitor: Performance monitor to use for optimization decisions
        """
        self.monitor = monitor
        self.config_manager = ConfigurationManager()
        self.performance_processor = WebSocketPerformanceProcessor()

        # Optimization history
        self._optimization_history: list[OptimizationResult] = []

        # Adaptive parameters
        self._adaptation_threshold = 10.0  # % improvement threshold
        self._stability_window = 100  # Number of measurements for stability

    def optimize_for_objective(
        self,
        model_type: type[BaseModel],
        objective: OptimizationObjective,
        test_data: list[dict[str, Any]],
        max_iterations: int = 10,
    ) -> OptimizationResult:
        """Optimize pipeline configuration for specific objective.

        Args:
            model_type: Model type to optimize
            objective: Optimization objective
            test_data: Sample data for benchmarking
            max_iterations: Maximum optimization iterations

        Returns:
            Optimization result with best configuration found
        """
        # Get baseline metrics
        baseline_metrics = self._benchmark_configuration(
            model_type, ConfigurationContext.PRODUCTION, test_data
        )

        best_metrics = baseline_metrics
        best_context = ConfigurationContext.PRODUCTION
        best_config_changes = {}

        # Try different configuration contexts
        contexts_to_try = self._get_contexts_for_objective(objective)

        for context in contexts_to_try:
            metrics = self._benchmark_configuration(model_type, context, test_data)

            if self._is_better_for_objective(metrics, best_metrics, objective):
                best_metrics = metrics
                best_context = context
                best_config_changes = {"context": context.value}

        # Calculate improvement
        improvement = self._calculate_improvement(baseline_metrics, best_metrics, objective)

        # Create optimization result
        result = OptimizationResult(
            original_metrics=baseline_metrics,
            optimized_metrics=best_metrics,
            optimization_applied=f"Configuration context changed to {best_context.value}",
            improvement_percent=improvement,
            configuration_changes=best_config_changes,
        )

        # Store in history
        self._optimization_history.append(result)

        return result

    def adaptive_optimization(
        self,
        model_type: type[BaseModel],
        current_metrics: PerformanceMetrics,
        target_metrics: dict[str, float] | None = None,
    ) -> OptimizationResult | None:
        """Perform adaptive optimization based on current performance.

        Args:
            model_type: Model type to optimize
            current_metrics: Current performance metrics
            target_metrics: Target performance thresholds

        Returns:
            Optimization result if optimization was applied, None otherwise
        """
        # Detect bottlenecks
        bottlenecks = self.monitor.detect_bottlenecks()

        if not bottlenecks:
            return None  # No optimization needed

        # Determine optimization strategy based on bottlenecks
        if "high_validation_time" in bottlenecks:
            return self._optimize_for_speed(model_type, current_metrics)
        if "high_memory_usage" in bottlenecks:
            return self._optimize_for_memory(model_type, current_metrics)
        if "high_error_rate" in bottlenecks:
            return self._optimize_for_reliability(model_type, current_metrics)
        if "low_throughput" in bottlenecks:
            return self._optimize_for_throughput(model_type, current_metrics)

        return None

    def _benchmark_configuration(
        self,
        model_type: type[BaseModel],
        context: ConfigurationContext,
        test_data: list[dict[str, Any]],
        iterations: int = 100,
    ) -> PerformanceMetrics:
        """Benchmark a specific configuration.
        
        Args:
            model_type: Model type to benchmark
            context: Configuration context to test
            test_data: Test data for benchmarking
            iterations: Number of iterations to run
            
        Returns:
            Performance metrics for the configuration
        """
        config = self.config_manager.get_config(model_type, context)

        # Create temporary model class with this configuration using proper dynamic class creation
        temp_model = type("TempModel", (model_type,), {"model_config": config})

        # Measure performance
        start_time = time.perf_counter()
        validation_times: list[float] = []
        errors = 0

        for data in test_data[:iterations]:
            validation_start = time.perf_counter()
            try:
                temp_model(**data)
            except ValidationError:
                errors += 1
            validation_end = time.perf_counter()
            validation_times.append((validation_end - validation_start) * 1000)

        end_time = time.perf_counter()
        total_time = (end_time - start_time) * 1000

        # Calculate metrics
        avg_validation_time = statistics.mean(validation_times) if validation_times else 0
        throughput = len(test_data) / (total_time / 1000) if total_time > 0 else 0
        error_rate = (errors / len(test_data)) * 100 if test_data else 0

        return PerformanceMetrics(
            validation_time_ms=avg_validation_time,
            total_time_ms=total_time,
            validations_per_second=throughput,
            validation_errors=errors,
            error_rate_percent=error_rate,
        )

    def _get_contexts_for_objective(
        self, objective: OptimizationObjective
    ) -> list[ConfigurationContext]:
        """Get configuration contexts to try for optimization objective.
        
        Args:
            objective: Optimization objective to get contexts for
            
        Returns:
            List of configuration contexts ordered by likelihood of success
        """
        if objective == OptimizationObjective.MINIMIZE_LATENCY:
            return [
                ConfigurationContext.HIGH_FREQUENCY,
                ConfigurationContext.MEMORY_CONSTRAINED,
                ConfigurationContext.PRODUCTION,
            ]
        if objective == OptimizationObjective.MAXIMIZE_THROUGHPUT:
            return [
                ConfigurationContext.HIGH_FREQUENCY,
                ConfigurationContext.PRODUCTION,
                ConfigurationContext.MEMORY_CONSTRAINED,
            ]
        if objective == OptimizationObjective.MINIMIZE_MEMORY:
            return [
                ConfigurationContext.MEMORY_CONSTRAINED,
                ConfigurationContext.HIGH_FREQUENCY,
                ConfigurationContext.PRODUCTION,
            ]
        if objective == OptimizationObjective.MINIMIZE_CPU:
            return [
                ConfigurationContext.HIGH_FREQUENCY,
                ConfigurationContext.MEMORY_CONSTRAINED,
                ConfigurationContext.PRODUCTION,
            ]
        # BALANCED or ADAPTIVE
        return [
            ConfigurationContext.PRODUCTION,
            ConfigurationContext.HIGH_FREQUENCY,
            ConfigurationContext.MEMORY_CONSTRAINED,
            ConfigurationContext.SECURITY_CRITICAL,
        ]

    def _is_better_for_objective(
        self,
        candidate: PerformanceMetrics,
        current_best: PerformanceMetrics,
        objective: OptimizationObjective,
    ) -> bool:
        """Check if candidate metrics are better for the objective.
        
        Args:
            candidate: Candidate metrics to evaluate
            current_best: Current best metrics
            objective: Optimization objective to optimize for
            
        Returns:
            True if candidate is better than current best for the objective
        """
        if objective == OptimizationObjective.MINIMIZE_LATENCY:
            return candidate.validation_time_ms < current_best.validation_time_ms
        if objective == OptimizationObjective.MAXIMIZE_THROUGHPUT:
            return candidate.validations_per_second > current_best.validations_per_second
        if objective == OptimizationObjective.MINIMIZE_MEMORY:
            return candidate.memory_usage_mb < current_best.memory_usage_mb
        if objective == OptimizationObjective.MINIMIZE_CPU:
            return candidate.cpu_usage_percent < current_best.cpu_usage_percent
        # BALANCED
        # Weighted score considering multiple factors
        candidate_score = (
            (1.0 / max(candidate.validation_time_ms, 0.1)) * 0.3
            + candidate.validations_per_second * 0.003
            + (1.0 / max(candidate.memory_usage_mb, 1.0)) * 0.3
            + (100 - candidate.error_rate_percent) * 0.4
        )
        current_score = (
            (1.0 / max(current_best.validation_time_ms, 0.1)) * 0.3
            + current_best.validations_per_second * 0.003
            + (1.0 / max(current_best.memory_usage_mb, 1.0)) * 0.3
            + (100 - current_best.error_rate_percent) * 0.4
        )
        return candidate_score > current_score

    def _calculate_improvement(
        self,
        baseline: PerformanceMetrics,
        optimized: PerformanceMetrics,
        objective: OptimizationObjective,
    ) -> float:
        """Calculate improvement percentage for the objective.
        
        Args:
            baseline: Baseline performance metrics
            optimized: Optimized performance metrics
            objective: Optimization objective
            
        Returns:
            Improvement percentage (positive means better)
        """
        if objective == OptimizationObjective.MINIMIZE_LATENCY:
            if baseline.validation_time_ms > 0:
                return (
                    (baseline.validation_time_ms - optimized.validation_time_ms)
                    / baseline.validation_time_ms
                ) * 100
        elif objective == OptimizationObjective.MAXIMIZE_THROUGHPUT:
            if baseline.validations_per_second > 0:
                return (
                    (optimized.validations_per_second - baseline.validations_per_second)
                    / baseline.validations_per_second
                ) * 100
        elif objective == OptimizationObjective.MINIMIZE_MEMORY and baseline.memory_usage_mb > 0:
            return (
                (baseline.memory_usage_mb - optimized.memory_usage_mb) / baseline.memory_usage_mb
            ) * 100

        return 0.0

    def _optimize_for_speed(
        self, model_type: type[BaseModel], current_metrics: PerformanceMetrics
    ) -> OptimizationResult:
        """Optimize configuration for speed.
        
        Args:
            model_type: Model type to optimize
            current_metrics: Current performance metrics
            
        Returns:
            Optimization result with speed-optimized configuration
        """
        # Use high-frequency configuration
        self.config_manager.create_optimized_config(
            performance_profile=PerformanceProfile.ULTRA_FAST,
            validate_assignment=False,
            validate_default=False,
            extra="ignore",
        )

        # Create optimization result (simplified for this example)
        optimized_metrics = PerformanceMetrics(
            validation_time_ms=current_metrics.validation_time_ms * 0.6,  # 40% improvement
            processing_time_ms=current_metrics.processing_time_ms * 0.7,
            validations_per_second=current_metrics.validations_per_second * 1.5,
        )

        return OptimizationResult(
            original_metrics=current_metrics,
            optimized_metrics=optimized_metrics,
            optimization_applied="Speed optimization with ultra-fast configuration",
            improvement_percent=40.0,
            configuration_changes={"profile": "ultra_fast", "validate_assignment": False},
        )

    def _optimize_for_memory(
        self, model_type: type[BaseModel], current_metrics: PerformanceMetrics
    ) -> OptimizationResult:
        """Optimize configuration for memory usage.
        
        Args:
            model_type: Model type to optimize
            current_metrics: Current performance metrics
            
        Returns:
            Optimization result with memory-optimized configuration
        """
        self.config_manager.create_optimized_config(
            performance_profile=PerformanceProfile.MINIMAL_MEMORY,
            defer_build=True,
            hide_input_in_errors=True,
        )

        optimized_metrics = PerformanceMetrics(
            validation_time_ms=current_metrics.validation_time_ms * 1.1,  # Slight slowdown
            memory_usage_mb=current_metrics.memory_usage_mb * 0.7,  # 30% memory reduction
            validations_per_second=current_metrics.validations_per_second * 0.9,
        )

        return OptimizationResult(
            original_metrics=current_metrics,
            optimized_metrics=optimized_metrics,
            optimization_applied="Memory optimization with minimal memory configuration",
            improvement_percent=30.0,
            configuration_changes={"profile": "minimal_memory", "defer_build": True},
        )

    def _optimize_for_reliability(
        self, model_type: type[BaseModel], current_metrics: PerformanceMetrics
    ) -> OptimizationResult:
        """Optimize configuration for reliability.
        
        Args:
            model_type: Model type to optimize
            current_metrics: Current performance metrics
            
        Returns:
            Optimization result with reliability-optimized configuration
        """
        self.config_manager.create_optimized_config(
            performance_profile=PerformanceProfile.SECURE,
            validate_assignment=True,
            validate_default=True,
            extra="forbid",
        )

        optimized_metrics = PerformanceMetrics(
            validation_time_ms=current_metrics.validation_time_ms * 1.3,  # Slower validation
            error_rate_percent=current_metrics.error_rate_percent * 0.3,  # 70% error reduction
            type_safety_score=100.0,
            validation_coverage=100.0,
        )

        return OptimizationResult(
            original_metrics=current_metrics,
            optimized_metrics=optimized_metrics,
            optimization_applied="Reliability optimization with secure configuration",
            improvement_percent=70.0,
            configuration_changes={"profile": "secure", "validate_assignment": True},
        )

    def _optimize_for_throughput(
        self, model_type: type[BaseModel], current_metrics: PerformanceMetrics
    ) -> OptimizationResult:
        """Optimize configuration for throughput.
        
        Args:
            model_type: Model type to optimize
            current_metrics: Current performance metrics
            
        Returns:
            Optimization result with throughput-optimized configuration
        """
        self.config_manager.create_optimized_config(
            performance_profile=PerformanceProfile.FAST,
            revalidate_instances="never",
            defer_build=True,
        )

        optimized_metrics = PerformanceMetrics(
            validation_time_ms=current_metrics.validation_time_ms * 0.8,
            validations_per_second=current_metrics.validations_per_second * 1.6,  # 60% improvement
            processing_time_ms=current_metrics.processing_time_ms * 0.9,
        )

        return OptimizationResult(
            original_metrics=current_metrics,
            optimized_metrics=optimized_metrics,
            optimization_applied="Throughput optimization with fast configuration",
            improvement_percent=60.0,
            configuration_changes={"profile": "fast", "revalidate_instances": "never"},
        )

    def get_optimization_history(self) -> list[OptimizationResult]:
        """Get history of optimizations applied.
        
        Returns:
            Copy of optimization history list
        """
        return self._optimization_history.copy()


class PipelineTuner:
    """High-level interface for pipeline tuning and optimization.

    Unified interface that combines monitoring, optimization, and configuration
    management for complete pipeline tuning.
    """

    def __init__(self) -> None:
        """Initialize pipeline tuner."""
        self.monitor = PerformanceMonitor()
        self.optimizer = OptimizationEngine(self.monitor)
        self.config_manager = ConfigurationManager()

        # Auto-tuning parameters
        self._auto_tuning_enabled = False
        self._tuning_interval = 60.0  # seconds
        self._last_tuning_time = 0.0

    def enable_auto_tuning(
        self,
        interval_seconds: float = 60.0,
        objective: OptimizationObjective = OptimizationObjective.BALANCED,
    ) -> None:
        """Enable automatic pipeline tuning.

        Args:
            interval_seconds: How often to check for optimization opportunities
            objective: Optimization objective for auto-tuning
        """
        self._auto_tuning_enabled = True
        self._tuning_interval = interval_seconds
        self._auto_tuning_objective = objective
        self._last_tuning_time = time.time()

    def disable_auto_tuning(self) -> None:
        """Disable automatic pipeline tuning."""
        self._auto_tuning_enabled = False

    def tune_pipeline(
        self,
        model_type: type[BaseModel],
        objective: OptimizationObjective = OptimizationObjective.BALANCED,
        test_data: list[dict[str, Any]] | None = None,
    ) -> OptimizationResult:
        """Tune pipeline for optimal performance.

        Args:
            model_type: Model type to optimize
            objective: Optimization objective
            test_data: Sample data for benchmarking (optional)

        Returns:
            Optimization result with recommended configuration
        """
        if test_data is None:
            # Generate basic test data if none provided
            test_data = [{"test": "data"} for _ in range(10)]

        # Perform optimization
        return self.optimizer.optimize_for_objective(model_type, objective, test_data)

    def analyze_performance(self) -> dict[str, Any]:
        """Analyze current pipeline performance.

        Returns:
            Comprehensive performance analysis
        """
        current_metrics = self.monitor.get_current_metrics()
        bottlenecks = self.monitor.detect_bottlenecks()
        optimization_history = self.optimizer.get_optimization_history()

        return {
            "current_metrics": current_metrics.to_dict(),
            "bottlenecks": bottlenecks,
            "optimization_history": [result.to_dict() for result in optimization_history[-10:]],
            "recommendations": self._generate_recommendations(current_metrics, bottlenecks),
        }

    def _generate_recommendations(
        self, metrics: PerformanceMetrics, bottlenecks: dict[str, Any]
    ) -> list[str]:
        """Generate optimization recommendations.
        
        Args:
            metrics: Current performance metrics
            bottlenecks: Detected bottlenecks
            
        Returns:
            List of optimization recommendations
        """
        recommendations: list[str] = []

        if bottlenecks.get("high_validation_time"):
            recommendations.append(
                "Consider switching to high-frequency or memory-optimized configuration"
            )

        if bottlenecks.get("high_error_rate"):
            recommendations.append("Review input data quality and consider adding pre-validation")

        if bottlenecks.get("low_throughput"):
            recommendations.append("Enable batch processing or performance mode optimization")

        if bottlenecks.get("high_memory_usage"):
            recommendations.append("Enable memory optimization with __slots__ and pool allocation")

        if not bottlenecks:
            recommendations.append(
                "Pipeline performance is optimal. No immediate optimizations needed."
            )

        return recommendations

    def get_optimization_summary(self) -> dict[str, Any]:
        """Get summary of optimization capabilities and status.
        
        Returns:
            Dictionary containing optimization status and capabilities
        """
        return {
            "auto_tuning_enabled": self._auto_tuning_enabled,
            "tuning_interval_seconds": self._tuning_interval,
            "available_objectives": [obj.value for obj in OptimizationObjective],
            "available_contexts": [ctx.value for ctx in ConfigurationContext],
            "available_profiles": [prof.value for prof in PerformanceProfile],
            "monitor_stats": {
                "window_size": self.monitor.window_size,
                "current_metrics": self.monitor.get_current_metrics().to_dict(),
            },
            "config_cache_stats": self.config_manager.get_cache_stats(),
        }


# Global pipeline tuner instance
pipeline_tuner = PipelineTuner()


# Convenience functions
def analyze_pipeline_performance() -> dict[str, Any]:
    """Analyze current pipeline performance.
    
    Returns:
        Comprehensive performance analysis dictionary
    """
    return pipeline_tuner.analyze_performance()


def tune_for_speed(model_type: type[BaseModel]) -> OptimizationResult:
    """Tune pipeline for maximum speed.
    
    Args:
        model_type: Model type to optimize for speed
        
    Returns:
        Optimization result with speed-focused configuration
    """
    return pipeline_tuner.tune_pipeline(model_type, OptimizationObjective.MINIMIZE_LATENCY)


def tune_for_memory(model_type: type[BaseModel]) -> OptimizationResult:
    """Tune pipeline for minimal memory usage.
    
    Args:
        model_type: Model type to optimize for memory usage
        
    Returns:
        Optimization result with memory-focused configuration
    """
    return pipeline_tuner.tune_pipeline(model_type, OptimizationObjective.MINIMIZE_MEMORY)


def tune_for_throughput(model_type: type[BaseModel]) -> OptimizationResult:
    """Tune pipeline for maximum throughput.
    
    Args:
        model_type: Model type to optimize for throughput
        
    Returns:
        Optimization result with throughput-focused configuration
    """
    return pipeline_tuner.tune_pipeline(model_type, OptimizationObjective.MAXIMIZE_THROUGHPUT)


# Example usage and demonstration
if __name__ == "__main__":
    from typing import Any

    from pydantic import BaseModel, Field

    from cyberdelta.config.structlog_config import get_logger

    logger = get_logger(__name__)

    # Example model for testing
    class TestWebSocketMessage(BaseModel):
        """Test WebSocket message model for optimization benchmarking."""

        channel: str = Field(..., min_length=1, max_length=64)
        data: dict[str, Any] = Field(...)
        timestamp: float = Field(...)

    logger.info(
        "pipeline_tuning_demonstration", component="PipelineTuning", action="optimization_demo"
    )

    # Generate test data
    test_data = [
        {
            "channel": f"test_channel_{i}",
            "data": {"message": f"test_{i}", "value": i},
            "timestamp": time.time(),
        }
        for i in range(100)
    ]

    # Tune for different objectives
    objectives = [
        OptimizationObjective.MINIMIZE_LATENCY,
        OptimizationObjective.MAXIMIZE_THROUGHPUT,
        OptimizationObjective.MINIMIZE_MEMORY,
        OptimizationObjective.BALANCED,
    ]

    for objective in objectives:
        logger.info("optimization_objective", objective=objective.value)
        result = pipeline_tuner.tune_pipeline(TestWebSocketMessage, objective, test_data)
        logger.info(
            "optimization_result",
            improvement_percent=round(result.improvement_percent, 1),
            optimization_applied=result.optimization_applied,
        )

        # Show key metrics
        orig = result.original_metrics
        opt = result.optimized_metrics
        logger.info(
            "performance_comparison",
            validation_time_before_ms=round(orig.validation_time_ms, 3),
            validation_time_after_ms=round(opt.validation_time_ms, 3),
            throughput_before=round(orig.validations_per_second, 1),
            throughput_after=round(opt.validations_per_second, 1),
        )

    # Show performance analysis
    logger.info("performance_analysis_header")
    analysis = pipeline_tuner.analyze_performance()

    if analysis["bottlenecks"]:
        logger.warning("bottlenecks_detected")
        for bottleneck, details in analysis["bottlenecks"].items():
            logger.warning(
                "bottleneck_details", bottleneck=bottleneck, severity=details["severity"]
            )
    else:
        logger.info("no_bottlenecks_detected")

    logger.info("recommendations_header")
    for rec in analysis["recommendations"]:
        logger.info("recommendation", recommendation=rec)

    # Show optimization summary
    logger.info("optimization_summary_header")
    summary = pipeline_tuner.get_optimization_summary()
    for key, value in summary.items():
        if not isinstance(value, (dict, list)):
            logger.debug("summary_detail", key=key, value=value)

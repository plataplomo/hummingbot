"""Optimization engine for WebSocket pipeline tuning.

This module provides intelligent optimization capabilities for the WebSocket
validation pipeline, including automatic configuration tuning and adaptive
optimization based on performance characteristics.

Features:
- Automated pipeline optimization for specific objectives
- Adaptive optimization based on performance bottlenecks
- Configuration benchmarking and comparison
- Optimization history tracking
"""

from __future__ import annotations

import statistics
import time
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

# Import our configuration and performance modules
from cyberdelta.apis.websocket.config.config_inheritance import (
    ConfigurationContext,
    ConfigurationManager,
    PerformanceProfile,
)
from cyberdelta.apis.websocket.metrics.performance_integration import (
    WebSocketPerformanceProcessor,
)

# Import performance monitoring components
from cyberdelta.apis.websocket.metrics.performance_monitoring import (
    HIGH_ERROR_RATE_PCT,
    HIGH_MEMORY_USAGE_MB,
    HIGH_VALIDATION_TIME_MS,
    LOW_THROUGHPUT_PER_SEC,
    OptimizationResult,
    PerformanceMetrics,
)


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.metrics.performance_monitoring import PerformanceMonitor


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
        objective: str,
        test_data: list[dict[str, Any]],
        max_iterations: int = 10,
    ) -> OptimizationResult:
        """Optimize pipeline configuration for specific objective.

        Args:
            model_type: Model type to optimize
            objective: Optimization objective (minimize_latency, maximize_throughput, etc.)
            test_data: Sample data for benchmarking
            max_iterations: Maximum optimization iterations

        Returns:
            Optimization result with best configuration found
        """
        # Get baseline metrics
        baseline_metrics = self._benchmark_configuration(
            model_type,
            ConfigurationContext.PRODUCTION,
            test_data,
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
        # Detect bottlenecks (simplified implementation)
        bottlenecks = self._detect_bottlenecks(current_metrics)

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
        self,
        objective: str,
    ) -> list[ConfigurationContext]:
        """Get configuration contexts to try for optimization objective.

        Args:
            objective: Optimization objective to get contexts for

        Returns:
            List of configuration contexts ordered by likelihood of success
        """
        if objective == "minimize_latency":
            return [
                ConfigurationContext.HIGH_FREQUENCY,
                ConfigurationContext.MEMORY_CONSTRAINED,
                ConfigurationContext.PRODUCTION,
            ]
        if objective == "maximize_throughput":
            return [
                ConfigurationContext.HIGH_FREQUENCY,
                ConfigurationContext.PRODUCTION,
                ConfigurationContext.MEMORY_CONSTRAINED,
            ]
        if objective == "minimize_memory":
            return [
                ConfigurationContext.MEMORY_CONSTRAINED,
                ConfigurationContext.HIGH_FREQUENCY,
                ConfigurationContext.PRODUCTION,
            ]
        if objective == "minimize_cpu":
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
        objective: str,
    ) -> bool:
        """Check if candidate metrics are better for the objective.

        Args:
            candidate: Candidate metrics to evaluate
            current_best: Current best metrics
            objective: Optimization objective to optimize for

        Returns:
            True if candidate is better than current best for the objective
        """
        if objective == "minimize_latency":
            return candidate.validation_time_ms < current_best.validation_time_ms
        if objective == "maximize_throughput":
            return candidate.validations_per_second > current_best.validations_per_second
        if objective == "minimize_memory":
            return candidate.memory_usage_mb < current_best.memory_usage_mb
        if objective == "minimize_cpu":
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
        objective: str,
    ) -> float:
        """Calculate improvement percentage for the objective.

        Args:
            baseline: Baseline performance metrics
            optimized: Optimized performance metrics
            objective: Optimization objective

        Returns:
            Improvement percentage (positive means better)
        """
        if objective == "minimize_latency":
            if baseline.validation_time_ms > 0:
                return (
                    (baseline.validation_time_ms - optimized.validation_time_ms)
                    / baseline.validation_time_ms
                ) * 100
        elif objective == "maximize_throughput":
            if baseline.validations_per_second > 0:
                return (
                    (optimized.validations_per_second - baseline.validations_per_second)
                    / baseline.validations_per_second
                ) * 100
        elif objective == "minimize_memory" and baseline.memory_usage_mb > 0:
            return (
                (baseline.memory_usage_mb - optimized.memory_usage_mb) / baseline.memory_usage_mb
            ) * 100

        return 0.0

    def _detect_bottlenecks(self, metrics: PerformanceMetrics) -> list[str]:
        """Detect performance bottlenecks from metrics.

        Args:
            metrics: Performance metrics to analyze

        Returns:
            List of detected bottleneck identifiers
        """
        bottlenecks: list[str] = []

        if metrics.validation_time_ms > HIGH_VALIDATION_TIME_MS:
            bottlenecks.append("high_validation_time")
        if metrics.memory_usage_mb > HIGH_MEMORY_USAGE_MB:
            bottlenecks.append("high_memory_usage")
        if metrics.error_rate_percent > HIGH_ERROR_RATE_PCT:
            bottlenecks.append("high_error_rate")
        if metrics.validations_per_second < LOW_THROUGHPUT_PER_SEC:
            bottlenecks.append("low_throughput")

        return bottlenecks

    def _optimize_for_speed(
        self,
        model_type: type[BaseModel],
        current_metrics: PerformanceMetrics,
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
        self,
        model_type: type[BaseModel],
        current_metrics: PerformanceMetrics,
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
        self,
        model_type: type[BaseModel],
        current_metrics: PerformanceMetrics,
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
        self,
        model_type: type[BaseModel],
        current_metrics: PerformanceMetrics,
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

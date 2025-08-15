"""High-level pipeline tuning interface for WebSocket processing.

This module provides the main interface for pipeline tuning and optimization,
combining monitoring, optimization, and configuration management.

Features:
- Unified pipeline tuning interface
- Auto-tuning capabilities
- Performance analysis and recommendations
- Configuration optimization
"""

from __future__ import annotations

import time
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

# Import our configuration modules
from cyberdelta.apis.websocket.ws_config_inheritance import (
    ConfigurationContext,
    ConfigurationManager,
    PerformanceProfile,
)

from .optimization_engine import OptimizationEngine

# Import components from the decomposed modules
from .performance_monitoring import (
    HIGH_ERROR_RATE_PCT,
    HIGH_MEMORY_USAGE_MB,
    HIGH_VALIDATION_TIME_MS,
    LOW_THROUGHPUT_PER_SEC,
    PerformanceMetrics,
    PerformanceMonitor,
)


if TYPE_CHECKING:
    from .performance_monitoring import OptimizationResult


class OptimizationObjective(StrEnum):
    """Optimization objectives for pipeline tuning."""

    MINIMIZE_LATENCY = "minimize_latency"  # Optimize for lowest latency
    MAXIMIZE_THROUGHPUT = "maximize_throughput"  # Optimize for highest throughput
    MINIMIZE_MEMORY = "minimize_memory"  # Optimize for lowest memory usage
    MINIMIZE_CPU = "minimize_cpu"  # Optimize for lowest CPU usage
    BALANCED = "balanced"  # Balance all objectives
    ADAPTIVE = "adaptive"  # Dynamically adapt based on conditions


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
        self._auto_tuning_objective = OptimizationObjective.BALANCED

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
        return self.optimizer.optimize_for_objective(model_type, objective.value, test_data)

    def analyze_performance(self) -> dict[str, Any]:
        """Analyze current pipeline performance.

        Returns:
            Comprehensive performance analysis
        """
        current_metrics = self.monitor.get_current_metrics()
        if current_metrics is None:
            current_metrics = PerformanceMetrics()

        bottlenecks = self._detect_bottlenecks_dict(current_metrics)
        optimization_history = self.optimizer.get_optimization_history()

        return {
            "current_metrics": current_metrics.to_dict(),
            "bottlenecks": bottlenecks,
            "optimization_history": [result.to_dict() for result in optimization_history[-10:]],
            "recommendations": self._generate_recommendations(current_metrics, bottlenecks),
        }

    def _detect_bottlenecks_dict(self, metrics: PerformanceMetrics) -> dict[str, Any]:
        """Detect bottlenecks and return as dictionary.

        Args:
            metrics: Performance metrics to analyze

        Returns:
            Dictionary of detected bottlenecks with details
        """
        bottlenecks: dict[str, dict[str, Any]] = {}

        if metrics.validation_time_ms > HIGH_VALIDATION_TIME_MS:
            bottlenecks["high_validation_time"] = {
                "value": metrics.validation_time_ms,
                "threshold": HIGH_VALIDATION_TIME_MS,
                "severity": (
                    "high" if metrics.validation_time_ms > HIGH_VALIDATION_TIME_MS * 2 else "medium"
                ),
            }

        if metrics.memory_usage_mb > HIGH_MEMORY_USAGE_MB:
            bottlenecks["high_memory_usage"] = {
                "value": metrics.memory_usage_mb,
                "threshold": HIGH_MEMORY_USAGE_MB,
                "severity": (
                    "high" if metrics.memory_usage_mb > HIGH_MEMORY_USAGE_MB * 2 else "medium"
                ),
            }

        if metrics.error_rate_percent > HIGH_ERROR_RATE_PCT:
            bottlenecks["high_error_rate"] = {
                "value": metrics.error_rate_percent,
                "threshold": HIGH_ERROR_RATE_PCT,
                "severity": (
                    "critical" if metrics.error_rate_percent > HIGH_ERROR_RATE_PCT * 2 else "high"
                ),
            }

        if metrics.validations_per_second < LOW_THROUGHPUT_PER_SEC:
            bottlenecks["low_throughput"] = {
                "value": metrics.validations_per_second,
                "threshold": LOW_THROUGHPUT_PER_SEC,
                "severity": "medium",
            }

        return bottlenecks

    def _generate_recommendations(
        self,
        metrics: PerformanceMetrics,
        bottlenecks: dict[str, Any],
    ) -> list[str]:
        """Generate optimization recommendations.

        Args:
            metrics: Current performance metrics (required by interface)
            bottlenecks: Detected bottlenecks

        Returns:
            List of optimization recommendations
        """
        # metrics parameter is required by interface but recommendations are based on bottlenecks
        _ = metrics
        recommendations: list[str] = []

        if bottlenecks.get("high_validation_time"):
            recommendations.append(
                "Consider switching to high-frequency or memory-optimized configuration",
            )

        if bottlenecks.get("high_error_rate"):
            recommendations.append("Review input data quality and consider adding pre-validation")

        if bottlenecks.get("low_throughput"):
            recommendations.append("Enable batch processing or performance mode optimization")

        if bottlenecks.get("high_memory_usage"):
            recommendations.append("Enable memory optimization with __slots__ and pool allocation")

        if not bottlenecks:
            recommendations.append(
                "Pipeline performance is optimal. No immediate optimizations needed.",
            )

        return recommendations

    def get_optimization_summary(self) -> dict[str, Any]:
        """Get summary of optimization capabilities and status.

        Returns:
            Dictionary containing optimization status and capabilities
        """
        current_metrics = self.monitor.get_current_metrics()
        if current_metrics is None:
            current_metrics = PerformanceMetrics()

        return {
            "auto_tuning_enabled": self._auto_tuning_enabled,
            "tuning_interval_seconds": self._tuning_interval,
            "available_objectives": [obj.value for obj in OptimizationObjective],
            "available_contexts": [ctx.value for ctx in ConfigurationContext],
            "available_profiles": [prof.value for prof in PerformanceProfile],
            "monitor_stats": {
                "max_history": self.monitor.max_history,
                "current_metrics": current_metrics.to_dict(),
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
        "pipeline_tuning_demonstration",
        component="PipelineTuning",
        action="optimization_demo",
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
                "bottleneck_details",
                bottleneck=bottleneck,
                severity=details["severity"],
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
            logger.info("summary_item", key=key, value=value)

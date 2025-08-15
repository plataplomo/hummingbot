"""WebSocket pipeline tuning and optimization package.

This package provides comprehensive pipeline tuning and optimization capabilities
for WebSocket processing, including performance monitoring, automatic optimization,
and configuration management.

The package is decomposed into focused modules:

- performance_monitoring: Performance metrics collection and monitoring
- optimization_engine: Intelligent optimization and configuration tuning
- pipeline_tuning: High-level interface for pipeline tuning

## Usage

### Basic Pipeline Tuning

```python
from cyberdelta.apis.websocket.pipeline import (
    pipeline_tuner,
    OptimizationObjective,
)

# Tune pipeline for minimum latency
result = pipeline_tuner.tune_pipeline(
    model_type=MyModel,
    objective=OptimizationObjective.MINIMIZE_LATENCY,
    test_data=test_data,
)

print(f"Improvement: {result.improvement_percent}%")
```

### Performance Monitoring

```python
from cyberdelta.apis.websocket.pipeline import PerformanceMonitor

monitor = PerformanceMonitor()
monitor.start_monitoring()

# Record custom metrics
metrics = PerformanceMetrics(
    validation_time_ms=5.2,
    messages_per_second=250.0,
    error_rate_percent=0.1,
)
monitor.record_metrics(metrics)

# Analyze trends
trends = monitor.analyze_performance_trends()
```

### Custom Optimization

```python
from cyberdelta.apis.websocket.pipeline import (
    OptimizationEngine,
    PerformanceMonitor,
)

monitor = PerformanceMonitor()
optimizer = OptimizationEngine(monitor)

# Adaptive optimization based on current performance
current_metrics = monitor.get_current_metrics()
if current_metrics:
    result = optimizer.adaptive_optimization(
        model_type=MyModel, current_metrics=current_metrics
    )
    if result:
        print(f"Applied: {result.optimization_applied}")
```
"""

from __future__ import annotations

# Import main components from each module
from cyberdelta.apis.websocket.metrics.performance_monitoring import (
    CRITICAL_ERROR_RATE_PCT,
    CRITICAL_MEMORY_USAGE_MB,
    CRITICAL_PROCESSING_TIME_MS,
    CRITICAL_VALIDATION_TIME_MS,
    HIGH_ERROR_RATE_PCT,
    HIGH_MEMORY_USAGE_MB,
    HIGH_PROCESSING_TIME_MS,
    HIGH_VALIDATION_TIME_MS,
    LOW_THROUGHPUT_PER_SEC,
    OptimizationResult,
    PerformanceMetrics,
    PerformanceMonitor,
)

from .optimization_engine import OptimizationEngine
from .pipeline_tuning import (
    OptimizationObjective,
    PipelineTuner,
    analyze_pipeline_performance,
    pipeline_tuner,
)


__all__ = [
    "CRITICAL_ERROR_RATE_PCT",
    "CRITICAL_MEMORY_USAGE_MB",
    "CRITICAL_PROCESSING_TIME_MS",
    "CRITICAL_VALIDATION_TIME_MS",
    "HIGH_ERROR_RATE_PCT",
    "HIGH_MEMORY_USAGE_MB",
    "HIGH_PROCESSING_TIME_MS",
    "HIGH_VALIDATION_TIME_MS",
    "LOW_THROUGHPUT_PER_SEC",
    "OptimizationEngine",
    "OptimizationObjective",
    "OptimizationResult",
    "PerformanceMetrics",
    "PerformanceMonitor",
    "PipelineTuner",
    "analyze_pipeline_performance",
    "pipeline_tuner",
]

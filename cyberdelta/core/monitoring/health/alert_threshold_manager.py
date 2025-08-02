"""Alert threshold management service."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import Field
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class AlertThreshold:
    """Alert threshold configuration."""
    
    metric_name: str
    warning_threshold: float
    critical_threshold: float
    comparison_operator: str = Field(pattern="^(gt|lt|eq|gte|lte)$")  # gt, lt, eq, gte, lte
    enabled: bool = True
    suppress_duration_minutes: int = Field(default=5, ge=1)


class AlertThresholdManager:
    """Manages alert thresholds and threshold evaluations."""

    def __init__(self) -> None:
        """Initialize the alert threshold manager."""
        self._thresholds: dict[str, AlertThreshold] = {}
        self._setup_default_thresholds()
        
    def _setup_default_thresholds(self) -> None:
        """Setup default alert thresholds."""
        self._thresholds = {
            "cpu_usage_percent": AlertThreshold(
                metric_name="cpu_usage_percent",
                warning_threshold=80.0,
                critical_threshold=95.0,
                comparison_operator="gt"
            ),
            "memory_usage_percent": AlertThreshold(
                metric_name="memory_usage_percent", 
                warning_threshold=85.0,
                critical_threshold=95.0,
                comparison_operator="gt"
            ),
            "disk_usage_percent": AlertThreshold(
                metric_name="disk_usage_percent",
                warning_threshold=80.0,
                critical_threshold=90.0,
                comparison_operator="gt"
            ),
            "error_rate_percent": AlertThreshold(
                metric_name="error_rate_percent",
                warning_threshold=5.0,
                critical_threshold=10.0,
                comparison_operator="gt"
            ),
            "response_time_ms": AlertThreshold(
                metric_name="average_response_time_ms",
                warning_threshold=1000.0,
                critical_threshold=5000.0,
                comparison_operator="gt"
            )
        }

    def add_threshold(self, threshold: AlertThreshold) -> None:
        """Add or update an alert threshold.
        
        Args:
            threshold: Alert threshold configuration to add.
        """
        self._thresholds[threshold.metric_name] = threshold
        logger.info("Alert threshold updated", metric=threshold.metric_name)

    def remove_threshold(self, metric_name: str) -> bool:
        """Remove an alert threshold.
        
        Args:
            metric_name: Name of the metric threshold to remove.
            
        Returns:
            True if threshold was removed, False if not found.
        """
        if metric_name in self._thresholds:
            del self._thresholds[metric_name]
            logger.info("Alert threshold removed", metric=metric_name)
            return True
        return False

    def get_threshold(self, metric_name: str) -> AlertThreshold | None:
        """Get threshold configuration for a metric.
        
        Args:
            metric_name: Name of the metric.
            
        Returns:
            AlertThreshold if found, None otherwise.
        """
        return self._thresholds.get(metric_name)

    def evaluate_metric(self, metric_name: str, metric_value: float) -> AlertSeverity | None:
        """Evaluate if a metric value violates any thresholds.
        
        Args:
            metric_name: Name of the metric to check.
            metric_value: Current value of the metric.
            
        Returns:
            AlertSeverity if threshold is violated, None otherwise.
        """
        if metric_name not in self._thresholds:
            return None
            
        threshold = self._thresholds[metric_name]
        if not threshold.enabled:
            return None
            
        return self._evaluate_threshold(metric_value, threshold)

    def _evaluate_threshold(self, value: float, threshold: AlertThreshold) -> AlertSeverity | None:
        """Evaluate if a value violates threshold and determine severity.
        
        Args:
            value: Value to evaluate.
            threshold: Threshold configuration.
            
        Returns:
            AlertSeverity if threshold is violated, None if within limits.
        """
        op = threshold.comparison_operator
        
        # Check critical threshold first
        if self._compare_values(value, threshold.critical_threshold, op):
            return AlertSeverity.CRITICAL
            
        # Check warning threshold
        if self._compare_values(value, threshold.warning_threshold, op):
            return AlertSeverity.HIGH if op in {"gt", "gte"} else AlertSeverity.MEDIUM
            
        return None

    def _compare_values(self, value: float, threshold: float, operator: str) -> bool:
        """Compare values based on operator.
        
        Args:
            value: Value to compare.
            threshold: Threshold to compare against.
            operator: Comparison operator (gt, gte, lt, lte, eq).
            
        Returns:
            True if comparison succeeds, False otherwise.
        """
        if operator == "gt":
            return value > threshold
        if operator == "gte":
            return value >= threshold
        if operator == "lt":
            return value < threshold
        if operator == "lte":
            return value <= threshold
        if operator == "eq":
            float_tolerance = 0.001
            return abs(value - threshold) < float_tolerance
        return False

    def get_all_thresholds(self) -> dict[str, AlertThreshold]:
        """Get all configured thresholds.
        
        Returns:
            Dictionary of all alert thresholds by metric name.
        """
        return self._thresholds.copy()

    def get_threshold_summary(self) -> dict[str, Any]:
        """Get summary of threshold configuration.
        
        Returns:
            Dictionary containing threshold configuration summary.
        """
        enabled_count = sum(1 for t in self._thresholds.values() if t.enabled)
        disabled_count = len(self._thresholds) - enabled_count
        
        return {
            "total_thresholds": len(self._thresholds),
            "enabled_thresholds": enabled_count,
            "disabled_thresholds": disabled_count,
            "monitored_metrics": list(self._thresholds.keys()),
        }
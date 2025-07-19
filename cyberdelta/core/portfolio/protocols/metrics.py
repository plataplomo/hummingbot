"""Metrics collection protocols for portfolio monitoring."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable  
class MetricsCollectorProtocol(Protocol):
    """Protocol for metrics collection and recording.
    
    This protocol defines the interface for objects that can collect
    and record metrics for monitoring and observability.
    """
    
    def collect_metrics(self) -> dict[str, Any]:
        """Collect current metrics.
        
        Returns:
            Dictionary of metric names to values
        """
        ...
    
    def record_metric(self, name: str, value: float) -> None:
        """Record a single metric value.
        
        Args:
            name: Metric name
            value: Metric value
        """
        ...
    
    def record_metrics(self, metrics: dict[str, float | int]) -> None:
        """Record multiple metrics at once.
        
        Args:
            metrics: Dictionary of metric names to values
        """
        ...
    
    def get_metric(self, name: str) -> float | int | None:
        """Get a specific metric value.
        
        Args:
            name: Metric name
            
        Returns:
            Metric value or None if not found
        """
        ...
    
    def clear_metrics(self) -> None:
        """Clear all recorded metrics."""
        ...
    
    def get_metric_names(self) -> list[str]:
        """Get all metric names.
        
        Returns:
            List of metric names
        """
        ...
    
    def record_state_update(self, entity_type: str, exchange: str, operation: str | bool) -> None:
        """Record a state update metric.
        
        Args:
            entity_type: Type of entity updated
            exchange: Exchange identifier
            operation: Operation performed or success status
        """
        ...
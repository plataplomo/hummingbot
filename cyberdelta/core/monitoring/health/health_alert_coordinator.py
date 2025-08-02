"""Health alert coordination service."""

from __future__ import annotations

from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.infrastructure.services.base_service import BaseService
from cyberdelta.core.monitoring.health.alert_lifecycle_service import (
    AlertLifecycleService,
    HealthAlert,
)
from cyberdelta.core.monitoring.health.alert_threshold_manager import (
    AlertSeverity,
    AlertThreshold,
    AlertThresholdManager,
)


logger = get_logger(__name__)


class HealthAlertCoordinator(BaseService):
    """Coordinates alert threshold management and lifecycle operations."""

    def __init__(self, max_alerts: int = 100, auto_resolve_after_hours: int = 24) -> None:
        """Initialize the health alert coordinator.
        
        Args:
            max_alerts: Maximum number of alerts to keep in history.
            auto_resolve_after_hours: Hours after which to auto-resolve stale alerts.
        """
        super().__init__(name="HealthAlertCoordinator")
        
        # Initialize sub-services
        self.threshold_manager = AlertThresholdManager()
        self.lifecycle_service = AlertLifecycleService(max_alerts, auto_resolve_after_hours)

    async def _initialize_service(self) -> None:
        """Initialize the alert coordinator and sub-services."""
        await self.lifecycle_service.initialize()
        logger.info("Health alert coordinator initialized")

    async def _shutdown_service(self) -> None:
        """Shutdown the alert coordinator and sub-services."""
        await self.lifecycle_service.shutdown()
        logger.info("Health alert coordinator shutdown")

    # Threshold management methods
    def add_threshold(self, threshold: AlertThreshold) -> None:
        """Add or update an alert threshold.
        
        Args:
            threshold: Alert threshold configuration to add.
        """
        self.threshold_manager.add_threshold(threshold)

    def remove_threshold(self, metric_name: str) -> bool:
        """Remove an alert threshold.
        
        Args:
            metric_name: Name of the metric threshold to remove.
            
        Returns:
            True if threshold was removed, False if not found.
        """
        return self.threshold_manager.remove_threshold(metric_name)

    # Alert evaluation and creation
    def check_metric_thresholds(
        self, 
        metric_name: str, 
        metric_value: float, 
        component: str = "portfolio"
    ) -> HealthAlert | None:
        """Check if a metric value violates any thresholds and create alert if needed.
        
        Args:
            metric_name: Name of the metric to check.
            metric_value: Current value of the metric.
            component: Component name for alert identification.
            
        Returns:
            HealthAlert if threshold is violated and alert created, None otherwise.
        """
        severity = self.threshold_manager.evaluate_metric(metric_name, metric_value)
        if severity is None:
            return None
            
        # Get threshold for threshold value
        threshold = self.threshold_manager.get_threshold(metric_name)
        if threshold is None:
            return None
            
        threshold_value = (
            threshold.critical_threshold 
            if severity in {AlertSeverity.HIGH, AlertSeverity.CRITICAL} 
            else threshold.warning_threshold
        )
        
        return self.lifecycle_service.create_alert(
            severity=severity,
            metric_name=metric_name,
            metric_value=metric_value,
            threshold_value=threshold_value,
            component=component
        )

    # Alert lifecycle methods
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an active alert.
        
        Args:
            alert_id: ID of the alert to acknowledge.
            
        Returns:
            True if alert was acknowledged, False if not found.
        """
        return self.lifecycle_service.acknowledge_alert(alert_id)

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an active alert.
        
        Args:
            alert_id: ID of the alert to resolve.
            
        Returns:
            True if alert was resolved, False if not found.
        """
        return self.lifecycle_service.resolve_alert(alert_id)

    def suppress_alert(self, alert_id: str, minutes: int = 60) -> bool:
        """Suppress an alert for specified duration.
        
        Args:
            alert_id: ID of the alert to suppress.
            minutes: Duration in minutes to suppress the alert.
            
        Returns:
            True if alert was suppressed, False if not found.
        """
        return self.lifecycle_service.suppress_alert(alert_id, minutes)

    def get_active_alerts(self, severity: AlertSeverity | None = None) -> list[HealthAlert]:
        """Get all active alerts, optionally filtered by severity.
        
        Args:
            severity: Optional severity filter.
            
        Returns:
            List of active alerts, sorted by severity and creation time.
        """
        return self.lifecycle_service.get_active_alerts(severity)

    def get_alert_summary(self) -> dict[str, Any]:
        """Get summary of alert status and thresholds.
        
        Returns:
            Dictionary containing alert counts and threshold summary.
        """
        alert_summary = self.lifecycle_service.get_alert_summary()
        threshold_summary = self.threshold_manager.get_threshold_summary()
        
        return {
            **alert_summary,
            "thresholds": threshold_summary,
        }
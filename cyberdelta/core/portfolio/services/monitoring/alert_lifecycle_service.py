"""Alert lifecycle management service."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import Field
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService
from cyberdelta.core.portfolio.services.monitoring.alert_threshold_manager import AlertSeverity


logger = get_logger(__name__)


class AlertStatus(Enum):
    """Alert status states."""
    
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


@dataclass  
class HealthAlert:
    """Health alert with full lifecycle tracking."""
    
    severity: AlertSeverity
    metric_name: str
    metric_value: float
    threshold_value: float
    title: str
    description: str
    component: str
    alert_id: str = Field(default_factory=lambda: str(uuid4()))
    status: AlertStatus = AlertStatus.ACTIVE
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    acknowledged_at: datetime | None = None
    resolved_at: datetime | None = None
    suppressed_until: datetime | None = None
    occurrence_count: int = Field(default=1, ge=1)
    
    def acknowledge(self) -> None:
        """Acknowledge the alert."""
        if self.status == AlertStatus.ACTIVE:
            self.status = AlertStatus.ACKNOWLEDGED
            self.acknowledged_at = datetime.now(UTC)
    
    def resolve(self) -> None:
        """Resolve the alert."""
        self.status = AlertStatus.RESOLVED
        self.resolved_at = datetime.now(UTC)
    
    def suppress(self, minutes: int) -> None:
        """Suppress the alert for specified duration.
        
        Args:
            minutes: Duration in minutes to suppress the alert.
        """
        self.status = AlertStatus.SUPPRESSED
        self.suppressed_until = datetime.now(UTC) + timedelta(minutes=minutes)
    
    def is_suppressed(self) -> bool:
        """Check if alert is currently suppressed.
        
        Returns:
            True if alert is currently suppressed, False otherwise.
        """
        if self.status != AlertStatus.SUPPRESSED or self.suppressed_until is None:
            return False
        return datetime.now(UTC) < self.suppressed_until


class AlertLifecycleService(BasePortfolioService):
    """Service for managing alert lifecycle operations."""

    def __init__(self, max_alerts: int = 100, auto_resolve_after_hours: int = 24) -> None:
        """Initialize the alert lifecycle service.
        
        Args:
            max_alerts: Maximum number of alerts to keep in history.
            auto_resolve_after_hours: Hours after which to auto-resolve stale alerts.
        """
        super().__init__(name="AlertLifecycleService")
        self.max_alerts = max_alerts
        self.auto_resolve_after_hours = auto_resolve_after_hours
        
        # Alert storage
        self._active_alerts: dict[str, HealthAlert] = {}
        self._alert_history: list[HealthAlert] = []
        
        # Cleanup task
        self._cleanup_task: asyncio.Task[None] | None = None

    async def _initialize_service(self) -> None:
        """Initialize the alert lifecycle service."""
        await self.initialize()

    async def _shutdown_service(self) -> None:
        """Shutdown the alert lifecycle service."""
        await self.shutdown()

    async def initialize(self) -> None:
        """Initialize the alert lifecycle service."""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Alert lifecycle service initialized")

    async def shutdown(self) -> None:
        """Shutdown the alert lifecycle service."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task
        logger.info("Alert lifecycle service shutdown")

    async def _start_internal(self) -> None:
        """Start internal service operations."""
        await self.initialize()

    async def _stop_internal(self) -> None:
        """Stop internal service operations."""
        await self.shutdown()

    def create_alert(
        self,
        severity: AlertSeverity,
        metric_name: str,
        metric_value: float,
        threshold_value: float,
        component: str = "portfolio"
    ) -> HealthAlert:
        """Create a new health alert.
        
        Args:
            severity: Alert severity level.
            metric_name: Name of the metric that triggered the alert.
            metric_value: Current value of the metric.
            threshold_value: Threshold that was exceeded.
            component: Component name for alert identification.
            
        Returns:
            Created HealthAlert instance.
        """
        # Check if we already have an active alert for this metric
        existing_alert_id = f"{component}_{metric_name}"
        if existing_alert_id in self._active_alerts:
            existing_alert = self._active_alerts[existing_alert_id]
            if not existing_alert.is_suppressed():
                existing_alert.occurrence_count += 1
                return existing_alert
            return existing_alert  # Return suppressed alert without incrementing

        # Create new alert
        alert = HealthAlert(
            alert_id=existing_alert_id,
            severity=severity,
            metric_name=metric_name,
            metric_value=metric_value,
            threshold_value=threshold_value,
            title=f"{metric_name.replace('_', ' ').title()} {severity.value.upper()}",
            description=(
                f"{metric_name} is {metric_value:.2f}, exceeding {severity.value} threshold"
            ),
            component=component
        )
        
        self._active_alerts[existing_alert_id] = alert
        self._add_to_history(alert)
        
        logger.warning(
            "Health alert created",
            alert_id=alert.alert_id,
            severity=severity.value,
            metric=metric_name,
            value=metric_value,
            threshold=threshold_value
        )
        
        return alert

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an active alert.
        
        Args:
            alert_id: ID of the alert to acknowledge.
            
        Returns:
            True if alert was acknowledged, False if not found.
        """
        if alert_id in self._active_alerts:
            self._active_alerts[alert_id].acknowledge()
            logger.info("Alert acknowledged", alert_id=alert_id)
            return True
        return False

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an active alert.
        
        Args:
            alert_id: ID of the alert to resolve.
            
        Returns:
            True if alert was resolved, False if not found.
        """
        if alert_id in self._active_alerts:
            alert = self._active_alerts[alert_id]
            alert.resolve()
            del self._active_alerts[alert_id]
            logger.info("Alert resolved", alert_id=alert_id)
            return True
        return False

    def suppress_alert(self, alert_id: str, minutes: int = 60) -> bool:
        """Suppress an alert for specified duration.
        
        Args:
            alert_id: ID of the alert to suppress.
            minutes: Duration in minutes to suppress the alert.
            
        Returns:
            True if alert was suppressed, False if not found.
        """
        if alert_id in self._active_alerts:
            self._active_alerts[alert_id].suppress(minutes)
            logger.info("Alert suppressed", alert_id=alert_id, minutes=minutes)
            return True
        return False

    def get_active_alerts(self, severity: AlertSeverity | None = None) -> list[HealthAlert]:
        """Get all active alerts, optionally filtered by severity.
        
        Args:
            severity: Optional severity filter.
            
        Returns:
            List of active alerts, sorted by severity and creation time.
        """
        alerts = list(self._active_alerts.values())
        
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
            
        # Sort by severity and creation time
        severity_order = {
            AlertSeverity.CRITICAL: 0,
            AlertSeverity.HIGH: 1, 
            AlertSeverity.MEDIUM: 2,
            AlertSeverity.LOW: 3
        }
        
        alerts.sort(key=lambda a: (severity_order[a.severity], a.created_at))
        return alerts

    def get_alert_summary(self) -> dict[str, Any]:
        """Get summary of alert status.
        
        Returns:
            Dictionary containing alert counts and summary information.
        """
        active_alerts = self.get_active_alerts()
        
        counts_by_severity = {
            AlertSeverity.CRITICAL: 0,
            AlertSeverity.HIGH: 0,
            AlertSeverity.MEDIUM: 0,
            AlertSeverity.LOW: 0
        }
        
        for alert in active_alerts:
            counts_by_severity[alert.severity] += 1
            
        return {
            "total_active": len(active_alerts),
            "by_severity": {s.value: count for s, count in counts_by_severity.items()},
            "total_historical": len(self._alert_history),
        }

    def _add_to_history(self, alert: HealthAlert) -> None:
        """Add alert to history with size management.
        
        Args:
            alert: Alert to add to history.
        """
        self._alert_history.append(alert)
        
        # Keep history manageable
        if len(self._alert_history) > self.max_alerts * 2:
            self._alert_history = self._alert_history[-self.max_alerts:]

    async def _cleanup_loop(self) -> None:
        """Background task to clean up old alerts."""
        while True:
            try:
                await self._cleanup_old_alerts()
                await asyncio.sleep(3600)  # Run every hour
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Error in alert cleanup loop", error=str(e))
                await asyncio.sleep(3600)

    async def _cleanup_old_alerts(self) -> None:
        """Clean up old resolved alerts and auto-resolve stale alerts."""
        current_time = datetime.now(UTC)
        cutoff_time = current_time - timedelta(hours=self.auto_resolve_after_hours)
        
        # Auto-resolve very old active alerts
        alerts_to_resolve = []
        for alert_id, alert in self._active_alerts.items():
            if alert.created_at < cutoff_time:
                alerts_to_resolve.append(alert_id)
        
        for alert_id in alerts_to_resolve:
            self.resolve_alert(alert_id)
            logger.info("Auto-resolved stale alert", alert_id=alert_id)
        
        # Clean up suppressed alerts that should be reactivated
        for alert in self._active_alerts.values():
            if alert.status == AlertStatus.SUPPRESSED and not alert.is_suppressed():
                alert.status = AlertStatus.ACTIVE
                logger.info("Reactivated suppressed alert", alert_id=alert.alert_id)
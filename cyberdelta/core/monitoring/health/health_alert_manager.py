"""Health alert management service for portfolio monitoring alerts and notifications."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import Field
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.infrastructure.services.base_service import BaseService


@dataclass
class HealthAlert:
    """Health alert for portfolio monitoring."""
    alert_id: str
    alert_type: str
    severity: str  # "critical", "warning", "info"
    title: str
    message: str
    source_component: str
    details: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    resolved: bool = Field(default=False)
    resolution_time: datetime | None = Field(default=None)
    escalation_level: int = Field(default=0, ge=0, le=5)
    auto_resolve: bool = Field(default=False)


@dataclass  
class AlertRule:
    """Rule for alert generation and management."""
    rule_id: str
    name: str
    condition: str
    threshold_value: float
    severity: str
    enabled: bool = Field(default=True)
    auto_resolve: bool = Field(default=False)
    escalation_rules: list[dict[str, Any]] = Field(default_factory=list)


class HealthAlertManager(BaseService):
    """Manages health alerts for portfolio monitoring system."""

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__("health_alert_manager", config)
        self._raw_config = config or {}
        self.logger = get_logger(__name__)
        
        # Alert storage
        self.active_alerts: dict[str, HealthAlert] = {}
        self.alert_history: list[HealthAlert] = []
        self.alert_rules: dict[str, AlertRule] = {}
        
        # Configuration
        self.max_active_alerts = self._raw_config.get("max_active_alerts", 100)
        self.alert_history_days = self._raw_config.get("alert_history_days", 30)
        self.escalation_enabled = self._raw_config.get("escalation_enabled", True)
        
        # Alert counters
        self.alert_counters: dict[str, dict[str, int]] = {
            "total": {},
            "by_severity": {"critical": 0, "warning": 0, "info": 0},
            "by_component": {},
            "by_type": {}
        }

    async def _initialize_service(self) -> None:
        """Initialize health alert manager."""
        self.logger.info("Initializing health alert manager")
        await self._load_alert_rules()
        await self._load_alert_history()

    async def _shutdown_service(self) -> None:
        """Shutdown health alert manager."""
        self.logger.info("Shutting down health alert manager")
        await self._save_alert_data()

    async def _load_alert_rules(self) -> None:
        """Load alert rules configuration."""
        # Default alert rules
        default_rules = [
            AlertRule(
                rule_id="high_cpu_usage",
                name="High CPU Usage",
                condition="cpu_usage > threshold",
                threshold_value=80.0,
                severity="warning",
                auto_resolve=True
            ),
            AlertRule(
                rule_id="critical_cpu_usage", 
                name="Critical CPU Usage",
                condition="cpu_usage > threshold",
                threshold_value=95.0,
                severity="critical",
                escalation_rules=[{"level": 1, "delay_minutes": 5}]
            ),
            AlertRule(
                rule_id="high_memory_usage",
                name="High Memory Usage", 
                condition="memory_usage > threshold",
                threshold_value=85.0,
                severity="warning",
                auto_resolve=True
            ),
            AlertRule(
                rule_id="negative_balance",
                name="Negative Balance Detected",
                condition="balance < 0",
                threshold_value=0.0,
                severity="critical"
            ),
            AlertRule(
                rule_id="stale_data",
                name="Stale Portfolio Data",
                condition="data_age > threshold",
                threshold_value=3600.0,  # 1 hour in seconds
                severity="warning",
                auto_resolve=True
            )
        ]
        
        for rule in default_rules:
            self.alert_rules[rule.rule_id] = rule

    async def _load_alert_history(self) -> None:
        """Load alert history from persistence."""
        # Placeholder - would load from database/file
        self.logger.debug("Loading alert history")

    async def _save_alert_data(self) -> None:
        """Save alert data to persistence."""
        # Placeholder - would save to database/file
        self.logger.debug("Saving alert data")

    async def create_alert(
        self,
        alert_type: str,
        severity: str,
        title: str,
        message: str,
        source_component: str,
        details: dict[str, Any] | None = None
    ) -> str:
        """Create a new health alert."""
        alert_id = f"{alert_type}_{source_component}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        
        alert = HealthAlert(
            alert_id=alert_id,
            alert_type=alert_type,
            severity=severity,
            title=title,
            message=message,
            source_component=source_component,
            details=details or {},
            timestamp=datetime.now(UTC)
        )
        
        # Check if similar alert already exists
        existing_alert = self._find_similar_alert(alert)
        if existing_alert:
            self.logger.debug(f"Similar alert already exists: {existing_alert.alert_id}")
            return existing_alert.alert_id
        
        # Add to active alerts
        self.active_alerts[alert_id] = alert
        
        # Update counters
        self._update_alert_counters(alert, increment=True)
        
        # Check for escalation
        if self.escalation_enabled:
            await self._check_escalation(alert)
        
        self.logger.info(f"Created alert: {alert_id} - {title}")
        
        # Trim active alerts if needed
        await self._trim_active_alerts()
        
        return alert_id

    async def resolve_alert(self, alert_id: str, resolution_message: str | None = None) -> bool:
        """Resolve an active alert."""
        if alert_id not in self.active_alerts:
            self.logger.warning(f"Alert not found: {alert_id}")
            return False
        
        alert = self.active_alerts[alert_id]
        alert.resolved = True
        alert.resolution_time = datetime.now(UTC)
        
        # Move to history
        self.alert_history.append(alert)
        del self.active_alerts[alert_id]
        
        # Update counters
        self._update_alert_counters(alert, increment=False)
        
        self.logger.info(f"Resolved alert: {alert_id}")
        
        # Cleanup old history
        await self._cleanup_old_alerts()
        
        return True

    async def resolve_alerts_by_type(self, alert_type: str, source_component: str | None = None) -> int:
        """Resolve all alerts of a specific type."""
        resolved_count = 0
        alerts_to_resolve = []
        
        for alert in self.active_alerts.values():
            if alert.alert_type == alert_type:
                if source_component is None or alert.source_component == source_component:
                    alerts_to_resolve.append(alert.alert_id)
        
        for alert_id in alerts_to_resolve:
            if await self.resolve_alert(alert_id):
                resolved_count += 1
        
        return resolved_count

    async def get_active_alerts(
        self, 
        severity: str | None = None, 
        component: str | None = None
    ) -> list[HealthAlert]:
        """Get active alerts, optionally filtered."""
        alerts = list(self.active_alerts.values())
        
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        
        if component:
            alerts = [a for a in alerts if a.source_component == component]
        
        # Sort by timestamp (newest first)
        alerts.sort(key=lambda x: x.timestamp, reverse=True)
        
        return alerts

    async def get_alert_history(
        self,
        hours: int = 24,
        severity: str | None = None
    ) -> list[HealthAlert]:
        """Get alert history for specified period."""
        cutoff_time = datetime.now(UTC) - timedelta(hours=hours)
        
        history = [a for a in self.alert_history if a.timestamp >= cutoff_time]
        
        if severity:
            history = [a for a in history if a.severity == severity]
        
        # Sort by timestamp (newest first)
        history.sort(key=lambda x: x.timestamp, reverse=True)
        
        return history

    async def get_alert_summary(self) -> dict[str, Any]:
        """Get summary of alert statistics."""
        return {
            "active_alerts": len(self.active_alerts),
            "alerts_by_severity": {
                "critical": len([a for a in self.active_alerts.values() if a.severity == "critical"]),
                "warning": len([a for a in self.active_alerts.values() if a.severity == "warning"]),
                "info": len([a for a in self.active_alerts.values() if a.severity == "info"])
            },
            "alerts_by_component": self._get_alerts_by_component(),
            "total_counters": self.alert_counters,
            "oldest_active_alert": self._get_oldest_active_alert(),
            "alert_rules_count": len(self.alert_rules),
            "escalation_enabled": self.escalation_enabled
        }

    def _find_similar_alert(self, new_alert: HealthAlert) -> HealthAlert | None:
        """Find similar existing alert to prevent duplicates."""
        for alert in self.active_alerts.values():
            if (alert.alert_type == new_alert.alert_type and
                alert.source_component == new_alert.source_component and
                alert.severity == new_alert.severity):
                # Check if alerts are within 5 minutes of each other
                time_diff = abs((new_alert.timestamp - alert.timestamp).total_seconds())
                if time_diff < 300:  # 5 minutes
                    return alert
        return None

    def _update_alert_counters(self, alert: HealthAlert, increment: bool) -> None:
        """Update alert counters."""
        delta = 1 if increment else -1
        
        # By severity
        if alert.severity in self.alert_counters["by_severity"]:
            self.alert_counters["by_severity"][alert.severity] += delta
        
        # By component
        if alert.source_component not in self.alert_counters["by_component"]:
            self.alert_counters["by_component"][alert.source_component] = 0
        self.alert_counters["by_component"][alert.source_component] += delta
        
        # By type
        if alert.alert_type not in self.alert_counters["by_type"]:
            self.alert_counters["by_type"][alert.alert_type] = 0
        self.alert_counters["by_type"][alert.alert_type] += delta

    async def _check_escalation(self, alert: HealthAlert) -> None:
        """Check if alert needs escalation."""
        rule = self.alert_rules.get(alert.alert_type)
        if not rule or not rule.escalation_rules:
            return
        
        # Implement escalation logic based on rules
        for escalation_rule in rule.escalation_rules:
            level = escalation_rule.get("level", 0)
            delay_minutes = escalation_rule.get("delay_minutes", 5)
            
            # This would schedule escalation notifications
            self.logger.debug(f"Escalation rule for {alert.alert_id}: level {level} after {delay_minutes} minutes")

    async def _trim_active_alerts(self) -> None:
        """Trim active alerts if exceeding maximum."""
        if len(self.active_alerts) <= self.max_active_alerts:
            return
        
        # Sort by timestamp and keep newest
        sorted_alerts = sorted(
            self.active_alerts.items(),
            key=lambda x: x[1].timestamp,
            reverse=True
        )
        
        # Move oldest to history
        alerts_to_remove = sorted_alerts[self.max_active_alerts:]
        for alert_id, alert in alerts_to_remove:
            alert.resolved = True
            alert.resolution_time = datetime.now(UTC)
            self.alert_history.append(alert)
            del self.active_alerts[alert_id]
            self._update_alert_counters(alert, increment=False)

    async def _cleanup_old_alerts(self) -> None:
        """Remove old alerts from history."""
        cutoff_time = datetime.now(UTC) - timedelta(days=self.alert_history_days)
        self.alert_history = [a for a in self.alert_history if a.timestamp >= cutoff_time]

    def _get_alerts_by_component(self) -> dict[str, int]:
        """Get count of active alerts by component."""
        component_counts: dict[str, int] = {}
        for alert in self.active_alerts.values():
            component = alert.source_component
            component_counts[component] = component_counts.get(component, 0) + 1
        return component_counts

    def _get_oldest_active_alert(self) -> dict[str, Any] | None:
        """Get information about the oldest active alert."""
        if not self.active_alerts:
            return None
        
        oldest_alert = min(self.active_alerts.values(), key=lambda x: x.timestamp)
        age_seconds = (datetime.now(UTC) - oldest_alert.timestamp).total_seconds()
        
        return {
            "alert_id": oldest_alert.alert_id,
            "alert_type": oldest_alert.alert_type,
            "severity": oldest_alert.severity,
            "age_seconds": age_seconds,
            "age_hours": age_seconds / 3600,
            "source_component": oldest_alert.source_component
        }
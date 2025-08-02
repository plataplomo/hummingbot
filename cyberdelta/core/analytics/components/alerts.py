"""Alert management component for portfolio analytics."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from typing import Dict, List, Any, Optional
from enum import Enum

from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertType(str, Enum):
    """Alert types."""
    MAX_DRAWDOWN_EXCEEDED = "max_drawdown_exceeded"
    DAILY_LOSS_EXCEEDED = "daily_loss_exceeded"
    POSITION_CONCENTRATION = "position_concentration"
    LEVERAGE_LIMIT_EXCEEDED = "leverage_limit_exceeded"
    RISK_LIMIT_BREACH = "risk_limit_breach"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    SYSTEM_ERROR = "system_error"


class AlertManager:
    """Manager for analytics alerts and notifications."""
    
    def __init__(self, thresholds: Optional[Dict[str, Decimal]] = None) -> None:
        """Initialize alert manager.
        
        Args:
            thresholds: Alert thresholds configuration
        """
        self.thresholds = thresholds or {}
        self._initialized = False
        self._alert_history: List[Dict[str, Any]] = []
        self._active_alerts: Dict[str, Dict[str, Any]] = {}
        self._alert_handlers: Dict[AlertType, List[Any]] = {}
        
    async def initialize(self) -> None:
        """Initialize the alert manager."""
        if self._initialized:
            return
            
        logger.info("Initializing alert manager")
        
        # Set default thresholds if not provided
        self._set_default_thresholds()
        
        # Register default handlers
        self._register_default_handlers()
        
        self._initialized = True
        
    async def shutdown(self) -> None:
        """Shutdown the alert manager."""
        if not self._initialized:
            return
            
        logger.info("Shutting down alert manager")
        
        # Clear active alerts
        self._active_alerts.clear()
        
        self._initialized = False
        
    async def process_alert(self, alert: Dict[str, Any]) -> None:
        """Process a new alert.
        
        Args:
            alert: Alert data including type, severity, value, etc.
        """
        alert_id = self._generate_alert_id(alert)
        alert["id"] = alert_id
        alert["processed_at"] = datetime.now(UTC)
        
        # Log the alert
        logger.warning(
            "Alert triggered",
            alert_type=alert.get("type"),
            severity=alert.get("severity"),
            value=str(alert.get("value")),
            threshold=str(alert.get("threshold"))
        )
        
        # Add to history
        self._alert_history.append(alert)
        
        # Add to active alerts
        self._active_alerts[alert_id] = alert
        
        # Execute handlers
        await self._execute_handlers(alert)
        
        # Check if alert needs escalation
        if self._needs_escalation(alert):
            await self._escalate_alert(alert)
            
    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an active alert.
        
        Args:
            alert_id: ID of alert to acknowledge
            acknowledged_by: User acknowledging the alert
            
        Returns:
            True if acknowledged successfully
        """
        if alert_id not in self._active_alerts:
            return False
            
        alert = self._active_alerts[alert_id]
        alert["acknowledged"] = True
        alert["acknowledged_by"] = acknowledged_by
        alert["acknowledged_at"] = datetime.now(UTC)
        
        logger.info(
            "Alert acknowledged",
            alert_id=alert_id,
            acknowledged_by=acknowledged_by
        )
        
        return True
        
    async def resolve_alert(self, alert_id: str, resolution: str) -> bool:
        """Resolve an active alert.
        
        Args:
            alert_id: ID of alert to resolve
            resolution: Resolution description
            
        Returns:
            True if resolved successfully
        """
        if alert_id not in self._active_alerts:
            return False
            
        alert = self._active_alerts.pop(alert_id)
        alert["resolved"] = True
        alert["resolution"] = resolution
        alert["resolved_at"] = datetime.now(UTC)
        
        # Keep in history
        self._alert_history.append(alert)
        
        logger.info(
            "Alert resolved",
            alert_id=alert_id,
            resolution=resolution
        )
        
        return True
        
    def get_active_alerts(self, severity: Optional[AlertSeverity] = None) -> List[Dict[str, Any]]:
        """Get active alerts, optionally filtered by severity.
        
        Args:
            severity: Optional severity filter
            
        Returns:
            List of active alerts
        """
        alerts = list(self._active_alerts.values())
        
        if severity:
            alerts = [a for a in alerts if a.get("severity") == severity]
            
        return alerts
        
    def get_alert_history(
        self, 
        hours: int = 24,
        alert_type: Optional[AlertType] = None
    ) -> List[Dict[str, Any]]:
        """Get alert history for specified period.
        
        Args:
            hours: Number of hours to look back
            alert_type: Optional alert type filter
            
        Returns:
            List of historical alerts
        """
        cutoff = datetime.now(UTC) - timedelta(hours=hours)
        
        history = [
            a for a in self._alert_history
            if a.get("timestamp", datetime.min) > cutoff
        ]
        
        if alert_type:
            history = [h for h in history if h.get("type") == alert_type]
            
        return history
        
    def register_handler(self, alert_type: AlertType, handler: Any) -> None:
        """Register a handler for specific alert type.
        
        Args:
            alert_type: Type of alert to handle
            handler: Handler function/object
        """
        if alert_type not in self._alert_handlers:
            self._alert_handlers[alert_type] = []
            
        self._alert_handlers[alert_type].append(handler)
        
    def update_threshold(self, threshold_name: str, value: Decimal) -> None:
        """Update an alert threshold.
        
        Args:
            threshold_name: Name of threshold to update
            value: New threshold value
        """
        self.thresholds[threshold_name] = value
        logger.info(
            "Alert threshold updated",
            threshold=threshold_name,
            value=str(value)
        )
        
    def _set_default_thresholds(self) -> None:
        """Set default alert thresholds."""
        defaults = {
            "max_drawdown": Decimal("0.15"),  # 15%
            "daily_loss": Decimal("0.05"),     # 5%
            "position_concentration": Decimal("0.25"),  # 25%
            "leverage_limit": Decimal("3.0"),   # 3x
            "var_95": Decimal("0.1"),          # 10% VaR
            "sharpe_degradation": Decimal("0.5"),  # 50% Sharpe reduction
        }
        
        for key, value in defaults.items():
            if key not in self.thresholds:
                self.thresholds[key] = value
                
    def _register_default_handlers(self) -> None:
        """Register default alert handlers."""
        # Would register actual handlers like email, slack, etc.
        pass
        
    def _generate_alert_id(self, alert: Dict[str, Any]) -> str:
        """Generate unique alert ID."""
        timestamp = datetime.now(UTC).timestamp()
        alert_type = alert.get("type", "unknown")
        return f"{alert_type}_{int(timestamp * 1000)}"
        
    async def _execute_handlers(self, alert: Dict[str, Any]) -> None:
        """Execute registered handlers for alert."""
        alert_type = alert.get("type")
        
        if alert_type in self._alert_handlers:
            for handler in self._alert_handlers[alert_type]:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(alert)
                    else:
                        handler(alert)
                except Exception as e:
                    logger.error(
                        "Alert handler error",
                        handler=str(handler),
                        error=str(e)
                    )
                    
    def _needs_escalation(self, alert: Dict[str, Any]) -> bool:
        """Check if alert needs escalation."""
        severity = alert.get("severity")
        
        # Escalate critical and high severity alerts
        return severity in [AlertSeverity.CRITICAL, AlertSeverity.HIGH]
        
    async def _escalate_alert(self, alert: Dict[str, Any]) -> None:
        """Escalate an alert."""
        logger.critical(
            "ALERT ESCALATION",
            alert_type=alert.get("type"),
            severity=alert.get("severity"),
            value=str(alert.get("value")),
            threshold=str(alert.get("threshold")),
            message="Alert requires immediate attention"
        )
        
        # Would trigger additional escalation actions like:
        # - Send emergency notifications
        # - Trigger automatic risk reduction
        # - Notify operations team
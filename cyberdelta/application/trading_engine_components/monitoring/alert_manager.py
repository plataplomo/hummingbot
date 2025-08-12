"""Alert manager for trading engine alert operations."""

from __future__ import annotations

from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.monitoring.alert_service import AlertService
from cyberdelta.enums.monitoring import AlertLevel


logger = get_logger(__name__)


class AlertManager:
    """Manages alert operations for the trading engine."""

    def __init__(self, alert_service: AlertService) -> None:
        """Initialize alert manager with alert service.

        Args:
            alert_service: Alert service instance
        """
        self._alert_service = alert_service

    async def create_alert(
        self,
        title: str,
        description: str,
        level: str,
        source: str = "trading_engine",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Create an alert through the alert service.

        Args:
            title: Alert title
            description: Alert description
            level: Alert level ("info", "warning", "error", "critical")
            source: Alert source
            metadata: Additional alert metadata

        Returns:
            Alert summary if created, None if suppressed/disabled

        IMPORTANT: Following CODING_STANDARDS.md:
        - Converts string level to AlertLevel enum
        - Returns structured alert data
        """
        try:
            alert_level = AlertLevel(level.lower())
        except ValueError:
            logger.warning(
                "invalid_alert_level", level=level, valid_levels=[lv.value for lv in AlertLevel]
            )
            return None

        alert = await self._alert_service.create_alert(
            title=title,
            description=description,
            level=alert_level,
            source=source,
            metadata=metadata,
        )

        if alert:
            return {
                "alert_id": alert.alert_id,
                "title": alert.title,
                "level": alert.level.value,
                "status": alert.status.value,
                "timestamp": alert.timestamp.isoformat(),
                "channels_notified": alert.channels_notified,
            }

        return None

    async def acknowledge_alert(
        self, alert_id: str, acknowledged_by: str = "trading_engine"
    ) -> bool:
        """Acknowledge an active alert.

        Args:
            alert_id: ID of alert to acknowledge
            acknowledged_by: Who acknowledged the alert

        Returns:
            True if acknowledgment successful
        """
        return await self._alert_service.acknowledge_alert(alert_id, acknowledged_by)

    async def resolve_alert(self, alert_id: str, resolved_by: str = "trading_engine") -> bool:
        """Resolve an active alert.

        Args:
            alert_id: ID of alert to resolve
            resolved_by: Who resolved the alert

        Returns:
            True if resolution successful
        """
        return await self._alert_service.resolve_alert(alert_id, resolved_by)

    def get_active_alerts(self) -> list[dict[str, Any]]:
        """Get list of currently active alerts.

        Returns:
            List of active alert summaries
        """
        return self._alert_service.get_active_alerts()

    def get_alert_service_status(self) -> dict[str, Any]:
        """Get alert service status and statistics.

        Returns:
            Alert service status information
        """
        return self._alert_service.get_alert_stats()

"""Health manager for trading engine system health monitoring."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.monitoring.alert_service import AlertService
from cyberdelta.domain.monitoring.service_health_monitor import ServiceHealthMonitor
from cyberdelta.enums.monitoring import AlertLevel
from cyberdelta.models.monitoring.system_health_models import HealthStatus


if TYPE_CHECKING:
    pass


logger = get_logger(__name__)


class HealthManager:
    """Manages system health monitoring and reporting."""

    def __init__(
        self,
        config: AppSettings,
        health_monitor: ServiceHealthMonitor,
        alert_service: AlertService,
    ) -> None:
        """Initialize health manager with required dependencies.

        Args:
            config: Application settings
            health_monitor: Health monitoring system
            alert_service: Alert service
        """
        self.config = config
        self._health_monitor = health_monitor
        self._alert_service = alert_service

    async def perform_health_checks(self) -> None:
        """Perform health checks on all services using health monitor.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured health check thresholds via health monitor
        - NO assumptions about service availability
        - Delegates to centralized health monitoring system
        """
        try:
            # Use health monitor for comprehensive health checking
            health_report = await self._health_monitor.get_system_health()

            logger.debug(
                "health_check_completed",
                overall_status=health_report.overall_health_status,
                services_checked=len(health_report.service_statuses),
                alerts_triggered=len(health_report.active_alerts or []),
            )

            # Log individual service health
            for check in health_report.service_statuses.values():
                if check.health_status.value in {"degraded", "unhealthy", "critical"}:
                    logger.warning(
                        "service_health_issue",
                        service_name=check.service_name,
                        service_type=check.service_type.value,
                        status=check.health_status.value,
                        response_time_ms=float(check.response_time_ms),
                    )

            # Log any system-level alerts
            if health_report.active_alerts:
                for alert in health_report.active_alerts:
                    logger.warning(
                        "system_health_alert",
                        alert_message=alert,
                        overall_status=health_report.overall_health_status,
                    )

            # Check for critical system health issues and create alerts
            if health_report.overall_health_status in {
                HealthStatus.CRITICAL,
                HealthStatus.UNHEALTHY,
            }:
                critical_services = [
                    check.service_name
                    for check in health_report.service_statuses.values()
                    if check.health_status == HealthStatus.CRITICAL
                ]

                logger.error(
                    "system_health_critical",
                    overall_status=health_report.overall_health_status,
                    critical_services=critical_services,
                )

                # Create critical health alert
                await self._alert_service.create_alert(
                    title=f"System Health {health_report.overall_health_status.value.upper()}",
                    description=f"System health is {health_report.overall_health_status.value}. "
                    f"Critical services: "
                    f"{', '.join(critical_services) if critical_services else 'None'}",
                    level=AlertLevel.CRITICAL
                    if health_report.overall_health_status == HealthStatus.CRITICAL
                    else AlertLevel.ERROR,
                    source="health_monitor",
                    metadata={
                        "overall_status": health_report.overall_health_status.value,
                        "critical_services": critical_services,
                        "total_services": len(health_report.service_statuses),
                        "alerts_triggered": health_report.active_alerts,
                    },
                )

        except Exception as e:
            logger.exception("health_check_error", error=str(e))

    async def get_system_health_report(self) -> dict[str, Any]:
        """Get comprehensive system health report from health monitor.

        Returns:
            System health report with all service checks

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns structured health data
        - Uses health monitor for accurate status
        """
        try:
            health_report = await self._health_monitor.get_system_health()
            return {
                "overall_status": health_report.overall_health_status.value,
                "timestamp": health_report.report_timestamp.isoformat(),
                "service_checks": [
                    {
                        "service_name": check.service_name,
                        "service_type": check.service_type.value,
                        "status": check.health_status.value,
                        "response_time_ms": float(check.response_time_ms),
                        "timestamp": check.last_check_timestamp.isoformat(),
                    }
                    for _, check in health_report.service_statuses.items()
                ],
                "system_metrics": health_report.system_metrics,
                "alerts_triggered": health_report.active_alerts,
                "configuration": health_report.monitoring_configuration,
            }
        except Exception as e:
            logger.exception("get_system_health_report_error", error=str(e))
            return {
                "overall_status": "unknown",
                "error": str(e),
                "timestamp": datetime.now(UTC).isoformat(),
            }

    def get_health_monitor_status(self) -> dict[str, Any]:
        """Get health monitor status and configuration.

        Returns:
            Health monitor status information

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns explicit status information
        - Configuration-driven reporting
        """
        return {
            # Monitoring is enabled based on configuration
            "enabled": self.config.monitoring.notifications_enabled,
            "running": self._health_monitor.is_running(),
            "registered_services": self._health_monitor.get_registered_services(),
            "configuration": {
                "check_interval_sec": float(self.config.monitoring.health_check_interval_seconds),
                "response_time_threshold_ms": float(
                    self.config.monitoring.response_time_threshold_ms
                ),
                "error_rate_threshold": float(self.config.monitoring.error_rate_threshold),
                "stale_data_threshold_sec": float(
                    self.config.monitoring.stale_data_threshold_seconds
                ),
            },
        }

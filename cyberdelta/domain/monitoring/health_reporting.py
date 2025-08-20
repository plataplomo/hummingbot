"""Health status reporting and aggregation.

This module contains reporting logic extracted from ServiceHealthMonitor
to maintain file size under 600 lines while keeping the same business logic.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.monitoring import HealthStatus
from cyberdelta.models.monitoring.system_health_models import (
    MonitoringConfiguration,
    ServiceHealthStatus,
    SystemHealthReport,
    SystemMetrics,
)


# Constants for alert thresholds
MULTIPLE_UNHEALTHY_SERVICES_THRESHOLD = 2


if TYPE_CHECKING:
    from cyberdelta.config.models import AppSettings
    from cyberdelta.domain.monitoring.health_metrics import HealthMetricsCollector

logger = get_logger(__name__)


class HealthReporter:
    """Health status reporting and aggregation.

    This class contains the reporting logic extracted from ServiceHealthMonitor
    to maintain file size limits while preserving exact business logic.
    """

    def __init__(
        self,
        config: AppSettings,
        metrics_collector: HealthMetricsCollector,
    ) -> None:
        """Initialize health reporter.

        Args:
            config: Application settings
            metrics_collector: Metrics collector instance
        """
        self.config = config
        self._monitoring_config = config.monitoring
        self._metrics_collector = metrics_collector

        # Cache for last health check results
        self._last_health_checks: dict[str, ServiceHealthStatus] = {}

        # Alert tracking
        self._active_alerts: list[str] = []
        self._alert_history: list[tuple[datetime, str, HealthStatus]] = []

    async def generate_system_health_report(
        self,
        service_checks: list[ServiceHealthStatus],
    ) -> SystemHealthReport:
        """Generate comprehensive system health report.

        Args:
            service_checks: List of individual service health checks

        Returns:
            SystemHealthReport with aggregated health information
        """
        # Collect system metrics
        system_metrics = await self._metrics_collector.collect_system_metrics()

        # Determine overall status
        overall_status = self.determine_overall_status(service_checks)

        # Count services by status
        healthy_count = sum(1 for s in service_checks if s.health_status == HealthStatus.HEALTHY)
        degraded_count = sum(1 for s in service_checks if s.health_status == HealthStatus.DEGRADED)
        unhealthy_count = sum(
            1 for s in service_checks if s.health_status == HealthStatus.UNHEALTHY
        )
        critical_count = sum(1 for s in service_checks if s.health_status == HealthStatus.CRITICAL)

        # Check for alerts
        alerts = self.check_alerts(service_checks, system_metrics)

        # Create monitoring configuration
        monitoring_config = MonitoringConfiguration(
            monitoring_enabled=True,
            check_interval_sec=self._monitoring_config.health_check_interval_seconds,
            total_services=len(service_checks),
            thresholds=self._metrics_collector.create_operational_thresholds(),
        )

        # Create report
        report = SystemHealthReport(
            report_timestamp=datetime.now(UTC),
            overall_health_status=overall_status,
            total_services_monitored=len(service_checks),
            healthy_services_count=healthy_count,
            unhealthy_services_count=unhealthy_count + critical_count + degraded_count,
            service_statuses={s.service_name: s for s in service_checks},
            system_metrics=system_metrics,
            monitoring_configuration=monitoring_config,
            active_alerts=alerts,
        )

        logger.info(
            "system_health_report_generated",
            overall_status=overall_status.value,
            service_count=len(service_checks),
            healthy=healthy_count,
            degraded=degraded_count,
            unhealthy=unhealthy_count,
            critical=critical_count,
            alert_count=len(alerts),
        )

        return report

    def determine_overall_status(self, service_checks: list[ServiceHealthStatus]) -> HealthStatus:
        """Determine overall system health status.

        Args:
            service_checks: List of service health checks

        Returns:
            Overall health status

        Logic:
        - CRITICAL if any service is critical
        - UNHEALTHY if any service is unhealthy
        - DEGRADED if any service is degraded
        - HEALTHY if all services are healthy
        """
        if not service_checks:
            return HealthStatus.UNKNOWN

        # Check for critical services
        if any(s.health_status == HealthStatus.CRITICAL for s in service_checks):
            return HealthStatus.CRITICAL

        # Check for unhealthy services
        if any(s.health_status == HealthStatus.UNHEALTHY for s in service_checks):
            return HealthStatus.UNHEALTHY

        # Check for degraded services
        if any(s.health_status == HealthStatus.DEGRADED for s in service_checks):
            return HealthStatus.DEGRADED

        # All services healthy
        return HealthStatus.HEALTHY

    def check_alerts(
        self,
        service_checks: list[ServiceHealthStatus],
        system_metrics: SystemMetrics,
    ) -> list[str]:
        """Check for alert conditions.

        Args:
            service_checks: Service health checks
            system_metrics: System metrics

        Returns:
            List of alert messages
        """
        alerts: list[str] = []

        # Check different alert conditions
        self._check_critical_services(service_checks, alerts)
        self._check_unhealthy_services(service_checks, alerts)
        self._check_system_resources(system_metrics, alerts)
        self._check_error_rates(service_checks, alerts)

        return alerts

    def _check_critical_services(
        self, service_checks: list[ServiceHealthStatus], alerts: list[str]
    ) -> None:
        """Check for critical services and add alerts.

        Args:
            service_checks: Service health checks
            alerts: List to append alerts to
        """
        critical_services = [s for s in service_checks if s.health_status == HealthStatus.CRITICAL]
        if critical_services:
            for service in critical_services:
                alert = f"CRITICAL: Service '{service.service_name}' is in critical state"
                if alert not in alerts:
                    alerts.append(alert)
                self._record_alert(alert, HealthStatus.CRITICAL)

    def _check_unhealthy_services(
        self, service_checks: list[ServiceHealthStatus], alerts: list[str]
    ) -> None:
        """Check for multiple unhealthy services and add alerts.

        Args:
            service_checks: Service health checks
            alerts: List to append alerts to
        """
        unhealthy_services = [
            s for s in service_checks if s.health_status == HealthStatus.UNHEALTHY
        ]
        if len(unhealthy_services) >= MULTIPLE_UNHEALTHY_SERVICES_THRESHOLD:
            alert = f"WARNING: {len(unhealthy_services)} services are unhealthy"
            if alert not in alerts:
                alerts.append(alert)
            self._record_alert(alert, HealthStatus.UNHEALTHY)

    def _check_system_resources(self, system_metrics: SystemMetrics, alerts: list[str]) -> None:
        """Check system resource usage and add alerts.

        Args:
            system_metrics: System metrics
            alerts: List to append alerts to
        """
        # Check CPU usage
        if system_metrics.cpu_usage_percent > self._monitoring_config.cpu_threshold_percent:
            alert = f"HIGH CPU: {float(system_metrics.cpu_usage_percent):.1f}% usage"
            if alert not in alerts:
                alerts.append(alert)
            self._record_alert(alert, HealthStatus.DEGRADED)

        # Check memory usage
        memory_threshold = Decimal(str(self._monitoring_config.memory_threshold_mb))
        if system_metrics.memory_usage_percent > memory_threshold:
            alert = f"HIGH MEMORY: {float(system_metrics.memory_usage_percent):.1f}% usage"
            if alert not in alerts:
                alerts.append(alert)
            self._record_alert(alert, HealthStatus.DEGRADED)

    def _check_error_rates(
        self, service_checks: list[ServiceHealthStatus], alerts: list[str]
    ) -> None:
        """Check for services with high error rates and add alerts.

        Args:
            service_checks: Service health checks
            alerts: List to append alerts to
        """
        for service in service_checks:
            if service.error_count and service.success_count:
                total_requests = service.error_count + service.success_count
                error_rate = (service.error_count / total_requests) * 100
                if error_rate > self._monitoring_config.error_rate_threshold * 100:
                    alert = (
                        f"HIGH ERROR RATE: Service '{service.service_name}' "
                        f"has {error_rate:.1f}% error rate"
                    )
                    if alert not in alerts:
                        alerts.append(alert)
                    self._record_alert(alert, HealthStatus.UNHEALTHY)

    def _record_alert(self, alert_message: str, severity: HealthStatus) -> None:
        """Record an alert in history.

        Args:
            alert_message: Alert message
            severity: Alert severity
        """
        # Add to active alerts if not already present
        if alert_message not in self._active_alerts:
            self._active_alerts.append(alert_message)

        # Add to history
        self._alert_history.append((datetime.now(UTC), alert_message, severity))

        # Trim history to prevent unbounded growth
        max_history = 1000  # From config in production
        if len(self._alert_history) > max_history:
            self._alert_history = self._alert_history[-max_history:]

    def cache_health_check(self, service_name: str, status: ServiceHealthStatus) -> None:
        """Cache health check result for a service.

        Args:
            service_name: Service name
            status: Health check status
        """
        self._last_health_checks[service_name] = status

        # Log significant status changes
        if status.health_status in {HealthStatus.CRITICAL, HealthStatus.UNHEALTHY}:
            logger.warning(
                "service_health_issue",
                service_name=service_name,
                status=status.health_status.value,
                error_count=status.error_count,
                response_time_ms=float(status.response_time_ms),
            )

    def get_last_check(self, service_name: str) -> ServiceHealthStatus | None:
        """Get last health check result for a service.

        Args:
            service_name: Service name

        Returns:
            Last health status or None if never checked
        """
        return self._last_health_checks.get(service_name)

    def get_all_last_checks(self) -> dict[str, ServiceHealthStatus]:
        """Get all last health check results.

        Returns:
            Dictionary of service name to last health status
        """
        return self._last_health_checks.copy()

    def get_active_alerts(self) -> list[str]:
        """Get currently active alerts.

        Returns:
            List of active alert messages
        """
        return self._active_alerts.copy()

    def clear_alert(self, alert_message: str) -> bool:
        """Clear an active alert.

        Args:
            alert_message: Alert message to clear

        Returns:
            True if alert was cleared, False if not found
        """
        if alert_message in self._active_alerts:
            self._active_alerts.remove(alert_message)
            logger.info("alert_cleared", alert=alert_message)
            return True
        return False

    def clear_all_alerts(self) -> int:
        """Clear all active alerts.

        Returns:
            Number of alerts cleared
        """
        count = len(self._active_alerts)
        self._active_alerts.clear()
        logger.info("all_alerts_cleared", count=count)
        return count

    def get_alert_history(
        self,
        hours: int = 24,
        severity_filter: HealthStatus | None = None,
    ) -> list[tuple[datetime, str, HealthStatus]]:
        """Get alert history.

        Args:
            hours: Number of hours of history to return
            severity_filter: Optional severity filter

        Returns:
            List of (timestamp, message, severity) tuples
        """
        cutoff = datetime.now(UTC).replace(tzinfo=UTC) - timedelta(hours=hours)

        history = [(ts, msg, sev) for ts, msg, sev in self._alert_history if ts >= cutoff]

        if severity_filter:
            history = [(ts, msg, sev) for ts, msg, sev in history if sev == severity_filter]

        return history

    def generate_summary_report(self) -> dict[str, Any]:
        """Generate a summary report of current health status.

        Returns:
            Dictionary with summary information
        """
        all_checks = self.get_all_last_checks()

        # Count by status
        status_counts = {
            "healthy": 0,
            "degraded": 0,
            "unhealthy": 0,
            "critical": 0,
            "unknown": 0,
        }

        for status in all_checks.values():
            if status.health_status == HealthStatus.HEALTHY:
                status_counts["healthy"] += 1
            elif status.health_status == HealthStatus.DEGRADED:
                status_counts["degraded"] += 1
            elif status.health_status == HealthStatus.UNHEALTHY:
                status_counts["unhealthy"] += 1
            elif status.health_status == HealthStatus.CRITICAL:
                status_counts["critical"] += 1
            else:
                status_counts["unknown"] += 1

        # Calculate average response time
        response_times = [
            float(s.response_time_ms)
            for s in all_checks.values()
            if s.response_time_ms != Decimal(0)
        ]
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0.0

        # Calculate overall error rate
        total_errors = sum(s.error_count for s in all_checks.values() if s.error_count is not None)
        total_successes = sum(
            s.success_count for s in all_checks.values() if s.success_count is not None
        )
        total_requests = total_errors + total_successes
        overall_error_rate = (total_errors / total_requests * 100) if total_requests > 0 else 0.0

        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "total_services": len(all_checks),
            "status_counts": status_counts,
            "active_alerts": len(self._active_alerts),
            "average_response_time_ms": avg_response_time,
            "overall_error_rate": overall_error_rate,
            "services": {
                name: {
                    "status": status.health_status.value,
                    "response_time_ms": float(status.response_time_ms),
                    "error_count": status.error_count,
                    "success_count": status.success_count,
                    "is_running": status.is_running,
                }
                for name, status in all_checks.items()
            },
        }

    def should_alert_cooldown(self, alert_message: str) -> bool:
        """Check if an alert is in cooldown period.

        Args:
            alert_message: Alert message to check

        Returns:
            True if alert should be suppressed due to cooldown
        """
        cooldown_seconds = self._monitoring_config.alert_suppression_seconds
        cutoff = datetime.now(UTC) - timedelta(seconds=cooldown_seconds)

        # Check if this alert was recently sent
        recent_alerts = [
            (ts, msg) for ts, msg, _ in self._alert_history if ts >= cutoff and msg == alert_message
        ]

        return len(recent_alerts) > 0

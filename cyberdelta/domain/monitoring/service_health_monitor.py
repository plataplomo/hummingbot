"""Service health monitoring system for trading services.

This module provides comprehensive health monitoring capabilities with
configuration-driven thresholds and structured reporting for individual
trading services (portfolio, execution, risk, etc.).

File has been refactored to stay under 600 lines by extracting:
- Health check logic to health_checks.py
- Metrics collection to health_metrics.py
- Reporting logic to health_reporting.py
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.monitoring.health_checks import HealthChecks
from cyberdelta.domain.monitoring.health_metrics import HealthMetrics, HealthMetricsCollector
from cyberdelta.domain.monitoring.health_reporting import HealthReporter
from cyberdelta.exceptions.monitoring import HealthMonitorAlreadyRunningError
from cyberdelta.models.monitoring.system_health_models import (
    ExecutionStatistics,
    ServiceHealthStatus,
    SystemHealthReport,
)
from cyberdelta.protocols import HealthCheckable


if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


class ServiceHealthMonitor:
    """Central health monitoring system with configuration-driven behavior.

    This is the main orchestrator that uses specialized modules:
    - HealthChecks: Individual service health checks
    - HealthMetricsCollector: Metrics collection and calculation
    - HealthReporter: Status reporting and aggregation

    Configuration Usage:
    - Uses config.monitoring.health_check_interval_seconds for check frequency
    - Uses config.monitoring.health_check_thresholds for status determination
    - Uses config.monitoring.stale_data_threshold_seconds for staleness checks
    - Uses config.monitoring.response_time_threshold_ms for performance checks
    - Uses config.monitoring.error_rate_threshold for error rate monitoring

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL thresholds from AppSettings, NO hardcoded values
    - Uses structured logging only
    - Fail-fast on configuration violations
    - Type-safe service registration

    File size management:
    - Operations split into HealthChecks, HealthMetricsCollector, HealthReporter
    - Each module stays under 600 lines
    - Business logic preserved exactly as before
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize health monitor with configuration.

        Args:
            config: Application settings containing monitoring configuration
        """
        self.config = config
        self._monitoring_config = config.monitoring

        # Initialize specialized modules
        self._health_checks = HealthChecks(config)
        self._metrics_collector = HealthMetricsCollector(config)
        self._reporter = HealthReporter(config, self._metrics_collector)

        # Monitoring state
        self._running = False
        self._monitor_task: asyncio.Task[None] | None = None

        # Extract configuration settings
        self._check_interval = self._monitoring_config.health_check_interval_seconds

        logger.info(
            "service_health_monitor_initialized",
            check_interval=self._check_interval,
            stale_threshold=self._monitoring_config.stale_data_threshold_seconds,
            response_threshold=self._monitoring_config.response_time_threshold_ms,
            error_threshold=self._monitoring_config.error_rate_threshold,
        )

    def register_service(self, name: str, service: HealthCheckable) -> None:
        """Register a service for health monitoring.

        Args:
            name: Unique service name
            service: Service implementing HealthCheckable protocol

        Delegates to HealthChecks module which may raise ServiceAlreadyRegisteredError.
        """
        self._health_checks.register_service(name, service)
        self._metrics_collector.record_service_start(name)

    def unregister_service(self, name: str) -> bool:
        """Unregister a service from health monitoring.

        Args:
            name: Service name to unregister

        Returns:
            True if service was unregistered, False if not found

        Delegates to HealthChecks module.
        """
        return self._health_checks.unregister_service(name)

    async def start(self) -> None:
        """Start the health monitoring loop.

        Raises:
            HealthMonitorAlreadyRunningError: If monitor is already running
        """
        if self._running:
            raise HealthMonitorAlreadyRunningError

        self._running = True
        self._monitor_task = asyncio.create_task(self._monitoring_loop())

        logger.info(
            "health_monitor_started",
            check_interval=self._check_interval,
            registered_services=len(self._health_checks.get_registered_services()),
        )

    async def stop(self) -> None:
        """Stop the health monitoring loop.

        Waits for the monitoring task to complete gracefully.
        """
        if not self._running:
            logger.warning("health_monitor_not_running", action="stop_requested")
            return

        self._running = False

        if self._monitor_task:
            self._monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._monitor_task
            self._monitor_task = None

        logger.info("health_monitor_stopped")

    async def check_service_health(self, service_name: str) -> ServiceHealthStatus:
        """Check health of a specific service.

        Args:
            service_name: Name of service to check

        Returns:
            ServiceHealthStatus with check results

        Delegates to HealthChecks module which may raise ServiceNotRegisteredError.
        """
        status = await self._health_checks.check_service_health(service_name)
        self._reporter.cache_health_check(service_name, status)
        return status

    async def get_system_health(self) -> SystemHealthReport:
        """Get comprehensive system health report.

        Returns:
            SystemHealthReport with all service statuses and system metrics
        """
        # Check all registered services
        service_checks: list[ServiceHealthStatus] = []
        for service_name in self._health_checks.get_registered_services():
            try:
                status = await self.check_service_health(service_name)
                service_checks.append(status)
            except Exception as e:
                logger.exception(
                    "failed_to_check_service",
                    service_name=service_name,
                    error=str(e),
                )

        # Generate comprehensive report
        return await self._reporter.generate_system_health_report(service_checks)

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop that periodically checks all services.

        Runs until stopped, checking each service at the configured interval.
        """
        logger.info("health_monitoring_loop_started")

        while self._running:
            try:
                # Check all services
                for service_name in self._health_checks.get_registered_services():
                    try:
                        await self.check_service_health(service_name)
                    except Exception as e:
                        logger.exception(
                            "service_health_check_error",
                            service_name=service_name,
                            error=str(e),
                        )

                # Wait for next check interval
                await asyncio.sleep(self._check_interval)

            except asyncio.CancelledError:
                logger.info("health_monitoring_loop_cancelled")
                break
            except Exception as e:
                logger.exception(
                    "health_monitoring_loop_error",
                    error=str(e),
                )
                # Continue monitoring despite errors
                if self._running:
                    await asyncio.sleep(self._check_interval)

        logger.info("health_monitoring_loop_stopped")

    def get_last_check(self, service_name: str) -> ServiceHealthStatus | None:
        """Get last health check result for a service.

        Args:
            service_name: Service name

        Returns:
            Last health status or None if never checked

        Delegates to HealthReporter module.
        """
        return self._reporter.get_last_check(service_name)

    def get_all_last_checks(self) -> dict[str, ServiceHealthStatus]:
        """Get all last health check results.

        Returns:
            Dictionary of service name to last health status

        Delegates to HealthReporter module.
        """
        return self._reporter.get_all_last_checks()

    def is_running(self) -> bool:
        """Check if health monitor is running.

        Returns:
            True if monitoring loop is active
        """
        return self._running

    def get_registered_services(self) -> list[str]:
        """Get list of registered service names.

        Returns:
            List of service names

        Delegates to HealthChecks module.
        """
        return self._health_checks.get_registered_services()

    def get_active_alerts(self) -> list[str]:
        """Get currently active alerts.

        Returns:
            List of active alert messages

        Delegates to HealthReporter module.
        """
        return self._reporter.get_active_alerts()

    def clear_alert(self, alert_message: str) -> bool:
        """Clear an active alert.

        Args:
            alert_message: Alert message to clear

        Returns:
            True if alert was cleared, False if not found

        Delegates to HealthReporter module.
        """
        return self._reporter.clear_alert(alert_message)

    def generate_summary_report(self) -> dict[str, Any]:
        """Generate a summary report of current health status.

        Returns:
            Dictionary with summary information

        Delegates to HealthReporter module.
        """
        return self._reporter.generate_summary_report()

    def _extract_health_metric(
        self, health_data: dict[str, Any] | ExecutionStatistics, service_name: str
    ) -> HealthMetrics:
        """Extract health metrics from service response.

        Args:
            health_data: Raw health check response
            service_name: Name of service

        Returns:
            Extracted health metrics

        Note: This is a compatibility method for existing code.
        """
        if isinstance(health_data, ExecutionStatistics):
            # Convert Decimal to float for response time
            response_time = None
            if health_data.average_execution_time_ms is not None:
                response_time = float(health_data.average_execution_time_ms)

            return self._metrics_collector.create_health_metrics(
                service_name=service_name,
                response_time_ms=response_time,
                error_count=(
                    len(health_data.recent_error_messages)
                    if health_data.recent_error_messages
                    else 0
                ),
                success_count=1 if not health_data.recent_error_messages else 0,
            )

        # Handle dict response
        return self._metrics_collector.create_health_metrics(
            service_name=service_name,
            response_time_ms=health_data.get("response_time_ms"),
            error_count=health_data.get("error_count", 0),
            success_count=health_data.get("success_count", 0),
            last_activity=health_data.get("last_activity"),
            memory_usage_mb=health_data.get("memory_usage_mb"),
            cpu_usage_percent=health_data.get("cpu_usage_percent"),
        )

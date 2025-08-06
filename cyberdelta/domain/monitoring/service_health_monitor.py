"""Service health monitoring system for trading services.

This module provides comprehensive health monitoring capabilities with
configuration-driven thresholds and structured reporting for individual
trading services (portfolio, execution, risk, etc.).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.enums.monitoring import ServiceType


if TYPE_CHECKING:
    from cyberdelta.models.monitoring.system_health_models import ExecutionStatistics

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models.monitoring.system_health_models import (
    HealthCheckDetails,
    HealthStatus,
    MonitoringConfiguration,
    OperationalThresholds,
    ServiceHealthStatus,
    SystemHealthReport,
    SystemMetrics,
)
from cyberdelta.protocols import HealthCheckable


logger = get_logger(__name__)


@dataclass
class HealthMetrics:
    """Health metrics for a service."""

    response_time_ms: float | None
    error_count: int
    success_count: int
    last_activity: datetime | None
    uptime_seconds: float | None
    memory_usage_mb: float | None
    cpu_usage_percent: float | None


# Use ServiceHealthStatus from models instead of duplicate HealthCheck


# Use SystemHealthReport from models instead of duplicate


class ServiceHealthMonitor:
    """Central health monitoring system with configuration-driven behavior.

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
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize health monitor with configuration.

        Args:
            config: Application settings containing monitoring configuration
        """
        self.config = config
        self._monitoring_config = config.monitoring
        self._services: dict[str, HealthCheckable] = {}
        self._last_checks: dict[str, ServiceHealthStatus] = {}
        self._running = False
        self._monitor_task: asyncio.Task[None] | None = None

        # Extract configuration settings - NO hardcoded defaults
        self._check_interval = self._monitoring_config.health_check_interval_seconds
        self._stale_threshold = self._monitoring_config.stale_data_threshold_seconds
        self._response_time_threshold = self._monitoring_config.response_time_threshold_ms
        self._error_rate_threshold = self._monitoring_config.error_rate_threshold
        self._memory_threshold_mb = self._monitoring_config.memory_threshold_mb
        self._cpu_threshold_percent = self._monitoring_config.cpu_threshold_percent

        # Health check thresholds from config
        self._thresholds = self._monitoring_config.health_check_thresholds

        logger.info(
            "service_health_monitor_initialized",
            check_interval_sec=float(self._check_interval),
            stale_threshold_sec=float(self._stale_threshold),
            response_time_threshold_ms=float(self._response_time_threshold),
            error_rate_threshold=float(self._error_rate_threshold),
            memory_threshold_mb=float(self._memory_threshold_mb),
            cpu_threshold_percent=float(self._cpu_threshold_percent),
        )

    def register_service(self, name: str, service: HealthCheckable) -> None:
        """Register a service for health monitoring.

        Args:
            name: Unique name for the service
            service: Service instance that implements HealthCheckable protocol


        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit service registration only
        - Type safety through protocol
        - No auto-discovery
        """
        if name in self._services:
            logger.warning(
                "service_already_registered",
                service_name=name,
                service_type=service.get_service_type().value,
            )
            return

        self._services[name] = service

        logger.info(
            "service_registered_for_monitoring",
            service_name=name,
            service_type=service.get_service_type().value,
            total_services=len(self._services),
        )

    def unregister_service(self, name: str) -> bool:
        """Unregister a service from health monitoring.

        Args:
            name: Name of service to unregister


        Returns:
            True if service was unregistered, False if not found


        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit return value for success/failure
        - Clean removal from tracking
        """
        if name not in self._services:
            logger.warning(
                "unregister_service_not_found",
                service_name=name,
                available_services=list(self._services.keys()),
            )
            return False

        del self._services[name]
        self._last_checks.pop(name, None)

        logger.info(
            "service_unregistered_from_monitoring",
            service_name=name,
            remaining_services=len(self._services),
        )
        return True

    async def start(self) -> None:
        """Start health monitoring loop.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured intervals
        - Proper task management
        - Fail-fast if already running
        """
        if self._running:
            logger.warning("service_health_monitor_already_running")
            return

        self._running = True
        self._monitor_task = asyncio.create_task(self._monitoring_loop())

        logger.info(
            "service_health_monitor_started",
            check_interval_sec=float(self._check_interval),
            registered_services=len(self._services),
            service_names=list(self._services.keys()),
        )

    async def stop(self) -> None:
        """Stop health monitoring loop.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Graceful task cancellation
        - Proper cleanup
        - Uses configured shutdown timeout
        """
        if not self._running:
            logger.warning("service_health_monitor_not_running")
            return

        self._running = False

        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await asyncio.wait_for(
                    self._monitor_task, timeout=float(self.config.general.shutdown_grace_period)
                )
            except TimeoutError:
                logger.warning(
                    "service_health_monitor_shutdown_timeout",
                    grace_period_sec=float(self.config.general.shutdown_grace_period),
                )
            except asyncio.CancelledError:
                pass

        logger.info("service_health_monitor_stopped")

    async def check_service_health(self, service_name: str) -> ServiceHealthStatus:
        """Check health of a specific service.

        Args:
            service_name: Name of service to check


        Returns:
            ServiceHealthStatus result with current status

        Note:
            Following CODING_STANDARDS.md:
            - Uses configured thresholds for status determination
            - Structured error handling
            - No assumptions about service state
        """
        if service_name not in self._services:
            self._raise_service_not_registered_error(service_name)

        service = self._services[service_name]
        start_time = datetime.now(UTC)

        try:
            # Perform health check with timeout
            health_data = await asyncio.wait_for(
                service.check_health(),
                timeout=float(self._monitoring_config.health_check_timeout_seconds),
            )

            # Calculate response time
            response_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

            # Determine health status based on configured thresholds
            status = self._determine_health_status(service_name, health_data, response_time)

            # Create health check result
            health_check = ServiceHealthStatus(
                service_name=service_name,
                service_type=service.get_service_type(),
                is_healthy=(status == HealthStatus.HEALTHY),
                is_running=True,
                health_status=status,
                last_check_timestamp=datetime.now(UTC),
                response_time_ms=Decimal(str(response_time)),
                error_count=self._extract_health_metric(health_data, "error_count"),
                success_count=self._extract_health_metric(health_data, "success_count"),
                health_details=HealthCheckDetails(
                    status=status.value,
                    thresholds_used=OperationalThresholds(
                        response_time_ms=float(self._response_time_threshold),
                        error_rate=float(self._error_rate_threshold),
                        stale_data_sec=float(self._stale_threshold),
                    ),
                ),
            )

            logger.debug(
                "service_health_check_completed",
                service_name=service_name,
                status=status.value,
                response_time_ms=response_time,
                error_count=self._extract_health_metric(health_data, "error_count"),
                success_count=self._extract_health_metric(health_data, "success_count"),
            )

        except TimeoutError:
            logger.exception(
                "service_health_check_timeout",
                service_name=service_name,
                timeout_sec=float(self._monitoring_config.health_check_timeout_seconds),
            )

            health_check = ServiceHealthStatus(
                service_name=service_name,
                service_type=service.get_service_type(),
                is_healthy=False,
                is_running=False,
                health_status=HealthStatus.CRITICAL,
                last_check_timestamp=datetime.now(UTC),
                response_time_ms=Decimal(
                    str(float(self._monitoring_config.health_check_timeout_seconds) * 1000)
                ),
                health_details=HealthCheckDetails(
                    error_message="Health check timeout",
                    timeout_sec=float(self._monitoring_config.health_check_timeout_seconds),
                ),
            )

        except Exception as e:
            logger.exception("service_health_check_error", service_name=service_name, error=str(e))

            health_check = ServiceHealthStatus(
                service_name=service_name,
                service_type=service.get_service_type(),
                is_healthy=False,
                is_running=False,
                health_status=HealthStatus.CRITICAL,
                last_check_timestamp=datetime.now(UTC),
                response_time_ms=Decimal(0),
                health_details=HealthCheckDetails(
                    error_message=str(e),
                ),
            )
        else:
            # Cache the successful result
            self._last_checks[service_name] = health_check

        return health_check

    async def get_system_health(self) -> SystemHealthReport:
        """Get comprehensive system health report.

        Returns:
            Complete system health report with all services


        IMPORTANT: Following CODING_STANDARDS.md:
        - Aggregates health from all registered services
        - Uses configured thresholds for overall status
        - Returns structured report
        """
        logger.debug("generating_system_health_report", service_count=len(self._services))

        # Check all registered services
        service_checks: list[ServiceHealthStatus] = []
        for service_name in self._services:
            try:
                health_check = await self.check_service_health(service_name)
                service_checks.append(health_check)
            except (ValueError, TimeoutError, ConnectionError, OSError) as e:
                logger.exception(
                    "system_health_check_service_failed", service_name=service_name, error=str(e)
                )
                # Create a failed health check
                # Determine service type - try to get it from the service if possible
                try:
                    service_type = self._services[service_name].get_service_type()
                except (AttributeError, ValueError, TypeError):
                    # If we can't get the service type, default to a known type
                    service_type = ServiceType.PORTFOLIO  # Use a valid ServiceType

                service_checks.append(
                    ServiceHealthStatus(
                        service_name=service_name,
                        service_type=service_type,
                        is_healthy=False,
                        is_running=False,
                        health_status=HealthStatus.CRITICAL,
                        last_check_timestamp=datetime.now(UTC),
                        response_time_ms=Decimal(0),
                        health_details=HealthCheckDetails(
                            error_message=f"Health check failed: {e!s}",
                        ),
                    )
                )

        # Determine overall system status
        overall_status = self._determine_overall_status(service_checks)

        # Collect system metrics
        system_metrics = await self._collect_system_metrics()

        # Check for alerts
        alerts = self._check_alerts(service_checks, system_metrics)

        # Count healthy/unhealthy services
        healthy_count = sum(1 for check in service_checks if check.is_healthy)
        unhealthy_count = len(service_checks) - healthy_count

        # Convert service_checks to service_statuses dict
        service_statuses = {check.service_name: check for check in service_checks}

        report = SystemHealthReport(
            overall_health_status=overall_status.value,
            report_timestamp=datetime.now(UTC),
            total_services_monitored=len(service_checks),
            healthy_services_count=healthy_count,
            unhealthy_services_count=unhealthy_count,
            service_statuses=service_statuses,
            system_metrics=system_metrics,
            active_alerts=alerts,
            monitoring_configuration=MonitoringConfiguration(
                monitoring_enabled=self._monitoring_config.notifications_enabled,
                check_interval_sec=float(self._check_interval),
                total_services=len(self._services),
                thresholds=OperationalThresholds(
                    response_time_ms=float(self._response_time_threshold),
                    error_rate=float(self._error_rate_threshold),
                    stale_data_sec=float(self._stale_threshold),
                ),
            ),
        )

        logger.info(
            "system_health_report_generated",
            overall_status=overall_status.value,
            healthy_services=sum(
                1 for check in service_checks if check.health_status == HealthStatus.HEALTHY
            ),
            total_services=len(service_checks),
            alerts_count=len(alerts),
        )

        return report

    def _extract_health_metric(
        self, health_data: dict[str, Any] | ExecutionStatistics, metric_name: str
    ) -> int:
        """Extract metric from health data, handling both dict and typed objects.

        Returns:
            Metric value as integer, 0 if not found
        """
        if isinstance(health_data, dict):
            value = health_data.get(metric_name, 0)
            return int(value) if value is not None else 0

        # Handle ExecutionStatistics with order_tracking composition
        if hasattr(health_data, "order_tracking"):
            value = getattr(health_data.order_tracking, metric_name, 0)
            return int(value) if value is not None else 0

        # Handle direct attributes
        value = getattr(health_data, metric_name, 0)
        return int(value) if value is not None else 0

    def _determine_health_status(
        self,
        service_name: str,
        health_data: dict[str, Any] | ExecutionStatistics,
        response_time_ms: float,
    ) -> HealthStatus:
        """Determine health status based on configured thresholds.

        Args:
            service_name: Name of the service
            health_data: Health data from service
            response_time_ms: Response time in milliseconds

        Returns:
            Determined health status

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses only configured thresholds
        - Explicit status determination logic
        - No hardcoded threshold values
        """
        # Check critical conditions first
        if not self._is_service_running(health_data):
            logger.warning("service_not_running", service_name=service_name)
            return HealthStatus.CRITICAL

        # Check response time
        if self._is_response_time_slow(service_name, response_time_ms):
            return HealthStatus.DEGRADED

        # Check error rate
        error_status = self._check_error_rate(service_name, health_data)
        if error_status != HealthStatus.HEALTHY:
            return error_status

        # Check data staleness
        if self._is_data_stale(service_name, health_data):
            return HealthStatus.DEGRADED

        # Check resource usage
        resource_status = self._check_resource_usage(service_name, health_data)
        if resource_status != HealthStatus.HEALTHY:
            return resource_status

        # If all checks pass, service is healthy
        return HealthStatus.HEALTHY

    def _is_service_running(self, health_data: dict[str, Any] | ExecutionStatistics) -> bool:
        """Check if service is running.

        Returns:
            True if service is running, False otherwise
        """
        if isinstance(health_data, dict):
            value = health_data.get("is_running", True)
            return bool(value)
        if hasattr(health_data, "is_running"):
            return getattr(health_data, "is_running", True)
        return True

    def _is_response_time_slow(self, service_name: str, response_time_ms: float) -> bool:
        """Check if response time exceeds threshold.

        Returns:
            True if response time is slow, False otherwise
        """
        if response_time_ms > self._response_time_threshold:
            logger.warning(
                "service_slow_response",
                service_name=service_name,
                response_time_ms=response_time_ms,
                threshold_ms=float(self._response_time_threshold),
            )
            return True
        return False

    def _check_error_rate(
        self, service_name: str, health_data: dict[str, Any] | ExecutionStatistics
    ) -> HealthStatus:
        """Check error rate against configured threshold.

        Returns:
            HealthStatus.UNHEALTHY if error rate too high, HealthStatus.HEALTHY otherwise
        """
        success_count = self._extract_health_metric(health_data, "success_count")
        error_count = self._extract_health_metric(health_data, "error_count")
        total_requests = success_count + error_count

        if total_requests > 0:
            error_rate = error_count / total_requests
            if error_rate > self._error_rate_threshold:
                logger.warning(
                    "service_high_error_rate",
                    service_name=service_name,
                    error_rate=error_rate,
                    threshold=float(self._error_rate_threshold),
                )
                return HealthStatus.UNHEALTHY

        return HealthStatus.HEALTHY

    def _is_data_stale(
        self, service_name: str, health_data: dict[str, Any] | ExecutionStatistics
    ) -> bool:
        """Check if data is stale based on last activity.

        Returns:
            True if data is stale, False otherwise
        """
        last_activity = self._extract_last_activity(health_data)

        if last_activity:
            if isinstance(last_activity, str):
                last_activity = datetime.fromisoformat(last_activity.rstrip("Z")).replace(
                    tzinfo=UTC
                )

            time_since_activity = (datetime.now(UTC) - last_activity).total_seconds()
            if time_since_activity > self._stale_threshold:
                logger.warning(
                    "service_stale_data",
                    service_name=service_name,
                    time_since_activity_sec=time_since_activity,
                    threshold_sec=float(self._stale_threshold),
                )
                return True

        return False

    def _extract_last_activity(
        self, health_data: dict[str, Any] | ExecutionStatistics
    ) -> datetime | str | None:
        """Extract last activity timestamp from health data.

        Returns:
            Last activity timestamp or None if not found
        """
        if isinstance(health_data, dict):
            return health_data.get("last_activity")
        if hasattr(health_data, "last_activity_timestamp"):
            return getattr(health_data, "last_activity_timestamp", None)
        if hasattr(health_data, "last_activity"):
            return getattr(health_data, "last_activity", None)
        return None

    def _check_resource_usage(
        self, service_name: str, health_data: dict[str, Any] | ExecutionStatistics
    ) -> HealthStatus:
        """Check memory and CPU usage against thresholds.

        Returns:
            HealthStatus.DEGRADED if resources high, HealthStatus.HEALTHY otherwise
        """
        # Check memory usage
        if self._is_memory_usage_high(service_name, health_data):
            return HealthStatus.DEGRADED

        # Check CPU usage
        if self._is_cpu_usage_high(service_name, health_data):
            return HealthStatus.DEGRADED

        return HealthStatus.HEALTHY

    def _is_memory_usage_high(
        self, service_name: str, health_data: dict[str, Any] | ExecutionStatistics
    ) -> bool:
        """Check if memory usage exceeds threshold.

        Returns:
            True if memory usage is high, False otherwise
        """
        memory_usage = self._extract_metric_value(health_data, "memory_usage_mb")

        if memory_usage and memory_usage > self._memory_threshold_mb:
            logger.warning(
                "service_high_memory_usage",
                service_name=service_name,
                memory_usage_mb=memory_usage,
                threshold_mb=float(self._memory_threshold_mb),
            )
            return True
        return False

    def _is_cpu_usage_high(
        self, service_name: str, health_data: dict[str, Any] | ExecutionStatistics
    ) -> bool:
        """Check if CPU usage exceeds threshold.

        Returns:
            True if CPU usage is high, False otherwise
        """
        cpu_usage = self._extract_metric_value(health_data, "cpu_usage_percent")

        if cpu_usage and cpu_usage > self._cpu_threshold_percent:
            logger.warning(
                "service_high_cpu_usage",
                service_name=service_name,
                cpu_usage_percent=cpu_usage,
                threshold_percent=float(self._cpu_threshold_percent),
            )
            return True
        return False

    def _extract_metric_value(
        self,
        health_data: dict[str, Any] | ExecutionStatistics,
        metric_name: str,
    ) -> float | None:
        """Extract a metric value from health data.

        Returns:
            Metric value or None if not found
        """
        if isinstance(health_data, dict):
            return health_data.get(metric_name)
        if hasattr(health_data, metric_name):
            return getattr(health_data, metric_name, None)
        return None

    def _determine_overall_status(self, service_checks: list[ServiceHealthStatus]) -> HealthStatus:
        """Determine overall system status from service checks.

        Args:
            service_checks: List of individual service health checks


        Returns:
            Overall system health status


        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses explicit status aggregation logic
        - Based on configured system health thresholds
        """
        if not service_checks:
            return HealthStatus.UNKNOWN

        # Count services by status
        status_counts: dict[HealthStatus, int] = {}
        for check in service_checks:
            status_counts[check.health_status] = status_counts.get(check.health_status, 0) + 1

        total_services = len(service_checks)
        critical_count = status_counts.get(HealthStatus.CRITICAL, 0)
        unhealthy_count = status_counts.get(HealthStatus.UNHEALTHY, 0)
        degraded_count = status_counts.get(HealthStatus.DEGRADED, 0)

        # Use configured thresholds for overall status determination
        critical_threshold = self._thresholds.critical_service_threshold
        unhealthy_threshold = self._thresholds.unhealthy_service_threshold
        degraded_threshold = self._thresholds.degraded_service_threshold

        # Determine overall status based on configured ratios
        if critical_count >= total_services * critical_threshold:
            return HealthStatus.CRITICAL
        if (critical_count + unhealthy_count) >= total_services * unhealthy_threshold:
            return HealthStatus.UNHEALTHY
        if (
            critical_count + unhealthy_count + degraded_count
        ) >= total_services * degraded_threshold:
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY

    async def _collect_system_metrics(self) -> SystemMetrics:
        """Collect system-level metrics.

        Returns:
            Dictionary of system metrics


        IMPORTANT: Following CODING_STANDARDS.md:
        - Collects only configured metrics
        - No hardcoded system monitoring
        """
        # Return basic system metrics directly as SystemMetrics object
        return SystemMetrics(
            cpu_usage_percent=Decimal(0),  # Placeholder - would normally collect from system
            memory_usage_percent=Decimal(0),  # Placeholder - would normally collect from system
            disk_usage_percent=Decimal(0),  # Placeholder - would normally collect from system
            uptime_seconds=0,  # Placeholder - would normally collect from system
            metrics_collection_timestamp=datetime.now(UTC),
        )

    def _check_alerts(
        self, service_checks: list[ServiceHealthStatus], system_metrics: SystemMetrics
    ) -> list[str]:
        """Check for alert conditions based on health checks.

        Args:
            service_checks: List of service health checks
            system_metrics: System-level metrics


        Returns:
            List of alert messages


        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured alert thresholds
        - Returns explicit alert messages
        """
        alerts: list[str] = []

        # Check for critical services
        critical_services = [
            check.service_name
            for check in service_checks
            if check.health_status == HealthStatus.CRITICAL
        ]
        if critical_services:
            alerts.append(f"Critical services detected: {', '.join(critical_services)}")

        # Check for high error rates across services
        total_errors = sum(check.error_count or 0 for check in service_checks)
        total_requests = sum(
            (check.error_count or 0) + (check.success_count or 0) for check in service_checks
        )

        if total_requests > 0:
            system_error_rate = total_errors / total_requests
            if system_error_rate > self._error_rate_threshold:
                alerts.append(
                    f"System error rate {system_error_rate:.2%} exceeds threshold "
                    f"{self._error_rate_threshold:.2%}"
                )

        # Check for slow services
        slow_services = [
            check.service_name
            for check in service_checks
            if check.response_time_ms and check.response_time_ms > self._response_time_threshold
        ]
        if slow_services:
            alerts.append(f"Slow response services: {', '.join(slow_services)}")

        return alerts

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop that runs periodic health checks.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured check intervals
        - Handles errors without stopping loop
        - Structured logging for all events
        """
        logger.info(
            "service_health_monitoring_loop_started",
            interval_sec=float(self._check_interval),
            services_count=len(self._services),
        )

        while self._running:
            try:
                # Generate system health report
                health_report = await self.get_system_health()

                # Log system status
                logger.info(
                    "health_check_cycle_completed",
                    overall_status=health_report.overall_health_status,
                    services_checked=health_report.total_services_monitored,
                    alerts_count=len(health_report.active_alerts or []),
                )

                # Log any alerts
                if health_report.active_alerts:
                    for alert in health_report.active_alerts:
                        logger.warning("health_alert_triggered", alert_message=alert)

                # Wait for next cycle
                await asyncio.sleep(float(self._check_interval))

            except asyncio.CancelledError:
                logger.info("service_health_monitoring_loop_cancelled")
                break
            except Exception as e:
                logger.exception("service_health_monitoring_loop_error", error=str(e))

                # Use exponential backoff from config for errors
                error_backoff = float(self.config.execution.retry_delay_base_sec) * float(
                    self.config.execution.retry_backoff_multiplier
                )
                await asyncio.sleep(error_backoff)

        logger.info("service_health_monitoring_loop_ended")

    def get_last_check(self, service_name: str) -> ServiceHealthStatus | None:
        """Get the last health check result for a service.

        Args:
            service_name: Name of service


        Returns:
            Last health check result or None if not found
        """
        return self._last_checks.get(service_name)

    def get_all_last_checks(self) -> dict[str, ServiceHealthStatus]:
        """Get all last health check results.

        Returns:
            Dictionary mapping service names to their last health checks
        """
        return self._last_checks.copy()

    def is_running(self) -> bool:
        """Check if health monitoring is running.

        Returns:
            True if monitoring loop is active
        """
        return self._running

    def get_registered_services(self) -> list[str]:
        """Get list of registered service names.

        Returns:
            List of service names currently being monitored
        """
        return list(self._services.keys())

    def _raise_service_not_registered_error(self, service_name: str) -> None:
        """Raise an error for unregistered service.

        Args:
            service_name: Name of the unregistered service

        Raises:
            ValueError: If service is not registered
        """
        msg = f"Service '{service_name}' not registered for monitoring"
        raise ValueError(msg)

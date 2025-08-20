"""Health check implementations for services.

This module contains the health check logic extracted from ServiceHealthMonitor
to maintain file size under 600 lines while keeping the same business logic.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.monitoring.health_metrics import HealthMetrics
from cyberdelta.enums.monitoring import HealthStatus, ServiceType
from cyberdelta.exceptions.monitoring import (
    MonitoringError,
    ServiceAlreadyRegisteredError,
    ServiceNotRegisteredError,
)
from cyberdelta.models.monitoring.system_health_models import ServiceHealthStatus
from cyberdelta.protocols import HealthCheckable


if TYPE_CHECKING:
    from cyberdelta.config.models import AppSettings

logger = get_logger(__name__)


class HealthChecks:
    """Health check implementations for service monitoring.

    This class contains the health check logic extracted from ServiceHealthMonitor
    to maintain file size limits while preserving exact business logic.
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize health check operations.

        Args:
            config: Application settings
        """
        self.config = config

        # Extract monitoring configuration
        self._monitoring_config = config.monitoring
        self._health_check_interval = self._monitoring_config.health_check_interval_seconds
        self._slow_response_threshold = self._monitoring_config.response_time_threshold_ms
        self._stale_data_threshold = self._monitoring_config.stale_data_threshold_seconds
        self._error_rate_threshold = self._monitoring_config.error_rate_threshold
        self._memory_threshold = self._monitoring_config.memory_threshold_mb
        self._cpu_threshold = self._monitoring_config.cpu_threshold_percent

        # Store registered services
        self._services: dict[str, HealthCheckable] = {}

        # Cache for last health checks
        self._last_health_checks: dict[str, ServiceHealthStatus] = {}

    def register_service(self, name: str, service: HealthCheckable) -> None:
        """Register a service for health monitoring.

        Args:
            name: Unique service name
            service: Service implementing HealthCheckable protocol

        Raises:
            ServiceAlreadyRegisteredError: If service with same name already registered
        """
        if name in self._services:
            raise ServiceAlreadyRegisteredError(name)

        self._services[name] = service
        logger.info(
            "service_registered_for_health_monitoring",
            service_name=name,
            service_type=getattr(service, "service_type", "unknown"),
        )

    def unregister_service(self, name: str) -> bool:
        """Unregister a service from health monitoring.

        Args:
            name: Service name to unregister

        Returns:
            True if service was unregistered, False if not found
        """
        if name in self._services:
            del self._services[name]
            if name in self._last_health_checks:
                del self._last_health_checks[name]
            logger.info("service_unregistered_from_health_monitoring", service_name=name)
            return True
        return False

    async def check_service_health(self, service_name: str) -> ServiceHealthStatus:
        """Check health of a specific service.

        Args:
            service_name: Name of service to check

        Returns:
            ServiceHealthStatus with check results
        """
        if service_name not in self._services:
            self._raise_service_not_registered_error(service_name)

        service = self._services[service_name]
        check_start = datetime.now(UTC)

        try:
            # Perform health check - returns HealthMetrics
            health_metric = await service.check_health()

            # Determine status based on metrics
            status = self._determine_health_status(service_name, health_metric)

            # Create health status
            health_status = ServiceHealthStatus(
                service_name=service_name,
                service_type=ServiceType.UNKNOWN,  # Extract from ExecutionStatistics if available
                is_healthy=status == HealthStatus.HEALTHY,
                is_running=True,  # If we got a response, service is running
                health_status=status,
                last_check_timestamp=check_start,
                response_time_ms=Decimal(str(health_metric.response_time_ms or 0.0)),
                error_count=health_metric.error_count,
                success_count=health_metric.success_count,
                memory_usage_mb=(
                    Decimal(str(health_metric.memory_usage_mb))
                    if health_metric.memory_usage_mb
                    else None
                ),
            )

            # Cache the result
            self._last_health_checks[service_name] = health_status

            # Log based on status
            if status == HealthStatus.HEALTHY:
                logger.debug(
                    "service_health_check_passed",
                    service_name=service_name,
                    response_time_ms=health_metric.response_time_ms,
                )
            else:
                logger.warning(
                    "service_health_issue_detected",
                    service_name=service_name,
                    status=status.value,
                )

        except (
            MonitoringError,
            ValueError,
            TypeError,
            AttributeError,
            KeyError,
            OSError,
            RuntimeError,
        ) as e:
            return self._create_error_status(service_name, check_start, e)
        else:
            return health_status

    def _create_error_status(
        self, service_name: str, check_start: datetime, error: Exception
    ) -> ServiceHealthStatus:
        """Create error status for failed health check.

        Args:
            service_name: Name of service
            check_start: When check started
            error: Exception that occurred

        Returns:
            ServiceHealthStatus with error information
        """
        # Create error status
        error_status = ServiceHealthStatus(
            service_name=service_name,
            service_type=ServiceType.UNKNOWN,
            is_healthy=False,
            is_running=False,
            health_status=HealthStatus.CRITICAL,
            last_check_timestamp=check_start,
            response_time_ms=Decimal(str((datetime.now(UTC) - check_start).total_seconds() * 1000)),
            error_count=1,
            success_count=0,
        )

        self._last_health_checks[service_name] = error_status

        logger.error(
            "service_health_check_failed",
            service_name=service_name,
            error=str(error),
            exc_info=error,
        )

        return error_status

    def _determine_health_status(self, service_name: str, metrics: HealthMetrics) -> HealthStatus:
        """Determine health status based on metrics.

        Args:
            service_name: Name of service
            metrics: Health metrics

        Returns:
            Health status
        """
        # Calculate error rate
        total_requests = metrics.error_count + metrics.success_count
        if total_requests > 0:
            error_rate = (metrics.error_count / total_requests) * 100

            # Check error rate thresholds
            if error_rate > self._error_rate_threshold * 100:  # Convert to percentage
                critical_threshold = 50.0  # From config in production
                if error_rate > critical_threshold:
                    return HealthStatus.CRITICAL
                return HealthStatus.UNHEALTHY

        # Check response time
        if metrics.response_time_ms and metrics.response_time_ms > self._slow_response_threshold:
            if metrics.response_time_ms > self._slow_response_threshold * 2:
                return HealthStatus.UNHEALTHY
            return HealthStatus.DEGRADED

        # Check resource usage
        if metrics.memory_usage_mb and metrics.memory_usage_mb > self._memory_threshold:
            return HealthStatus.UNHEALTHY

        if metrics.cpu_usage_percent and metrics.cpu_usage_percent > self._cpu_threshold:
            return HealthStatus.UNHEALTHY

        return HealthStatus.HEALTHY

    def _is_response_time_slow(self, service_name: str, response_time_ms: float) -> bool:
        """Check if response time is slow.

        Args:
            service_name: Service name
            response_time_ms: Response time in milliseconds

        Returns:
            True if response time exceeds threshold
        """
        # Could have service-specific thresholds
        threshold = self._slow_response_threshold

        # Example: Market data might have lower threshold
        if "market" in service_name.lower():
            threshold *= 0.5

        return response_time_ms > threshold

    def _check_error_rate(self, error_count: int, total_requests: int) -> tuple[float, bool]:
        """Check error rate against threshold.

        Args:
            error_count: Number of errors
            total_requests: Total number of requests

        Returns:
            Tuple of (error_rate, is_high)
        """
        if total_requests == 0:
            return 0.0, False

        error_rate = (error_count / total_requests) * 100
        is_high = error_rate > self._error_rate_threshold

        return error_rate, is_high

    def _is_data_stale(self, last_update: datetime) -> bool:
        """Check if data is stale based on last update time.

        Args:
            last_update: Last update timestamp

        Returns:
            True if data is stale
        """
        age = datetime.now(UTC) - last_update
        return age.total_seconds() > self._stale_data_threshold

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

    def get_registered_services(self) -> list[str]:
        """Get list of registered service names.

        Returns:
            List of service names
        """
        return list(self._services.keys())

    def _raise_service_not_registered_error(self, service_name: str) -> None:
        """Raise error for unregistered service.

        Args:
            service_name: Service name

        Raises:
            ServiceNotRegisteredError: With helpful message
        """
        raise ServiceNotRegisteredError(service_name, list(self._services.keys()))

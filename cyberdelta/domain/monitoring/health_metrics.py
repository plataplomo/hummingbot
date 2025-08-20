"""Health metrics collection and calculation.

This module contains metrics collection logic extracted from ServiceHealthMonitor
to maintain file size under 600 lines while keeping the same business logic.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import Field
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.monitoring import HealthStatus, ServiceType
from cyberdelta.models.monitoring.system_health_models import (
    HealthCheckDetails,
    OperationalThresholds,
    SystemMetrics,
)


# Constants for health thresholds
CRITICAL_ERROR_RATE_PERCENT = 50.0
MULTIPLE_ISSUES_THRESHOLD = 2


if TYPE_CHECKING:
    from cyberdelta.config.models import AppSettings

logger = get_logger(__name__)


@dataclass
class HealthMetrics:
    """Health metrics for a service.

    This is a Pydantic dataclass following CODING_STANDARDS.md requirements.
    All fields are properly typed for monitoring health data.
    """

    response_time_ms: float | None = Field(
        default=None, description="Response time in milliseconds"
    )
    error_count: int = Field(default=0, description="Number of errors")
    success_count: int = Field(default=0, description="Number of successful operations")
    last_activity: datetime | None = Field(default=None, description="Last activity timestamp")
    uptime_seconds: float | None = Field(default=None, description="Uptime in seconds")
    memory_usage_mb: float | None = Field(default=None, description="Memory usage in MB")
    cpu_usage_percent: float | None = Field(default=None, description="CPU usage percentage")
    is_running: bool | None = Field(default=None, description="Whether service is running")


class HealthMetricsCollector:
    """Metrics collection and calculation for health monitoring.

    This class contains the metrics collection logic extracted from ServiceHealthMonitor
    to maintain file size limits while preserving exact business logic.
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize metrics collector.

        Args:
            config: Application settings
        """
        self.config = config
        self._monitoring_config = config.monitoring

        # Extract thresholds from configuration
        self._stale_threshold = self._monitoring_config.stale_data_threshold_seconds
        self._response_time_threshold = self._monitoring_config.response_time_threshold_ms
        self._error_rate_threshold = self._monitoring_config.error_rate_threshold
        self._memory_threshold_mb = self._monitoring_config.memory_threshold_mb
        self._cpu_threshold_percent = self._monitoring_config.cpu_threshold_percent

        # Track service start times for uptime calculation
        self._service_start_times: dict[str, datetime] = {}

    async def collect_system_metrics(self) -> SystemMetrics:
        """Collect overall system metrics.

        Returns:
            SystemMetrics with current system state
        """
        # Get actual system metrics - fail fast if not available
        raise NotImplementedError(
            "System metrics collection not implemented. "
            "Must integrate with actual system monitoring service."
        )

    def check_resource_usage(
        self, memory_mb: float | None, cpu_percent: float | None
    ) -> tuple[bool, bool]:
        """Check if resource usage exceeds thresholds.

        Args:
            memory_mb: Memory usage in MB
            cpu_percent: CPU usage percentage

        Returns:
            Tuple of (memory_high, cpu_high)
        """
        memory_high = self.is_memory_usage_high(memory_mb)
        cpu_high = self.is_cpu_usage_high(cpu_percent)
        return memory_high, cpu_high

    def is_memory_usage_high(self, memory_mb: float | None) -> bool:
        """Check if memory usage exceeds threshold.

        Args:
            memory_mb: Memory usage in MB

        Returns:
            True if memory usage is high
        """
        if memory_mb is None:
            return False

        # Could have service-specific thresholds
        threshold = self._memory_threshold_mb

        # Example: Trading service might have higher threshold
        # This would come from config in production
        return memory_mb > threshold

    def is_cpu_usage_high(self, cpu_percent: float | None) -> bool:
        """Check if CPU usage exceeds threshold.

        Args:
            cpu_percent: CPU usage percentage

        Returns:
            True if CPU usage is high
        """
        if cpu_percent is None:
            return False

        # Could have service-specific thresholds
        threshold = self._cpu_threshold_percent

        # Example: Risk service might have lower threshold
        # This would come from config in production
        return cpu_percent > threshold

    def calculate_uptime(self, service_name: str) -> float | None:
        """Calculate service uptime in seconds.

        Args:
            service_name: Name of the service

        Returns:
            Uptime in seconds or None if not tracked
        """
        if service_name not in self._service_start_times:
            return None

        start_time = self._service_start_times[service_name]
        return (datetime.now(UTC) - start_time).total_seconds()

    def record_service_start(self, service_name: str) -> None:
        """Record service start time for uptime tracking.

        Args:
            service_name: Name of the service
        """
        self._service_start_times[service_name] = datetime.now(UTC)
        logger.debug(
            "service_start_recorded",
            service_name=service_name,
            start_time=self._service_start_times[service_name].isoformat(),
        )

    def calculate_error_rate(self, error_count: int, total_count: int) -> float:
        """Calculate error rate percentage.

        Args:
            error_count: Number of errors
            total_count: Total number of requests

        Returns:
            Error rate as percentage (0-100)
        """
        if total_count == 0:
            return 0.0

        return (error_count / total_count) * 100.0

    def is_error_rate_high(self, error_rate: float) -> bool:
        """Check if error rate exceeds threshold.

        Args:
            error_rate: Error rate percentage

        Returns:
            True if error rate is high
        """
        return error_rate > self._error_rate_threshold

    def is_response_time_slow(
        self, response_time_ms: float, service_type: ServiceType | None = None
    ) -> bool:
        """Check if response time exceeds threshold.

        Args:
            response_time_ms: Response time in milliseconds
            service_type: Optional service type for specific thresholds

        Returns:
            True if response time is slow
        """
        # Could have service-specific thresholds based on type
        threshold = self._response_time_threshold

        # Example: Market data service might have lower threshold
        if service_type == ServiceType.MARKET_DATA:
            threshold *= 0.5
        # Example: Risk service might have higher threshold
        elif service_type == ServiceType.RISK:
            threshold *= 1.5

        return response_time_ms > threshold

    def is_data_stale(self, last_update: datetime) -> bool:
        """Check if data is stale based on last update time.

        Args:
            last_update: Last update timestamp

        Returns:
            True if data is stale
        """
        age = (datetime.now(UTC) - last_update).total_seconds()
        return age > self._stale_threshold

    def extract_last_activity(self, details: HealthMetrics) -> datetime | None:
        """Extract last activity timestamp from service details.

        Args:
            details: Service health metrics

        Returns:
            Last activity timestamp or None
        """
        return details.last_activity

    def create_health_metrics(
        self,
        service_name: str,
        response_time_ms: float | None = None,
        error_count: int = 0,
        success_count: int = 0,
        last_activity: datetime | None = None,
        memory_usage_mb: float | None = None,
        cpu_usage_percent: float | None = None,
        is_running: bool | None = None,
    ) -> HealthMetrics:
        """Create health metrics object for a service.

        Args:
            service_name: Name of the service
            response_time_ms: Response time in milliseconds
            error_count: Number of errors
            success_count: Number of successes
            last_activity: Last activity timestamp
            memory_usage_mb: Memory usage in MB
            cpu_usage_percent: CPU usage percentage
            is_running: Whether service is running

        Returns:
            HealthMetrics object
        """
        uptime_seconds = self.calculate_uptime(service_name)

        return HealthMetrics(
            response_time_ms=response_time_ms,
            error_count=error_count,
            success_count=success_count,
            last_activity=last_activity,
            uptime_seconds=uptime_seconds,
            memory_usage_mb=memory_usage_mb,
            cpu_usage_percent=cpu_usage_percent,
            is_running=is_running,
        )

    def determine_health_status(
        self,
        metrics: HealthMetrics,
        service_type: ServiceType | None = None,
    ) -> HealthStatus:
        """Determine health status based on metrics.

        Args:
            metrics: Health metrics to evaluate
            service_type: Optional service type for specific logic

        Returns:
            Health status
        """
        # Check various health indicators
        total_requests = metrics.error_count + metrics.success_count

        # Critical conditions
        if total_requests > 0:
            error_rate = self.calculate_error_rate(metrics.error_count, total_requests)
            if error_rate > CRITICAL_ERROR_RATE_PERCENT:
                return HealthStatus.CRITICAL

        # Check resource usage
        if metrics.memory_usage_mb and self.is_memory_usage_high(metrics.memory_usage_mb):
            return HealthStatus.UNHEALTHY

        if metrics.cpu_usage_percent and self.is_cpu_usage_high(metrics.cpu_usage_percent):
            return HealthStatus.UNHEALTHY

        # Check response time
        if metrics.response_time_ms and self.is_response_time_slow(
            metrics.response_time_ms, service_type
        ):
            # Very slow response is unhealthy
            if metrics.response_time_ms > self._response_time_threshold * 2:
                return HealthStatus.UNHEALTHY
            # Moderately slow is degraded
            return HealthStatus.DEGRADED

        # Check staleness
        if metrics.last_activity and self.is_data_stale(metrics.last_activity):
            return HealthStatus.DEGRADED

        # Check error rate for non-critical levels
        if total_requests > 0:
            error_rate = self.calculate_error_rate(metrics.error_count, total_requests)
            if self.is_error_rate_high(error_rate):
                return HealthStatus.UNHEALTHY

        # All checks passed
        return HealthStatus.HEALTHY

    def create_operational_thresholds(self) -> OperationalThresholds:
        """Create operational thresholds from configuration.

        Returns:
            OperationalThresholds object
        """
        return OperationalThresholds(
            response_time_ms=self._response_time_threshold,
            error_rate=self._error_rate_threshold,
            stale_data_sec=self._stale_threshold,
        )

    def create_health_check_details(
        self, metrics: HealthMetrics, additional_details: HealthMetrics | None = None
    ) -> HealthCheckDetails:
        """Create health check details from metrics.

        Args:
            metrics: Health metrics
            additional_details: Additional service-specific details

        Returns:
            HealthCheckDetails object
        """
        # Build diagnostic info with issues found
        issues: list[str] = []

        if metrics.response_time_ms and self.is_response_time_slow(metrics.response_time_ms):
            issues.append(f"Slow response time: {metrics.response_time_ms}ms")

        if metrics.memory_usage_mb and self.is_memory_usage_high(metrics.memory_usage_mb):
            issues.append(f"High memory usage: {metrics.memory_usage_mb}MB")

        if metrics.cpu_usage_percent and self.is_cpu_usage_high(metrics.cpu_usage_percent):
            issues.append(f"High CPU usage: {metrics.cpu_usage_percent}%")

        total_requests = metrics.error_count + metrics.success_count
        if total_requests > 0:
            error_rate = self.calculate_error_rate(metrics.error_count, total_requests)
            if self.is_error_rate_high(error_rate):
                issues.append(f"High error rate: {error_rate:.1f}%")

        # Build diagnostic info string
        diagnostic_info = "Metrics collected: "
        diagnostic_info += f"response_time_ms={metrics.response_time_ms}, "
        diagnostic_info += f"error_count={metrics.error_count}, "
        diagnostic_info += f"success_count={metrics.success_count}, "
        diagnostic_info += f"uptime_seconds={metrics.uptime_seconds}, "
        diagnostic_info += f"memory_usage_mb={metrics.memory_usage_mb}, "
        diagnostic_info += f"cpu_usage_percent={metrics.cpu_usage_percent}"

        if issues:
            diagnostic_info += f"\nIssues found: {'; '.join(issues)}"

        if additional_details:
            diagnostic_info += f"\nAdditional details: {additional_details}"

        # Determine overall status based on issues
        status = HealthStatus.HEALTHY
        if len(issues) >= MULTIPLE_ISSUES_THRESHOLD:
            status = HealthStatus.UNHEALTHY
        elif issues:
            status = HealthStatus.DEGRADED

        return HealthCheckDetails(
            status=status,
            diagnostic_info=diagnostic_info,
            thresholds_used=OperationalThresholds(
                response_time_ms=float(self._response_time_threshold),
                error_rate=self._error_rate_threshold,
                stale_data_sec=float(self._stale_threshold),
            ),
        )

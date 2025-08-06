"""Typed models for system health and monitoring data.

This module provides typed Pydantic models to replace dict[str, Any] patterns
in health monitoring, ensuring type safety for system status reporting.
Used by both ServiceHealthMonitor and CircuitBreakerHealthMonitor.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field

from cyberdelta.enums.monitoring import ServiceType
from cyberdelta.models.trading.order_tracker_statistics import OrderTrackerStatistics


class HealthCheckDetails(BaseModel):
    """Type-safe health check details.

    This model replaces dict[str, Any] patterns in health check details
    to ensure type safety for additional health information.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Optional fields for flexible health check data
    - Type-safe threshold information
    """

    # Status information
    status: str | None = Field(default=None, description="Current status description")

    # Error information
    error_message: str | None = Field(
        default=None, description="Error message if health check failed"
    )

    timeout_sec: float | None = Field(default=None, description="Timeout duration in seconds")

    # Operational thresholds used in health check
    thresholds_used: OperationalThresholds | None = Field(
        default=None, description="Operational thresholds used for health determination"
    )

    # Additional diagnostic data
    diagnostic_info: str | None = Field(
        default=None, description="Additional diagnostic information"
    )

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class MonitoringConfiguration(BaseModel):
    """Type-safe monitoring configuration.

    This model replaces dict[str, Any] patterns in monitoring configuration
    to ensure type safety for system monitoring settings.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Type-safe configuration data
    """

    monitoring_enabled: bool = Field(description="Whether monitoring is enabled")

    check_interval_sec: float = Field(description="Health check interval in seconds")

    total_services: int = Field(description="Total number of services monitored")

    thresholds: OperationalThresholds = Field(description="Operational monitoring thresholds")

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class OperationalThresholds(BaseModel):
    """Type-safe operational thresholds for monitoring.

    This model provides specific threshold values used during monitoring operations.
    Different from HealthCheckThresholds (config) which are service-level ratios.

    IMPORTANT: Following CODING_STANDARDS.md:
    - All thresholds use appropriate numeric types
    """

    response_time_ms: float = Field(description="Response time threshold in milliseconds")

    error_rate: float = Field(description="Error rate threshold as decimal")

    stale_data_sec: float = Field(description="Data staleness threshold in seconds")

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class CircuitBreakerSystemHealth(BaseModel):
    """Type-safe circuit breaker system health.

    This model replaces dict[str, object] returns in circuit breaker health monitoring
    to ensure type safety for safety system health reporting.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    """

    status: str = Field(description="System status (healthy/degraded/impaired/critical)")

    health_ratio: float = Field(description="Ratio of healthy to total breakers")

    total_breakers: int = Field(description="Total number of circuit breakers")

    healthy_breakers: int = Field(description="Number of healthy (non-open) breakers")

    open_breakers: int = Field(description="Number of open circuit breakers")

    half_open_breakers: int = Field(description="Number of half-open circuit breakers")

    enabled: bool = Field(description="Whether circuit breaker system is enabled")

    configuration: CircuitBreakerConfiguration = Field(description="Circuit breaker configuration")

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class CircuitBreakerConfiguration(BaseModel):
    """Type-safe circuit breaker configuration.

    This model replaces nested dict[str, Any] patterns in circuit breaker configuration
    to ensure type safety for safety system settings.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Type-safe configuration data
    """

    global_failure_threshold: int = Field(description="Global consecutive failure threshold")

    cooldown_period_sec: int = Field(description="Cooldown period in seconds")

    per_service_enabled: bool = Field(description="Whether per-service breakers are enabled")

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class HealthStatus(Enum):
    """Health status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class ServiceHealthStatus(BaseModel):
    """Type-safe health status for individual services.

    This model replaces dict[str, Any] returns in service health checks
    to ensure type safety for service monitoring.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - Uses enums for service types
    """

    service_name: str = Field(description="Name of the service being monitored")

    service_type: ServiceType = Field(description="Type of service (execution, portfolio, etc.)")

    is_healthy: bool = Field(description="Whether service is currently healthy")

    is_running: bool = Field(description="Whether service is currently running")

    health_status: HealthStatus = Field(description="Current health status level")

    last_check_timestamp: datetime = Field(description="Timestamp of last health check")

    response_time_ms: Decimal = Field(description="Response time in milliseconds")

    # Optional service-specific metrics
    active_connections: int | None = Field(default=None, description="Number of active connections")

    error_count: int | None = Field(default=None, description="Number of errors since last reset")

    success_count: int | None = Field(default=None, description="Number of successful operations")

    memory_usage_mb: Decimal | None = Field(default=None, description="Memory usage in megabytes")

    # Health check specific data
    health_details: HealthCheckDetails = Field(
        default_factory=HealthCheckDetails, description="Additional health check details"
    )

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class SystemHealthReport(BaseModel):
    """Type-safe comprehensive system health report.

    This model replaces dict[str, Any] returns in system health monitoring
    to ensure type safety for overall system status.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - All performance metrics use Decimal type
    """

    overall_health_status: str = Field(
        description="Overall system health (healthy/degraded/unhealthy)"
    )

    report_timestamp: datetime = Field(description="UTC timestamp when report was generated")

    total_services_monitored: int = Field(description="Total number of services being monitored")

    healthy_services_count: int = Field(description="Number of healthy services")

    unhealthy_services_count: int = Field(description="Number of unhealthy services")

    # Service-specific health statuses
    service_statuses: dict[str, ServiceHealthStatus] = Field(
        description="Health status for each monitored service"
    )

    # System-level metrics
    system_metrics: SystemMetrics = Field(description="System-level performance metrics")

    # Configuration snapshot
    monitoring_configuration: MonitoringConfiguration = Field(
        description="Current monitoring configuration settings"
    )

    # Optional alerts and issues
    active_alerts: list[str] | None = Field(
        default=None, description="List of active system alerts"
    )

    critical_issues: list[str] | None = Field(
        default=None, description="List of critical issues requiring attention"
    )

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class SystemMetrics(BaseModel):
    """Type-safe system-level performance metrics.

    This model replaces dict[str, Any] patterns in system metrics collection
    to ensure type safety for performance monitoring.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - All metrics use Decimal type for precision
    """

    cpu_usage_percent: Decimal = Field(description="CPU usage as percentage")

    memory_usage_percent: Decimal = Field(description="Memory usage as percentage")

    disk_usage_percent: Decimal = Field(description="Disk usage as percentage")

    uptime_seconds: int = Field(description="System uptime in seconds")

    metrics_collection_timestamp: datetime = Field(description="When metrics were collected")

    # Optional detailed metrics
    load_average_1min: Decimal | None = Field(default=None, description="1-minute load average")

    load_average_5min: Decimal | None = Field(default=None, description="5-minute load average")

    available_memory_mb: Decimal | None = Field(
        default=None, description="Available memory in megabytes"
    )

    network_connections: int | None = Field(
        default=None, description="Number of active network connections"
    )

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class ExecutionStatistics(BaseModel):
    """Type-safe execution engine statistics.

    This model replaces dict[str, Any] returns in execution engine monitoring
    to ensure type safety for trading execution metrics.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - All monetary values use Decimal type
    """

    # Order tracking statistics (composed from OrderTracker)
    order_tracking: OrderTrackerStatistics = Field(description="Order tracking statistics")

    # Execution engine specific fields
    api_clients_available: int = Field(description="Number of available API clients")

    safe_mode_enabled: bool = Field(description="Whether safe mode is enabled")

    max_slippage_pct: Decimal = Field(description="Maximum allowed slippage percentage")

    max_retries: int = Field(description="Maximum retry attempts for orders")

    # Optional performance metrics
    average_execution_time_ms: Decimal | None = Field(
        default=None, description="Average order execution time in milliseconds"
    )

    recent_error_messages: list[str] | None = Field(
        default=None, description="Recent error messages for diagnostics"
    )

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class CircuitBreakerStatistics(BaseModel):
    """Type-safe circuit breaker statistics.

    This model replaces dict[str, object] returns in circuit breaker monitoring
    to ensure type safety for safety system metrics.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    """

    breaker_name: str = Field(description="Name of the circuit breaker")

    current_state: str = Field(description="Current breaker state (closed/open/half_open)")

    failure_count: int = Field(description="Number of consecutive failures")

    failure_threshold: int = Field(description="Threshold for opening breaker")

    success_count: int = Field(description="Number of successful operations")

    last_failure_timestamp: datetime | None = Field(
        default=None, description="Timestamp of last failure"
    )

    last_success_timestamp: datetime | None = Field(
        default=None, description="Timestamp of last success"
    )

    # State transition tracking
    state_changed_timestamp: datetime | None = Field(
        default=None, description="When breaker last changed state"
    )

    times_opened: int = Field(description="Total number of times breaker has opened")

    # Optional diagnostic data
    recent_errors: list[str] | None = Field(default=None, description="Recent error messages")

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True

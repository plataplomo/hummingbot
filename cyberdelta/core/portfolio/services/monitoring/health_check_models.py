"""Health check models and data structures."""

from __future__ import annotations

import time
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.core.portfolio.exceptions.service import HealthCheckValidationError


# Constants
REASONABLE_METRIC_MAX = 1e15  # Maximum reasonable metric value


def _health_status_dict_factory() -> dict[str, HealthStatus]:
    """Factory function that preserves dict[str, HealthStatus] type information.
    
    Returns:
        Empty dictionary typed as dict[str, HealthStatus]
    """
    return {}


class HealthStatus(Enum):
    """Health status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class HealthCheckDetails(BaseModel):
    """Health check details with validation."""

    total_services: int = Field(default=0, ge=0, description="Total number of services")
    healthy_count: int = Field(default=0, ge=0, description="Number of healthy services")
    degraded_count: int = Field(default=0, ge=0, description="Number of degraded services")
    unhealthy_count: int = Field(default=0, ge=0, description="Number of unhealthy services")
    uptime_seconds: float = Field(default=0.0, ge=0, description="Service uptime in seconds")
    last_check_duration_ms: float = Field(
        default=0.0, ge=0, description="Last check duration in milliseconds"
    )
    memory_usage_mb: float = Field(default=0.0, ge=0, description="Memory usage in MB")
    cpu_usage_percent: float = Field(default=0.0, ge=0, le=100, description="CPU usage percentage")
    error_count: int = Field(default=0, ge=0, description="Total error count")
    warning_count: int = Field(default=0, ge=0, description="Total warning count")

    @field_validator("uptime_seconds", "last_check_duration_ms", "memory_usage_mb", mode="before")
    @classmethod
    def validate_metrics(cls, v: float | str) -> float:
        """Validate metric values are finite and reasonable.
        
        Args:
            v: Metric value to validate
            
        Returns:
            Validated float value
            
        Raises:
            HealthCheckValidationError: If value is negative or exceeds reasonable limits
        """
        value: float = float(v)
        if not (0 <= value < REASONABLE_METRIC_MAX):  # Must be non-negative and reasonable
            raise HealthCheckValidationError(
                metric_type="metric_value",
                requirement="must be non-negative and finite",
                value=str(value),
            )
        return value

    @field_validator(
        "total_services",
        "healthy_count",
        "degraded_count",
        "unhealthy_count",
        "error_count",
        "warning_count",
        mode="before",
    )
    @classmethod
    def validate_counts(cls, v: str | float) -> int:
        """Validate count values are non-negative.
        
        Args:
            v: Count value to validate
            
        Returns:
            Validated integer count
            
        Raises:
            HealthCheckValidationError: If count is negative
        """
        value: int = int(v)
        if value < 0:
            raise HealthCheckValidationError(
                metric_type="count_value", requirement="must be non-negative", value=str(value)
            )
        return value


@dataclass
class HealthCheckResult:
    """Result of a health check."""

    status: HealthStatus
    service_name: str
    timestamp: float = Field(default_factory=time.time)
    message: str | None = None
    details: HealthCheckDetails = Field(default_factory=HealthCheckDetails)
    dependencies: dict[str, HealthStatus] = Field(default_factory=_health_status_dict_factory)

    @property
    def is_healthy(self) -> bool:
        """Check if service is healthy.
        
        Returns:
            True if service status is HEALTHY.
        """
        return self.status == HealthStatus.HEALTHY

    @property
    def is_degraded(self) -> bool:
        """Check if service is degraded.
        
        Returns:
            True if service status is DEGRADED.
        """
        return self.status == HealthStatus.DEGRADED

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation.
        
        Returns:
            Dictionary with all health check result fields
        """
        return {
            "status": self.status.value,
            "service_name": self.service_name,
            "timestamp": self.timestamp,
            "message": self.message,
            "details": self.details.model_dump(),
            "dependencies": {k: v.value for k, v in self.dependencies.items()},
        }


@runtime_checkable
class HealthCheckable(Protocol):
    """Protocol for services that support health checks."""

    async def check_health(self) -> HealthCheckResult:
        """Check service health.
        
        Returns:
            Health check result with current service status.
        """
        ...


@runtime_checkable
class DependencyHealthCheck(Protocol):
    """Protocol for checking dependency health."""

    async def check_dependency_health(self, dependency_name: str) -> HealthStatus:
        """Check health of a specific dependency.
        
        Args:
            dependency_name: Name of the dependency to check.
            
        Returns:
            Health status of the specified dependency.
        """
        ...
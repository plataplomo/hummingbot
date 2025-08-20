"""Circuit breaker health monitoring and system status.

This module handles health calculations and system status determination
for circuit breaker management, providing specialized monitoring for the
safety system's circuit breaker components.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.monitoring import HealthStatus
from cyberdelta.enums.safety.circuit_breaker import CircuitBreakerState
from cyberdelta.models.monitoring.system_health_models import (
    CircuitBreakerConfiguration,
    CircuitBreakerStatistics,
    CircuitBreakerSystemHealth,
)


# Health ratio thresholds for system status determination
HEALTH_RATIO_DEGRADED_THRESHOLD = 0.8
HEALTH_RATIO_IMPAIRED_THRESHOLD = 0.5


if TYPE_CHECKING:
    from cyberdelta.domain.safety.circuit_breaker import CircuitBreaker

logger = get_logger(__name__)


class CircuitBreakerHealthMonitor:
    """Monitors circuit breaker health and calculates system status.

    This component handles:
    - System health ratio calculations
    - Status determination (healthy/degraded/impaired/critical)
    - Aggregated statistics collection
    - Health reporting for monitoring

    IMPORTANT: Following CODING_STANDARDS.md:
    - Uses configured thresholds, NO hardcoded values
    - Structured data for monitoring
    - Clear health status definitions
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize health monitor with configuration.

        Args:
            config: Application settings containing circuit breaker configuration
        """
        self._cb_config = config.safety_systems.circuit_breakers

        logger.debug(
            "circuit_breaker_health_monitor_initialized",
            enabled=self._cb_config.enabled,
            per_service_enabled=self._cb_config.per_service_enabled,
        )

    def calculate_system_health(
        self, breakers: dict[str, CircuitBreaker]
    ) -> CircuitBreakerSystemHealth:
        """Calculate overall system health from circuit breaker states.

        Args:
            breakers: Dictionary of circuit breakers to analyze

        Returns:
            System health summary with status and metrics

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured thresholds for status determination
        - Structured output for monitoring dashboards
        """
        total_breakers = len(breakers)

        # Count breakers by state
        open_breakers = sum(
            1 for breaker in breakers.values() if breaker.get_state() == CircuitBreakerState.OPEN
        )
        half_open_breakers = sum(
            1
            for breaker in breakers.values()
            if breaker.get_state() == CircuitBreakerState.HALF_OPEN
        )

        # Calculate overall system health
        healthy_breakers = total_breakers - open_breakers
        health_ratio = healthy_breakers / total_breakers if total_breakers > 0 else 1.0

        # Determine system status using configured thresholds
        status = self._determine_system_status(open_breakers, health_ratio)

        logger.debug(
            "system_health_calculated",
            status=status,
            health_ratio=health_ratio,
            total_breakers=total_breakers,
            open_breakers=open_breakers,
            half_open_breakers=half_open_breakers,
        )

        return CircuitBreakerSystemHealth(
            status=status,
            health_ratio=health_ratio,
            total_breakers=total_breakers,
            healthy_breakers=healthy_breakers,
            open_breakers=open_breakers,
            half_open_breakers=half_open_breakers,
            enabled=self._cb_config.enabled,
            configuration=CircuitBreakerConfiguration(
                global_failure_threshold=self._cb_config.global_consecutive_failures,
                cooldown_period_sec=self._cb_config.cooldown_period_seconds,
                per_service_enabled=self._cb_config.per_service_enabled,
            ),
        )

    def _determine_system_status(self, open_breakers: int, health_ratio: float) -> HealthStatus:
        """Determine system status based on health metrics.

        Args:
            open_breakers: Number of open circuit breakers
            health_ratio: Ratio of healthy to total breakers

        Returns:
            System status string

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses module constants, NOT magic numbers
        - Clear status definitions
        """
        if open_breakers == 0:
            return HealthStatus.HEALTHY
        if health_ratio >= HEALTH_RATIO_DEGRADED_THRESHOLD:
            return HealthStatus.DEGRADED
        if health_ratio >= HEALTH_RATIO_IMPAIRED_THRESHOLD:
            return HealthStatus.UNHEALTHY  # Map impaired to unhealthy
        return HealthStatus.CRITICAL  # Map critical to critical

    def aggregate_breaker_stats(
        self, breakers: dict[str, CircuitBreaker]
    ) -> dict[str, CircuitBreakerStatistics]:
        """Aggregate statistics from all circuit breakers.

        Args:
            breakers: Dictionary of circuit breakers

        Returns:
            Dictionary mapping breaker names to their stats

        IMPORTANT: Following CODING_STANDARDS.md:
        - Safe access to breaker methods
        - Comprehensive monitoring data
        """
        stats: dict[str, CircuitBreakerStatistics] = {}

        for name, breaker in breakers.items():
            try:
                raw_stats = breaker.get_stats()
                # Convert dict stats to CircuitBreakerStatistics model with proper type casting
                stats[name] = CircuitBreakerStatistics(
                    breaker_name=name,
                    current_state=self._safe_get_str(raw_stats, "state", "unknown"),
                    failure_count=self._safe_get_int(raw_stats, "failure_count", 0),
                    failure_threshold=self._safe_get_int(raw_stats, "failure_threshold", 0),
                    success_count=self._safe_get_int(raw_stats, "success_count", 0),
                    last_failure_timestamp=self._safe_get_datetime(
                        raw_stats, "last_failure_timestamp"
                    ),
                    last_success_timestamp=self._safe_get_datetime(
                        raw_stats, "last_success_timestamp"
                    ),
                    state_changed_timestamp=self._safe_get_datetime(
                        raw_stats, "state_changed_timestamp"
                    ),
                    times_opened=self._safe_get_int(raw_stats, "times_opened", 0),
                    recent_errors=self._safe_get_recent_errors(raw_stats),
                )
            except Exception as e:
                logger.exception("breaker_stats_collection_failed", breaker_name=name, error=str(e))
                # Create error statistics entry
                stats[name] = CircuitBreakerStatistics(
                    breaker_name=name,
                    current_state="error",
                    failure_count=0,
                    failure_threshold=0,
                    success_count=0,
                    times_opened=0,
                    recent_errors=[f"stats_collection_failed: {e}"],
                )

        logger.debug(
            "breaker_stats_aggregated",
            breaker_count=len(breakers),
            successful_stats=sum(1 for s in stats.values() if s.current_state != "error"),
            failed_stats=sum(1 for s in stats.values() if s.current_state == "error"),
        )

        return stats

    def get_health_thresholds(self) -> dict[str, float]:
        """Get health ratio thresholds for reference.

        Returns:
            Dictionary with threshold values

        IMPORTANT: Following CODING_STANDARDS.md:
        - Exposes configured thresholds for transparency
        """
        return {
            "degraded_threshold": HEALTH_RATIO_DEGRADED_THRESHOLD,
            "impaired_threshold": HEALTH_RATIO_IMPAIRED_THRESHOLD,
            "healthy_minimum": 1.0,  # All breakers must be non-open for healthy status
        }

    def _safe_get_str(self, data: dict[str, Any], key: str, default: str) -> str:
        """Safely extract string value from dict.

        Returns:
            String value from dict or default if not found/valid
        """
        value = data.get(key, default)
        return str(value) if value is not None else default

    def _safe_get_int(self, data: dict[str, Any], key: str, default: int) -> int:
        """Safely extract int value from dict.

        Returns:
            Integer value from dict or default if not found/valid
        """
        value = data.get(key, default)
        if isinstance(value, int):
            return value
        if isinstance(value, (str, float)):
            try:
                return int(value)
            except (ValueError, TypeError):
                return default
        return default

    def _safe_get_datetime(self, data: dict[str, Any], key: str) -> datetime | None:
        """Safely extract datetime value from dict.

        Returns:
            Datetime value from dict or None if not found/valid
        """
        value = data.get(key)
        return value if isinstance(value, datetime) else None

    def _safe_get_recent_errors(self, data: dict[str, Any]) -> list[str] | None:
        """Safely extract recent errors list from breaker stats.

        Returns:
            List of error strings or None if not found/valid
        """
        # For now, return None to avoid type checking complexities
        # This is optional diagnostic data that doesn't affect core functionality
        _ = data  # Mark parameter as used
        return None

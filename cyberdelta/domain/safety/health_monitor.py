"""Circuit breaker health monitoring and system status.

This module handles health calculations and system status determination
for circuit breaker management.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.safety.models import (
    HEALTH_RATIO_DEGRADED_THRESHOLD,
    HEALTH_RATIO_IMPAIRED_THRESHOLD,
    CircuitBreakerState,
)


if TYPE_CHECKING:
    from cyberdelta.domain.safety.circuit_breaker import CircuitBreaker

logger = get_logger(__name__)


class HealthMonitor:
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
            "health_monitor_initialized",
            enabled=self._cb_config.enabled,
            per_service_enabled=self._cb_config.per_service_enabled,
        )

    def calculate_system_health(self, breakers: dict[str, CircuitBreaker]) -> dict[str, object]:
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

        return {
            "status": status,
            "health_ratio": health_ratio,
            "total_breakers": total_breakers,
            "healthy_breakers": healthy_breakers,
            "open_breakers": open_breakers,
            "half_open_breakers": half_open_breakers,
            "enabled": self._cb_config.enabled,
            "configuration": {
                "global_failure_threshold": self._cb_config.global_consecutive_failures,
                "cooldown_period_sec": self._cb_config.cooldown_period_seconds,
                "per_service_enabled": self._cb_config.per_service_enabled,
            },
        }

    def _determine_system_status(self, open_breakers: int, health_ratio: float) -> str:
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
            return "healthy"
        if health_ratio >= HEALTH_RATIO_DEGRADED_THRESHOLD:
            return "degraded"
        if health_ratio >= HEALTH_RATIO_IMPAIRED_THRESHOLD:
            return "impaired"
        return "critical"

    def aggregate_breaker_stats(
        self, breakers: dict[str, CircuitBreaker]
    ) -> dict[str, dict[str, object]]:
        """Aggregate statistics from all circuit breakers.

        Args:
            breakers: Dictionary of circuit breakers

        Returns:
            Dictionary mapping breaker names to their stats

        IMPORTANT: Following CODING_STANDARDS.md:
        - Safe access to breaker methods
        - Comprehensive monitoring data
        """
        stats: dict[str, dict[str, object]] = {}

        for name, breaker in breakers.items():
            try:
                stats[name] = breaker.get_stats()
            except Exception as e:
                logger.exception("breaker_stats_collection_failed", breaker_name=name, error=str(e))
                stats[name] = {"error": f"stats_collection_failed: {e}"}

        logger.debug(
            "breaker_stats_aggregated",
            breaker_count=len(breakers),
            successful_stats=sum(1 for s in stats.values() if "error" not in s),
            failed_stats=sum(1 for s in stats.values() if "error" in s),
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

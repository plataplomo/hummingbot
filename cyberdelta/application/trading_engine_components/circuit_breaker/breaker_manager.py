"""Circuit breaker manager for trading engine safety systems."""

from __future__ import annotations

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.safety.circuit_breaker import CircuitBreakerManager
from cyberdelta.models.monitoring.system_health_models import CircuitBreakerSystemHealth


logger = get_logger(__name__)


class BreakerManager:
    """Manages circuit breaker operations for the trading engine."""

    def __init__(self, circuit_breakers: CircuitBreakerManager) -> None:
        """Initialize breaker manager with circuit breaker manager.

        Args:
            circuit_breakers: Circuit breaker manager instance
        """
        self._circuit_breakers = circuit_breakers

    def reset_circuit_breaker(self, service_name: str) -> bool:
        """Reset a specific circuit breaker.

        Args:
            service_name: Name of service circuit breaker to reset

        Returns:
            True if reset successful, False otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit reset capability for operations
        - Returns success status for caller
        """
        success = self._circuit_breakers.reset_breaker(service_name)

        logger.info(
            "circuit_breaker_reset_requested",
            service_name=service_name,
            success=success,
            requested_by="trading_engine",
        )

        return success

    def reset_all_circuit_breakers(self) -> None:
        """Reset all circuit breakers.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Emergency reset capability
        - Logs action for audit trail
        """
        logger.warning(
            "all_circuit_breakers_reset_requested",
            requested_by="trading_engine",
            reason="manual_intervention",
        )

        self._circuit_breakers.reset_all()

    def get_circuit_breaker_health(self) -> CircuitBreakerSystemHealth:
        """Get circuit breaker system health.

        Returns:
            System health from circuit breaker perspective

        IMPORTANT: Following CODING_STANDARDS.md:
        - Dedicated health check method
        - Returns structured health data
        """
        return self._circuit_breakers.get_system_health()

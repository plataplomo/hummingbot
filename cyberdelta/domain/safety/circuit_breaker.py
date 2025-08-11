"""Circuit breaker implementation using modular components.

This module provides the main CircuitBreaker and CircuitBreakerManager classes
that orchestrate the modular components for safety system functionality.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.safety.circuit_breaker_health import CircuitBreakerHealthMonitor
from cyberdelta.domain.safety.failure_tracker import FailureTracker
from cyberdelta.domain.safety.state_manager import StateManager
from cyberdelta.enums.safety.circuit_breaker import CircuitBreakerState
from cyberdelta.models.exceptions.circuit_breaker import CircuitBreakerViolationError
from cyberdelta.models.monitoring.system_health_models import (
    CircuitBreakerStatistics,
    CircuitBreakerSystemHealth,
)


logger = get_logger(__name__)


class CircuitBreaker:
    """Circuit breaker using modular components for separation of concerns.

    This class orchestrates:
    - StateManager: Handles state transitions and recovery logic
    - FailureTracker: Tracks failures, successes, and analytics

    IMPORTANT: Following CODING_STANDARDS.md:
    - Uses composed components for single responsibility
    - ALL configuration from AppSettings
    - Structured logging throughout
    """

    def __init__(self, name: str, config: AppSettings) -> None:
        """Initialize circuit breaker with modular components.

        Args:
            name: Unique name for this circuit breaker
            config: Application settings containing circuit breaker configuration
        """
        self.name = name
        self.config = config

        # Initialize modular components
        self._state_manager = StateManager(name, config)
        self._failure_tracker = FailureTracker(name, config)

        logger.info("circuit_breaker_initialized", name=self.name, components_initialized=2)

    async def call(
        self,
        operation_name: str,
        func: Callable[..., object],
        *args: object,
        **kwargs: object,
    ) -> object:
        """Execute function with circuit breaker protection.

        Args:
            operation_name: Name of the operation for logging
            func: Function to execute
            *args: Positional arguments to pass to function
            **kwargs: Keyword arguments to pass to function

        Returns:
            Function result

        Raises:
            CircuitBreakerViolationError: If circuit breaker is open
        """
        # Check current state and determine if we can proceed
        await self._state_manager.check_state_transition()

        if self._state_manager.is_open():
            consecutive_failures = self._failure_tracker.get_consecutive_failures()
            logger.warning(
                "circuit_breaker_blocking_call",
                breaker_name=self.name,
                operation=operation_name,
                state=self._state_manager.get_state().value,
                consecutive_failures=consecutive_failures,
            )
            raise CircuitBreakerViolationError(
                self.name,
                self._state_manager.get_state(),
                f"Too many failures ({consecutive_failures})",
            )

        # Track the call for half-open state
        if self._state_manager.is_half_open():
            self._state_manager.track_half_open_call()

        try:
            logger.debug(
                "circuit_breaker_executing_call",
                breaker_name=self.name,
                operation=operation_name,
                state=self._state_manager.get_state().value,
                attempt_number=self._failure_tracker.get_total_calls() + 1,
            )

            # Execute the protected function
            result = (
                await func(*args, **kwargs)
                if asyncio.iscoroutinefunction(func)
                else func(*args, **kwargs)
            )

        except Exception as e:
            # Record failure and check if we should trip
            should_trip_closed, should_trip_half_open = await self._failure_tracker.record_failure(
                operation_name, e
            )

            current_state = self._state_manager.get_state()

            # Handle state transitions based on failure
            if current_state == CircuitBreakerState.CLOSED and should_trip_closed:
                logger.exception(
                    "circuit_breaker_tripped",
                    breaker_name=self.name,
                    operation=operation_name,
                    consecutive_failures=self._failure_tracker.get_consecutive_failures(),
                )
                self._state_manager.set_state_open(datetime.now(UTC))

            elif current_state == CircuitBreakerState.HALF_OPEN and should_trip_half_open:
                logger.warning(
                    "circuit_breaker_half_open_failed",
                    breaker_name=self.name,
                    operation=operation_name,
                )
                self._state_manager.set_state_open(datetime.now(UTC))

            raise
        else:
            # Record success and handle state changes
            await self._failure_tracker.record_success(
                operation_name, self._state_manager.get_state()
            )

            # Track success for half-open recovery
            if self._state_manager.is_half_open():
                self._state_manager.track_half_open_success()

            return result

    def get_state(self) -> CircuitBreakerState:
        """Get current circuit breaker state.

        Returns:
            Current circuit breaker state
        """
        return self._state_manager.get_state()

    def get_stats(self) -> dict[str, object]:
        """Get comprehensive circuit breaker statistics.

        Returns:
            Dictionary with current statistics
        """
        # Combine stats from both components
        state_info = self._state_manager.get_state_info()
        tracking_stats = self._failure_tracker.get_tracking_stats()

        return {"name": self.name, **state_info, **tracking_stats}

    def reset(self) -> None:
        """Reset circuit breaker to initial state.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Coordinates reset across all components
        - Logs reset action for audit trail
        """
        logger.info(
            "circuit_breaker_manual_reset",
            breaker_name=self.name,
            previous_state=self._state_manager.get_state().value,
            consecutive_failures=self._failure_tracker.get_consecutive_failures(),
        )

        self._state_manager.reset_state()
        self._failure_tracker.reset_tracking()


class CircuitBreakerManager:
    """Manages multiple circuit breakers using health monitoring.

    This class orchestrates:
    - CircuitBreaker instances: Individual breaker management
    - CircuitBreakerHealthMonitor: System health calculations and monitoring

    IMPORTANT: Following CODING_STANDARDS.md:
    - Centralized management of all circuit breakers
    - Configuration-driven behavior
    - Uses health monitoring for system status
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize circuit breaker manager with health monitoring.

        Args:
            config: Application settings containing circuit breaker configuration
        """
        self.config = config
        self._cb_config = config.safety_systems.circuit_breakers
        self._breakers: dict[str, CircuitBreaker] = {}
        self._enabled = self._cb_config.enabled

        # Initialize health monitor
        self._health_monitor = CircuitBreakerHealthMonitor(config)

        # Create service-specific circuit breakers if enabled
        if self._cb_config.per_service_enabled:
            self._create_service_breakers()

        logger.info(
            "circuit_breaker_manager_initialized",
            enabled=self._enabled,
            per_service_enabled=self._cb_config.per_service_enabled,
            breaker_count=len(self._breakers),
            health_monitoring_enabled=True,
        )

    def _create_service_breakers(self) -> None:
        """Create circuit breakers for each service.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit service protection
        - Same configuration for all services
        """
        services = [
            "portfolio_service",
            "market_data_service",
            "trading_service",
            "execution_engine",
            "risk_service",
            "signal_service",
            "strategy_service",
        ]

        for service in services:
            self._breakers[service] = CircuitBreaker(service, self.config)

        logger.info(
            "service_circuit_breakers_created",
            services=services,
            configuration_source="global_circuit_breaker_config",
        )

    def get_breaker(self, name: str) -> CircuitBreaker:
        """Get or create circuit breaker for a service.

        Args:
            name: Service name for circuit breaker

        Returns:
            CircuitBreaker instance
        """
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name, self.config)

            logger.info(
                "circuit_breaker_created_on_demand",
                breaker_name=name,
                total_breakers=len(self._breakers),
            )

        return self._breakers[name]

    async def protect(
        self,
        service_name: str,
        operation_name: str,
        func: Callable[..., object],
        *args: object,
        **kwargs: object,
    ) -> object:
        """Execute function with circuit breaker protection.

        Args:
            service_name: Name of the service
            operation_name: Name of the operation
            func: Function to execute
            *args: Positional arguments to pass to function
            **kwargs: Keyword arguments to pass to function

        Returns:
            Function result
        """
        if not self._enabled:
            # Circuit breakers disabled - execute directly
            return (
                await func(*args, **kwargs)
                if asyncio.iscoroutinefunction(func)
                else func(*args, **kwargs)
            )

        breaker = self.get_breaker(service_name)
        return await breaker.call(operation_name, func, *args, **kwargs)

    def get_all_stats(self) -> dict[str, CircuitBreakerStatistics]:
        """Get statistics for all circuit breakers.

        Returns:
            Dictionary mapping breaker names to their stats
        """
        return self._health_monitor.aggregate_breaker_stats(self._breakers)

    def get_system_health(self) -> CircuitBreakerSystemHealth:
        """Get overall system health from circuit breaker perspective.

        Returns:
            System health summary calculated by health monitor
        """
        return self._health_monitor.calculate_system_health(self._breakers)

    def reset_all(self) -> None:
        """Reset all circuit breakers.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Emergency reset capability
        - Logs reset action for audit
        """
        logger.warning(
            "resetting_all_circuit_breakers",
            breaker_count=len(self._breakers),
            reason="manual_reset_requested",
        )

        for breaker in self._breakers.values():
            breaker.reset()

        logger.info("all_circuit_breakers_reset_completed")

    def reset_breaker(self, name: str) -> bool:
        """Reset specific circuit breaker.

        Args:
            name: Name of circuit breaker to reset

        Returns:
            True if breaker was reset, False if not found
        """
        if name in self._breakers:
            self._breakers[name].reset()

            logger.info("circuit_breaker_reset_completed", breaker_name=name)
            return True
        logger.warning(
            "circuit_breaker_reset_failed",
            breaker_name=name,
            reason="breaker_not_found",
            available_breakers=list(self._breakers.keys()),
        )
        return False

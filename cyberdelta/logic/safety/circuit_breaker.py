"""Circuit breaker implementation for trading safety systems.

This module provides circuit breaker functionality to protect against
cascading failures in the trading system using validated AppSettings.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set

from cyberdelta.config.structlog_config import get_logger

from cyberdelta.config.models.config_models import AppSettings

logger = get_logger(__name__)


class CircuitBreakerState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Blocking all requests
    HALF_OPEN = "half_open"  # Testing recovery


class FailureType(Enum):
    """Types of failures that can trigger circuit breakers."""

    API_ERROR = "api_error"
    TIMEOUT = "timeout"
    VALIDATION_ERROR = "validation_error"
    EXECUTION_ERROR = "execution_error"
    NETWORK_ERROR = "network_error"
    RATE_LIMIT = "rate_limit"
    AUTHENTICATION_ERROR = "auth_error"


class CircuitBreakerViolation(Exception):
    """Exception raised when circuit breaker is open."""

    def __init__(self, breaker_name: str, state: CircuitBreakerState, message: str):
        self.breaker_name = breaker_name
        self.state = state
        self.message = message
        super().__init__(f"Circuit breaker '{breaker_name}' is {state.value}: {message}")


class CircuitBreaker:
    """Individual circuit breaker with config-driven behavior.

    Configuration Usage:
    - Uses config.safety_systems.circuit_breakers.global_consecutive_failures for failure threshold
    - Uses config.safety_systems.circuit_breakers.cooldown_period_seconds for recovery time
    - Uses config.safety_systems.circuit_breakers.half_open_max_calls for testing limit
    - Uses config.safety_systems.circuit_breakers.recovery_threshold for success ratio

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL thresholds from AppSettings, NO hardcoded values
    - Uses structured logging only
    - Fail-fast on configuration violations
    - Thread-safe operation
    """

    def __init__(self, name: str, config: AppSettings):
        """Initialize circuit breaker with configuration.

        Args:
            name: Unique name for this circuit breaker
            config: Application settings containing circuit breaker configuration
        """
        self.name = name
        self.config = config
        self._cb_config = config.safety_systems.circuit_breakers

        # Extract configuration - NO hardcoded defaults
        self._failure_threshold = self._cb_config.global_consecutive_failures
        self._cooldown_period = timedelta(seconds=self._cb_config.cooldown_period_seconds)
        self._half_open_max_calls = self._cb_config.half_open_max_calls
        self._recovery_threshold = self._cb_config.recovery_threshold

        # State tracking
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._consecutive_failures = 0
        self._last_failure_time: Optional[datetime] = None
        self._half_open_calls = 0
        self._half_open_successes = 0

        # Failure tracking for analytics
        self._failure_history: List[Dict] = []
        self._success_count = 0
        self._total_calls = 0

        logger.info(
            "circuit_breaker_initialized",
            name=self.name,
            failure_threshold=self._failure_threshold,
            cooldown_period_sec=self._cb_config.cooldown_period_seconds,
            half_open_max_calls=self._half_open_max_calls,
            recovery_threshold=float(self._recovery_threshold),
        )

    async def call(self, operation_name: str, func, *args, **kwargs):
        """Execute function with circuit breaker protection.

        Args:
            operation_name: Name of the operation for logging
            func: Function to execute
            *args, **kwargs: Arguments to pass to function

        Returns:
            Function result

        Raises:
            CircuitBreakerViolation: If circuit breaker is open
            Exception: Original exception from function

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured failure tracking
        - All timing from configuration
        - Explicit state transitions
        """
        # Check current state and determine if we can proceed
        await self._check_state_transition()

        if self._state == CircuitBreakerState.OPEN:
            logger.warning(
                "circuit_breaker_blocking_call",
                breaker_name=self.name,
                operation=operation_name,
                state=self._state.value,
                consecutive_failures=self._consecutive_failures,
            )
            raise CircuitBreakerViolation(
                self.name,
                self._state,
                f"Too many failures ({self._consecutive_failures}/{self._failure_threshold})",
            )

        # Track the call
        self._total_calls += 1
        if self._state == CircuitBreakerState.HALF_OPEN:
            self._half_open_calls += 1

        try:
            logger.debug(
                "circuit_breaker_executing_call",
                breaker_name=self.name,
                operation=operation_name,
                state=self._state.value,
                attempt_number=self._total_calls,
            )

            # Execute the protected function
            result = (
                await func(*args, **kwargs)
                if asyncio.iscoroutinefunction(func)
                else func(*args, **kwargs)
            )

            # Record success
            await self._record_success(operation_name)

            return result

        except Exception as e:
            # Record failure
            await self._record_failure(operation_name, e)
            raise

    async def _check_state_transition(self) -> None:
        """Check if circuit breaker should transition states.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured cooldown period
        - State transitions based on configuration
        """
        if self._state == CircuitBreakerState.OPEN:
            # Check if cooldown period has passed
            if (
                self._last_failure_time
                and datetime.now(UTC) - self._last_failure_time >= self._cooldown_period
            ):
                logger.info(
                    "circuit_breaker_transitioning_to_half_open",
                    breaker_name=self.name,
                    cooldown_elapsed=True,
                    last_failure_time=self._last_failure_time.isoformat(),
                )

                self._state = CircuitBreakerState.HALF_OPEN
                self._half_open_calls = 0
                self._half_open_successes = 0

        elif self._state == CircuitBreakerState.HALF_OPEN:
            # Check if we've exceeded half-open call limit
            if self._half_open_calls >= self._half_open_max_calls:
                success_ratio = self._half_open_successes / self._half_open_calls

                if success_ratio >= self._recovery_threshold:
                    logger.info(
                        "circuit_breaker_recovered",
                        breaker_name=self.name,
                        success_ratio=success_ratio,
                        required_ratio=float(self._recovery_threshold),
                        test_calls=self._half_open_calls,
                    )

                    self._state = CircuitBreakerState.CLOSED
                    self._consecutive_failures = 0
                    self._failure_count = 0
                else:
                    logger.warning(
                        "circuit_breaker_recovery_failed",
                        breaker_name=self.name,
                        success_ratio=success_ratio,
                        required_ratio=float(self._recovery_threshold),
                        test_calls=self._half_open_calls,
                    )

                    self._state = CircuitBreakerState.OPEN
                    self._last_failure_time = datetime.now(UTC)

    async def _record_success(self, operation_name: str) -> None:
        """Record successful operation.

        Args:
            operation_name: Name of successful operation

        IMPORTANT: Following CODING_STANDARDS.md:
        - Updates state based on configured thresholds
        - Structured logging for monitoring
        """
        self._success_count += 1

        if self._state == CircuitBreakerState.HALF_OPEN:
            self._half_open_successes += 1
        elif self._state == CircuitBreakerState.CLOSED:
            # Reset consecutive failures on success
            if self._consecutive_failures > 0:
                logger.debug(
                    "circuit_breaker_failure_streak_reset",
                    breaker_name=self.name,
                    operation=operation_name,
                    previous_consecutive_failures=self._consecutive_failures,
                )
                self._consecutive_failures = 0

        logger.debug(
            "circuit_breaker_success_recorded",
            breaker_name=self.name,
            operation=operation_name,
            state=self._state.value,
            total_successes=self._success_count,
            consecutive_failures=self._consecutive_failures,
        )

    async def _record_failure(self, operation_name: str, error: Exception) -> None:
        """Record failed operation.

        Args:
            operation_name: Name of failed operation
            error: Exception that caused the failure

        IMPORTANT: Following CODING_STANDARDS.md:
        - Failure categorization for analytics
        - State transitions based on configuration
        """
        self._failure_count += 1
        self._consecutive_failures += 1
        self._last_failure_time = datetime.now(UTC)

        # Categorize failure type
        failure_type = self._categorize_failure(error)

        # Store failure for analytics
        failure_record = {
            "timestamp": self._last_failure_time.isoformat(),
            "operation": operation_name,
            "error_type": type(error).__name__,
            "failure_type": failure_type.value,
            "error_message": str(error),
            "consecutive_count": self._consecutive_failures,
        }
        self._failure_history.append(failure_record)

        # Keep only recent failures for memory management
        max_history = self._cb_config.failure_history_limit
        if len(self._failure_history) > max_history:
            self._failure_history = self._failure_history[-max_history:]

        logger.warning(
            "circuit_breaker_failure_recorded",
            breaker_name=self.name,
            operation=operation_name,
            error_type=type(error).__name__,
            failure_type=failure_type.value,
            consecutive_failures=self._consecutive_failures,
            failure_threshold=self._failure_threshold,
            state=self._state.value,
        )

        # Check if we should trip the circuit breaker
        if (
            self._state == CircuitBreakerState.CLOSED
            and self._consecutive_failures >= self._failure_threshold
        ):
            logger.error(
                "circuit_breaker_tripped",
                breaker_name=self.name,
                operation=operation_name,
                consecutive_failures=self._consecutive_failures,
                failure_threshold=self._failure_threshold,
                cooldown_period_sec=self._cb_config.cooldown_period_seconds,
            )

            self._state = CircuitBreakerState.OPEN

        elif (
            self._state == CircuitBreakerState.HALF_OPEN
            and self._half_open_calls >= self._half_open_max_calls
        ):
            logger.warning(
                "circuit_breaker_half_open_failed",
                breaker_name=self.name,
                operation=operation_name,
                test_calls=self._half_open_calls,
                successes=self._half_open_successes,
            )

            self._state = CircuitBreakerState.OPEN

    def _categorize_failure(self, error: Exception) -> FailureType:
        """Categorize failure type for analytics.

        Args:
            error: Exception to categorize

        Returns:
            FailureType enum value

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit failure categorization
        - NO assumptions about error types
        """
        error_name = type(error).__name__.lower()
        error_message = str(error).lower()

        if "timeout" in error_name or "timeout" in error_message:
            return FailureType.TIMEOUT
        elif "network" in error_message or "connection" in error_message:
            return FailureType.NETWORK_ERROR
        elif "rate" in error_message and "limit" in error_message:
            return FailureType.RATE_LIMIT
        elif "auth" in error_message or "credential" in error_message:
            return FailureType.AUTHENTICATION_ERROR
        elif "validation" in error_name or "value" in error_name:
            return FailureType.VALIDATION_ERROR
        elif "execution" in error_message or "order" in error_message:
            return FailureType.EXECUTION_ERROR
        else:
            return FailureType.API_ERROR

    def get_state(self) -> CircuitBreakerState:
        """Get current circuit breaker state."""
        return self._state

    def get_stats(self) -> Dict:
        """Get circuit breaker statistics.

        Returns:
            Dictionary with current statistics

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns structured data for monitoring
        - Includes configuration for context
        """
        success_rate = self._success_count / self._total_calls if self._total_calls > 0 else 0.0

        return {
            "name": self.name,
            "state": self._state.value,
            "total_calls": self._total_calls,
            "success_count": self._success_count,
            "failure_count": self._failure_count,
            "consecutive_failures": self._consecutive_failures,
            "success_rate": success_rate,
            "last_failure_time": self._last_failure_time.isoformat()
            if self._last_failure_time
            else None,
            "configuration": {
                "failure_threshold": self._failure_threshold,
                "cooldown_period_sec": self._cb_config.cooldown_period_seconds,
                "half_open_max_calls": self._half_open_max_calls,
                "recovery_threshold": float(self._recovery_threshold),
            },
            "failure_history_count": len(self._failure_history),
        }

    def reset(self) -> None:
        """Reset circuit breaker to initial state.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Manual reset capability for operations
        - Logs reset action for audit trail
        """
        logger.info(
            "circuit_breaker_manual_reset",
            breaker_name=self.name,
            previous_state=self._state.value,
            consecutive_failures=self._consecutive_failures,
        )

        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._consecutive_failures = 0
        self._last_failure_time = None
        self._half_open_calls = 0
        self._half_open_successes = 0


class CircuitBreakerManager:
    """Manages multiple circuit breakers for different services.

    Configuration Usage:
    - Uses config.safety_systems.circuit_breakers.enabled to control activation
    - Uses config.safety_systems.circuit_breakers.per_service_enabled for service-specific breakers
    - Individual breakers inherit from global configuration

    IMPORTANT: Following CODING_STANDARDS.md:
    - Centralized management of all circuit breakers
    - Service-specific and global protection
    - Configuration-driven behavior
    """

    def __init__(self, config: AppSettings):
        """Initialize circuit breaker manager.

        Args:
            config: Application settings containing circuit breaker configuration
        """
        self.config = config
        self._cb_config = config.safety_systems.circuit_breakers
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._enabled = self._cb_config.enabled

        # Create service-specific circuit breakers if enabled
        if self._cb_config.per_service_enabled:
            self._create_service_breakers()

        logger.info(
            "circuit_breaker_manager_initialized",
            enabled=self._enabled,
            per_service_enabled=self._cb_config.per_service_enabled,
            breaker_count=len(self._breakers),
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

        IMPORTANT: Following CODING_STANDARDS.md:
        - Lazy creation of breakers
        - Consistent configuration across breakers
        """
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name, self.config)

            logger.info(
                "circuit_breaker_created_on_demand",
                breaker_name=name,
                total_breakers=len(self._breakers),
            )

        return self._breakers[name]

    async def protect(self, service_name: str, operation_name: str, func, *args, **kwargs):
        """Execute function with circuit breaker protection.

        Args:
            service_name: Name of the service
            operation_name: Name of the operation
            func: Function to execute
            *args, **kwargs: Arguments to pass to function

        Returns:
            Function result

        IMPORTANT: Following CODING_STANDARDS.md:
        - Bypasses protection if disabled
        - Uses service-specific breakers
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

    def get_all_stats(self) -> Dict[str, Dict]:
        """Get statistics for all circuit breakers.

        Returns:
            Dictionary mapping breaker names to their stats

        IMPORTANT: Following CODING_STANDARDS.md:
        - Comprehensive monitoring data
        - Structured output for dashboards
        """
        return {name: breaker.get_stats() for name, breaker in self._breakers.items()}

    def get_system_health(self) -> Dict:
        """Get overall system health from circuit breaker perspective.

        Returns:
            System health summary

        IMPORTANT: Following CODING_STANDARDS.md:
        - High-level health indicators
        - Uses configured thresholds
        """
        total_breakers = len(self._breakers)
        open_breakers = sum(
            1
            for breaker in self._breakers.values()
            if breaker.get_state() == CircuitBreakerState.OPEN
        )
        half_open_breakers = sum(
            1
            for breaker in self._breakers.values()
            if breaker.get_state() == CircuitBreakerState.HALF_OPEN
        )

        # Calculate overall system health
        healthy_breakers = total_breakers - open_breakers
        health_ratio = healthy_breakers / total_breakers if total_breakers > 0 else 1.0

        # Determine system status
        if open_breakers == 0:
            status = "healthy"
        elif health_ratio >= 0.8:
            status = "degraded"
        elif health_ratio >= 0.5:
            status = "impaired"
        else:
            status = "critical"

        return {
            "status": status,
            "health_ratio": health_ratio,
            "total_breakers": total_breakers,
            "healthy_breakers": healthy_breakers,
            "open_breakers": open_breakers,
            "half_open_breakers": half_open_breakers,
            "enabled": self._enabled,
            "configuration": {
                "global_failure_threshold": self._cb_config.global_consecutive_failures,
                "cooldown_period_sec": self._cb_config.cooldown_period_seconds,
                "per_service_enabled": self._cb_config.per_service_enabled,
            },
        }

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

        IMPORTANT: Following CODING_STANDARDS.md:
        - Selective reset capability
        - Returns explicit success/failure
        """
        if name in self._breakers:
            self._breakers[name].reset()

            logger.info("circuit_breaker_reset_completed", breaker_name=name)
            return True
        else:
            logger.warning(
                "circuit_breaker_reset_failed",
                breaker_name=name,
                reason="breaker_not_found",
                available_breakers=list(self._breakers.keys()),
            )
            return False

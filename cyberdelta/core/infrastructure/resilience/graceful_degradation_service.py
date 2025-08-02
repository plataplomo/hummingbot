"""Graceful degradation service for fallback handling and service degradation."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, TypeVar, cast

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.infrastructure.services.base_service import BaseService

logger = get_logger(__name__)

T = TypeVar("T", bound=object)


class GracefulDegradationService(BaseService):
    """Service for managing graceful degradation and fallback mechanisms."""

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__("graceful_degradation_service", config)
        self._raw_config = config or {}
        self.logger = get_logger(__name__)
        
        # Fallback management
        self.fallback_handlers: dict[str, Callable[..., Awaitable[Any]]] = {}
        self.degradation_modes: dict[str, bool] = {}
        self.service_priorities: dict[str, int] = {}
        
        # Fallback statistics
        self.fallback_usage_count: dict[str, int] = {}
        self.primary_failure_count: dict[str, int] = {}

    async def _initialize_service(self) -> None:
        """Initialize graceful degradation service."""
        self.logger.info("Initializing graceful degradation service")

    async def _shutdown_service(self) -> None:
        """Shutdown graceful degradation service."""
        self.logger.info("Shutting down graceful degradation service")

    async def _start_internal(self) -> None:
        """Start internal degradation operations."""
        pass

    async def _stop_internal(self) -> None:
        """Stop internal degradation operations."""
        pass

    def register_fallback(
        self,
        service_name: str,
        fallback_handler: Callable[..., Awaitable[Any]],
        priority: int = 5,
    ) -> None:
        """Register a fallback handler for a service."""
        self.fallback_handlers[service_name] = fallback_handler
        self.service_priorities[service_name] = priority
        self.degradation_modes[service_name] = False
        self.fallback_usage_count[service_name] = 0
        self.primary_failure_count[service_name] = 0

        self.logger.info("fallback_registered", service_name=service_name, priority=priority)

    def unregister_fallback(self, service_name: str) -> None:
        """Unregister fallback handler for a service."""
        self.fallback_handlers.pop(service_name, None)
        self.service_priorities.pop(service_name, None)
        self.degradation_modes.pop(service_name, None)
        self.fallback_usage_count.pop(service_name, None)
        self.primary_failure_count.pop(service_name, None)

        self.logger.info("fallback_unregistered", service_name=service_name)

    def enable_degradation(self, service_name: str) -> None:
        """Enable degradation mode for a service."""
        if service_name in self.degradation_modes:
            was_enabled = self.degradation_modes[service_name]
            self.degradation_modes[service_name] = True
            
            if not was_enabled:
                self.logger.warning(
                    "degradation_mode_enabled",
                    service_name=service_name,
                    priority=self.service_priorities.get(service_name, 0),
                )

    def disable_degradation(self, service_name: str) -> None:
        """Disable degradation mode for a service."""
        if service_name in self.degradation_modes:
            was_enabled = self.degradation_modes[service_name]
            self.degradation_modes[service_name] = False
            
            if was_enabled:
                self.logger.info("degradation_mode_disabled", service_name=service_name)

    def is_degradation_enabled(self, service_name: str) -> bool:
        """Check if degradation mode is enabled for a service."""
        return self.degradation_modes.get(service_name, False)

    async def execute_with_fallback(
        self,
        service_name: str,
        primary_func: Callable[..., Awaitable[T]],
        *args: object,
        **kwargs: object,
    ) -> T:
        """Execute function with fallback capability."""
        # Check if degradation mode is enabled
        if self.degradation_modes.get(service_name, False):
            fallback_handler = self.fallback_handlers.get(service_name)
            if fallback_handler:
                self.logger.info("using_fallback_handler", service_name=service_name)
                self.fallback_usage_count[service_name] += 1
                result = await fallback_handler(*args, **kwargs)
                return cast(T, result)

        # Try primary function
        try:
            return await primary_func(*args, **kwargs)
        except Exception as e:
            self.primary_failure_count[service_name] = self.primary_failure_count.get(service_name, 0) + 1
            
            # Enable degradation and try fallback
            self.enable_degradation(service_name)

            fallback_handler = self.fallback_handlers.get(service_name)
            if fallback_handler:
                self.logger.warning(
                    "primary_function_failed_using_fallback", 
                    service_name=service_name,
                    error=str(e),
                )
                self.fallback_usage_count[service_name] += 1
                fallback_result = await fallback_handler(*args, **kwargs)
                return cast(T, fallback_result)
            
            self.logger.exception("no_fallback_available", service_name=service_name)
            raise

    def get_degradation_status(self) -> dict[str, Any]:
        """Get current degradation status for all services."""
        return {
            "degradation_modes": self.degradation_modes.copy(),
            "service_priorities": self.service_priorities.copy(),
            "registered_fallbacks": list(self.fallback_handlers.keys()),
            "fallback_usage_stats": self.fallback_usage_count.copy(),
            "primary_failure_stats": self.primary_failure_count.copy(),
        }

    def get_service_degradation_info(self, service_name: str) -> dict[str, Any]:
        """Get degradation information for a specific service."""
        return {
            "service_name": service_name,
            "degradation_enabled": self.degradation_modes.get(service_name, False),
            "has_fallback": service_name in self.fallback_handlers,
            "priority": self.service_priorities.get(service_name, 0),
            "fallback_usage_count": self.fallback_usage_count.get(service_name, 0),
            "primary_failure_count": self.primary_failure_count.get(service_name, 0),
        }

    def reset_statistics(self, service_name: str | None = None) -> None:
        """Reset degradation statistics."""
        if service_name:
            self.fallback_usage_count[service_name] = 0
            self.primary_failure_count[service_name] = 0
            self.logger.info("degradation_stats_reset", service_name=service_name)
        else:
            self.fallback_usage_count.clear()
            self.primary_failure_count.clear()
            self.logger.info("all_degradation_stats_reset")

    def get_critical_services(self) -> list[str]:
        """Get list of critical services (high priority) in degradation mode."""
        critical_services = []
        for service_name, priority in self.service_priorities.items():
            if priority >= 8 and self.degradation_modes.get(service_name, False):
                critical_services.append(service_name)
        return critical_services

    def get_degradation_summary(self) -> dict[str, Any]:
        """Get summary of degradation status across all services."""
        total_services = len(self.degradation_modes)
        degraded_services = sum(1 for enabled in self.degradation_modes.values() if enabled)
        services_with_fallbacks = len(self.fallback_handlers)
        
        total_fallback_usage = sum(self.fallback_usage_count.values())
        total_primary_failures = sum(self.primary_failure_count.values())
        
        return {
            "total_services": total_services,
            "degraded_services": degraded_services,
            "healthy_services": total_services - degraded_services,
            "services_with_fallbacks": services_with_fallbacks,
            "degradation_percentage": (degraded_services / total_services * 100) if total_services > 0 else 0,
            "total_fallback_usage": total_fallback_usage,
            "total_primary_failures": total_primary_failures,
            "critical_services_degraded": len(self.get_critical_services()),
        }

    def bulk_enable_degradation(self, service_names: list[str]) -> None:
        """Enable degradation mode for multiple services."""
        for service_name in service_names:
            self.enable_degradation(service_name)
        self.logger.warning("bulk_degradation_enabled", services=service_names)

    def bulk_disable_degradation(self, service_names: list[str]) -> None:
        """Disable degradation mode for multiple services."""
        for service_name in service_names:
            self.disable_degradation(service_name)
        self.logger.info("bulk_degradation_disabled", services=service_names)
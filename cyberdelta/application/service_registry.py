"""Service registry for dependency injection.

This module provides the service registry for managing service dependencies
with explicit registration and type-safe retrieval.
"""

from __future__ import annotations

from typing import Any, Dict, Type, TypeVar, cast

from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class ServiceRegistry:
    """Central service registry for dependency injection.

    Provides type-safe service registration and retrieval with explicit
    interface contracts.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO auto-discovery - explicit registration only
    - NO defaults - services must be explicitly registered
    - Fail fast on missing services
    """

    def __init__(self) -> None:
        """Initialize empty service registry."""
        self._services: Dict[Type[Any], Any] = {}

    def register(self, interface: Type[T], implementation: T) -> None:
        """Register service implementation for an interface.

        Args:
            interface: Service interface type (typically a Protocol or ABC)
            implementation: Concrete implementation of the interface

        Raises:
            ValueError: If interface is already registered
            TypeError: If implementation doesn't match interface
        """
        # Check if already registered
        if interface in self._services:
            raise ValueError(f"Service already registered for interface {interface.__name__}")

        if not isinstance(implementation, type(implementation)):
            raise TypeError(f"Implementation must be an instance, got {type(implementation)}")

        self._services[interface] = implementation

        logger.info(
            "service_registered",
            interface=interface.__name__,
            implementation=implementation.__class__.__name__,
            total_services=len(self._services),
        )

    def get(self, interface: Type[T]) -> T:
        """Get service implementation for an interface.

        Args:
            interface: Service interface type to retrieve

        Returns:
            Service implementation

        Raises:
            ValueError: If no implementation registered for interface
        """
        if interface not in self._services:
            raise ValueError(
                f"No implementation registered for interface {interface.__name__}. "
                f"Available interfaces: {list(self._services.keys())}"
            )

        implementation = self._services[interface]

        logger.debug(
            "service_retrieved",
            interface=interface.__name__,
            implementation=implementation.__class__.__name__,
        )

        return cast(T, implementation)

    def is_registered(self, interface: Type[Any]) -> bool:
        """Check if an interface has a registered implementation.

        Args:
            interface: Interface type to check

        Returns:
            True if registered, False otherwise
        """
        return interface in self._services

    def unregister(self, interface: Type[Any]) -> None:
        """Unregister a service interface.

        Args:
            interface: Interface type to unregister

        Raises:
            ValueError: If interface is not registered
        """
        if interface not in self._services:
            raise ValueError(
                f"Cannot unregister - no implementation found for interface {interface.__name__}"
            )

        implementation = self._services.pop(interface)

        logger.info(
            "service_unregistered",
            interface=interface.__name__,
            implementation=implementation.__class__.__name__,
            remaining_services=len(self._services),
        )

    def clear(self) -> None:
        """Clear all registered services.

        Used primarily for testing and shutdown scenarios.
        """
        service_count = len(self._services)
        self._services.clear()

        logger.info("service_registry_cleared", services_removed=service_count)

    def get_registered_interfaces(self) -> list[Type[Any]]:
        """Get list of all registered interface types.

        Returns:
            List of registered interface types
        """
        return list(self._services.keys())

    def get_service_count(self) -> int:
        """Get total number of registered services.

        Returns:
            Number of registered services
        """
        return len(self._services)

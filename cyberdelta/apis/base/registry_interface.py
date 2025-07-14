"""Registry pattern interfaces for service component management.

This module provides generic registry interfaces that enable dependency injection
and loose coupling between service components.
"""

from abc import ABC, abstractmethod
from typing import TypeVar


T = TypeVar("T")


class IComponentRegistry[T](ABC):
    """Generic interface for component registries.

    Provides a standard pattern for registering and retrieving
    components by name or type, enabling dependency injection.
    """

    @abstractmethod
    def register(self, name: str, component: T) -> None:
        """Register a component with the given name.

        Args:
            name: Unique identifier for the component
            component: The component instance to register

        Raises:
            ValueError: If name is already registered
        """

    @abstractmethod
    def get(self, name: str) -> T:
        """Retrieve a component by name.

        Args:
            name: Identifier of the component to retrieve

        Returns:
            The registered component

        Raises:
            KeyError: If name is not registered
        """

    @abstractmethod
    def is_registered(self, name: str) -> bool:
        """Check if a component is registered.

        Args:
            name: Identifier to check

        Returns:
            True if component is registered, False otherwise
        """

    @abstractmethod
    def list_registered(self) -> list[str]:
        """List all registered component names.

        Returns:
            List of registered component identifiers
        """

    @abstractmethod
    def unregister(self, name: str) -> T:
        """Unregister and return a component.

        Args:
            name: Identifier of the component to unregister

        Returns:
            The unregistered component

        Raises:
            KeyError: If name is not registered
        """


class BaseComponentRegistry(IComponentRegistry[T]):
    """Base implementation of component registry with common functionality."""

    def __init__(self) -> None:
        """Initialize an empty component registry."""
        self._components: dict[str, T] = {}

    def register(self, name: str, component: T) -> None:
        """Register a component with the given name."""
        if name in self._components:
            msg = f"Component '{name}' is already registered"
            raise ValueError(msg)

        self._components[name] = component

    def get(self, name: str) -> T:
        """Retrieve a component by name."""
        if name not in self._components:
            available = ", ".join(self._components.keys())
            msg = f"Component '{name}' not found. Available: {available}"
            raise KeyError(msg)

        return self._components[name]

    def is_registered(self, name: str) -> bool:
        """Check if a component is registered."""
        return name in self._components

    def list_registered(self) -> list[str]:
        """List all registered component names."""
        return list(self._components.keys())

    def unregister(self, name: str) -> T:
        """Unregister and return a component."""
        if name not in self._components:
            msg = f"Component '{name}' not found"
            raise KeyError(msg)

        return self._components.pop(name)

    def clear(self) -> None:
        """Clear all registered components."""
        self._components.clear()

    def __len__(self) -> int:
        """Return number of registered components."""
        return len(self._components)

    def __contains__(self, name: str) -> bool:
        """Check if component is registered using 'in' operator."""
        return name in self._components

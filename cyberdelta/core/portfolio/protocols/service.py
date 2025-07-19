"""Service lifecycle protocols for portfolio module."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ServiceLifecycle(Protocol):
    """Protocol for services with managed lifecycle.
    
    This protocol defines the interface for services that have
    a well-defined lifecycle with initialization, running state,
    and proper shutdown procedures.
    """
    
    @property
    def is_initialized(self) -> bool:
        """Check if the service has been initialized.
        
        Returns:
            True if the service has been properly initialized
        """
        ...
    
    @property
    def is_running(self) -> bool:
        """Check if the service is currently running.
        
        Returns:
            True if the service is currently running and operational
        """
        ...
    
    async def initialize(self) -> None:
        """Initialize the service.
        
        This method should be called before start() and should
        set up any required resources or configurations.
        """
        ...
    
    async def start(self) -> None:
        """Start the service.
        
        This method begins service operation. The service should
        be initialized before calling this method.
        """
        ...
    
    async def stop(self) -> None:
        """Stop the service.
        
        This method gracefully stops service operation and
        cleans up any resources.
        """
        ...
    
    async def health_check(self) -> bool:
        """Perform a health check on the service.
        
        Returns:
            True if the service is healthy and operational
        """
        ...


@runtime_checkable
class Cacheable(Protocol):
    """Protocol for objects that support caching.
    
    This protocol defines the interface for objects that can
    manage their own cache lifecycle and invalidation.
    """
    
    def invalidate_cache(self) -> None:
        """Invalidate the object's cache.
        
        This method should clear any cached data and force
        fresh computation on next access.
        """
        ...
    
    def warm_cache(self) -> None:
        """Warm the object's cache.
        
        This method should pre-populate cache with commonly
        accessed data to improve performance.
        """
        ...
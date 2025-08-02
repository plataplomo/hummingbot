"""Base service pattern for all portfolio services."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.portfolio_types.protocols import ServiceLifecycle


class BaseService(BaseModel, ServiceLifecycle):
    """Base class for all portfolio services."""

    service_name: str = Field(..., description="Name of the service")
    _initialized: bool = Field(default=False, description="Service initialization state")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def initialize(self) -> None:
        """Initialize the service."""
        if self._initialized:
            return

        await self._initialize_service()
        self._initialized = True

    async def shutdown(self) -> None:
        """Shutdown the service."""
        if not self._initialized:
            return

        await self._shutdown_service()
        self._initialized = False

    @abstractmethod
    async def _initialize_service(self) -> None:
        """Service-specific initialization."""
        pass

    @abstractmethod
    async def _shutdown_service(self) -> None:
        """Service-specific shutdown."""
        pass

    @property
    def is_initialized(self) -> bool:
        """Check if service is initialized."""
        return self._initialized

    def get_service_name(self) -> str:
        """Get the service name."""
        return self.service_name

    def get_service_status(self) -> dict[str, Any]:
        """Get service status information."""
        return {
            "name": self.service_name,
            "initialized": self._initialized,
            "status": "running" if self._initialized else "stopped",
        }
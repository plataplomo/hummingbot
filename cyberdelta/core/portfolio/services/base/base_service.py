"""Base service class for all portfolio infrastructure services."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from collections.abc import Mapping

logger = get_logger(__name__)


class BasePortfolioService(ABC):
    """Abstract base class for all portfolio infrastructure services.

    Provides common functionality for service lifecycle management,
    health checks, and proper shutdown procedures.
    """

    def __init__(self, name: str, config: Mapping[str, Any] | None = None) -> None:
        """Initialize the base service.

        Args:
            name: Human-readable name for this service
            config: Optional configuration dictionary
        """
        self.name = name
        self.config = config or {}
        self._lock = asyncio.Lock()
        self._running = False
        self._health_check_enabled = True

        logger.info(
            "service_created",
            service_name=name,
            service_type=self.__class__.__name__,
        )

    async def start(self) -> None:
        """Start the service. Called once during startup."""
        async with self._lock:
            if self._running:
                return

            await self._start_internal()
            self._running = True

            logger.info(
                "service_started",
                service_name=self.name,
            )

    async def stop(self) -> None:
        """Stop the service. Called during system shutdown."""
        async with self._lock:
            if not self._running:
                return

            await self._stop_internal()
            self._running = False

            logger.info(
                "service_stopped",
                service_name=self.name,
            )

    @abstractmethod
    async def _start_internal(self) -> None:
        """Internal startup logic. Override in subclasses."""

    @abstractmethod
    async def _stop_internal(self) -> None:
        """Internal shutdown logic. Override in subclasses."""

    async def health_check(self) -> bool:
        """Perform a health check on the service.

        Returns:
            True if service is healthy, False otherwise
        """
        if not self._health_check_enabled:
            return True

        try:
            return await self._health_check_internal()
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            logger.warning(
                "service_health_check_failed",
                service_name=self.name,
                error=str(e),
            )
            return False

    async def _health_check_internal(self) -> bool:
        """Internal health check logic. Override in subclasses."""
        return self._running

    @property
    def is_running(self) -> bool:
        """Check if the service is running."""
        return self._running

    def _ensure_running(self) -> None:
        """Raise an error if the service is not running."""
        if not self._running:
            raise RuntimeError

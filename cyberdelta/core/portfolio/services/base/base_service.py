"""Base service class for all portfolio infrastructure services."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, field_validator

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions import EmptyServiceNameError, InvalidServiceTimeoutError


# Constants
MAX_TIMEOUT_SECONDS = 3600  # 1 hour maximum timeout


if TYPE_CHECKING:
    from collections.abc import Mapping


class ServiceConfiguration(BaseModel):
    """Base configuration for portfolio services with validation."""

    name: str = Field(min_length=1, description="Service name")
    health_check_enabled: bool = Field(default=True, description="Enable health checks")
    health_check_interval_seconds: float = Field(
        default=30.0, gt=0, le=3600, description="Health check interval"
    )
    startup_timeout_seconds: float = Field(
        default=30.0, gt=0, le=300, description="Startup timeout"
    )
    shutdown_timeout_seconds: float = Field(
        default=30.0, gt=0, le=300, description="Shutdown timeout"
    )
    log_level: str = Field(
        default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$", description="Log level"
    )
    enable_metrics: bool = Field(default=True, description="Enable metrics collection")

    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate service name is non-empty."""
        if not v or not v.strip():
            raise EmptyServiceNameError
        return v.strip()

    @field_validator(
        "health_check_interval_seconds",
        "startup_timeout_seconds",
        "shutdown_timeout_seconds",
        mode="before",
    )
    @classmethod
    def validate_timeouts(cls, v: float) -> float:
        """Validate timeout values are reasonable."""
        value: float = float(v)
        if not (0 < value <= MAX_TIMEOUT_SECONDS):  # Between 0 and 1 hour
            raise InvalidServiceTimeoutError(timeout=value, max_timeout=MAX_TIMEOUT_SECONDS)
        return value


logger = get_logger(__name__)


class BasePortfolioService(ABC):
    """Abstract base class for all portfolio infrastructure services.

    Provides common functionality for service lifecycle management,
    health checks, and proper shutdown procedures.
    """

    def __init__(self, name: str, config: Mapping[str, object] | None = None) -> None:
        """Initialize the base service.

        Args:
            name: Human-readable name for this service
            config: Optional configuration dictionary
        """
        self.name = name

        # Convert raw config to validated ServiceConfiguration
        config_dict = dict(config) if config else {}
        config_dict["name"] = name  # Ensure name is set

        try:
            self.config = ServiceConfiguration.model_validate(config_dict)
        except (ValueError, TypeError, AttributeError) as e:
            # Fallback to default config with provided name if validation fails
            logger.warning(
                "service_config_validation_failed",
                service_name=name,
                error=str(e),
                fallback_to_default=True,
            )
            self.config = ServiceConfiguration(name=name)

        self._lock = asyncio.Lock()
        self._running = False
        self._health_check_enabled = self.config.health_check_enabled

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

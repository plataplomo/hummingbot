"""Service lifecycle management base class with proper startup/shutdown handling."""

from __future__ import annotations

import asyncio
import contextlib
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.service import (
    ServiceCleanupError,
    ServiceInitializationFailedError,
    ServiceInitializationStateError,
    ServiceShutdownFailedError,
    ServiceShutdownTimeoutError,
    ServiceStartFailedError,
    ServiceStartStateError,
    ServiceStartupTimeoutError,
)


# Type-preserving factory function
def _str_list_factory() -> list[str]:
    """Factory function that preserves list[str] type information."""
    return []


if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine


logger = get_logger(__name__)


class ServiceStatus(Enum):
    """Service lifecycle states."""

    CREATED = "created"
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    DESTROYED = "destroyed"


@dataclass
class ServiceMetrics:
    """Metrics for service lifecycle."""

    initialization_time_ms: float = 0.0
    startup_time_ms: float = 0.0
    shutdown_time_ms: float = 0.0
    total_uptime_seconds: float = 0.0
    restart_count: int = 0
    error_count: int = 0
    last_error: str | None = None
    last_error_time: float | None = None


@dataclass
class ServiceHealthInfo:
    """Service health information."""

    status: ServiceStatus
    is_healthy: bool
    last_health_check: float
    health_issues: list[str] = field(default_factory=_str_list_factory)
    metrics: ServiceMetrics = field(default_factory=ServiceMetrics)


class ServiceLifecycle(ABC):
    """Base class for services with proper lifecycle management."""

    def __init__(self, service_name: str, config: dict[str, Any] | None = None) -> None:
        """Initialize service lifecycle.

        Args:
            service_name: Name of the service
            config: Service configuration
        """
        self.service_name = service_name
        self.config = config or {}
        self._status = ServiceStatus.CREATED
        self._metrics = ServiceMetrics()
        self._start_time: float | None = None
        self._health_check_task: asyncio.Task[None] | None = None
        self._shutdown_handlers: list[Callable[[], Coroutine[Any, Any, None]]] = []
        self._startup_handlers: list[Callable[[], Coroutine[Any, Any, None]]] = []

        # Configuration
        self._health_check_interval = float(self.config.get("health_check_interval", 30.0))
        self._startup_timeout = float(self.config.get("startup_timeout", 30.0))
        self._shutdown_timeout = float(self.config.get("shutdown_timeout", 30.0))
        self._enable_health_checks = bool(self.config.get("enable_health_checks", True))

        logger.info(
            "service_lifecycle_created",
            service_name=service_name,
            health_check_interval=self._health_check_interval,
            startup_timeout=self._startup_timeout,
            shutdown_timeout=self._shutdown_timeout,
        )

    @property
    def status(self) -> ServiceStatus:
        """Get current service status."""
        return self._status

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._status == ServiceStatus.RUNNING

    @property
    def is_healthy(self) -> bool:
        """Check if service is healthy."""
        return self._status == ServiceStatus.RUNNING and self._check_health_internal()

    @abstractmethod
    async def _initialize(self) -> None:
        """Initialize service resources.

        This method should set up any resources needed by the service
        but not start any background tasks.
        """
        ...

    @abstractmethod
    async def _start(self) -> None:
        """Start service operations.

        This method should start any background tasks or operations.
        """
        ...

    @abstractmethod
    async def _stop(self) -> None:
        """Stop service operations.

        This method should stop all background tasks and operations.
        """
        ...

    @abstractmethod
    async def _cleanup(self) -> None:
        """Clean up service resources.

        This method should release all resources held by the service.
        """
        ...

    @abstractmethod
    async def _check_health(self) -> tuple[bool, list[str]]:
        """Check service health.

        Returns:
            Tuple of (is_healthy, list_of_issues)
        """
        ...

    async def initialize(self) -> None:
        """Initialize the service with proper error handling."""
        if self._status not in {ServiceStatus.CREATED, ServiceStatus.STOPPED, ServiceStatus.FAILED}:
            raise ServiceInitializationStateError(
                service_name=self.service_name,
                current_state=self._status.value,
            )

        self._status = ServiceStatus.INITIALIZING
        start_time = time.time()

        try:
            logger.info("service_initializing", service_name=self.service_name)
            await self._initialize()
            self._status = ServiceStatus.INITIALIZED
            self._metrics.initialization_time_ms = (time.time() - start_time) * 1000
            logger.info(
                "service_initialized",
                service_name=self.service_name,
                duration_ms=self._metrics.initialization_time_ms,
            )
        except Exception as e:
            self._status = ServiceStatus.FAILED
            self._metrics.error_count += 1
            self._metrics.last_error = str(e)
            self._metrics.last_error_time = time.time()
            logger.exception(
                "service_initialization_failed",
                service_name=self.service_name,
                error=str(e),
            )
            raise ServiceInitializationFailedError(
                service_name=self.service_name,
                cause=str(e),
            ) from e

    async def start(self) -> None:
        """Start the service with proper error handling."""
        if self._status != ServiceStatus.INITIALIZED:
            if self._status == ServiceStatus.CREATED:
                await self.initialize()
            else:
                raise ServiceStartStateError(
                    service_name=self.service_name,
                    current_state=self._status.value,
                )

        self._status = ServiceStatus.STARTING
        start_time = time.time()

        try:
            logger.info("service_starting", service_name=self.service_name)

            # Run startup handlers
            for handler in self._startup_handlers:
                await handler()

            # Start the service with timeout
            await asyncio.wait_for(self._start(), timeout=self._startup_timeout)

            self._status = ServiceStatus.RUNNING
            self._start_time = time.time()
            self._metrics.startup_time_ms = (time.time() - start_time) * 1000

            # Start health checks if enabled
            if self._enable_health_checks:
                self._health_check_task = asyncio.create_task(self._health_check_loop())

            logger.info(
                "service_started",
                service_name=self.service_name,
                duration_ms=self._metrics.startup_time_ms,
            )
        except TimeoutError as e:
            self._status = ServiceStatus.FAILED
            self._metrics.error_count += 1
            self._metrics.last_error = "Startup timeout"
            self._metrics.last_error_time = time.time()
            logger.exception(
                "service_startup_timeout",
                service_name=self.service_name,
                timeout=self._startup_timeout,
            )
            raise ServiceStartupTimeoutError(
                service_name=self.service_name,
                timeout_seconds=self._startup_timeout,
            ) from e
        except Exception as e:
            self._status = ServiceStatus.FAILED
            self._metrics.error_count += 1
            self._metrics.last_error = str(e)
            self._metrics.last_error_time = time.time()
            logger.exception(
                "service_startup_failed",
                service_name=self.service_name,
                error=str(e),
            )
            raise ServiceStartFailedError(
                service_name=self.service_name,
                cause=str(e),
            ) from e

    async def stop(self) -> None:
        """Stop the service with proper error handling."""
        if self._status not in {ServiceStatus.RUNNING, ServiceStatus.FAILED}:
            logger.warning(
                "service_already_stopped",
                service_name=self.service_name,
                current_state=self._status.value,
            )
            return

        self._status = ServiceStatus.STOPPING
        start_time = time.time()

        try:
            logger.info("service_stopping", service_name=self.service_name)

            # Cancel health checks
            if self._health_check_task:
                self._health_check_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._health_check_task
                self._health_check_task = None

            # Stop the service with timeout
            await asyncio.wait_for(self._stop(), timeout=self._shutdown_timeout)

            # Run shutdown handlers
            for handler in self._shutdown_handlers:
                try:
                    await handler()
                except (
                    ValueError,
                    TypeError,
                    KeyError,
                    AttributeError,
                    ArithmeticError,
                    RuntimeError,
                ) as e:
                    logger.warning(
                        "shutdown_handler_error",
                        service_name=self.service_name,
                        error=str(e),
                    )

            # Update metrics
            if self._start_time:
                self._metrics.total_uptime_seconds += time.time() - self._start_time
                self._start_time = None

            self._status = ServiceStatus.STOPPED
            self._metrics.shutdown_time_ms = (time.time() - start_time) * 1000

            logger.info(
                "service_stopped",
                service_name=self.service_name,
                duration_ms=self._metrics.shutdown_time_ms,
            )
        except TimeoutError as e:
            logger.exception(
                "service_shutdown_timeout",
                service_name=self.service_name,
                timeout=self._shutdown_timeout,
            )
            # Force status to stopped even on timeout
            self._status = ServiceStatus.STOPPED
            raise ServiceShutdownTimeoutError(
                service_name=self.service_name,
                timeout_seconds=self._shutdown_timeout,
            ) from e
        except Exception as e:
            self._status = ServiceStatus.STOPPED
            logger.exception(
                "service_shutdown_error",
                service_name=self.service_name,
                error=str(e),
            )
            raise ServiceShutdownFailedError(
                service_name=self.service_name,
                cause=str(e),
            ) from e

    async def restart(self) -> None:
        """Restart the service."""
        logger.info("service_restarting", service_name=self.service_name)

        if self._status == ServiceStatus.RUNNING:
            await self.stop()

        await self.cleanup()
        await self.initialize()
        await self.start()

        self._metrics.restart_count += 1
        logger.info(
            "service_restarted",
            service_name=self.service_name,
            restart_count=self._metrics.restart_count,
        )

    async def cleanup(self) -> None:
        """Clean up service resources."""
        if self._status == ServiceStatus.RUNNING:
            await self.stop()

        if self._status != ServiceStatus.STOPPED:
            logger.warning(
                "cleanup_in_unexpected_state",
                service_name=self.service_name,
                current_state=self._status.value,
            )

        try:
            logger.info("service_cleaning_up", service_name=self.service_name)
            await self._cleanup()
            self._status = ServiceStatus.DESTROYED
            logger.info("service_cleaned_up", service_name=self.service_name)
        except Exception as e:
            logger.exception(
                "service_cleanup_error",
                service_name=self.service_name,
                error=str(e),
            )
            raise ServiceCleanupError(
                service_name=self.service_name,
                cause=str(e),
            ) from e

    def add_startup_handler(self, handler: Callable[[], Coroutine[Any, Any, None]]) -> None:
        """Add a startup handler."""
        self._startup_handlers.append(handler)

    def add_shutdown_handler(self, handler: Callable[[], Coroutine[Any, Any, None]]) -> None:
        """Add a shutdown handler."""
        self._shutdown_handlers.append(handler)

    def _check_health_internal(self) -> bool:
        """Internal health check wrapper."""
        try:
            # Run synchronous health check
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're in an async context, we can't run async code synchronously
                # Return True and rely on the async health check loop
                return True
            is_healthy, _ = loop.run_until_complete(self._check_health())
        except Exception:
            logger.exception(
                "health_check_error",
                service_name=self.service_name,
            )
            return False
        else:
            return is_healthy

    async def _health_check_loop(self) -> None:
        """Background health check loop."""
        while self._status == ServiceStatus.RUNNING:
            try:
                is_healthy, issues = await self._check_health()

                if not is_healthy:
                    logger.warning(
                        "service_unhealthy",
                        service_name=self.service_name,
                        issues=issues,
                    )

                await asyncio.sleep(self._health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception(
                    "health_check_loop_error",
                    service_name=self.service_name,
                )
                await asyncio.sleep(self._health_check_interval)

    def get_health_info(self) -> ServiceHealthInfo:
        """Get current health information."""
        try:
            is_healthy, issues = asyncio.run(self._check_health())
        except (
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
            ArithmeticError,
            RuntimeError,
        ) as e:
            is_healthy = False
            issues = [f"Health check failed: {e!s}"]

        return ServiceHealthInfo(
            status=self._status,
            is_healthy=is_healthy and self._status == ServiceStatus.RUNNING,
            last_health_check=time.time(),
            health_issues=issues,
            metrics=self._metrics,
        )

    def get_metrics(self) -> dict[str, Any]:
        """Get service metrics."""
        current_uptime = 0.0
        if self._start_time and self._status == ServiceStatus.RUNNING:
            current_uptime = time.time() - self._start_time

        return {
            "service_name": self.service_name,
            "status": self._status.value,
            "initialization_time_ms": self._metrics.initialization_time_ms,
            "startup_time_ms": self._metrics.startup_time_ms,
            "shutdown_time_ms": self._metrics.shutdown_time_ms,
            "total_uptime_seconds": self._metrics.total_uptime_seconds + current_uptime,
            "current_uptime_seconds": current_uptime,
            "restart_count": self._metrics.restart_count,
            "error_count": self._metrics.error_count,
            "last_error": self._metrics.last_error,
            "last_error_time": self._metrics.last_error_time,
        }

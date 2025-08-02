"""Health check orchestration service."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.monitoring.health.health_check_models import (
    HealthCheckable,
    HealthCheckDetails,
    HealthCheckResult,
    HealthStatus,
)


if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

logger = get_logger(__name__)


class HealthCheckOrchestrator:
    """Service for aggregating and monitoring health checks."""

    def __init__(self, check_interval: float = 30.0) -> None:
        """Initialize health check orchestrator.
        
        Args:
            check_interval: Interval between health checks in seconds.
        """
        self._services: dict[str, HealthCheckable] = {}
        self._check_interval = check_interval
        self._latest_results: dict[str, HealthCheckResult] = {}
        self._health_check_task: asyncio.Task[None] | None = None
        self._callbacks: list[Callable[[HealthCheckResult], Coroutine[Any, Any, None]]] = []

        logger.info("health_check_orchestrator_initialized", check_interval=check_interval)

    def register_service(self, service_name: str, service: HealthCheckable) -> None:
        """Register a service for health monitoring.
        
        Args:
            service_name: Name to identify the service.
            service: Service instance that implements HealthCheckable.
        """
        self._services[service_name] = service
        logger.info("health_check_service_registered", service_name=service_name)

    def register_callback(
        self, callback: Callable[[HealthCheckResult], Coroutine[Any, Any, None]]
    ) -> None:
        """Register a callback for health status changes.
        
        Args:
            callback: Async callback function to call on health status changes.
        """
        self._callbacks.append(callback)

    async def start(self) -> None:
        """Start health check monitoring."""
        if self._health_check_task is None:
            self._health_check_task = asyncio.create_task(self._health_check_loop())
            logger.info("health_check_monitoring_started")

    async def stop(self) -> None:
        """Stop health check monitoring."""
        if self._health_check_task:
            self._health_check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._health_check_task
            self._health_check_task = None
            logger.info("health_check_monitoring_stopped")

    async def check_all(self) -> dict[str, HealthCheckResult]:
        """Check health of all registered services.
        
        Returns:
            Dictionary mapping service names to their health check results
        """
        results: dict[str, HealthCheckResult] = {}

        # Check all services concurrently
        tasks = {
            service_name: asyncio.create_task(self._check_service(service_name, service))
            for service_name, service in self._services.items()
        }

        for service_name, task in tasks.items():
            try:
                result = await task
                results[service_name] = result
                self._latest_results[service_name] = result

                # Notify callbacks on status change
                await self._notify_callbacks(result)

            except (RuntimeError, ValueError, TypeError, OSError, AttributeError) as e:
                logger.exception(
                    "health_check_error",
                    service_name=service_name,
                    error=str(e),
                )
                results[service_name] = HealthCheckResult(
                    status=HealthStatus.UNKNOWN,
                    service_name=service_name,
                    message=f"Health check failed: {e!s}",
                )

        return results

    async def get_aggregate_status(self) -> HealthCheckResult:
        """Get aggregated health status of all services.
        
        Returns:
            Aggregate health check result combining all service statuses
        """
        if not self._latest_results:
            return HealthCheckResult(
                status=HealthStatus.UNKNOWN,
                service_name="aggregate",
                message="No health checks performed yet",
            )

        # Determine aggregate status
        statuses = [result.status for result in self._latest_results.values()]

        if all(s == HealthStatus.HEALTHY for s in statuses):
            aggregate_status = HealthStatus.HEALTHY
            message = "All services healthy"
        elif any(s == HealthStatus.UNHEALTHY for s in statuses):
            aggregate_status = HealthStatus.UNHEALTHY
            unhealthy_count = sum(1 for s in statuses if s == HealthStatus.UNHEALTHY)
            message = f"{unhealthy_count} service(s) unhealthy"
        elif any(s == HealthStatus.DEGRADED for s in statuses):
            aggregate_status = HealthStatus.DEGRADED
            degraded_count = sum(1 for s in statuses if s == HealthStatus.DEGRADED)
            message = f"{degraded_count} service(s) degraded"
        else:
            aggregate_status = HealthStatus.UNKNOWN
            message = "Unable to determine aggregate status"

        dependencies: dict[str, HealthStatus] = {
            name: result.status for name, result in self._latest_results.items()
        }

        return HealthCheckResult(
            status=aggregate_status,
            service_name="aggregate",
            message=message,
            dependencies=dependencies,
            details=HealthCheckDetails(
                total_services=len(self._services),
                healthy_count=sum(1 for s in statuses if s == HealthStatus.HEALTHY),
                degraded_count=sum(1 for s in statuses if s == HealthStatus.DEGRADED),
                unhealthy_count=sum(1 for s in statuses if s == HealthStatus.UNHEALTHY),
            ),
        )

    def get_latest_results(self) -> dict[str, HealthCheckResult]:
        """Get the latest health check results for all services.
        
        Returns:
            Dictionary of service names to their latest health check results.
        """
        return self._latest_results.copy()

    def get_service_count(self) -> int:
        """Get the number of registered services.
        
        Returns:
            Number of services registered for health monitoring.
        """
        return len(self._services)

    async def _check_service(
        self, service_name: str, service: HealthCheckable
    ) -> HealthCheckResult:
        """Check health of a single service.
        
        Args:
            service_name: Name of the service to check
            service: The service instance to check
            
        Returns:
            Health check result for the service
        """
        try:
            return await asyncio.wait_for(
                service.check_health(),
                timeout=10.0,  # 10 second timeout for health checks
            )
        except TimeoutError:
            return HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                service_name=service_name,
                message="Health check timed out",
            )
        except (RuntimeError, ValueError, TypeError, OSError, AttributeError) as e:
            return HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                service_name=service_name,
                message=f"Health check error: {e!s}",
            )

    async def _health_check_loop(self) -> None:
        """Background health check loop."""
        while True:
            try:
                await self.check_all()
                await asyncio.sleep(self._check_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("health_check_loop_error")
                await asyncio.sleep(self._check_interval)

    async def _notify_callbacks(self, result: HealthCheckResult) -> None:
        """Notify callbacks of health status changes.
        
        Args:
            result: Health check result to notify callbacks about.
        """
        for callback in self._callbacks:
            try:
                await callback(result)
            except Exception:
                logger.exception(
                    "health_check_callback_error",
                    service_name=result.service_name,
                )
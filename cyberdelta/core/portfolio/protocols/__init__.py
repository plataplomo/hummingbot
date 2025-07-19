"""Portfolio protocols package."""

# For backward compatibility, create ValidationServiceProtocol
from typing import Protocol, runtime_checkable

from cyberdelta.core.portfolio.models.base import BaseStateModel

from .concurrency import AsyncLockProtocol
from .container import StateContainerProtocol
from .events import EventHandler
from .lifecycle import HealthCheckable, Initializable, Shutdownable
from .metrics import MetricsCollectorProtocol
from .service import ServiceLifecycle
from .state import Snapshotable, StateStorable
from .validation import Validatable


@runtime_checkable
class ValidationServiceProtocol(Protocol):
    """Protocol for validation services."""
    
    def validate(self, obj: BaseStateModel) -> bool:
        """Validate an object."""
        ...


__all__ = [
    "AsyncLockProtocol",
    "EventHandler", 
    "HealthCheckable",
    "Initializable",
    "MetricsCollectorProtocol",
    "ServiceLifecycle",
    "Shutdownable",
    "Snapshotable",
    "StateContainerProtocol",
    "StateStorable",
    "Validatable",
    "ValidationServiceProtocol",
]
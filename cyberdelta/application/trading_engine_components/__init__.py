"""Trading engine components for modular architecture.

This package contains decomposed components from the main TradingEngine class,
following DDD principles with clear boundaries and single responsibilities.
"""

from cyberdelta.application.trading_engine_components.background_loops.loop_manager import (
    LoopManager,
)
from cyberdelta.application.trading_engine_components.circuit_breaker.breaker_manager import (
    BreakerManager,
)
from cyberdelta.application.trading_engine_components.event_handling.event_processors import (
    EventProcessor,
)
from cyberdelta.application.trading_engine_components.event_handling.event_router import (
    EventRouter,
)
from cyberdelta.application.trading_engine_components.event_handling.event_validators import (
    EventValidator,
)
from cyberdelta.application.trading_engine_components.lifecycle.shutdown_manager import (
    ShutdownManager,
)
from cyberdelta.application.trading_engine_components.lifecycle.startup_manager import (
    StartupManager,
)
from cyberdelta.application.trading_engine_components.monitoring.alert_manager import (
    AlertManager,
)
from cyberdelta.application.trading_engine_components.monitoring.health_manager import (
    HealthManager,
)
from cyberdelta.application.trading_engine_components.monitoring.metrics_manager import (
    MetricsManager,
)
from cyberdelta.application.trading_engine_components.snapshot.snapshot_manager import (
    SnapshotManager,
)
from cyberdelta.application.trading_engine_components.status.status_reporter import (
    StatusReporter,
)


__all__ = [
    "AlertManager",
    "BreakerManager",
    "EventProcessor",
    "EventRouter",
    "EventValidator",
    "HealthManager",
    "LoopManager",
    "MetricsManager",
    "ShutdownManager",
    "SnapshotManager",
    "StartupManager",
    "StatusReporter",
]

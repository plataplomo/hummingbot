# State Management Implementation Blueprint

**Date:** 2025-01-18
**Status:** Ready for Implementation
**Scope:** Complete implementation guide with code examples

---

## 📦 Part 1: Module Structure Implementation

### 1.1 Directory Layout
```bash
cyberdelta/state/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── state_store.py          # Central storage engine
│   ├── state_manager.py        # Lifecycle and coordination
│   ├── state_snapshot.py       # Snapshot functionality
│   └── state_context.py        # State access context
├── models/
│   ├── __init__.py
│   ├── base.py                 # Base state models
│   ├── component_state.py      # FSM states
│   ├── portfolio_state.py      # Portfolio-specific
│   ├── safety_state.py         # Circuit breaker states
│   ├── strategy_state.py       # Strategy states
│   └── system_state.py         # System-wide state
├── events/
│   ├── __init__.py
│   └── state_events.py         # State change events
├── persistence/
│   ├── __init__.py
│   ├── backend.py               # Abstract backend
│   ├── file_backend.py         # File persistence
│   ├── memory_backend.py       # In-memory (testing)
│   └── serialization.py        # msgspec helpers
├── protocols/
│   ├── __init__.py
│   └── state_protocols.py      # Core protocols
└── recovery/
    ├── __init__.py
    ├── recovery_manager.py     # Recovery logic
    └── versioning.py           # Version migration
```

---

## 🏗️ Part 2: Core Components Implementation

### 2.1 State Store (`core/state_store.py`)

```python
"""Central state storage with typed access and persistence."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, TypeVar

import msgspec

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.state.events.state_events import StateChanged, StateLoaded
from cyberdelta.state.persistence.backend import PersistenceBackend
from cyberdelta.state.persistence.file_backend import FileBackend


logger = get_logger(__name__)

T = TypeVar("T", bound=msgspec.Struct)


class StateStore:
    """Central state storage providing typed access and persistence.

    This is the single source of truth for all application state,
    inspired by Nautilus Trader's Cache but adapted for our needs.

    Features:
    - Type-safe storage and retrieval
    - Automatic serialization with msgspec
    - Event emission on state changes
    - Pluggable persistence backends
    - Namespace support for organization

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO hardcoded values
    - All configuration from AppSettings
    - Proper error handling with fail-fast
    - Structured logging throughout
    """

    def __init__(
        self,
        config: AppSettings,
        event_bus: EventBus,
        backend: PersistenceBackend | None = None,
    ) -> None:
        """Initialize state store with configuration.

        Args:
            config: Application settings
            event_bus: Event bus for state change notifications
            backend: Optional persistence backend (defaults to FileBackend)
        """
        self._config = config
        self._event_bus = event_bus
        self._backend = backend or FileBackend(config)

        # Storage structures
        self._store: dict[str, bytes] = {}
        self._metadata: dict[str, dict[str, Any]] = defaultdict(dict)
        self._namespaces: set[str] = set()

        # Serialization
        self._encoder = msgspec.msgpack.Encoder()
        self._decoders: dict[type, msgspec.msgpack.Decoder] = {}

        # State tracking
        self._last_modified: dict[str, datetime] = {}
        self._access_count: dict[str, int] = defaultdict(int)
        self._dirty_keys: set[str] = set()

        # Locking for thread safety
        self._lock = asyncio.Lock()

        logger.info(
            "state_store_initialized",
            backend_type=type(self._backend).__name__,
            namespaces=[],
        )

    def add(self, key: str, value: msgspec.Struct, namespace: str = "default") -> None:
        """Store a typed object with optional namespace.

        Args:
            key: Storage key
            value: msgspec Struct to store
            namespace: Optional namespace for organization

        Raises:
            TypeError: If value is not a msgspec.Struct
        """
        if not isinstance(value, msgspec.Struct):
            raise TypeError(f"Value must be msgspec.Struct, got {type(value)}")

        full_key = f"{namespace}:{key}"

        # Serialize value
        encoded = self._encoder.encode(value)

        # Store with metadata
        self._store[full_key] = encoded
        self._metadata[full_key] = {
            "type": type(value).__name__,
            "namespace": namespace,
            "size": len(encoded),
            "created": datetime.now(UTC),
        }

        # Update tracking
        self._namespaces.add(namespace)
        self._last_modified[full_key] = datetime.now(UTC)
        self._dirty_keys.add(full_key)

        # Emit event
        self._event_bus.publish(StateChanged(
            key=key,
            namespace=namespace,
            value_type=type(value).__name__,
            timestamp=datetime.now(UTC),
        ))

        logger.debug(
            "state_added",
            key=key,
            namespace=namespace,
            type=type(value).__name__,
            size=len(encoded),
        )

    def get(self, key: str, type_: type[T], namespace: str = "default") -> T | None:
        """Retrieve a typed object from storage.

        Args:
            key: Storage key
            type_: Expected type for deserialization
            namespace: Namespace to search in

        Returns:
            Deserialized object or None if not found
        """
        full_key = f"{namespace}:{key}"

        if full_key not in self._store:
            return None

        # Track access
        self._access_count[full_key] += 1

        # Get or create decoder
        if type_ not in self._decoders:
            self._decoders[type_] = msgspec.msgpack.Decoder(type_)

        try:
            return self._decoders[type_].decode(self._store[full_key])
        except msgspec.DecodeError as e:
            logger.error(
                "state_decode_error",
                key=key,
                namespace=namespace,
                expected_type=type_.__name__,
                error=str(e),
            )
            return None

    def exists(self, key: str, namespace: str = "default") -> bool:
        """Check if a key exists in storage.

        Args:
            key: Storage key
            namespace: Namespace to check

        Returns:
            True if key exists
        """
        return f"{namespace}:{key}" in self._store

    def delete(self, key: str, namespace: str = "default") -> bool:
        """Delete a key from storage.

        Args:
            key: Storage key
            namespace: Namespace containing the key

        Returns:
            True if key was deleted, False if not found
        """
        full_key = f"{namespace}:{key}"

        if full_key not in self._store:
            return False

        del self._store[full_key]
        del self._metadata[full_key]
        self._last_modified.pop(full_key, None)
        self._access_count.pop(full_key, None)
        self._dirty_keys.discard(full_key)

        logger.debug("state_deleted", key=key, namespace=namespace)
        return True

    def list_keys(self, namespace: str | None = None) -> list[str]:
        """List all keys, optionally filtered by namespace.

        Args:
            namespace: Optional namespace filter

        Returns:
            List of keys (without namespace prefix)
        """
        if namespace:
            prefix = f"{namespace}:"
            return [
                key.removeprefix(prefix)
                for key in self._store
                if key.startswith(prefix)
            ]
        return list(self._store.keys())

    def get_namespaces(self) -> set[str]:
        """Get all namespaces in use.

        Returns:
            Set of namespace names
        """
        return self._namespaces.copy()

    async def save(self, keys: list[str] | None = None) -> None:
        """Save state to persistence backend.

        Args:
            keys: Optional list of specific keys to save (saves all if None)
        """
        async with self._lock:
            if keys:
                to_save = {k: v for k, v in self._store.items() if k in keys}
            else:
                to_save = self._store.copy()

            await self._backend.save(to_save, self._metadata)
            self._dirty_keys.clear()

            logger.info(
                "state_saved",
                keys_count=len(to_save),
                backend=type(self._backend).__name__,
            )

    async def load(self) -> None:
        """Load state from persistence backend."""
        async with self._lock:
            data, metadata = await self._backend.load()

            self._store = data
            self._metadata = metadata

            # Rebuild namespaces
            self._namespaces = {
                meta.get("namespace", "default")
                for meta in metadata.values()
            }

            # Emit loaded event
            self._event_bus.publish(StateLoaded(
                keys_count=len(data),
                namespaces=list(self._namespaces),
                timestamp=datetime.now(UTC),
            ))

            logger.info(
                "state_loaded",
                keys_count=len(data),
                namespaces=list(self._namespaces),
            )

    def get_stats(self) -> dict[str, Any]:
        """Get storage statistics.

        Returns:
            Dictionary with storage stats
        """
        total_size = sum(len(v) for v in self._store.values())

        return {
            "total_keys": len(self._store),
            "total_size_bytes": total_size,
            "namespaces": list(self._namespaces),
            "dirty_keys": len(self._dirty_keys),
            "most_accessed": sorted(
                self._access_count.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10],
        }
```

### 2.2 State Manager (`core/state_manager.py`)

```python
"""State lifecycle management and coordination."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.component_state import ComponentState
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.models import Fill, Order
from cyberdelta.state.core.state_snapshot import SnapshotManager
from cyberdelta.state.core.state_store import StateStore
from cyberdelta.state.events.state_events import (
    ComponentStateChanged,
    StateAutoSaved,
    SystemStateChanged,
)
from cyberdelta.state.models.system_state import SystemState
from cyberdelta.state.recovery.recovery_manager import RecoveryManager


logger = get_logger(__name__)


class StateManager:
    """Manages state lifecycle and coordinates all state operations.

    This is the main entry point for state management, handling:
    - Component state transitions
    - Automatic persistence
    - State snapshots
    - Recovery operations
    - Event coordination

    IMPORTANT: Following CODING_STANDARDS.md:
    - Configuration-driven behavior
    - Event-based state updates
    - Fail-fast error handling
    """

    def __init__(
        self,
        config: AppSettings,
        state_store: StateStore,
        event_bus: EventBus,
    ) -> None:
        """Initialize state manager.

        Args:
            config: Application settings
            state_store: Central state storage
            event_bus: Event bus for notifications
        """
        self._config = config
        self._store = state_store
        self._event_bus = event_bus

        # Managers
        self._snapshot_manager = SnapshotManager(config, state_store)
        self._recovery_manager = RecoveryManager(config, state_store)

        # Component tracking
        self._component_states: dict[str, ComponentState] = {}
        self._component_metadata: dict[str, dict[str, Any]] = {}

        # Tasks
        self._save_task: asyncio.Task | None = None
        self._snapshot_task: asyncio.Task | None = None

        # Subscribe to critical events
        self._subscribe_to_events()

        logger.info(
            "state_manager_initialized",
            auto_save_interval=config.state.auto_save_interval,
            snapshot_interval=config.state.snapshot_interval,
        )

    def _subscribe_to_events(self) -> None:
        """Subscribe to events that affect state."""
        # Trading events
        self._event_bus.subscribe(Fill, self._on_fill)
        self._event_bus.subscribe(Order, self._on_order)

        # System events
        self._event_bus.subscribe(ComponentStateChanged, self._on_component_state_changed)

    async def initialize(self) -> SystemState:
        """Initialize state management system.

        Returns:
            Loaded or new system state

        Raises:
            StateError: If initialization fails
        """
        logger.info("state_manager_initializing")

        # Try to load existing state
        try:
            await self._store.load()
            system_state = self._store.get("system", SystemState)

            if system_state:
                logger.info(
                    "state_loaded_from_persistence",
                    components=len(system_state.component_states),
                )
                self._restore_component_states(system_state)
                return system_state

        except Exception as e:
            logger.warning(
                "state_load_failed_attempting_recovery",
                error=str(e),
            )

            # Attempt recovery
            recovered_state = await self._recovery_manager.recover()
            if recovered_state:
                self._restore_component_states(recovered_state)
                return recovered_state

        # Create new state
        logger.info("creating_new_system_state")
        system_state = SystemState(
            component_states={},
            active_strategies=[],
            timestamp=datetime.now(UTC),
        )

        # Save initial state
        self._store.add("system", system_state)
        await self._store.save()

        # Start background tasks
        await self._start_background_tasks()

        return system_state

    async def _start_background_tasks(self) -> None:
        """Start auto-save and snapshot tasks."""
        # Auto-save task
        if self._config.state.auto_save_interval > 0:
            self._save_task = asyncio.create_task(
                self._auto_save_loop(),
                name="state_auto_save"
            )

        # Snapshot task
        if self._config.state.snapshot_interval > 0:
            self._snapshot_task = asyncio.create_task(
                self._snapshot_loop(),
                name="state_snapshot"
            )

    async def _auto_save_loop(self) -> None:
        """Periodic state persistence loop."""
        interval = self._config.state.auto_save_interval

        while True:
            try:
                await asyncio.sleep(interval)
                await self.save_all()

                self._event_bus.publish(StateAutoSaved(
                    timestamp=datetime.now(UTC),
                    keys_saved=len(self._store.list_keys()),
                ))

            except asyncio.CancelledError:
                logger.info("auto_save_loop_cancelled")
                break
            except Exception as e:
                logger.error(
                    "auto_save_error",
                    error=str(e),
                    exc_info=e,
                )

    async def _snapshot_loop(self) -> None:
        """Periodic snapshot creation loop."""
        interval = self._config.state.snapshot_interval

        while True:
            try:
                await asyncio.sleep(interval)
                await self._snapshot_manager.create_snapshot()

            except asyncio.CancelledError:
                logger.info("snapshot_loop_cancelled")
                break
            except Exception as e:
                logger.error(
                    "snapshot_error",
                    error=str(e),
                    exc_info=e,
                )

    def register_component(
        self,
        component_id: str,
        initial_state: ComponentState = ComponentState.PRE_INITIALIZED,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Register a component for state tracking.

        Args:
            component_id: Unique component identifier
            initial_state: Initial component state
            metadata: Optional component metadata
        """
        self._component_states[component_id] = initial_state
        self._component_metadata[component_id] = metadata or {}

        logger.info(
            "component_registered",
            component_id=component_id,
            initial_state=initial_state.value,
        )

    def transition_component(
        self,
        component_id: str,
        new_state: ComponentState,
        reason: str | None = None,
    ) -> None:
        """Transition a component to a new state.

        Args:
            component_id: Component to transition
            new_state: Target state
            reason: Optional transition reason

        Raises:
            ValueError: If component not registered
        """
        if component_id not in self._component_states:
            raise ValueError(f"Component {component_id} not registered")

        old_state = self._component_states[component_id]

        # Validate transition
        if not self._is_valid_transition(old_state, new_state):
            logger.warning(
                "invalid_state_transition",
                component_id=component_id,
                from_state=old_state.value,
                to_state=new_state.value,
            )
            return

        # Update state
        self._component_states[component_id] = new_state

        # Emit event
        self._event_bus.publish(ComponentStateChanged(
            component_id=component_id,
            old_state=old_state,
            new_state=new_state,
            reason=reason,
            timestamp=datetime.now(UTC),
        ))

        logger.info(
            "component_state_changed",
            component_id=component_id,
            from_state=old_state.value,
            to_state=new_state.value,
            reason=reason,
        )

    def _is_valid_transition(
        self,
        from_state: ComponentState,
        to_state: ComponentState,
    ) -> bool:
        """Check if state transition is valid.

        Args:
            from_state: Current state
            to_state: Target state

        Returns:
            True if transition is valid
        """
        # Define valid transitions
        valid_transitions = {
            ComponentState.PRE_INITIALIZED: {
                ComponentState.READY,
                ComponentState.DISPOSED,
            },
            ComponentState.READY: {
                ComponentState.RUNNING,
                ComponentState.DISPOSED,
            },
            ComponentState.RUNNING: {
                ComponentState.STOPPED,
                ComponentState.DEGRADED,
                ComponentState.FAULTED,
            },
            ComponentState.STOPPED: {
                ComponentState.READY,
                ComponentState.DISPOSED,
            },
            ComponentState.DEGRADED: {
                ComponentState.RUNNING,
                ComponentState.FAULTED,
                ComponentState.STOPPED,
            },
            ComponentState.FAULTED: {
                ComponentState.STOPPED,
                ComponentState.DISPOSED,
            },
            ComponentState.DISPOSED: set(),  # Terminal state
        }

        return to_state in valid_transitions.get(from_state, set())

    def get_component_state(self, component_id: str) -> ComponentState | None:
        """Get current state of a component.

        Args:
            component_id: Component identifier

        Returns:
            Current state or None if not registered
        """
        return self._component_states.get(component_id)

    def get_all_component_states(self) -> dict[str, ComponentState]:
        """Get states of all registered components.

        Returns:
            Dictionary of component states
        """
        return self._component_states.copy()

    async def save_all(self) -> None:
        """Save all state to persistence."""
        # Update system state
        system_state = SystemState(
            component_states=self._component_states,
            active_strategies=self._get_active_strategies(),
            timestamp=datetime.now(UTC),
        )

        self._store.add("system", system_state)

        # Save to backend
        await self._store.save()

        logger.info(
            "state_saved",
            components=len(self._component_states),
            namespaces=len(self._store.get_namespaces()),
        )

    async def shutdown(self) -> None:
        """Gracefully shutdown state management."""
        logger.info("state_manager_shutting_down")

        # Cancel background tasks
        if self._save_task:
            self._save_task.cancel()
            try:
                await self._save_task
            except asyncio.CancelledError:
                pass

        if self._snapshot_task:
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass

        # Final save
        if self._config.state.save_on_shutdown:
            await self.save_all()

        logger.info("state_manager_shutdown_complete")

    def _restore_component_states(self, system_state: SystemState) -> None:
        """Restore component states from loaded state.

        Args:
            system_state: Loaded system state
        """
        self._component_states = system_state.component_states.copy()

        for component_id, state in self._component_states.items():
            logger.info(
                "component_state_restored",
                component_id=component_id,
                state=state.value,
            )

    def _get_active_strategies(self) -> list[str]:
        """Get list of active strategy IDs.

        Returns:
            List of strategy component IDs in RUNNING state
        """
        return [
            component_id
            for component_id, state in self._component_states.items()
            if component_id.startswith("strategy_") and state == ComponentState.RUNNING
        ]

    async def _on_fill(self, fill: Fill) -> None:
        """Handle fill event for state updates.

        Args:
            fill: Fill event
        """
        # Store fill in state
        fill_key = f"fill_{fill.fill_id}"
        self._store.add(fill_key, fill, namespace="fills")

    async def _on_order(self, order: Order) -> None:
        """Handle order event for state updates.

        Args:
            order: Order event
        """
        # Store order in state
        order_key = f"order_{order.client_order_id}"
        self._store.add(order_key, order, namespace="orders")

    async def _on_component_state_changed(self, event: ComponentStateChanged) -> None:
        """Handle component state change event.

        Args:
            event: Component state change event
        """
        # Update our tracking
        self._component_states[event.component_id] = event.new_state

        # Check for system-wide impacts
        if event.new_state == ComponentState.FAULTED:
            logger.error(
                "component_faulted",
                component_id=event.component_id,
                reason=event.reason,
            )

            # Potentially trigger system-wide safety measures
            self._event_bus.publish(SystemStateChanged(
                severity="critical",
                component_id=event.component_id,
                timestamp=datetime.now(UTC),
            ))
```

---

## 📦 Part 3: State Models with msgspec

### 3.1 Base Models (`models/base.py`)

```python
"""Base state models using msgspec for performance."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import msgspec


class VersionedState(msgspec.Struct):
    """Base class for versioned state models.

    Provides version tracking for migration support.
    """

    version: int = 1
    timestamp: datetime

    def upgrade(self) -> VersionedState:
        """Upgrade state to latest version.

        Returns:
            Upgraded state (may be self if already latest)
        """
        return self


class NamespacedState(msgspec.Struct):
    """Base class for namespaced state models.

    Provides namespace organization for state storage.
    """

    namespace: str = "default"
    key: str
    timestamp: datetime

    def get_storage_key(self) -> str:
        """Get full storage key including namespace.

        Returns:
            Full storage key
        """
        return f"{self.namespace}:{self.key}"


class MetadataState(msgspec.Struct):
    """Base class for state with metadata.

    Provides additional metadata tracking.
    """

    metadata: dict[str, Any] = msgspec.field(default_factory=dict)
    created_at: datetime
    updated_at: datetime
    access_count: int = 0
```

### 3.2 Component States (`models/component_state.py`)

```python
"""Component state definitions using FSM pattern."""

from __future__ import annotations

from enum import Enum


class ComponentState(str, Enum):
    """Component state machine states.

    Based on Nautilus Trader's component states but simplified.
    """

    PRE_INITIALIZED = "PRE_INITIALIZED"
    READY = "READY"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    DEGRADED = "DEGRADED"
    FAULTED = "FAULTED"
    DISPOSED = "DISPOSED"

    def can_transition_to(self, target: ComponentState) -> bool:
        """Check if transition to target state is valid.

        Args:
            target: Target state

        Returns:
            True if transition is valid
        """
        valid_transitions = {
            ComponentState.PRE_INITIALIZED: {
                ComponentState.READY,
                ComponentState.DISPOSED,
            },
            ComponentState.READY: {
                ComponentState.RUNNING,
                ComponentState.DISPOSED,
            },
            ComponentState.RUNNING: {
                ComponentState.STOPPED,
                ComponentState.DEGRADED,
                ComponentState.FAULTED,
            },
            ComponentState.STOPPED: {
                ComponentState.READY,
                ComponentState.DISPOSED,
            },
            ComponentState.DEGRADED: {
                ComponentState.RUNNING,
                ComponentState.FAULTED,
                ComponentState.STOPPED,
            },
            ComponentState.FAULTED: {
                ComponentState.STOPPED,
                ComponentState.DISPOSED,
            },
            ComponentState.DISPOSED: set(),
        }

        return target in valid_transitions.get(self, set())

    def is_operational(self) -> bool:
        """Check if component is in operational state.

        Returns:
            True if component can process
        """
        return self in {ComponentState.RUNNING, ComponentState.DEGRADED}

    def is_terminal(self) -> bool:
        """Check if state is terminal.

        Returns:
            True if no further transitions possible
        """
        return self == ComponentState.DISPOSED
```

### 3.3 System State (`models/system_state.py`)

```python
"""System-wide state model."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import msgspec

from cyberdelta.state.models.base import VersionedState
from cyberdelta.state.models.component_state import ComponentState


class SystemState(VersionedState):
    """Overall system state combining all components.

    This is the top-level state that gets persisted and restored.
    """

    # Component states
    component_states: dict[str, ComponentState] = msgspec.field(default_factory=dict)

    # Active entities
    active_strategies: list[str] = msgspec.field(default_factory=list)
    active_exchanges: list[str] = msgspec.field(default_factory=list)

    # System metrics
    uptime_seconds: float = 0.0
    total_orders_placed: int = 0
    total_fills_processed: int = 0

    # Risk metrics
    total_exposure_usd: Decimal = Decimal("0")
    max_drawdown_pct: Decimal = Decimal("0")

    # Health status
    is_healthy: bool = True
    health_issues: list[str] = msgspec.field(default_factory=list)

    def get_operational_components(self) -> list[str]:
        """Get list of operational components.

        Returns:
            Component IDs in operational states
        """
        return [
            comp_id
            for comp_id, state in self.component_states.items()
            if state.is_operational()
        ]

    def has_critical_issues(self) -> bool:
        """Check if system has critical issues.

        Returns:
            True if any component is faulted
        """
        return any(
            state == ComponentState.FAULTED
            for state in self.component_states.values()
        )
```

---

## 🔌 Part 4: Event Integration

### 4.1 State Events (`events/state_events.py`)

```python
"""State-related events for EventBus integration."""

from __future__ import annotations

from datetime import datetime

import msgspec

from cyberdelta.state.models.component_state import ComponentState


class StateChanged(msgspec.Struct):
    """Event emitted when state changes."""

    key: str
    namespace: str
    value_type: str
    timestamp: datetime


class StateLoaded(msgspec.Struct):
    """Event emitted when state is loaded from persistence."""

    keys_count: int
    namespaces: list[str]
    timestamp: datetime


class StateSaved(msgspec.Struct):
    """Event emitted when state is saved to persistence."""

    keys_count: int
    backend: str
    timestamp: datetime


class StateAutoSaved(msgspec.Struct):
    """Event emitted on automatic state save."""

    timestamp: datetime
    keys_saved: int


class ComponentStateChanged(msgspec.Struct):
    """Event emitted when component state changes."""

    component_id: str
    old_state: ComponentState
    new_state: ComponentState
    reason: str | None
    timestamp: datetime


class SystemStateChanged(msgspec.Struct):
    """Event emitted on system-wide state changes."""

    severity: str  # "info", "warning", "critical"
    component_id: str | None
    timestamp: datetime


class StateSnapshotCreated(msgspec.Struct):
    """Event emitted when state snapshot is created."""

    snapshot_id: str
    snapshot_path: str
    size_bytes: int
    timestamp: datetime


class StateRecovered(msgspec.Struct):
    """Event emitted when state is recovered."""

    source: str  # "file", "backup", "snapshot"
    recovery_time_ms: float
    keys_recovered: int
    timestamp: datetime
```

---

## 🗄️ Part 5: Persistence Layer

### 5.1 Abstract Backend (`persistence/backend.py`)

```python
"""Abstract persistence backend protocol."""

from __future__ import annotations

from typing import Any, Protocol


class PersistenceBackend(Protocol):
    """Protocol for state persistence backends.

    Implementations provide actual storage mechanisms.
    """

    async def save(
        self,
        data: dict[str, bytes],
        metadata: dict[str, dict[str, Any]],
    ) -> None:
        """Save state data and metadata.

        Args:
            data: State data to persist
            metadata: Associated metadata
        """
        ...

    async def load(self) -> tuple[dict[str, bytes], dict[str, dict[str, Any]]]:
        """Load state data and metadata.

        Returns:
            Tuple of (data, metadata)
        """
        ...

    async def exists(self) -> bool:
        """Check if persisted state exists.

        Returns:
            True if state exists
        """
        ...

    async def delete(self) -> None:
        """Delete all persisted state."""
        ...

    async def backup(self, backup_id: str) -> None:
        """Create a backup of current state.

        Args:
            backup_id: Unique backup identifier
        """
        ...

    async def restore(self, backup_id: str) -> None:
        """Restore state from backup.

        Args:
            backup_id: Backup to restore from
        """
        ...
```

### 5.2 File Backend (`persistence/file_backend.py`)

```python
"""File-based persistence backend."""

from __future__ import annotations

import asyncio
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import msgspec

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


class FileBackend:
    """File-based state persistence.

    Provides atomic saves with backup rotation.
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize file backend.

        Args:
            config: Application settings
        """
        self._config = config

        # Paths from config
        self._state_file = Path(config.state.state_file)
        self._backup_dir = Path(config.state.backup_dir)
        self._backup_count = config.state.backup_count

        # Ensure directories exist
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        self._backup_dir.mkdir(parents=True, exist_ok=True)

        # Serialization
        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder()

        logger.info(
            "file_backend_initialized",
            state_file=str(self._state_file),
            backup_dir=str(self._backup_dir),
        )

    async def save(
        self,
        data: dict[str, bytes],
        metadata: dict[str, dict[str, Any]],
    ) -> None:
        """Save state to file atomically.

        Args:
            data: State data
            metadata: State metadata
        """
        # Prepare save data
        save_data = {
            "version": 1,
            "timestamp": datetime.now(UTC).isoformat(),
            "data": data,
            "metadata": metadata,
        }

        # Encode
        encoded = self._encoder.encode(save_data)

        # Atomic write with temp file
        temp_file = self._state_file.with_suffix(".tmp")

        try:
            # Write to temp file
            await asyncio.to_thread(temp_file.write_bytes, encoded)

            # Atomic rename
            await asyncio.to_thread(temp_file.replace, self._state_file)

            logger.debug(
                "state_saved_to_file",
                file=str(self._state_file),
                size_bytes=len(encoded),
            )

        except Exception as e:
            logger.error(
                "state_save_failed",
                file=str(self._state_file),
                error=str(e),
            )
            # Clean up temp file if exists
            if temp_file.exists():
                temp_file.unlink()
            raise

    async def load(self) -> tuple[dict[str, bytes], dict[str, dict[str, Any]]]:
        """Load state from file.

        Returns:
            Tuple of (data, metadata)

        Raises:
            FileNotFoundError: If state file doesn't exist
            ValueError: If state file is corrupted
        """
        if not self._state_file.exists():
            logger.info("no_state_file_found", file=str(self._state_file))
            return {}, {}

        try:
            # Read file
            encoded = await asyncio.to_thread(self._state_file.read_bytes)

            # Decode
            save_data = self._decoder.decode(encoded)

            logger.info(
                "state_loaded_from_file",
                file=str(self._state_file),
                keys_count=len(save_data.get("data", {})),
            )

            return save_data.get("data", {}), save_data.get("metadata", {})

        except Exception as e:
            logger.error(
                "state_load_failed",
                file=str(self._state_file),
                error=str(e),
            )
            raise ValueError(f"Failed to load state: {e}") from e

    async def exists(self) -> bool:
        """Check if state file exists.

        Returns:
            True if state file exists
        """
        return self._state_file.exists()

    async def delete(self) -> None:
        """Delete state file."""
        if self._state_file.exists():
            await asyncio.to_thread(self._state_file.unlink)
            logger.info("state_file_deleted", file=str(self._state_file))

    async def backup(self, backup_id: str) -> None:
        """Create backup of current state.

        Args:
            backup_id: Unique backup identifier
        """
        if not self._state_file.exists():
            logger.warning("no_state_to_backup")
            return

        # Create backup filename
        backup_file = self._backup_dir / f"state_{backup_id}.msgpack"

        # Copy state file
        await asyncio.to_thread(
            shutil.copy2,
            self._state_file,
            backup_file,
        )

        # Rotate old backups
        await self._rotate_backups()

        logger.info(
            "state_backup_created",
            backup_file=str(backup_file),
        )

    async def restore(self, backup_id: str) -> None:
        """Restore state from backup.

        Args:
            backup_id: Backup to restore

        Raises:
            FileNotFoundError: If backup doesn't exist
        """
        backup_file = self._backup_dir / f"state_{backup_id}.msgpack"

        if not backup_file.exists():
            raise FileNotFoundError(f"Backup {backup_id} not found")

        # Copy backup to state file
        await asyncio.to_thread(
            shutil.copy2,
            backup_file,
            self._state_file,
        )

        logger.info(
            "state_restored_from_backup",
            backup_id=backup_id,
        )

    async def _rotate_backups(self) -> None:
        """Rotate old backups to maintain count limit."""
        # Get all backup files
        backups = sorted(
            self._backup_dir.glob("state_*.msgpack"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

        # Remove old backups
        for old_backup in backups[self._backup_count:]:
            await asyncio.to_thread(old_backup.unlink)
            logger.debug("old_backup_removed", file=str(old_backup))
```

---

## 🔐 Part 6: Configuration Integration

### 6.1 State Configuration (`config/models/state_config.py`)

```python
"""State management configuration."""

from __future__ import annotations

from pydantic import BaseModel, Field


class StateConfig(BaseModel):
    """Unified state management configuration.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical settings
    - All paths and intervals must be explicit
    """

    # Persistence
    backend: str = Field(
        description="Backend type: 'file', 'redis', or 'memory'",
    )
    state_file: str = Field(
        description="Path to main state file",
    )
    backup_dir: str = Field(
        description="Directory for state backups",
    )
    backup_count: int = Field(
        description="Number of backups to retain",
    )

    # Auto-save
    auto_save_interval: int = Field(
        description="Seconds between auto-saves (0 to disable)",
    )
    save_on_shutdown: bool = Field(
        description="Save state on graceful shutdown",
    )

    # Snapshots
    snapshot_interval: int = Field(
        description="Seconds between snapshots (0 to disable)",
    )
    snapshot_dir: str = Field(
        description="Directory for state snapshots",
    )
    max_snapshots: int = Field(
        description="Maximum snapshots to retain",
    )

    # Recovery
    validate_on_load: bool = Field(
        description="Validate state on load",
    )
    recover_from_backup: bool = Field(
        description="Attempt backup recovery on failure",
    )
    max_recovery_attempts: int = Field(
        description="Maximum recovery attempts",
    )

    # Performance
    use_compression: bool = Field(
        description="Compress state data",
    )
    buffer_writes: bool = Field(
        description="Buffer writes for performance",
    )

    class Config:
        """Pydantic configuration."""

        frozen = True
```

### 6.2 Update AppSettings

```python
# In config/models/app_config.py

from cyberdelta.config.models.state_config import StateConfig

class AppSettings(BaseModel):
    """Main application settings."""

    # ... existing fields ...

    state: StateConfig = Field(
        description="State management configuration"
    )
```

---

## 🧪 Part 7: Integration Example

### 7.1 Using State Management in a Service

```python
"""Example of integrating state management in a service."""

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.state.core.state_manager import StateManager
from cyberdelta.state.core.state_store import StateStore
from cyberdelta.state.models.component_state import ComponentState
import msgspec


class PortfolioService:
    """Example portfolio service using state management."""

    def __init__(
        self,
        config: AppSettings,
        state_manager: StateManager,
        state_store: StateStore,
        event_bus: EventBus,
    ) -> None:
        """Initialize with state management.

        Args:
            config: Application settings
            state_manager: State lifecycle manager
            state_store: State storage
            event_bus: Event bus
        """
        self._config = config
        self._state_manager = state_manager
        self._state_store = state_store
        self._event_bus = event_bus

        # Register as component
        self._component_id = "portfolio_service"
        self._state_manager.register_component(
            self._component_id,
            ComponentState.PRE_INITIALIZED,
            metadata={"type": "core_service"},
        )

    async def initialize(self) -> None:
        """Initialize portfolio service."""
        # Transition to READY
        self._state_manager.transition_component(
            self._component_id,
            ComponentState.READY,
            reason="Initialization complete",
        )

        # Load portfolio state
        portfolio_state = self._state_store.get(
            "portfolio",
            PortfolioState,
            namespace="portfolio",
        )

        if portfolio_state:
            # Restore from saved state
            self._positions = portfolio_state.positions
            self._balances = portfolio_state.balances
        else:
            # Initialize empty
            self._positions = {}
            self._balances = {}

    async def start(self) -> None:
        """Start portfolio service."""
        self._state_manager.transition_component(
            self._component_id,
            ComponentState.RUNNING,
        )

    async def update_position(self, position: Position) -> None:
        """Update position and save to state.

        Args:
            position: Position to update
        """
        # Update internal state
        self._positions[position.symbol] = position

        # Save to state store
        portfolio_state = PortfolioState(
            positions=self._positions,
            balances=self._balances,
            timestamp=datetime.now(UTC),
        )

        self._state_store.add(
            "portfolio",
            portfolio_state,
            namespace="portfolio",
        )

        # State will be auto-saved by StateManager
```

---

## 🎯 Conclusion

This implementation blueprint provides:

1. **Complete module structure** with clear separation of concerns
2. **Type-safe state management** using msgspec for performance
3. **Event-driven architecture** integrated with existing EventBus
4. **Robust persistence** with atomic saves and backups
5. **Component lifecycle management** with FSM states
6. **Configuration-driven behavior** following CODING_STANDARDS.md

**Next Steps:**
1. Create the module structure
2. Implement core components (StateStore, StateManager)
3. Add persistence backends
4. Integrate with existing services
5. Comprehensive testing

The unified state management system will eliminate the current fragmentation and provide a solid foundation for reliable trading operations.

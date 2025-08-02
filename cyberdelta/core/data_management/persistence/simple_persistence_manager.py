"""Simplified type-safe state persistence manager."""

from __future__ import annotations

import asyncio
import gzip
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.models.base import BaseStateModel, StateWrapper, ValidationResult
from cyberdelta.core.data_management.persistence.backup_manager import BackupManager
from cyberdelta.core.data_management.persistence.persistence_models import (
    PersistenceConfig,
    PersistenceStats,
    TypedStatePersistenceError,
)
from cyberdelta.core.data_management.persistence.state_serializer import PydanticJSONSerializer


logger = get_logger(__name__)

# Generic type variable bound to BaseModel
T = TypeVar("T", bound=BaseModel)


class SimplePersistenceManager[T: BaseModel](BaseStateModel):
    """Simplified type-safe state persistence manager using StateWrapper[T]."""

    # Configuration
    config: PersistenceConfig = Field(description="Service configuration")

    # Statistics
    stats: PersistenceStats = Field(
        default_factory=PersistenceStats, description="Operation statistics"
    )

    # Service state
    is_initialized: bool = Field(default=False)
    is_running: bool = Field(default=False)

    def __init__(
        self, config: PersistenceConfig, state_id: str | None = None
    ) -> None:
        """Initialize persistence manager with type safety.
        
        Args:
            config: Persistence configuration settings.
            state_id: Optional state identifier for this manager instance.
        """
        if state_id is None:
            state_id = f"persistence_manager_{int(time.time())}"
        super().__init__(state_id=state_id)
        self.config = config

        # Type-safe serializer
        self._serializer: PydanticJSONSerializer = PydanticJSONSerializer()

        # Async lock for thread safety
        self._lock: asyncio.Lock = asyncio.Lock()

        # Storage paths
        self._storage_path = Path(self.config.storage_path)
        self._backup_path = self._storage_path / "backups"
        
        # Backup manager
        self._backup_manager = BackupManager(
            self._storage_path, self._backup_path, self.config.max_backups
        )

        logger.info(
            "simple_persistence_manager_created",
            state_id=self.state_id,
            storage_path=str(self._storage_path),
        )

    async def initialize(self) -> None:
        """Initialize the persistence manager."""
        if self.is_initialized:
            logger.warning("persistence_manager_already_initialized", state_id=self.state_id)
            return

        # Create storage directories
        self._storage_path.mkdir(parents=True, exist_ok=True)

        if self.config.auto_backup:
            self._backup_path.mkdir(exist_ok=True)

        self.is_initialized = True
        logger.info(
            "persistence_manager_initialized",
            state_id=self.state_id,
            storage_path=str(self._storage_path),
        )

    async def start(self) -> None:
        """Start the persistence manager."""
        if not self.is_initialized:
            await self.initialize()

        self.is_running = True
        logger.info("persistence_manager_started", state_id=self.state_id)

    async def stop(self) -> None:
        """Stop the persistence manager."""
        self.is_running = False
        logger.info(
            "persistence_manager_stopped",
            state_id=self.state_id,
            final_stats=self.stats.model_dump(),
        )

    async def health_check(self) -> bool:
        """Check health of persistence manager.

        Returns:
            True if manager is healthy and operational, False otherwise
        """
        return (
            self.is_initialized
            and self.is_running
            and self._storage_path.exists()
            and self._storage_path.is_dir()
        )

    async def save_state(
        self,
        state_data: T,
        state_id: str | None = None,
        metadata: dict[str, str | int | float | bool] | None = None,
    ) -> str:
        """Save state using StateWrapper[T] with full type safety.

        Args:
            state_data: Pydantic model to save
            state_id: Optional state identifier
            metadata: Optional metadata with strict typing

        Returns:
            Path to saved file

        Raises:
            TypedStatePersistenceError: If state saving fails
        """
        async with self._lock:
            try:
                # Create type-safe wrapper
                wrapper = StateWrapper[T](
                    state_id=state_id or f"state_{int(time.time())}",
                    timestamp=time.time(),
                    datetime_iso=datetime.now(UTC).isoformat(),
                    metadata=metadata or {},
                    data=state_data,
                )

                # Serialize with full type safety
                serialized = self._serializer.serialize(wrapper)

                # Compress if requested
                if self.config.compression:
                    serialized = gzip.compress(serialized)

                # Determine file path
                extension = self._serializer.get_extension()
                if self.config.compression:
                    extension += ".gz"

                file_path = self._storage_path / f"{wrapper.state_id}{extension}"

                # Backup existing file if needed
                if self.config.auto_backup and file_path.exists():
                    await self._backup_manager.create_backup(file_path, wrapper.state_id)

                # Write file
                file_path.write_bytes(serialized)

                # Update statistics
                self.stats.total_saves += 1
                self.stats.bytes_written += len(serialized)

                logger.info(
                    "state_saved",
                    state_id=wrapper.state_id,
                    file_path=str(file_path),
                    size_bytes=len(serialized),
                    data_type=type(state_data).__name__,
                )

                return str(file_path)

            except Exception as e:
                self.stats.total_errors += 1
                logger.exception(
                    "state_save_failed", state_id=state_id or "unknown", error_type=type(e).__name__
                )
                raise TypedStatePersistenceError("save", state_id or "unknown", e) from e

    async def load_state(self, state_id: str, model_class: type[T]) -> StateWrapper[T]:
        """Load state with full type preservation.

        Args:
            state_id: State identifier
            model_class: Expected model class for type safety

        Returns:
            StateWrapper[T] with preserved type information

        Raises:
            TypedStatePersistenceError: If state loading fails
        """
        try:
            async with self._lock:
                # Determine file path
                extension = self._serializer.get_extension()
                if self.config.compression:
                    extension += ".gz"

                file_path = self._storage_path / f"{state_id}{extension}"

                if not file_path.exists():
                    raise TypedStatePersistenceError(
                        "load", state_id, FileNotFoundError(f"State file not found: {file_path}")
                    )

                # Read file
                serialized = file_path.read_bytes()

                # Decompress if needed
                if self.config.compression:
                    serialized = gzip.decompress(serialized)

                # Deserialize with type safety
                wrapper = self._serializer.deserialize(serialized, model_class)

                # Update statistics
                self.stats.total_loads += 1
                self.stats.bytes_read += len(serialized)

                logger.info(
                    "state_loaded",
                    state_id=state_id,
                    file_path=str(file_path),
                    size_bytes=len(serialized),
                    data_type=type(wrapper.data).__name__,
                )

                return wrapper

        except Exception as e:
            self.stats.total_errors += 1
            logger.exception("state_load_failed", state_id=state_id, error_type=type(e).__name__)
            if isinstance(e, TypedStatePersistenceError):
                raise
            raise TypedStatePersistenceError("load", state_id, e) from e

    def get_stats(self) -> PersistenceStats:
        """Get persistence statistics.

        Returns:
            PersistenceStats containing operation metrics
        """
        return self.stats

    async def validate_state(self) -> ValidationResult:
        """Validate persistence manager state.

        Returns:
            ValidationResult containing validation status and any issues
        """
        result = ValidationResult(valid=True)

        # Validate configuration
        if not self.config.storage_path:
            result.add_error("Storage path cannot be empty")

        if self.config.max_backups < 0:
            result.add_error("Max backups cannot be negative")

        # Validate paths
        if not self._storage_path.exists():
            result.add_warning(f"Storage path does not exist: {self._storage_path}")
        elif not self._storage_path.is_dir():
            result.add_error(f"Storage path is not a directory: {self._storage_path}")

        # Validate service state
        if not self.state_id:
            result.add_error("State ID cannot be empty")

        return result

    @property
    def state_key(self) -> str:
        """State key for StateStorable protocol compliance.
        
        Returns:
            Unique state key for this persistence manager instance.
        """
        return f"persistence_manager_{self.state_id}"

    def to_state_dict(
        self,
    ) -> dict[str, str | int | float | bool | dict[str, str | int | float | bool]]:
        """Convert to state dictionary for persistence.

        Returns:
            Dictionary containing manager state data for persistence
        """
        return {
            "state_id": self.state_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "config": self.config.model_dump(),
            "stats": self.stats.model_dump(),
            "is_initialized": self.is_initialized,
            "is_running": self.is_running,
        }
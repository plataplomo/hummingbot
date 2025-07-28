"""Type-safe state persistence service using StateWrapper[T] and Pydantic.

COMPLETE REPLACEMENT - Eliminates all dict[str, Any] dynamic access patterns.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions import StatePersistenceError as BaseStatePersistenceError
from cyberdelta.core.portfolio.models.base import BaseStateModel, StateWrapper, ValidationResult


logger = get_logger(__name__)

# Generic type variable bound to BaseModel
T = TypeVar("T", bound=BaseModel)


class PersistenceStats(BaseModel):
    """Statistics for persistence operations."""

    total_saves: int = Field(default=0, description="Total number of save operations")
    total_loads: int = Field(default=0, description="Total number of load operations")
    total_errors: int = Field(default=0, description="Total number of errors")
    bytes_written: int = Field(default=0, description="Total bytes written")
    bytes_read: int = Field(default=0, description="Total bytes read")


class PersistenceConfig(BaseModel):
    """Configuration for persistence service."""

    storage_path: str = Field(description="Base path for state storage")
    auto_backup: bool = Field(default=True, description="Whether to auto-backup")
    max_backups: int = Field(default=10, description="Maximum number of backups")
    compression: bool = Field(default=False, description="Whether to compress files")
    file_extension: str = Field(default=".json", description="File extension to use")


class TypedStatePersistenceError(BaseStatePersistenceError):
    """Typed state persistence error."""

    def __init__(self, operation: str, state_id: str, cause: Exception | None = None) -> None:
        """Initialize with operation details."""
        super().__init__(f"Typed persistence {operation} failed for {state_id}")
        self.operation = operation
        self.state_id = state_id
        self.__cause__ = cause


class StateSerializer(ABC):
    """Abstract base class for type-safe state serializers."""

    @abstractmethod
    def serialize(self, wrapper: StateWrapper[T]) -> bytes:
        """Serialize StateWrapper to bytes with full type safety."""
        ...

    @abstractmethod
    def deserialize(self, data: bytes, model_class: type[T]) -> StateWrapper[T]:
        """Deserialize bytes to StateWrapper with type preservation."""
        ...

    @abstractmethod
    def get_extension(self) -> str:
        """Get file extension for this serializer."""
        ...


class PydanticJSONSerializer:
    """Type-safe JSON serializer using Pydantic."""

    def serialize(self, wrapper: StateWrapper[T]) -> bytes:
        """Serialize StateWrapper using Pydantic JSON serialization.

        Args:
            wrapper: StateWrapper containing typed data

        Returns:
            JSON bytes with full type safety

        Raises:
            TypedStatePersistenceError: If serialization fails
        """
        try:
            # Pydantic handles all serialization automatically
            json_str = wrapper.model_dump_json(indent=2)
            return json_str.encode("utf-8")
        except Exception as e:
            raise TypedStatePersistenceError("serialize", wrapper.state_id, e) from e

    def deserialize(self, data: bytes, model_class: type[T]) -> StateWrapper[T]:
        """Deserialize bytes to typed StateWrapper.

        Args:
            data: JSON bytes to deserialize
            model_class: Expected model class for type safety

        Returns:
            StateWrapper[T] with preserved type information

        Raises:
            TypedStatePersistenceError: If deserialization fails
        """
        try:
            json_str = data.decode("utf-8")
            json_data = json.loads(json_str)

            # Reconstruct StateWrapper with proper typing
            return StateWrapper[T](
                state_id=json_data["state_id"],
                timestamp=json_data["timestamp"],
                datetime_iso=json_data["datetime_iso"],
                metadata=json_data["metadata"],
                data=model_class.model_validate(json_data["data"]),
            )
        except Exception as e:
            state_id = "unknown"
            try:
                json_str = data.decode("utf-8")
                json_data = json.loads(json_str)
                state_id = json_data.get("state_id", "unknown")
            except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
                logger.debug("Failed to extract state_id from corrupted data")
            raise TypedStatePersistenceError("deserialize", state_id, e) from e

    def get_extension(self) -> str:
        """Get file extension.

        Returns:
            File extension string for JSON files
        """
        return ".json"


class StatePersistenceService[T: BaseModel](BaseStateModel):
    """Type-safe state persistence service using StateWrapper[T].

    COMPLETE REPLACEMENT of the old implementation that caused 2 pyright errors
    due to dynamic dict access patterns.

    Features:
    - StateWrapper[T] for type-safe serialization
    - Pydantic-only data handling - no dict[str, Any]
    - Generic[T] for type preservation throughout operations
    - Protocol compliance for ServiceLifecycle, StateStorable, Validatable
    """

    # Configuration
    config: PersistenceConfig = Field(description="Service configuration")

    # Statistics
    stats: PersistenceStats = Field(
        default_factory=PersistenceStats, description="Operation statistics"
    )

    # Service state
    is_initialized: bool = Field(default=False)
    is_running: bool = Field(default=False)

    # Metadata
    metadata: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Service metadata with strict typing"
    )

    def __init__(
        self, config: PersistenceConfig, state_id: str | None = None, **data: str | float | bool
    ) -> None:
        """Initialize persistence service with type safety."""
        if state_id is None:
            state_id = f"persistence_service_{int(time.time())}"
        super().__init__(state_id=state_id)
        self.config = config

        # Type-safe serializer - no dynamic types
        self._serializer: PydanticJSONSerializer = PydanticJSONSerializer()

        # Explicit asyncio.Lock - no type inference
        self._lock: asyncio.Lock = asyncio.Lock()

        # Storage paths
        self._storage_path = Path(self.config.storage_path)
        self._backup_path = self._storage_path / "backups"

        logger.info(
            "state_persistence_service_created",
            state_id=self.state_id,
            storage_path=str(self._storage_path),
            config=self.config.model_dump(),
        )

    async def initialize(self) -> None:
        """Initialize the persistence service."""
        if self.is_initialized:
            logger.warning("persistence_service_already_initialized", state_id=self.state_id)
            return

        # Create storage directories
        self._storage_path.mkdir(parents=True, exist_ok=True)

        if self.config.auto_backup:
            self._backup_path.mkdir(exist_ok=True)

        self.is_initialized = True
        logger.info(
            "persistence_service_initialized",
            state_id=self.state_id,
            storage_path=str(self._storage_path),
        )

    async def start(self) -> None:
        """Start the persistence service."""
        if not self.is_initialized:
            await self.initialize()

        self.is_running = True
        logger.info("persistence_service_started", state_id=self.state_id)

    async def stop(self) -> None:
        """Stop the persistence service."""
        self.is_running = False
        logger.info(
            "persistence_service_stopped",
            state_id=self.state_id,
            final_stats=self.stats.model_dump(),
        )

    async def health_check(self) -> bool:
        """Check health of persistence service.

        Returns:
            True if service is healthy and operational, False otherwise
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
                # Create type-safe wrapper - T is preserved
                wrapper = StateWrapper[T](
                    state_id=state_id or f"state_{int(time.time())}",
                    timestamp=time.time(),
                    datetime_iso=datetime.now(UTC).isoformat(),
                    metadata=metadata or {},
                    data=state_data,  # T type is preserved here
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
                    await self._create_backup(file_path, wrapper.state_id)

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
                    self._raise_file_not_found_error(state_id, file_path)

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

                return wrapper  # Returns StateWrapper[T] with type preservation

        except Exception as e:
            self.stats.total_errors += 1
            logger.exception("state_load_failed", state_id=state_id, error_type=type(e).__name__)
            if isinstance(e, TypedStatePersistenceError):
                raise
            raise TypedStatePersistenceError("load", state_id, e) from e

    def _raise_file_not_found_error(self, state_id: str, file_path: Path) -> None:
        """Raise TypedStatePersistenceError for file not found.

        Args:
            state_id: State identifier for error context
            file_path: Path that was not found

        Raises:
            TypedStatePersistenceError: Always raises with file not found details
        """
        raise TypedStatePersistenceError(
            "load", state_id, FileNotFoundError(f"State file not found: {file_path}")
        )

    async def _create_backup(self, file_path: Path, state_id: str) -> None:
        """Create backup of existing state file."""
        try:
            # Generate backup filename
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            backup_name = f"{state_id}_{timestamp}{file_path.suffix}"
            backup_file = self._backup_path / backup_name

            # Copy file
            backup_file.write_bytes(file_path.read_bytes())

            # Clean up old backups
            await self._cleanup_old_backups(state_id)

            logger.debug(
                "backup_created", original_file=str(file_path), backup_file=str(backup_file)
            )

        except OSError:
            logger.warning("backup_creation_failed", file_path=str(file_path))

    async def _cleanup_old_backups(self, state_id: str) -> None:
        """Remove old backups exceeding max_backups limit."""
        try:
            # Find backups for this state_id
            pattern = f"{state_id}_*"
            backups = sorted(self._backup_path.glob(pattern))

            # Remove oldest backups if exceeding limit
            if len(backups) > self.config.max_backups:
                for backup in backups[: -self.config.max_backups]:
                    backup.unlink()
                    logger.debug("old_backup_removed", backup_file=str(backup))

        except OSError:
            logger.warning("backup_cleanup_failed")

    def get_stats(self) -> PersistenceStats:
        """Get persistence statistics.

        Returns:
            PersistenceStats containing operation metrics
        """
        return self.stats

    async def validate_state(self) -> ValidationResult:
        """Validate persistence service state.

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
        """State key for StateStorable protocol compliance."""
        return f"persistence_service_{self.state_id}"

    def to_state_dict(
        self,
    ) -> dict[str, str | int | float | bool | dict[str, str | int | float | bool]]:
        """Convert to state dictionary for persistence.

        Returns:
            Dictionary containing service state data for persistence
        """
        return {
            "state_id": self.state_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "config": self.config.model_dump(),
            "stats": self.stats.model_dump(),
            "is_initialized": self.is_initialized,
            "is_running": self.is_running,
            "metadata": self.metadata.copy(),
        }

    @classmethod
    def from_state_dict(
        cls,
        data: dict[str, str | int | float | bool | dict[str, str | int | float | bool]],
    ) -> StatePersistenceService[T]:
        """Create service from state dictionary.

        Args:
            data: State dictionary containing service configuration and data

        Returns:
            StatePersistenceService instance created from state data

        Raises:
            TypeError: If config data is not a valid dictionary
        """
        config_data = data["config"]
        if isinstance(config_data, dict):
            config = PersistenceConfig.model_validate(config_data)
        else:
            msg = "Invalid config data"
            raise TypeError(msg)

        stats_data = data.get("stats", {})
        if isinstance(stats_data, dict):
            PersistenceStats.model_validate(stats_data)
        else:
            PersistenceStats()

        state_id = str(data["state_id"])

        return cls(
            config=config,
            state_id=state_id,
        )


# Factory function for easy service creation
def create_persistence_service(
    storage_path: str | Path,
    auto_backup: bool = True,
    max_backups: int = 10,
    compression: bool = False,
) -> StatePersistenceService[BaseModel]:
    """Create a persistence service with default configuration.

    Args:
        storage_path: Base path for state storage
        auto_backup: Whether to auto-backup before overwriting
        max_backups: Maximum number of backups to keep
        compression: Whether to compress state files

    Returns:
        Configured StatePersistenceService instance
    """
    config = PersistenceConfig(
        storage_path=str(storage_path),
        auto_backup=auto_backup,
        max_backups=max_backups,
        compression=compression,
    )

    service: StatePersistenceService[BaseModel] = StatePersistenceService(config=config)
    return service

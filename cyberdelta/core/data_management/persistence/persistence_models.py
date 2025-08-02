"""Persistence models and configuration."""

from __future__ import annotations

from pydantic import BaseModel, Field

from cyberdelta.core.portfolio.exceptions import StatePersistenceError as BaseStatePersistenceError


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
        """Initialize with operation details.
        
        Args:
            operation: The operation that failed (save, load, etc.).
            state_id: ID of the state being operated on.
            cause: The underlying exception that caused the error.
        """
        super().__init__(f"Typed persistence {operation} failed for {state_id}")
        self.operation = operation
        self.state_id = state_id
        self.__cause__ = cause
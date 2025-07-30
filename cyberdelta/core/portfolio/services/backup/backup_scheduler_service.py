"""Backup scheduling and cleanup service."""

from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService

if TYPE_CHECKING:
    from .backup_metadata_service import BackupMetadataService
    from .backup_storage_service import BackupStorageService

logger = get_logger(__name__)

# Default scheduling intervals
DEFAULT_BACKUP_INTERVAL = 3600  # 1 hour
DEFAULT_CLEANUP_INTERVAL = 3600  # 1 hour
DEFAULT_RETENTION_DAYS = 30
DEFAULT_MAX_BACKUPS = 100


class BackupType(Enum):
    """Types of scheduled backups."""

    FULL = "full"
    INCREMENTAL = "incremental"
    SNAPSHOT = "snapshot"


class BackupSchedulerService(BasePortfolioService):
    """Handles backup scheduling and cleanup only."""

    backup_interval_seconds: int = Field(
        default=DEFAULT_BACKUP_INTERVAL, ge=60, description="Backup interval in seconds"
    )
    cleanup_interval_seconds: int = Field(
        default=DEFAULT_CLEANUP_INTERVAL, ge=300, description="Cleanup interval in seconds"
    )
    retention_days: int = Field(
        default=DEFAULT_RETENTION_DAYS, ge=1, description="Backup retention in days"
    )
    max_backups: int = Field(
        default=DEFAULT_MAX_BACKUPS, ge=1, description="Maximum number of backups"
    )
    
    # Service dependencies
    metadata_service: BackupMetadataService | None = Field(
        default=None, description="Metadata service dependency"
    )
    storage_service: BackupStorageService | None = Field(
        default=None, description="Storage service dependency"
    )
    
    # Internal state
    _backup_task: asyncio.Task[None] | None = Field(default=None, exclude=True)
    _cleanup_task: asyncio.Task[None] | None = Field(default=None, exclude=True)
    _shutdown_event: asyncio.Event = Field(default_factory=asyncio.Event, exclude=True)

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def _initialize_internal(self) -> None:
        """Initialize scheduler service."""
        if not self.metadata_service or not self.storage_service:
            raise ValueError("Metadata and storage services must be provided")
        
        # Start background tasks
        self._backup_task = asyncio.create_task(self._run_backup_scheduler())
        self._cleanup_task = asyncio.create_task(self._run_cleanup_scheduler())
        
        logger.info(
            "Backup scheduler service initialized",
            backup_interval=self.backup_interval_seconds,
            cleanup_interval=self.cleanup_interval_seconds,
        )

    async def _shutdown_internal(self) -> None:
        """Shutdown scheduler service."""
        self._shutdown_event.set()
        
        # Cancel background tasks
        if self._backup_task:
            self._backup_task.cancel()
            with asyncio.suppress(asyncio.CancelledError):
                await self._backup_task
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            with asyncio.suppress(asyncio.CancelledError):
                await self._cleanup_task
        
        logger.info("Backup scheduler service shutdown")

    async def trigger_backup_now(self, backup_type: BackupType = BackupType.FULL) -> str:
        """Trigger an immediate backup.
        
        Returns:
            The backup ID of the created backup.
        """
        backup_id = f"manual_{int(time.time())}"
        
        # This would trigger the actual backup creation
        # In practice, this would coordinate with other services
        logger.info("Manual backup triggered", backup_id=backup_id, backup_type=backup_type.value)
        
        return backup_id

    async def cleanup_expired_backups(self) -> int:
        """Clean up expired backups.
        
        Returns:
            Number of backups cleaned up.
        """
        if not self.metadata_service or not self.storage_service:
            return 0
        
        current_time = time.time()
        retention_seconds = self.retention_days * 24 * 3600
        
        all_backups = await self.metadata_service.list_all_backups()
        expired_backups = []
        
        # Find expired backups
        for backup_id, metadata in all_backups.items():
            backup_time = metadata.get("timestamp", 0)
            if isinstance(backup_time, (int, float)) and current_time - backup_time > retention_seconds:
                expired_backups.append(backup_id)
        
        # Remove expired backups
        cleanup_count = 0
        for backup_id in expired_backups:
            metadata = all_backups[backup_id]
            backup_format = metadata.get("format", "json")
            
            # Convert string format to enum
            from .backup_storage_service import BackupFormat
            format_enum = BackupFormat(backup_format)
            
            # Delete file and metadata
            if await self.storage_service.delete_backup_file(backup_id, format_enum):
                if await self.metadata_service.remove_backup_metadata(backup_id):
                    cleanup_count += 1
        
        logger.info("Backup cleanup completed", expired_backups=cleanup_count)
        return cleanup_count

    async def cleanup_excess_backups(self) -> int:
        """Clean up excess backups beyond max limit.
        
        Returns:
            Number of backups cleaned up.
        """
        if not self.metadata_service or not self.storage_service:
            return 0
        
        all_backups = await self.metadata_service.list_all_backups()
        
        if len(all_backups) <= self.max_backups:
            return 0
        
        # Sort by timestamp (oldest first)
        sorted_backups = sorted(
            all_backups.items(),
            key=lambda x: x[1].get("timestamp", 0)
        )
        
        # Remove oldest backups
        excess_count = len(all_backups) - self.max_backups
        cleanup_count = 0
        
        for backup_id, metadata in sorted_backups[:excess_count]:
            backup_format = metadata.get("format", "json")
            
            # Convert string format to enum
            from .backup_storage_service import BackupFormat
            format_enum = BackupFormat(backup_format)
            
            # Delete file and metadata
            if await self.storage_service.delete_backup_file(backup_id, format_enum):
                if await self.metadata_service.remove_backup_metadata(backup_id):
                    cleanup_count += 1
        
        logger.info("Excess backup cleanup completed", removed_backups=cleanup_count)
        return cleanup_count

    async def get_schedule_status(self) -> dict[str, object]:
        """Get current schedule status."""
        return {
            "backup_interval_seconds": self.backup_interval_seconds,
            "cleanup_interval_seconds": self.cleanup_interval_seconds,
            "retention_days": self.retention_days,
            "max_backups": self.max_backups,
            "backup_task_running": self._backup_task is not None and not self._backup_task.done(),
            "cleanup_task_running": self._cleanup_task is not None and not self._cleanup_task.done(),
        }

    async def _run_backup_scheduler(self) -> None:
        """Run periodic backup scheduler."""
        logger.info("Backup scheduler started", interval=self.backup_interval_seconds)
        
        while not self._shutdown_event.is_set():
            try:
                # Wait for interval or shutdown
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=self.backup_interval_seconds
                )
                break  # Shutdown requested
            except asyncio.TimeoutError:
                # Time to create a backup
                try:
                    backup_id = await self.trigger_backup_now(BackupType.FULL)
                    logger.info("Scheduled backup created", backup_id=backup_id)
                except Exception as e:
                    logger.error("Scheduled backup failed", error=str(e))
        
        logger.info("Backup scheduler stopped")

    async def _run_cleanup_scheduler(self) -> None:
        """Run periodic cleanup scheduler."""
        logger.info("Cleanup scheduler started", interval=self.cleanup_interval_seconds)
        
        while not self._shutdown_event.is_set():
            try:
                # Wait for interval or shutdown
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=self.cleanup_interval_seconds
                )
                break  # Shutdown requested
            except asyncio.TimeoutError:
                # Time to run cleanup
                try:
                    expired_count = await self.cleanup_expired_backups()
                    excess_count = await self.cleanup_excess_backups()
                    total_cleaned = expired_count + excess_count
                    
                    if total_cleaned > 0:
                        logger.info(
                            "Scheduled cleanup completed",
                            expired_backups=expired_count,
                            excess_backups=excess_count,
                        )
                except Exception as e:
                    logger.error("Scheduled cleanup failed", error=str(e))
        
        logger.info("Cleanup scheduler stopped")

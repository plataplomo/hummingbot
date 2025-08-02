"""Backup orchestration service - coordinates focused backup services."""

from __future__ import annotations

import time
from datetime import datetime, UTC
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.models.portfolio_state import PortfolioStateData
from cyberdelta.core.infrastructure.services.base_service import BaseService

from .backup_metadata_service import BackupMetadataService
from .backup_scheduler_service import BackupSchedulerService, BackupType
from .backup_storage_service import BackupFormat, BackupStorageService

if TYPE_CHECKING:
    from cyberdelta.core.portfolio.portfolio_types.protocols import StateManagerProtocol

logger = get_logger(__name__)


class BackupOrchestrator(BaseService):
    """Orchestrates backup operations using focused services."""

    # Service dependencies
    metadata_service: BackupMetadataService = Field(..., description="Metadata service")
    storage_service: BackupStorageService = Field(..., description="Storage service")
    scheduler_service: BackupSchedulerService = Field(..., description="Scheduler service")
    state_manager: StateManagerProtocol | None = Field(
        default=None, description="Portfolio state manager"
    )

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def _initialize_internal(self) -> None:
        """Initialize orchestrator and all services."""
        # Configure scheduler with service dependencies
        self.scheduler_service.metadata_service = self.metadata_service
        self.scheduler_service.storage_service = self.storage_service
        
        logger.info("Backup orchestrator initialized")

    async def _shutdown_internal(self) -> None:
        """Shutdown orchestrator and all services."""
        logger.info("Backup orchestrator shutdown")

    async def create_backup(
        self,
        backup_type: BackupType = BackupType.FULL,
        backup_format: BackupFormat = BackupFormat.COMPRESSED_JSON,
    ) -> str:
        """Create a new backup.
        
        Returns:
            The backup ID of the created backup.
        """
        if not self.state_manager:
            raise ValueError("State manager not configured")
        
        backup_id = f"{backup_type.value}_{int(time.time())}"
        
        try:
            # Get current portfolio state
            portfolio_state = await self._get_portfolio_state()
            
            # Save backup file
            storage_info = await self.storage_service.save_backup(
                backup_id, portfolio_state, backup_format
            )
            
            # Create metadata
            metadata: dict[str, object] = {
                "backup_id": backup_id,
                "backup_type": backup_type.value,
                "timestamp": time.time(),
                "format": backup_format.value,
                "file_path": storage_info["file_path"],
                "file_size": storage_info["file_size"],
                "checksum": storage_info["checksum"],
                "status": "completed",
            }
            
            # Save metadata
            await self.metadata_service.save_backup_metadata(backup_id, metadata)
            
            logger.info(
                "Backup created successfully",
                backup_id=backup_id,
                backup_type=backup_type.value,
                file_size=storage_info["file_size"],
            )
            
            return backup_id
            
        except Exception as e:
            logger.error("Backup creation failed", backup_id=backup_id, error=str(e))
            
            # Save failed metadata
            failed_metadata: dict[str, object] = {
                "backup_id": backup_id,
                "backup_type": backup_type.value,
                "timestamp": time.time(),
                "status": "failed",
                "error": str(e),
            }
            await self.metadata_service.save_backup_metadata(backup_id, failed_metadata)
            
            raise

    async def restore_backup(self, backup_id: str) -> PortfolioStateData:
        """Restore portfolio state from backup.
        
        Returns:
            The restored portfolio state data.
        """
        # Get backup metadata
        metadata = await self.metadata_service.get_backup_metadata(backup_id)
        if not metadata:
            raise ValueError(f"Backup not found: {backup_id}")
        
        # Check backup status
        if metadata.get("status") != "completed":
            raise ValueError(f"Cannot restore backup with status: {metadata.get('status')}")
        
        # Get backup format
        format_str = metadata.get("format", "json")
        backup_format = BackupFormat(format_str)
        
        # Validate backup file
        expected_checksum = metadata.get("checksum", "")
        if expected_checksum and isinstance(expected_checksum, str) and not await self.storage_service.validate_backup_file(
            backup_id, backup_format, expected_checksum
        ):
            raise ValueError(f"Backup file validation failed: {backup_id}")
        
        # Load backup data
        portfolio_state = await self.storage_service.load_backup(backup_id, backup_format)
        
        logger.info("Backup restored successfully", backup_id=backup_id)
        return portfolio_state

    async def delete_backup(self, backup_id: str) -> bool:
        """Delete a backup.
        
        Returns:
            True if backup was deleted successfully.
        """
        # Get backup metadata
        metadata = await self.metadata_service.get_backup_metadata(backup_id)
        if not metadata:
            return False
        
        # Get backup format
        format_str = metadata.get("format", "json")
        backup_format = BackupFormat(format_str)
        
        # Delete storage file
        storage_deleted = await self.storage_service.delete_backup_file(backup_id, backup_format)
        
        # Delete metadata
        metadata_deleted = await self.metadata_service.remove_backup_metadata(backup_id)
        
        success = storage_deleted and metadata_deleted
        
        if success:
            logger.info("Backup deleted successfully", backup_id=backup_id)
        else:
            logger.error("Backup deletion failed", backup_id=backup_id)
        
        return success

    async def list_backups(self) -> dict[str, dict[str, object]]:
        """List all available backups.
        
        Returns:
            Dictionary of backup ID to metadata.
        """
        return await self.metadata_service.list_all_backups()

    async def get_backup_statistics(self) -> dict[str, object]:
        """Get comprehensive backup statistics.
        
        Returns:
            Dictionary with backup statistics and scheduler status.
        """
        metadata_stats = await self.metadata_service.get_backup_statistics()
        schedule_status = await self.scheduler_service.get_schedule_status()
        
        return {
            **metadata_stats,
            "scheduler_status": schedule_status,
        }

    async def trigger_scheduled_backup(self) -> str:
        """Trigger a scheduled backup.
        
        Returns:
            The backup ID of the created backup.
        """
        return await self.scheduler_service.trigger_backup_now(BackupType.FULL)

    async def cleanup_old_backups(self) -> int:
        """Clean up old backups.
        
        Returns:
            Number of backups cleaned up.
        """
        expired_count = await self.scheduler_service.cleanup_expired_backups()
        excess_count = await self.scheduler_service.cleanup_excess_backups()
        return expired_count + excess_count

    async def _get_portfolio_state(self) -> PortfolioStateData:
        """Get current portfolio state from state manager."""
        if not self.state_manager:
            raise ValueError("State manager not configured")
        
        # Get current state data
        positions = await self.state_manager.get_all_positions()
        balances = await self.state_manager.get_all_balances()
        orders = await self.state_manager.get_open_orders()
        
        # Create portfolio state data
        # Note: positions are tracked in exchange_summaries, not directly
        return PortfolioStateData(
            state_id="backup_snapshot",
            portfolio_id="backup_snapshot",
            balances=balances,  # dict[str, SpotBalance]
            orders=orders,      # list[Order]
            updated_at=datetime.now(UTC),
            # Other fields will use defaults
        )

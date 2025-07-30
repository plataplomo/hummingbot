"""Backup management for state persistence."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


class BackupManager:
    """Manages backup operations for state persistence."""

    def __init__(self, storage_path: Path, backup_path: Path, max_backups: int = 10) -> None:
        """Initialize backup manager.
        
        Args:
            storage_path: Main storage directory path.
            backup_path: Directory for storing backups.
            max_backups: Maximum number of backups to keep per state.
        """
        self._storage_path = storage_path
        self._backup_path = backup_path
        self._max_backups = max_backups

    async def create_backup(self, file_path: Path, state_id: str) -> bool:
        """Create backup of existing state file.
        
        Args:
            file_path: Path to the file to backup.
            state_id: State identifier for backup naming.
            
        Returns:
            True if backup was created successfully, False otherwise.
        """
        try:
            # Ensure backup directory exists
            self._backup_path.mkdir(parents=True, exist_ok=True)

            # Generate backup filename
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            backup_name = f"{state_id}_{timestamp}{file_path.suffix}"
            backup_file = self._backup_path / backup_name

            # Copy file
            backup_file.write_bytes(file_path.read_bytes())

            # Clean up old backups
            await self.cleanup_old_backups(state_id)

            logger.debug(
                "backup_created", original_file=str(file_path), backup_file=str(backup_file)
            )
            return True

        except OSError as e:
            logger.warning("backup_creation_failed", file_path=str(file_path), error=str(e))
            return False

    async def cleanup_old_backups(self, state_id: str) -> int:
        """Remove old backups exceeding max_backups limit.
        
        Args:
            state_id: State identifier for finding relevant backups.
            
        Returns:
            Number of backups removed.
        """
        try:
            # Find backups for this state_id
            pattern = f"{state_id}_*"
            backups = sorted(self._backup_path.glob(pattern))

            # Remove oldest backups if exceeding limit
            removed_count = 0
            if len(backups) > self._max_backups:
                for backup in backups[: -self._max_backups]:
                    backup.unlink()
                    removed_count += 1
                    logger.debug("old_backup_removed", backup_file=str(backup))

            return removed_count

        except OSError as e:
            logger.warning("backup_cleanup_failed", error=str(e))
            return 0

    def list_backups(self, state_id: str | None = None) -> list[Path]:
        """List available backups.
        
        Args:
            state_id: Optional state identifier to filter backups.
            
        Returns:
            List of backup file paths, sorted by creation time.
        """
        try:
            if state_id:
                pattern = f"{state_id}_*"
                backups = list(self._backup_path.glob(pattern))
            else:
                backups = [f for f in self._backup_path.iterdir() if f.is_file()]
            
            return sorted(backups)
        
        except OSError as e:
            logger.warning("backup_listing_failed", error=str(e))
            return []

    def get_backup_count(self, state_id: str) -> int:
        """Get the number of backups for a state.
        
        Args:
            state_id: State identifier to count backups for.
            
        Returns:
            Number of backups found for the state.
        """
        return len(self.list_backups(state_id))

    def get_backup_info(self) -> dict[str, int]:
        """Get backup information summary.
        
        Returns:
            Dictionary with backup statistics.
        """
        try:
            all_backups = self.list_backups()
            total_size = sum(backup.stat().st_size for backup in all_backups)
            
            return {
                "total_backups": len(all_backups),
                "total_size_bytes": total_size,
                "backup_directory": str(self._backup_path),
                "max_backups_per_state": self._max_backups,
            }
        
        except OSError as e:
            logger.warning("backup_info_failed", error=str(e))
            return {
                "total_backups": 0,
                "total_size_bytes": 0,
                "backup_directory": str(self._backup_path),
                "max_backups_per_state": self._max_backups,
            }
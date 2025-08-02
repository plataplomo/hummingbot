"""Backup metadata management service."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService

if TYPE_CHECKING:
    from cyberdelta.core.portfolio.portfolio_types.protocols import (
        PersistenceServiceProtocol,
    )

logger = get_logger(__name__)


class BackupMetadataService(BasePortfolioService):
    """Manages backup metadata only - single responsibility."""

    backup_dir: Path = Field(..., description="Directory for backup metadata")
    metadata_index: dict[str, dict[str, object]] = Field(
        default_factory=dict, description="In-memory metadata index"
    )

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def _initialize_internal(self) -> None:
        """Initialize metadata service."""
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        await self._load_metadata_index()
        logger.info("Backup metadata service initialized", backup_dir=str(self.backup_dir))

    async def _shutdown_internal(self) -> None:
        """Shutdown metadata service."""
        await self._save_metadata_index()
        logger.info("Backup metadata service shutdown")

    async def get_backup_metadata(self, backup_id: str) -> dict[str, object] | None:
        """Get metadata for a specific backup."""
        return self.metadata_index.get(backup_id)

    async def save_backup_metadata(
        self, backup_id: str, metadata: dict[str, object]
    ) -> None:
        """Save metadata for a backup."""
        self.metadata_index[backup_id] = metadata
        await self._save_metadata_index()
        logger.info("Backup metadata saved", backup_id=backup_id)

    async def remove_backup_metadata(self, backup_id: str) -> bool:
        """Remove metadata for a backup."""
        if backup_id in self.metadata_index:
            del self.metadata_index[backup_id]
            await self._save_metadata_index()
            logger.info("Backup metadata removed", backup_id=backup_id)
            return True
        return False

    async def list_all_backups(self) -> dict[str, dict[str, object]]:
        """List all backup metadata."""
        return self.metadata_index.copy()

    async def get_backup_statistics(self) -> dict[str, object]:
        """Get backup statistics."""
        total_backups = len(self.metadata_index)
        backup_sizes: list[float] = []
        backup_ages: list[float] = []
        current_time = time.time()

        for metadata in self.metadata_index.values():
            if "file_size" in metadata:
                file_size = metadata["file_size"]
                if isinstance(file_size, (int, float)):
                    backup_sizes.append(float(file_size))
            if "timestamp" in metadata:
                timestamp = metadata["timestamp"]
                if isinstance(timestamp, (int, float)):
                    backup_ages.append(current_time - float(timestamp))

        return {
            "total_backups": total_backups,
            "total_size_bytes": sum(backup_sizes) if backup_sizes else 0,
            "average_size_bytes": sum(backup_sizes) / len(backup_sizes) if backup_sizes else 0,
            "oldest_backup_age_seconds": max(backup_ages) if backup_ages else 0,
            "newest_backup_age_seconds": min(backup_ages) if backup_ages else 0,
        }

    async def _load_metadata_index(self) -> None:
        """Load metadata index from file."""
        index_file = self.backup_dir / "backup_index.json"
        if index_file.exists():
            try:
                content = index_file.read_text()
                self.metadata_index = json.loads(content)
                logger.info(
                    "Metadata index loaded", backup_count=len(self.metadata_index)
                )
            except (json.JSONDecodeError, OSError) as e:
                logger.error("Failed to load metadata index", error=str(e))
                self.metadata_index = {}
        else:
            self.metadata_index = {}

    async def _save_metadata_index(self) -> None:
        """Save metadata index to file."""
        index_file = self.backup_dir / "backup_index.json"
        try:
            index_file.write_text(json.dumps(self.metadata_index, indent=2))
        except OSError as e:
            logger.error("Failed to save metadata index", error=str(e))
            raise

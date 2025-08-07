"""File-based repository for portfolio state persistence.

This module provides a file-based implementation of the portfolio storage
protocol using JSON serialization.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import aiofiles
import orjson

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models.portfolio.state import PortfolioState
from cyberdelta.protocols.domain.portfolio import (
    PortfolioStorageProtocol,
    StorageError,
)


logger = get_logger(__name__)


class FilePortfolioStorage(PortfolioStorageProtocol):
    """File-based implementation of portfolio storage.

    Uses JSON files for persistence with atomic writes and backup rotation.

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL paths from config, NO hardcoded paths
    - NO assumptions about file system permissions
    - Explicit error handling with context
    - NO silent failures
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize file storage with configuration.

        Args:
            config: Application settings containing file paths and options
        """
        self.config = config

        # Extract paths from config - NO hardcoded paths
        self._state_file = Path(config.general.state_file)
        self._backup_dir = Path(config.general.state_backup_directory)
        self._backup_count = config.general.state_backup_count

        # Ensure directories exist
        self._ensure_directories()

        logger.info(
            "file_storage_initialized",
            state_file=str(self._state_file),
            backup_dir=str(self._backup_dir),
            backup_count=self._backup_count,
        )

    def _ensure_directories(self) -> None:
        """Ensure required directories exist.

        Raises:
            StorageError: If directory creation fails
        """
        try:
            # Create parent directory for state file
            self._state_file.parent.mkdir(parents=True, exist_ok=True)

            # Create backup directory
            self._backup_dir.mkdir(parents=True, exist_ok=True)

        except OSError as e:
            msg = f"Failed to create storage directories: {e}"
            raise StorageError(
                msg,
                operation="directory_creation",
                original_error=e,
            ) from e

    async def save_state(self, state: PortfolioState) -> None:
        """Save portfolio state to JSON file with atomic write.

        Args:
            state: Portfolio state to persist

        Raises:
            StorageError: If save operation fails
        """
        try:
            # Use temporary file for atomic write
            temp_file = self._state_file.with_suffix(".tmp")

            # Serialize state to JSON
            state_data = state.model_dump(mode="json")

            # Write to temporary file first
            async with aiofiles.open(temp_file, "wb") as f:
                # orjson returns bytes, write in binary mode for better performance
                await f.write(orjson.dumps(state_data, option=orjson.OPT_INDENT_2))

            # Atomic move to final location
            temp_file.replace(self._state_file)

            logger.info(
                "portfolio_state_saved",
                state_file=str(self._state_file),
                timestamp=state.timestamp.isoformat(),
                balance_count=len(state.balances),
                position_count=len(state.positions),
            )

        except (OSError, ValueError) as e:
            msg = f"Failed to save portfolio state: {e}"
            raise StorageError(msg, operation="save_state", original_error=e) from e

    async def load_state(self) -> PortfolioState | None:
        """Load portfolio state from JSON file.

        Returns:
            PortfolioState if file exists and is valid, None if file doesn't exist

        Raises:
            StorageError: If load operation fails
        """
        if not self._state_file.exists():
            logger.info("portfolio_state_file_not_found", state_file=str(self._state_file))
            return None

        try:
            async with aiofiles.open(self._state_file, "rb") as f:
                content = await f.read()
                state_data = orjson.loads(content)

            # Validate and create PortfolioState
            state = PortfolioState.model_validate(state_data)

            logger.info(
                "portfolio_state_loaded",
                state_file=str(self._state_file),
                timestamp=state.timestamp.isoformat(),
                balance_count=len(state.balances),
                position_count=len(state.positions),
            )

        except (OSError, orjson.JSONDecodeError, ValueError) as e:
            msg = f"Failed to load portfolio state: {e}"
            raise StorageError(msg, operation="load_state", original_error=e) from e
        else:
            return state

    async def save_snapshot(self, state: PortfolioState, snapshot_name: str) -> None:
        """Save a named snapshot of portfolio state.

        Args:
            state: Portfolio state to snapshot
            snapshot_name: Name for the snapshot file

        Raises:
            StorageError: If snapshot operation fails
        """
        try:
            # Create snapshot filename
            snapshot_file = self._backup_dir / f"{snapshot_name}.json"

            # Serialize state to JSON
            state_data = state.model_dump(mode="json")

            # Add snapshot metadata
            snapshot_data = {
                "snapshot_name": snapshot_name,
                "created_at": datetime.now(UTC).isoformat(),
                "state": state_data,
            }

            # Write snapshot
            async with aiofiles.open(snapshot_file, "wb") as f:
                await f.write(orjson.dumps(snapshot_data, option=orjson.OPT_INDENT_2))

            # Rotate backups based on config
            await self._rotate_snapshots()

            logger.info(
                "portfolio_snapshot_saved",
                snapshot_name=snapshot_name,
                snapshot_file=str(snapshot_file),
            )

        except (OSError, ValueError) as e:
            msg = f"Failed to save snapshot '{snapshot_name}': {e}"
            raise StorageError(
                msg,
                operation="save_snapshot",
                original_error=e,
            ) from e

    async def list_snapshots(self) -> list[str]:
        """List all available snapshots.

        Returns:
            List of snapshot names

        Raises:
            StorageError: If listing operation fails
        """
        try:
            if not self._backup_dir.exists():
                return []

            snapshots: list[str] = []
            for file_path in self._backup_dir.glob("*.json"):
                # Extract snapshot name (filename without extension)
                snapshot_name = file_path.stem
                snapshots.append(snapshot_name)

            # Sort by creation time (most recent first)
            snapshots.sort(reverse=True)

            logger.debug(
                "snapshots_listed",
                count=len(snapshots),
                snapshots=snapshots[:5],  # Log first 5 for brevity
            )

        except OSError as e:
            msg = f"Failed to list snapshots: {e}"
            raise StorageError(msg, operation="list_snapshots", original_error=e) from e
        else:
            return snapshots

    async def delete_snapshot(self, snapshot_name: str) -> None:
        """Delete a named snapshot.

        Args:
            snapshot_name: Name of snapshot to delete

        Raises:
            StorageError: If delete operation fails
        """
        try:
            snapshot_file = self._backup_dir / f"{snapshot_name}.json"

            if not snapshot_file.exists():
                msg = f"Snapshot '{snapshot_name}' not found"
                raise StorageError(msg, operation="delete_snapshot")

            snapshot_file.unlink()

            logger.info(
                "snapshot_deleted", snapshot_name=snapshot_name, snapshot_file=str(snapshot_file)
            )

        except OSError as e:
            msg = f"Failed to delete snapshot '{snapshot_name}': {e}"
            raise StorageError(
                msg,
                operation="delete_snapshot",
                original_error=e,
            ) from e

    async def _rotate_snapshots(self) -> None:
        """Rotate snapshots based on configured backup count.

        Removes oldest snapshots if count exceeds config.general.state_backup_count.
        """
        try:
            snapshots = await self.list_snapshots()

            # Remove excess snapshots (keep newest ones)
            if len(snapshots) > self._backup_count:
                snapshots_to_remove = snapshots[self._backup_count :]

                for snapshot_name in snapshots_to_remove:
                    await self.delete_snapshot(snapshot_name)

                logger.info(
                    "snapshots_rotated",
                    removed_count=len(snapshots_to_remove),
                    kept_count=self._backup_count,
                )

        except OSError as e:
            # Log but don't raise - snapshot rotation failure shouldn't
            # prevent the main snapshot operation from completing
            logger.warning("snapshot_rotation_failed", error=str(e), exc_info=True)

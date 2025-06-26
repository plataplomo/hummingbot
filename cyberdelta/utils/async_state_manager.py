"""Async State Management System.

This module provides async utilities for managing application state persistence
and recovery in the CyberDeltaEngine trading system.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


class AsyncStateManager:
    """Provide reliable async state persistence and recovery.

    Responsible for:
    - Atomic state saving with validation (async)
    - State restoration with integrity checks (async)
    - State backup rotation
    - Corruption detection and recovery
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize the async state manager.

        Args:
            config: Application configuration
        """
        self.config = config

        # Load state parameters from config
        self.state_file: str = config.general.state_file
        self.backup_dir: str = config.general.state_backup_directory
        self.backup_count: int = config.general.state_backup_count

        # Ensure backup directory exists
        Path(self.backup_dir).mkdir(parents=True, exist_ok=True)

        # Current state
        self.current_state: dict[str, Any] = {}
        self.last_save_time: datetime | None = None
        self._lock = asyncio.Lock()

    async def load_state(self) -> bool:
        """Load state from file asynchronously.

        Returns:
            True if state was loaded successfully, False otherwise
        """
        try:
            if not os.path.exists(self.state_file):
                logger.info(
                    "state_file_not_found",
                    action="loading_state",
                    message=(
                        f"State file {self.state_file} does not exist, starting with empty state"
                    ),
                    state_file=self.state_file,
                )
                return False

            # Read state file asynchronously
            state_data = await self._async_read_json(self.state_file)

            if state_data is None:
                logger.error(
                    "state_file_read_failed",
                    action="loading_state",
                    message=f"Failed to read state file {self.state_file}",
                    state_file=self.state_file,
                )
                return await self._recover_from_backup()

            # Verify state integrity
            if not await self._verify_state_integrity(state_data):
                logger.warning(
                    "state_integrity_check_failed",
                    action="loading_state",
                    message=(
                        f"State file {self.state_file} failed integrity check, attempting to "
                        f"recover from backup"
                    ),
                    state_file=self.state_file,
                )
                return await self._recover_from_backup()

            # State is valid, update current state
            async with self._lock:
                self.current_state = state_data["state"]

            logger.info(
                "state_loaded_successfully",
                action="loading_state",
                message=f"Successfully loaded state from {self.state_file}",
                state_file=self.state_file,
            )
            return True

        except Exception as e:
            logger.error(
                "state_load_error",
                action="loading_state",
                message=f"Error loading state from {self.state_file}: {e}",
                state_file=self.state_file,
                error=str(e),
                exc_info=True,
            )
            return await self._recover_from_backup()

    async def save_state(self, state: dict[str, Any]) -> bool:
        """Save state to file asynchronously.

        Args:
            state: State to save

        Returns:
            True if state was saved successfully, False otherwise
        """
        try:
            # Update current state
            async with self._lock:
                self.current_state = state

            # Create state data with metadata
            state_data = {
                "state": state,
                "metadata": {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "checksum": await self._calculate_checksum(state),
                },
            }

            # Create a backup before saving (rotation)
            await self._create_backup()

            # Write state asynchronously
            success = await self._async_write_json(self.state_file, state_data)

            if success:
                # Update last save time
                self.last_save_time = datetime.now(UTC)
                logger.info(
                    "state_saved_successfully",
                    action="saving_state",
                    message=f"Successfully saved state to {self.state_file}",
                    state_file=self.state_file,
                )
                return True
            else:
                logger.error(
                    "state_write_failed",
                    action="saving_state",
                    message=f"Failed to write state to {self.state_file}",
                    state_file=self.state_file,
                )
                return False

        except Exception as e:
            logger.error(
                "state_save_error",
                action="saving_state",
                message=f"Error saving state to {self.state_file}: {e}",
                state_file=self.state_file,
                error=str(e),
                exc_info=True,
            )
            return False

    async def get_current_state(self) -> dict[str, Any]:
        """Get the current state.

        Returns:
            Current state (copy)
        """
        async with self._lock:
            return self.current_state.copy()

    async def _create_backup(self) -> bool:
        """Create a backup of the current state file asynchronously.

        Returns:
            True if backup was created successfully, False otherwise
        """
        if not os.path.exists(self.state_file):
            return False

        try:
            # Generate backup filename with timestamp
            timestamp: int = int(time.time())
            backup_path: str = os.path.join(self.backup_dir, f"state_{timestamp}.json")

            # Copy current state file to backup asynchronously
            await self._async_copy_file(self.state_file, backup_path)

            # Rotate backups (keep only the most recent ones)
            await self._rotate_backups()

            logger.debug(
                "state_backup_created",
                action="creating_backup",
                message=f"Created state backup at {backup_path}",
                backup_path=backup_path,
                timestamp=timestamp,
            )
            return True

        except Exception as e:
            logger.error(
                "state_backup_error",
                action="creating_backup",
                message=f"Error creating state backup: {e}",
                error=str(e),
                exc_info=True,
            )
            return False

    async def _rotate_backups(self) -> None:
        """Rotate state backups asynchronously, keeping only the most recent ones."""
        try:
            files = await self._get_sorted_backup_files()

            # Remove excess backups
            for backup_path in files[self.backup_count :]:
                await self._async_remove_file(backup_path)
                logger.debug(
                    "old_backup_removed",
                    action="rotating_backups",
                    message=f"Removed old state backup {backup_path}",
                    backup_path=backup_path,
                )

        except Exception as e:
            logger.error(
                "backup_rotation_error",
                action="rotating_backups",
                message=f"Error rotating backups: {e}",
                error=str(e),
                exc_info=True,
            )

    async def _get_sorted_backup_files(self) -> list[str]:
        """Get sorted list of backup files asynchronously."""
        loop = asyncio.get_event_loop()

        def _get_backup_files() -> list[str]:
            files: list[str] = []
            for filename in os.listdir(self.backup_dir):
                if filename.startswith("state_") and filename.endswith(".json"):
                    backup_path: str = os.path.join(self.backup_dir, filename)
                    files.append(backup_path)

            if not files:
                return []

            # Sort by modification time (newest first)
            files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            return files

        return await loop.run_in_executor(None, _get_backup_files)

    async def _recover_from_backup(self) -> bool:
        """Attempt to recover state from a backup asynchronously.

        Returns:
            True if recovery was successful, False otherwise
        """
        try:
            files = await self._get_sorted_backup_files()

            if not files:
                logger.warning("No state backups available for recovery")
                return False

            # Try each backup in order until one works
            for backup_path in files:
                try:
                    # Read backup file
                    state_data = await self._async_read_json(backup_path)

                    if state_data is None:
                        logger.warning(
                            "backup_read_failed",
                            action="recovering_from_backup",
                            message=f"Failed to read backup {backup_path}",
                            backup_path=backup_path,
                        )
                        continue

                    # Verify state integrity
                    if await self._verify_state_integrity(state_data):
                        # Backup is valid, update current state
                        async with self._lock:
                            self.current_state = state_data["state"]

                        # Copy backup to state file
                        await self._async_copy_file(backup_path, self.state_file)

                        logger.info(
                            "state_recovered_from_backup",
                            action="recovering_from_backup",
                            message=f"Successfully recovered state from backup {backup_path}",
                            backup_path=backup_path,
                        )
                        return True

                except Exception as e:
                    logger.warning(
                        "backup_load_error",
                        action="recovering_from_backup",
                        message=f"Error loading backup {backup_path}: {e}",
                        backup_path=backup_path,
                        error=str(e),
                    )
                    continue

            # All backups failed
            logger.error("Failed to recover state from any backup")
            return False

        except Exception as e:
            logger.error(
                "recovery_process_error",
                action="recovering_from_backup",
                message=f"Error during recovery process: {e}",
                error=str(e),
                exc_info=True,
            )
            return False

    async def _verify_state_integrity(self, state_data: dict[str, Any]) -> bool:
        """Verify the integrity of a state asynchronously.

        Args:
            state_data: State data to verify

        Returns:
            True if state is valid, False otherwise
        """
        # Check for required top-level keys
        if "state" not in state_data or "metadata" not in state_data:
            return False

        # Check for required metadata keys
        metadata_raw = state_data.get("metadata", {})
        if not isinstance(metadata_raw, dict):
            return False
        metadata = cast(dict[str, Any], metadata_raw)

        if "timestamp" not in metadata or "checksum" not in metadata:
            return False

        # Verify checksum
        expected_checksum = metadata.get("checksum")
        if not isinstance(expected_checksum, str):
            type_name = (
                type(expected_checksum).__name__ if expected_checksum is not None else "None"
            )
            logger.error(
                "invalid_checksum_type",
                action="verifying_state_integrity",
                message=f"Expected checksum must be a string, got {type_name}",
                actual_type=type_name,
                expected_type="string",
            )
            return False

        actual_checksum = await self._calculate_checksum(state_data["state"])

        # Return true if checksums match
        return bool(expected_checksum == actual_checksum)

    async def _calculate_checksum(self, state: dict[str, Any]) -> str:
        """Calculate a checksum for a state asynchronously.

        Args:
            state: State to calculate checksum for

        Returns:
            Checksum string
        """
        loop = asyncio.get_event_loop()

        def _calc_sync() -> str:
            # For simplicity, using a JSON hash as the checksum
            state_json: str = json.dumps(state, sort_keys=True)
            return str(hash(state_json))

        return await loop.run_in_executor(None, _calc_sync)

    async def _async_read_json(self, file_path: str) -> dict[str, Any] | None:
        """Read JSON from a file asynchronously."""
        try:
            loop = asyncio.get_event_loop()

            def _read_sync() -> dict[str, Any]:
                with open(file_path, encoding="utf-8") as f:
                    data = json.load(f)
                    return cast(dict[str, Any], data)

            return await loop.run_in_executor(None, _read_sync)
        except Exception as e:
            logger.error(
                "json_read_error",
                action="reading_json_file",
                message=f"Error reading JSON from {file_path}: {e}",
                file_path=file_path,
                error=str(e),
            )
            return None

    async def _async_write_json(self, file_path: str, data: dict[str, Any]) -> bool:
        """Write JSON to a file asynchronously."""
        try:
            loop = asyncio.get_event_loop()

            def _write_sync() -> None:
                # Write to temporary file first
                temp_file = f"{file_path}.tmp"
                with open(temp_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                # Atomic replace
                os.replace(temp_file, file_path)

            await loop.run_in_executor(None, _write_sync)
            return True
        except Exception as e:
            logger.error(
                "json_write_error",
                action="writing_json_file",
                message=f"Error writing JSON to {file_path}: {e}",
                file_path=file_path,
                error=str(e),
            )
            return False

    async def _async_copy_file(self, src: str, dst: str) -> None:
        """Copy a file asynchronously."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, shutil.copy2, src, dst)

    async def _async_remove_file(self, file_path: str) -> None:
        """Remove a file asynchronously."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, os.remove, file_path)


async def load_async_state_manager(config: AppSettings) -> AsyncStateManager:
    """Create and initialize an async state manager.

    Args:
        config: Application configuration

    Returns:
        Initialized async state manager
    """
    state_manager = AsyncStateManager(config)
    await state_manager.load_state()
    return state_manager

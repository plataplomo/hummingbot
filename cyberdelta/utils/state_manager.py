"""State Management System.

This module provides utilities for managing application state persistence
and recovery in the CyberDeltaEngine trading system.
"""

from __future__ import annotations

import json
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


class StateManager:
    """Provide reliable state persistence and recovery.

    Responsible for:
    - Atomic state saving with validation
    - State restoration with integrity checks
    - State backup rotation
    - Corruption detection and recovery
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize the state manager.

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

    def load_state(self) -> bool:
        """Load state from file.

        Returns:
            True if state was loaded successfully, False otherwise

        """
        try:
            if not Path(self.state_file).exists():
                logger.info(
                    "state_file_not_exists_starting_empty",
                    state_file=self.state_file,
                    action="starting_with_empty_state",
                    message=(
                        f"State file {self.state_file} does not exist, starting with empty state"
                    ),
                )
                return False

            # Read state file
            with Path(self.state_file).open(encoding="utf-8") as file:
                state_data = json.load(file)

            # Verify state integrity
            if not self._verify_state_integrity(state_data):
                logger.warning(
                    "state_integrity_check_failed",
                    state_file=self.state_file,
                    action="attempting_backup_recovery",
                    message=(
                        f"State file {self.state_file} failed integrity check, "
                        f"attempting to recover from backup"
                    ),
                )
                return self._recover_from_backup()

            # State is valid, update current state
            self.current_state = state_data["state"]
            logger.info(
                "state_loaded_successfully",
                state_file=self.state_file,
                state_keys=list(self.current_state.keys()),
                action="state_loaded",
                message=f"Successfully loaded state from {self.state_file}",
            )

        except json.JSONDecodeError as e:
            logger.exception(
                "state_file_decode_error",
                state_file=self.state_file,
                error=str(e),
                action="attempting_backup_recovery",
                message=(
                    f"Error decoding state file {self.state_file}, "
                    f"attempting to recover from backup"
                ),
            )
            return self._recover_from_backup()

        except (
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
            ArithmeticError,
            OSError,
            PermissionError,
        ) as e:
            logger.exception(
                "state_loading_error",
                state_file=self.state_file,
                error=str(e),
                action="attempting_backup_recovery",
                message=f"Error loading state from {self.state_file}: {e!s}",
            )
            return self._recover_from_backup()
        else:
            return True

    def save_state(self, state: dict[str, Any]) -> bool:
        """Save state to file.

        Args:
            state: State to save

        Returns:
            True if state was saved successfully, False otherwise

        """
        try:
            # Update current state
            self.current_state = state

            # Create state data with metadata
            state_data = {
                "state": state,
                "metadata": {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "checksum": self._calculate_checksum(state),
                },
            }

            # Create a backup before saving (rotation)
            self._create_backup()

            # Write state to a temporary file first
            temp_file: str = f"{self.state_file}.tmp"
            with Path(temp_file).open("w", encoding="utf-8") as file:
                json.dump(state_data, file, indent=2)

            # Atomically replace the state file
            shutil.move(temp_file, self.state_file)

            # Update last save time
            self.last_save_time = datetime.now(UTC)

            logger.info(
                "state_saved_successfully",
                state_file=self.state_file,
                state_keys=list(state.keys()),
                checksum=state_data["metadata"]["checksum"],
                action="state_saved",
                message=f"Successfully saved state to {self.state_file}",
            )

        except (
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
            ArithmeticError,
            OSError,
            PermissionError,
        ) as e:
            logger.exception(
                "state_saving_error",
                state_file=self.state_file,
                error=str(e),
                action="save_failed",
                message=f"Error saving state to {self.state_file}: {e!s}",
            )
            return False
        else:
            return True

    def get_current_state(self) -> dict[str, Any]:
        """Get the current state.

        Returns:
            Current state

        """
        return self.current_state.copy()

    def _create_backup(self) -> bool:
        """Create a backup of the current state file.

        Returns:
            True if backup was created successfully, False otherwise

        """
        if not Path(self.state_file).exists():
            return False

        try:
            # Generate backup filename with timestamp
            timestamp: int = int(time.time())
            backup_path: str = str(Path(self.backup_dir) / f"state_{timestamp}.json")

            # Copy current state file to backup
            shutil.copy2(self.state_file, backup_path)

            # Rotate backups (keep only the most recent ones)
            self._rotate_backups()

            logger.debug(
                "state_backup_created",
                backup_path=backup_path,
                timestamp=timestamp,
                action="backup_created",
                message=f"Created state backup at {backup_path}",
            )

        except (
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
            ArithmeticError,
            OSError,
            PermissionError,
        ) as e:
            logger.exception(
                "state_backup_creation_error",
                backup_dir=self.backup_dir,
                state_file=self.state_file,
                error=str(e),
                action="backup_failed",
                message=f"Error creating state backup: {e!s}",
            )
            return False
        else:
            return True

    def _rotate_backups(self) -> None:
        """Rotate state backups, keeping only the most recent ones."""
        try:
            # Get all backup files
            backup_dir_path = Path(self.backup_dir)
            files: list[str] = [
                str(file_path)
                for file_path in backup_dir_path.iterdir()
                if file_path.name.startswith("state_") and file_path.name.endswith(".json")
            ]

            # Sort by modification time (newest first)
            def get_mtime(file_path: str) -> float:
                return Path(file_path).stat().st_mtime

            files.sort(key=get_mtime, reverse=True)

            # Remove excess backups
            for backup_path in files[self.backup_count :]:
                Path(backup_path).unlink()
                logger.debug(
                    "old_backup_removed",
                    backup_path=backup_path,
                    backup_count_limit=self.backup_count,
                    action="backup_rotated",
                    message=f"Removed old state backup {backup_path}",
                )

        except (
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
            ArithmeticError,
            OSError,
            PermissionError,
        ) as e:
            logger.exception(
                "backup_rotation_error",
                backup_dir=self.backup_dir,
                backup_count=self.backup_count,
                error=str(e),
                action="rotation_failed",
                message=f"Error rotating backups: {e!s}",
            )

    def _recover_from_backup(self) -> bool:
        """Attempt to recover state from a backup.

        Returns:
            True if recovery was successful, False otherwise

        """
        try:
            # Get all backup files
            backup_dir_path = Path(self.backup_dir)
            files: list[str] = [
                str(file_path)
                for file_path in backup_dir_path.iterdir()
                if file_path.name.startswith("state_") and file_path.name.endswith(".json")
            ]

            if not files:
                logger.warning(
                    "no_backups_available_for_recovery",
                    backup_dir=self.backup_dir,
                    action="recovery_failed",
                    message="No state backups available for recovery",
                )
                return False

            # Sort by modification time (newest first)
            def get_mtime(file_path: str) -> float:
                return Path(file_path).stat().st_mtime

            files.sort(key=get_mtime, reverse=True)

            # Try each backup in order until one works
            for backup_path in files:
                try:
                    # Read backup file
                    with Path(backup_path).open(encoding="utf-8") as file:
                        state_data = json.load(file)

                    # Verify state integrity
                    if self._verify_state_integrity(state_data):
                        # Backup is valid, update current state
                        self.current_state = state_data["state"]

                        # Copy backup to state file
                        shutil.copy2(backup_path, self.state_file)

                        logger.info(
                            "state_recovered_from_backup",
                            backup_path=backup_path,
                            state_file=self.state_file,
                            state_keys=list(self.current_state.keys()),
                            action="recovery_successful",
                            message=f"Successfully recovered state from backup {backup_path}",
                        )
                        return True

                except (json.JSONDecodeError, FileNotFoundError, PermissionError, OSError) as e:
                    logger.warning(
                        "backup_loading_error",
                        backup_path=backup_path,
                        error=str(e),
                        action="trying_next_backup",
                        message=f"Error loading backup {backup_path}: {e!s}",
                    )
                    continue

            # All backups failed
            logger.error(
                "all_backups_failed_recovery",
                backup_dir=self.backup_dir,
                backups_tried=len(files),
                action="recovery_failed",
                message="Failed to recover state from any backup",
            )

        except (
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
            ArithmeticError,
            OSError,
            PermissionError,
        ) as e:
            logger.exception(
                "recovery_process_error",
                backup_dir=self.backup_dir,
                error=str(e),
                action="recovery_failed",
                message=f"Error during recovery process: {e!s}",
            )
            return False
        else:
            return False

    def _verify_state_integrity(self, state_data: dict[str, Any]) -> bool:
        """Verify the integrity of a state.

        Args:
            state_data: State data to verify

        Returns:
            True if state is valid, False otherwise

        """
        # Type hint ensures state_data is a dict, no runtime check needed here.

        # Check for required top-level keys
        if "state" not in state_data or "metadata" not in state_data:
            return False

        # Check for required metadata keys
        metadata_raw = state_data.get("metadata", {})
        if not isinstance(metadata_raw, dict):
            return False

        # Type is now known to be dict, cast it explicitly
        metadata = cast("dict[str, Any]", metadata_raw)

        if "timestamp" not in metadata or "checksum" not in metadata:
            return False

        # Verify checksum
        expected_checksum_raw = metadata.get("checksum")
        if not isinstance(expected_checksum_raw, str):
            # Get type name safely
            type_name = (
                type(expected_checksum_raw).__name__
                if expected_checksum_raw is not None
                else "None"
            )
            logger.error(
                "checksum_type_mismatch",
                expected_type="string",
                actual_type=type_name,
                action="integrity_check_failed",
                message=f"Expected checksum must be a string, got {type_name}",
            )
            return False

        # Type narrowed here:
        expected_checksum: str = expected_checksum_raw
        actual_checksum = self._calculate_checksum(state_data["state"])

        # Return true if checksums match
        return bool(expected_checksum == actual_checksum)

    def _calculate_checksum(self, state: dict[str, Any]) -> str:
        """Calculate a checksum for a state.

        Args:
            state: State to calculate checksum for

        Returns:
            Checksum string

        """
        # For simplicity, we're using a JSON hash as the checksum
        # In a production system, you might want to use a more robust algorithm
        state_json: str = json.dumps(state, sort_keys=True)
        return str(hash(state_json))


def load_state_manager(config: AppSettings) -> StateManager:
    """Create and initialize a state manager.

    Args:
        config: Application configuration

    Returns:
        Initialized state manager

    """
    state_manager = StateManager(config)
    state_manager.load_state()
    return state_manager

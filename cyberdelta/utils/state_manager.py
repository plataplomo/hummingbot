import json
import logging
import os
import shutil
import time
from datetime import UTC, datetime
from typing import Any

from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)


class StateManager:
    """
    Provide reliable state persistence and recovery.

    Responsible for:
    - Atomic state saving with validation
    - State restoration with integrity checks
    - State backup rotation
    - Corruption detection and recovery
    """

    def __init__(self, config: Config) -> None:
        """
        Initialize the state manager.

        Args:
            config: Application configuration
        """
        self.config = config

        # Load state parameters from config
        self.state_file = config.get("general.state_file", "state.json")
        self.backup_dir = config.get("general.state_backup_directory", "state_backups")
        self.backup_count = config.get("general.state_backup_count", 5)

        # Ensure backup directory exists
        os.makedirs(self.backup_dir, exist_ok=True)

        # Current state
        self.current_state: dict[str, Any] = {}
        self.last_save_time: datetime | None = None

    def load_state(self) -> bool:
        """
        Load state from file.

        Returns:
            True if state was loaded successfully, False otherwise
        """
        try:
            if not os.path.exists(self.state_file):
                logger.info(
                    f"State file {self.state_file} does not exist, starting with empty state"
                )
                return False

            # Read state file
            with open(self.state_file) as file:
                state_data = json.load(file)

            # Verify state integrity
            if not self._verify_state_integrity(state_data):
                logger.warning(
                    f"State file {self.state_file} failed integrity check, "
                    f"attempting to recover from backup"
                )
                return self._recover_from_backup()

            # State is valid, update current state
            self.current_state = state_data["state"]
            logger.info(f"Successfully loaded state from {self.state_file}")

            return True

        except json.JSONDecodeError:
            logger.error(
                f"Error decoding state file {self.state_file}, attempting to recover from backup"
            )
            return self._recover_from_backup()

        except Exception as e:
            logger.error(f"Error loading state from {self.state_file}: {str(e)}", exc_info=True)
            return self._recover_from_backup()

    def save_state(self, state: dict[str, Any]) -> bool:
        """
        Save state to file.

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
            temp_file = f"{self.state_file}.tmp"
            with open(temp_file, "w") as file:
                json.dump(state_data, file, indent=2)

            # Atomically replace the state file
            shutil.move(temp_file, self.state_file)

            # Update last save time
            self.last_save_time = datetime.now(UTC)

            logger.info(f"Successfully saved state to {self.state_file}")
            return True

        except Exception as e:
            logger.error(f"Error saving state to {self.state_file}: {str(e)}", exc_info=True)
            return False

    def get_current_state(self) -> dict[str, Any]:
        """
        Get the current state.

        Returns:
            Current state
        """
        return self.current_state.copy()

    def _create_backup(self) -> bool:
        """
        Create a backup of the current state file.

        Returns:
            True if backup was created successfully, False otherwise
        """
        if not os.path.exists(self.state_file):
            return False

        try:
            # Generate backup filename with timestamp
            timestamp = int(time.time())
            backup_path = os.path.join(self.backup_dir, f"state_{timestamp}.json")

            # Copy current state file to backup
            shutil.copy2(self.state_file, backup_path)

            # Rotate backups (keep only the most recent ones)
            self._rotate_backups()

            logger.debug(f"Created state backup at {backup_path}")
            return True

        except Exception as e:
            logger.error(f"Error creating state backup: {str(e)}", exc_info=True)
            return False

    def _rotate_backups(self) -> None:
        """Rotate state backups, keeping only the most recent ones."""
        try:
            # Get all backup files
            backup_files = []
            for filename in os.listdir(self.backup_dir):
                if filename.startswith("state_") and filename.endswith(".json"):
                    backup_path = os.path.join(self.backup_dir, filename)
                    backup_files.append((backup_path, os.path.getmtime(backup_path)))

            # Sort by modification time (newest first)
            backup_files.sort(key=lambda x: x[1], reverse=True)

            # Remove excess backups
            for backup_path, _ in backup_files[self.backup_count :]:
                os.remove(backup_path)
                logger.debug(f"Removed old state backup {backup_path}")

        except Exception as e:
            logger.error(f"Error rotating backups: {str(e)}", exc_info=True)

    def _recover_from_backup(self) -> bool:
        """
        Attempt to recover state from a backup.

        Returns:
            True if recovery was successful, False otherwise
        """
        try:
            # Get all backup files
            backup_files = []
            for filename in os.listdir(self.backup_dir):
                if filename.startswith("state_") and filename.endswith(".json"):
                    backup_path = os.path.join(self.backup_dir, filename)
                    backup_files.append((backup_path, os.path.getmtime(backup_path)))

            if not backup_files:
                logger.warning("No state backups available for recovery")
                return False

            # Sort by modification time (newest first)
            backup_files.sort(key=lambda x: x[1], reverse=True)

            # Try each backup in order until one works
            for backup_path, _ in backup_files:
                try:
                    # Read backup file
                    with open(backup_path) as file:
                        state_data = json.load(file)

                    # Verify state integrity
                    if self._verify_state_integrity(state_data):
                        # Backup is valid, update current state
                        self.current_state = state_data["state"]

                        # Copy backup to state file
                        shutil.copy2(backup_path, self.state_file)

                        logger.info(f"Successfully recovered state from backup {backup_path}")
                        return True

                except Exception as e:
                    logger.warning(f"Error loading backup {backup_path}: {str(e)}")
                    continue

            # All backups failed
            logger.error("Failed to recover state from any backup")
            return False

        except Exception as e:
            logger.error(f"Error during recovery process: {str(e)}", exc_info=True)
            return False

    def _verify_state_integrity(self, state_data: dict[str, Any]) -> bool:
        """
        Verify the integrity of a state.

        Args:
            state_data: State data to verify

        Returns:
            True if state is valid, False otherwise
        """
        # Check if state data has the expected structure
        if not isinstance(state_data, dict):
            return False

        # Check for required top-level keys
        if "state" not in state_data or "metadata" not in state_data:
            return False

        # Check for required metadata keys
        metadata = state_data.get("metadata", {})
        if not isinstance(metadata, dict):
            return False

        if "timestamp" not in metadata or "checksum" not in metadata:
            return False

        # Verify checksum
        expected_checksum = metadata["checksum"]
        actual_checksum = self._calculate_checksum(state_data["state"])

        # Return true if checksums match
        return expected_checksum == actual_checksum

    def _calculate_checksum(self, state: dict[str, Any]) -> str:
        """
        Calculate a checksum for a state.

        Args:
            state: State to calculate checksum for

        Returns:
            Checksum string
        """
        # For simplicity, we're using a JSON hash as the checksum
        # In a production system, you might want to use a more robust algorithm
        state_json = json.dumps(state, sort_keys=True)
        return str(hash(state_json))


def load_state_manager(config: Config) -> StateManager:
    """
    Create and initialize a state manager.

    Args:
        config: Application configuration

    Returns:
        Initialized state manager
    """
    state_manager = StateManager(config)
    state_manager.load_state()
    return state_manager

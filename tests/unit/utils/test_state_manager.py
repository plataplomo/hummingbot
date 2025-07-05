"""Unit tests for the state manager module.

Tests state persistence and recovery functionality including backup rotation.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from cyberdelta.config.models.config_models import AppSettings, GeneralSettings
from cyberdelta.utils.state_manager import StateManager, load_state_manager


@pytest.fixture
def temp_state_dir(tmp_path: Path) -> Path:
    """Create temporary directory for state testing."""
    state_dir = tmp_path / "state_test"
    state_dir.mkdir(exist_ok=True)
    return state_dir


@pytest.fixture
def mock_config(temp_state_dir: Path) -> Mock:
    """Create mock configuration with test paths."""
    config = Mock(spec=AppSettings)
    config.general = Mock(spec=GeneralSettings)
    config.general.state_file = str(temp_state_dir / "state.json")
    config.general.state_backup_directory = str(temp_state_dir / "backups")
    config.general.state_backup_count = 3
    return config


@pytest.fixture
def state_manager(mock_config: Mock) -> StateManager:
    """Create StateManager instance for testing."""
    return StateManager(mock_config)


class TestStateManagerInit:
    """Test suite for StateManager initialization."""

    # ==================== SUCCESS CASES ====================

    def test_init_success(self, mock_config: Mock) -> None:
        """Test successful initialization of StateManager."""
        # Act
        manager = StateManager(mock_config)

        # Assert
        assert manager.config == mock_config
        assert manager.state_file == mock_config.general.state_file
        assert manager.backup_dir == mock_config.general.state_backup_directory
        assert manager.backup_count == 3
        assert manager.current_state == {}
        assert manager.last_save_time is None

    def test_init_creates_backup_directory(self, mock_config: Mock, temp_state_dir: Path) -> None:
        """Test that initialization creates backup directory if it doesn't exist."""
        # Arrange
        backup_dir = temp_state_dir / "backups"
        assert not backup_dir.exists()

        # Act
        StateManager(mock_config)

        # Assert
        assert backup_dir.exists()
        assert backup_dir.is_dir()

    # ==================== EDGE CASES ====================

    def test_init_edge_existing_backup_directory(
        self, mock_config: Mock, temp_state_dir: Path
    ) -> None:
        """Test initialization when backup directory already exists."""
        # Arrange
        backup_dir = temp_state_dir / "backups"
        backup_dir.mkdir(exist_ok=True)
        # Create a file in the directory to ensure it's not cleared
        test_file = backup_dir / "test.txt"
        test_file.write_text("existing file")

        # Act
        StateManager(mock_config)

        # Assert
        assert backup_dir.exists()
        assert test_file.exists()
        assert test_file.read_text() == "existing file"


class TestLoadState:
    """Test suite for state loading functionality."""

    # ==================== SUCCESS CASES ====================

    def test_load_state_success_valid_file(
        self, state_manager: StateManager, temp_state_dir: Path
    ) -> None:
        """Test successful loading of valid state file."""
        # Arrange
        test_state = {"key1": "value1", "key2": 42}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": str(hash(json.dumps(test_state, sort_keys=True))),
            },
        }
        state_file = Path(state_manager.state_file)
        state_file.write_text(json.dumps(state_data, indent=2), encoding="utf-8")

        # Act
        result = state_manager.load_state()

        # Assert
        assert result is True
        assert state_manager.current_state == test_state

    def test_load_state_success_empty_state(self, state_manager: StateManager) -> None:
        """Test loading when state file doesn't exist."""
        # Act
        result = state_manager.load_state()

        # Assert
        assert result is False
        assert state_manager.current_state == {}

    # ==================== EDGE CASES ====================

    def test_load_state_edge_empty_state_dict(
        self, state_manager: StateManager, temp_state_dir: Path
    ) -> None:
        """Test loading state with empty state dictionary."""
        # Arrange
        test_state: dict[str, Any] = {}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": str(hash(json.dumps(test_state, sort_keys=True))),
            },
        }
        state_file = Path(state_manager.state_file)
        state_file.write_text(json.dumps(state_data, indent=2), encoding="utf-8")

        # Act
        result = state_manager.load_state()

        # Assert
        assert result is True
        assert state_manager.current_state == {}

    # ==================== FAILURE CASES ====================

    def test_load_state_failure_invalid_json(
        self, state_manager: StateManager, temp_state_dir: Path
    ) -> None:
        """Test loading state with invalid JSON."""
        # Arrange
        state_file = Path(state_manager.state_file)
        state_file.write_text("{ invalid json", encoding="utf-8")

        # Act
        with patch.object(
            state_manager, "_recover_from_backup", return_value=False
        ) as mock_recover:
            result = state_manager.load_state()

        # Assert
        assert result is False
        mock_recover.assert_called_once()
        # Verify that current_state remains clean after failed recovery
        assert state_manager.current_state == {}

    def test_load_state_failure_invalid_checksum(
        self, state_manager: StateManager, temp_state_dir: Path
    ) -> None:
        """Test loading state with invalid checksum."""
        # Arrange
        test_state = {"key": "value"}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": "invalid_checksum",
            },
        }
        state_file = Path(state_manager.state_file)
        state_file.write_text(json.dumps(state_data, indent=2), encoding="utf-8")

        # Act
        with patch.object(
            state_manager, "_recover_from_backup", return_value=False
        ) as mock_recover:
            result = state_manager.load_state()

        # Assert
        assert result is False
        mock_recover.assert_called_once()

    def test_load_state_failure_missing_metadata(
        self, state_manager: StateManager, temp_state_dir: Path
    ) -> None:
        """Test loading state without metadata."""
        # Arrange
        state_data = {"state": {"key": "value"}}
        state_file = Path(state_manager.state_file)
        state_file.write_text(json.dumps(state_data, indent=2), encoding="utf-8")

        # Act
        with patch.object(
            state_manager, "_recover_from_backup", return_value=False
        ) as mock_recover:
            result = state_manager.load_state()

        # Assert
        assert result is False
        mock_recover.assert_called_once()


class TestSaveState:
    """Test suite for state saving functionality."""

    # ==================== SUCCESS CASES ====================

    def test_save_state_success(self, state_manager: StateManager) -> None:
        """Test successful state saving."""
        # Arrange
        test_state = {"key1": "value1", "key2": 42}

        # Act
        result = state_manager.save_state(test_state)

        # Assert
        assert result is True
        assert state_manager.current_state == test_state
        assert state_manager.last_save_time is not None

        # Verify file contents
        state_file = Path(state_manager.state_file)
        assert state_file.exists()
        saved_data = json.loads(state_file.read_text(encoding="utf-8"))
        assert saved_data["state"] == test_state
        assert "metadata" in saved_data
        assert "timestamp" in saved_data["metadata"]
        assert "checksum" in saved_data["metadata"]

    @pytest.mark.timing
    def test_save_state_success_with_backup(self, state_manager: StateManager) -> None:
        """Test state saving creates backup of existing state."""
        # Arrange
        # Save initial state
        initial_state = {"initial": "state"}
        state_manager.save_state(initial_state)

        # Wait a bit to ensure different timestamps
        time.sleep(0.1)

        # Act - save new state
        new_state = {"new": "state"}
        result = state_manager.save_state(new_state)

        # Assert
        assert result is True
        backup_dir = Path(state_manager.backup_dir)
        backups = list(backup_dir.glob("state_*.json"))
        assert len(backups) == 1  # One backup created

    # ==================== EDGE CASES ====================

    def test_save_state_edge_empty_state(self, state_manager: StateManager) -> None:
        """Test saving empty state dictionary."""
        # Arrange
        empty_state: dict[str, Any] = {}

        # Act
        result = state_manager.save_state(empty_state)

        # Assert
        assert result is True
        assert state_manager.current_state == {}

    def test_save_state_edge_complex_nested_state(self, state_manager: StateManager) -> None:
        """Test saving complex nested state."""
        # Arrange
        complex_state = {
            "positions": [{"id": 1, "size": 100.5}, {"id": 2, "size": -50.3}],
            "balances": {"USD": 10000.0, "BTC": 0.5},
            "metadata": {"last_update": "2024-01-01", "active": True},
        }

        # Act
        result = state_manager.save_state(complex_state)

        # Assert
        assert result is True
        assert state_manager.current_state == complex_state

    # ==================== FAILURE CASES ====================

    def test_save_state_failure_write_error(self, state_manager: StateManager) -> None:
        """Test save state handles write errors gracefully."""
        # Arrange
        test_state = {"key": "value"}

        # Act
        with patch("pathlib.Path.open", side_effect=PermissionError("No write permission")):
            result = state_manager.save_state(test_state)

        # Assert
        assert result is False


class TestGetCurrentState:
    """Test suite for getting current state."""

    # ==================== SUCCESS CASES ====================

    def test_get_current_state_success(self, state_manager: StateManager) -> None:
        """Test getting current state returns copy."""
        # Arrange
        test_state = {"key": "value"}
        state_manager.current_state = test_state

        # Act
        result = state_manager.get_current_state()

        # Assert
        assert result == test_state
        assert result is not test_state  # Should be a copy

    def test_get_current_state_success_empty(self, state_manager: StateManager) -> None:
        """Test getting empty current state."""
        # Act
        result = state_manager.get_current_state()

        # Assert
        assert result == {}


class TestBackupFunctionality:
    """Test suite for backup-related functionality."""

    # ==================== SUCCESS CASES ====================

    def test_create_backup_success(self, state_manager: StateManager) -> None:
        """Test backup creation through public save_state API."""
        # Arrange
        initial_state = {"key": "initial_value"}
        state_manager.save_state(initial_state)

        # Act - Save new state which triggers backup of previous state
        new_state = {"key": "new_value"}
        result = state_manager.save_state(new_state)

        # Assert - Backup should be created automatically
        assert result is True
        backup_dir = Path(state_manager.backup_dir)
        backups = list(backup_dir.glob("state_*.json"))
        assert len(backups) > 0

    @pytest.mark.timing
    def test_rotate_backups_success(self, state_manager: StateManager) -> None:
        """Test backup rotation keeps only recent backups through save_state."""
        # Arrange
        test_state = {"key": "value"}
        state_manager.save_state(test_state)

        # Create multiple backups by saving state multiple times
        # The rotation should happen automatically when saving
        for i in range(5):
            time.sleep(0.1)  # Ensure different timestamps
            state_manager.save_state({"key": f"value{i}"})

        # Assert - Verify rotation happened through public behavior
        backup_dir = Path(state_manager.backup_dir)
        backups = list(backup_dir.glob("state_*.json"))
        # Rotation should have kept only backup_count backups
        assert len(backups) <= state_manager.backup_count

    # ==================== EDGE CASES ====================

    def test_create_backup_edge_no_state_file(self, state_manager: StateManager) -> None:
        """Test backup creation when state file doesn't exist through save_state."""
        # Act - Try to save state which should handle backup creation
        # When there's no existing state file, save_state should still work
        result = state_manager.save_state({"first": "state"})

        # Assert - save_state should succeed even without existing state
        assert result is True
        # Verify no backup was created since there was no previous state
        backup_dir = Path(state_manager.backup_dir)
        backups = list(backup_dir.glob("state_*.json"))
        assert len(backups) == 0  # No backup for first save

    # ==================== FAILURE CASES ====================

    def test_create_backup_failure_copy_error(self, state_manager: StateManager) -> None:
        """Test backup creation handles copy errors through save_state."""
        # Arrange
        state_manager.save_state({"key": "value"})

        # Act - Save new state with backup creation failing
        with patch("shutil.copy2", side_effect=OSError("Copy failed")):
            # save_state should still succeed even if backup fails
            result = state_manager.save_state({"key": "new_value"})

        # Assert - State should be saved even if backup fails
        assert result is True
        # Verify state was updated despite backup failure
        current_state = state_manager.get_current_state()
        assert current_state.get("key") == "new_value"


class TestStateRecovery:
    """Test suite for state recovery functionality."""

    # ==================== SUCCESS CASES ====================

    def test_recover_from_backup_success(self, state_manager: StateManager) -> None:
        """Test successful recovery from backup through load_state."""
        # Arrange
        # First save a valid state to create backup
        original_state = {"original": "data"}
        state_manager.save_state(original_state)

        # Now corrupt the main state file
        Path(state_manager.state_file).write_text("invalid json", encoding="utf-8")

        # Act - load_state should recover from backup
        result = state_manager.load_state()

        # Assert
        assert result is True
        # Should have recovered the original state from backup
        current_state = state_manager.get_current_state()
        assert current_state == original_state
        # State file should be restored
        assert Path(state_manager.state_file).exists()

    # ==================== EDGE CASES ====================

    def test_recover_from_backup_edge_multiple_backups(self, state_manager: StateManager) -> None:
        """Test recovery tries backups in order."""
        # Arrange
        backup_dir = Path(state_manager.backup_dir)

        # Create invalid backup (older)
        invalid_backup = backup_dir / "state_10000.json"
        invalid_backup.write_text("invalid json", encoding="utf-8")

        # Create valid backup (newer)
        valid_state = {"valid": "backup"}
        valid_data = {
            "state": valid_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": str(hash(json.dumps(valid_state, sort_keys=True))),
            },
        }
        valid_backup = backup_dir / "state_20000.json"
        valid_backup.write_text(json.dumps(valid_data, indent=2), encoding="utf-8")

        # Touch files to set modification times
        invalid_backup.touch()
        time.sleep(0.1)
        valid_backup.touch()

        # Act - Corrupt main state file and load, should recover from backup
        Path(state_manager.state_file).write_text("corrupted", encoding="utf-8")
        result = state_manager.load_state()

        # Assert
        assert result is True
        # Should have recovered from the valid backup
        current_state = state_manager.get_current_state()
        assert current_state == valid_state

    # ==================== FAILURE CASES ====================

    def test_recover_from_backup_failure_no_backups(self, state_manager: StateManager) -> None:
        """Test recovery fails when no backups available through load_state."""
        # Arrange - Create corrupted state file with no backups
        Path(state_manager.state_file).write_text("invalid json", encoding="utf-8")

        # Act - load_state should fail with no backups to recover from
        result = state_manager.load_state()

        # Assert
        assert result is False

    def test_recover_from_backup_failure_all_invalid(self, state_manager: StateManager) -> None:
        """Test recovery fails when all backups are invalid through load_state."""
        # Arrange
        backup_dir = Path(state_manager.backup_dir)

        # Create multiple invalid backups
        for i in range(3):
            backup_file = backup_dir / f"state_{i}.json"
            backup_file.write_text("invalid json", encoding="utf-8")

        # Create corrupted main state file
        Path(state_manager.state_file).write_text("invalid json", encoding="utf-8")

        # Act - load_state should fail when all backups are invalid
        result = state_manager.load_state()

        # Assert
        assert result is False


class TestStateIntegrity:
    """Test suite for state integrity verification through public API."""

    # ==================== SUCCESS CASES ====================

    def test_verify_state_integrity_success(self, state_manager: StateManager) -> None:
        """Test successful state integrity verification through save/load."""
        # Arrange
        test_state = {"key": "value"}

        # Act - Save state which includes integrity data
        save_result = state_manager.save_state(test_state)
        # Load state which verifies integrity
        load_result = state_manager.load_state()

        # Assert
        assert save_result is True
        assert load_result is True
        assert state_manager.get_current_state() == test_state

    # ==================== EDGE CASES ====================

    def test_verify_state_integrity_edge_empty_state(self, state_manager: StateManager) -> None:
        """Test integrity check with empty state through save/load."""
        # Arrange
        empty_state: dict[str, Any] = {}

        # Act - Save empty state and verify it loads correctly
        save_result = state_manager.save_state(empty_state)
        load_result = state_manager.load_state()

        # Assert
        assert save_result is True
        assert load_result is True
        assert state_manager.get_current_state() == empty_state

    # ==================== FAILURE CASES ====================

    def test_verify_state_integrity_failure_missing_state(
        self, state_manager: StateManager
    ) -> None:
        """Test integrity check fails with missing state key through load_state."""
        # Arrange - Create invalid state file missing state key
        state_data = {
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": "some_checksum",
            },
        }
        state_file = Path(state_manager.state_file)
        state_file.write_text(json.dumps(state_data), encoding="utf-8")

        # Act - load_state should fail and try recovery
        with patch.object(state_manager, "_recover_from_backup", return_value=False):
            result = state_manager.load_state()

        # Assert
        assert result is False

    def test_verify_state_integrity_failure_wrong_checksum(
        self, state_manager: StateManager
    ) -> None:
        """Test integrity check fails with wrong checksum through load_state."""
        # Arrange - Create state file with wrong checksum
        test_state = {"key": "value"}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": "wrong_checksum",
            },
        }
        state_file = Path(state_manager.state_file)
        state_file.write_text(json.dumps(state_data), encoding="utf-8")

        # Act - load_state should fail due to checksum mismatch
        with patch.object(state_manager, "_recover_from_backup", return_value=False):
            result = state_manager.load_state()

        # Assert
        assert result is False

    def test_verify_state_integrity_failure_non_string_checksum(
        self, state_manager: StateManager
    ) -> None:
        """Test integrity check fails with non-string checksum through load_state."""
        # Arrange - Create state file with non-string checksum
        state_data = {
            "state": {"key": "value"},
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": 12345,  # Not a string
            },
        }
        state_file = Path(state_manager.state_file)
        state_file.write_text(json.dumps(state_data), encoding="utf-8")

        # Act - load_state should fail due to invalid checksum type
        with patch.object(state_manager, "_recover_from_backup", return_value=False):
            result = state_manager.load_state()

        # Assert
        assert result is False


class TestCalculateChecksum:
    """Test suite for checksum calculation through public API."""

    # ==================== SUCCESS CASES ====================

    def test_calculate_checksum_success(self, state_manager: StateManager) -> None:
        """Test checksum calculation for state through save/load."""
        # Arrange
        test_state = {"key": "value"}

        # Act - Save state twice and verify consistency
        state_manager.save_state(test_state)
        # Read the saved file to check checksum consistency
        state_file1 = Path(state_manager.state_file)
        data1 = json.loads(state_file1.read_text())

        # Save again and read
        state_manager.save_state(test_state)
        data2 = json.loads(state_file1.read_text())

        # Assert - Same state should produce same checksum
        assert "metadata" in data1
        assert "checksum" in data1["metadata"]
        assert "metadata" in data2
        assert "checksum" in data2["metadata"]
        assert isinstance(data1["metadata"]["checksum"], str)
        assert data1["metadata"]["checksum"] == data2["metadata"]["checksum"]

    def test_calculate_checksum_success_different_states(self, state_manager: StateManager) -> None:
        """Test different states produce different checksums through save."""
        # Arrange
        state1 = {"key": "value1"}
        state2 = {"key": "value2"}

        # Act - Save different states and read checksums
        state_manager.save_state(state1)
        state_file = Path(state_manager.state_file)
        data1 = json.loads(state_file.read_text())
        checksum1 = data1["metadata"]["checksum"]

        state_manager.save_state(state2)
        data2 = json.loads(state_file.read_text())
        checksum2 = data2["metadata"]["checksum"]

        # Assert
        assert checksum1 != checksum2

    # ==================== EDGE CASES ====================

    def test_calculate_checksum_edge_order_independent(self, state_manager: StateManager) -> None:
        """Test checksum is order-independent for dict keys through save."""
        # Arrange
        state1 = {"a": 1, "b": 2, "c": 3}
        state2 = {"c": 3, "a": 1, "b": 2}

        # Act - Save states with different key order
        state_manager.save_state(state1)
        state_file = Path(state_manager.state_file)
        data1 = json.loads(state_file.read_text())
        checksum1 = data1["metadata"]["checksum"]

        state_manager.save_state(state2)
        data2 = json.loads(state_file.read_text())
        checksum2 = data2["metadata"]["checksum"]

        # Assert - Checksums should be the same regardless of key order
        assert checksum1 == checksum2


class TestLoadStateManager:
    """Test suite for load_state_manager factory function."""

    # ==================== SUCCESS CASES ====================

    def test_load_state_manager_success(self, mock_config: Mock) -> None:
        """Test successful creation and initialization of StateManager."""
        # Act
        with patch.object(StateManager, "load_state", return_value=True) as mock_load:
            manager = load_state_manager(mock_config)

        # Assert
        assert isinstance(manager, StateManager)
        assert manager.config == mock_config
        mock_load.assert_called_once()

    def test_load_state_manager_success_no_existing_state(self, mock_config: Mock) -> None:
        """Test creation when no existing state file."""
        # Act
        with patch.object(StateManager, "load_state", return_value=False) as mock_load:
            manager = load_state_manager(mock_config)

        # Assert
        assert isinstance(manager, StateManager)
        mock_load.assert_called_once()

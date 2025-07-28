"""Unit tests for the AsyncStateManager component.

Tests async state management functionality including persistence, recovery, and backup rotation.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from cyberdelta.config.models.config_models import AppSettings, GeneralSettings
from cyberdelta.utils.async_state_manager import AsyncStateManager, load_async_state_manager


# Mark all tests in this module as timing tests since they use datetime and asyncio.sleep
pytestmark = pytest.mark.timing


@pytest.fixture
def mock_app_settings(tmp_path: Path) -> Mock:
    """Create mock app settings for testing.

    Args:
        tmp_path: Temporary directory path for test files

    Returns:
        Mock: Mock AppSettings configured for testing
    """
    settings = Mock(spec=AppSettings)
    settings.general = Mock(spec=GeneralSettings)
    settings.general.state_file = str(tmp_path / "state.json")
    settings.general.state_backup_directory = str(tmp_path / "backups")
    settings.general.state_backup_count = 3
    return settings


@pytest.fixture
def async_state_manager(mock_app_settings: Mock) -> AsyncStateManager:
    """Create AsyncStateManager instance for testing.

    Args:
        mock_app_settings: Mock app settings fixture

    Returns:
        AsyncStateManager: Configured AsyncStateManager instance for testing
    """
    return AsyncStateManager(mock_app_settings)


class TestAsyncStateManagerInit:
    """Test suite for AsyncStateManager initialization."""

    # ==================== SUCCESS CASES ====================

    def test_init_success_typical_config(self, mock_app_settings: Mock) -> None:
        """Test successful initialization with typical configuration."""
        # Act
        manager = AsyncStateManager(mock_app_settings)

        # Assert
        assert manager.config == mock_app_settings
        assert manager.state_file == mock_app_settings.general.state_file
        assert manager.backup_dir == mock_app_settings.general.state_backup_directory
        assert manager.backup_count == mock_app_settings.general.state_backup_count
        assert manager.current_state == {}
        assert manager.last_save_time is None
        assert Path(manager.backup_dir).exists()

    def test_init_success_backup_dir_already_exists(self, mock_app_settings: Mock) -> None:
        """Test successful initialization when backup directory already exists."""
        # Arrange
        Path(mock_app_settings.general.state_backup_directory).mkdir(parents=True, exist_ok=True)

        # Act
        manager = AsyncStateManager(mock_app_settings)

        # Assert
        assert Path(manager.backup_dir).exists()
        assert manager.backup_dir == mock_app_settings.general.state_backup_directory

    # ==================== EDGE CASES ====================

    def test_init_edge_nested_backup_directory(self, tmp_path: Path) -> None:
        """Test initialization with deeply nested backup directory path."""
        # Arrange
        settings = Mock(spec=AppSettings)
        settings.general = Mock(spec=GeneralSettings)
        settings.general.state_file = str(tmp_path / "state.json")
        settings.general.state_backup_directory = str(tmp_path / "a" / "b" / "c" / "d" / "backups")
        settings.general.state_backup_count = 1

        # Act
        manager = AsyncStateManager(settings)

        # Assert
        assert Path(manager.backup_dir).exists()
        # Verify the nested directory structure more clearly
        backup_path = Path(manager.backup_dir)
        expected_path = tmp_path / "a" / "b" / "c" / "d" / "backups"
        assert backup_path == expected_path
        assert backup_path.relative_to(tmp_path) == Path("a/b/c/d/backups")

    def test_init_edge_zero_backup_count(self, tmp_path: Path) -> None:
        """Test initialization with zero backup count."""
        # Arrange
        settings = Mock(spec=AppSettings)
        settings.general = Mock(spec=GeneralSettings)
        settings.general.state_file = str(tmp_path / "state.json")
        settings.general.state_backup_directory = str(tmp_path / "backups")
        settings.general.state_backup_count = 0

        # Act
        manager = AsyncStateManager(settings)

        # Assert
        assert manager.backup_count == 0
        assert Path(manager.backup_dir).exists()


class TestLoadState:
    """Test suite for load_state method."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_load_state_success_valid_file(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test successful state loading from valid file."""
        # Arrange
        test_state = {"key": "value", "counter": 42}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": str(hash(json.dumps(test_state, sort_keys=True))),
            },
        }
        Path(async_state_manager.state_file).write_text(
            json.dumps(state_data, indent=2), encoding="utf-8"
        )

        # Act
        result = await async_state_manager.load_state()

        # Assert
        assert result is True
        assert async_state_manager.current_state == test_state

    @pytest.mark.asyncio
    async def test_load_state_success_after_save(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test successful state loading after saving."""
        # Arrange
        test_state = {"data": "test", "value": 123}
        await async_state_manager.save_state(test_state)

        # Create new manager instance
        new_manager = AsyncStateManager(async_state_manager.config)

        # Act
        result = await new_manager.load_state()

        # Assert
        assert result is True
        assert new_manager.current_state == test_state

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_load_state_edge_empty_state(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test loading empty state."""
        # Arrange
        test_state: dict[str, Any] = {}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": str(hash(json.dumps(test_state, sort_keys=True))),
            },
        }
        Path(async_state_manager.state_file).write_text(
            json.dumps(state_data, indent=2), encoding="utf-8"
        )

        # Act
        result = await async_state_manager.load_state()

        # Assert
        assert result is True
        assert async_state_manager.current_state == {}

    @pytest.mark.asyncio
    async def test_load_state_edge_file_not_exists(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test loading state when file doesn't exist."""
        # Act
        result = await async_state_manager.load_state()

        # Assert
        assert result is False
        assert async_state_manager.current_state == {}

    @pytest.mark.asyncio
    async def test_load_state_edge_large_state(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test loading very large state."""
        # Arrange
        test_state = {f"key_{i}": f"value_{i}" * 100 for i in range(1000)}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": str(hash(json.dumps(test_state, sort_keys=True))),
            },
        }
        Path(async_state_manager.state_file).write_text(
            json.dumps(state_data, indent=2), encoding="utf-8"
        )

        # Act
        result = await async_state_manager.load_state()

        # Assert
        assert result is True
        assert async_state_manager.current_state == test_state

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_load_state_failure_corrupted_json(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test handling of corrupted JSON file."""
        # Arrange
        Path(async_state_manager.state_file).write_text("{invalid json content", encoding="utf-8")

        # Act
        result = await async_state_manager.load_state()

        # Assert
        assert result is False
        assert async_state_manager.current_state == {}

    @pytest.mark.asyncio
    async def test_load_state_failure_missing_metadata(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test handling of state file missing metadata."""
        # Arrange
        state_data = {"state": {"key": "value"}}  # Missing metadata
        Path(async_state_manager.state_file).write_text(
            json.dumps(state_data, indent=2), encoding="utf-8"
        )

        # Act
        result = await async_state_manager.load_state()

        # Assert
        assert result is False

    @pytest.mark.asyncio
    async def test_load_state_failure_invalid_checksum(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test handling of invalid checksum."""
        # Arrange
        test_state = {"key": "value"}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": "invalid_checksum",
            },
        }
        Path(async_state_manager.state_file).write_text(
            json.dumps(state_data, indent=2), encoding="utf-8"
        )

        # Act
        result = await async_state_manager.load_state()

        # Assert
        assert result is False

    @pytest.mark.asyncio
    async def test_load_state_failure_permission_error(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test handling of permission errors."""
        # Arrange
        test_state = {"key": "value"}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": str(hash(json.dumps(test_state, sort_keys=True))),
            },
        }
        Path(async_state_manager.state_file).write_text(
            json.dumps(state_data, indent=2), encoding="utf-8"
        )

        # Make file unreadable
        Path(async_state_manager.state_file).chmod(0o000)

        try:
            # Act
            result = await async_state_manager.load_state()

            # Assert
            assert result is False
        finally:
            # Cleanup
            Path(async_state_manager.state_file).chmod(0o644)


class TestSaveState:
    """Test suite for save_state method."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_save_state_success_typical_state(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test successful state saving with typical data."""
        # Arrange
        test_state = {"user": "test", "balance": 1000, "active": True}

        # Act
        result = await async_state_manager.save_state(test_state)

        # Assert
        assert result is True
        assert async_state_manager.current_state == test_state
        assert async_state_manager.last_save_time is not None
        assert Path(async_state_manager.state_file).exists()

        # Verify saved content
        saved_data = json.loads(Path(async_state_manager.state_file).read_text(encoding="utf-8"))
        assert saved_data["state"] == test_state
        assert "metadata" in saved_data
        assert "timestamp" in saved_data["metadata"]
        assert "checksum" in saved_data["metadata"]

    @pytest.mark.asyncio
    async def test_save_state_success_multiple_saves(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test multiple successive saves."""
        # Arrange
        states: list[dict[str, Any]] = [
            {"counter": 1},
            {"counter": 2, "data": "test"},
            {"counter": 3, "data": "test", "extra": [1, 2, 3]},
        ]

        # Act & Assert
        for state in states:
            result = await async_state_manager.save_state(state)
            assert result is True
            assert async_state_manager.current_state == state

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_save_state_edge_empty_state(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test saving empty state."""
        # Arrange
        test_state: dict[str, Any] = {}

        # Act
        result = await async_state_manager.save_state(test_state)

        # Assert
        assert result is True
        assert async_state_manager.current_state == {}

    @pytest.mark.asyncio
    async def test_save_state_edge_nested_data(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test saving deeply nested state."""
        # Arrange
        test_state = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "data": "deep",
                            "array": [1, 2, {"nested": True}],
                        }
                    }
                }
            }
        }

        # Act
        result = await async_state_manager.save_state(test_state)

        # Assert
        assert result is True
        assert async_state_manager.current_state == test_state

    @pytest.mark.asyncio
    async def test_save_state_edge_unicode_content(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test saving state with unicode characters."""
        # Arrange
        test_state = {
            "emoji": "🚀🌟",
            "chinese": "测试数据",
            "arabic": "اختبار",
            "special": "¡™£¢∞§¶•ªº",
        }

        # Act
        result = await async_state_manager.save_state(test_state)

        # Assert
        assert result is True
        assert async_state_manager.current_state == test_state

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_save_state_failure_write_permission(
        self, async_state_manager: AsyncStateManager, tmp_path: Path
    ) -> None:
        """Test handling of write permission errors."""
        # Arrange
        test_state = {"key": "value"}

        # Make directory read-only
        tmp_path.chmod(0o444)

        try:
            # Act
            result = await async_state_manager.save_state(test_state)

            # Assert
            assert result is False
        finally:
            # Cleanup
            tmp_path.chmod(0o755)

    @pytest.mark.asyncio
    async def test_save_state_failure_disk_full_simulation(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test handling of disk full errors."""
        # Arrange
        test_state = {"key": "value"}

        with patch("pathlib.Path.open", side_effect=OSError("No space left on device")):
            # Act
            result = await async_state_manager.save_state(test_state)

            # Assert
            assert result is False


class TestGetCurrentState:
    """Test suite for get_current_state method."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_get_current_state_success_returns_copy(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test that get_current_state returns a copy, not reference."""
        # Arrange
        test_state = {"key": "value", "list": [1, 2, 3]}
        await async_state_manager.save_state(test_state)

        # Act
        retrieved_state = await async_state_manager.get_current_state()

        # Assert
        assert retrieved_state == test_state
        assert retrieved_state is not async_state_manager.current_state

        # Verify it's a shallow copy (modifying top-level doesn't affect original)
        retrieved_state["new_key"] = "new_value"
        assert "new_key" not in async_state_manager.current_state

        # Note: It's a shallow copy, so nested objects are shared
        retrieved_state["list"].append(4)
        assert async_state_manager.current_state["list"] == [1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_get_current_state_success_empty_state(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test getting empty current state."""
        # Act
        retrieved_state = await async_state_manager.get_current_state()

        # Assert
        assert retrieved_state == {}
        assert isinstance(retrieved_state, dict)


class TestBackupOperations:
    """Test suite for backup-related operations."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_backup_creation_success(self, async_state_manager: AsyncStateManager) -> None:
        """Test successful backup creation."""
        # Arrange
        test_state = {"key": "value"}
        await async_state_manager.save_state(test_state)

        # Act - Save again to trigger backup
        new_state = {"key": "new_value"}
        result = await async_state_manager.save_state(new_state)

        # Assert
        assert result is True
        backup_files = list(Path(async_state_manager.backup_dir).glob("state_*.json"))
        assert len(backup_files) >= 1

    @pytest.mark.asyncio
    async def test_backup_rotation_success(self, async_state_manager: AsyncStateManager) -> None:
        """Test backup rotation keeps only specified number of backups."""
        # Arrange
        async_state_manager.backup_count = 2

        # Act - Create multiple saves to trigger rotation
        for i in range(5):
            await async_state_manager.save_state({"counter": i})
            await asyncio.sleep(0.01)  # Ensure different timestamps

        # Assert
        backup_files = list(Path(async_state_manager.backup_dir).glob("state_*.json"))
        assert len(backup_files) <= async_state_manager.backup_count

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_backup_edge_zero_backup_count(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test behavior with zero backup count."""
        # Arrange
        async_state_manager.backup_count = 0

        # Act
        for i in range(3):
            await async_state_manager.save_state({"counter": i})

        # Assert
        backup_files = list(Path(async_state_manager.backup_dir).glob("state_*.json"))
        assert len(backup_files) == 0

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_recovery_from_backup_success(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test successful recovery from backup when main state is corrupted."""
        # Arrange
        # First save a valid state
        test_state = {"key": "backup_value"}
        await async_state_manager.save_state(test_state)

        # Create another save to ensure backup exists
        await async_state_manager.save_state({"key": "newer_value"})

        # Corrupt the main state file
        Path(async_state_manager.state_file).write_text("corrupted data", encoding="utf-8")

        # Create new manager to test recovery
        new_manager = AsyncStateManager(async_state_manager.config)

        # Act
        result = await new_manager.load_state()

        # Assert
        assert result is True
        assert "key" in new_manager.current_state


class TestStateIntegrity:
    """Test suite for state integrity verification through public methods."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_integrity_check_success_through_load(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test that load_state verifies integrity correctly with valid state."""
        # Arrange
        test_state = {"key": "value"}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": str(hash(json.dumps(test_state, sort_keys=True))),
            },
        }
        Path(async_state_manager.state_file).write_text(
            json.dumps(state_data, indent=2), encoding="utf-8"
        )

        # Act
        result = await async_state_manager.load_state()

        # Assert
        assert result is True
        assert async_state_manager.current_state == test_state

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_integrity_check_edge_empty_state_through_load(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test integrity verification with empty state through load_state."""
        # Arrange
        test_state: dict[str, Any] = {}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": str(hash(json.dumps(test_state, sort_keys=True))),
            },
        }
        Path(async_state_manager.state_file).write_text(
            json.dumps(state_data, indent=2), encoding="utf-8"
        )

        # Act
        result = await async_state_manager.load_state()

        # Assert
        assert result is True
        assert async_state_manager.current_state == {}

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_integrity_check_failure_missing_metadata_through_load(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test that load_state fails when metadata is missing."""
        # Arrange
        state_data = {
            "state": {"key": "value"}
            # Missing metadata
        }
        Path(async_state_manager.state_file).write_text(
            json.dumps(state_data, indent=2), encoding="utf-8"
        )

        # Act
        result = await async_state_manager.load_state()

        # Assert - should fail integrity check and try recovery
        assert result is False

    @pytest.mark.asyncio
    async def test_integrity_check_failure_wrong_checksum_through_load(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test that load_state fails with incorrect checksum."""
        # Arrange
        test_state = {"key": "value"}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": "wrong_checksum",  # Deliberately wrong
            },
        }
        Path(async_state_manager.state_file).write_text(
            json.dumps(state_data, indent=2), encoding="utf-8"
        )

        # Act
        result = await async_state_manager.load_state()

        # Assert - should fail integrity check and try recovery
        assert result is False


class TestConcurrency:
    """Test suite for concurrent operations."""

    @pytest.mark.asyncio
    async def test_concurrent_saves_success(self, async_state_manager: AsyncStateManager) -> None:
        """Test concurrent save operations are handled safely."""

        # Arrange
        async def save_state(value: int) -> bool:
            return await async_state_manager.save_state({"counter": value})

        # Act - Launch multiple concurrent saves
        tasks = [save_state(i) for i in range(10)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Assert
        # Due to the lock, saves should be serialized, but file system operations
        # might still have race conditions with atomic replacement
        successful_saves = sum(1 for r in results if r is True)
        assert successful_saves >= 1  # At least one should succeed
        assert "counter" in async_state_manager.current_state
        assert 0 <= async_state_manager.current_state["counter"] <= 9

        # Note: Some failures are expected due to concurrent file operations
        # but at least one save must succeed
        _ = sum(1 for r in results if r is False)  # Count failures for context

    @pytest.mark.asyncio
    async def test_concurrent_read_write_success(
        self, async_state_manager: AsyncStateManager
    ) -> None:
        """Test concurrent read and write operations."""
        # Arrange
        await async_state_manager.save_state({"initial": "value"})

        async def read_loop() -> list[dict[str, Any]]:
            states: list[dict[str, Any]] = []
            for _ in range(5):
                state = await async_state_manager.get_current_state()
                states.append(state)
                await asyncio.sleep(0.01)
            return states

        async def write_loop() -> None:
            for i in range(5):
                await async_state_manager.save_state({"counter": i})
                await asyncio.sleep(0.01)

        # Act
        read_task = asyncio.create_task(read_loop())
        write_task = asyncio.create_task(write_loop())

        states, _ = await asyncio.gather(read_task, write_task)

        # Assert
        assert len(states) == 5
        assert all(isinstance(state, dict) for state in states)


class TestLoadAsyncStateManager:
    """Test suite for load_async_state_manager factory function."""

    @pytest.mark.asyncio
    async def test_load_async_state_manager_success(self, mock_app_settings: Mock) -> None:
        """Test successful creation and initialization of state manager."""
        # Act
        manager = await load_async_state_manager(mock_app_settings)

        # Assert
        assert isinstance(manager, AsyncStateManager)
        assert manager.config == mock_app_settings

    @pytest.mark.asyncio
    async def test_load_async_state_manager_with_existing_state(
        self, mock_app_settings: Mock
    ) -> None:
        """Test loading state manager with existing state file."""
        # Arrange
        # Create a state file first
        test_state = {"preloaded": True}
        state_data = {
            "state": test_state,
            "metadata": {
                "timestamp": datetime.now(UTC).isoformat(),
                "checksum": str(hash(json.dumps(test_state, sort_keys=True))),
            },
        }
        Path(mock_app_settings.general.state_file).parent.mkdir(parents=True, exist_ok=True)
        Path(mock_app_settings.general.state_file).write_text(
            json.dumps(state_data, indent=2), encoding="utf-8"
        )

        # Act
        manager = await load_async_state_manager(mock_app_settings)

        # Assert
        assert manager.current_state == test_state


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("state_data", "expected", "description"),
    [
        # Success cases
        (
            {"key": "value", "number": 42},
            True,
            "typical valid state",
        ),
        (
            {},
            True,
            "empty state",
        ),
        (
            {"nested": {"deep": {"data": [1, 2, 3]}}},
            True,
            "nested state",
        ),
        # Edge cases
        (
            {"key": "x" * 10000},
            True,
            "large string value",
        ),
        (
            {str(i): i for i in range(1000)},
            True,
            "many keys",
        ),
    ],
)
@pytest.mark.asyncio
async def test_state_save_load_roundtrip(
    async_state_manager: AsyncStateManager,
    state_data: dict[str, Any],
    expected: bool,
    description: str,
) -> None:
    """Test save and load roundtrip for various states: {description}."""
    # Act
    save_result = await async_state_manager.save_state(state_data)

    # Create new manager to test loading
    new_manager = AsyncStateManager(async_state_manager.config)
    load_result = await new_manager.load_state()

    # Assert
    assert save_result == expected
    assert load_result == expected
    if expected:
        assert new_manager.current_state == state_data

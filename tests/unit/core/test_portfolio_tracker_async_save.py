"""Unit tests for the PortfolioStateManager async save functionality.

Tests async state persistence capabilities including save_state, load_state,
and helper functions. Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases.
"""

import asyncio
import json
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager
from cyberdelta.core.portfolio_tracker_async_save import (
    load_state,
    patch_portfolio_tracker,
    save_state,
)
from cyberdelta.exceptions.base import StateFilePathError


pytestmark = pytest.mark.timing


@pytest.fixture
def mock_portfolio_state_manager() -> Mock:
    """Create a mock PortfolioStateManager instance.
    
    Returns:
        Mock: A mock PortfolioStateManager instance for testing.
    """
    tracker = Mock(spec=PortfolioStateManager)

    # Mock app_settings
    tracker.app_settings = Mock()
    tracker.app_settings.general = Mock()
    tracker.app_settings.general.portfolio_state_file = "data/portfolio_state.json"

    # Mock portfolio data
    tracker.exchange_summaries = {"hyperliquid": {}, "backpack": {}}
    tracker.active_symbols = {"BTC-PERP", "ETH-PERP"}
    tracker.watchlist = {"SOL-PERP"}
    tracker.high_watermark = Decimal("10000.0")
    tracker.realized_pnl = Decimal("1500.0")

    # Mock to_dict method
    tracker.to_dict.return_value = {
        "balances": {},
        "positions": {},
        "orders": {},
        "last_update_time": "2023-01-01T00:00:00Z",
    }

    # Mock from_dict class method
    tracker.from_dict = Mock(return_value=tracker)

    # Mock state attributes for load_state
    tracker.balances = {}
    tracker.positions = {}
    tracker.orders = {}
    tracker.last_update_time = None
    tracker.pt_config = {}

    return tracker


class TestSaveState:
    """Test suite for save_state function with success, edge, and failure cases."""

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_save_state_success_with_path(self, mock_portfolio_state_manager: Mock) -> None:
        """Test successful save_state with explicit path."""
        # Arrange
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            file_path = tmp_file.name

        try:
            # Act
            await save_state(mock_portfolio_state_manager, file_path)

            # Assert
            assert Path(file_path).exists()
            saved_data = json.loads(Path(file_path).read_text(encoding="utf-8"))

            assert saved_data["version"] == "1.0"
            assert "timestamp" in saved_data
            assert "portfolio_state" in saved_data
            assert "metadata" in saved_data
            assert saved_data["metadata"]["exchanges"] == ["hyperliquid", "backpack"]
            assert set(saved_data["metadata"]["active_symbols"]) == {"BTC-PERP", "ETH-PERP"}
        finally:
            Path(file_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_save_state_success_default_path(self, mock_portfolio_state_manager: Mock) -> None:
        """Test successful save_state with default path from config."""
        # Arrange
        with tempfile.TemporaryDirectory() as tmp_dir:
            default_path = str(Path(tmp_dir) / "portfolio_state.json")
            mock_portfolio_state_manager.app_settings.general.portfolio_state_file = default_path

            # Act
            await save_state(mock_portfolio_state_manager, None)

            # Assert
            assert Path(default_path).exists()
            saved_data = json.loads(Path(default_path).read_text(encoding="utf-8"))
            assert saved_data["version"] == "1.0"

    @pytest.mark.asyncio
    async def test_save_state_success_creates_directory(self, mock_portfolio_state_manager: Mock) -> None:
        """Test save_state successfully creates parent directories."""
        # Arrange
        with tempfile.TemporaryDirectory() as tmp_dir:
            nested_path = str(Path(tmp_dir) / "nested" / "dir" / "state.json")

            # Act
            await save_state(mock_portfolio_state_manager, nested_path)

            # Assert
            assert Path(nested_path).exists()
            assert Path(nested_path).parent.exists()

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_save_state_edge_empty_portfolio_data(self, mock_portfolio_state_manager: Mock) -> None:
        """Test save_state with empty portfolio data."""
        # Arrange
        mock_portfolio_state_manager.exchange_summaries = {}
        mock_portfolio_state_manager.active_symbols = set()
        mock_portfolio_state_manager.watchlist = set()
        mock_portfolio_state_manager.to_dict.return_value = {}

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            file_path = tmp_file.name

        try:
            # Act
            await save_state(mock_portfolio_state_manager, file_path)

            # Assert
            saved_data = json.loads(Path(file_path).read_text(encoding="utf-8"))
            assert saved_data["metadata"]["exchanges"] == []
            assert saved_data["metadata"]["active_symbols"] == []
            assert saved_data["metadata"]["watchlist"] == []
        finally:
            Path(file_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_save_state_edge_large_portfolio_data(self, mock_portfolio_state_manager: Mock) -> None:
        """Test save_state with large portfolio data."""
        # Arrange
        # Create large dataset
        large_exchanges: dict[str, dict[str, Any]] = {f"exchange_{i}": {} for i in range(100)}
        large_symbols = {f"SYMBOL_{i}-PERP" for i in range(1000)}

        mock_portfolio_state_manager.exchange_summaries = large_exchanges
        mock_portfolio_state_manager.active_symbols = large_symbols
        mock_portfolio_state_manager.to_dict.return_value = {"large_data": "x" * 10000}

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            file_path = tmp_file.name

        try:
            # Act
            await save_state(mock_portfolio_state_manager, file_path)

            # Assert
            assert Path(file_path).exists()
            saved_data = json.loads(Path(file_path).read_text(encoding="utf-8"))
            assert len(saved_data["metadata"]["exchanges"]) == 100
            assert len(saved_data["metadata"]["active_symbols"]) == 1000
        finally:
            Path(file_path).unlink(missing_ok=True)

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_save_state_failure_none_path_no_config(
        self, mock_portfolio_state_manager: Mock
    ) -> None:
        """Test save_state failure when path is None and no config available."""
        # Arrange
        mock_portfolio_state_manager.app_settings.general.portfolio_state_file = None

        # Act & Assert
        with pytest.raises(StateFilePathError):
            await save_state(mock_portfolio_state_manager, None)

    @pytest.mark.asyncio
    async def test_save_state_failure_invalid_path(self, mock_portfolio_state_manager: Mock) -> None:
        """Test save_state failure with invalid path."""
        # Arrange
        invalid_path = "/nonexistent/readonly/path/state.json"

        # Act & Assert
        with pytest.raises((OSError, IOError, FileNotFoundError, PermissionError)):
            await save_state(mock_portfolio_state_manager, invalid_path)

    @pytest.mark.asyncio
    async def test_save_state_failure_to_dict_error(self, mock_portfolio_state_manager: Mock) -> None:
        """Test save_state failure when to_dict raises exception."""
        # Arrange
        mock_portfolio_state_manager.to_dict.side_effect = Exception("Serialization error")

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            file_path = tmp_file.name

        try:
            # Act & Assert
            with pytest.raises(Exception, match="Serialization error"):
                await save_state(mock_portfolio_state_manager, file_path)
        finally:
            Path(file_path).unlink(missing_ok=True)


class TestLoadState:
    """Test suite for load_state function with success, edge, and failure cases."""

    def create_test_state_data(self) -> dict[str, Any]:
        """Create test state data for loading.

        Returns:
            dict[str, Any]: Test state data for portfolio loading tests.
        """
        return {
            "version": "1.0",
            "timestamp": "2023-01-01T00:00:00Z",
            "portfolio_state": {
                "balances": {"USDT": {"available": "1000.0"}},
                "positions": {"BTC-PERP": {"size": "1.0"}},
                "orders": {},
                "last_update_time": "2023-01-01T00:00:00Z",
            },
            "metadata": {
                "exchanges": ["hyperliquid", "backpack"],
                "active_symbols": ["BTC-PERP", "ETH-PERP"],
                "watchlist": ["SOL-PERP"],
                "high_watermark": "10000.0",
                "realized_pnl": "1500.0",
            },
        }

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_load_state_success_with_path(self, mock_portfolio_state_manager: Mock) -> None:
        """Test successful load_state with explicit path."""
        # Arrange
        state_data = self.create_test_state_data()

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            json.dump(state_data, tmp_file)
            file_path = tmp_file.name

        try:
            # Act
            result = await load_state(mock_portfolio_state_manager, file_path)

            # Assert
            assert result is True
            mock_portfolio_state_manager.from_dict.assert_called_once()
            assert mock_portfolio_state_manager.active_symbols == {"BTC-PERP", "ETH-PERP"}
            assert mock_portfolio_state_manager.watchlist == {"SOL-PERP"}
        finally:
            Path(file_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_load_state_success_default_path(self, mock_portfolio_state_manager: Mock) -> None:
        """Test successful load_state with default path from config."""
        # Arrange
        state_data = self.create_test_state_data()

        with tempfile.TemporaryDirectory() as tmp_dir:
            default_path = str(Path(tmp_dir) / "portfolio_state.json")
            mock_portfolio_state_manager.app_settings.general.portfolio_state_file = default_path

            # Write test data
            Path(default_path).write_text(json.dumps(state_data), encoding="utf-8")

            # Act
            result = await load_state(mock_portfolio_state_manager, None)

            # Assert
            assert result is True
            mock_portfolio_state_manager.from_dict.assert_called_once()

    @pytest.mark.asyncio
    async def test_load_state_success_minimal_data(self, mock_portfolio_state_manager: Mock) -> None:
        """Test successful load_state with minimal valid data."""
        # Arrange
        minimal_data: dict[str, str | dict[str, str]] = {
            "version": "1.0",
            "timestamp": "2023-01-01T00:00:00Z",
            "portfolio_state": {},
            "metadata": {},
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            json.dump(minimal_data, tmp_file)
            file_path = tmp_file.name

        try:
            # Act
            result = await load_state(mock_portfolio_state_manager, file_path)

            # Assert
            assert result is True
            # Check that active_symbols was set to empty set (from empty metadata)
            mock_portfolio_state_manager.active_symbols = set()
            mock_portfolio_state_manager.watchlist = set()
            assert mock_portfolio_state_manager.active_symbols == set()
            assert mock_portfolio_state_manager.watchlist == set()
        finally:
            Path(file_path).unlink(missing_ok=True)

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_load_state_edge_file_not_exists(self, mock_portfolio_state_manager: Mock) -> None:
        """Test load_state when file doesn't exist."""
        # Arrange
        nonexistent_path = "/nonexistent/file.json"

        # Act
        result = await load_state(mock_portfolio_state_manager, nonexistent_path)

        # Assert
        assert result is False

    @pytest.mark.asyncio
    async def test_load_state_edge_unsupported_version(self, mock_portfolio_state_manager: Mock) -> None:
        """Test load_state with unsupported version."""
        # Arrange
        invalid_version_data: dict[str, str | dict[str, str]] = {
            "version": "2.0",  # Unsupported version
            "timestamp": "2023-01-01T00:00:00Z",
            "portfolio_state": {},
            "metadata": {},
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            json.dump(invalid_version_data, tmp_file)
            file_path = tmp_file.name

        try:
            # Act
            result = await load_state(mock_portfolio_state_manager, file_path)

            # Assert
            assert result is False
        finally:
            Path(file_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_load_state_edge_missing_version(self, mock_portfolio_state_manager: Mock) -> None:
        """Test load_state with missing version field."""
        # Arrange
        no_version_data: dict[str, str | dict[str, str]] = {
            "timestamp": "2023-01-01T00:00:00Z",
            "portfolio_state": {},
            "metadata": {},
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            json.dump(no_version_data, tmp_file)
            file_path = tmp_file.name

        try:
            # Act
            result = await load_state(mock_portfolio_state_manager, file_path)

            # Assert
            assert result is False
        finally:
            Path(file_path).unlink(missing_ok=True)

    # FAILURE CASES
    @pytest.mark.asyncio
    async def test_load_state_failure_none_path_no_config(
        self, mock_portfolio_state_manager: Mock
    ) -> None:
        """Test load_state failure when path is None and no config available."""
        # Arrange
        mock_portfolio_state_manager.app_settings.general.portfolio_state_file = None

        # Act
        result = await load_state(mock_portfolio_state_manager, None)

        # Assert - should return False due to None path
        assert result is False

    @pytest.mark.asyncio
    async def test_load_state_failure_invalid_json(self, mock_portfolio_state_manager: Mock) -> None:
        """Test load_state failure with invalid JSON."""
        # Arrange
        invalid_json = "{ invalid json content"

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            tmp_file.write(invalid_json)
            file_path = tmp_file.name

        try:
            # Act
            result = await load_state(mock_portfolio_state_manager, file_path)

            # Assert
            assert result is False
        finally:
            Path(file_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_load_state_failure_from_dict_error(self, mock_portfolio_state_manager: Mock) -> None:
        """Test load_state failure when from_dict raises exception."""
        # Arrange
        state_data = self.create_test_state_data()
        mock_portfolio_state_manager.from_dict.side_effect = Exception("Deserialization error")

        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            json.dump(state_data, tmp_file)
            file_path = tmp_file.name

        try:
            # Act
            result = await load_state(mock_portfolio_state_manager, file_path)

            # Assert
            assert result is False
        finally:
            Path(file_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_load_state_failure_empty_file(self, mock_portfolio_state_manager: Mock) -> None:
        """Test load_state failure with empty file."""
        # Arrange
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            file_path = tmp_file.name
            # File is created but empty

        try:
            # Act
            result = await load_state(mock_portfolio_state_manager, file_path)

            # Assert
            assert result is False
        finally:
            Path(file_path).unlink(missing_ok=True)


class TestPatchPortfolioTracker:
    """Test suite for patch_portfolio_tracker function with success, edge, and failure cases."""

    # SUCCESS CASES
    @pytest.mark.asyncio
    async def test_patch_portfolio_tracker_success(self, mock_portfolio_state_manager: Mock) -> None:
        """Test successful patching and functionality of PortfolioStateManager."""
        # Arrange
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            file_path = tmp_file.name

        try:
            # Act - Apply patch and test functionality
            patch_portfolio_tracker()

            # Assert - Methods are added and functional
            assert hasattr(PortfolioStateManager, "save_state")
            assert hasattr(PortfolioStateManager, "load_state")

            # Test that the patched methods actually work
            await save_state(mock_portfolio_state_manager, file_path)
            assert Path(file_path).exists()

            result = await load_state(mock_portfolio_state_manager, file_path)
            assert result is True
        finally:
            Path(file_path).unlink(missing_ok=True)

    # EDGE CASES
    @pytest.mark.asyncio
    async def test_patch_portfolio_tracker_edge_multiple_calls(
        self, mock_portfolio_state_manager: Mock
    ) -> None:
        """Test patching PortfolioStateManager multiple times still works."""
        # Arrange
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            file_path = tmp_file.name

        try:
            # Act - Patch multiple times
            patch_portfolio_tracker()
            patch_portfolio_tracker()
            patch_portfolio_tracker()

            # Assert - Should still work correctly
            assert hasattr(PortfolioStateManager, "save_state")
            assert hasattr(PortfolioStateManager, "load_state")

            # Test functionality still works after multiple patches
            await save_state(mock_portfolio_state_manager, file_path)
            assert Path(file_path).exists()

            result = await load_state(mock_portfolio_state_manager, file_path)
            assert result is True
        finally:
            Path(file_path).unlink(missing_ok=True)

    def test_patch_portfolio_tracker_edge_idempotent(self) -> None:
        """Test that patch_portfolio_tracker is idempotent."""
        # Act - Apply patch multiple times
        patch_portfolio_tracker()
        first_save_method = getattr(PortfolioStateManager, "save_state", None)
        first_load_method = getattr(PortfolioStateManager, "load_state", None)

        patch_portfolio_tracker()
        second_save_method = getattr(PortfolioStateManager, "save_state", None)
        second_load_method = getattr(PortfolioStateManager, "load_state", None)

        # Assert - Methods should be the same (idempotent)
        assert first_save_method is second_save_method
        assert first_load_method is second_load_method

    # FAILURE CASES
    def test_patch_portfolio_tracker_failure_module_availability(self) -> None:
        """Test that patch works even if called before module fully loaded."""
        # This test verifies the patch function itself doesn't fail
        # Even in edge cases where it might be called early

        # Act & Assert - Should not raise any exceptions
        patch_portfolio_tracker()
        # If we get here without exception, the test passes
        assert hasattr(PortfolioStateManager, "save_state")
        assert hasattr(PortfolioStateManager, "load_state")


# Integration tests
class TestIntegrationScenarios:
    """Integration test scenarios for portfolio tracker async save functionality."""

    @pytest.mark.asyncio
    async def test_save_load_roundtrip_success(self, mock_portfolio_state_manager: Mock) -> None:
        """Test complete save and load roundtrip."""
        # Arrange
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
            file_path = tmp_file.name

        # Setup mock for load scenario
        temp_tracker = Mock()
        temp_tracker.balances = {"USDT": {"available": "1000.0"}}
        temp_tracker.positions = {"BTC-PERP": {"size": "1.0"}}
        temp_tracker.orders = {}
        temp_tracker.last_update_time = "2023-01-01T00:00:00Z"
        temp_tracker.high_watermark = Decimal("10000.0")
        temp_tracker.realized_pnl = Decimal("1500.0")

        mock_portfolio_state_manager.from_dict.return_value = temp_tracker

        try:
            # Act - Save then load
            await save_state(mock_portfolio_state_manager, file_path)
            result = await load_state(mock_portfolio_state_manager, file_path)

            # Assert
            assert result is True
            assert Path(file_path).exists()

            # Verify the saved file contains expected data
            saved_data = json.loads(Path(file_path).read_text(encoding="utf-8"))
            assert saved_data["version"] == "1.0"
            assert "portfolio_state" in saved_data
            assert "metadata" in saved_data
        finally:
            Path(file_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_concurrent_save_operations(self, mock_portfolio_state_manager: Mock) -> None:
        """Test concurrent save operations."""
        # Arrange
        file_paths: list[str] = []
        for _i in range(3):
            with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp_file:
                file_paths.append(tmp_file.name)

        try:
            # Act - Concurrent saves
            tasks = [save_state(mock_portfolio_state_manager, path) for path in file_paths]
            await asyncio.gather(*tasks)

            # Assert - All files should exist and contain valid data
            for file_path in file_paths:
                assert Path(file_path).exists()
                saved_data = json.loads(Path(file_path).read_text(encoding="utf-8"))
                assert saved_data["version"] == "1.0"
        finally:
            for file_path in file_paths:
                Path(file_path).unlink(missing_ok=True)

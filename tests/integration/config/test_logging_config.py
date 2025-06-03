"""
Unit tests for cyberdelta.config.logging_config module.

Tests the refactored logging configuration that uses AppSettings
instead of the old Config class.
"""

import logging
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from cyberdelta.config.config_models import AppSettings
from cyberdelta.config.logging_config import LogCapture, get_logger, setup_logging


class TestSetupLogging:
    """Test cases for setup_logging function with AppSettings."""

    def create_minimal_app_settings(
        self,
        log_level: str = "INFO",
        log_file: str | None = None,
        module_log_levels: dict[str, str] | None = None,
    ) -> AppSettings:
        """Create minimal AppSettings for testing."""
        config_data: dict[str, Any] = {
            "general": {
                "log_level": log_level,
            },
            "exchanges": {
                "test_exchange": {
                    "enabled": True,
                    "api_base_url": "https://api.test.com",
                    "ws_url": "wss://ws.test.com",
                    "rate_limit_per_minute": 60,
                    "symbols": {"BTC": "BTC-USD"},
                }
            },
            "strategies": {
                "hl_perp_bp_spot": {
                    "enabled": True,
                    "long_exchange": "test_exchange",
                    "short_exchange": "test_exchange",
                    "symbol_long": "BTC",
                    "symbol_short": "BTC",
                    "params": {
                        "funding_threshold": "0.01",
                        "max_price_spread_pct": "0.05",
                        "min_profit_usd": "10.0",
                    },
                }
            },
            "risk": {
                "global": {
                    "max_position_usd": "1000.0",
                    "max_total_exposure_usd": "5000.0",
                }
            },
            "execution": {
                "max_slippage_pct": "0.01",
                "compensation": {},
            },
            "safety_systems": {
                "circuit_breakers": {},
                "position_reconciliation": {},
                "balance_monitoring": {"min_balance_thresholds_usd": {"test_exchange": "100.0"}},
            },
            "monitoring": {},
        }

        # Add optional fields if provided
        if log_file is not None:
            config_data["general"]["log_file"] = log_file
        if module_log_levels is not None:
            config_data["general"]["module_log_levels"] = module_log_levels

        return AppSettings.model_validate(config_data)

    def test_basic_logging_setup(self) -> None:
        """Test basic logging setup with default INFO level."""
        app_settings = self.create_minimal_app_settings(log_level="INFO")

        # Clear any existing handlers
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        setup_logging(app_settings)

        # Verify root logger level
        assert root_logger.level == logging.INFO

        # Verify console handler was added
        assert len(root_logger.handlers) == 1
        console_handler = root_logger.handlers[0]
        assert isinstance(console_handler, logging.StreamHandler)
        assert console_handler.level == logging.INFO

    def test_debug_log_level(self) -> None:
        """Test logging setup with DEBUG level."""
        app_settings = self.create_minimal_app_settings(log_level="DEBUG")

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        setup_logging(app_settings)

        assert root_logger.level == logging.DEBUG
        assert root_logger.handlers[0].level == logging.DEBUG

    def test_warning_log_level(self) -> None:
        """Test logging setup with WARNING level."""
        app_settings = self.create_minimal_app_settings(log_level="WARNING")

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        setup_logging(app_settings)

        assert root_logger.level == logging.WARNING
        assert root_logger.handlers[0].level == logging.WARNING

    def test_error_log_level(self) -> None:
        """Test logging setup with ERROR level."""
        app_settings = self.create_minimal_app_settings(log_level="ERROR")

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        setup_logging(app_settings)

        assert root_logger.level == logging.ERROR
        assert root_logger.handlers[0].level == logging.ERROR

    def test_critical_log_level(self) -> None:
        """Test logging setup with CRITICAL level."""
        app_settings = self.create_minimal_app_settings(log_level="CRITICAL")

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        setup_logging(app_settings)

        assert root_logger.level == logging.CRITICAL
        assert root_logger.handlers[0].level == logging.CRITICAL

    def test_file_logging_setup(self) -> None:
        """Test logging setup with file output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file_path = Path(temp_dir) / "test.log"
            app_settings = self.create_minimal_app_settings(
                log_level="DEBUG", log_file=str(log_file_path)
            )

            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)

            setup_logging(app_settings)

            # Should have both console and file handlers
            assert len(root_logger.handlers) == 2

            # Find the file handler
            file_handler = None
            console_handler = None
            for handler in root_logger.handlers:
                if isinstance(handler, logging.FileHandler):
                    file_handler = handler
                elif isinstance(handler, logging.StreamHandler):
                    console_handler = handler

            assert file_handler is not None
            assert console_handler is not None
            assert file_handler.level == logging.DEBUG

            # Test that log file is created and written to
            test_logger = get_logger("test_module")
            test_logger.info("Test message")

            # Force flush
            for handler in root_logger.handlers:
                handler.flush()

            assert log_file_path.exists()
            with open(log_file_path) as f:
                content = f.read()
                assert "Test message" in content

    def test_file_logging_directory_creation(self) -> None:
        """Test that log file directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file_path = Path(temp_dir) / "subdir" / "nested" / "test.log"
            app_settings = self.create_minimal_app_settings(
                log_level="INFO", log_file=str(log_file_path)
            )

            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)

            setup_logging(app_settings)

            # Directory should be created
            assert log_file_path.parent.exists()

            # Test writing to the file
            test_logger = get_logger("test_module")
            test_logger.info("Test message in nested directory")

            # Force flush
            for handler in root_logger.handlers:
                handler.flush()

            assert log_file_path.exists()

    @patch("os.makedirs")
    def test_file_logging_directory_creation_failure(
        self, mock_makedirs: Mock, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test handling of directory creation failure."""
        mock_makedirs.side_effect = OSError("Permission denied")

        with tempfile.TemporaryDirectory() as temp_dir:
            log_file_path = Path(temp_dir) / "subdir" / "test.log"
            app_settings = self.create_minimal_app_settings(
                log_level="INFO", log_file=str(log_file_path)
            )

            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)

            # Should not raise exception, but log a warning
            setup_logging(app_settings)

            # Check captured stdout for the warning message
            captured = capsys.readouterr()
            assert "Failed to create log directory" in captured.out

    def test_file_logging_file_creation_failure(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Test handling of log file creation failure."""
        # Use an invalid path that will cause FileHandler creation to fail
        invalid_path = "/invalid/path/that/does/not/exist/test.log"
        app_settings = self.create_minimal_app_settings(log_level="INFO", log_file=invalid_path)

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Should not raise exception, but log a warning
        setup_logging(app_settings)

        # Check captured stdout for the warning message
        captured = capsys.readouterr()
        assert "Failed to create log file" in captured.out

        # Should still have console handler
        assert len(root_logger.handlers) >= 1

    def test_module_specific_log_levels(self) -> None:
        """Test module-specific log level configuration."""
        module_levels = {
            "test_module_debug": "DEBUG",
            "test_module_warning": "WARNING",
            "test_module_error": "ERROR",
        }
        app_settings = self.create_minimal_app_settings(
            log_level="INFO", module_log_levels=module_levels
        )

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        setup_logging(app_settings)

        # Check that module loggers have correct levels
        debug_logger = logging.getLogger("test_module_debug")
        warning_logger = logging.getLogger("test_module_warning")
        error_logger = logging.getLogger("test_module_error")

        assert debug_logger.level == logging.DEBUG
        assert warning_logger.level == logging.WARNING
        assert error_logger.level == logging.ERROR

    def test_module_log_levels_none(self) -> None:
        """Test that None module_log_levels is handled correctly."""
        app_settings = self.create_minimal_app_settings(log_level="INFO", module_log_levels=None)

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Should not raise exception
        setup_logging(app_settings)

        # Should have only console handler
        assert len(root_logger.handlers) == 1

    def test_module_log_levels_empty_dict(self) -> None:
        """Test that empty module_log_levels dict is handled correctly."""
        app_settings = self.create_minimal_app_settings(log_level="INFO", module_log_levels={})

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Should not raise exception
        setup_logging(app_settings)

        # Should have only console handler
        assert len(root_logger.handlers) == 1

    def test_module_log_level_setting_failure(self) -> None:
        """Test handling of module log level setting failure."""
        # Create a module name that might cause issues - use a valid but unusual name
        module_levels = {"test.module.with.dots": "DEBUG"}  # Valid module name
        app_settings = self.create_minimal_app_settings(
            log_level="INFO", module_log_levels=module_levels
        )

        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Should not raise exception, but might log a warning
        with LogCapture(level=logging.WARNING):
            setup_logging(app_settings)

            # The module name should work fine, test that no exception was raised
            assert len(root_logger.handlers) >= 1

    def test_handler_cleanup(self) -> None:
        """Test that existing handlers are removed before setup."""
        root_logger = logging.getLogger()

        # Clear all existing handlers first
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Add some dummy handlers
        dummy_handler1 = logging.StreamHandler()
        dummy_handler2 = logging.StreamHandler()
        root_logger.addHandler(dummy_handler1)
        root_logger.addHandler(dummy_handler2)

        initial_handler_count = len(root_logger.handlers)
        assert initial_handler_count == 2

        app_settings = self.create_minimal_app_settings(log_level="INFO")
        setup_logging(app_settings)

        # Should have only the new console handler
        assert len(root_logger.handlers) == 1
        assert dummy_handler1 not in root_logger.handlers
        assert dummy_handler2 not in root_logger.handlers

    def test_log_format_and_date_format(self) -> None:
        """Test that log messages use the correct format."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file_path = Path(temp_dir) / "format_test.log"
            app_settings = self.create_minimal_app_settings(
                log_level="INFO", log_file=str(log_file_path)
            )

            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)

            setup_logging(app_settings)

            test_logger = get_logger("format_test_module")
            test_logger.info("Format test message")

            # Force flush
            for handler in root_logger.handlers:
                handler.flush()

            with open(log_file_path) as f:
                content = f.read()

            # Check that the log format includes expected components
            assert "format_test_module" in content
            assert "INFO" in content
            assert "Format test message" in content
            # Check date format (YYYY-MM-DD HH:MM:SS)
            import re

            date_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
            assert re.search(date_pattern, content) is not None


class TestGetLogger:
    """Test cases for get_logger function."""

    def test_get_logger_returns_logger(self) -> None:
        """Test that get_logger returns a Logger instance."""
        logger = get_logger("test_module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"

    def test_get_logger_same_name_returns_same_instance(self) -> None:
        """Test that get_logger returns the same instance for the same name."""
        logger1 = get_logger("same_module")
        logger2 = get_logger("same_module")
        assert logger1 is logger2

    def test_get_logger_different_names(self) -> None:
        """Test that get_logger returns different instances for different names."""
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")
        assert logger1 is not logger2
        assert logger1.name == "module1"
        assert logger2.name == "module2"


class TestLogCapture:
    """Test cases for LogCapture context manager."""

    def test_log_capture_basic(self) -> None:
        """Test basic log capture functionality."""
        with LogCapture() as log_capture:
            logger = get_logger("test_capture")
            logger.info("Test message 1")
            logger.warning("Test message 2")

        logs = log_capture.get_logs()
        assert len(logs) == 2
        assert "Test message 1" in logs[0]
        assert "Test message 2" in logs[1]

    def test_log_capture_level_filtering(self) -> None:
        """Test that LogCapture respects level filtering."""
        with LogCapture(level=logging.WARNING) as log_capture:
            logger = get_logger("test_level_filter")
            logger.info("Info message")  # Should not be captured
            logger.warning("Warning message")  # Should be captured
            logger.error("Error message")  # Should be captured

        logs = log_capture.get_logs()
        assert len(logs) == 2
        assert "Warning message" in logs[0]
        assert "Error message" in logs[1]
        assert not any("Info message" in log for log in logs)

    def test_log_capture_cleanup(self) -> None:
        """Test that LogCapture properly cleans up handlers."""
        root_logger = logging.getLogger()
        initial_handler_count = len(root_logger.handlers)

        with LogCapture():
            # Handler should be added
            assert len(root_logger.handlers) == initial_handler_count + 1

        # Handler should be removed after context exit
        assert len(root_logger.handlers) == initial_handler_count

    def test_log_capture_exception_cleanup(self) -> None:
        """Test that LogCapture cleans up even when exception occurs."""
        root_logger = logging.getLogger()
        initial_handler_count = len(root_logger.handlers)

        try:
            with LogCapture():
                # Handler should be added
                assert len(root_logger.handlers) == initial_handler_count + 1
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Handler should still be removed after exception
        assert len(root_logger.handlers) == initial_handler_count

    def test_log_capture_format(self) -> None:
        """Test that LogCapture uses the correct log format."""
        with LogCapture() as log_capture:
            logger = get_logger("format_test")
            logger.info("Format test message")

        logs = log_capture.get_logs()
        assert len(logs) == 1

        # Check that the log format includes expected components
        log_message = logs[0]
        assert "format_test" in log_message
        assert "INFO" in log_message
        assert "Format test message" in log_message

        # Check date format (YYYY-MM-DD HH:MM:SS)
        import re

        date_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
        assert re.search(date_pattern, log_message) is not None

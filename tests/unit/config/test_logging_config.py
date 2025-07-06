"""Unit tests for the logging configuration module.

Tests logging setup and configuration functionality.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

import contextlib
import logging
import sys
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from cyberdelta.config.logging_config import (
    DEFAULT_DATE_FORMAT,
    DEFAULT_LOG_FORMAT,
    CapturingMemoryHandler,
    LogCapture,
    get_logger,
    setup_logging,
)
from cyberdelta.config.models.config_models import AppSettings, GeneralSettings


@pytest.fixture
def basic_app_settings() -> Mock:
    """Create basic mock app settings for testing."""
    settings = Mock(spec=AppSettings)
    general = Mock(spec=GeneralSettings)
    general.log_level = "INFO"
    general.log_file = None
    general.module_log_levels = None
    settings.general = general
    return settings


@pytest.fixture
def file_logging_app_settings() -> Mock:
    """Create mock app settings with file logging configured."""
    settings = Mock(spec=AppSettings)
    general = Mock(spec=GeneralSettings)
    general.log_level = "DEBUG"
    general.log_file = "test.log"  # Use relative path instead of /tmp
    general.module_log_levels = {"test_module": "ERROR"}
    settings.general = general
    return settings


@pytest.fixture
def temp_log_file() -> str:
    """Create a temporary log file path."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        return tmp.name


class TestSetupLogging:
    """Test suite for setup_logging function."""

    # ==================== SUCCESS CASES ====================

    def test_setup_logging_success_basic_console(self, basic_app_settings: Mock) -> None:
        """Test successful logging setup with basic console configuration."""
        # Arrange
        original_handlers = logging.getLogger().handlers[:]

        try:
            # Act
            setup_logging(basic_app_settings)

            # Assert
            root_logger = logging.getLogger()
            assert root_logger.level == logging.INFO
            assert len(root_logger.handlers) >= 1

            # Check console handler exists
            console_handlers: list[logging.StreamHandler[Any]] = [
                h for h in root_logger.handlers if isinstance(h, logging.StreamHandler)
            ]
            assert len(console_handlers) >= 1

            console_handler: logging.StreamHandler[Any] = console_handlers[0]
            assert console_handler.level == logging.INFO
            assert console_handler.stream == sys.stdout

        finally:
            # Cleanup - restore original handlers
            root_logger = logging.getLogger()
            root_logger.handlers.clear()
            root_logger.handlers.extend(original_handlers)

    def test_setup_logging_success_with_file_handler(self, temp_log_file: str) -> None:
        """Test successful logging setup with file handler."""
        # Arrange
        settings = Mock(spec=AppSettings)
        general = Mock(spec=GeneralSettings)
        general.log_level = "WARNING"
        general.log_file = temp_log_file
        general.module_log_levels = None
        settings.general = general

        original_handlers = logging.getLogger().handlers[:]

        try:
            # Act
            setup_logging(settings)

            # Assert
            root_logger = logging.getLogger()
            assert root_logger.level == logging.WARNING

            # Check file handler exists
            file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
            assert len(file_handlers) >= 1

            file_handler = file_handlers[0]
            assert file_handler.level == logging.WARNING
            assert Path(temp_log_file).exists()

        finally:
            # Cleanup
            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                if isinstance(handler, logging.FileHandler):
                    handler.close()
            root_logger.handlers.clear()
            root_logger.handlers.extend(original_handlers)
            # Clean up temp file
            with contextlib.suppress(FileNotFoundError):
                Path(temp_log_file).unlink()

    def test_setup_logging_success_with_module_levels(self, basic_app_settings: Mock) -> None:
        """Test successful logging setup with module-specific log levels."""
        # Arrange
        basic_app_settings.general.module_log_levels = {
            "test_module1": "DEBUG",
            "test_module2": "ERROR",
        }
        original_handlers = logging.getLogger().handlers[:]

        try:
            # Act
            setup_logging(basic_app_settings)

            # Assert
            test_logger1 = logging.getLogger("test_module1")
            test_logger2 = logging.getLogger("test_module2")
            assert test_logger1.level == logging.DEBUG
            assert test_logger2.level == logging.ERROR

        finally:
            # Cleanup
            root_logger = logging.getLogger()
            root_logger.handlers.clear()
            root_logger.handlers.extend(original_handlers)

    def test_setup_logging_success_all_log_levels(self) -> None:
        """Test successful logging setup with all supported log levels."""
        # Arrange
        log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        original_handlers = logging.getLogger().handlers[:]

        for level_str in log_levels:
            settings = Mock(spec=AppSettings)
            general = Mock(spec=GeneralSettings)
            general.log_level = level_str
            general.log_file = None
            general.module_log_levels = None
            settings.general = general

            try:
                # Act
                setup_logging(settings)

                # Assert
                root_logger = logging.getLogger()
                expected_level = getattr(logging, level_str)
                assert root_logger.level == expected_level

            finally:
                # Cleanup
                root_logger = logging.getLogger()
                root_logger.handlers.clear()
                root_logger.handlers.extend(original_handlers)

    # ==================== EDGE CASES ====================

    def test_setup_logging_edge_creates_log_directory(self) -> None:
        """Test logging setup creates log directory when it doesn't exist."""
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file_path = Path(temp_dir) / "nested" / "dir" / "test.log"

            settings = Mock(spec=AppSettings)
            general = Mock(spec=GeneralSettings)
            general.log_level = "INFO"
            general.log_file = str(log_file_path)
            general.module_log_levels = None
            settings.general = general

            original_handlers = logging.getLogger().handlers[:]

            try:
                # Act
                setup_logging(settings)

                # Assert
                assert log_file_path.parent.exists()
                assert log_file_path.exists()

            finally:
                # Cleanup
                root_logger = logging.getLogger()
                for handler in root_logger.handlers[:]:
                    if isinstance(handler, logging.FileHandler):
                        handler.close()
                root_logger.handlers.clear()
                root_logger.handlers.extend(original_handlers)

    def test_setup_logging_edge_empty_module_levels(self, basic_app_settings: Mock) -> None:
        """Test logging setup with empty module levels dictionary."""
        # Arrange
        basic_app_settings.general.module_log_levels = {}
        original_handlers = logging.getLogger().handlers[:]

        try:
            # Act
            setup_logging(basic_app_settings)

            # Assert - Should complete without error
            root_logger = logging.getLogger()
            assert root_logger.level == logging.INFO

        finally:
            # Cleanup
            root_logger = logging.getLogger()
            root_logger.handlers.clear()
            root_logger.handlers.extend(original_handlers)

    def test_setup_logging_edge_removes_existing_handlers(self, basic_app_settings: Mock) -> None:
        """Test logging setup removes existing handlers."""
        # Arrange
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]

        # Add a dummy handler
        dummy_handler = logging.StreamHandler()
        root_logger.addHandler(dummy_handler)
        initial_handler_count = len(root_logger.handlers)
        _ = initial_handler_count  # Used for context

        try:
            # Act
            setup_logging(basic_app_settings)

            # Assert - Dummy handler should be removed
            assert dummy_handler not in root_logger.handlers
            # Should have new handlers from setup_logging
            assert len(root_logger.handlers) >= 1

        finally:
            # Cleanup
            root_logger.handlers.clear()
            root_logger.handlers.extend(original_handlers)

    # ==================== FAILURE CASES ====================

    def test_setup_logging_failure_directory_creation_permission_error(self) -> None:
        """Test logging setup handles directory creation permission errors."""
        # Arrange
        settings = Mock(spec=AppSettings)
        general = Mock(spec=GeneralSettings)
        general.log_level = "INFO"
        general.log_file = "/root/forbidden/test.log"  # Likely to cause permission error
        general.module_log_levels = None
        settings.general = general

        original_handlers = logging.getLogger().handlers[:]

        try:
            # Act - Should not raise exception despite permission error
            setup_logging(settings)

            # Assert - Should still configure console logging
            root_logger = logging.getLogger()
            assert root_logger.level == logging.INFO
            console_handlers: list[logging.StreamHandler[Any]] = [
                h for h in root_logger.handlers if isinstance(h, logging.StreamHandler)
            ]
            assert len(console_handlers) >= 1

        finally:
            # Cleanup
            root_logger = logging.getLogger()
            root_logger.handlers.clear()
            root_logger.handlers.extend(original_handlers)

    def test_setup_logging_failure_file_creation_permission_error(self) -> None:
        """Test logging setup handles file creation permission errors."""
        # Arrange
        settings = Mock(spec=AppSettings)
        general = Mock(spec=GeneralSettings)
        general.log_level = "INFO"
        general.log_file = "/proc/test.log"  # Should cause permission error
        general.module_log_levels = None
        settings.general = general

        original_handlers = logging.getLogger().handlers[:]

        try:
            # Act - Should not raise exception despite file creation error
            setup_logging(settings)

            # Assert - Should still configure console logging
            root_logger = logging.getLogger()
            assert root_logger.level == logging.INFO
            console_handlers: list[logging.StreamHandler[Any]] = [
                h for h in root_logger.handlers if isinstance(h, logging.StreamHandler)
            ]
            assert len(console_handlers) >= 1

        finally:
            # Cleanup
            root_logger = logging.getLogger()
            root_logger.handlers.clear()
            root_logger.handlers.extend(original_handlers)

    def test_setup_logging_failure_module_level_error(self, basic_app_settings: Mock) -> None:
        """Test logging setup handles module level setting errors gracefully."""
        # Arrange
        basic_app_settings.general.module_log_levels = {
            "valid_module": "INFO",
            "problem_module": "DEBUG",
        }
        original_handlers = logging.getLogger().handlers[:]

        # Mock getLogger to raise an exception for problem_module
        original_getLogger = logging.getLogger

        def mock_getLogger(name: str | None = None) -> logging.Logger:
            if name == "problem_module":
                raise OSError("Module logger creation failed")
            return original_getLogger(name)

        try:
            with patch("logging.getLogger", side_effect=mock_getLogger):
                # Act - Should not raise exception despite module error
                setup_logging(basic_app_settings)

            # Assert - Should still configure basic logging
            root_logger = logging.getLogger()
            assert root_logger.level == logging.INFO

        finally:
            # Cleanup
            root_logger = logging.getLogger()
            root_logger.handlers.clear()
            root_logger.handlers.extend(original_handlers)


class TestGetLogger:
    """Test suite for get_logger function."""

    # ==================== SUCCESS CASES ====================

    def test_get_logger_success_module_name(self) -> None:
        """Test successful logger retrieval with module name."""
        # Act
        logger = get_logger("test_module")

        # Assert
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"

    def test_get_logger_success_nested_module(self) -> None:
        """Test successful logger retrieval with nested module name."""
        # Act
        logger = get_logger("cyberdelta.core.test")

        # Assert
        assert isinstance(logger, logging.Logger)
        assert logger.name == "cyberdelta.core.test"

    def test_get_logger_success_multiple_calls_same_name(self) -> None:
        """Test multiple calls with same name return same logger instance."""
        # Act
        logger1 = get_logger("same_module")
        logger2 = get_logger("same_module")

        # Assert
        assert logger1 is logger2
        assert logger1.name == "same_module"

    # ==================== EDGE CASES ====================

    def test_get_logger_edge_empty_string(self) -> None:
        """Test logger retrieval with empty string."""
        # Act
        logger = get_logger("")

        # Assert
        assert isinstance(logger, logging.Logger)
        # Empty string returns root logger which has name "root"
        assert logger.name == "root"

    def test_get_logger_edge_special_characters(self) -> None:
        """Test logger retrieval with special characters in name."""
        # Act
        logger = get_logger("test.module-with_special@chars")

        # Assert
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test.module-with_special@chars"

    def test_get_logger_edge_very_long_name(self) -> None:
        """Test logger retrieval with very long name."""
        # Arrange
        long_name = "a" * 1000

        # Act
        logger = get_logger(long_name)

        # Assert
        assert isinstance(logger, logging.Logger)
        assert logger.name == long_name


class TestCapturingMemoryHandler:
    """Test suite for CapturingMemoryHandler class."""

    # ==================== SUCCESS CASES ====================

    def test_capturing_memory_handler_success_initialization(self) -> None:
        """Test successful initialization of CapturingMemoryHandler."""
        # Arrange
        logs_list: list[str] = []

        # Act
        handler = CapturingMemoryHandler(capacity=100, logs_list=logs_list)

        # Assert
        assert handler.capacity == 100
        assert handler.logs_list is logs_list
        assert isinstance(handler.formatter, logging.Formatter)

    def test_capturing_memory_handler_success_emit_record(self) -> None:
        """Test successful emission and capture of log record."""
        # Arrange
        logs_list: list[str] = []
        handler = CapturingMemoryHandler(capacity=100, logs_list=logs_list)

        # Create a log record
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        # Act
        handler.emit(record)

        # Assert
        assert len(logs_list) == 1
        assert "Test message" in logs_list[0]
        assert "test_logger" in logs_list[0]
        assert "INFO" in logs_list[0]

    def test_capturing_memory_handler_success_multiple_records(self) -> None:
        """Test capturing multiple log records."""
        # Arrange
        logs_list: list[str] = []
        handler = CapturingMemoryHandler(capacity=100, logs_list=logs_list)

        # Act
        for i in range(5):
            record = logging.LogRecord(
                name=f"logger_{i}",
                level=logging.INFO,
                pathname="test.py",
                lineno=i,
                msg=f"Message {i}",
                args=(),
                exc_info=None,
            )
            handler.emit(record)

        # Assert
        assert len(logs_list) == 5
        for i, log_msg in enumerate(logs_list):
            assert f"Message {i}" in log_msg
            assert f"logger_{i}" in log_msg

    # ==================== EDGE CASES ====================

    def test_capturing_memory_handler_edge_zero_capacity(self) -> None:
        """Test handler with zero capacity."""
        # Arrange
        logs_list: list[str] = []

        # Act
        handler = CapturingMemoryHandler(capacity=0, logs_list=logs_list)

        # Assert
        assert handler.capacity == 0
        assert handler.logs_list is logs_list

    def test_capturing_memory_handler_edge_capacity_exceeded(self) -> None:
        """Test behavior when capacity is exceeded."""
        # Arrange
        logs_list: list[str] = []
        handler = CapturingMemoryHandler(capacity=2, logs_list=logs_list)

        # Act - Emit more records than capacity
        for i in range(5):
            record = logging.LogRecord(
                name="test_logger",
                level=logging.INFO,
                pathname="test.py",
                lineno=i,
                msg=f"Message {i}",
                args=(),
                exc_info=None,
            )
            handler.emit(record)

        # Assert - All messages should still be captured in logs_list
        assert len(logs_list) == 5
        for i, log_msg in enumerate(logs_list):
            assert f"Message {i}" in log_msg


class TestLogCapture:
    """Test suite for LogCapture context manager."""

    # ==================== SUCCESS CASES ====================

    def test_log_capture_success_basic_usage(self) -> None:
        """Test successful basic usage of LogCapture context manager."""
        # Arrange
        test_logger = logging.getLogger("test_capture")

        # Act
        with LogCapture(level=logging.INFO) as capture:
            test_logger.info("Test message")

        # Assert
        logs = capture.get_logs()
        assert len(logs) == 1
        assert "Test message" in logs[0]
        assert "test_capture" in logs[0]

    def test_log_capture_success_multiple_messages(self) -> None:
        """Test capturing multiple log messages."""
        # Arrange
        test_logger = logging.getLogger("test_multiple")
        test_logger.setLevel(logging.DEBUG)  # Ensure logger level allows debug messages

        # Act
        with LogCapture(level=logging.DEBUG) as capture:
            test_logger.debug("Debug message")
            test_logger.info("Info message")
            test_logger.warning("Warning message")

        # Assert
        logs = capture.get_logs()
        assert len(logs) == 3
        assert "Debug message" in logs[0]
        assert "Info message" in logs[1]
        assert "Warning message" in logs[2]

    def test_log_capture_success_level_filtering(self) -> None:
        """Test log level filtering works correctly."""
        # Arrange
        test_logger = logging.getLogger("test_filtering")
        test_logger.setLevel(logging.DEBUG)

        # Act
        with LogCapture(level=logging.WARNING) as capture:
            test_logger.debug("Debug message")  # Should be filtered out
            test_logger.info("Info message")  # Should be filtered out
            test_logger.warning("Warning message")  # Should be captured
            test_logger.error("Error message")  # Should be captured

        # Assert
        logs = capture.get_logs()
        assert len(logs) == 2
        assert "Warning message" in logs[0]
        assert "Error message" in logs[1]

    def test_log_capture_success_cleanup_after_context(self) -> None:
        """Test handler is properly removed after context exits."""
        # Arrange
        root_logger = logging.getLogger()
        initial_handler_count = len(root_logger.handlers)

        # Act
        with LogCapture() as capture:
            _ = capture  # Used for context
            # Handler should be added
            assert len(root_logger.handlers) == initial_handler_count + 1

        # Assert - Handler should be removed
        assert len(root_logger.handlers) == initial_handler_count

    # ==================== EDGE CASES ====================

    def test_log_capture_edge_no_messages(self) -> None:
        """Test LogCapture with no messages logged."""
        # Act
        with LogCapture() as capture:
            pass  # No logging

        # Assert
        logs = capture.get_logs()
        assert len(logs) == 0

    def test_log_capture_edge_exception_in_context(self) -> None:
        """Test LogCapture properly cleans up even when exception occurs."""
        # Arrange
        root_logger = logging.getLogger()
        initial_handler_count = len(root_logger.handlers)
        test_logger = logging.getLogger("test_exception")

        # Act & Assert
        capture: LogCapture | None = None
        try:
            with LogCapture() as capture:
                test_logger.info("Before exception")
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected

        # Assert - Handler should still be cleaned up
        assert len(root_logger.handlers) == initial_handler_count

        # And logs should be captured before exception
        assert capture is not None
        logs = capture.get_logs()
        assert len(logs) == 1
        assert "Before exception" in logs[0]

    def test_log_capture_edge_very_high_capacity(self) -> None:
        """Test LogCapture with very high capacity."""
        # Arrange
        test_logger = logging.getLogger("test_high_capacity")

        # Act
        with LogCapture() as capture:
            # Generate many log messages
            for i in range(1500):  # More than default capacity of 1000
                test_logger.info("Message %d", i)

        # Assert - All messages should be captured
        logs = capture.get_logs()
        assert len(logs) == 1500
        assert "Message 0" in logs[0]
        assert "Message 1499" in logs[-1]

    # ==================== FAILURE CASES ====================

    def test_log_capture_failure_handler_already_removed(self) -> None:
        """Test LogCapture handles case where handler is already removed."""
        # Arrange
        root_logger = logging.getLogger()

        # Act
        capture = LogCapture()
        # Test that context manager handles missing handler gracefully
        with capture:
            # Manually remove the handler (simulate external removal)
            if capture.handler and capture.handler in root_logger.handlers:
                root_logger.removeHandler(capture.handler)
        # Context manager should exit cleanly even if handler was removed

        # Assert - No exception should be raised


class TestConstants:
    """Test suite for module constants."""

    # ==================== SUCCESS CASES ====================

    def test_constants_defined(self) -> None:
        """Test that module constants are properly defined."""
        # Assert
        assert isinstance(DEFAULT_LOG_FORMAT, str)
        assert isinstance(DEFAULT_DATE_FORMAT, str)
        assert len(DEFAULT_LOG_FORMAT) > 0
        assert len(DEFAULT_DATE_FORMAT) > 0

    def test_default_log_format_contains_required_fields(self) -> None:
        """Test that default log format contains required fields."""
        # Assert
        assert "%(asctime)s" in DEFAULT_LOG_FORMAT
        assert "%(name)s" in DEFAULT_LOG_FORMAT
        assert "%(levelname)s" in DEFAULT_LOG_FORMAT
        assert "%(message)s" in DEFAULT_LOG_FORMAT

    def test_default_date_format_valid(self) -> None:
        """Test that default date format is valid."""
        # Assert
        assert DEFAULT_DATE_FORMAT == "%Y-%m-%d %H:%M:%S"


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("log_level", "expected_level"),
    [
        ("DEBUG", logging.DEBUG),
        ("INFO", logging.INFO),
        ("WARNING", logging.WARNING),
        ("ERROR", logging.ERROR),
        ("CRITICAL", logging.CRITICAL),
    ],
)
def test_setup_logging_all_levels_parametrized(log_level: str, expected_level: int) -> None:
    """Test setup_logging with all supported log levels."""
    # Arrange
    settings = Mock(spec=AppSettings)
    general = Mock(spec=GeneralSettings)
    general.log_level = log_level
    general.log_file = None
    general.module_log_levels = None
    settings.general = general

    original_handlers = logging.getLogger().handlers[:]

    try:
        # Act
        setup_logging(settings)

        # Assert
        root_logger = logging.getLogger()
        assert root_logger.level == expected_level

    finally:
        # Cleanup
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.handlers.extend(original_handlers)


@pytest.mark.parametrize(
    ("capture_level", "log_level", "should_capture"),
    [
        (logging.DEBUG, logging.DEBUG, True),
        (logging.DEBUG, logging.INFO, True),
        (logging.INFO, logging.DEBUG, False),
        (logging.INFO, logging.INFO, True),
        (logging.WARNING, logging.INFO, False),
        (logging.WARNING, logging.WARNING, True),
        (logging.ERROR, logging.WARNING, False),
        (logging.ERROR, logging.ERROR, True),
    ],
)
def test_log_capture_level_filtering_parametrized(
    capture_level: int, log_level: int, should_capture: bool
) -> None:
    """Test LogCapture level filtering with various combinations."""
    # Arrange
    test_logger = logging.getLogger("test_parametrized")
    test_logger.setLevel(logging.DEBUG)

    # Act
    with LogCapture(level=capture_level) as capture:
        test_logger.log(log_level, "Test message")

    # Assert
    logs = capture.get_logs()
    if should_capture:
        assert len(logs) == 1
        assert "Test message" in logs[0]
    else:
        assert len(logs) == 0


@pytest.mark.parametrize(
    ("module_name", "module_level"),
    [
        ("test.module1", "DEBUG"),
        ("cyberdelta.core", "INFO"),
        ("app.handlers", "WARNING"),
        ("service.api", "ERROR"),
        ("critical.component", "CRITICAL"),
    ],
)
def test_setup_logging_module_levels_parametrized(module_name: str, module_level: str) -> None:
    """Test setup_logging with various module-specific log levels."""
    # Arrange
    settings = Mock(spec=AppSettings)
    general = Mock(spec=GeneralSettings)
    general.log_level = "INFO"
    general.log_file = None
    general.module_log_levels = {module_name: module_level}
    settings.general = general

    original_handlers = logging.getLogger().handlers[:]

    try:
        # Act
        setup_logging(settings)

        # Assert
        module_logger = logging.getLogger(module_name)
        expected_level = getattr(logging, module_level)
        assert module_logger.level == expected_level

    finally:
        # Cleanup
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.handlers.extend(original_handlers)

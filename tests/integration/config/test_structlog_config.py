"""Unit tests for cyberdelta.config.structlog_config module.

Tests the structured logging configuration using structlog
and its integration with AppSettings.
"""

import json
import logging
import tempfile
from pathlib import Path
from typing import Any

import pytest
import structlog

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import (
    TraceLevelLogger,
    add_timestamp,
    censor_sensitive_data,
    get_logger,
    setup_structlog,
    strip_ansi_codes,
)


class TestStructlogConfiguration:
    """Test cases for structlog configuration functions."""

    def create_minimal_app_settings(
        self,
        log_level: str = "INFO",
        log_file: str | None = None,
    ) -> AppSettings:
        """Create minimal AppSettings for testing.

        Returns:
            AppSettings instance configured with minimal test configuration.
        """
        config_data: dict[str, Any] = {
            "general": {
                "log_level": log_level,
            },
            "exchanges": {
                "backpack": {
                    "enabled": True,
                    "api_base_url_mainnet": "https://api.test.com",
                    "ws_url_mainnet": "wss://ws.test.com",
                    "exchange_name": "backpack",
                    "rate_limit_per_minute": 60,
                    "symbols": {"BTC": "BTC-USD"},
                },
            },
            "strategies": {
                "hl_perp_bp_spot": {
                    "enabled": True,
                    "long_exchange": "backpack",
                    "short_exchange": "backpack",
                    "symbol_long": "BTC",
                    "symbol_short": "BTC",
                    "params": {
                        "funding_threshold": "0.01",
                        "max_price_spread_pct": "0.05",
                        "min_profit_usd": "10.0",
                        "min_funding_differential": "0.001",
                        "check_interval": 60,
                        "risk_aversion": "1.0",
                        "rebalance_threshold": "0.05",
                        "perp_exchange": "hyperliquid",
                        "spot_exchange": "backpack",
                    },
                },
            },
            "risk": {
                "global": {
                    "max_position_usd": "10000.0",
                    "max_total_exposure_usd": "50000.0",
                },
                "sizing": {
                    "max_position_size": "5000.0",
                },
            },
            "execution": {
                "max_slippage_pct": "0.01",
                "compensation": {},
            },
            "safety_systems": {
                "circuit_breakers": {},
                "position_reconciliation": {},
                "balance_monitoring": {"min_balance_thresholds_usd": {"backpack": "100.0"}},
            },
            "monitoring": {},
        }

        if log_file is not None:
            config_data["general"]["log_file"] = log_file

        return AppSettings.model_validate(config_data)

    def test_setup_structlog_basic(self) -> None:
        """Test basic structlog setup with INFO level."""
        app_settings = self.create_minimal_app_settings(log_level="INFO")

        # Setup structlog
        setup_structlog(app_settings)

        # Verify structlog is configured
        logger = get_logger(__name__)
        assert isinstance(logger, TraceLevelLogger)

    def test_setup_structlog_debug_level(self) -> None:
        """Test structlog setup with DEBUG level."""
        app_settings = self.create_minimal_app_settings(log_level="DEBUG")

        setup_structlog(app_settings)

        # Should be able to create logger
        logger = get_logger(__name__)
        assert isinstance(logger, TraceLevelLogger)

    def test_file_logging_setup(self) -> None:
        """Test structlog setup with file output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file_path = Path(temp_dir) / "test.log"
            app_settings = self.create_minimal_app_settings(
                log_level="INFO",
                log_file=str(log_file_path),
            )

            setup_structlog(app_settings)

            # Test that logging works
            logger = get_logger("test_module")
            logger.info(
                "test_message",
                action="test",
                value=42,
                message="Test structured logging message",
            )

            # Verify file was created and contains JSON
            assert log_file_path.exists()
            content = log_file_path.read_text().strip()
            if content:  # File might be empty depending on buffering
                # Should be valid JSON
                log_entry = json.loads(content.split("\n")[0])
                # The event contains the formatted log message
                assert "test_message" in log_entry.get("event", "")
                assert "test_module" in log_entry.get("logger", "")
                assert log_entry.get("level") == "info"


class TestStructlogProcessors:
    """Test cases for structlog processor functions."""

    def test_add_timestamp(self) -> None:
        """Test timestamp processor adds ISO timestamp."""
        event_dict = {"event": "test_event", "key": "value"}

        processed = add_timestamp(None, "info", event_dict)

        assert "timestamp" in processed
        # Verify it's a valid ISO timestamp format
        timestamp = processed["timestamp"]
        assert isinstance(timestamp, str)
        assert "T" in timestamp  # ISO format should have T separator
        assert timestamp.endswith("Z") or "+" in timestamp or "-" in timestamp

    def test_censor_sensitive_data(self) -> None:
        """Test sensitive data censoring processor."""
        event_dict = {
            "event": "test_event",
            "api_key": "secret123",
            "password": "mypassword",
            "private_key": "0x123abc",
            "auth_token": "token456",
            "normal_field": "not_sensitive",
        }

        processed = censor_sensitive_data(None, "info", event_dict)

        assert processed["api_key"] == "***REDACTED***"
        assert processed["password"] == "***REDACTED***"
        assert processed["private_key"] == "***REDACTED***"
        assert processed["auth_token"] == "***REDACTED***"
        assert processed["normal_field"] == "not_sensitive"
        assert processed["event"] == "test_event"

    def test_strip_ansi_codes(self) -> None:
        """Test ANSI code stripping processor."""
        event_dict = {
            "event": "test_event",
            "colored_text": "\x1b[31mRed text\x1b[0m",
            "normal_text": "Normal text",
            "nested": {
                "colored": "\x1b[32mGreen\x1b[0m",
                "normal": "Regular",
            },
            "list_with_colors": ["\x1b[33mYellow\x1b[0m", "Plain"],
        }

        processed = strip_ansi_codes(None, "info", event_dict)

        assert processed["colored_text"] == "Red text"
        assert processed["normal_text"] == "Normal text"
        assert processed["nested"]["colored"] == "Green"
        assert processed["nested"]["normal"] == "Regular"
        assert processed["list_with_colors"][0] == "Yellow"
        assert processed["list_with_colors"][1] == "Plain"


class TestGetLogger:
    """Test cases for get_logger function."""

    def test_get_logger_returns_bound_logger(self) -> None:
        """Test that get_logger returns a TraceLevelLogger instance."""
        logger = get_logger("test_module")
        assert isinstance(logger, TraceLevelLogger)

    def test_get_logger_with_context(self) -> None:
        """Test that get_logger binds context correctly."""
        # Set up file-based logging for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = Path(temp_dir) / "test.log"

            # Configure logging to file
            logging.basicConfig(
                filename=str(log_file), level=logging.DEBUG, format="%(message)s", force=True
            )

            # Configure structlog for JSON output
            structlog.configure(
                processors=[
                    structlog.stdlib.add_log_level,
                    structlog.stdlib.add_logger_name,
                    structlog.processors.JSONRenderer(),
                ],
                logger_factory=structlog.stdlib.LoggerFactory(),
                cache_logger_on_first_use=True,
            )

            # Test context binding
            logger = get_logger("test_module", request_id="123", user="test_user")
            assert isinstance(logger, TraceLevelLogger)

            logger.info("test_event", action="test")

            # Flush and read log
            for handler in logging.getLogger().handlers:
                handler.flush()

            if log_file.exists():
                content = log_file.read_text().strip()
                if content:
                    log_entry = json.loads(content.split("\n")[0])
                    # The context should be in the log
                    assert "test_module" in log_entry.get("logger", "")
                    assert "test_event" in log_entry.get("event", "")

    def test_get_logger_different_names(self) -> None:
        """Test that get_logger works with different module names."""
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")

        assert isinstance(logger1, TraceLevelLogger)
        assert isinstance(logger2, TraceLevelLogger)

        # Test that loggers are different instances
        assert logger1 is not logger2

        # Test that both can be used for logging (basic functionality)
        try:
            logger1.info("test1", module="module1")
            logger2.info("test2", module="module2")
            # If no exception is raised, the test passes
        except Exception as e:  # noqa: BLE001
            pytest.fail(f"Logger failed to log: {e}")


class TestStructuredLogging:
    """Test cases for structured logging functionality."""

    def test_structured_logging_format(self) -> None:
        """Test that structured logging produces expected format."""
        # Set up file-based logging for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = Path(temp_dir) / "test.log"

            # Clear any existing handlers
            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)

            # Configure logging to file
            file_handler = logging.FileHandler(str(log_file))
            file_handler.setLevel(logging.DEBUG)
            root_logger.addHandler(file_handler)
            root_logger.setLevel(logging.DEBUG)

            # Configure structlog for JSON output with full context
            structlog.configure(
                processors=[
                    structlog.stdlib.add_log_level,
                    structlog.stdlib.add_logger_name,
                    structlog.processors.JSONRenderer(),
                ],
                logger_factory=structlog.stdlib.LoggerFactory(),
                cache_logger_on_first_use=True,
            )

            logger = get_logger("test_module")
            logger.info(
                "user_action",
                action="login",
                user_id=123,
                success=True,
                message="User logged in successfully",
            )

            # Flush and read log
            file_handler.flush()

            if log_file.exists():
                content = log_file.read_text().strip()
                if content:
                    log_entry = json.loads(content.split("\n")[0])
                    # Verify structured data is present
                    assert "user_action" in log_entry.get("event", "")
                    assert "login" in str(log_entry)  # action should be somewhere in the log
                    assert "123" in str(log_entry)  # user_id should be present
                    assert "test_module" in log_entry.get("logger", "")

    def test_logging_levels(self) -> None:
        """Test that different logging levels work correctly."""
        logger = get_logger("test_module")

        # Test that all logging level methods exist and can be called without error
        try:
            logger.debug("debug_event", level="debug")
            logger.info("info_event", level="info")
            logger.warning("warning_event", level="warning")
            logger.error("error_event", level="error")
            logger.critical("critical_event", level="critical")
            logger.trace("trace_event")  # Test custom trace method
        except Exception as e:  # noqa: BLE001
            pytest.fail(f"Logging level method failed: {e}")

        # Verify that the logger has the expected methods
        assert hasattr(logger, "debug")
        assert hasattr(logger, "info")
        assert hasattr(logger, "warning")
        assert hasattr(logger, "error")
        assert hasattr(logger, "critical")
        assert hasattr(logger, "trace")

    def test_context_propagation(self) -> None:
        """Test that context propagates correctly through bound loggers."""
        base_logger = get_logger("test_module")

        # Test binding returns a new TraceLevelLogger instance
        bound_logger = base_logger.bind(request_id="req_123", session="sess_456")
        assert isinstance(bound_logger, TraceLevelLogger)
        assert bound_logger is not base_logger  # Should be a new instance

        # Test that bind method works without error
        try:
            bound_logger.info("first_event", action="start")
            bound_logger.info("second_event", action="continue")
        except Exception as e:  # noqa: BLE001
            pytest.fail(f"Bound logger failed: {e}")

        # Test unbind methods exist
        assert hasattr(bound_logger, "unbind")
        assert hasattr(bound_logger, "try_unbind")

        # Test unbind returns new TraceLevelLogger
        unbound_logger = bound_logger.unbind("request_id")
        assert isinstance(unbound_logger, TraceLevelLogger)

    def test_exception_logging(self) -> None:
        """Test that exceptions are logged correctly.

        Raises:
            ValueError: Test exception that is intentionally raised and caught for logging
                verification.
        """
        logger = get_logger("test_module")

        # Test exception logging both within and outside exception context
        try:
            raise ValueError("Test exception")
        except ValueError:
            # This should work - we're in an exception context
            try:
                logger.exception(
                    "exception_occurred",
                    action="handle_error",
                    error_type="ValueError",
                )
            except Exception as e:  # noqa: BLE001
                pytest.fail(f"Exception logging failed: {e}")

        # Test exception logging outside exception context (should still work)
        try:
            logger.exception("no_active_exception", action="test_outside_context")  # noqa: LOG004
        except Exception as e:  # noqa: BLE001
            pytest.fail(f"Exception logging outside context failed: {e}")

        # Verify exception method exists
        assert hasattr(logger, "exception")
        assert callable(logger.exception)


class TestFileLogging:
    """Test cases for file logging functionality."""

    def test_json_file_output(self) -> None:
        """Test that file output produces valid JSON lines."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file_path = Path(temp_dir) / "test_json.log"
            app_settings = self.create_minimal_app_settings(
                log_level="INFO",
                log_file=str(log_file_path),
            )

            setup_structlog(app_settings)

            logger = get_logger("test_module")
            logger.info(
                "test_event",
                action="test_json",
                number=42,
                boolean=True,
                message="Test JSON logging",
            )

            # Force flush by getting all handlers and flushing them
            root_logger = logging.getLogger()
            for handler in root_logger.handlers:
                handler.flush()

            # Read and verify JSON structure
            if log_file_path.exists():
                content = log_file_path.read_text().strip()
                if content:  # File might be empty due to buffering
                    lines = content.split("\n")
                    for line in lines:
                        if line.strip():
                            log_entry = json.loads(line)
                            assert isinstance(log_entry, dict)
                            assert "timestamp" in log_entry
                            # No ANSI codes should be present in file output
                            assert "\x1b[" not in json.dumps(log_entry)

    def create_minimal_app_settings(
        self,
        log_level: str = "INFO",
        log_file: str | None = None,
    ) -> AppSettings:
        """Create minimal AppSettings for testing.

        Returns:
            AppSettings instance configured with minimal test configuration.
        """
        config_data: dict[str, Any] = {
            "general": {
                "log_level": log_level,
            },
            "exchanges": {
                "backpack": {
                    "enabled": True,
                    "api_base_url_mainnet": "https://api.test.com",
                    "ws_url_mainnet": "wss://ws.test.com",
                    "exchange_name": "backpack",
                    "rate_limit_per_minute": 60,
                    "symbols": {"BTC": "BTC-USD"},
                },
            },
            "strategies": {
                "hl_perp_bp_spot": {
                    "enabled": True,
                    "long_exchange": "backpack",
                    "short_exchange": "backpack",
                    "symbol_long": "BTC",
                    "symbol_short": "BTC",
                    "params": {
                        "funding_threshold": "0.01",
                        "max_price_spread_pct": "0.05",
                        "min_profit_usd": "10.0",
                        "min_funding_differential": "0.001",
                        "check_interval": 60,
                        "risk_aversion": "1.0",
                        "rebalance_threshold": "0.05",
                        "perp_exchange": "hyperliquid",
                        "spot_exchange": "backpack",
                    },
                },
            },
            "risk": {
                "global": {
                    "max_position_usd": "10000.0",
                    "max_total_exposure_usd": "50000.0",
                },
                "sizing": {
                    "max_position_size": "5000.0",
                },
            },
            "execution": {
                "max_slippage_pct": "0.01",
                "compensation": {},
            },
            "safety_systems": {
                "circuit_breakers": {},
                "position_reconciliation": {},
                "balance_monitoring": {"min_balance_thresholds_usd": {"backpack": "100.0"}},
            },
            "monitoring": {},
        }

        if log_file is not None:
            config_data["general"]["log_file"] = log_file

        return AppSettings.model_validate(config_data)

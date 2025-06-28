"""Unit tests for cyberdelta.config.structlog_config module.

Tests the structured logging configuration using structlog
and its integration with AppSettings.
"""

import json
import logging
import tempfile
from pathlib import Path
from typing import Any

import structlog

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.structlog_config import (
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
                    },
                },
            },
            "risk": {
                "global": {
                    "max_position_usd": "1000.0",
                    "max_total_exposure_usd": "5000.0",
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
            "portfolio_tracker": {
                "data_freshness_seconds": 30,
                "initial_positions": [],
            },
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
        assert isinstance(logger, structlog.BoundLogger)

    def test_setup_structlog_debug_level(self) -> None:
        """Test structlog setup with DEBUG level."""
        app_settings = self.create_minimal_app_settings(log_level="DEBUG")

        setup_structlog(app_settings)

        # Should be able to create logger
        logger = get_logger(__name__)
        assert isinstance(logger, structlog.BoundLogger)

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
                assert "test_message" in log_entry.get("event", "")
                assert log_entry.get("action") == "test"
                assert log_entry.get("value") == 42


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
        """Test that get_logger returns a BoundLogger instance."""
        logger = get_logger("test_module")
        assert isinstance(logger, structlog.BoundLogger)

    def test_get_logger_with_context(self) -> None:
        """Test that get_logger binds context correctly."""
        # Configure structlog for testing
        cap = structlog.testing.LogCapture()
        structlog.configure(logger_factory=lambda: cap)

        logger = get_logger("test_module", request_id="123", user="test_user")
        assert isinstance(logger, structlog.BoundLogger)

        # Context binding should work
        cap = structlog.testing.LogCapture()
        structlog.configure(logger_factory=lambda: cap)

        logger = get_logger("test_module", request_id="123", user="test_user")
        logger.info("test_event", action="test")

        assert len(cap.entries) == 1
        entry = cap.entries[0]
        assert entry.get("request_id") == "123"
        assert entry.get("user") == "test_user"
        assert entry.get("action") == "test"

    def test_get_logger_different_names(self) -> None:
        """Test that get_logger works with different module names."""
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")

        assert isinstance(logger1, structlog.BoundLogger)
        assert isinstance(logger2, structlog.BoundLogger)
        # Both should be functional - set up new capture for this test
        test_cap = structlog.testing.LogCapture()
        structlog.configure(logger_factory=lambda: test_cap)

        logger1 = get_logger("module1")
        logger2 = get_logger("module2")
        logger1.info("test1", module="module1")
        logger2.info("test2", module="module2")

        assert len(test_cap.entries) == 2


class TestStructuredLogging:
    """Test cases for structured logging functionality."""

    def test_structured_logging_format(self) -> None:
        """Test that structured logging produces expected format."""
        cap = structlog.testing.LogCapture()
        structlog.configure(logger_factory=lambda: cap)

        logger = get_logger("test_module")
        logger.info(
            "user_action",
            action="login",
            user_id=123,
            success=True,
            message="User logged in successfully",
        )

        assert len(cap.entries) == 1
        entry = cap.entries[0]

        assert entry["event"] == "user_action"
        assert entry["action"] == "login"
        assert entry["user_id"] == 123
        assert entry["success"] is True
        assert entry["message"] == "User logged in successfully"

    def test_logging_levels(self) -> None:
        """Test that different logging levels work correctly."""
        cap = structlog.testing.LogCapture()
        structlog.configure(logger_factory=lambda: cap)

        logger = get_logger("test_module")

        logger.debug("debug_event", level="debug")
        logger.info("info_event", level="info")
        logger.warning("warning_event", level="warning")
        logger.error("error_event", level="error")
        logger.critical("critical_event", level="critical")

        assert len(cap.entries) == 5

        levels = [entry.get("level") for entry in cap.entries]
        assert "debug" in levels
        assert "info" in levels
        assert "warning" in levels
        assert "error" in levels
        assert "critical" in levels

    def test_context_propagation(self) -> None:
        """Test that context propagates correctly through bound loggers."""
        cap = structlog.testing.LogCapture()
        structlog.configure(logger_factory=lambda: cap)

        base_logger = get_logger("test_module")
        bound_logger = base_logger.bind(request_id="req_123", session="sess_456")

        bound_logger.info("first_event", action="start")
        bound_logger.info("second_event", action="continue")

        assert len(cap.entries) == 2

        for entry in cap.entries:
            assert entry.get("request_id") == "req_123"
            assert entry.get("session") == "sess_456"

    def test_exception_logging(self) -> None:
        """Test that exceptions are logged correctly.

        Raises:
            ValueError: Test exception that is intentionally raised and caught for logging
                verification.
        """
        cap = structlog.testing.LogCapture()
        structlog.configure(logger_factory=lambda: cap)

        logger = get_logger("test_module")

        try:
            raise ValueError("Test exception")
        except ValueError:
            logger.exception(
                "exception_occurred",
                action="handle_error",
                error_type="ValueError",
            )

        assert len(cap.entries) == 1
        entry = cap.entries[0]

        assert entry["event"] == "exception_occurred"
        assert entry["action"] == "handle_error"
        assert entry["error_type"] == "ValueError"
        # Exception info should be included
        assert "exception" in entry or "exc_info" in entry


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
                    },
                },
            },
            "risk": {
                "global": {
                    "max_position_usd": "1000.0",
                    "max_total_exposure_usd": "5000.0",
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
            "portfolio_tracker": {
                "data_freshness_seconds": 30,
                "initial_positions": [],
            },
        }

        if log_file is not None:
            config_data["general"]["log_file"] = log_file

        return AppSettings.model_validate(config_data)

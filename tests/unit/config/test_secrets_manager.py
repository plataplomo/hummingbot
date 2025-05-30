"""
Unit tests for cyberdelta.config.secrets_manager module.

Tests the SecretsManager class for loading and validating secrets configuration,
including file handling, validation, and error scenarios.
"""

import os
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
import yaml

from cyberdelta.config.secrets_manager import ConfigurationError, SecretsManager
from cyberdelta.config.secrets_models import SecretsConfig


class TestSecretsManager:
    """Test cases for SecretsManager class."""

    def create_valid_secrets_dict(self) -> dict[str, Any]:
        """Create valid secrets dictionary for testing."""
        return {
            "exchanges": {
                "backpack": {
                    "api_key": "bp_api_key",
                    "api_secret": "bp_api_secret",
                },
                "hyperliquid": {
                    "api_key": "hl_api_key",
                    "api_secret": "hl_api_secret",
                    "private_key": (
                        "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                    ),
                },
            },
            "notifications": {
                "telegram": {
                    "bot_token": "telegram_bot_token",
                    "chat_id": "telegram_chat_id",
                }
            },
            "logfire": {
                "write_token": "logfire_write_token",
            },
        }

    def create_secrets_file(self, temp_dir: str, filename: str = "secrets.yaml") -> Path:
        """Create a temporary secrets file with valid content."""
        secrets_path = Path(temp_dir) / filename
        secrets_data = self.create_valid_secrets_dict()

        with open(secrets_path, "w") as f:
            yaml.safe_dump(secrets_data, f)

        return secrets_path

    def test_init_with_valid_secrets_file(self) -> None:
        """Test SecretsManager initialization with valid secrets file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = self.create_secrets_file(temp_dir)

            manager = SecretsManager(str(secrets_path))

            assert manager.secrets_loaded is True
            assert manager.secrets_data is not None
            assert isinstance(manager.secrets_data, SecretsConfig)
            assert "backpack" in manager.secrets_data.exchanges
            assert "hyperliquid" in manager.secrets_data.exchanges

    def test_init_with_explicit_path(self) -> None:
        """Test SecretsManager initialization with explicit path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = self.create_secrets_file(temp_dir, "custom_secrets.yaml")

            manager = SecretsManager(str(secrets_path))

            assert manager.secrets_path == secrets_path
            assert manager.secrets_loaded is True

    def test_init_file_not_found(self) -> None:
        """Test SecretsManager initialization when secrets file doesn't exist."""
        non_existent_path = "/path/that/does/not/exist/secrets.yaml"

        with pytest.raises(ConfigurationError) as exc_info:
            SecretsManager(non_existent_path)

        assert "Secrets file not found" in str(exc_info.value)
        assert non_existent_path in str(exc_info.value)

    def test_init_invalid_yaml(self) -> None:
        """Test SecretsManager initialization with invalid YAML."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = Path(temp_dir) / "invalid.yaml"

            # Write invalid YAML
            with open(secrets_path, "w") as f:
                f.write("invalid: yaml: content: [\n")

            with pytest.raises(ConfigurationError) as exc_info:
                SecretsManager(str(secrets_path))

            assert "Error reading secrets file" in str(exc_info.value)

    def test_init_empty_file(self) -> None:
        """Test SecretsManager initialization with empty file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = Path(temp_dir) / "empty.yaml"

            # Write empty file
            with open(secrets_path, "w") as f:
                f.write("")

            with pytest.raises(ConfigurationError) as exc_info:
                SecretsManager(str(secrets_path))

            assert "Invalid or empty content" in str(exc_info.value)

    def test_init_invalid_secrets_structure(self) -> None:
        """Test SecretsManager initialization with invalid secrets structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = Path(temp_dir) / "invalid_structure.yaml"

            # Write invalid structure (missing required fields)
            invalid_data = {
                "exchanges": {
                    "test": {
                        "api_key": "key",
                        # Missing api_secret
                    }
                }
                # Missing notifications and logfire
            }

            with open(secrets_path, "w") as f:
                yaml.safe_dump(invalid_data, f)

            with pytest.raises(ConfigurationError) as exc_info:
                SecretsManager(str(secrets_path))

            assert "Invalid secrets configuration" in str(exc_info.value)

    @patch.dict(os.environ, {}, clear=True)
    def test_get_secrets_path_default(self) -> None:
        """Test _get_secrets_path with default location."""
        # Test the default path logic by checking the actual behavior
        # rather than calling the protected method directly
        # Create a secrets file in the default location
        default_secrets_dir = Path.home() / ".cyberdelta"
        default_secrets_dir.mkdir(exist_ok=True)
        default_secrets_path = default_secrets_dir / "secrets.yaml"

        try:
            # Create a valid secrets file
            with open(default_secrets_path, "w") as f:
                yaml.safe_dump(self.create_valid_secrets_dict(), f)

            # Initialize without explicit path - should find the default
            manager = SecretsManager()

            assert manager.secrets_path == default_secrets_path
            assert manager.secrets_loaded is True

        finally:
            # Clean up
            if default_secrets_path.exists():
                default_secrets_path.unlink()
            if default_secrets_dir.exists() and not any(default_secrets_dir.iterdir()):
                default_secrets_dir.rmdir()

    @patch.dict(os.environ, {"CYBERDELTA_SECRETS_PATH": "/custom/path/secrets.yaml"})
    def test_get_secrets_path_from_env(self) -> None:
        """Test _get_secrets_path with environment variable."""
        # Test environment variable behavior by creating the file and testing initialization
        with tempfile.TemporaryDirectory() as temp_dir:
            custom_path = Path(temp_dir) / "custom_secrets.yaml"

            # Create the custom secrets file
            with open(custom_path, "w") as f:
                yaml.safe_dump(self.create_valid_secrets_dict(), f)

            # Patch the environment variable to point to our test file
            with patch.dict(os.environ, {"CYBERDELTA_SECRETS_PATH": str(custom_path)}):
                manager = SecretsManager()

                assert manager.secrets_path == custom_path
                assert manager.secrets_loaded is True

    def test_load_method_success(self) -> None:
        """Test load method with valid secrets."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = self.create_secrets_file(temp_dir)

            # Create manager without auto-loading
            manager = SecretsManager.__new__(SecretsManager)
            manager.secrets_path = secrets_path
            manager.secrets_data = None
            manager.secrets_loaded = False

            # Call load explicitly
            manager.load()

            # Test assertions after successful load
            # If load() raised an exception, we wouldn't reach here
            assert manager.secrets_loaded is True
            assert manager.secrets_data is not None

    def test_load_method_file_not_found(self) -> None:
        """Test load method when file doesn't exist."""
        manager = SecretsManager.__new__(SecretsManager)
        manager.secrets_path = Path("/nonexistent/secrets.yaml")
        manager.secrets_data = None
        manager.secrets_loaded = False

        with pytest.raises(ConfigurationError) as exc_info:
            manager.load()

        assert "Secrets file not found" in str(exc_info.value)
        assert manager.secrets_loaded is False
        assert manager.secrets_data is None

    def test_load_method_yaml_error(self) -> None:
        """Test load method with YAML parsing error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = Path(temp_dir) / "invalid.yaml"

            # Write invalid YAML
            with open(secrets_path, "w") as f:
                f.write("invalid: yaml: [unclosed\n")

            manager = SecretsManager.__new__(SecretsManager)
            manager.secrets_path = secrets_path
            manager.secrets_data = None
            manager.secrets_loaded = False

            with pytest.raises(ConfigurationError) as exc_info:
                manager.load()

            assert "Error reading secrets file" in str(exc_info.value)
            assert manager.secrets_loaded is False
            assert manager.secrets_data is None

    def test_load_method_validation_error(self) -> None:
        """Test load method with Pydantic validation error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = Path(temp_dir) / "invalid_structure.yaml"

            # Write structurally valid YAML but invalid secrets structure
            invalid_data = {"invalid": "structure"}

            with open(secrets_path, "w") as f:
                yaml.safe_dump(invalid_data, f)

            manager = SecretsManager.__new__(SecretsManager)
            manager.secrets_path = secrets_path
            manager.secrets_data = None
            manager.secrets_loaded = False

            with pytest.raises(ConfigurationError) as exc_info:
                manager.load()

            assert "Invalid secrets configuration" in str(exc_info.value)
            assert manager.secrets_loaded is False
            assert manager.secrets_data is None

    def test_reload_method(self) -> None:
        """Test reload method."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = self.create_secrets_file(temp_dir)

            # Initialize manager
            manager = SecretsManager(str(secrets_path))
            original_data = manager.secrets_data

            # Modify the secrets file
            modified_data = self.create_valid_secrets_dict()
            modified_data["exchanges"]["new_exchange"] = {
                "api_key": "new_key",
                "api_secret": "new_secret",
            }

            with open(secrets_path, "w") as f:
                yaml.safe_dump(modified_data, f)

            # Reload
            manager.reload()

            # Verify data was reloaded
            assert manager.secrets_loaded is True
            assert manager.secrets_data is not None
            assert "new_exchange" in manager.secrets_data.exchanges
            assert manager.secrets_data is not original_data  # Should be a new instance

    def test_reload_method_failure(self) -> None:
        """Test reload method when reload fails."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = self.create_secrets_file(temp_dir)

            # Initialize manager
            manager = SecretsManager(str(secrets_path))

            # Corrupt the secrets file
            with open(secrets_path, "w") as f:
                f.write("invalid: yaml: [unclosed\n")

            # Reload should fail
            with pytest.raises(ConfigurationError):
                manager.reload()

            # State should be reset
            assert manager.secrets_loaded is False
            assert manager.secrets_data is None

    def test_hyperliquid_private_key_validation(self) -> None:
        """Test Hyperliquid private key validation through SecretsManager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_data = self.create_valid_secrets_dict()
            # Use a valid private key
            secrets_data["exchanges"]["hyperliquid"]["private_key"] = (
                "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            )

            secrets_path = Path(temp_dir) / "secrets.yaml"
            with open(secrets_path, "w") as f:
                yaml.safe_dump(secrets_data, f)

            manager = SecretsManager(str(secrets_path))

            assert manager.secrets_loaded is True
            assert manager.secrets_data is not None
            hl_secrets = manager.secrets_data.exchanges["hyperliquid"]
            assert hl_secrets.private_key is not None

    def test_hyperliquid_passphrase_validation(self) -> None:
        """Test Hyperliquid passphrase validation through SecretsManager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_data = self.create_valid_secrets_dict()
            # Remove private_key and add valid passphrase
            del secrets_data["exchanges"]["hyperliquid"]["private_key"]
            secrets_data["exchanges"]["hyperliquid"]["passphrase"] = (
                "abandon abandon abandon abandon abandon abandon abandon abandon "
                "abandon abandon abandon about"
            )

            secrets_path = Path(temp_dir) / "secrets.yaml"
            with open(secrets_path, "w") as f:
                yaml.safe_dump(secrets_data, f)

            manager = SecretsManager(str(secrets_path))

            assert manager.secrets_loaded is True
            assert manager.secrets_data is not None
            hl_secrets = manager.secrets_data.exchanges["hyperliquid"]
            assert hl_secrets.passphrase is not None
            assert hl_secrets.private_key is None

    def test_hyperliquid_validation_failure(self) -> None:
        """Test Hyperliquid validation failure through SecretsManager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_data = self.create_valid_secrets_dict()
            # Remove private_key (and no passphrase) - should fail validation
            del secrets_data["exchanges"]["hyperliquid"]["private_key"]

            secrets_path = Path(temp_dir) / "secrets.yaml"
            with open(secrets_path, "w") as f:
                yaml.safe_dump(secrets_data, f)

            with pytest.raises(ConfigurationError) as exc_info:
                SecretsManager(str(secrets_path))

            assert "either 'private_key' or 'passphrase' must be provided" in str(exc_info.value)

    @patch("cyberdelta.config.secrets_manager.logger")
    def test_logging_on_success(self, mock_logger: Mock) -> None:
        """Test that successful loading logs appropriate messages."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = self.create_secrets_file(temp_dir)

            SecretsManager(str(secrets_path))

            # Check that info log was called for successful loading
            mock_logger.info.assert_called()
            info_calls = [call for call in mock_logger.info.call_args_list]
            assert any("loaded and validated successfully" in str(call) for call in info_calls)

    @patch("cyberdelta.config.secrets_manager.logger")
    def test_logging_on_file_not_found(self, mock_logger: Mock) -> None:
        """Test that file not found logs critical message."""
        non_existent_path = "/path/that/does/not/exist/secrets.yaml"

        with pytest.raises(ConfigurationError):
            SecretsManager(non_existent_path)

        # Check that critical log was called
        mock_logger.critical.assert_called()
        critical_calls = [call for call in mock_logger.critical.call_args_list]
        assert any("Secrets file not found" in str(call) for call in critical_calls)

    @patch("cyberdelta.config.secrets_manager.logger")
    def test_logging_on_validation_error(self, mock_logger: Mock) -> None:
        """Test that validation errors log critical messages."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = Path(temp_dir) / "invalid.yaml"

            # Write invalid structure
            with open(secrets_path, "w") as f:
                yaml.safe_dump({"invalid": "structure"}, f)

            with pytest.raises(ConfigurationError):
                SecretsManager(str(secrets_path))

            # Check that critical log was called for validation failure
            mock_logger.critical.assert_called()
            critical_calls = [call for call in mock_logger.critical.call_args_list]
            assert any("Secrets validation failed" in str(call) for call in critical_calls)

    def test_secrets_data_access(self) -> None:
        """Test accessing secrets data after successful loading."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = self.create_secrets_file(temp_dir)

            manager = SecretsManager(str(secrets_path))

            # Test accessing exchange secrets
            assert manager.secrets_data is not None
            backpack_secrets = manager.secrets_data.exchanges["backpack"]
            assert backpack_secrets.api_key.get_secret_value() == "bp_api_key"
            assert backpack_secrets.api_secret.get_secret_value() == "bp_api_secret"

            # Test accessing notification secrets
            telegram_secrets = manager.secrets_data.notifications.telegram
            assert telegram_secrets.bot_token.get_secret_value() == "telegram_bot_token"
            assert telegram_secrets.chat_id == "telegram_chat_id"

            # Test accessing logfire secrets
            logfire_secrets = manager.secrets_data.logfire
            assert logfire_secrets.write_token.get_secret_value() == "logfire_write_token"

    def test_file_permissions_error(self) -> None:
        """Test handling of file permission errors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = Path(temp_dir) / "secrets.yaml"

            # Create file but make it unreadable
            with open(secrets_path, "w") as f:
                yaml.safe_dump(self.create_valid_secrets_dict(), f)

            # Make file unreadable (this might not work on all systems)
            try:
                secrets_path.chmod(0o000)

                with pytest.raises(ConfigurationError) as exc_info:
                    SecretsManager(str(secrets_path))

                assert "Error reading secrets file" in str(exc_info.value)

            finally:
                # Restore permissions for cleanup
                secrets_path.chmod(0o644)

    def test_yaml_safe_load_security(self) -> None:
        """Test that YAML loading uses safe_load for security."""
        with tempfile.TemporaryDirectory() as temp_dir:
            secrets_path = Path(temp_dir) / "secrets.yaml"

            # Write YAML with potentially dangerous content (should be safe with safe_load)
            dangerous_yaml = """
exchanges:
  test:
    api_key: "key"
    api_secret: "secret"
notifications:
  telegram:
    bot_token: "token"
    chat_id: "id"
logfire:
  write_token: "token"
# This would be dangerous with yaml.load but safe with yaml.safe_load
dangerous_tag: !!python/object/apply:os.system ["echo 'this should not execute'"]
"""

            with open(secrets_path, "w") as f:
                f.write(dangerous_yaml)

            # Should fail validation due to extra field, not execute dangerous code
            with pytest.raises(ConfigurationError) as exc_info:
                SecretsManager(str(secrets_path))

            # Should be a YAML parsing error about the dangerous tag, not a validation error
            assert "Error reading secrets file" in str(exc_info.value)
            assert "could not determine a constructor" in str(exc_info.value)


class TestConfigurationError:
    """Test cases for ConfigurationError exception."""

    def test_configuration_error_inheritance(self) -> None:
        """Test that ConfigurationError inherits from Exception."""
        error = ConfigurationError("test message")
        assert isinstance(error, Exception)
        assert str(error) == "test message"

    def test_configuration_error_with_cause(self) -> None:
        """Test ConfigurationError with cause chain."""
        original_error = ValueError("original error")

        try:
            raise original_error
        except ValueError as e:
            config_error = ConfigurationError("config error")
            config_error.__cause__ = e

        assert isinstance(config_error, ConfigurationError)
        assert config_error.__cause__ is original_error
        assert str(config_error) == "config error"

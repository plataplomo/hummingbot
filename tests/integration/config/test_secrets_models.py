"""Unit tests for cyberdelta.config.secrets_models module.

Tests all Pydantic models for secrets configuration validation,
including field validation, model validation, and security features.
"""

from typing import Any

import pytest
from pydantic import SecretStr, TypeAdapter, ValidationError

from cyberdelta.config.secrets_models import (
    AnyExchangeSecrets,
    ApiKeyAuthSecrets,
    LogfireSecrets,
    NotificationsConfig,
    PrivateKeyAuthSecrets,
    SecretsConfig,
    TelegramSecrets,
)
from cyberdelta.exceptions.parsing import EmptyStringError


class TestApiKeyAuthSecrets:
    """Test cases for ApiKeyAuthSecrets model."""

    def test_valid_api_key_auth_secrets(self) -> None:
        """Test valid ApiKeyAuthSecrets with all required fields."""
        data = {
            "auth_type": "api_key",
            "api_key": "test_api_key",
            "api_secret": "test_api_secret",
        }

        secrets = ApiKeyAuthSecrets.model_validate(data)

        assert secrets.auth_type == "api_key"
        assert secrets.api_key.get_secret_value() == "test_api_key"
        assert secrets.api_secret.get_secret_value() == "test_api_secret"

    def test_auth_type_default_value(self) -> None:
        """Test that auth_type defaults to 'api_key'."""
        data = {
            "api_key": "test_api_key",
            "api_secret": "test_api_secret",
        }

        secrets = ApiKeyAuthSecrets.model_validate(data)
        assert secrets.auth_type == "api_key"

    def test_missing_required_fields(self) -> None:
        """Test that missing required fields raise ValidationError."""
        # Missing api_key
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyAuthSecrets.model_validate({"api_secret": "test_secret"})
        assert "api_key" in str(exc_info.value)

        # Missing api_secret
        with pytest.raises(ValidationError) as exc_info:
            ApiKeyAuthSecrets.model_validate({"api_key": "test_key"})
        assert "api_secret" in str(exc_info.value)

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        data = {
            "api_key": "test_api_key",
            "api_secret": "test_api_secret",
            "extra_field": "not_allowed",
        }

        with pytest.raises(ValidationError) as exc_info:
            ApiKeyAuthSecrets.model_validate(data)
        assert "extra_field" in str(exc_info.value)

    def test_secret_str_security(self) -> None:
        """Test that SecretStr fields don't expose values in repr."""
        secrets = ApiKeyAuthSecrets.model_validate(
            {
                "api_key": "secret_key",
                "api_secret": "secret_value",
            },
        )

        # SecretStr should not expose the actual value in string representation
        secrets_repr = repr(secrets)
        assert "secret_key" not in secrets_repr
        assert "secret_value" not in secrets_repr
        assert "**********" in secrets_repr or "SecretStr" in secrets_repr

    def test_frozen_model(self) -> None:
        """Test that the model is frozen (immutable)."""
        secrets = ApiKeyAuthSecrets.model_validate(
            {
                "api_key": "test_key",
                "api_secret": "test_secret",
            },
        )

        with pytest.raises(ValidationError):
            secrets.api_key = SecretStr("new_key")


class TestPrivateKeyAuthSecrets:
    """Test cases for PrivateKeyAuthSecrets model."""

    def test_valid_private_key_auth_secrets(self) -> None:
        """Test valid PrivateKeyAuthSecrets with private key."""
        data = {
            "auth_type": "private_key",
            "private_key": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        }

        secrets = PrivateKeyAuthSecrets.model_validate(data)

        assert secrets.auth_type == "private_key"
        expected_key = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert secrets.private_key.get_secret_value() == expected_key
        assert secrets.passphrase is None

    def test_valid_private_key_auth_secrets_with_passphrase(self) -> None:
        """Test valid PrivateKeyAuthSecrets with passphrase."""
        data = {
            "auth_type": "private_key",
            "private_key": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "passphrase": "test_passphrase",
        }

        secrets = PrivateKeyAuthSecrets.model_validate(data)

        assert secrets.auth_type == "private_key"
        expected_key = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert secrets.private_key.get_secret_value() == expected_key
        assert secrets.passphrase is not None
        assert secrets.passphrase.get_secret_value() == "test_passphrase"

    def test_auth_type_default_value(self) -> None:
        """Test that auth_type defaults to 'private_key'."""
        data = {
            "private_key": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        }

        secrets = PrivateKeyAuthSecrets.model_validate(data)
        assert secrets.auth_type == "private_key"

    def test_missing_private_key(self) -> None:
        """Test that missing private_key raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            PrivateKeyAuthSecrets.model_validate({"passphrase": "test_passphrase"})
        assert "private_key" in str(exc_info.value)

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        data = {
            "private_key": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "extra_field": "not_allowed",
        }

        with pytest.raises(ValidationError) as exc_info:
            PrivateKeyAuthSecrets.model_validate(data)
        assert "extra_field" in str(exc_info.value)


class TestAnyExchangeSecrets:
    """Test cases for AnyExchangeSecrets discriminated union."""

    def test_api_key_auth_secrets_discrimination(self) -> None:
        """Test that ApiKeyAuthSecrets is correctly discriminated."""
        data = {
            "auth_type": "api_key",
            "api_key": "test_key",
            "api_secret": "test_secret",
        }

        # Parse as the union type

        adapter: TypeAdapter[AnyExchangeSecrets] = TypeAdapter(AnyExchangeSecrets)
        secrets = adapter.validate_python(data)

        assert isinstance(secrets, ApiKeyAuthSecrets)
        assert secrets.auth_type == "api_key"

    def test_private_key_auth_secrets_discrimination(self) -> None:
        """Test that PrivateKeyAuthSecrets is correctly discriminated."""
        data = {
            "auth_type": "private_key",
            "private_key": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        }

        # Parse as the union type
        adapter: TypeAdapter[AnyExchangeSecrets] = TypeAdapter(AnyExchangeSecrets)
        secrets = adapter.validate_python(data)

        assert isinstance(secrets, PrivateKeyAuthSecrets)
        assert secrets.auth_type == "private_key"

    def test_invalid_auth_type(self) -> None:
        """Test that invalid auth_type raises ValidationError."""
        data = {
            "auth_type": "invalid_type",
            "api_key": "test_key",
            "api_secret": "test_secret",
        }

        adapter: TypeAdapter[AnyExchangeSecrets] = TypeAdapter(AnyExchangeSecrets)
        with pytest.raises(ValidationError) as exc_info:
            adapter.validate_python(data)
        assert "auth_type" in str(exc_info.value) or "discriminator" in str(exc_info.value)


class TestTelegramSecrets:
    """Test cases for TelegramSecrets model."""

    def test_valid_telegram_secrets(self) -> None:
        """Test valid TelegramSecrets."""
        data = {
            "bot_token": "123456789:ABCdefGHIjklMNOpqrsTUVwxyz",
            "chat_id": "-1001234567890",
        }

        secrets = TelegramSecrets.model_validate(data)

        assert secrets.bot_token.get_secret_value() == "123456789:ABCdefGHIjklMNOpqrsTUVwxyz"
        assert secrets.chat_id == "-1001234567890"

    def test_chat_id_validation(self) -> None:
        """Test chat_id field validation."""
        # Valid chat IDs
        valid_chat_ids = [
            "123456789",
            "-1001234567890",
            "@username",
            "group_chat_id",
        ]

        for chat_id in valid_chat_ids:
            data = {
                "bot_token": "test_token",
                "chat_id": chat_id,
            }
            secrets = TelegramSecrets.model_validate(data)
            assert secrets.chat_id == chat_id

    def test_chat_id_validation_failures(self) -> None:
        """Test chat_id validation failures."""
        # Empty chat_id (raises EmptyStringError)
        with pytest.raises(EmptyStringError) as exc_info:
            TelegramSecrets.model_validate(
                {
                    "bot_token": "test_token",
                    "chat_id": "",
                },
            )
        assert "chat_id" in str(exc_info.value)

        # Non-string chat_id - should be converted to string and validated
        with pytest.raises(ValidationError):
            TelegramSecrets.model_validate(
                {
                    "bot_token": "test_token",
                    "chat_id": 123456789,
                },
            )

        # Too long chat_id
        with pytest.raises(ValidationError) as exc_info_val2:
            TelegramSecrets.model_validate(
                {
                    "bot_token": "test_token",
                    "chat_id": "x" * 101,  # Exceeds max_length=100
                },
            )
        assert "chat_id" in str(exc_info_val2.value)

    def test_missing_required_fields(self) -> None:
        """Test missing required fields."""
        # Missing bot_token
        with pytest.raises(ValidationError) as exc_info:
            TelegramSecrets.model_validate({"chat_id": "123456789"})
        assert "bot_token" in str(exc_info.value)

        # Missing chat_id
        with pytest.raises(ValidationError) as exc_info:
            TelegramSecrets.model_validate({"bot_token": "test_token"})
        assert "chat_id" in str(exc_info.value)


class TestNotificationsConfig:
    """Test cases for NotificationsConfig model."""

    def test_valid_notifications_config(self) -> None:
        """Test valid NotificationsConfig."""
        data = {
            "telegram": {
                "bot_token": "test_token",
                "chat_id": "test_chat_id",
            },
        }

        config = NotificationsConfig.model_validate(data)

        assert isinstance(config.telegram, TelegramSecrets)
        assert config.telegram.bot_token.get_secret_value() == "test_token"
        assert config.telegram.chat_id == "test_chat_id"

    def test_missing_telegram_config(self) -> None:
        """Test missing telegram configuration."""
        with pytest.raises(ValidationError) as exc_info:
            NotificationsConfig.model_validate({})
        assert "telegram" in str(exc_info.value)

    def test_invalid_telegram_config(self) -> None:
        """Test invalid telegram configuration."""
        data = {
            "telegram": {
                "bot_token": "test_token",
                # Missing chat_id
            },
        }

        with pytest.raises(ValidationError) as exc_info:
            NotificationsConfig.model_validate(data)
        assert "chat_id" in str(exc_info.value)


class TestLogfireSecrets:
    """Test cases for LogfireSecrets model."""

    def test_valid_logfire_secrets(self) -> None:
        """Test valid LogfireSecrets."""
        data = {"write_token": "logfire_write_token_123"}

        secrets = LogfireSecrets.model_validate(data)

        assert secrets.write_token.get_secret_value() == "logfire_write_token_123"

    def test_missing_write_token(self) -> None:
        """Test missing write_token."""
        with pytest.raises(ValidationError) as exc_info:
            LogfireSecrets.model_validate({})
        assert "write_token" in str(exc_info.value)

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        data = {
            "write_token": "test_token",
            "extra_field": "not_allowed",
        }

        with pytest.raises(ValidationError) as exc_info:
            LogfireSecrets.model_validate(data)
        assert "extra_field" in str(exc_info.value)


class TestSecretsConfig:
    """Test cases for SecretsConfig model."""

    def create_valid_secrets_data(self) -> dict[str, Any]:
        """Create valid secrets data for testing.

        Returns:
            Dictionary containing valid secrets configuration data for testing.
        """
        return {
            "exchanges": {
                "backpack": {
                    "auth_type": "api_key",
                    "api_key": "bp_api_key",
                    "api_secret": "bp_api_secret",
                },
                "hyperliquid": {
                    "auth_type": "private_key",
                    "private_key": (
                        "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                    ),
                },
            },
            "notifications": {
                "telegram": {
                    "bot_token": "telegram_bot_token",
                    "chat_id": "telegram_chat_id",
                },
            },
            "logfire": {
                "write_token": "logfire_write_token",
            },
        }

    def test_valid_secrets_config(self) -> None:
        """Test valid SecretsConfig."""
        data = self.create_valid_secrets_data()

        config = SecretsConfig.model_validate(data)

        # Test exchanges
        assert "backpack" in config.exchanges
        assert "hyperliquid" in config.exchanges

        # Test backpack (ApiKeyAuthSecrets)
        backpack_secrets = config.exchanges["backpack"]
        assert isinstance(backpack_secrets, ApiKeyAuthSecrets)
        assert backpack_secrets.auth_type == "api_key"
        assert backpack_secrets.api_key.get_secret_value() == "bp_api_key"

        # Test hyperliquid (PrivateKeyAuthSecrets)
        hyperliquid_secrets = config.exchanges["hyperliquid"]
        assert isinstance(hyperliquid_secrets, PrivateKeyAuthSecrets)
        assert hyperliquid_secrets.auth_type == "private_key"
        expected_key = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert hyperliquid_secrets.private_key.get_secret_value() == expected_key

        # Test notifications
        assert isinstance(config.notifications, NotificationsConfig)
        assert config.notifications.telegram.bot_token.get_secret_value() == "telegram_bot_token"

        # Test logfire
        assert isinstance(config.logfire, LogfireSecrets)
        assert config.logfire.write_token.get_secret_value() == "logfire_write_token"

    def test_exchanges_validation(self) -> None:
        """Test exchanges field validation."""
        data = self.create_valid_secrets_data()

        # Test empty exchange name (raises EmptyStringError)
        data["exchanges"][""] = {"auth_type": "api_key", "api_key": "key", "api_secret": "secret"}
        with pytest.raises(EmptyStringError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "exchanges" in str(exc_info.value)

        # Test too long exchange name (raises ValidationError)
        data = self.create_valid_secrets_data()
        data["exchanges"]["x" * 51] = {
            "auth_type": "api_key",
            "api_key": "key",
            "api_secret": "secret",
        }
        with pytest.raises(ValidationError) as exc_info_validation:
            SecretsConfig.model_validate(data)
        assert "exchanges" in str(exc_info_validation.value)

    def test_hyperliquid_wrong_auth_type(self) -> None:
        """Test Hyperliquid validation when wrong auth_type is provided."""
        data = self.create_valid_secrets_data()
        data["exchanges"]["hyperliquid"] = {
            "auth_type": "api_key",  # Wrong auth type for Hyperliquid
            "api_key": "hl_api_key",
            "api_secret": "hl_api_secret",
        }

        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "Hyperliquid configuration in secrets must have auth_type 'private_key'" in str(
            exc_info.value,
        )

    def test_hyperliquid_empty_private_key(self) -> None:
        """Test Hyperliquid validation with empty private_key."""
        data = self.create_valid_secrets_data()
        data["exchanges"]["hyperliquid"] = {
            "auth_type": "private_key",
            "private_key": "",  # Empty private key
        }

        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "Hyperliquid 'private_key' cannot be empty" in str(exc_info.value)

    def test_backpack_wrong_auth_type(self) -> None:
        """Test Backpack validation when wrong auth_type is provided."""
        data = self.create_valid_secrets_data()
        data["exchanges"]["backpack"] = {
            "auth_type": "private_key",  # Wrong auth type for Backpack
            "private_key": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        }

        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "Backpack configuration in secrets must have auth_type 'api_key'" in str(
            exc_info.value,
        )

    def test_backpack_empty_api_credentials(self) -> None:
        """Test Backpack validation with empty API credentials."""
        data = self.create_valid_secrets_data()

        # Test empty api_key
        data["exchanges"]["backpack"] = {
            "auth_type": "api_key",
            "api_key": "",  # Empty api_key
            "api_secret": "test_secret",
        }
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "Backpack 'api_key' (ED25519 Public Key) cannot be empty" in str(exc_info.value)

        # Test empty api_secret
        data["exchanges"]["backpack"] = {
            "auth_type": "api_key",
            "api_key": "test_key",
            "api_secret": "",  # Empty api_secret
        }
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "Backpack 'api_secret' (ED25519 Private Key) cannot be empty" in str(exc_info.value)

    def test_non_hyperliquid_backpack_exchange_no_validation(self) -> None:
        """Test that exchanges other than Hyperliquid/Backpack don't trigger special validation."""
        data = self.create_valid_secrets_data()
        data["exchanges"]["other_exchange"] = {
            "auth_type": "api_key",
            "api_key": "other_key",
            "api_secret": "other_secret",
        }

        config = SecretsConfig.model_validate(data)
        assert "other_exchange" in config.exchanges
        assert isinstance(config.exchanges["other_exchange"], ApiKeyAuthSecrets)
        assert config.exchanges["other_exchange"].auth_type == "api_key"

    def test_missing_required_sections(self) -> None:
        """Test missing required sections."""
        # Missing exchanges
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(
                {
                    "notifications": {"telegram": {"bot_token": "token", "chat_id": "id"}},
                    "logfire": {"write_token": "token"},
                },
            )
        assert "exchanges" in str(exc_info.value)

        # Missing notifications
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(
                {
                    "exchanges": {
                        "test": {
                            "auth_type": "api_key",
                            "api_key": "key",
                            "api_secret": "secret",
                        },
                    },
                    "logfire": {"write_token": "token"},
                },
            )
        assert "notifications" in str(exc_info.value)

        # Missing logfire
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(
                {
                    "exchanges": {
                        "test": {
                            "auth_type": "api_key",
                            "api_key": "key",
                            "api_secret": "secret",
                        },
                    },
                    "notifications": {"telegram": {"bot_token": "token", "chat_id": "id"}},
                },
            )
        assert "logfire" in str(exc_info.value)

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        data = self.create_valid_secrets_data()
        data["extra_field"] = "not_allowed"

        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "extra_field" in str(exc_info.value)

    def test_frozen_model(self) -> None:
        """Test that the model is frozen (immutable)."""
        data = self.create_valid_secrets_data()
        config = SecretsConfig.model_validate(data)

        with pytest.raises(ValidationError):
            config.exchanges = {}

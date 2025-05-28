"""
Unit tests for cyberdelta.config.secrets_models module.

Tests all Pydantic models for secrets configuration validation,
including field validation, model validation, and security features.
"""

from typing import Any

import pytest
from pydantic import SecretStr, ValidationError

from cyberdelta.config.secrets_models import (
    ExchangeSecrets,
    LogfireSecrets,
    NotificationsConfig,
    SecretsConfig,
    TelegramSecrets,
)


class TestExchangeSecrets:
    """Test cases for ExchangeSecrets model."""

    def test_valid_exchange_secrets_minimal(self) -> None:
        """Test valid ExchangeSecrets with minimal required fields."""
        data = {
            "api_key": "test_api_key",
            "api_secret": "test_api_secret",
        }

        secrets = ExchangeSecrets.model_validate(data)

        assert secrets.api_key.get_secret_value() == "test_api_key"
        assert secrets.api_secret.get_secret_value() == "test_api_secret"
        assert secrets.private_key is None
        assert secrets.passphrase is None

    def test_valid_exchange_secrets_with_optional_fields(self) -> None:
        """Test valid ExchangeSecrets with all fields."""
        data = {
            "api_key": "test_api_key",
            "api_secret": "test_api_secret",
            "private_key": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "passphrase": "test_passphrase",
        }

        secrets = ExchangeSecrets.model_validate(data)

        assert secrets.api_key.get_secret_value() == "test_api_key"
        assert secrets.api_secret.get_secret_value() == "test_api_secret"
        assert secrets.private_key is not None
        assert (
            secrets.private_key.get_secret_value()
            == "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        )
        assert secrets.passphrase is not None
        assert secrets.passphrase.get_secret_value() == "test_passphrase"

    def test_missing_required_fields(self) -> None:
        """Test that missing required fields raise ValidationError."""
        # Missing api_key
        with pytest.raises(ValidationError) as exc_info:
            ExchangeSecrets.model_validate({"api_secret": "test_secret"})
        assert "api_key" in str(exc_info.value)

        # Missing api_secret
        with pytest.raises(ValidationError) as exc_info:
            ExchangeSecrets.model_validate({"api_key": "test_key"})
        assert "api_secret" in str(exc_info.value)

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        data = {
            "api_key": "test_api_key",
            "api_secret": "test_api_secret",
            "extra_field": "not_allowed",
        }

        with pytest.raises(ValidationError) as exc_info:
            ExchangeSecrets.model_validate(data)
        assert "extra_field" in str(exc_info.value)

    def test_secret_str_security(self) -> None:
        """Test that SecretStr fields don't expose values in repr."""
        secrets = ExchangeSecrets.model_validate(
            {
                "api_key": "secret_key",
                "api_secret": "secret_value",
            }
        )

        # SecretStr should not expose the actual value in string representation
        secrets_repr = repr(secrets)
        assert "secret_key" not in secrets_repr
        assert "secret_value" not in secrets_repr
        assert "**********" in secrets_repr or "SecretStr" in secrets_repr

    def test_frozen_model(self) -> None:
        """Test that the model is frozen (immutable)."""
        secrets = ExchangeSecrets.model_validate(
            {
                "api_key": "test_key",
                "api_secret": "test_secret",
            }
        )

        with pytest.raises(ValidationError):
            secrets.api_key = SecretStr("new_key")


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
        # Empty chat_id
        with pytest.raises(ValidationError) as exc_info:
            TelegramSecrets.model_validate(
                {
                    "bot_token": "test_token",
                    "chat_id": "",
                }
            )
        assert "chat_id" in str(exc_info.value)

        # Non-string chat_id
        with pytest.raises(ValidationError) as exc_info:
            TelegramSecrets.model_validate(
                {
                    "bot_token": "test_token",
                    "chat_id": 123456789,
                }
            )
        # Should be converted to string and validated

        # Too long chat_id
        with pytest.raises(ValidationError) as exc_info:
            TelegramSecrets.model_validate(
                {
                    "bot_token": "test_token",
                    "chat_id": "x" * 101,  # Exceeds max_length=100
                }
            )
        assert "chat_id" in str(exc_info.value)

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
            }
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
            }
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
        """Create valid secrets data for testing."""
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

    def test_valid_secrets_config(self) -> None:
        """Test valid SecretsConfig."""
        data = self.create_valid_secrets_data()

        config = SecretsConfig.model_validate(data)

        # Test exchanges
        assert "backpack" in config.exchanges
        assert "hyperliquid" in config.exchanges
        assert config.exchanges["backpack"].api_key.get_secret_value() == "bp_api_key"
        assert config.exchanges["hyperliquid"].private_key is not None

        # Test notifications
        assert isinstance(config.notifications, NotificationsConfig)
        assert config.notifications.telegram.bot_token.get_secret_value() == "telegram_bot_token"

        # Test logfire
        assert isinstance(config.logfire, LogfireSecrets)
        assert config.logfire.write_token.get_secret_value() == "logfire_write_token"

    def test_exchanges_validation(self) -> None:
        """Test exchanges field validation."""
        data = self.create_valid_secrets_data()

        # Test empty exchange name
        data["exchanges"][""] = {"api_key": "key", "api_secret": "secret"}
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "exchanges" in str(exc_info.value)

        # Test too long exchange name
        data = self.create_valid_secrets_data()
        data["exchanges"]["x" * 51] = {"api_key": "key", "api_secret": "secret"}
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "exchanges" in str(exc_info.value)

    def test_hyperliquid_validation_private_key_only(self) -> None:
        """Test Hyperliquid validation with private_key only."""
        data = self.create_valid_secrets_data()
        data["exchanges"]["hyperliquid"] = {
            "api_key": "hl_api_key",
            "api_secret": "hl_api_secret",
            "private_key": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        }

        config = SecretsConfig.model_validate(data)
        assert config.exchanges["hyperliquid"].private_key is not None
        assert config.exchanges["hyperliquid"].passphrase is None

    def test_hyperliquid_validation_passphrase_only(self) -> None:
        """Test Hyperliquid validation with passphrase only."""
        data = self.create_valid_secrets_data()
        # Valid 12-word BIP-39 mnemonic
        valid_mnemonic = (
            "abandon abandon abandon abandon abandon abandon abandon abandon "
            "abandon abandon abandon about"
        )
        data["exchanges"]["hyperliquid"] = {
            "api_key": "hl_api_key",
            "api_secret": "hl_api_secret",
            "passphrase": valid_mnemonic,
        }

        config = SecretsConfig.model_validate(data)
        assert config.exchanges["hyperliquid"].passphrase is not None
        assert config.exchanges["hyperliquid"].private_key is None

    def test_hyperliquid_validation_neither_provided(self) -> None:
        """Test Hyperliquid validation when neither private_key nor passphrase is provided."""
        data = self.create_valid_secrets_data()
        data["exchanges"]["hyperliquid"] = {
            "api_key": "hl_api_key",
            "api_secret": "hl_api_secret",
        }

        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "either 'private_key' or 'passphrase' must be provided" in str(exc_info.value)

    def test_hyperliquid_validation_both_provided(self) -> None:
        """Test Hyperliquid validation when both private_key and passphrase are provided."""
        data = self.create_valid_secrets_data()
        valid_mnemonic = (
            "abandon abandon abandon abandon abandon abandon abandon abandon "
            "abandon abandon abandon about"
        )
        data["exchanges"]["hyperliquid"] = {
            "api_key": "hl_api_key",
            "api_secret": "hl_api_secret",
            "private_key": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "passphrase": valid_mnemonic,
        }

        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "provide 'private_key' OR 'passphrase', not both" in str(exc_info.value)

    def test_hyperliquid_invalid_private_key_format(self) -> None:
        """Test Hyperliquid validation with invalid private key format."""
        data = self.create_valid_secrets_data()

        # Too short
        data["exchanges"]["hyperliquid"]["private_key"] = "0x123"
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "64-character hex string" in str(exc_info.value)

        # Invalid characters
        data["exchanges"]["hyperliquid"]["private_key"] = "0x" + "g" * 64
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "64-character hex string" in str(exc_info.value)

    def test_hyperliquid_invalid_private_key_crypto(self) -> None:
        """Test Hyperliquid validation with cryptographically invalid private key."""
        data = self.create_valid_secrets_data()

        # All zeros (invalid private key) - actually, let's use a value that's too large for secp256k1
        # The secp256k1 curve order is 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
        # So we'll use all F's which is larger than the curve order
        data["exchanges"]["hyperliquid"]["private_key"] = "0x" + "F" * 64
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "not cryptographically valid" in str(exc_info.value)

    def test_hyperliquid_invalid_passphrase_word_count(self) -> None:
        """Test Hyperliquid validation with invalid passphrase word count."""
        data = self.create_valid_secrets_data()

        # Remove private_key and add invalid passphrase
        data["exchanges"]["hyperliquid"] = {
            "api_key": "hl_api_key",
            "api_secret": "hl_api_secret",
            "passphrase": "abandon abandon abandon",  # Too few words
        }
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "12 or 24 words" in str(exc_info.value)

        # Too many words
        data["exchanges"]["hyperliquid"]["passphrase"] = " ".join(["abandon"] * 25)
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "12 or 24 words" in str(exc_info.value)

    def test_hyperliquid_invalid_passphrase_bip39(self) -> None:
        """Test Hyperliquid validation with invalid BIP-39 mnemonic."""
        data = self.create_valid_secrets_data()

        # Remove private_key and add invalid passphrase with exactly 12 words that are not in BIP-39 wordlist
        data["exchanges"]["hyperliquid"] = {
            "api_key": "hl_api_key",
            "api_secret": "hl_api_secret",
            "passphrase": "invalid words that are not in bip39 wordlist at all here today",
        }
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(data)
        assert "not a valid BIP-39 mnemonic" in str(exc_info.value)

    def test_hyperliquid_private_key_without_0x_prefix(self) -> None:
        """Test Hyperliquid validation with private key without 0x prefix."""
        data = self.create_valid_secrets_data()
        data["exchanges"]["hyperliquid"]["private_key"] = (
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        )

        # Should work without 0x prefix
        config = SecretsConfig.model_validate(data)
        assert config.exchanges["hyperliquid"].private_key is not None

    def test_non_hyperliquid_exchange_no_validation(self) -> None:
        """Test that non-Hyperliquid exchanges don't trigger special validation."""
        data = self.create_valid_secrets_data()
        data["exchanges"]["other_exchange"] = {
            "api_key": "other_key",
            "api_secret": "other_secret",
            # No private_key or passphrase - should be fine for non-Hyperliquid
        }

        config = SecretsConfig.model_validate(data)
        assert "other_exchange" in config.exchanges
        assert config.exchanges["other_exchange"].private_key is None
        assert config.exchanges["other_exchange"].passphrase is None

    def test_missing_required_sections(self) -> None:
        """Test missing required sections."""
        # Missing exchanges
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(
                {
                    "notifications": {"telegram": {"bot_token": "token", "chat_id": "id"}},
                    "logfire": {"write_token": "token"},
                }
            )
        assert "exchanges" in str(exc_info.value)

        # Missing notifications
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(
                {
                    "exchanges": {"test": {"api_key": "key", "api_secret": "secret"}},
                    "logfire": {"write_token": "token"},
                }
            )
        assert "notifications" in str(exc_info.value)

        # Missing logfire
        with pytest.raises(ValidationError) as exc_info:
            SecretsConfig.model_validate(
                {
                    "exchanges": {"test": {"api_key": "key", "api_secret": "secret"}},
                    "notifications": {"telegram": {"bot_token": "token", "chat_id": "id"}},
                }
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

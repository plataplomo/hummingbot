"""
cyberdelta.config.secrets_models
-------------------------------
Pydantic models for secrets.yaml configuration validation.

This module defines the schema for sensitive configuration data including
API keys, tokens, and other credentials. All sensitive fields use SecretStr
to prevent accidental exposure in logs or tracebacks.
"""

from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    ValidationInfo,
    field_validator,
    model_validator,
)

from cyberdelta.utils.parsing import validate_str_field


class BaseExchangeSecrets(BaseModel):
    """Base class for all exchange secret configurations."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ApiKeyAuthSecrets(BaseExchangeSecrets):
    """
    Secrets configuration for exchanges using API key/secret authentication.
    
    This covers exchanges using traditional API key pairs or Backpack's ED25519 
    keys (where api_key=public key and api_secret=private key).
    """

    auth_type: Literal["api_key"] = "api_key"
    api_key: SecretStr
    api_secret: SecretStr


class PrivateKeyAuthSecrets(BaseExchangeSecrets):
    """
    Secrets configuration for exchanges using private key authentication.
    
    This covers exchanges like Hyperliquid that use Ethereum private keys
    or mnemonic passphrases for authentication.
    """

    auth_type: Literal["private_key"] = "private_key"
    private_key: SecretStr
    passphrase: SecretStr | None = Field(default=None)


# Discriminated union for all exchange secret types
AnyExchangeSecrets = Annotated[
    ApiKeyAuthSecrets | PrivateKeyAuthSecrets,
    Field(discriminator="auth_type")
]


class TelegramSecrets(BaseModel):
    """
    Secrets configuration for Telegram notifications.

    Contains bot token and chat ID for sending notifications via Telegram.
    """

    bot_token: SecretStr
    chat_id: str

    @field_validator("chat_id", mode="before")
    @classmethod
    def validate_chat_id(cls, v: object, info: ValidationInfo) -> str:
        """Validate chat_id is a non-empty string."""
        return validate_str_field(
            v,
            field_name=info.field_name or "chat_id",
            allow_empty=False,
            max_length=100,  # Reasonable limit for Telegram chat IDs
        )

    model_config = ConfigDict(extra="forbid", frozen=True)


class NotificationsConfig(BaseModel):
    """
    Configuration for all notification services.

    Currently supports Telegram notifications. Can be extended
    for other notification services in the future.
    """

    telegram: TelegramSecrets

    model_config = ConfigDict(extra="forbid", frozen=True)


class LogfireSecrets(BaseModel):
    """
    Secrets for Pydantic Logfire integration.

    Contains the write token required for sending logs to Pydantic Logfire.
    """

    write_token: SecretStr

    model_config = ConfigDict(extra="forbid", frozen=True)


class SecretsConfig(BaseModel):
    """
    Root configuration model for secrets.yaml.

    Contains all sensitive configuration data including exchange
    API credentials, notification service tokens, and Logfire configuration.
    """

    exchanges: dict[str, AnyExchangeSecrets]
    notifications: NotificationsConfig
    logfire: LogfireSecrets

    @field_validator("exchanges", mode="after")
    @classmethod
    def validate_exchanges(
        cls, v: dict[str, AnyExchangeSecrets], info: ValidationInfo
    ) -> dict[str, AnyExchangeSecrets]:
        """Validate exchange dictionary keys are valid strings."""
        validated_exchanges: dict[str, AnyExchangeSecrets] = {}

        for exchange_name, exchange_secrets in v.items():
            # Validate exchange name is a non-empty string
            validated_key = validate_str_field(
                exchange_name,
                field_name=f"{info.field_name or 'exchanges'}.key",
                allow_empty=False,
                max_length=50,  # Reasonable limit for exchange names
            )

            validated_exchanges[validated_key] = exchange_secrets

        return validated_exchanges

    @model_validator(mode="after")
    def validate_exchange_specific_secret_configurations(self) -> Self:
        """
        Validate exchange-specific secret configurations and authentication requirements.
        
        Ensures that each exchange uses the correct auth_type and has valid credentials.
        Basic presence checks are performed here; deeper cryptographic validation is
        performed in the respective API component factories.
        """
        for exchange_name, secrets_config_item in self.exchanges.items():
            if exchange_name == "hyperliquid":
                if not isinstance(secrets_config_item, PrivateKeyAuthSecrets):
                    raise ValueError(
                        "Hyperliquid configuration in secrets must have auth_type 'private_key' and corresponding fields."
                    )
                # Basic presence check for private_key
                pk_val = secrets_config_item.private_key.get_secret_value()
                if not pk_val or not pk_val.strip():
                    raise ValueError("Hyperliquid 'private_key' cannot be empty in secrets.")
                
                # Note: Cryptographic validation (valid hex, BIP-39) is moved to 
                # HyperliquidAPIComponentsFactory or HyperliquidEip712Authenticator constructor
                # where the secret is actually used to create an auth component.

            elif exchange_name == "backpack":
                if not isinstance(secrets_config_item, ApiKeyAuthSecrets):
                    raise ValueError(
                        "Backpack configuration in secrets must have auth_type 'api_key' and corresponding fields."
                    )
                # Basic presence checks for Backpack (ED25519 keys)
                api_key_val = secrets_config_item.api_key.get_secret_value()
                api_secret_val = secrets_config_item.api_secret.get_secret_value()
                if not (api_key_val and api_key_val.strip()):
                    raise ValueError("Backpack 'api_key' (ED25519 Public Key) cannot be empty.")
                if not (api_secret_val and api_secret_val.strip()):
                    raise ValueError("Backpack 'api_secret' (ED25519 Private Key) cannot be empty.")
                # Note: Deeper crypto validation (is it valid base64 ED25519) is performed 
                # in BackpackAPIComponentsFactory.

        return self

    model_config = ConfigDict(extra="forbid", frozen=True)

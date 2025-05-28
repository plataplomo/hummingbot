"""
cyberdelta.config.secrets_models
-------------------------------
Pydantic models for secrets.yaml configuration validation.

This module defines the schema for sensitive configuration data including
API keys, tokens, and other credentials. All sensitive fields use SecretStr
to prevent accidental exposure in logs or tracebacks.
"""

from typing import Self

from eth_account.account import Account
from mnemonic import Mnemonic
from pydantic import (
    BaseModel,
    ConfigDict,
    SecretStr,
    ValidationInfo,
    field_validator,
    model_validator,
)

from cyberdelta.utils.parsing import validate_str_field


class ExchangeSecrets(BaseModel):
    """
    Secrets configuration for a single exchange.

    Contains API credentials required for exchange connectivity.
    All sensitive fields use SecretStr for security.

    Optional fields (private_key, passphrase) are used by specific exchanges
    like Hyperliquid but not required for all exchanges.
    """

    api_key: SecretStr
    api_secret: SecretStr
    private_key: SecretStr | None = None
    passphrase: SecretStr | None = None

    model_config = ConfigDict(extra="forbid", frozen=True)


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

    exchanges: dict[str, ExchangeSecrets]
    notifications: NotificationsConfig
    logfire: LogfireSecrets

    @field_validator("exchanges", mode="after")
    @classmethod
    def validate_exchanges(
        cls, v: dict[str, ExchangeSecrets], info: ValidationInfo
    ) -> dict[str, ExchangeSecrets]:
        """Validate exchange dictionary keys are valid strings."""
        validated_exchanges: dict[str, ExchangeSecrets] = {}

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
    def validate_hyperliquid_requirements(self) -> Self:
        """
        Validate Hyperliquid-specific authentication requirements.

        For Hyperliquid exchange, either 'private_key' or 'passphrase' must be provided,
        but not both. If passphrase is provided, it must be 12 or 24 words and a valid
        BIP-39 mnemonic. If private_key is provided, it must be a valid Ethereum private key.
        """
        for exchange_name, exchange_config in self.exchanges.items():
            if exchange_name == "hyperliquid":
                hyperliquid_secrets = exchange_config

                # Check if private_key is provided and not empty
                pk_provided = False
                if hyperliquid_secrets.private_key is not None:
                    pk_provided = hyperliquid_secrets.private_key.get_secret_value().strip() != ""

                # Check if passphrase is provided and not empty
                pp_provided = False
                if hyperliquid_secrets.passphrase is not None:
                    pp_provided = hyperliquid_secrets.passphrase.get_secret_value().strip() != ""

                # Constraint 1: One of them MUST be provided
                if not pk_provided and not pp_provided:
                    raise ValueError(
                        "For Hyperliquid, either 'private_key' or 'passphrase' must be provided."
                    )

                # Constraint 2: NOT BOTH can be provided
                if pk_provided and pp_provided:
                    raise ValueError(
                        "For Hyperliquid, provide 'private_key' OR 'passphrase', not both."
                    )

                # Constraint 3: Private key cryptographic validation
                if pk_provided and hyperliquid_secrets.private_key is not None:
                    pk_str = hyperliquid_secrets.private_key.get_secret_value()

                    # Strip "0x" prefix if present
                    processed_pk_str = pk_str[2:] if pk_str.startswith("0x") else pk_str

                    # Validate format (64-character hex string)
                    if not (
                        len(processed_pk_str) == 64
                        and all(c in "0123456789abcdefABCDEF" for c in processed_pk_str)
                    ):
                        raise ValueError(
                            "Hyperliquid private_key must be a 64-character hex string "
                            "(with or without '0x' prefix)."
                        )

                    # Cryptographic validation using eth_account
                    try:
                        Account.from_key(processed_pk_str)
                    except Exception as e:
                        raise ValueError(
                            f"Hyperliquid private_key is not cryptographically valid: {e}"
                        ) from e

                # Constraint 4: Passphrase cryptographic validation
                if pp_provided and hyperliquid_secrets.passphrase is not None:
                    phrase_str = hyperliquid_secrets.passphrase.get_secret_value()

                    # Word count check (12 or 24 words)
                    num_words = len(phrase_str.split())
                    if num_words not in (12, 24):
                        raise ValueError(
                            f"Hyperliquid passphrase must consist of 12 or 24 words, "
                            f"got {num_words} words."
                        )

                    # BIP-39 mnemonic validation
                    try:
                        mnemonic_validator = Mnemonic("english")
                        if not mnemonic_validator.check(phrase_str):
                            raise ValueError(
                                "Hyperliquid passphrase is not a valid BIP-39 mnemonic "
                                "(checksum or wordlist error)."
                            )
                    except Exception as e:
                        # Handle any other exceptions from mnemonic validation
                        if "not a valid BIP-39 mnemonic" not in str(e):
                            raise ValueError(
                                f"Error validating Hyperliquid passphrase "
                                f"with mnemonic library: {e}"
                            ) from e
                        raise

        return self

    model_config = ConfigDict(extra="forbid", frozen=True)

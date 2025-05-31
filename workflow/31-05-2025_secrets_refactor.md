**Prompt for AI Coder (Angel): Refactor Exchange Secrets with Discriminated Union and Simplified Auth Types**

**Project:** CyberDeltaEngine
**Context:**
Our `ExchangeSecrets` Pydantic model in `secrets_models.py` currently mandates `api_key` and `api_secret` for all exchanges. This is incompatible with exchanges like Hyperliquid that use private key-based authentication, causing validation errors when `api_key`/`api_secret` are omitted in `secrets.yaml`. We need to refactor this to accurately model different authentication schemes.

**Goal:**
Implement a discriminated union for `ExchangeSecrets` to support different sets of required secret fields based on the exchange's authentication mechanism. The discriminator will be `auth_type` with simple values like `"api_key"` or `"private_key"`.

**Why:**
This refactoring will:
*   Provide accurate Pydantic validation for the secrets required by each configured exchange.
*   Improve type safety and clarity for code consuming these secrets.
*   Make the secrets configuration in `secrets.yaml` more explicit and robust.
*   Allow easier extension for future exchanges with different authentication needs.

**What to do (Step-by-Step):**

1.  **Define New Secret Models in `cyberdelta/config/secrets_models.py`:**
    *   **`BaseExchangeSecrets(BaseModel)`:**
        ```python
        class BaseExchangeSecrets(BaseModel):
            model_config = ConfigDict(extra="forbid", frozen=True)
        ```
    *   **`ApiKeyAuthSecrets(BaseExchangeSecrets)`:** For exchanges using API key/secret pairs (this will now cover Backpack's ED25519 keys using these field names, and traditional API key pairs).
        ```python
        from pydantic import SecretStr # Ensure SecretStr is imported

        class ApiKeyAuthSecrets(BaseExchangeSecrets):
            auth_type: Literal["api_key"] = "api_key"
            api_key: SecretStr
            api_secret: SecretStr
        ```
    *   **`PrivateKeyAuthSecrets(BaseExchangeSecrets)`:** For exchanges using a private key (e.g., Hyperliquid).
        ```python
        class PrivateKeyAuthSecrets(BaseExchangeSecrets):
            auth_type: Literal["private_key"] = "private_key"
            private_key: SecretStr
            passphrase: SecretStr | None = Field(default=None)
        ```
    *   **Define the Discriminated Union `AnyExchangeSecrets`:**
        ```python
        from typing import Union, Annotated, Literal # Ensure all are imported
        from pydantic import Field

        AnyExchangeSecrets = Annotated[
            Union[
                ApiKeyAuthSecrets,
                PrivateKeyAuthSecrets,
            ],
            Field(discriminator="auth_type")
        ]
        ```

2.  **Update `SecretsConfig` Model:**
    *   **File:** `cyberdelta/config/secrets_models.py`
    *   **Change:** Modify the `exchanges` field:
        ```python
        # Old: exchanges: dict[str, ExchangeSecrets]
        # New:
        exchanges: dict[str, AnyExchangeSecrets]
        ```
    *   **Remove/Replace Old Model:** Delete the old `ExchangeSecrets` model definition.
    *   **Adapt `@model_validator validate_hyperliquid_requirements`:**
        *   This validator in `SecretsConfig` checked for `private_key` or `passphrase` for Hyperliquid and warned about unused `api_key`/`secret`.
        *   The presence of `private_key` for Hyperliquid is now enforced by `HyperliquidSecrets` model if we were using a specific model for it. Since we are using `PrivateKeyAuthSecrets` and identifying Hyperliquid by the `exchange_name` key in the `exchanges` dict, the logic would be:
            ```python
            @model_validator(mode="after")
            def validate_exchange_specific_secret_configurations(self) -> "SecretsConfig":
                for exchange_name, secrets_config_item in self.exchanges.items():
                    if exchange_name == "hyperliquid":
                        if not isinstance(secrets_config_item, PrivateKeyAuthSecrets):
                            raise ValueError(
                                f"Hyperliquid configuration in secrets must have auth_type 'private_key' and corresponding fields."
                            )
                        # Cryptographic validation of private_key/passphrase for Hyperliquid should be moved
                        # to HyperliquidAPIComponentsFactory or HyperliquidEip712Authenticator constructor.
                        # For example, the check for valid hex or BIP-39.
                        # Here, we just ensure the correct model type is used.
                        pk_val = secrets_config_item.private_key.get_secret_value()
                        if not pk_val or not pk_val.strip():
                             raise ValueError("Hyperliquid 'private_key' cannot be empty in secrets.")

                    elif exchange_name == "backpack":
                        if not isinstance(secrets_config_item, ApiKeyAuthSecrets):
                            raise ValueError(
                                f"Backpack configuration in secrets must have auth_type 'api_key' and corresponding fields."
                            )
                        # Basic presence checks for Backpack (ED25519 keys)
                        api_key_val = secrets_config_item.api_key.get_secret_value()
                        api_secret_val = secrets_config_item.api_secret.get_secret_value()
                        if not (api_key_val and api_key_val.strip()):
                            raise ValueError("Backpack 'api_key' (ED25519 Public Key) cannot be empty.")
                        if not (api_secret_val and api_secret_val.strip()):
                            raise ValueError("Backpack 'api_secret' (ED25519 Private Key) cannot be empty.")
                        # Deeper crypto validation (is it valid base64 ED25519) can be in BackpackAPIComponentsFactory.
                return self
            ```

3.  **Update `secrets.yaml.example`:**
    *   Modify the example to reflect the new structure with `auth_type`:
        ```yaml
        exchanges:
          hyperliquid:
            auth_type: "private_key"
            private_key: "YOUR_HYPERLIQUID_WALLET_PRIVATE_KEY_0x..."
            # passphrase: "YOUR_HYPERLIQUID_PASSPHRASE_IF_ANY" # Optional
          backpack:
            auth_type: "api_key" # Backpack uses its ED25519 keys here
            api_key: "YOUR_BACKPACK_ED25519_PUBLIC_KEY_B64"
            api_secret: "YOUR_BACKPACK_ED25519_PRIVATE_KEY_B64"
          # Example for a traditional API key/secret exchange (if added later)
          # some_other_exchange:
          #   auth_type: "api_key"
          #   api_key: "TRADITIONAL_API_KEY"
          #   api_secret: "TRADITIONAL_API_SECRET"
        ```

4.  **Refactor `BackpackAPIComponentsFactory`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api_components_factory.py`
    *   **Imports:** `from cyberdelta.config.secrets_models import ApiKeyAuthSecrets, AnyExchangeSecrets`
    *   **Method:** `__init__`'s `exchange_secrets` parameter should now be `AnyExchangeSecrets`.
    *   **Method:** `create_authenticator` (and any other method using `self.exchange_secrets` for API keys/secrets):
        *   Check `isinstance(self.exchange_secrets, ApiKeyAuthSecrets)`.
        *   If true, access `self.exchange_secrets.api_key` and `self.exchange_secrets.api_secret` (knowing these are the ED25519 pub/priv keys for Backpack).
        *   If false, log an error or raise `ConfigurationError` as Backpack expects `auth_type: "api_key"` with ED25519 keys in those fields.
        *   Update `self._api_key` and `self._api_secret` initialization in the factory's `__init__` accordingly.

5.  **Refactor `HyperliquidAPIComponentsFactory`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api_components_factory.py`
    *   **Imports:** `from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets, AnyExchangeSecrets`
    *   **Method:** `__init__`'s `exchange_secrets` parameter should now be `AnyExchangeSecrets`.
    *   **Method:** `create_authenticator` (and other methods):
        *   Check `isinstance(self.exchange_secrets, PrivateKeyAuthSecrets)`.
        *   If true, access `self.exchange_secrets.private_key` and `self.exchange_secrets.passphrase`.
        *   If false, log an error or raise `ConfigurationError`.
        *   Update `self._private_key` and `self._passphrase` initialization in the factory's `__init__`.
        *   **Move cryptographic validation** of `private_key` (is it valid hex, can `Account.from_key` use it?) and `passphrase` (BIP-39) from `SecretsConfig` model validator into this factory's `create_authenticator` method or directly into `HyperliquidEip712Authenticator.__init__`. This is where the secret is actually *used* to create an auth component.

6.  **Testing Requirements:**
    *   Update unit tests for `SecretsConfig` with various valid and invalid `exchanges` structures using the new `auth_type` and specific models.
    *   Test scenarios where `auth_type` is missing or mismatched with the provided fields.
    *   Update unit tests for `BackpackAPIComponentsFactory` and `HyperliquidAPIComponentsFactory` to mock/provide the `AnyExchangeSecrets` union correctly (e.g., providing a `PrivateKeyAuthSecrets` instance when testing Hyperliquid factory).
    *   Ensure tests for authenticator creation pass with the new secrets structure.
    *   **Crucially, ensure local `secrets.yaml` used for testing is updated to the new format so that running the script like `python scripts/data_collection/fix_backpack_time_endpoint.py` now passes the secrets validation stage.**

7.  **Static Analysis and Reporting:**
    *   Run static analysis (Mypy, Pylint, Ruff) after changes.
    *   List all files modified.
    *   Confirm all relevant tests pass, including successful loading of a correctly formatted `secrets.yaml`.


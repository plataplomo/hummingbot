
**Prompt for AI Coder (Angel): Relocate Cryptographic Secrets Validation to Authenticator**

**Project:** CyberDeltaEngine
**Context:**
Currently, the `HyperliquidAPIComponentsFactory.create_authenticator()` method performs cryptographic validation of the Hyperliquid private key (hex format, `eth_account` loadability) and passphrase (BIP-39 mnemonic). This makes the factory responsible for more than just component assembly.

**Goal:**
1.  Move the cryptographic validation logic for `private_key` and `passphrase` from `HyperliquidAPIComponentsFactory.create_authenticator()` into the `HyperliquidEip712Authenticator.__init__` method.
2.  The factory will now pass the `SecretStr` objects for `private_key` and `passphrase` directly to the `HyperliquidEip712Authenticator` constructor.
3.  `HyperliquidEip712Authenticator.__init__` will perform `get_secret_value()`, stripping, and all cryptographic checks (hex format, `Account.from_key`, BIP-39 validation). It should raise `ValueError` if these checks fail.
4.  `HyperliquidAPIComponentsFactory.create_authenticator()` will catch `ValueError` from the authenticator's constructor and handle it (log error, return `None`).

**Why:**
*   **Improved Separation of Concerns:** The component that directly uses the cryptographic material (`HyperliquidEip712Authenticator`) becomes responsible for validating its cryptographic integrity and usability.
*   **Cleaner Factory:** The `HyperliquidAPIComponentsFactory` focuses on component instantiation and dependency injection, relying on its created components to validate their specific operational inputs.
*   **Robustness:** Validation still occurs early (during authenticator creation), preventing issues later.

**What to do (Step-by-Step):**

1.  **Modify `HyperliquidEip712Authenticator.__init__`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_auth.py`
    *   **Change Signature:**
        ```python
        def __init__(
            self,
            *,
            wallet_private_key_secret: SecretStr, # Expect SecretStr
            chain_id: int,
            account_object: LocalAccount | None = None, # Keep this alternative init path
            passphrase_secret: SecretStr | None = None, # Expect SecretStr
            logger_param: logging.Logger | None = None,
        ) -> None:
        ```
    *   **Add Validation Logic:**
        *   Inside `__init__`, if `account_object` is not provided:
            *   Call `wallet_private_key_secret.get_secret_value()`.
            *   Perform all the hex format validation, stripping "0x", length check, and `Account.from_key()` trial that is currently in `HyperliquidAPIComponentsFactory`. Raise `ValueError` with a descriptive message if any check fails.
            *   If `passphrase_secret` is provided, get its value and perform the BIP-39 word count and validity checks currently in the factory. Raise `ValueError` on failure.
        *   If `account_object` *is* provided, these secret validations are skipped (as the account object is already formed).

2.  **Simplify `HyperliquidAPIComponentsFactory.create_authenticator`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_api_components_factory.py`
    *   **Remove Validation Logic:** Delete the detailed cryptographic validation code for `private_key` and `passphrase` from this method.
    *   **Update Instantiation:**
        ```python
        # ... inside create_authenticator, after checking isinstance(self.exchange_secrets, PrivateKeyAuthSecrets) ...
        secrets: PrivateKeyAuthSecrets = self.exchange_secrets # Type cast for clarity

        if secrets.private_key:
            try:
                return HyperliquidEip712Authenticator(
                    wallet_private_key_secret=secrets.private_key, # Pass SecretStr
                    passphrase_secret=secrets.passphrase,         # Pass SecretStr or None
                    chain_id=self.chain_id,
                )
            except ValueError as e: # Catch init errors from Authenticator
                logger.error(f"Failed to initialize HyperliquidEip712Authenticator: {e}")
                return None
        elif secrets.passphrase: # This branch might be mostly for if only passphrase was a valid path
            logger.error("Hyperliquid authentication via passphrase only is not fully supported for direct authenticator creation without a private key from passphrase derivation being implemented here.")
            # Or, if passphrase was meant to be used WITH a private key from a keystore (not current design)
            return None # Keep consistent with current factory logic for passphrase-only
        else:
            logger.warning("Hyperliquid secrets provided but missing private_key. Cannot create authenticator.")
            return None
        ```

3.  **Testing Requirements:**
    *   **Update `HyperliquidEip712Authenticator` Tests:**
        *   Add tests to verify that its `__init__` method correctly validates (and rejects invalid) `private_key` and `passphrase` `SecretStr` inputs. Test various failure modes (bad hex, wrong length, invalid mnemonic).
    *   **Update `HyperliquidAPIComponentsFactory` Tests:**
        *   Ensure tests verify that the factory correctly passes `SecretStr` objects to the authenticator.
        *   Test that the factory handles `ValueError` from the authenticator's constructor gracefully (logs error, returns `None`).
    *   Ensure overall application startup tests (if any that involve API client creation) still pass with valid secrets.

4.  **Static Analysis and Reporting:**
    *   Run static analysis.
    *   List modified files.
    *   Confirm tests pass.

----

**Prompt for AI Coder (Angel): Relocate Backpack ED25519 Secrets Validation to Authenticator**

**Project:** CyberDeltaEngine
**Context:**
Similar to the Hyperliquid authenticator refactoring, the `BackpackAPIComponentsFactory.create_authenticator()` method currently performs the base64 decoding and cryptographic loading of Backpack's ED25519 keys. We want to move this validation to the `BackpackEd25519Authenticator` itself.

**Goal:**
1.  Move the logic for base64 decoding `api_key_b64` (public key) and `private_key_b64` (private key), and loading the private key using `Ed25519PrivateKey.from_private_bytes()`, from `BackpackAPIComponentsFactory.create_authenticator()` into the `BackpackEd25519Authenticator.__init__` method.
2.  The factory will now pass the `SecretStr` objects for `api_key` (containing base64 public key) and `api_secret` (containing base64 private key) directly to the `BackpackEd25519Authenticator` constructor.
3.  `BackpackEd25519Authenticator.__init__` will perform `get_secret_value()`, base64 decoding, and cryptographic key loading. It should raise `ValueError` if these checks fail (e.g., invalid base64, invalid ED25519 key bytes).
4.  `BackpackAPIComponentsFactory.create_authenticator()` will catch `ValueError` from the authenticator's constructor and handle it (log error, return `None`).

**Why:**
*   **Improved Separation of Concerns:** The `BackpackEd25519Authenticator` becomes responsible for validating the integrity and usability of the cryptographic keys it needs.
*   **Cleaner Factory:** The `BackpackAPIComponentsFactory` focuses on component instantiation.
*   **Robustness:** Validation still occurs early during authenticator creation.

**What to do (Step-by-Step):**

1.  **Modify `BackpackEd25519Authenticator.__init__`:**
    *   **File:** `cyberdelta/apis/backpack/bp_auth.py`
    *   **Imports:** Add `import base64` and `from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey`.
    *   **Change Signature:**
        ```python
        # from pydantic import SecretStr # Assuming SecretStr will be imported if not already

        def __init__(self, api_key_b64_secret: SecretStr, private_key_b64_secret: SecretStr) -> None:
            # Pass SecretStr objects
        ```
    *   **Add Validation Logic:**
        *   Inside `__init__`:
            *   Get secret values:
                ```python
                api_key_b64 = api_key_b64_secret.get_secret_value().strip()
                private_key_b64 = private_key_b64_secret.get_secret_value().strip()
                ```
            *   Perform existing checks (e.g., not empty):
                ```python
                if not api_key_b64:
                    raise ValueError("API key (Base64 public ED25519 key) cannot be empty")
                if not private_key_b64:
                    raise ValueError("Private key (Base64 private ED25519 key) cannot be empty")
                self._api_key_b64 = api_key_b64 # Store the public key string
                ```
            *   Move the private key decoding and loading logic here:
                ```python
                try:
                    private_key_bytes = base64.b64decode(private_key_b64)
                    self._ed25519_private_key = Ed25519PrivateKey.from_private_bytes(private_key_bytes)
                except Exception as e:
                    logger.error(f"Failed to load ED25519 private key from Base64 string: {e}")
                    raise ValueError(f"Invalid Base64 ED25519 private key: {e}") from e
                ```
            *   The rest of `__init__` (like setting up `INSTRUCTION_MAP`) remains.

2.  **Simplify `BackpackAPIComponentsFactory.create_authenticator`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api_components_factory.py`
    *   **Imports:** `from cyberdelta.config.secrets_models import ApiKeyAuthSecrets` (if not already there, ensure it's the correct model name we decided on, which uses `auth_type: "api_key"` and `api_key`/`api_secret` fields for Backpack's ED25519 keys).
    *   **Remove Validation Logic:** Delete the `base64.b64decode` and `Ed25519PrivateKey.from_private_bytes` logic from this method.
    *   **Update Instantiation:**
        ```python
        # ... inside create_authenticator, after checking isinstance(self.exchange_secrets, ApiKeyAuthSecrets) ...
        # And after retrieving self._api_key and self._api_secret from self.exchange_secrets.api_key and .api_secret
        # Note: self._api_key and self._api_secret in the factory are string values.
        # We need to pass the SecretStr objects from self.exchange_secrets.

        secrets: ApiKeyAuthSecrets = self.exchange_secrets # Type cast for clarity

        if secrets.api_key and secrets.api_secret: # Check if SecretStr objects themselves exist
            logger.info("Attempting to create BackpackEd25519Authenticator (ED25519 authentication)")
            try:
                return BackpackEd25519Authenticator(
                    api_key_b64_secret=secrets.api_key,         # Pass SecretStr for public key
                    private_key_b64_secret=secrets.api_secret   # Pass SecretStr for private key
                )
            except ValueError as e: # Catch init errors from Authenticator
                logger.error(f"Failed to initialize BackpackEd25519Authenticator: {e}")
                return None
        else:
            logger.warning(
                "Backpack secrets (api_key or api_secret as SecretStr) not fully provided. "
                "Cannot create ED25519 authenticator."
            )
            return None
        ```

3.  **Testing Requirements:**
    *   **Update `BackpackEd25519Authenticator` Tests:**
        *   Add tests to verify that its `__init__` method correctly validates (and rejects invalid) base64 encoded keys passed as `SecretStr`. Test invalid base64, non-ED25519 key data.
    *   **Update `BackpackAPIComponentsFactory` Tests:**
        *   Ensure tests verify that the factory correctly passes the `SecretStr` objects (for `api_key` and `api_secret` fields of `ApiKeyAuthSecrets`) to the authenticator.
        *   Test that the factory handles `ValueError` from the authenticator's constructor gracefully.
    *   Ensure overall application startup tests involving the Backpack API client still pass with valid secrets.

4.  **Static Analysis and Reporting:**
    *   Run static analysis.
    *   List modified files.
    *   Confirm tests pass.

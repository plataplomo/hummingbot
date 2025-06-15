

This means `ExchangeSpecificConfig` will hold *both* mainnet and testnet URLs, and the `HyperliquidAPI` (or its factory) will select the correct set of URLs based on the `is_mainnet_environment` flag.

Let's refine the prompts for Phase 1 to reflect this full implementation.

---
**Sub-Prompt 1.1.A (Revised): Modify Pydantic Config Models for Explicit Mainnet/Testnet URL Support**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** HL_ENV_P1A_CONFIG_MODELS_EXPLICIT
**Task:** Update `ExchangeSpecificConfig` for Explicit Mainnet/Testnet URLs and Environment Flag

**1. Goal:**
Modify `cyberdelta/config/config_models.py` to enhance `ExchangeSpecificConfig`. It should store distinct URLs for mainnet and testnet environments and include a flag to determine which environment's URLs are considered "active".

**2. Why This Is Important:**
This allows the configuration to explicitly define endpoints for both mainnet and testnet, with the `HyperliquidAPI` client dynamically selecting the correct URLs based on an environment flag. This is more robust than just changing the default `api_base_url` in `config.yaml`.

**3. File to Modify:**
*   `cyberdelta/config/config_models.py`

**4. Detailed Steps & Implementation Guidance:**

   *   **Locate `ExchangeSpecificConfig` Model.**
   *   **Mainnet URL Fields (Rename if `api_base_url` currently implies active):**
        *   Ensure you have:
            *   `api_base_url_mainnet: HttpUrl = Field(..., description="Base URL for the exchange's mainnet REST API.")` (If `api_base_url` exists, rename it to this or make it the mainnet one).
            *   `ws_url_mainnet: AnyUrl = Field(..., description="Base URL for the exchange's mainnet WebSocket API.")` (Similarly for `ws_url`).
   *   **Add Testnet URL Fields:**
        *   `api_base_url_testnet: HttpUrl | None = Field(default=None, description="Optional base URL for the exchange's testnet REST API.")`
        *   `ws_url_testnet: AnyUrl | None = Field(default=None, description="Optional base URL for the exchange's testnet WebSocket API.")`
   *   **Add `is_mainnet_environment` Flag:**
        *   `is_mainnet_environment: bool = Field(default=True, description="If True, mainnet URLs are used. If False, testnet URLs are used (if provided).")`
   *   **`chain_id` Field:**
        *   Keep `chain_id: int | None`. For Hyperliquid, this will be `1337`. The `is_mainnet_environment` flag will control the `source` byte in their EIP-712 signing, not this `chain_id`.
   *   **Deprecate/Remove Old Fields (If Renaming):** If you renamed `api_base_url` to `api_base_url_mainnet`, ensure the old `api_base_url` is removed to avoid confusion. (Pydantic's validation will likely catch this if you rename and don't update `config.yaml` immediately, which is good).

**5. Testing Requirements:**
*   Test `ExchangeSpecificConfig` instantiation:
    *   With only mainnet URLs and `is_mainnet_environment=True`.
    *   With mainnet and testnet URLs, and `is_mainnet_environment=True`.
    *   With mainnet and testnet URLs, and `is_mainnet_environment=False`.
    *   With only mainnet URLs, and `is_mainnet_environment=False` (should this be an error, or imply mainnet is used as fallback? Decide and test. Prefer explicit error if testnet URLs are expected but missing when `is_mainnet_environment=False`).
    *   Ensure it fails if `is_mainnet_environment=False` but `api_base_url_testnet` is not provided (for exchanges that have a testnet).

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, Model Architecture V1.
```

---
**Sub-Prompt 1.1.B: Modify Pydantic Secrets Models (No Change from Previous)**
This prompt for `PrivateKeyAuthSecrets` and `testnet_seed_passphrase` remains the same as it's already well-defined for optional testnet-specific credentials.
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** HL_ENV_P1B_SECRETS_MODELS_UNCHANGED
**Task:** Update `PrivateKeyAuthSecrets` for Optional Testnet Seed Passphrase

**1. Goal:**
Modify `cyberdelta/config/secrets_models.py` to enhance `PrivateKeyAuthSecrets` by adding an optional `testnet_seed_passphrase`.

**2. Why This Is Important:**
Allows specifying a separate seed passphrase for testnet environments, enabling derivation of multiple test wallets for Hyperliquid testnet.

**3. File to Modify:**
*   `cyberdelta/config/secrets_models.py`

**4. Detailed Steps & Implementation Guidance:**

   *   Locate `PrivateKeyAuthSecrets` Model.
   *   Add `testnet_seed_passphrase: SecretStr | None = Field(default=None, description="Optional BIP-39 seed passphrase for testnet wallets.")`

**5. Testing Requirements:**
*   Verify `PrivateKeyAuthSecrets` instantiation with and without `testnet_seed_passphrase`.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, Model Architecture V1.
```

---
**Sub-Prompt 1.1.C (Revised): Update Example Configuration Files for Explicit URLs**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** HL_ENV_P1C_EXAMPLE_CONFIGS_EXPLICIT
**Task:** Update Example `config.yaml` and `secrets.yaml` for Explicit Mainnet/Testnet URLs

**1. Goal:**
Update `cyberdelta/config/config.yaml.example` and `cyberdelta/config/secrets.yaml.example` to reflect the new `ExchangeSpecificConfig` fields. The `config.yaml` should set Hyperliquid to use testnet by default by setting `is_mainnet_environment: false` and providing the testnet URLs in the `api_base_url_testnet` and `ws_url_testnet` fields, while also having the mainnet URLs present in `api_base_url_mainnet` and `ws_url_mainnet`.

**2. Why This Is Important:**
Example files must showcase the new explicit URL structure and demonstrate configuring Hyperliquid for testnet as the default operational mode for current development.

**3. Files to Modify:**
*   `cyberdelta/config/config.yaml.example` (and instruct Human Lead to update their actual `config.yaml`)
*   `cyberdelta/config/secrets.yaml.example`

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Update `config.yaml.example` (and ensure active `config.yaml` is updated accordingly):**
      *   Locate the `hyperliquid` section under `exchanges`.
      *   **Set `is_mainnet_environment`:**
          *   `is_mainnet_environment: false`  (This makes testnet active by default if testnet URLs are present)
      *   **Populate Mainnet URLs:**
          *   `api_base_url_mainnet: "https://api.hyperliquid.xyz"`
          *   `ws_url_mainnet: "wss://api.hyperliquid.xyz/ws"`
      *   **Populate Testnet URLs:**
          *   `api_base_url_testnet: "https://api.hyperliquid-testnet.xyz"`
          *   `ws_url_testnet: "wss://api.hyperliquid-testnet.xyz/ws"`
      *   **Remove/Clarify old `api_base_url` / `ws_url`:**
            *   The fields `api_base_url` and `ws_url` in `ExchangeSpecificConfig` no longer exist if they were renamed to `*_mainnet`. Ensure `config.yaml` reflects this. If they were kept and now represent the *active* URL, this step is different (but renaming to `*_mainnet` is cleaner). *Assuming they were renamed based on P1A.*
      *   **`chain_id`:** Keep `chain_id: 1337`.
      *   Add comments explaining `is_mainnet_environment` and the presence of both sets of URLs.

   **4.2. Update `secrets.yaml.example`:**
      *   (No change from previous Sub-Prompt 1.1.B for this file - it already includes `testnet_seed_passphrase` as optional.)

**5. Testing Requirements:**
*   Manual review by Human Lead.
*   Human Lead updates their actual `config.yaml`.

**6. Project Rules Adherence:**
*   Documentation clarity.
```

---
**Sub-Prompt 1.1.D (Revised): Refactor HyperliquidAPI and Factory for Explicit URL Selection**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** HL_ENV_P1D_API_FACTORY_URL_SELECT
**Task:** Refactor `HyperliquidAPI` and Factory for Explicit Mainnet/Testnet URL Selection and Authenticator Configuration

**1. Goal:**
Modify `HyperliquidAPI.__init__`, `HyperliquidAPIComponentsFactory.__init__`, and `HyperliquidEip712Authenticator.__init__` to:
1.  Correctly select mainnet or testnet URLs in `HyperliquidAPI` based on `exchange_config.is_mainnet_environment` and the new `api_base_url_mainnet/testnet` fields.
2.  Ensure the `HyperliquidEip712Authenticator` is initialized with the `is_mainnet_environment` flag to control signing behavior.
3.  (Optional Stretch) Implement logic in `HyperliquidAPIComponentsFactory.create_authenticator` to use `secrets.testnet_seed_passphrase` for deriving testnet accounts if `is_mainnet_environment` is false and the passphrase is provided.

**2. Why This Is Important:**
The API client must use the correct URLs and signing parameters for the targeted Hyperliquid environment (mainnet or testnet), as determined by the configuration.

**3. Files to Modify:**
*   `cyberdelta/apis/hyperliquid/hl_api.py`
*   `cyberdelta/apis/hyperliquid/hl_api_components_factory.py`
*   `cyberdelta/apis/hyperliquid/hl_auth.py`

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Modify `HyperliquidEip712Authenticator.__init__` (in `hl_auth.py`):**
      *   Add `is_mainnet_environment: bool` parameter. Store as `self._is_mainnet_env`.
      *   Use `self._is_mainnet_env` for the `is_mainnet` boolean argument when calling SDK signing functions (e.g., `sign_l1_action(..., is_mainnet=self._is_mainnet_env)`).
      *   *(Optional Stretch for Passphrase Derivation - can be a separate prompt if too complex now):*
          *   If `is_mainnet_environment` is `False` and `testnet_seed_passphrase` is provided (and `wallet_private_key_secret` is `None` or less preferred for testnet), add logic to derive `self._account` from the testnet seed. This involves using `mnemonic_to_private_key` from `eth_account.hdaccount` or similar. You'd need to handle derivation paths (e.g., `m/44'/60'/0'/0/0`). This is advanced. For now, focus on passing the flag.

   **4.2. Modify `HyperliquidAPIComponentsFactory` (in `hl_api_components_factory.py`):**
      *   `__init__`: No change, it already takes `exchange_config`.
      *   `create_authenticator`:
          *   When creating `HyperliquidEip712Authenticator`, pass `is_mainnet_environment=self.exchange_config.is_mainnet_environment`.
          *   If implementing the optional stretch from 4.1:
              ```python
              # In create_authenticator, after getting secrets
              account_for_auth: LocalAccount | None = None
              pk_secret_for_auth: SecretStr | None = secrets.private_key

              if not self.exchange_config.is_mainnet_environment and secrets.testnet_seed_passphrase:
                  try:
                      # Basic BIP-39 to PK derivation (replace with actual HD account derivation if needed)
                      # from eth_account.hdaccount import Mnemonic, mnemonic_to_private_key
                      # pk_bytes = mnemonic_to_private_key(secrets.testnet_seed_passphrase.get_secret_value())
                      # account_for_auth = Account.from_key(pk_bytes)
                      # pk_secret_for_auth = None # Don't pass the main private key if using derived
                      logger.info("Using testnet seed passphrase to derive account for authenticator.")
                      # Placeholder for actual derivation - for now, let it fall through or error if pk_secret_for_auth is None and account_for_auth is None
                      pass # Needs full derivation logic
                  except Exception as e_derive:
                      logger.error(f"Failed to derive account from testnet_seed_passphrase: {e_derive}. Falling back to private_key if available.")
              
              # Then, when creating HyperliquidEip712Authenticator:
              # authenticator = HyperliquidEip712Authenticator(
              #    wallet_private_key_secret=pk_secret_for_auth, # Might be None if seed used
              #    account_object=account_for_auth,             # Might be set if seed used
              #    passphrase_secret=secrets.passphrase,
              #    chain_id=self.chain_id,
              #    is_mainnet_environment=self.exchange_config.is_mainnet_environment
              # )
              # The HyperliquidEip712Authenticator constructor needs to handle (pk_secret OR account_object).
              ```
              For now, focus on simply passing the `is_mainnet_environment` flag. The seed derivation is a significant addition.

   **4.3. Modify `HyperliquidAPI.__init__` (in `hl_api.py`):**
      *   It receives `exchange_config: ExchangeSpecificConfig`.
      *   **URL Selection Logic:**
          ```python
          if exchange_config.is_mainnet_environment:
              self.active_api_base_url = str(exchange_config.api_base_url_mainnet)
              self.active_ws_url = str(exchange_config.ws_url_mainnet) if exchange_config.ws_url_mainnet else None
              logger.info(f"[{self.exchange_name}] Initializing for MAINNET environment.")
          elif exchange_config.api_base_url_testnet: # Check if testnet URL is actually configured
              self.active_api_base_url = str(exchange_config.api_base_url_testnet)
              self.active_ws_url = str(exchange_config.ws_url_testnet) if exchange_config.ws_url_testnet else None
              logger.info(f"[{self.exchange_name}] Initializing for TESTNET environment.")
          else:
              # Fallback or error if is_mainnet_environment is False but no testnet URLs
              logger.error(
                  f"[{self.exchange_name}] Configuration error: is_mainnet_environment is False, "
                  f"but no testnet URLs (api_base_url_testnet) are provided. "
                  f"Falling back to mainnet URLs."
              )
              self.active_api_base_url = str(exchange_config.api_base_url_mainnet)
              self.active_ws_url = str(exchange_config.ws_url_mainnet) if exchange_config.ws_url_mainnet else None
          ```
      *   **Pass selected URLs to `super().__init__`:**
          *   The `config` dict passed to `super().__init__` should use these `active_api_base_url` and `active_ws_url` for its `rest_endpoint` and `ws_url` keys.
            ```python
            config_dict_for_super = {
                "exchange_name": exchange_config.exchange_name.value,
                "rest_endpoint": self.active_api_base_url, # Use active URL
                "ws_url": self.active_ws_url,             # Use active URL
                # ... other common config fields ...
            }
            ```
      *   The `HyperliquidAPIComponentsFactory` is initialized with `exchange_config`, so it will pass the correct `is_mainnet_environment` to the authenticator.
      *   Update `self.rest_endpoint` and `self.ws_endpoint` attributes of `HyperliquidAPI` to use `self.active_api_base_url` and `self.active_ws_url`.

**5. Testing Requirements:**
*   **`HyperliquidEip712Authenticator`:** Test it passes the `is_mainnet_environment` flag correctly to mocked SDK signers.
*   **`HyperliquidAPIComponentsFactory`:** Test `create_authenticator` passes `is_mainnet_environment` from config.
*   **`HyperliquidAPI` Instantiation:**
    *   Test with `is_mainnet_environment=True` -> uses mainnet URLs.
    *   Test with `is_mainnet_environment=False` and testnet URLs provided -> uses testnet URLs.
    *   Test with `is_mainnet_environment=False` and testnet URLs *not* provided -> verify fallback/error behavior.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
```

---
**Sub-Prompt 1.2.A (Revised): Refactor Test Fixtures for Full Environment Awareness**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** HL_ENV_P1E_TEST_FIXTURES_FULL_AWARENESS
**Task:** Refactor Test Helpers and Fixtures for Full Mainnet/Testnet Environment Control

**1. Goal:**
1.  Refactor `create_test_exchange_config` in `tests/unit/apis/hyperliquid/test_hl_api.py` to generate `ExchangeSpecificConfig` that accurately reflects *both* mainnet and testnet URLs and the `is_mainnet_environment` flag, based on a parameter.
2.  Update `tests/conftest.py` (or relevant conftest) fixtures (`active_hl_config`, `active_hl_secrets`, `hl_api_with_di`) to be fully controlled by an environment selector (e.g., `hl_test_environment` fixture defaulting to "testnet").

**2. Why This Is Important:**
Tests need to reliably instantiate `HyperliquidAPI` configured for the correct environment (mainnet or testnet), using the appropriate URLs and the `is_mainnet_environment` flag, which influences signing. This allows for targeted testing and cassette recording.

**3. Files to Modify/Create:**
*   `tests/unit/apis/hyperliquid/test_hl_api.py`
*   `tests/conftest.py` (or `tests/unit/apis/hyperliquid/conftest.py`)

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Refactor `create_test_exchange_config` (in `test_hl_api.py`):**
      *   Make it accept an optional `env_type: Literal["mainnet", "testnet"] = "testnet"` parameter.
      *   Based on `env_type`:
          *   Set `is_mainnet_environment` (`True` for "mainnet", `False` for "testnet").
      *   Populate `api_base_url_mainnet`, `ws_url_mainnet` with mainnet URLs.
      *   Populate `api_base_url_testnet`, `ws_url_testnet` with testnet URLs.
      *   The returned `ExchangeSpecificConfig` should always have both sets of URLs defined, and `is_mainnet_environment` set according to `env_type`.
      *   Example:
        ```python
        def create_test_exchange_config(
            env_type: Literal["mainnet", "testnet"] = "testnet",
            **kwargs: Any,
        ) -> ExchangeSpecificConfig:
            is_mainnet_env = env_type == "mainnet"
            config_dict = {
                "exchange_name": ExchangeName.HYPERLIQUID,
                "api_base_url_mainnet": "https://api.hyperliquid.xyz",
                "ws_url_mainnet": "wss://api.hyperliquid.xyz/ws",
                "api_base_url_testnet": "https://api.hyperliquid-testnet.xyz",
                "ws_url_testnet": "wss://api.hyperliquid-testnet.xyz/ws",
                "is_mainnet_environment": is_mainnet_env,
                "chain_id": 1337,
                # ... other necessary default config values ...
                **kwargs,
            }
            return ExchangeSpecificConfig.model_validate(config_dict)
        ```

   **4.2. Update Environment-Aware Fixtures (in `tests/conftest.py`):**
      *   **`hl_test_environment` fixture:** (No change from previous prompt) Defaults to "testnet", can be overridden by `CYBERDELTA_TEST_ENV_HL`.
      *   **`active_hl_config` fixture:**
          *   This fixture will now use the `hl_test_environment` fixture to decide how to populate the `ExchangeSpecificConfig`.
          *   It will set `is_mainnet_environment` based on `hl_test_environment`.
          *   It will always populate *both* `api_base_url_mainnet`/`ws_url_mainnet` AND `api_base_url_testnet`/`ws_url_testnet`.
            ```python
            @pytest.fixture(scope="session")
            def active_hl_config(hl_test_environment: str) -> ExchangeSpecificConfig:
                is_mainnet_env_flag = hl_test_environment == "mainnet"
                
                return ExchangeSpecificConfig.model_validate({
                    "exchange_name": ExchangeName.HYPERLIQUID,
                    "api_base_url_mainnet": "https://api.hyperliquid.xyz",
                    "ws_url_mainnet": "wss://api.hyperliquid.xyz/ws",
                    "api_base_url_testnet": "https://api.hyperliquid-testnet.xyz",
                    "ws_url_testnet": "wss://api.hyperliquid-testnet.xyz/ws",
                    "is_mainnet_environment": is_mainnet_env_flag,
                    "chain_id": 1337,
                    # ... other necessary default config values ...
                })
            ```
      *   **`active_hl_secrets` fixture:** (No change from previous, still uses `HL_TESTNET_PRIVATE_KEY` and `HL_TESTNET_SEED_PASSPHRASE` environment variables if set, otherwise placeholders).
      *   **Update `hl_api_with_di` Fixture (in `test_hl_api.py`):**
          *   Ensure it uses `active_hl_config` and `active_hl_secrets` correctly. The `HyperliquidAPI` instance created will now inherently know its target environment via `active_hl_config.is_mainnet_environment` and will select the correct active URLs internally (as per Task HL_ENV_P1D_API_FACTORY_URL_SELECT).

**5. Testing Requirements:**
*   Test `create_test_exchange_config` to ensure it correctly sets `is_mainnet_environment` and populates all four URL fields based on its `env_type` parameter.
*   Test the `active_hl_config` fixture:
    *   When `hl_test_environment` is "testnet" (default), assert `active_hl_config.is_mainnet_environment` is `False` and all URL fields are correctly populated.
    *   When `hl_test_environment` is "mainnet" (e.g., by setting `CYBERDELTA_TEST_ENV_HL=mainnet`), assert `active_hl_config.is_mainnet_environment` is `True`.
*   Modify a test using `hl_api_with_di` to assert that `api.rest_endpoint` (the active one) matches the expected testnet/mainnet URL based on `hl_test_environment`.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, Code Clarity.
*   Avoid hardcoding URLs directly in tests; rely on the fixtures.
```

This set of prompts for Phase 1 should give us a robust, configurable way to target Hyperliquid's testnet by default, while allowing for mainnet testing (likely with cassettes later) and supporting different credentials if needed. The key is that `HyperliquidAPI` itself becomes aware of the target environment through its `ExchangeSpecificConfig`.
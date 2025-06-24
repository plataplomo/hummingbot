
**Revised Plan (Focusing on Test Config Files):**

1.  **Test Configuration Files:**
    *   `tests/config/test_config.yaml`: Will store non-sensitive test configurations, like which environment (mainnet/testnet) to target for Hyperliquid by default, testnet URLs, default symbols for testing, etc.
    *   `tests/config/test_secrets.yaml`: Will store sensitive test configurations, like API keys and private keys for testnet accounts or placeholders. (An example file `test_secrets.yaml.example` will be version controlled).

2.  **Pytest Fixtures (`conftest.py`):**
    *   Fixtures will use `ConfigManager` to load settings from `tests/config/test_config.yaml`.
    *   Fixtures will use `SecretsManager` to load secrets from `tests/config/test_secrets.yaml`.
    *   These fixtures will then provide the correctly configured `ExchangeSpecificConfig` and secrets (e.g., `PrivateKeyAuthSecrets`, `ApiKeyAuthSecrets`) to the tests.

3.  **Test Code:**
    *   All tests will consume these fixtures, eliminating hardcoded values.

Let's break this down into actionable prompts for Angel.

---
**Sub-Prompt 1 (New - Test Config Structure): Create Test Configuration and Secrets Example Files**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** TEST_CONF_P1_FILES
**Task:** Create Test-Specific Configuration and Secrets Example Files

**1. Goal:**
Establish dedicated configuration and secrets files for the testing environment:
1.  Create `tests/config/test_config.yaml.example` to define non-sensitive test settings (e.g., default Hyperliquid environment, test URLs).
2.  Create `tests/config/test_secrets.yaml.example` to define the structure for test-specific secrets (e.g., testnet API keys/private keys).

**2. Why This Is Important:**
This separates test configuration from main application configuration, allowing tests to run with specific settings (like targeting testnets) without altering the main `config.yaml`. It promotes consistency by using a file-based approach similar to the main application.

**3. Files to Create:**
*   `tests/config/test_config.yaml.example`
*   `tests/config/test_secrets.yaml.example`
    (Ensure the `tests/config/` directory is created if it doesn't exist.)

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Create `tests/config/test_config.yaml.example`:**
      *   This file should mirror the structure of the main `config.yaml` but contain values suitable for testing.
      *   **Content Example:**
        ```yaml
        # tests/config/test_config.yaml.example
        # Test-specific configuration for CyberDeltaEngine
        # Copy to tests/config/test_config.yaml and customize for your test environment.

        general:
          # For testing, we might want a more verbose log level by default
          log_level: DEBUG
          # Other general settings can be minimal or specific for tests if needed

        exchanges:
          hyperliquid:
            exchange_name: "hyperliquid"
            enabled: true # Enable for testing
            # Explicitly define mainnet and testnet URLs
            api_base_url_mainnet: "https://api.hyperliquid.xyz"
            ws_url_mainnet: "wss://api.hyperliquid.xyz/ws"
            api_base_url_testnet: "https://api.hyperliquid-testnet.xyz"
            ws_url_testnet: "wss://api.hyperliquid-testnet.xyz/ws"
            # Default environment for tests using this config file
            is_mainnet_environment: false # DEFAULT TO TESTNET FOR HYPERLIQUID
            chain_id: 1337 # Constant for Hyperliquid EIP-712 domain
            symbols: {"ETH": "ETH", "BTC": "BTC", "PURP": "PURP"} # Sample test symbols
            # Test-specific rate limits (can be more lenient or specific)
            rate_limit_per_minute: 10000 # Higher for testing if needed, or specific test values
            ip_weight_limit_per_minute: 10000
            info_request_type_ip_weights: {"l2Book": 1, "allMids": 1} # Lower weights for tests
            default_info_weight: 1
            exchange_action_base_ip_weight: 1
            address_action_safety_net: {"rate_per_minute": 10000}
            websocket_send_rate_per_minute: 10000

          backpack:
            exchange_name: "backpack"
            enabled: true # Enable for testing
            api_base_url_mainnet: "https://api.backpack.exchange"
            ws_url_mainnet: "wss://ws.backpack.exchange"
            api_base_url_testnet: null # Backpack has no testnet
            ws_url_testnet: null       # Backpack has no testnet
            is_mainnet_environment: true # Always mainnet for Backpack
            chain_id: null
            rate_limit_per_minute: 10000 # Higher for testing if needed
            symbols: {"SOL_USDC": "SOL_USDC", "PYTH_USDC": "PYTH_USDC"} # Sample test symbols

          # Add other exchanges if they need specific test configurations

        # Other sections like strategies, risk, etc., can be minimal or omitted
        # if not directly needed for API client testing, or include test-specific values.
        strategies: {} # Minimal
        risk: {}       # Minimal
        execution: {}  # Minimal
        safety_systems: {} # Minimal
        monitoring: {} # Minimal
        portfolio_tracker: {} # Minimal
        ```
      *   Ensure it includes the new `api_base_url_mainnet`, `api_base_url_testnet`, `ws_url_mainnet`, `ws_url_testnet`, and `is_mainnet_environment` fields within `ExchangeSpecificConfig` structure for each relevant exchange.

   **4.2. Create `tests/config/test_secrets.yaml.example`:**
      *   This file defines the structure for secrets needed by tests.
      *   **Content Example:**
        ```yaml
        # tests/config/test_secrets.yaml.example
        # Test-specific secrets for CyberDeltaEngine
        # Copy to tests/config/test_secrets.yaml and populate with
        # placeholder values or actual TESTNET credentials.
        # DO NOT COMMIT REAL MAINNET SECRETS HERE.

        exchanges:
          hyperliquid:
            auth_type: "private_key"
            # For most unit/integration tests with cassettes, these can be placeholders.
            # For E2E tests against live testnet, a real testnet private key is needed.
            private_key: "0x0000000000000000000000000000000000000000000000000000000000000001"
            # Optional: Dedicated private key for testnet if different from above
            private_key_testnet: "0x0000000000000000000000000000000000000000000000000000000000000002"
            # Optional: Seed passphrase for deriving multiple testnet accounts
            testnet_seed_passphrase: "test test test test test test test test test test test junk"
            passphrase: null # Or your encryption passphrase if the test keys are encrypted

          backpack:
            auth_type: "api_key"
            # For most unit/integration tests with cassettes, these can be placeholders.
            # For E2E tests against live MAINNET (use with extreme caution), real keys are needed.
            api_key: "YOUR_BACKPACK_ED25519_PUBLIC_KEY_B64_PLACEHOLDER_FOR_TESTS"
            api_secret: "YOUR_BACKPACK_ED25519_PRIVATE_KEY_B64_PLACEHOLDER_FOR_TESTS"

        # Other secrets sections (notifications, logfire) can be omitted or have placeholders
        # if not used during API client testing.
        notifications:
          telegram:
            bot_token: "TELEGRAM_BOT_TOKEN_PLACEHOLDER"
            chat_id: "TELEGRAM_CHAT_ID_PLACEHOLDER"

        logfire:
          write_token: "LOGFIRE_WRITE_TOKEN_PLACEHOLDER"
        ```
      *   Include `private_key_testnet` and `testnet_seed_passphrase` for Hyperliquid.
      *   Emphasize using placeholders or dedicated testnet keys, and **never mainnet production keys**.

**5. Testing Requirements for This Task:**
*   Manually review the created example files for correctness and clarity.
*   The Human Lead will be responsible for creating their actual `tests/config/test_config.yaml` and `tests/config/test_secrets.yaml` files based on these examples, populating them with appropriate testnet details or placeholders.

**6. Project Rules Adherence:**
*   Clarity in example configurations.
```

---
**Sub-Prompt 2 (New - Test Config Fixtures): Implement Pytest Fixtures to Load Test Configurations**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** TEST_CONF_P2_FIXTURES
**Task:** Create Pytest Fixtures to Load Test-Specific `AppSettings` and `SecretsConfig`

**1. Goal:**
Implement pytest fixtures in `tests/conftest.py` (the root conftest) that use `ConfigManager` and `SecretsManager` to load configurations from `tests/config/test_config.yaml` and `tests/config/test_secrets.yaml` respectively. These fixtures will provide validated `AppSettings` and `SecretsConfig` objects for tests.

**2. Why This Is Important:**
This centralizes the loading of test configurations, making it consistent with how the main application loads its config. Tests will no longer need to manually instantiate config objects with hardcoded paths.

**3. File to Modify/Create:**
*   `tests/conftest.py` (Create if it doesn't exist, or add to it)

**4. Detailed Steps & Implementation Guidance:**

   *   **Add Imports:**
        ```python
        # In tests/conftest.py
        import pytest
        from pathlib import Path
        from cyberdelta.config import ConfigManager, SecretsManager, AppSettings, SecretsConfig, ConfigurationError
        ```
   *   **Define Path Fixtures (for clarity and reusability):**
        ```python
        @pytest.fixture(scope="session")
        def test_config_file_path() -> Path:
            # Assumes test_config.yaml is in tests/config/ relative to project root
            # Adjust path if your project structure is different or if conftest is nested deeper
            return Path(__file__).parent / "config" / "test_config.yaml"

        @pytest.fixture(scope="session")
        def test_secrets_file_path() -> Path:
            return Path(__file__).parent / "config" / "test_secrets.yaml"
        ```
        *   **Instruction for Angel:** Ensure these paths correctly point to `tests/config/test_config.yaml` and `tests/config/test_secrets.yaml` relative to the location of THIS `conftest.py` file. If `tests/conftest.py` is at the root of the `tests` directory, the paths would be `Path("tests/config/test_config.yaml")`. Adjust as necessary. For now, assume `tests/conftest.py` is at the root of the `tests` directory.

   *   **Create `test_app_settings` Fixture:**
        ```python
        @pytest.fixture(scope="session")
        def test_app_settings(test_config_file_path: Path) -> AppSettings:
            if not test_config_file_path.exists():
                pytest.skip(f"Test config file not found at {test_config_file_path}, skipping tests that need it.")
            try:
                manager = ConfigManager(str(test_config_file_path))
                if manager.settings is None: # Should be caught by ConfigManager raising ConfigurationError
                    raise ConfigurationError("ConfigManager loaded but settings are None.")
                return manager.settings
            except ConfigurationError as e:
                pytest.fail(f"Failed to load test AppSettings from {test_config_file_path}: {e}")
            # Add a default return to satisfy linters, though pytest.fail should exit
            # This path should ideally not be reached if pytest.fail works as expected.
            raise RuntimeError("test_app_settings fixture failed unexpectedly.")
        ```
   *   **Create `test_secrets_config` Fixture:**
        ```python
        @pytest.fixture(scope="session")
        def test_secrets_config(test_secrets_file_path: Path) -> SecretsConfig:
            if not test_secrets_file_path.exists():
                pytest.skip(f"Test secrets file not found at {test_secrets_file_path}, skipping tests that need it.")
            try:
                manager = SecretsManager(str(test_secrets_file_path))
                if manager.secrets_data is None: # Should be caught by SecretsManager raising ConfigurationError
                    raise ConfigurationError("SecretsManager loaded but secrets_data is None.")
                return manager.secrets_data
            except ConfigurationError as e:
                pytest.fail(f"Failed to load test SecretsConfig from {test_secrets_file_path}: {e}")
            # Add a default return to satisfy linters
            raise RuntimeError("test_secrets_config fixture failed unexpectedly.")
        ```
   *   **Environment Variable for Test Environment (Hyperliquid):**
        ```python
        @pytest.fixture(scope="session")
        def hl_test_environment(test_app_settings: AppSettings) -> str:
            # Get the default from test_config.yaml, allow override by environment variable
            # This assumes test_app_settings.exchanges['hyperliquid'].is_mainnet_environment exists
            # If not, this fixture needs to be more robust or have a simpler default.
            # For now, let's assume AppSettings loads and has this structure.
            is_mainnet_from_config = test_app_settings.exchanges.get("hyperliquid", {}).get("is_mainnet_environment", False) # Default to testnet if not in config

            # Allow override via environment variable
            env_override = os.environ.get("CYBERDELTA_TEST_ENV_HL")
            if env_override:
                return env_override.lower()
            return "mainnet" if is_mainnet_from_config else "testnet"
        ```

**5. Testing Requirements for This Task:**
*   Create a simple test (e.g., in `tests/unit/config/test_test_configs.py`) that uses the `test_app_settings` fixture and asserts that it can access a known value from `tests/config/test_config.yaml`.
*   Similarly, test the `test_secrets_config` fixture.
*   Test the `hl_test_environment` fixture by setting the `CYBERDELTA_TEST_ENV_HL` environment variable and asserting the fixture returns the correct value.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, Code Clarity.
*   Human Lead must create `tests/config/test_config.yaml` and `tests/config/test_secrets.yaml` based on the examples.
```

---
**Sub-Prompt 3 (New - Test API Client Fixtures): Implement Fixtures for API Client Instantiation**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** TEST_CONF_P3_API_FIXTURES
**Task:** Create Pytest Fixtures for Instantiating `HyperliquidAPI` and `BackpackAPI` Using Test Configurations

**1. Goal:**
Implement pytest fixtures (e.g., `active_hl_config`, `active_hl_secrets`, `hl_api_for_test_env`, `active_bp_config`, `active_bp_secrets`, `bp_api_for_test`) that provide correctly configured instances of `ExchangeSpecificConfig`, secrets, and fully initialized `HyperliquidAPI` and `BackpackAPI` clients for testing. These fixtures will use `test_app_settings` and `test_secrets_config` (from Task TEST_CONF_P2_FIXTURES).

**2. Why This Is Important:**
This provides a standardized, configuration-driven way for tests to get API client instances, ensuring they target the correct environments (testnet for HL by default) and use appropriate test credentials. It eliminates manual API client setup in individual tests.

**3. File to Modify/Create:**
*   `tests/apis/conftest.py` (Create this file if it doesn't exist, to hold fixtures common to API tests for both exchanges).
*   Or, place Hyperliquid-specific fixtures in `tests/integration/apis/hyperliquid/conftest.py` (and unit equivalents) and Backpack-specific ones in `tests/integration/apis/backpack/conftest.py` (and unit equivalents) if more fine-grained scoping is preferred. **Let's aim for placing them closest to use: `tests/integration/apis/EXCHANGE/conftest.py` and `tests/unit/apis/EXCHANGE/conftest.py`.**

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Create/Update `tests/integration/apis/hyperliquid/conftest.py`:**
      *   Add imports: `pytest`, `ExchangeSpecificConfig`, `PrivateKeyAuthSecrets`, `HyperliquidAPI`, relevant mocks.
      *   **`active_hl_config` Fixture:**
          ```python
          @pytest.fixture(scope="session")
          def active_hl_config(test_app_settings: AppSettings, hl_test_environment: str) -> ExchangeSpecificConfig:
              hl_config_from_file = test_app_settings.exchanges["hyperliquid"]
              # Override is_mainnet_environment based on hl_test_environment fixture
              # All other values (URLs, chain_id) come from test_config.yaml
              return hl_config_from_file.model_copy(
                  update={"is_mainnet_environment": hl_test_environment == "mainnet"}
              )
          ```
      *   **`active_hl_secrets` Fixture:**
          ```python
          @pytest.fixture(scope="session")
          def active_hl_secrets(test_secrets_config: SecretsConfig) -> PrivateKeyAuthSecrets:
              secrets = test_secrets_config.exchanges["hyperliquid"]
              if not isinstance(secrets, PrivateKeyAuthSecrets):
                  pytest.fail("Hyperliquid secrets in test_secrets.yaml are not PrivateKeyAuthSecrets type.")
              return secrets
          ```
      *   **`hl_api_for_test_env` Fixture (replaces/standardizes `hl_api_with_di`):**
          *   This fixture provides a `HyperliquidAPI` instance ready for integration tests (which will use cassettes). It should use real components where possible, or high-level mocks if certain parts aren't being tested directly by cassettes yet. For cassette recording, it *must* use a real `HttpClient` and `HyperliquidEip712Authenticator`.
          *   It will depend on `active_hl_config` and `active_hl_secrets`.
            ```python
            # Example structure - details depend on whether services are also mocked for some integration tests
            # For cassette tests of API methods, services should generally NOT be mocked.
            @pytest.fixture
            def hl_api_for_test_env(
                active_hl_config: ExchangeSpecificConfig,
                active_hl_secrets: PrivateKeyAuthSecrets,
                # Add specific mocks for components NOT covered by cassette if necessary,
                # but for full API integration tests, we want real components.
            ) -> HyperliquidAPI:
                # This will now use the correct URLs and is_mainnet_environment flag
                # from active_hl_config when initializing components.
                return HyperliquidAPI(
                    exchange_config=active_hl_config,
                    exchange_secrets=active_hl_secrets,
                    # Provide real or minimally mocked components as needed for cassette recording/playback
                    # error_mapper=HyperliquidErrorMapper(), # Real one
                    # request_builder=HyperliquidRequestBuilder(), # Real one
                    # response_handler=HyperliquidResponseHandler(), # Real one
                    # etc. OR rely on the factory within HyperliquidAPI to create real ones
                )
            ```

   **4.2. Create/Update `tests/integration/apis/backpack/conftest.py`:**
      *   Similar structure to Hyperliquid's, but for Backpack.
      *   **`active_bp_config` Fixture:**
          ```python
          @pytest.fixture(scope="session")
          def active_bp_config(test_app_settings: AppSettings) -> ExchangeSpecificConfig:
              # Backpack always uses its mainnet config from test_config.yaml
              return test_app_settings.exchanges["backpack"]
          ```
      *   **`active_bp_secrets` Fixture:**
          ```python
          @pytest.fixture(scope="session")
          def active_bp_secrets(test_secrets_config: SecretsConfig) -> ApiKeyAuthSecrets:
              secrets = test_secrets_config.exchanges["backpack"]
              if not isinstance(secrets, ApiKeyAuthSecrets):
                  pytest.fail("Backpack secrets in test_secrets.yaml are not ApiKeyAuthSecrets type.")
              return secrets
          ```
      *   **`bp_api_for_test_env` Fixture (replaces/standardizes `bp_api_with_di`):**
          *   Provides a `BackpackAPI` instance. Similar to `hl_api_for_test_env`, uses real components where cassettes will cover the interaction.

   **4.3. (Optional) Unit Test Fixtures:**
      *   If unit tests for API clients (in `tests/unit/apis/EXCHANGE/`) need heavily mocked API instances, they can define their own `conftest.py` or fixtures that use `active_EXCHANGE_config` but then inject many more mocks (e.g., mock `HttpClient`, mock all services). The `*_api_with_di` fixtures we discussed moving/refactoring previously would fit this pattern for unit tests.

**5. Testing Requirements for This Task:**
*   Create simple tests in `tests/integration/apis/hyperliquid/test_hl_config_fixtures.py` (new file) that use `active_hl_config` and `hl_api_for_test_env`, then assert the API instance has the correct (testnet by default) `rest_endpoint` and its authenticator is configured with `is_mainnet_environment=False`.
*   Do the same for Backpack in `tests/integration/apis/backpack/test_bp_config_fixtures.py`, asserting mainnet parameters.

**6. Project Rules Adherence:**
*   Static Analysis V3, Code Clarity.
```

---
**Sub-Prompt 4 (New - Test Code Refactoring): Update All API Tests to Use New Fixtures**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** TEST_CONF_P4_USE_FIXTURES
**Task:** Refactor All Hyperliquid and Backpack API Tests to Utilize Centralized Configuration Fixtures

**1. Goal:**
Systematically search all existing test files for `HyperliquidAPI` and `BackpackAPI` (within `tests/unit/apis/` and `tests/integration/apis/`) and refactor them to:
1.  Obtain `ExchangeSpecificConfig` instances exclusively from the `active_hl_config` or `active_bp_config` fixtures.
2.  Obtain secrets instances exclusively from the `active_hl_secrets` or `active_bp_secrets` fixtures.
3.  Obtain `HyperliquidAPI` or `BackpackAPI` instances primarily through standardized DI fixtures like `hl_api_for_test_env` or `bp_api_for_test_env` (or their unit-testing equivalents that inject more mocks, e.g., `hl_api_with_di_unit`).
4.  Remove any direct, hardcoded instantiation of `ExchangeSpecificConfig`, secrets models, or API clients with hardcoded parameters within test functions.
5.  Delete helper functions like `create_test_exchange_config` if they become entirely redundant after all tests are switched to using fixtures.

**2. Why This Is Important:**
This completes the standardization of configuration handling in our tests, ensuring all API-related tests consistently use configurations loaded from `test_config.yaml` and `test_secrets.yaml` via the new fixtures. This eliminates hardcoding, improves maintainability, and ensures tests correctly target the intended environments.

**3. Files to Modify:**
*   All `test_hl_*.py` files within `tests/unit/apis/hyperliquid/` and `tests/integration/apis/hyperliquid/`.
*   All `test_bp_*.py` files within `tests/unit/apis/backpack/` and `tests/integration/apis/backpack/`.
*   Potentially delete helper functions like `create_test_exchange_config` if they are no longer used.

**4. Detailed Steps & Implementation Guidance:**

   *   **Iterate Through Test Files:** For each relevant `test_hl_*.py` and `test_bp_*.py` file:
      *   **Identify Manual Instantiations:** Look for direct calls to `ExchangeSpecificConfig(...)`, `PrivateKeyAuthSecrets(...)`, `ApiKeyAuthSecrets(...)`, `HyperliquidAPI(...)`, `BackpackAPI(...)` where parameters like URLs, keys, or flags are hardcoded or come from local test helper functions that produce hardcoded values.
      *   **Replace with Fixture Usage:**
          *   If a test needs an `ExchangeSpecificConfig`, add `active_hl_config` or `active_bp_config` to its parameters.
          *   If a test needs secrets, add `active_hl_secrets` or `active_bp_secrets`.
          *   If a test needs an API client instance:
              *   For **integration tests**, use `hl_api_for_test_env` or `bp_api_for_test_env`.
              *   For **unit tests** (where API client is the SUT but its internal services/HTTP client are heavily mocked), ensure they use a DI fixture (like the old `hl_api_with_di` or `bp_api_with_di` which should now be defined in the unit conftest using `active_hl_config`/`active_bp_config` but injecting many mocks).
      *   **Remove Redundant Helpers:** If a helper function like `create_test_exchange_config` becomes unused after all its call sites are replaced by fixtures, delete the helper function.

**5. Testing Requirements for This Task:**
*   **Crucially, all tests in the entire test suite (`pytest tests/`) must pass after these refactorings.** This verifies that the switch to fixture-based configuration has been done correctly without altering test logic.
*   Manually review the diff for several refactored test files to confirm hardcoded values are gone and fixtures are used.

**6. Project Rules Adherence:**
*   Static Analysis V3, Code Clarity.

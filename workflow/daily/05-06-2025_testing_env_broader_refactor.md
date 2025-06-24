**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** TEST_ENV_CONFIG_STANDARDIZATION_GLOBAL
**Task:** Implement a Standardized, File-Based Configuration System for the Entire Testing Environment

**1. Goal:**
Refactor the entire testing environment (all tests under `tests/unit/` and `tests/integration/`) to eliminate hardcoded configuration values (e.g., API URLs, environment flags, placeholder secrets). Instead, tests must source these configurations from dedicated test-specific files (`tests/config/test_config.yaml` for general settings and `tests/config/test_secrets.yaml` for sensitive data), loaded via pytest fixtures using our existing `ConfigManager` and `SecretsManager` utilities.

**2. Why This Is Important (Overall Testing Environment Context):**
This is a foundational improvement for our entire testing strategy. Currently, many tests (as seen in `test_hl_market_data_public_endpoints.py` and likely elsewhere) hardcode configuration details or use ad-hoc environment variable checks. This makes tests:
    *   Brittle and hard to maintain.
    *   Inconsistent in how they handle different environments (e.g., Hyperliquid mainnet vs. testnet).
    *   Difficult to adapt for CI or for different developers' local setups.
    *   Less reflective of how the actual application consumes configuration.

By moving to a centralized, file-based test configuration system managed by fixtures, we will achieve:
    *   **Consistency:** Test configuration loading will mirror the main application.
    *   **Maintainability:** Test environment settings will be in one place.
    *   **Flexibility:** Easily switch test targets (e.g., Hyperliquid testnet by default, with the ability to override for specific mainnet cassette recording runs if needed via changes in `test_config.yaml` or environment variables that Pydantic can pick up).
    *   **Clarity:** Tests become cleaner as they receive configured components/values via fixtures.
    *   **Robustness:** This sets a solid foundation for reliable integration testing with `pytest-recording`.

**3. Scope of Refactoring:**
    *   **Configuration Models (`cyberdelta/config/`):**
        *   Ensure `ExchangeSpecificConfig` can explicitly define mainnet and testnet URLs and has a flag like `is_mainnet_environment`. It should also include computed properties (`active_api_base_url`, `active_ws_url`) to return the correct URL based on this flag. Implement necessary model validators for consistency (e.g., testnet URLs must exist if `is_mainnet_environment` is false for an exchange like Hyperliquid).
        *   Ensure `PrivateKeyAuthSecrets` in `secrets_models.py` supports optional testnet-specific credentials (e.g., `private_key_testnet`, `testnet_seed_passphrase`).
    *   **Test Configuration Files (Create under `tests/config/`):**
        *   `test_config.yaml.example`: Demonstrates the structure for non-sensitive test settings, including exchange configurations (URLs for mainnet/testnet, `is_mainnet_environment` flags). For Hyperliquid, default this to point to testnet. For Backpack, default to mainnet.
        *   `test_secrets.yaml.example`: Demonstrates the structure for test secrets, including placeholders for API keys/private keys for different environments if applicable.
    *   **Pytest Fixtures (`conftest.py` files):**
        *   Implement root-level fixtures in `tests/conftest.py` to load `AppSettings` from `tests/config/test_config.yaml` (let's call this fixture `test_app_settings`) and `SecretsConfig` from `tests/config/test_secrets.yaml` (let's call this `test_secrets_config`). These fixtures should use `ConfigManager` and `SecretsManager`.
        *   Implement exchange-specific fixtures (e.g., in `tests/apis/hyperliquid/conftest.py` and `tests/apis/backpack/conftest.py`) that derive `ExchangeSpecificConfig` and secrets (e.g., `active_hl_config`, `active_hl_secrets`, `active_bp_config`, `active_bp_secrets`) from `test_app_settings` and `test_secrets_config`.
        *   For Hyperliquid, ensure `active_hl_config` respects an environment variable (e.g., `CYBERDELTA_TEST_ENV_HL`, defaulting to "testnet") to set its `is_mainnet_environment` flag appropriately.
        *   Implement or refine DI fixtures like `hl_api_for_test_env` and `bp_api_for_test_env` that provide initialized API client instances using these "active" config and secrets fixtures.
    *   **Test Code Refactoring (All `tests/unit/**/test_*.py` and `tests/integration/**/test_*.py`):**
        *   Search all test files.
        *   Replace any hardcoded instantiation of API clients, `ExchangeSpecificConfig` objects, or secrets objects with the use of the newly created fixtures.
        *   Remove redundant test-local helper functions that were used to generate hardcoded configurations (like `simple_hl_config` in `test_hl_market_data_public_endpoints.py`).

**4. High-Level Implementation Strategy for Angel:**

   **Step 1: Model and Example File Enhancements:**
      *   Update `ExchangeSpecificConfig` with mainnet/testnet URL fields, `is_mainnet_environment`, computed active URL properties, and consistency validators.
      *   Update `PrivateKeyAuthSecrets` with optional testnet fields.
      *   Create the `test_config.yaml.example` and `test_secrets.yaml.example` files with appropriate structures, defaulting Hyperliquid to testnet.

   **Step 2: Implement Core Test Configuration Loading Fixtures:**
      *   In `tests/conftest.py`, create the `test_app_settings` and `test_secrets_config` fixtures using `ConfigManager` and `SecretsManager` pointed at the (to-be-created by Human Lead) `tests/config/test_config.yaml` and `tests/config/test_secrets.yaml`.
      *   Also, create the `hl_test_environment` fixture that determines if Hyperliquid tests should target "mainnet" or "testnet".

   **Step 3: Implement Exchange-Specific API Fixtures:**
      *   In `tests/apis/hyperliquid/conftest.py` (create if needed), define `active_hl_config`, `active_hl_secrets`, and `hl_api_for_test_env` (or refine existing `hl_api_with_di`). These should use the core fixtures from Step 2 and `hl_test_environment`.
      *   In `tests/apis/backpack/conftest.py` (create if needed), define `active_bp_config`, `active_bp_secrets`, and `bp_api_for_test_env` (or refine existing `bp_api_with_di`). These use core fixtures from Step 2.

   **Step 4: Iteratively Refactor Test Files:**
      *   Go through each test file in `tests/unit/apis/` and `tests/integration/apis/` (and their subdirectories).
      *   Replace direct configuration/client instantiation with the new fixtures.
      *   Delete helper functions like `simple_hl_config` and `simple_backpack_config` from `test_hl_market_data_public_endpoints.py` once they are no longer used.
      *   Focus first on tests for `HyperliquidAPI` and `BackpackAPI` and their services.
      *   Then, review tests for other components (mappers, models, etc.) to ensure they don't inadvertently contain hardcoded values that should come from a configuration context.

**5. Key Considerations for Angel:**
    *   **Human Lead Responsibility:** The Human Lead will create the actual `tests/config/test_config.yaml` and `tests/config/test_secrets.yaml` files from the examples you generate. Your fixtures should assume these files will exist at runtime.
    *   **Path Resolution:** Be careful with path resolutions for loading `test_config.yaml` from `tests/conftest.py`. `Path(__file__).parent` is context-dependent.
    *   **Fixture Scopes:** Use appropriate scopes for fixtures (e.g., `session` for configs that don't change, `function` or `module` for API client instances if they need to be fresh or have per-module setup).
    *   **Error Handling in Fixtures:** Fixtures loading configs should fail clearly (e.g., `pytest.fail()`) if the test config/secrets files are missing or malformed, or `pytest.skip()` if their absence is acceptable for some tests.

**6. Testing Requirements for This Task:**
*   The entire test suite must pass after these refactorings.
*   A manual review of the new `conftest.py` files and several refactored test files will be done by the Human Lead to ensure correctness and adherence to the new configuration pattern.
*   It should be demonstrable that changing `CYBERDELTA_TEST_ENV_HL` (or the default in `test_config.yaml`) correctly alters the configuration used by Hyperliquid tests.

**7. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.

**Architectural Goal for This Prerequisite Phase:**

*   All test instantiations of `HyperliquidAPI` and `BackpackAPI` (or their configurations/secrets) within the test suite (`tests/unit/**` and `tests/integration/**`) must be sourced from pytest fixtures defined in relevant `conftest.py` files.
*   These fixtures will provide `ExchangeSpecificConfig` and the appropriate secrets model (`PrivateKeyAuthSecrets` for HL, `ApiKeyAuthSecrets` for BP).
*   For Hyperliquid, these fixtures will default to providing testnet configurations (testnet URLs, `is_mainnet_environment=False`).
*   For Backpack, these fixtures will provide mainnet configurations (as it has no testnet).
*   Hardcoded URLs, chain IDs, or explicit `is_mainnet_environment` flags within test functions or test-level helper functions (like `create_test_exchange_config` if still used beyond its fixture role) must be eliminated and replaced by usage of these central fixtures.

**This makes the previous "Sub-Prompt 1.2.A (Revised): Refactor Test Fixtures for Full Environment Awareness" (Task ID: HL_ENV_P1E_TEST_FIXTURES_FULL_AWARENESS) and parts of "Sub-Prompt 2.2 (Standalone): Refactor and Relocate Hyperliquid API Integration Tests" (Task ID: BP_TEST_P2_INTEGRATION_REFACTOR_FULL, section 4.2) more global.**

Let's create a comprehensive prompt for Angel to achieve this across both Hyperliquid and Backpack tests.

---
**Prompt for Angel: Standardize API Client Test Configurations via Fixtures**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** TEST_CONFIG_STANDARDIZATION_P1_COMPLETE
**Task:** Standardize Configuration Handling in All API Tests for Hyperliquid and Backpack Using Pytest Fixtures

**1. Goal:**
Refactor the entire test suite for `HyperliquidAPI` and `BackpackAPI` (within `tests/unit/apis/` and `tests/integration/apis/`) to eliminate hardcoded configuration values. All instantiations of these API clients, their `ExchangeSpecificConfig`, or their respective secrets models within tests must be sourced from centralized pytest fixtures.
    *   For Hyperliquid, these fixtures will provide testnet-default configurations.
    *   For Backpack, these fixtures will provide mainnet configurations.

**2. Why This Is Important:**
This ensures consistency, maintainability, and flexibility in our testing environment. It allows tests to easily adapt to different environments (especially for Hyperliquid), reduces redundancy, and centralizes test configuration concerns, paving the way for robust cassette-based testing.

**3. Files/Directories to Search and Modify:**
    *   **All `test_hl_*.py` files:** Within `tests/unit/apis/hyperliquid/` and `tests/integration/apis/hyperliquid/` (and their subdirectories).
    *   **All `test_bp_*.py` files:** Within `tests/unit/apis/backpack/` and `tests/integration/apis/backpack/` (and their subdirectories).
    *   **Fixture Files:**
        *   `tests/conftest.py` (for global fixtures if any, or to define a root `hl_test_environment` selector).
        *   `tests/unit/apis/hyperliquid/conftest.py` and `tests/integration/apis/hyperliquid/conftest.py` (or a shared `tests/apis/hyperliquid/conftest.py`).
        *   `tests/unit/apis/backpack/conftest.py` and `tests/integration/apis/backpack/conftest.py` (or a shared `tests/apis/backpack/conftest.py`).

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Verify/Consolidate Hyperliquid Fixtures (from previous tasks):**
      *   **Location:** Ensure `active_hl_config` and `active_hl_secrets` fixtures (as defined in Task HL_ENV_P1E_TEST_FIXTURES_FULL_AWARENESS) are in an appropriate `conftest.py` (e.g., `tests/integration/apis/hyperliquid/conftest.py` or a shared `tests/apis/conftest.py` if unit tests also use them).
      *   **`active_hl_config`:** This fixture should return an `ExchangeSpecificConfig` for Hyperliquid. It must consult the `hl_test_environment` fixture (defaulting to "testnet") to correctly set:
          *   `is_mainnet_environment` (False for "testnet", True for "mainnet").
          *   Populate *both* `api_base_url_mainnet`/`ws_url_mainnet` AND `api_base_url_testnet`/`ws_url_testnet`.
          *   `chain_id` should be `1337`.
      *   **`active_hl_secrets`:** This fixture should return `PrivateKeyAuthSecrets`, using environment variables like `HL_TESTNET_PRIVATE_KEY` and `HL_TESTNET_SEED_PASSPHRASE` with placeholders as defaults.
      *   **`hl_api_with_di`:** Ensure this fixture (or a similarly named one that provides a fully mocked `HyperliquidAPI` instance) correctly uses `active_hl_config` and `active_hl_secrets` for its default configuration.

   **4.2. Implement/Verify Backpack Fixtures (from previous tasks):**
      *   **Location:** Ensure `active_bp_config` and `active_bp_secrets` fixtures (as defined in Task BP_TEST_P2_INTEGRATION_REFACTOR_FULL, section 4.2) are in `tests/integration/apis/backpack/conftest.py` (or a shared `tests/apis/conftest.py`).
      *   **`active_bp_config`:** This fixture should return `ExchangeSpecificConfig` for Backpack:
          *   `exchange_name: ExchangeName.BACKPACK`
          *   `api_base_url_mainnet` and `ws_url_mainnet` set to Backpack's mainnet URLs.
          *   `api_base_url_testnet` and `ws_url_testnet` set to `None`.
          *   `is_mainnet_environment: True`.
          *   `chain_id: None`.
          *   Include sensible defaults for `rate_limit_per_minute` and `symbols`.
      *   **`active_bp_secrets`:** This fixture returns `ApiKeyAuthSecrets`, using environment variables like `BP_MAINNET_API_KEY`/`SECRET` with placeholders.
      *   **`bp_api_with_di`:** Ensure this fixture for `BackpackAPI` uses `active_bp_config` and `active_bp_secrets`.

   **4.3. Refactor All Hyperliquid Test Files (`test_hl_*.py`):**
      *   Search all `test_hl_*.py` files in `tests/unit/apis/hyperliquid/` and `tests/integration/apis/hyperliquid/`.
      *   **Replace Hardcoded Config/Secrets:**
          *   Anywhere `ExchangeSpecificConfig` for Hyperliquid is created manually (e.g., via `create_test_exchange_config` or direct instantiation with hardcoded URLs/flags), replace it with the `active_hl_config` fixture.
          *   Anywhere `PrivateKeyAuthSecrets` for Hyperliquid is created manually, replace it with the `active_hl_secrets` fixture.
          *   Anywhere `HyperliquidAPI` is instantiated directly, refactor to use the `hl_api_with_di` fixture.
      *   **Remove `create_test_exchange_config`:** If the `create_test_exchange_config` helper function in `tests/unit/apis/hyperliquid/test_hl_api.py` is no longer used after these changes (because all tests now use fixtures), it can be deleted. If some very specific unit tests still need it for non-API config objects, ensure it *also* defaults to testnet parameters or is clearly parameterized.

   **4.4. Refactor All Backpack Test Files (`test_bp_*.py`):**
      *   Search all `test_bp_*.py` files in `tests/unit/apis/backpack/` and `tests/integration/apis/backpack/`.
      *   **Replace Hardcoded Config/Secrets:**
          *   Anywhere `ExchangeSpecificConfig` for Backpack is created manually, replace it with the `active_bp_config` fixture.
          *   Anywhere `ApiKeyAuthSecrets` for Backpack is created manually, replace it with the `active_bp_secrets` fixture.
          *   Anywhere `BackpackAPI` is instantiated directly, refactor to use the `bp_api_with_di` fixture (or an equivalent you guide Angel to create if it's not fully defined yet).
      *   Remove any Backpack-specific hardcoded config helper functions if they become redundant.

**5. Expected Outcome:**
    *   No hardcoded API URLs, `is_mainnet_environment` flags, or sensitive secret values directly within test functions or test-level helper functions for `HyperliquidAPI` or `BackpackAPI`.
    *   All such configurations are sourced from the new/updated pytest fixtures in `conftest.py` files.
    *   Hyperliquid tests will default to using testnet configuration.
    *   Backpack tests will use mainnet configuration.
    *   The test suite remains functional and all tests pass.

**6. Testing Requirements for This Task:**
    *   After Angel performs the refactoring, run the entire test suite (`pytest tests/`). All tests must pass.
    *   Manually review several refactored test files for both Hyperliquid and Backpack to confirm that hardcoded configurations have been replaced with fixture usage.
    *   Verify that the `active_hl_config` fixture indeed provides a testnet configuration by default (e.g., by temporarily adding an assertion or print statement in a test that uses it).

**7. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
*   Focus on replacing direct instantiations with fixture injections.

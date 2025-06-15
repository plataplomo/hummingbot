**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** BP_TEST_P2_INTEGRATION_REFACTOR_FULL
**Task:** Refactor All Backpack API Tests for Unit/Integration Separation, Fixture Usage, and Best Practices

**1. Goal:**
Systematically analyze and refactor all Backpack API test files (any `test_bp_*.py` file) currently located within the `tests/unit/apis/backpack/` directory and its subdirectories (e.g., `services/`, `mappers/`, `models/`, etc.).
The objectives for each test file are:
    a. **Identify Integration Tests:** Determine which tests are integration tests (testing interactions that would involve network calls to Backpack, even if currently mocked).
    b. **Relocate Integration Tests:** Move identified integration tests to a corresponding new structure under `tests/integration/apis/backpack/`.
    c. **Mark Integration Tests:** Mark all relocated integration tests with `@pytest.mark.integration`.
    d. **Implement and Use Configuration Fixtures:** Create Backpack-specific configuration and secrets fixtures in `tests/integration/apis/backpack/conftest.py`. Ensure tests involving `BackpackAPI` instances or `ExchangeSpecificConfig` for Backpack use these new fixtures. Since Backpack has no testnet, these fixtures will provide mainnet configuration details.
    e. **Enforce Unit Test Purity:** Ensure all tests remaining in the `tests/unit/apis/backpack/` tree are pure unit tests, with external dependencies thoroughly mocked, and adhering to the rule of not directly testing protected members.

**2. Why This Is Important:**
This refactoring extends our standardized testing structure to the Backpack API client. It ensures clear separation of test types, prepares for cassette-based testing of Backpack integration tests (which will hit live mainnet), and upholds testing principles for maintainability, reliability, and safety (given mainnet interaction).

**3. Files/Directories to Search, Modify, and Create:**
    *   **Search Scope:** All files matching `test_bp_*.py` located within `tests/unit/apis/backpack/` AND all its subdirectories.
    *   **Target Directory for Integration Tests:** Create `tests/integration/apis/backpack/` and mirror the subdirectory structure from `unit` as needed (e.g., `tests/integration/apis/backpack/services/`).
    *   **Fixture File:** Create/Update `tests/integration/apis/backpack/conftest.py` for Backpack-specific fixtures.
    *   **Reference (Global Fixtures):** `tests/conftest.py` for any truly global fixtures if needed, but prefer placing Backpack-specific fixtures closer.

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Create Base Integration Directory Structure for Backpack:**
      *   Ensure `tests/integration/apis/backpack/` exists.
      *   As you process subdirectories within `tests/unit/apis/backpack/`, create corresponding subdirectories within `tests/integration/apis/backpack/` (e.g., `services/`, `mappers/`) if they will contain moved integration tests.

   **4.2. Create Backpack-Specific Fixtures in `tests/integration/apis/backpack/conftest.py`:**
      *   Create the file `tests/integration/apis/backpack/conftest.py` if it doesn't exist.
      *   **Define `active_bp_config` Fixture:**
        ```python
        # In tests/integration/apis/backpack/conftest.py
        import pytest
        import os
        from pydantic import SecretStr
        from cyberdelta.config.config_models import ExchangeSpecificConfig, ExchangeName
        from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
        from cyberdelta.apis.backpack.bp_api import BackpackAPI
        from collections.abc import Callable # For Callable type hint
        from unittest.mock import MagicMock, AsyncMock # For mock API fixture

        @pytest.fixture(scope="session")
        def active_bp_config() -> ExchangeSpecificConfig:
            # Backpack uses mainnet config. It does not have a testnet.
            return ExchangeSpecificConfig.model_validate({
                "exchange_name": ExchangeName.BACKPACK,
                "api_base_url_mainnet": "https://api.backpack.exchange",
                "ws_url_mainnet": "wss://ws.backpack.exchange",
                "api_base_url_testnet": None, 
                "ws_url_testnet": None,
                "is_mainnet_environment": True, # Always True for Backpack
                "chain_id": None, 
                "rate_limit_per_minute": 120, 
                "symbols": {"SOL_USDC": "SOL_USDC", "BTC_USDC": "BTC_USDC"},
                # Add sensible defaults for any other required fields in ExchangeSpecificConfig
                # specific to Backpack, or ensure they are None if truly optional.
                # Example: if BackpackRateLimitStrategy required specific fields, they'd go here.
                # For SimpleTokenBucketStrategy, rate_limit_per_minute is key.
                "ip_weight_limit_per_minute": None, # Not applicable to Backpack's simple strategy
                "info_request_type_ip_weights": None, # Not applicable
                "default_info_weight": None, # Not applicable
                "exchange_action_base_ip_weight": None, # Not applicable
                "address_action_safety_net": None, # Not applicable
                "websocket_send_rate_per_minute": None, # Not applicable
            })
        ```
      *   **Define `active_bp_secrets` Fixture:**
        ```python
        @pytest.fixture(scope="session")
        def active_bp_secrets() -> ApiKeyAuthSecrets:
            # For tests, use placeholder API keys.
            # IMPORTANT: For actual E2E tests against live Backpack mainnet,
            # these secrets MUST be sourced securely (e.g., env vars, CI secrets)
            # and NOT committed. For cassette-based integration tests, placeholders
            # will be fine once requests are filtered.
            api_key = os.environ.get("BP_MAINNET_API_KEY", "B64_ENCODED_PUBLIC_KEY_PLACEHOLDER_FOR_TESTS")
            api_secret = os.environ.get("BP_MAINNET_API_SECRET", "B64_ENCODED_PRIVATE_KEY_PLACEHOLDER_FOR_TESTS")
            return ApiKeyAuthSecrets(api_key=SecretStr(api_key), api_secret=SecretStr(api_secret))
        ```
      *   **Define Backpack Mock Fixtures (similar to Hyperliquid's):**
        Create mock fixtures for all dependencies of `BackpackAPI` that are not the config/secrets. These will be used by `bp_api_with_di`.
        ```python
        @pytest.fixture
        def mock_bp_authenticator() -> MagicMock:
            # from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator # If needed for spec
            mock_auth = MagicMock() # spec=BackpackEd25519Authenticator
            mock_auth.prepare_request = AsyncMock()
            return mock_auth

        @pytest.fixture
        def mock_bp_error_mapper() -> MagicMock:
            # from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
            return MagicMock() # spec=BackpackErrorMapper

        @pytest.fixture
        def mock_bp_request_builder() -> MagicMock:
            # from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
            return MagicMock() # spec=BackpackRequestBuilder
            
        @pytest.fixture
        def mock_bp_response_handler() -> MagicMock:
            # from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
            return MagicMock() # spec=BackpackResponseHandler

        @pytest.fixture
        def mock_bp_account_data_mapper() -> MagicMock:
            # from cyberdelta.apis.backpack.mappers import BackpackAccountDataMapper
            return MagicMock() # spec=BackpackAccountDataMapper

        @pytest.fixture
        def mock_bp_market_data_mapper() -> MagicMock:
            # from cyberdelta.apis.backpack.mappers import BackpackMarketDataMapper
            return MagicMock() # spec=BackpackMarketDataMapper

        @pytest.fixture
        def mock_bp_trading_data_mapper() -> MagicMock:
            # from cyberdelta.apis.backpack.mappers import BackpackTradingDataMapper
            return MagicMock() # spec=BackpackTradingDataMapper
        
        @pytest.fixture
        def mock_bp_account_service() -> MagicMock:
            # from cyberdelta.apis.backpack.services import BackpackAccountService
            mock_service = MagicMock() # spec=BackpackAccountService
            # Add AsyncMocks for its methods if directly called by API for some reason
            # (though usually API calls service methods, which then call _request)
            return mock_service

        @pytest.fixture
        def mock_bp_market_data_service() -> MagicMock:
            # from cyberdelta.apis.backpack.services import BackpackMarketDataService
            mock_service = MagicMock() # spec=BackpackMarketDataService
            return mock_service

        @pytest.fixture
        def mock_bp_trading_service() -> MagicMock:
            # from cyberdelta.apis.backpack.services import BackpackTradingService
            mock_service = MagicMock() # spec=BackpackTradingService
            return mock_service
        ```
      *   **Define `bp_api_with_di` Fixture:**
        ```python
        @pytest.fixture
        def bp_api_with_di(
            active_bp_config: ExchangeSpecificConfig,
            active_bp_secrets: ApiKeyAuthSecrets,
            mock_bp_authenticator: MagicMock,
            mock_bp_error_mapper: MagicMock,
            mock_bp_request_builder: MagicMock,
            mock_bp_response_handler: MagicMock,
            mock_bp_account_data_mapper: MagicMock,
            mock_bp_market_data_mapper: MagicMock,
            mock_bp_trading_data_mapper: MagicMock,
            mock_bp_account_service: MagicMock,
            mock_bp_market_data_service: MagicMock,
            mock_bp_trading_service: MagicMock
        ) -> Callable[..., BackpackAPI]:
            def _create_api(
                config: ExchangeSpecificConfig | None = None,
                secrets: ApiKeyAuthSecrets | None = None,
                **overrides: MagicMock,
            ) -> BackpackAPI:
                final_config = config or active_bp_config
                final_secrets = secrets or active_bp_secrets
                
                # BackpackAPI uses a factory internally, so we pass mocks for components
                # the factory would create, or for the services themselves if API takes them directly.
                # Based on BackpackAPI.__init__, it takes components *and* services.
                return BackpackAPI(
                    exchange_config=final_config,
                    exchange_secrets=final_secrets,
                    authenticator=overrides.get("authenticator", mock_bp_authenticator),
                    error_mapper=overrides.get("error_mapper", mock_bp_error_mapper),
                    request_builder=overrides.get("request_builder", mock_bp_request_builder),
                    response_handler=overrides.get("response_handler", mock_bp_response_handler),
                    account_data_mapper=overrides.get("account_data_mapper", mock_bp_account_data_mapper),
                    market_data_mapper=overrides.get("market_data_mapper", mock_bp_market_data_mapper),
                    trading_data_mapper=overrides.get("trading_data_mapper", mock_bp_trading_data_mapper),
                    account_service=overrides.get("account_service", mock_bp_account_service),
                    market_data_service=overrides.get("market_data_service", mock_bp_market_data_service),
                    trading_service=overrides.get("trading_service", mock_bp_trading_service),
                )
            return _create_api
        ```

   **4.3. Iteratively Process Each `test_bp_*.py` File:**
      *   For every `test_bp_*.py` file found in `tests/unit/apis/backpack/` and its subdirectories:
         *   **Analyze Test Content:** Apply the same criteria as in "Task ID: HL_TEST_P2_FULL_REFACTOR" (Hyperliquid prompt) to distinguish integration vs. unit test candidates, and to identify direct protected member access.
         *   **Ensure Configuration Usage:** If a test involves `BackpackAPI` instantiation or its `ExchangeSpecificConfig`, refactor it to use the new Backpack-specific fixtures (`bp_api_with_di`, `active_bp_config`).

   **4.4. Relocate and Mark Integration Tests:**
      *   Move identified integration tests to `tests/integration/apis/backpack/` (mirroring subdirectories like `services/`, `mappers/`).
      *   If an entire test file moves, consider renaming it (e.g., `test_bp_account_service_integration.py`).
      *   Add `pytestmark = pytest.mark.integration` or `@pytest.mark.integration`.
      *   Correct all import paths.

   **4.5. Refactor and Purify Unit Tests:**
      *   For tests remaining in `tests/unit/apis/backpack/`:
          *   **Protected Member Refactoring:** Refactor tests directly calling protected members to use public interfaces or flag for Human Lead review (with a `TODO`) if direct refactoring is not straightforward. **No unit test should directly test protected members.**
          *   **Mocking:** Ensure all external dependencies are robustly mocked (especially `_http_client_requester` in service unit tests, or service methods in `BackpackAPI` unit tests).

**5. Expected Outcome:**
    *   The `tests/unit/apis/backpack/` tree contains *only* pure unit tests.
    *   The `tests/integration/apis/backpack/` tree contains all identified Backpack integration tests, correctly marked and using the new fixtures.
    *   All tests pass.
    *   The `tests/integration/apis/backpack/conftest.py` contains the new Backpack-specific fixtures.

**6. Testing Requirements for This Refactoring Task:**
    *   After refactoring, all tests (`pytest tests/`) must pass.
    *   Manually review a sample of moved (integration) and remaining (unit) Backpack test files for correctness in categorization, marking, fixture usage, and adherence to the protected member rule.
    *   Verify pytest marker filtering (`-m integration` and `-m "not integration"`) works correctly for Backpack tests.

**7. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
*   Strict adherence to: **Unit tests must not directly call or assert against protected members.**
*   Careful management of import paths when moving files.

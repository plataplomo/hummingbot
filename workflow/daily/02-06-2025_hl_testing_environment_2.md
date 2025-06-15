

**Overall Goal for Phase 2:** Systematically refactor all Hyperliquid API tests (`test_hl_*.py`) to be environment-aware (defaulting to testnet), separate true integration tests into a new `tests/integration/apis/hyperliquid/` structure, mark them accordingly, and ensure unit tests in `tests/unit/apis/hyperliquid/` are pure, respecting encapsulation by not directly testing protected members.

---

**Sub-Prompt 2.1: Setup Integration Test Directory and Initial Analysis**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** HL_TEST_P2A_SETUP_ANALYSIS
**Task:** Initial Setup for Hyperliquid Integration Tests and Analysis of `test_hl_api.py`

**1. Goal:**
1.  Create the directory structure for Hyperliquid integration tests: `tests/integration/apis/hyperliquid/`.
2.  Analyze the existing `tests/unit/apis/hyperliquid/test_hl_api.py` file. For each test class and test function within it:
    *   Determine if it primarily tests the public interface of `HyperliquidAPI` in a way that would involve network calls (these are integration test candidates).
    *   Determine if it's a pure unit test focusing on isolated logic or sub-components (e.g., testing `HyperliquidAPI` initialization with different mock configurations without triggering network-bound methods, or testing helper functions if any).
    *   Identify any tests directly calling protected members (e.g., `api._some_method()`).
3.  Propose which test classes/functions from `test_hl_api.py` should be moved to integration tests and which can remain as unit tests (after potential refactoring if they test protected members).

**2. Why This Is Important:**
This initial setup and analysis lays the groundwork for cleanly separating integration tests from unit tests and ensures we identify tests needing refactoring based on our testing principles.

**3. Files/Directories to Interact With:**
*   **Create:** `tests/integration/apis/hyperliquid/`
*   **Analyze:** `tests/unit/apis/hyperliquid/test_hl_api.py`
*   **Reference (for fixture understanding):** `tests/conftest.py` (or relevant conftest providing `hl_api_with_di`, `active_hl_config`, etc.)

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Create Directory:**
      *   Ensure the directory `tests/integration/apis/hyperliquid/` is created.

   **4.2. Analyze `test_hl_api.py`:**
      *   Read and understand each test class and test function.
      *   **For each test, determine its nature:**
          *   **Integration Candidate:** If it calls public methods of `HyperliquidAPI` like `get_ticker`, `place_order`, `get_balances`, `get_positions`, `get_open_orders`, `get_order_history`, `get_trade_history`, `get_market_data`, `cancel_order`, etc., that inherently imply network interaction. This includes tests that currently use `mock_hl_http_client.request` or service-level mocks for these operations.
          *   **Unit Test Candidate:** If it tests `HyperliquidAPI` initialization logic, configuration handling, or internal helper functions that do *not* make external calls, or if it tests methods that are fully self-contained after all external dependencies are mocked out *at a high level* (e.g., mocking entire service calls).
          *   **Protected Member Usage:** Specifically flag any test that directly calls methods like `api._request`, `api._route_ws_message`, `api._construct_subscription_payload`, or accesses attributes like `api._ws_handlers`.
   **4.3. Output of Analysis:**
      *   Provide a report listing:
          *   Test classes/functions from `test_hl_api.py` recommended to be **moved to integration tests**.
          *   Test classes/functions from `test_hl_api.py` recommended to **remain as unit tests** (note if they need refactoring due to protected member access).
          *   A list of any tests directly accessing protected members.

**5. Testing Requirements for This Task:**
*   None for Angel directly. Human Lead will review Angel's analysis.

**6. Project Rules Adherence:**
*   This task is analytical. Adhere to clarity in reporting findings.
```

---
**Sub-Prompt 2.2: Refactor and Relocate Integration Tests from `test_hl_api.py`**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** HL_TEST_P2B_REFACTOR_TEST_HL_API
**Task:** Refactor and Relocate Integration Tests from `tests/unit/apis/hyperliquid/test_hl_api.py`

**1. Goal:**
Based on the analysis from Task HL_TEST_P2A_SETUP_ANALYSIS:
1.  Move the identified integration test classes/functions from `tests/unit/apis/hyperliquid/test_hl_api.py` to a new file: `tests/integration/apis/hyperliquid/test_hl_api_integration.py`.
2.  Ensure all moved tests correctly use the environment-aware fixtures (e.g., `hl_api_with_di` which uses `active_hl_config` and `active_hl_secrets`).
3.  Mark all tests in the new integration file with `@pytest.mark.integration`.
4.  Refactor any tests that remained in `tests/unit/apis/hyperliquid/test_hl_api.py` that were directly testing protected members to instead test through public interfaces, or if not possible, mark them for Human Lead review for potential code refactoring of the SUT. If refactoring the test is not straightforward, these tests might also be moved to integration if their intent is to verify behavior resulting from protected member logic but observable via public APIs.

**2. Why This Is Important:**
This action implements the separation of integration tests, ensuring they are correctly located, marked, and use environment-aware configurations. It also cleans up the unit test file according to our testing principles.

**3. Files to Modify/Create:**
*   `tests/unit/apis/hyperliquid/test_hl_api.py` (tests will be removed or refactored)
*   **Create:** `tests/integration/apis/hyperliquid/test_hl_api_integration.py`
*   Ensure all necessary imports are correct in both files after changes.

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Create New Integration Test File:**
      *   Create `tests/integration/apis/hyperliquid/test_hl_api_integration.py`.
      *   Add `import pytest` and `pytestmark = pytest.mark.integration` at the top.

   **4.2. Move and Adapt Integration Tests:**
      *   For each test class/function identified as an integration test in the analysis of `test_hl_api.py`:
          *   Copy it to `test_hl_api_integration.py`.
          *   Ensure it uses the `hl_api_with_di` fixture (or similar environment-aware fixture) for `HyperliquidAPI` instances.
          *   Update any necessary imports.
          *   Remove it from the original `test_hl_api.py`.

   **4.3. Refactor Remaining Unit Tests in `test_hl_api.py`:**
      *   For tests remaining in `test_hl_api.py`:
          *   If any were identified as directly testing protected members:
              *   Attempt to refactor them to test the same logic via the `HyperliquidAPI`'s public interface. This might involve different assertions or calling a sequence of public methods to achieve the desired state or observe the behavior.
              *   If direct refactoring to use only public interfaces is not feasible or makes the test convoluted, and the test is verifying an outcome of interactions that *could* be considered integration-level, move it to `test_hl_api_integration.py` instead.
              *   If a protected method contains truly isolated, complex logic that *should* be unit tested but isn't exposed, add a `TODO` comment for the Human Lead to consider refactoring the SUT (System Under Test) code.
          *   Ensure all remaining tests are pure unit tests, with external network calls (e.g., from services used by `HyperliquidAPI`) thoroughly mocked.

**5. Testing Requirements for This Task:**
*   All tests in both `test_hl_api.py` and `test_hl_api_integration.py` must pass.
*   Run `pytest -m integration tests/integration/apis/hyperliquid/` and verify only integration tests run.
*   Run `pytest tests/unit/apis/hyperliquid/test_hl_api.py` and verify only unit tests run.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
*   Strict adherence to not directly testing protected members in unit tests.
```

---
**Sub-Prompt 2.3: Analyze and Refactor Service Layer Tests**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** HL_TEST_P2C_SERVICE_TEST_REFACTOR
**Task:** Analyze and Refactor Hyperliquid Service Layer Tests for Environment Awareness and Test Type Separation

**1. Goal:**
Perform a similar analysis and refactoring process as in Tasks P2A and P2B, but this time focused on test files for Hyperliquid *services* (e.g., `test_hl_account_service.py`, `test_hl_market_data_service.py`, `test_hl_trading_service.py`) located under `tests/unit/apis/hyperliquid/services/`.

1.  Analyze existing service tests to identify integration vs. unit test candidates.
2.  Move integration tests to `tests/integration/apis/hyperliquid/services/`.
3.  Mark them with `@pytest.mark.integration`.
4.  Ensure tests use environment-aware configurations where applicable (though service tests often mock the `_http_client_requester` directly).
5.  Ensure unit tests for services mock all external calls (especially the `_http_client_requester`) and do not directly test protected members of the service classes.

**2. Why This Is Important:**
Service layer tests also need to be correctly categorized and refactored to ensure unit tests are fast and isolated, and integration tests accurately reflect interactions (even if interactions are with mocked API responses or, later, cassettes).

**3. Files/Directories to Interact With:**
*   **Analyze:** Files like `tests/unit/apis/hyperliquid/services/test_hl_*.py`.
*   **Create/Move to:** `tests/integration/apis/hyperliquid/services/` (create subdirectories if they don't exist).
*   **Reference:** `tests/conftest.py` for any shared fixtures.

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Create Directory Structure:**
      *   Ensure `tests/integration/apis/hyperliquid/services/` exists.

   **4.2. Analyze Service Test Files:**
      *   For each `test_hl_*_service.py` file:
          *   **Integration Candidates:** Tests that mock `_http_client_requester` to return realistic API responses (dictionaries/lists) and then test the service's processing and mapping logic up to the point of returning an Internal Domain Model. These are good candidates for using cassettes later.
          *   **Unit Test Candidates:** Tests that focus on specific methods of a service with all collaborators (like `_http_client_requester`, `RequestBuilder`, `ResponseHandler`, `Mapper`) heavily mocked to test isolated logic paths, input validation, or internal state changes of the service itself.
          *   **Protected Member Usage:** Identify any tests directly calling protected service methods.

   **4.3. Relocate and Mark Integration Service Tests:**
      *   Move identified integration test files/classes/functions to `tests/integration/apis/hyperliquid/services/`.
      *   Add `@pytest.mark.integration`. Update imports.

   **4.4. Refactor Unit Service Tests:**
      *   Ensure remaining unit tests in `tests/unit/apis/hyperliquid/services/` thoroughly mock `_http_client_requester` (to return `None` or raise specific exceptions to test service error handling) and other collaborators.
      *   Refactor tests that directly call protected members to use public interfaces of the service. If not feasible, mark for Human Lead review or consider if it's an integration characteristic.

**5. Testing Requirements for This Task:**
*   All refactored tests in both unit and integration directories for services must pass.
*   Verify pytest marker filtering works for these service tests.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
*   No direct testing of protected members in service unit tests.
```

---
**Sub-Prompt 2.4 (Iterative): Analyze and Refactor Other Hyperliquid Unit Tests (Mappers, Models, Auth, etc.)**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** HL_TEST_P2D_OTHER_UNIT_REFACTOR
**Task:** Analyze and Refactor Remaining Hyperliquid Unit Tests for Purity and Protected Member Constraint

**1. Goal:**
Review all other Hyperliquid-related test files within `tests/unit/apis/hyperliquid/` (e.g., for mappers, raw models, `hl_auth.py`, `hl_request_builder.py`, `hl_response_handler.py`, etc.).
Ensure these tests are pure unit tests:
1.  They should not make or simulate network calls (unless specifically testing `HttpClient` itself, which is not the case here).
2.  They must not directly test protected members of the classes under test.
3.  Refactor any tests violating the protected member constraint to use public interfaces.

**2. Why This Is Important:**
To ensure that our entire suite of unit tests for Hyperliquid components is clean, focused, and adheres to our testing principles, paving the way for reliable testing and easier maintenance.

**3. Files/Directories to Interact With:**
*   **Analyze & Modify:** All `test_hl_*.py` files in `tests/unit/apis/hyperliquid/` and its subdirectories (like `mappers/`, `models/`) EXCLUDING `test_hl_api.py` and service test files already handled in previous sub-tasks.

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Iteratively Review Test Files:**
      *   For each remaining `test_hl_*.py` file:
          *   **Verify Unit Test Nature:** Confirm that tests are focused on a single unit/class and that all external dependencies are mocked. For example:
              *   Mapper tests should take raw Pydantic models (or dicts that parse into them) as input and assert the output Internal Domain Model.
              *   RequestBuilder tests should take input parameters and assert the output Raw Request Pydantic Model.
              *   ResponseHandler tests should take raw JSON-like dicts/lists and assert the output Raw Pydantic Model or specific exceptions.
              *   Raw Model tests should assert validation logic of the Pydantic models themselves.
          *   **Protected Member Check:** Identify any tests directly calling protected members (e.g., `_some_method()`).
              *   Refactor these tests to validate the logic through the class's public API.
              *   If a protected method contains complex logic that cannot be easily tested via public interfaces, add a `TODO: Human Lead - Consider refactoring SUT for better testability of this protected logic.`
          *   **Environment Awareness (Less Critical Here):** Most of these lower-level unit tests might not directly instantiate `HyperliquidAPI` or need environment-aware configs. However, if any *do* (e.g., a test for a component that takes `ExchangeSpecificConfig`), ensure they use a testnet-defaulted config or are parameterized if they need to test behavior related to `is_mainnet_environment`.

**5. Testing Requirements for This Task:**
*   All refactored unit tests must pass.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
*   Strict adherence to not directly testing protected members.

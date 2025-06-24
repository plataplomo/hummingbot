**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** GLOBAL_TEST_P2_REFACTOR_UNIT_INTEGRATION
**Task:** Systematically Refactor All Tests in `tests/unit/` for Clear Unit/Integration Separation, Environment Awareness, and Adherence to Testing Principles

**1. Goal:**
Conduct a comprehensive review and refactoring of **all** test files currently located within the `tests/unit/` directory and its subdirectories (e.g., `apis/`, `core/`, `utils/`, etc.). The objectives for each test file are:
    a. **Identify Integration Tests:** Determine which tests, by their nature, are integration tests. These typically test the interaction between multiple components or a component's interaction with an external system boundary (even if that boundary is currently mocked, like an HTTP client call).
    b. **Relocate Integration Tests:** Move identified integration tests to a corresponding new structure under `tests/integration/` (e.g., tests from `tests/unit/apis/some_module/` would move to `tests/integration/apis/some_module/`). Mirror the subdirectory structure.
    c. **Mark Integration Tests:** Mark all relocated integration tests with `@pytest.mark.integration`.
    d. **Ensure Environment Awareness (for API Client Tests):** For tests involving `ExchangeAPI` instances (primarily in `tests/unit/apis/`), ensure they use environment-aware fixtures (like `active_hl_config`, `active_bp_config` once created) that allow targeting different environments (e.g., testnet by default for Hyperliquid).
    e. **Enforce Unit Test Purity:** Ensure all tests remaining in the `tests/unit/` tree are pure unit tests. This means:
        i.  **Isolation:** External dependencies (especially network I/O, file system, databases, other complex classes) must be thoroughly mocked.
        ii. **No Protected Member Access:** Unit tests **must not** directly call or assert against protected members (methods/attributes starting with `_`) of the class under test. They must test behavior through the class's public interface only. Refactor offending tests or flag them for Human Lead review if a System Under Test (SUT) code refactor is needed for testability.

**2. Why This Is Important:**
This project-wide refactoring will establish a clean, robust, and maintainable testing hierarchy. It clearly distinguishes fast, isolated unit tests from more comprehensive integration tests. This improves test suite reliability, speed of feedback (by running unit tests more frequently), and prepares for advanced testing techniques like cassette-based testing for integration tests. Enforcing encapsulation in unit tests leads to better SUT design.

**3. Files/Directories to Search and Modify:**
    *   **Primary Search Scope:** The entire `tests/unit/` directory and all its subdirectories.
    *   **Target Directory for Integration Tests:** `tests/integration/` (create it and mirror the subdirectory structure from `tests/unit/` as needed, e.g., `tests/integration/apis/`, `tests/integration/core/`).
    *   **Configuration Files (for context, especially for API tests):** `tests/conftest.py` (and any other relevant conftest files for fixtures like `active_hl_config`, etc.).

**4. Detailed Steps & Implementation Guidance (Iterative Approach Recommended):**

   **It is recommended to apply these steps iteratively, focusing on one major subdirectory of `tests/unit/` at a time (e.g., first `tests/unit/apis/`, then `tests/unit/core/`, etc.). The Human Lead will specify the initial subdirectory.**

   **For the chosen subdirectory (e.g., `tests/unit/apis/`):**

   **4.1. Create Corresponding Integration Directory Structure:**
      *   If processing `tests/unit/foo/`, ensure `tests/integration/foo/` exists.

   **4.2. Iteratively Process Each Test File (`test_*.py`) in the Current Subdirectory Scope:**
      *   For each test class and test function:
         *   **Analyze and Categorize:**
            *   **Integration Test Candidate:** Tests interactions between distinct classes/modules, or a class's interaction with a (mocked) external boundary (network, filesystem). For API tests, this includes testing public `ExchangeAPI` methods that trigger service calls and would lead to HTTP requests, or service methods that test the full flow up to returning internal models from (mocked) HTTP responses.
            *   **Unit Test Candidate:** Tests isolated logic within a single class/module. All collaborators and external systems are mocked. Examples: Mapper transformations with predefined inputs, Pydantic model validation, utility functions, service method branches that don't involve `_http_client_requester`.
         *   **Check for Protected Member Access:** Identify any direct calls to `_protected_members`.
         *   **Check for Environment Awareness Needs (API Tests):** If the test involves `ExchangeAPI` instantiation or config, ensure it's set up to use environment-aware fixtures.

   **4.3. Relocate and Mark Integration Tests:**
      *   Move identified integration test files/classes/functions to the corresponding path under `tests/integration/`. Consider renaming files (e.g., `test_foo_integration.py`) if an entire file is moved.
      *   Add `pytestmark = pytest.mark.integration` or decorate individual tests/classes with `@pytest.mark.integration`.
      *   Update all imports in moved files and any files that might have imported them.

   **4.4. Refactor and Purify Unit Tests:**
      *   For tests remaining in (or originally in) the `tests/unit/` tree:
          *   **Protected Member Refactoring:** If a test directly calls a protected member:
              1.  Attempt to refactor the test to achieve the same validation goal using only the public interface of the class under test.
              2.  If not feasible, and the test is verifying a significant interaction that feels like integration, consider moving it to the integration tests.
              3.  If the protected logic is complex and truly needs isolated testing but has no public access path, add a `TODO: Human Lead - SUT class X, method _Y: Consider refactoring for better public testability or creating a testable helper.` Then, remove the problematic test if no compliant alternative is found.
          *   **Mocking:** Ensure all external dependencies are robustly mocked.
          *   **Environment Config (if applicable):** Ensure any `ExchangeSpecificConfig` used in API-related unit tests defaults to testnet parameters where appropriate.

**5. Expected Outcome (For Each Iteration/Subdirectory):**
    *   The processed subdirectory under `tests/unit/` contains only pure unit tests.
    *   A corresponding subdirectory under `tests/integration/` contains the moved integration tests, correctly marked.
    *   All tests pass.

**6. Testing Requirements for This Refactoring Task:**
    *   After refactoring each subdirectory, run `pytest` on that specific unit subdirectory and its corresponding integration subdirectory.
    *   Verify pytest marker filtering (`-m integration` and `-m "not integration"`) works as expected for the processed directories.
    *   Human Lead will review the categorization and refactoring for a sample of tests.

**7. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
*   Strict adherence to: **Unit tests must not directly call protected members.**
*   Careful management of import paths.

**Initial Focus for Angel:**

Please start this refactoring process with the `tests/unit/apis/hyperliquid/` directory and all its subdirectories (e.g., `services/`, `mappers/`, etc.), as this was our recent area of focus and has established environment-aware fixtures we can leverage. Apply all principles outlined above to this scope.

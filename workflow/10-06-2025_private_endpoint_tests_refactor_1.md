**What the Current Tests Are Testing (The Good):**

*   **They call our public API methods:** This is a huge improvement over the previous iteration. They correctly use fixtures like `bp_api_for_test_env` and call methods like `api.get_balances()`.
*   **They test the *success* path of the full pipeline:** They correctly assert that the return type is our Internal Domain Model (e.g., `SpotBalance`, `Order`) and check a few key properties.
*   **They use VCR:** This is great for recording the successful interactions.

**Where They Miss the Point (The Problem):**

The current tests are almost entirely **happy-path integration tests**. They test that when everything works (authentication, request, response, parsing, mapping), the final result is correct.

However, they are **not battle-testing the client**. They are missing critical validation for the layers they are supposed to be testing, especially regarding **authentication, error handling, and data integrity.**

Here's a breakdown of what's missing:

1.  **They Don't Actually Test if Authentication is Working:**
    *   The tests use `bp_api_for_test_env` which has a *real authenticator*. When recording, VCR captures the *successful* result of this real authentication. On playback, VCR just returns the successful response.
    *   **What's missing?** A test that proves our signing logic is correct by comparing a locally generated signature with a known-good signature (like the `signing_test.py` from the Hyperliquid SDK does). A test that proves a request *fails* with a 401/403 error if the signature is intentionally mangled. Without these, we're just assuming our authenticator works because the happy-path cassette plays back.

2.  **They Don't Test Our `ErrorMapper` Logic:**
    *   Where are the tests for what happens when Backpack returns an `"INSUFFICIENT_FUNDS"` error or Hyperliquid returns `"Order size too small"`?
    *   An integration test for a private endpoint should have corresponding error cassettes. For example, a test for `place_order` should have a cassette where the exchange returned an "insufficient funds" error, and the test should assert that our client correctly raises an `APIError` with `code=APIErrorCode.INSUFFICIENT_FUNDS`. This validates our `ErrorMapper` against real-world error responses.

3.  **The Assertions on the Internal Models are Too Shallow:**
    *   Checking `isinstance(balances, dict)` is a good start, but it doesn't go deep enough.
    *   We need to validate the *richness* of the mapping. For example, for a `DerivativePosition`, does `hl_details.leverage_type` get populated correctly? For a `SpotBalance`, do `bp_details` contain the right information? Are `Decimal` types used everywhere they should be for financial precision? The existing tests have some of this, but it could be more rigorous.

**Revised Architectural Goal for Private Endpoint Integration Tests:**

A private endpoint integration test should validate the full pipeline for **both success and failure scenarios.** For each logical private API method (e.g., `get_balances`), we need a suite of tests:

*   **A "Success" Test:** Records a successful interaction and validates the returned Internal Domain Model in detail.
*   **An "Auth Failure" Test:** Intentionally uses a bad key/signature to record a 401/403 error, and asserts that our `ErrorMapper` correctly translates this into `APIError(code=AUTHENTICATION_FAILED)`.
*   **A "Business Logic Error" Test:** Intentionally makes a call that the exchange will reject (e.g., placing an order with insufficient funds) to record the error response. Asserts that our `ErrorMapper` correctly translates this into the appropriate `APIError` (e.g., `APIError(code=INSUFFICIENT_FUNDS)`).

This approach will truly battle-test the entire stack: authentication, request signing, error handling, and data mapping.

**Let's craft a new prompt for Angel to refactor ONE test file (`test_bp_private_endpoints_integration.py`) according to this much higher standard.** This will serve as a template for all other private endpoint test files.

---
**Prompt for Angel: Refactor Backpack Private Endpoint Tests for Full Pipeline Validation (Success & Error Cases)**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** API_INTEGRATION_P4A_BP_PRIVATE_REFACTOR
**Task:** Refactor `tests/integration/apis/backpack/test_bp_private_endpoints_integration.py` for Comprehensive Full-Pipeline Validation

**1. Goal:**
Completely refactor the integration tests for Backpack's private endpoints. The current tests only cover the "happy path." The new tests must rigorously validate the entire client pipeline (`Service -> Authenticator -> (VCR) -> ResponseHandler -> Mapper -> Internal Model`) for **both successful and common error scenarios**.

**2. Why This Is Important:**
A robust API client must not only handle successful responses but also correctly interpret and standardize various error conditions from the exchange. These tests will validate our `BackpackEd25519Authenticator`, `BackpackErrorMapper`, and the full data transformation logic against real (recorded) exchange responses, giving us high confidence in our client's reliability and error handling capabilities.

**3. File to Modify:**
*   `tests/integration/apis/backpack/test_bp_private_endpoints_integration.py`

**4. Detailed Steps & Implementation Guidance:**

   **4.1. General Approach:**
      *   For each major private endpoint (e.g., `get_balances`, `place_order`), create multiple test cases within a test class (e.g., `TestGetBalancesIntegration`).
      *   Each test case will target a specific scenario (success, auth error, business logic error).
      *   All tests will use the `bp_api_for_test_env` fixture and `vcr`.

   **4.2. Refactor `test_get_balances_integration` into a Test Class:**
      *   Create `class TestGetBalancesIntegration:`.
      *   **Success Case (`test_get_balances_success`):**
          *   This will be similar to the existing test but with more detailed assertions.
          *   Call `balances = await api.get_balances()`.
          *   Assert `isinstance(balances, dict)`.
          *   If balances exist, pick one (e.g., "USDC") and assert:
              *   `isinstance(balance_usdc, SpotBalance)`.
              *   All numeric fields (`total_quantity`, `available_quantity`) are `Decimal`.
              *   Business logic holds (`total_quantity >= available_quantity`).
      *   **Authentication Failure Case (`test_get_balances_auth_failure`):**
          *   **Setup:** Use the `bp_api_with_di` fixture to inject a *bad* authenticator or secrets.
            ```python
            # Inside the test
            bad_secrets = ApiKeyAuthSecrets(api_key=SecretStr("bad_key"), api_secret=SecretStr("bad_secret"))
            api = bp_api_with_di(secrets=bad_secrets) 
            ```
          *   **Action & Assert:** Use `pytest.raises(APIError)` to wrap the call `await api.get_balances()`.
          *   Inside the `with` block, assert on the caught exception:
              *   `assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value`.
              *   `assert exc_info.value.http_status in [401, 403]`.
          *   **Recording:** This test will need to be recorded once with real but *invalid* credentials (or manually create a cassette that returns a 401/403) to test the error mapping. For now, focus on the test structure.

   **4.3. Refactor `test_place_and_cancel_order_integration` into `TestPlaceOrderIntegration` and `TestCancelOrderIntegration` Classes:**
      *   **`TestPlaceOrderIntegration`:**
          *   `test_place_order_success`: Similar to existing logic, places an order far from market, asserts the returned `Order` model is correct and status is `OPEN`.
          *   `test_place_order_insufficient_funds`:
              *   **Setup:** Make a `PlaceOrderArgs` with an unrealistically large quantity.
              *   **Action & Assert:** Wrap `await api.place_order(args)` in `pytest.raises(APIError)`.
              *   Assert `exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value`. (This tests that our `BackpackErrorMapper` correctly maps Backpack's "INSUFFICIENT_FUNDS" error string/code).
      *   **`TestCancelOrderIntegration`:**
          *   `test_cancel_order_success`: Places an order and then immediately cancels it, asserting `True`.
          *   `test_cancel_nonexistent_order`:
              *   **Setup:** Use a clearly fake order ID in `CancelOrderArgs`.
              *   **Action & Assert:** Wrap `await api.cancel_order(args)` in `pytest.raises(APIError)`.
              *   Assert `exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value`.

   **4.4. Apply This Pattern to All Other Private Endpoints:**
      *   Systematically create test classes for `get_positions`, `get_open_orders`, `get_order_history`, etc.
      *   For each, implement at least a "success" test that validates the returned internal model structure and types in detail.
      *   Where applicable, add tests for common error scenarios (e.g., `get_positions` when there are none, `get_order_history` with an invalid date range).

**5. Recording New Cassettes:**
   *   After refactoring, all old cassettes for these files must be **deleted**.
   *   New cassettes must be recorded for each test case.
   *   **For error cases:** You may need to temporarily modify test parameters (e.g., use bad API keys, place huge orders) during the recording run (`--vcr-record=once`) to capture the real error responses from the exchange. After recording, revert the test code to its stable state. The test will then play back the recorded error.
   *   **Critically inspect** recorded cassettes to ensure sensitive data (`X-API-Key`, `X-Signature`, `X-Timestamp`) is filtered.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
*   The primary focus is testing the public API methods and the integrity of the returned Internal Domain Models.

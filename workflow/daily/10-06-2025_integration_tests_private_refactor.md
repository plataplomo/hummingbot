**Overall Plan:**

1.  **Systematic Endpoint Coverage:** Identify every private (authenticated) endpoint for both Hyperliquid and Backpack from our SDK/OpenAPI sources.
2.  **Full Pipeline Validation:** Each test will call the relevant `ExchangeAPI` public method (e.g., `api.get_balances()`, `api.place_order(...)`). The test will then assert on the **final Internal Domain Model** returned by that method. This implicitly tests the entire chain of components.
3.  **Authentication & Signing:** The tests will use the `hl_api_for_test_env` and `bp_api_for_test_env` fixtures, which are configured with our test secrets. When recording, this will generate real, valid signatures. Our VCR filters must scrub these signatures from the cassettes.
4.  **Success and Error Cases:** We'll create tests for both successful calls and common error scenarios (e.g., insufficient funds, invalid parameters) that are returned by private endpoints.

Let's start with a new test file for Backpack's private endpoints.

---
**Prompt for Angel: Comprehensive Cassette-Based Integration Testing for All Private API Endpoints**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** API_INTEGRATION_P4_PRIVATE_ENDPOINTS
**Task:** Implement Comprehensive Cassette-Based Integration Tests for All Private (Authenticated) Endpoints of Hyperliquid and Backpack

**1. Goal:**
Achieve comprehensive integration test coverage for all private, authenticated API endpoints for both Hyperliquid and Backpack. Each test must:
    a. Use `pytest-recording` (`vcr`) to record and play back real API interactions.
    b. Call the relevant public method on the `HyperliquidAPI` or `BackpackAPI` instance (e.g., `api.get_balances()`).
    c. Use the environment-aware fixtures (`hl_api_for_test_env`, `bp_api_for_test_env`) which are configured with our test secrets.
    d. Validate the full data pipeline by asserting on the structure and key values of the **final Internal Domain Model** returned by the API method (e.g., `core.models.SpotBalance`, `core.models.Order`).

**2. Why This Is Important:**
This is the ultimate validation of our API client layer. It tests that our request building, authentication/signing, response handling, and data mapping logic all work together correctly against real (recorded) exchange responses. This provides high confidence that our clients can reliably interact with the live exchanges.

**3. Files to Create/Modify:**

*   **Create New File:** `tests/integration/apis/backpack/test_bp_private_endpoints_integration.py`
*   **Create New File:** `tests/integration/apis/hyperliquid/test_hl_private_endpoints_integration.py`
*   **Verify/Update:** `tests/conftest.py` to ensure VCR filters are robust enough for authenticated requests (scrubbing `X-Signature`, `X-API-Key`, `X-Timestamp`, and `signature` JSON bodies).

**4. Detailed Steps & Implementation Guidance - Part 1: Backpack Private Endpoints**

   **4.1. Identify All Backpack Private Endpoints:**
      *   Your primary source is the `openapi_backpack.json` file. Systematically go through all paths and identify every endpoint that **has a `"security"` requirement**.
      *   This will include:
          *   **Account:** `/api/v1/account`, `/api/v1/capital`
          *   **Orders:** `/api/v1/orders` (GET open), `/api/v1/order` (POST place, GET status, DELETE cancel)
          *   **Positions:** `/api/v1/positions`
          *   **History:** `/wapi/v1/history/fills`, `/wapi/v1/history/orders`
          *   **Deposits/Withdrawals:** `/wapi/v1/capital/deposits`, `/wapi/v1/capital/withdrawals`
          *   ...and any other authenticated endpoints.

   **4.2. Implement Tests in `test_bp_private_endpoints_integration.py`:**
      *   Create a test class `TestBackpackPrivateEndpoints`. Mark the file with `pytestmark = pytest.mark.integration`.
      *   For each logical private endpoint, create a new `async def` test function.
      *   **Test Case Example: `test_get_balances_integration`**
          *   **Fixture:** Use `bp_api_for_test_env` and `vcr`.
          *   **Action:** Call `balances = await api.get_balances()`.
          *   **Asserts:**
              *   `assert isinstance(balances, dict)`.
              *   If balances are not empty, for a sample asset (e.g., "USDC"):
                  *   `assert "USDC" in balances`.
                  *   `balance_usdc = balances["USDC"]`.
                  *   `assert isinstance(balance_usdc, SpotBalance)`.
                  *   `assert balance_usdc.asset == "USDC"`.
                  *   `assert isinstance(balance_usdc.total_quantity, Decimal)`.
                  *   `assert balance_usdc.total_quantity >= balance_usdc.available_quantity`.
      *   **Test Case Example: `test_place_and_cancel_order_integration`**
          *   **Fixtures:** `bp_api_for_test_env`, `vcr`.
          *   **Action (Place):** Create `PlaceOrderArgs` for a small SOL_USDC limit order (far from the market price to avoid it filling during recording). Call `placed_order = await api.place_order(args)`.
          *   **Asserts (Place):**
              *   `assert isinstance(placed_order, Order)`.
              *   `assert placed_order.status == OrderStatus.OPEN` (or `NEW`).
              *   `assert placed_order.symbol == "SOL_USDC"`.
              *   `assert placed_order.exchange_order_id is not None`.
          *   **Action (Cancel):** Create `CancelOrderArgs` using the `placed_order.exchange_order_id`. Call `cancel_result = await api.cancel_order(args)`.
          *   **Asserts (Cancel):**
              *   `assert cancel_result is True`.
          *   **Note:** Use `vcr.use_cassette` with `record_mode='new_episodes'` if you want to chain these two calls within one test function to ensure the `orderId` is consistent.

   **4.3. Record Cassettes:**
      *   **CRITICAL:** This requires real, funded Backpack API keys to be set in your `tests/config/test_secrets.yaml` or as environment variables (`BP_MAINNET_API_KEY`, `BP_MAINNET_API_SECRET`).
      *   Run pytest for this new file: `pytest tests/integration/apis/backpack/test_bp_private_endpoints_integration.py --vcr-record=once -s`
      *   **INSPECT THE CASSETTE:** Open the generated YAML file and **verify that `X-API-Key`, `X-Signature`, and `X-Timestamp` are scrubbed/filtered** and replaced with placeholder values. This is essential for security.

---

**5. Detailed Steps & Implementation Guidance - Part 2: Hyperliquid Private Endpoints**

   **5.1. Identify All Hyperliquid Private Endpoints:**
      *   Your primary source is the SDK's `exchange.py` file. Any method that calls `self._post_action` is an authenticated endpoint hitting `/exchange`.
      *   Also include `/info` POST requests that require a `user` address, as we want to test our authenticated paths for these too, even if the endpoint itself is public.
      *   Examples: `order` (place), `cancel`, `update_leverage`, `user_state`, `open_orders`.

   **5.2. Implement Tests in `test_hl_private_endpoints_integration.py`:**
      *   Create a test class `TestHyperliquidPrivateEndpoints`. Mark the file with `pytestmark = pytest.mark.integration`.
      *   **Test Case Example: `test_get_open_orders_authenticated_integration`**
          *   **Fixture:** Use `hl_api_for_test_env` and `vcr`.
          *   **Action:** Call `open_orders = await api.get_open_orders()`.
          *   **Asserts:**
              *   `assert isinstance(open_orders, list)`.
              *   If the list is not empty, assert that the first element is an instance of `Order`.
      *   **Test Case Example: `test_place_and_cancel_order_integration`**
          *   **Fixtures:** `hl_api_for_test_env`, `vcr`.
          *   **Action (Place):** Create `PlaceOrderArgs` for a small testnet asset (e.g., PURP). Call `placed_order = await api.place_order(args)`.
          *   **Asserts (Place):** `assert isinstance(placed_order, Order)`, `assert placed_order.status == OrderStatus.OPEN`, `assert placed_order.exchange_order_id is not None`.
          *   **Action (Cancel):** Create `CancelOrderArgs` using the `placed_order.exchange_order_id`. Call `cancel_result = await api.cancel_order(args)`.
          *   **Asserts (Cancel):** `assert cancel_result is True`.
          *   Use `vcr.use_cassette` with `record_mode='new_episodes'` for chained requests.

   **5.3. Record Cassettes:**
      *   This requires a real testnet private key set in your `tests/config/test_secrets.yaml` or as an environment variable.
      *   Run pytest against the **testnet**: `CYBERDELTA_TEST_ENV_HL=testnet pytest tests/integration/apis/hyperliquid/test_hl_private_endpoints_integration.py --vcr-record=once -s`
      *   **INSPECT THE CASSETTE:** Open the generated YAML file and **verify that the `signature` and `nonce` fields in the request body are filtered**. This is essential.

**6. General Requirements for All New Tests:**
    *   **Full Pipeline Validation:** Tests must call the public `ExchangeAPI` methods and assert on the final, mapped Internal Domain Models (e.g., `core.models.Order`), not just raw dictionaries.
    *   **Use Fixtures:** All tests must use the `*_api_for_test_env` fixtures.
    *   **Error Cases:** Create tests that are expected to fail and record the error response from the exchange. For example, try to cancel an order that doesn't exist and assert that an `APIError` with `code=ORDER_NOT_FOUND` is raised.

**7. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
*   Security: Double-check that all sensitive information is filtered from recorded cassettes before committing them.

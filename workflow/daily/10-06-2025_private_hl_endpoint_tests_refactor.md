**Prompt for Angel: Refactor Hyperliquid Private Endpoint Tests for Full Pipeline Validation (Success & Error Cases)**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** API_INTEGRATION_P4B_HL_PRIVATE_REFACTOR
**Task:** Refactor `tests/integration/apis/hyperliquid/test_hl_private_endpoints_integration.py` for Comprehensive Full-Pipeline Validation

**1. Goal:**
Completely refactor the integration tests for Hyperliquid's private (authenticated) endpoints. The current tests only cover the "happy path." The new tests must rigorously validate the entire client pipeline (`Service -> Authenticator -> (VCR) -> ResponseHandler -> Mapper -> Internal Model`) for **both successful and common error scenarios** using the Hyperliquid testnet.

**2. Why This Is Important:**
This task is crucial for battle-testing our Hyperliquid client. We must validate that our EIP-712 signing logic, request building for complex actions (like placing orders), and our `HyperliquidErrorMapper` work correctly against real (recorded) testnet responses. This provides high confidence in our client's reliability, especially for critical trading actions.

**3. File to Modify:**
*   `tests/integration/apis/hyperliquid/test_hl_private_endpoints_integration.py`

**4. Detailed Steps & Implementation Guidance:**

   **4.1. General Approach:**
      *   For each major private endpoint, create multiple test cases within a dedicated test class (e.g., `TestPlaceOrderIntegration`).
      *   Each test case will target a specific scenario (success, auth error, business logic error).
      *   All tests will use the `hl_api_for_test_env` fixture (which defaults to testnet) and `vcr`.

   **4.2. Refactor Account State Endpoint Tests (into `TestGetAccountStateIntegration`):**
      *   **Success Case (`test_get_balances_and_positions_success`):**
          *   **Context:** Since `get_balances`, `get_positions`, and `get_account_summary` on Hyperliquid all derive from the same `clearinghouseState` `/info` call, a single integration test can validate them all.
          *   **Action:** Call `balances, positions, summary = await asyncio.gather(api.get_balances(), api.get_positions(), api.get_account_summary())`.
          *   **Asserts for Balances:** `assert isinstance(balances, dict)`, if not empty assert first value is `SpotBalance` with `Decimal` quantities.
          *   **Asserts for Positions:** `assert isinstance(positions, list)`, if not empty assert first value is `DerivativePosition` with `Decimal` fields.
          *   **Asserts for Summary:** `assert isinstance(summary, MarginAccountSummary)`, assert `Decimal` types for equity fields.
      *   **Auth Failure Case (`test_get_account_state_auth_failure`):**
          *   **Setup:** Use `hl_api_with_di` to inject secrets with a bad private key.
          *   **Action & Assert:** Wrap `await api.get_balances()` in `pytest.raises(APIError)` and assert the exception has `code=AUTHENTICATION_FAILED`.

   **4.3. Refactor Trading Endpoint Tests (into `TestTradingActionsIntegration`):**
      *   **Success Case (`test_place_and_cancel_order_success`):**
          *   This test will use `vcr.use_cassette` with `record_mode='new_episodes'` to chain actions.
          *   **Action 1 (Place):** Create `PlaceOrderArgs` for a testnet asset (e.g., "PURP"). Use a limit order far from the market price to prevent fills during recording. Call `placed_order = await api.place_order(args)`.
          *   **Asserts 1 (Place):** Assert `isinstance(placed_order, Order)`, `placed_order.status == OrderStatus.OPEN`, and `placed_order.exchange_order_id` is a valid integer string.
          *   **Action 2 (Cancel):** Use the `placed_order.exchange_order_id` to create `CancelOrderArgs`. Call `cancel_result = await api.cancel_order(args)`.
          *   **Asserts 2 (Cancel):** Assert `cancel_result is True`.
      *   **Business Logic Error Case (`test_place_order_insufficient_funds`):**
          *   **Setup:** Use `PlaceOrderArgs` with a very large quantity that the testnet account does not have margin for.
          *   **Action & Assert:** Wrap `await api.place_order(large_order_args)` in `pytest.raises(APIError)`.
          *   Assert `exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value`. This tests that our `HyperliquidErrorMapper` correctly maps the "Insufficient balance" string from the exchange.
      *   **Error Case (`test_cancel_nonexistent_order`):**
          *   **Setup:** Use a clearly fake order ID (e.g., `99999999999`) in `CancelOrderArgs`.
          *   **Action & Assert:** Wrap `await api.cancel_order(args)` in `pytest.raises(APIError)`.
          *   Assert `exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value`. This tests our mapper's handling of "Order was never placed" or similar error strings.

   **4.4. Implement Tests for History Endpoints:**
      *   **`test_get_order_history_integration`:**
          *   Call `await api.get_order_history(...)` with a recent time range.
          *   Assert the result is a list and that if it's not empty, the items are `Order` instances with correct types.
      *   **`test_get_trade_history_integration`:**
          *   Call `await api.get_trade_history(...)`.
          *   Assert the result is a list and that if it's not empty, the items are `Trade` instances with `Decimal` prices/quantities and `hl_details` populated correctly.

**5. Recording New Cassettes:**
   *   **Environment:** Ensure you are targeting the Hyperliquid **testnet**. The test fixtures should default to this, but you can enforce it by setting the environment variable: `CYBERDELTA_TEST_ENV_HL=testnet`.
   *   **Secrets:** You will need a real, funded (with testnet funds) private key set in your `tests/config/test_secrets.yaml` or as an environment variable (`HL_TESTNET_PRIVATE_KEY`).
   *   **Process:**
      1.  Delete all old cassettes in `tests/cassettes/apis/hyperliquid/private/`.
      2.  Run pytest for this refactored file with the record mode set: `pytest tests/integration/apis/hyperliquid/test_hl_private_endpoints_integration.py --vcr-record=once -s`.
      3.  **INSPECT THE CASSETTE:** Open the generated YAML files. **Verify that the `signature` and `nonce` fields in the JSON request body for `/exchange` calls are scrubbed and replaced with placeholder values.** This is absolutely critical for security.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
*   All tests must use the `hl_api_for_test_env` fixture.
*   Assertions must be made against the final `Internal Domain Models` (from `cyberdelta.core.models`).
```

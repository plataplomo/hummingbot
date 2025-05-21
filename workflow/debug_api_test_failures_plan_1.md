# Debugging Plan for API Test Failures

**Overall Strategy:**

*   Prioritize fixes starting from base classes and foundational issues (like DNS resolution in tests) as these can have cascading effects.
*   Address test setup issues (fixture errors, incorrect mock configurations) systematically.
*   For each failing test or group of related failures, verify the expected behavior against the Pydantic models (as the source of truth for data structures) and OpenAPI specs (secondary reference).
*   Ensure all fixes are made in test files or application code that *uses* the Pydantic models, **not** in the models themselves, as per the constraint.

---

### Phase 1: Foundational Fixes & Test Environment

**Goal:** Stabilize the test environment and fix base-level issues.

1.  **Task 1.1: Resolve `socket.gaierror` in Hyperliquid API tests.**
    *   **Tests Affected:** e.g., `tests/unit/apis/hyperliquid/test_hl_api.py::test_place_order_calls_authenticate_and_request`.
    *   **Suspected Root Cause:** Tests making actual DNS lookups/network requests for `info.hyperliquid.xyz`.
    *   **Proposed Fix:** Identify and mock all external HTTP client calls in unit tests (e.g., `_info_http_client.request`, `_exchange_http_client.request`) within the scope of each test using `unittest.mock.patch.object` or `pytest-mock`.
    *   **Relevant Files:** `tests/unit/apis/hyperliquid/test_hl_api.py`, potentially service test files in `tests/unit/apis/hyperliquid/services/`.

2.  **Task 1.2: Fix `ValueError` in `ExchangeAPI._request` unpacking.**
    *   **Test Affected:** `tests/unit/apis/base/test_exchange_api.py::test_exchange_api_request_delegates_to_http_client_and_handles_response`.
    *   **Suspected Root Cause:** Mismatch between the expected (4) and actual (3) number of values returned by `self._http_client.request` and unpacked at `cyberdelta/apis/base/exchange_api.py:262`.
    *   **Proposed Fix:** Verify `HttpClient.request`'s return signature in `cyberdelta/apis/connectivity/http_client.py` (appears to be 4 values: content, status_code, processed_headers, raw_headers). Adjust the tuple unpacking in `ExchangeAPI._request` if it differs, and ensure the mock setup in `tests/unit/apis/base/test_exchange_api.py` correctly returns 4 values, including `status_code`.
    *   **Relevant Files:** `cyberdelta/apis/base/exchange_api.py`, `tests/unit/apis/base/test_exchange_api.py`.

3.  **Task 1.3: Fix `TypeError` in `ExchangeAPI._request` unhandled exception mapping.**
    *   **Test Affected:** `tests/unit/apis/base/test_exchange_api.py::test_exchange_api_request_delegates_to_http_client_and_handles_response` (secondary error).
    *   **Suspected Root Cause:** `self.error_mapper.map_exchange_error` at `cyberdelta/apis/base/exchange_api.py:341` might return a non-exception, causing `raise mapped_error from e_unhandled` to fail.
    *   **Proposed Fix:** Ensure `map_exchange_error` in all concrete error mappers always returns an instance of `APIError` or its subclass.
    *   **Relevant Files:** Concrete error mapper implementations like `cyberdelta/apis/backpack/bp_error_mapper.py`, `cyberdelta/apis/hyperliquid/hl_errors_mapper.py`.

4.  **Task 1.4: Add missing `mock_hyperliquid_mapper` fixture.**
    *   **Test Affected:** Setup error for `tests/unit/apis/hyperliquid/test_hl_api.py::test_get_funding_rates_success`.
    *   **Suspected Root Cause:** Fixture `mock_hyperliquid_mapper` not defined.
    *   **Proposed Fix:** Define a `pytest.fixture` named `mock_hyperliquid_mapper` (returning `MagicMock(spec=HyperliquidMapper)`) in `tests/unit/apis/hyperliquid/conftest.py` or locally in the test file.
    *   **Relevant Files:** `tests/unit/apis/hyperliquid/conftest.py` or `tests/unit/apis/hyperliquid/test_hl_api.py`.

---

### Phase 2: Hyperliquid Service Layer Test Fixes (`tests/unit/apis/hyperliquid/services/`)

**Goal:** Address failures related to mocking, error handling, and logic in service tests.

1.  **Task 2.1: Correct Mocked Method Names.**
    *   **Examples:** `build_user_open_orders_payload` -> `build_open_orders_payload`, `build_l2_book_payload` -> `build_l2_book_request_payload`, etc.
    *   **Proposed Fix:** Update mocked method names in tests to match actual method names in `HyperliquidRequestBuilder` (`cyberdelta/apis/hyperliquid/hl_request_builder.py`) and `HyperliquidResponseHandler` (`cyberdelta/apis/hyperliquid/hl_response_handler.py`).
    *   **Relevant Files:** All test files in `tests/unit/apis/hyperliquid/services/`.

2.  **Task 2.2: Correct Mock Call Assertions.**
    *   **Examples:** Keyword vs. positional arguments in `assert_called_once_with` (e.g., `handle_info_meta_and_asset_ctxs_response` called with extra kwargs `status_code`, `headers`). `mock_http_client_requester` missing `timeout_seconds=None`.
    *   **Proposed Fix:** Align `assert_called_once_with` parameters with actual method call signatures used in the service code.
    *   **Relevant Files:** `tests/unit/apis/hyperliquid/services/test_hl_market_data_service.py`.

3.  **Task 2.3: Address "DID NOT RAISE APIError" Failures.**
    *   **Examples:** `TestHyperliquidMarketDataService.test_get_all_asset_contexts_raw_http_client_returns_none`, `TestHyperliquidMarketDataService.test_get_historical_funding_rates_mapper_error`, `TestHyperliquidTradingService.test_get_order_http_client_returns_none_in_info_request`.
    *   **Proposed Fix:** Review error handling in service methods. Ensure `ValueError` from mappers or issues from `None` HTTP responses are caught and re-raised as `APIError`.
    *   **Relevant Files:** `cyberdelta/apis/hyperliquid/services/hl_market_data_service.py`, `cyberdelta/apis/hyperliquid/services/hl_trading_service.py`.

4.  **Task 2.4: Correct APIError Message Assertions.**
    *   **Examples:** Mismatches like "No content received from HTTP client for clearinghouseState" vs. "No data received for user state (for clearinghouse_state)...".
    *   **Proposed Fix:** Update asserted error messages in tests to match actual `APIError` messages.
    *   **Relevant Files:** `tests/unit/apis/hyperliquid/services/test_hl_account_service.py`, `tests/unit/apis/hyperliquid/services/test_hl_market_data_service.py`.

5.  **Task 2.5: Fix `unittest.mock.patch` Target for `datetime`.**
    *   **Test Affected:** `TestHyperliquidMarketDataService.test_get_funding_rate_success`.
    *   **Proposed Fix:** Determine correct patch target for `datetime.now` based on its import and usage in `cyberdelta/apis/hyperliquid/services/hl_market_data_service.py`.
    *   **Relevant Files:** `tests/unit/apis/hyperliquid/services/test_hl_market_data_service.py`.

6.  **Task 2.6: Fix `TypeError` in `map_side_effect` for `test_get_order_history_symbol_filter`.**
    *   **Test Affected:** `TestHyperliquidAccountService.test_get_order_history_symbol_filter`.
    *   **Proposed Fix:** Change `map_side_effect` signature in the test to accept keyword arguments (e.g., `raw_historical_order`).
    *   **Relevant Files:** `tests/unit/apis/hyperliquid/services/test_hl_account_service.py`.

7.  **Task 2.7: Fix `AssertionError` (empty list) in `get_trade_history` tests.**
    *   **Tests Affected:** `TestHyperliquidAccountService.test_get_trade_history_success`, `TestHyperliquidAccountService.test_get_trade_history_symbol_filter`.
    *   **Proposed Fix:** Debug `get_trade_history` in `cyberdelta/apis/hyperliquid/services/hl_account_service.py` to ensure correct processing and filtering of trades.
    *   **Relevant Files:** `cyberdelta/apis/hyperliquid/services/hl_account_service.py`, `tests/unit/apis/hyperliquid/services/test_hl_account_service.py`.

8.  **Task 2.8: Correct Error Code Assertion in `test_get_order_history_http_client_returns_none`.**
    *   **Test Affected:** `TestHyperliquidAccountService.test_get_order_history_http_client_returns_none`.
    *   **Proposed Fix:** Change test assertion from `APIErrorCode.INVALID_RESPONSE` to `APIErrorCode.INVALID_REQUEST` as the service correctly identifies missing `start_time`/`end_time`.
    *   **Relevant Files:** `tests/unit/apis/hyperliquid/services/test_hl_account_service.py`.

---

### Phase 3: Hyperliquid API Client (`hl_api.py`) Test Fixes

**Goal:** Address failures in `tests/unit/apis/hyperliquid/test_hl_api.py`.

1.  **Task 3.1: Fix Exception Identity Check in `test_get_ticker_handles_mapped_http_error`.**
    *   **Proposed Fix:** Change `assert exc_info.value is http_failure` to assert attributes like `code`, `message`, `http_status`.
    *   **Relevant Files:** `tests/unit/apis/hyperliquid/test_hl_api.py`.

2.  **Task 3.2: Bypass Client-Side `ValueError` in `test_place_order_handles_hl_string_error_in_response`.**
    *   **Proposed Fix:** For testing exchange error handling of MARKET orders, if Hyperliquid API needs a dummy `limitPx`, provide one in the test's call to `api.place_order` to pass client-side validation.
    *   **Relevant Files:** `tests/unit/apis/hyperliquid/test_hl_api.py`.

3.  **Task 3.3: Address `KeyError` in `HyperliquidTradingService._place_order_raw` affecting `test_place_order_handles_hl_error_object_in_response`.**
    *   **Proposed Fix:** Fix `KeyError: 0` in `cyberdelta/apis/hyperliquid/services/hl_trading_service.py:124` by ensuring `raw_response_tuple` is handled correctly, especially in error scenarios. Then re-verify the test mock.
    *   **Relevant Files:** `cyberdelta/apis/hyperliquid/services/hl_trading_service.py`, `tests/unit/apis/hyperliquid/test_hl_api.py`.

4.  **Task 3.4: Correct Patch Targets for Account Service Attributes in `test_get_account_summary_*` tests.**
    *   **Examples:** Patching `api.account_service._info_http_client_requester` or `api.account_service.get_account_summary_raw`.
    *   **Proposed Fix:** Identify the correct object and attribute to patch for HTTP calls and raw data retrieval related to account summary.
    *   **Relevant Files:** `tests/unit/apis/hyperliquid/test_hl_api.py`.

5.  **Task 3.5: Address "DID NOT RAISE APIError" in `get_account_summary` handler/mapper failure tests.**
    *   **Proposed Fix:** Ensure `HyperliquidAPI.get_account_summary` (and its helpers in `cyberdelta/apis/hyperliquid/hl_api.py`) catches `ValueError` from mappers and wraps it in an `APIError`.
    *   **Relevant Files:** `cyberdelta/apis/hyperliquid/hl_api.py`.

6.  **Task 3.6: Address Pydantic Validation Issue in `test_get_funding_rates_api_error`.**
    *   **Proposed Fix:** The test appears to be using fixture data that includes an unexpected `isDelisted` field in the `universe` list, causing premature `ValidationError` before the intended mock `side_effect` on `request` is hit. Adjust the test setup or the mock of `_info_http_client.request` specifically for this test to first allow successful parsing *or* ensure the intended `APIError` from `side_effect` is what's actually being asserted. For the broader issue of `isDelisted`, if it's a valid optional field from the API, the response handler (`cyberdelta/apis/hyperliquid/hl_response_handler.py`) should be made resilient to its presence without altering the Pydantic models.
    *   **Relevant Files:** `tests/unit/apis/hyperliquid/test_hl_api.py`, potentially `cyberdelta/apis/hyperliquid/hl_response_handler.py`.

---

This plan should provide a clear path to resolving the test failures.
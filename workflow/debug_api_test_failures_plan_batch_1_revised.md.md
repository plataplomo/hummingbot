# Debugging Plan: API Test Failures (Batch 1 - Revised)

**Objective:** Guide "Angel" (Code Mode) to diagnose and fix 10 specific test failures. All fixes must align with the defined Pydantic models and OpenAPI specifications.

**Sources of Truth (Immutable):**
1.  **Pydantic Models:**
    *   [`cyberdelta/apis/models`](cyberdelta/apis/models)
    *   [`cyberdelta/apis/backpack/models`](cyberdelta/apis/backpack/models)
    *   [`cyberdelta/apis/hyperliquid/models`](cyberdelta/apis/hyperliquid/models)
    *   [`cyberdelta/core/models`](cyberdelta/core/models)
2.  **OpenAPI Specifications:**
    *   [`openapi_backpack.json`](openapi_backpack.json)
    *   [`openapi_hl.json`](openapi_hl.json)

---

## Test Failure Analysis and Remediation Plan

### 1. `TypeError: exceptions must derive from BaseException`
   *   **Test File:** [`tests/unit/apis/base/test_exchange_api.py::test_exchange_api_request_delegates_to_http_client_and_handles_response`](tests/unit/apis/base/test_exchange_api.py)
   *   **Hypothesis:** The `mock_http_client_request` in the test ([`tests/unit/apis/base/test_exchange_api.py:280-284`](tests/unit/apis/base/test_exchange_api.py:280-284)) returns a 3-tuple. However, the `ExchangeAPI._request` method expects a 4-tuple from `self._http_client.request` (content, status\_code, processed\_headers, raw\_headers) as per its implementation ([`cyberdelta/apis/base/exchange_api.py:262-267`](cyberdelta/apis/base/exchange_api.py:262-267)). This tuple unpacking mismatch likely raises a `ValueError`. This `ValueError` is then caught by the generic `except Exception as e_unhandled:` block in `ExchangeAPI._request` ([`cyberdelta/apis/base/exchange_api.py:328`](cyberdelta/apis/base/exchange_api.py:328)). This block calls `self.error_mapper.map_exchange_error()`. Since `self.error_mapper` is a `MagicMock` in this test and its `map_exchange_error` method hasn't been configured with a specific `return_value` for this scenario, it defaults to returning another `MagicMock` instance. Raising this `MagicMock` instance (which doesn't derive from `BaseException`) then results in the `TypeError`.
   *   **Investigation Steps (Angel):**
        1.  Confirm the return signature of `HttpClient.request` in [`cyberdelta/apis/connectivity/http_client.py`](cyberdelta/apis/connectivity/http_client.py) (around [line 277](cyberdelta/apis/connectivity/http_client.py:277)) returns 4 items: `(content, status_code, processed_headers, raw_headers)`.
        2.  Verify that the mock setup for `mock_http_client_request.return_value` in [`tests/unit/apis/base/test_exchange_api.py:280-284`](tests/unit/apis/base/test_exchange_api.py:280-284) is indeed providing only 3 items.
        3.  Trace the execution in `ExchangeAPI._request` ([`cyberdelta/apis/base/exchange_api.py:262-341`](cyberdelta/apis/base/exchange_api.py:262-341)) to confirm the `ValueError` due to unpacking, and subsequent handling by the generic `except Exception` block leading to the `TypeError`.
   *   **Potential Remediation Strategies:**
        1.  Modify the mock setup in [`tests/unit/apis/base/test_exchange_api.py:280-284`](tests/unit/apis/base/test_exchange_api.py:280-284) to ensure `mock_http_client_request.return_value` is a 4-tuple, matching the expected signature from `HttpClient.request`. For example, it should be `(mock_response_content, mock_status_code, mock_processed_headers, mock_raw_headers_multidict)`.

---

### 2. `AssertionError: expected call not found.` (test_get_balances_success)
   *   **Test File:** [`tests/unit/apis/hyperliquid/services/test_hl_account_service.py::TestHyperliquidAccountService::test_get_balances_success`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py)
   *   **Hypothesis:** The `mock_http_client_requester.assert_called_once_with(...)` in the test ([`test_hl_account_service.py:208-214`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:208-214)) specifies `is_signed=False`. The SUT method `_get_raw_clearinghouse_state` ([`hl_account_service.py:145-151`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:145-151)) also correctly calls `_http_client_requester` with `is_signed=False`. The arguments explicitly asserted seem to match. The "expected call not found" error might be due to other implicit default arguments in the `HttpClientRequesterSig` not being perfectly matched if the `AsyncMock`'s spec is causing strict argument checking, or if some other part of the test setup (e.g., `mock_request_builder.build_user_state_payload`) isn't returning what's expected, preventing the call flow. The full `AssertionError` message detailing the expected vs. actual calls would be crucial. Could also be a slight mismatch in the `endpoint_path` if `self._info_url` is not what's expected. In `hyperliquid_account_service` fixture, `info_url` is "http://test-mock-url". So `mock_endpoint_path` should match this, not just "/info".
   *   **Investigation Steps (Angel):**
        1.  Examine the full `AssertionError` message. It usually shows the expected call signature and any actual calls made.
        2.  Verify the `endpoint_path` argument in the SUT call in [`cyberdelta/apis/hyperliquid/services/hl_account_service.py:147`](cyberdelta/apis/hyperliquid/services/hl_account_service.py). It uses `endpoint_path = "/info"`. However, the `_http_client_requester` (which maps to `HttpClient.request`) typically joins this with a base URL. The test assertion uses `endpoint_path=mock_endpoint_path` where `mock_endpoint_path = "/info"` ([`test_hl_account_service.py:121`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:121)). The `HttpClient.request` method constructs the `full_url` using `self.rest_endpoint`. If `self._info_url` is intended to be the full URL for info requests, the SUT should pass `self.info_url` as `endpoint_path` directly.
        3.  Check the call to `mock_request_builder.build_user_state_payload` ([`test_hl_account_service.py:207`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:207)) and its return value setup ([`test_hl_account_service.py:125-128`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:125-128)) to ensure `payload_dict` in SUT matches what the test expects.
   *   **Potential Remediation Strategies:**
        1.  If the `endpoint_path` in the SUT should be the full `self._info_url` for info requests, change [`hl_account_service.py:134`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:134) to `endpoint_path = self._info_url` and update the test assertion accordingly.
        2.  Adjust the `mock_http_client_requester.assert_called_once_with` to include any missing default parameters (like `authenticator=None`, `rate_limiter_service=None`) if the mock spec is being strict.

---

### 3. `AssertionError: Expected 'build_order_history_payload' to be called once. Called 0 times.`
   *   **Test File:** [`tests/unit/apis/hyperliquid/services/test_hl_account_service.py::TestHyperliquidAccountService::test_get_order_history_success`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py)
   *   **Hypothesis:** The assertion `mock_request_builder.build_order_history_payload.assert_called_once_with(...)` on [`test_hl_account_service.py:522`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:522) occurs *before* the SUT method `hyperliquid_account_service.get_order_history(...)` is called on [`test_hl_account_service.py:535`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:535).
   *   **Investigation Steps (Angel):**
        1.  Confirm the order of operations in the test.
   *   **Potential Remediation Strategies:**
        1.  Move the assertion `mock_request_builder.build_order_history_payload.assert_called_once_with(...)` (and related assertions for `mock_http_client_requester`, `mock_response_handler`, `mock_hl_order_mapper`) to *after* the call to `await hyperliquid_account_service.get_order_history(...)` on [`line 535`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:535).

---

### 4. `cyberdelta.apis.models.api_error.APIError: Unexpected error for HL order history...`
   *   **Test File:** [`tests/unit/apis/hyperliquid/services/test_hl_account_service.py::TestHyperliquidAccountService::test_get_order_history_symbol_filter`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py)
   *   **Hypothesis:** Similar to failure 3, a premature assertion of `build_order_history_payload` might exist. If not, an unexpected `APIError` is raised by the SUT method `get_order_history` ([`hl_account_service.py:342-444`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:342-444)). Given the client-side symbol filtering applied in the SUT ([`hl_account_service.py:390-392`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:390-392) and [`hl_account_service.py:401-408`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:401-408)), the error likely stems from issues in payload building, the HTTP request, response handling, or the mapping process *before* filtering.
   *   **Investigation Steps (Angel):**
        1.  Check for premature assertions as in Failure 3.
        2.  If assertions are correctly placed, examine the `get_order_history` method in [`cyberdelta/apis/hyperliquid/services/hl_account_service.py`](cyberdelta/apis/hyperliquid/services/hl_account_service.py) and the mocks for `_http_client_requester`, `_response_handler.handle_query_order_history_response`, and `_order_mapper.transform_raw_historical_order_to_internal` to see where an `APIError` might be raised or an unhandled exception occurs that gets wrapped into a generic `APIError`.
   *   **Potential Remediation Strategies:**
        1.  Correct assertion order if applicable.
        2.  Fix the underlying cause of the `APIError` within `get_order_history` or update mock configurations if they are causing unexpected behavior.

---

### 5. `AssertionError: assert [] == [<MagicMock i...>]` (test_get_trade_history_success)
   *   **Test File:** [`tests/unit/apis/hyperliquid/services/test_hl_account_service.py::TestHyperliquidAccountService::test_get_trade_history_success`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py)
   *   **Hypothesis:** The test uses `with patch.object(HyperliquidUserFillMapper, "map", ...)` ([`test_hl_account_service.py:665`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:665)). This patches the `map` method on the *class* `HyperliquidUserFillMapper`. However, the `HyperliquidAccountService` SUT is instantiated with `user_fill_mapper=mock_hl_user_fill_mapper` ([`test_hl_account_service.py:103`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:103)), and it calls `self._user_fill_mapper.map(raw_fill_obj)` ([`hl_account_service.py:524`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:524)). The patch doesn't affect the already instantiated `mock_hl_user_fill_mapper`'s `map` method. Thus, `self._user_fill_mapper.map()` (which is `mock_hl_user_fill_mapper.map`) likely returns a default `MagicMock` as it hasn't been directly configured for this test. The subsequent client-side symbol filter `if symbol is None or mapped_trade.symbol == symbol:` ([`hl_account_service.py:527`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:527)) would compare attributes of this fresh `MagicMock` to `None` or `"BTC"`, likely failing the condition and resulting in an empty `internal_trades` list.
   *   **Investigation Steps (Angel):**
        1. Confirm that `self._user_fill_mapper` in the SUT is indeed the `mock_hl_user_fill_mapper` instance.
        2. Verify that the class-level patch is not affecting the instance's `map` method.
   *   **Potential Remediation Strategies:**
        1. Change the patch to target the instance method: `with patch.object(mock_hl_user_fill_mapper, "map", return_value=mapped_trade) as mocked_map_method:`.

---

### 6. `AssertionError: assert [] == [<MagicMock i...>]` (test_get_trade_history_symbol_filter)
   *   **Test File:** [`tests/unit/apis/hyperliquid/services/test_hl_account_service.py::TestHyperliquidAccountService::test_get_trade_history_symbol_filter`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py)
   *   **Hypothesis:** Same as Failure 5. The `patch.object(HyperliquidUserFillMapper, "map", ...)` targets the class, not the injected `mock_hl_user_fill_mapper` instance.
   *   **Investigation Steps (Angel):**
        1. Same as Failure 5.
   *   **Potential Remediation Strategies:**
        1. Change the patch to target the instance method: `with patch.object(mock_hl_user_fill_mapper, "map", side_effect=map_side_effect_func) as mocked_map_method:`.

---

### 7. `AssertionError: assert 'No content received...' in 'No data received...'` (test_get_balances_http_client_returns_none_in_state_fetch)
   *   **Test File:** [`tests/unit/apis/hyperliquid/services/test_hl_account_service.py::TestHyperliquidAccountService::test_get_balances_http_client_returns_none_in_state_fetch`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py)
   *   **Hypothesis:** Direct string mismatch. The SUT's `_get_raw_clearinghouse_state` method ([`hl_account_service.py:158-165`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:158-165)) raises an `APIError` with the message `f"No data received for user state (for clearinghouse_state), status: {status_code}"`. The test ([`test_hl_account_service.py:811-813`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:811-813)) asserts that the string `"No content received from HTTP client for clearinghouseState"` is in the exception message.
   *   **Investigation Steps (Angel):**
        1. Confirm the exact string produced by SUT and expected by the test.
   *   **Potential Remediation Strategies:**
        1. Align the strings. Either change the SUT's message to match the test's expectation or change the test's expected substring to match the SUT's actual message. Consistency across error messages is preferred.

---

### 8. `AssertionError: assert 102 == 6` (test_get_order_history_http_client_returns_none)
   *   **Test File:** [`tests/unit/apis/hyperliquid/services/test_hl_account_service.py::TestHyperliquidAccountService::test_get_order_history_http_client_returns_none`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py)
   *   **Hypothesis:** The SUT's `get_order_history` method, when `raw_data` is `None` because the HTTP client returned `None`, raises an `APIError` with `code=APIErrorCode.INVALID_RESPONSE.value` ([`hl_account_service.py:356-360`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:356-360)). `APIErrorCode.INVALID_RESPONSE.value` should be `6`. The test asserts `exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value` ([`test_hl_account_service.py:862`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:862)). The failure `assert 102 == 6` indicates the actual error code raised was `102`. One possibility is that an `APIError` with code 102 was raised *before* reaching the `if raw_data is None:` check. For example, if `self._request_builder.build_order_history_payload` itself raised an `APIError` with code 102, or if the `self._http_client_requester` itself raised a pre-mapped `APIError` with code 102 instead of returning `(None, 200, MagicMock())`. The test configures `mock_http_client_requester.return_value = (None, 200, MagicMock())` ([`test_hl_account_service.py:853-857`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:853-857)), so the error must be originating from within the `try` block in `get_order_history` after the HTTP call but before or during the `if raw_data is None:` condition, or the `INVALID_RESPONSE` path has an issue.
      The test also calls `hyperliquid_account_service.get_order_history(symbol=symbol)` where `symbol` is `None`. The payload builder might be the source.
   *   **Investigation Steps (Angel):**
        1.  Carefully trace the `get_order_history` method in [`cyberdelta/apis/hyperliquid/services/hl_account_service.py`](cyberdelta/apis/hyperliquid/services/hl_account_service.py), from payload building, through the HTTP request, to the point where `raw_data is None` is checked.
        2.  Examine if any operation *before* the `if raw_data is None:` check ([`line 355`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:355)) could raise an `APIError` with code `102`.
        3.  Check the setup of `mock_request_builder.build_order_history_payload` in the test ([`test_hl_account_service.py:851`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:851)).
   *   **Potential Remediation Strategies:**
        1.  If the error originates before the intended `INVALID_RESPONSE` error, fix that preceding issue.
        2.  If there's an unexpected path leading to code 102 for this "None content" scenario, correct the SUT logic to ensure `APIErrorCode.INVALID_RESPONSE.value` (6) is used.

---

### 9. `AttributeError: Mock object has no attribute 'build_user_fills_payload'. Did you mean: 'build_user_state_payload'?`
   *   **Test File:** [`tests/unit/apis/hyperliquid/services/test_hl_account_service.py::TestHyperliquidAccountService::test_get_trade_history_http_client_returns_none`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py)
   *   **Hypothesis:** The SUT's `get_trade_history` method calls `self._request_builder.build_user_fills_request_payload(self._wallet_address)` ([`hl_account_service.py:463`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:463)). The `HyperliquidRequestBuilder` class *does* have a static method `build_user_fills_request_payload` ([`hl_request_builder.py:308-315`](cyberdelta/apis/hyperliquid/hl_request_builder.py:308-315)). The `AttributeError` from a mock usually means the `spec` used for the mock doesn't include this attribute, or the attribute was misspelled on the mock. However, the test ([`test_hl_account_service.py:903`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:903)) correctly sets `mock_request_builder.build_user_fills_payload.return_value`. This dynamic assignment should create the attribute on the mock if it doesn't exist due to spec. The error "Mock object has no attribute..." coming *from the SUT's attempt to call it* is therefore puzzling if `self._request_builder` in the SUT is indeed the `mock_request_builder` that has this attribute dynamically added.
      A possible cause: the `spec=HyperliquidRequestBuilder` on the `mock_request_builder` might be too strict or `unittest.mock` might not be recognizing the `@staticmethod` correctly for spec purposes, causing any attempt to access an attribute not explicitly part of the class's instance attributes (even if it's a static method) to fail if not pre-configured *on the mock's type*. However, direct assignment like `mock.method.return_value=` usually bypasses this for `MagicMock`. This needs closer inspection on how mocks and specs interact with static methods.
   *   **Investigation Steps (Angel):**
        1.  Verify that `self._request_builder` within `get_trade_history` is the exact same `mock_request_builder` instance from the test fixture.
        2.  Check if the `HyperliquidRequestBuilder` class when used as a `spec` with `MagicMock` properly allows dynamic creation or recognition of static methods as attributes.
        3.  Try removing the `spec=HyperliquidRequestBuilder` from the `mock_request_builder` fixture temporarily to see if the error changes. This would indicate an issue with how the spec interacts with static methods.
   *   **Potential Remediation Strategies:**
        1.  If it's a spec issue with static methods, one might need to explicitly add the method to the mock's `_mock_methods` or configure it using `configure_mock` if the dynamic assignment isn't working as expected due to the spec.
        2.  Ensure the SUT has the correct instance of the request builder.

---

### 10. `AssertionError: assert 'No content received...' in 'No data received...'` (test_get_open_orders_http_client_returns_none)
   *   **Test File:** [`tests/unit/apis/hyperliquid/services/test_hl_account_service.py::TestHyperliquidAccountService::test_get_open_orders_http_client_returns_none`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py)
   *   **Hypothesis:** Direct string mismatch. The SUT's `get_open_orders` method ([`hl_account_service.py:639-644`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:639-644)) raises an `APIError` with the message `f"No data received for open orders, status: {status_code}"`. The test ([`test_hl_account_service.py:983-985`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:983-985)) asserts that the string `"No content received from HTTP client for openOrders"` is in the exception message.
   *   **Investigation Steps (Angel):**
        1.  Confirm the exact string produced by SUT and expected by the test.
   *   **Potential Remediation Strategies:**
        1.  Align the strings. Either change the SUT's message to match the test's expectation or change the test's expected substring to match the SUT's actual message.

---

This revised plan provides a more targeted approach based on initial code review. Angel should use tools like `read_file`, `apply_diff`, and consult the Sources of Truth (`Pydantic Models` and `OpenAPI Specifications`) as needed.
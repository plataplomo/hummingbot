# Pytest Services Test Failure Analysis and Remediation Plan

This document outlines the analysis of `pytest` failures found in `pytest_services_output.txt` and a plan to address them.

**Summary of Failing Tests and Root Causes**

The primary reasons for the 24 test failures are:

1.  **Mock Assertion Failures (`AssertionError: expected call not found.` or `Called 0 times.` or `AttributeError` on mock):**
    *   **Argument Mismatches in Mock Calls:**
        *   **`is_info_endpoint` / `is_signed` / `authenticator` / `rate_limiter_service`:** In tests for `HyperliquidAccountService` involving `_get_raw_clearinghouse_state` (e.g., `test_get_balances_success`, `test_get_balances_http_client_returns_none_in_state_fetch`), the assertions for `mock_http_client_requester` expect parameters like `authenticator` or specific `is_signed` values that don't match the actual implementation in `_get_raw_clearinghouse_state()` which calls with `is_info_endpoint=True, is_signed=False` and omits `authenticator`.
        *   **Positional vs. Keyword Arguments:** Several tests assert calls to builder or handler methods using keyword arguments (e.g., `user_address="value"`) while the source code calls them positionally (e.g., `builder_method("value")`). This affects `test_get_trade_history_http_client_returns_none` (`test_hl_account_service.py:931`), `test_get_open_orders_state_fetch_returns_none` (`test_hl_account_service.py:1024`), `test_get_ticker_success` (`test_hl_market_data_service.py:139`), and `test_get_funding_rate_success` (`test_hl_market_data_service.py:228`).
        *   **Parameter Name Typos/Changes in Mocks:**
            *   `interval` vs. `timeframe`: `HyperliquidMarketDataService.get_market_data` (`hl_market_data_service.py:609`) calls `build_candle_snapshot_payload` with `timeframe=...`, but tests (`test_hl_market_data_service.py:646`, `test_hl_market_data_service.py:707`) assert with `interval=...`.
            *   `response_content` vs. `raw_response_content`: In `test_hl_market_data_service.py` for `handle_historical_funding_rates_response` tests (e.g., `test_hl_market_data_service.py:835`), assertions use `response_content`, but the source (`hl_market_data_service.py:563`) calls the handler with `raw_response_content`. Similar issues for `handle_info_meta_and_asset_ctxs_response` (`test_hl_market_data_service.py:133`) and `handle_info_l2_book_response` (`test_hl_market_data_service.py:356`).
        *   `headers=ANY` vs. actual headers: Using `ANY` for headers when the actual call might pass an empty dict `{}` can cause assertion mismatches.
    *   **Incorrect Method Names in Mocks (`AttributeError`):**
        *   Tests for `get_recent_trades` in `test_hl_market_data_service.py` (e.g., line 484) attempt to use `mock_hl_response_handler.handle_recent_trades_response`, but the actual method in `HyperliquidResponseHandler` and called by the service is `handle_info_recent_trades_response`.
    *   **Builder Method Not Called as Expected:**
        *   `test_get_order_history_http_client_returns_none` (`test_hl_account_service.py:916`): Asserts `build_order_history_payload` was called, but the service raises an error due to missing `start_time`/`end_time` *before* the builder is invoked.
        *   Failures in `test_hl_trading_service.py` (e.g., `test_cancel_order_http_client_returns_none_in_exchange_action:220`, `test_get_open_orders_http_client_returns_none_in_info_request:180`) show builder methods expected to be called but "Called 0 times". This is due to either positional/keyword argument mismatches or the service not calling the builder directly in some refactored paths (e.g., constructing `HyperliquidApiCancelOrderRequest` directly in `_cancel_order_raw`).

2.  **`DID NOT RAISE <class 'APIError'>` Failures:**
    *   `test_get_historical_funding_rates_mapper_error` (`test_hl_market_data_service.py:1065`): The service method `get_historical_funding_rates` (`hl_market_data_service.py:568-579`) does not catch `ValueError` raised by the mapper and wrap it in an `APIError`.
    *   `test_get_order_http_client_returns_none_in_info_request` (`test_hl_trading_service.py:138`): The service's `_get_order_status_raw` method (`hl_trading_service.py:254`) returns `None` instead of raising an `APIError` when the HTTP client returns no content.

3.  **Incorrect Error Message Assertion:**
    *   `test_get_historical_funding_rates_http_client_returns_none` (`test_hl_market_data_service.py:949`): The test asserts an error message containing `"No content received from HTTP client for fundingHistory"`, but the actual service-raised error message is `"No data received for historical funding rates for ETH, status: 200"`.

**Impact of Recent Deep Refactoring:**

The widespread nature of mock assertion failures strongly indicates that the recent deep refactoring changed internal call signatures and logic. The unit tests were not correspondingly updated.

**High-Level Plan to Address Failures**

**General Approach:** For each failing test:
1.  Examine traceback.
2.  Compare "Expected Call" vs. "Actual Call".
3.  Inspect service, builder, handler, mapper methods.
4.  Update test assertion or service code (respecting "RAW" model constraint).

**I. Address Failures in `tests/unit/apis/hyperliquid/services/test_hl_account_service.py`**

*   **Target Files:**
    *   [`tests/unit/apis/hyperliquid/services/test_hl_account_service.py`](tests/unit/apis/hyperliquid/services/test_hl_account_service.py:1)
    *   Possibly [`cyberdelta/apis/hyperliquid/services/hl_account_service.py`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:1)

*   **Actions:**
    1.  **`test_get_balances_success` & `test_get_order_history_success`:**
        *   Adjust `mock_http_client_requester` assertions to match service calls ([`hl_account_service.py:145`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:145), L343).
    2.  **`test_get_balances_http_client_returns_none_in_state_fetch`:**
        *   Align `mock_http_client_requester` assertion with [`_get_raw_clearinghouse_state()`](cyberdelta/apis/hyperliquid/services/hl_account_service.py:145) (no `authenticator`, `is_info_endpoint=True`, `is_signed=False`).
    3.  **`test_get_order_history_http_client_returns_none`:**
        *   Change `build_order_history_payload` assertion to `assert_not_called()` as service errors out first.
    4.  **`test_get_trade_history_http_client_returns_none`:**
        *   Change `build_user_fills_request_payload` assertion to use positional argument: `assert_called_once_with(wallet_address)`.
    5.  **`test_get_open_orders_state_fetch_returns_none`:**
        *   Change `build_user_state_payload` assertion to use positional argument: `assert_called_once_with(wallet_address)`.

**II. Address Failures in `tests/unit/apis/hyperliquid/services/test_hl_market_data_service.py`**

*   **Target Files:**
    *   [`tests/unit/apis/hyperliquid/services/test_hl_market_data_service.py`](tests/unit/apis/hyperliquid/services/test_hl_market_data_service.py:1)
    *   [`cyberdelta/apis/hyperliquid/services/hl_market_data_service.py`](cyberdelta/apis/hyperliquid/services/hl_market_data_service.py:1)

*   **Actions:**
    1.  **`test_get_all_asset_contexts_success`, `test_get_all_asset_contexts_raw_success`, `test_get_order_book_success`:**
        *   Adjust response handler assertions: call `response_content` positionally, use `headers={}` or actual mock header.
    2.  **`test_get_ticker_success`, `test_get_funding_rate_success`:**
        *   Change mapper call assertions to use positional arguments.
    3.  **`test_get_recent_trades_success`, `test_get_recent_trades_http_client_returns_none`:**
        *   Correct mocked method name to `mock_hl_response_handler.handle_info_recent_trades_response`.
    4.  **`test_get_market_data_success`, `test_get_market_data_http_client_returns_none`:**
        *   Change `build_candle_snapshot_payload` assertion to expect `timeframe=...`.
    5.  **`test_get_historical_funding_rates_success`, `test_get_historical_funding_rates_api_error_from_handler`:**
        *   Change `handle_historical_funding_rates_response` assertion to expect `raw_response_content=...`.
    6.  **`test_get_historical_funding_rates_http_client_returns_none`:**
        *   Update asserted error message to `"No data received for historical funding rates for ETH, status: 200"`.
    7.  **`test_get_historical_funding_rates_response_validation_error`:**
        *   Ensure service call is inside `with pytest.raises(APIError):`.
    8.  **`test_get_historical_funding_rates_mapper_error`:**
        *   Modify [`get_historical_funding_rates()`](cyberdelta/apis/hyperliquid/services/hl_market_data_service.py:528) in `hl_market_data_service.py` to catch `(ValidationError, ValueError)` around line 570 and wrap in `APIError`.

**III. Address Failures in `tests/unit/apis/hyperliquid/services/test_hl_trading_service.py`**

*   **Target Files:**
    *   [`tests/unit/apis/hyperliquid/services/test_hl_trading_service.py`](tests/unit/apis/hyperliquid/services/test_hl_trading_service.py:1)
    *   [`cyberdelta/apis/hyperliquid/services/hl_trading_service.py`](cyberdelta/apis/hyperliquid/services/hl_trading_service.py:1)

*   **Actions:**
    1.  **`test_get_order_http_client_returns_none_in_info_request`:**
        *   Modify [`_get_order_status_raw()`](cyberdelta/apis/hyperliquid/services/hl_trading_service.py:226) (around line 254) to raise `APIError` when `raw_response_content is None`.
    2.  **`test_get_open_orders_http_client_returns_none_in_info_request` & `test_cancel_all_orders_get_open_orders_returns_none`:**
        *   Change `build_open_orders_payload` assertion to positional: `assert_called_once_with(wallet_address)`.
    3.  **`test_cancel_order_http_client_returns_none_in_exchange_action`:**
        *   Remove `build_cancel_order_payload` assertion. Verify `data` argument passed to `_exchange_http_client_requester` in [`_cancel_order_raw()`](cyberdelta/apis/hyperliquid/services/hl_trading_service.py:155).
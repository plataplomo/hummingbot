# TASK: Create & Update Unit Tests for WebSocket Payloads, Subscription Logic, and Serializers

## 1. Goal
Develop new unit tests and update existing ones to thoroughly validate the recent refactoring of WebSocket subscription handling and to re-verify `HttpClient`'s request serialization. The objectives are:
1.  **Test New WebSocket Subscription Request Models:** Create unit tests for the Pydantic models defined in `bp_ws_payloads.py` and `hl_ws_payloads.py` to ensure they correctly validate inputs.
2.  **Test `_construct_subscription_payload` Methods:** Update/create tests for these methods in `BackpackAPI` and `HyperliquidAPI` to verify they return the correct Pydantic model instances (or raise appropriate exceptions for invalid topics/missing data).
3.  **Test `WebSocketManager.send_json()`:** Update/create tests to ensure it correctly serializes Pydantic `BaseModel` inputs and handles its strict `data: BaseModel` signature.
4.  **Re-verify `HttpClient.request` Serialization:** Confirm (and add targeted tests if necessary) that Pydantic request models for REST APIs are serialized as expected, paying attention to `by_alias` and `exclude_none` behavior.

## 2. Context and "Why"
The previous phase introduced Pydantic models for WebSocket subscription requests, refactored `_construct_subscription_payload` to return these models, and updated `WebSocketManager.send_json()` to serialize them. `HttpClient` already handled Pydantic models for REST requests. This testing phase is crucial to confirm that all these components work correctly together, ensuring that all outgoing structured messages (REST bodies and WS subscriptions) are validated and serialized reliably.

## 3. Project Rules Reminder
- Adhere strictly to all project rules.
- Tests should cover valid inputs, error conditions, and edge cases.
- `RULE-NO-SILENCING-V4`: Minimize `cast`; justify if unavoidable. No `type: ignore` in test runtime code.
- Raw Pydantic models for WS subscriptions should only perform syntactic validation. The `_construct_subscription_payload` methods (or the service layer calling them) handle business logic for forming a subscription request.

## 4. Detailed Instructions

### Part A: Unit Tests for New WebSocket Subscription Request Pydantic Models

**New Test Files:**
-   `tests/unit/apis/backpack/models/test_bp_ws_payloads.py` (Create if not existing)
-   `tests/unit/apis/hyperliquid/models/test_hl_ws_payloads.py` (Create if not existing)

**For EACH Pydantic model defined in `bp_ws_payloads.py` and `hl_ws_payloads.py` (e.g., `BackpackRawWsSubscriptionRequest`, `HyperliquidRawWsSubscribeRequest`, and its inner payload types like `HyperliquidRawWsL2BookSubscriptionPayload`):**

1.  **Test Valid Instantiation:**
    *   Cases with all required fields.
    *   Cases with all optional fields populated.
    *   Verify attribute values and types (e.g., `Literal` fields, nested Pydantic models).
2.  **Test Invalid Data Handling (Field-Level Validation):**
    *   Provide data that violates field types or constraints (e.g., wrong string for `Literal`, invalid format for `RawBp...`/`RawHl...` common types).
    *   Use `pytest.raises(ValidationError)` and inspect `excinfo.value.errors()` for specific error messages.
3.  **Test Model Structure:**
    *   For wrapper models like `HyperliquidRawWsSubscribeRequest` (which contains a `Union` for `subscription`), test that Pydantic correctly discriminates and validates the nested payload based on its `type` field.

### Part B: Unit Tests for `_construct_subscription_payload` Methods

**Test Files to Update/Augment:**
-   `tests/unit/apis/backpack/test_bp_api.py`
-   `tests/unit/apis/hyperliquid/test_hl_api.py`

**For `BackpackAPI._construct_subscription_payload` and `HyperliquidAPI._construct_subscription_payload`:**

1.  **Test Valid Topic-to-Model Conversion:**
    *   For various valid `topic` strings (public and private for Backpack; l2Book, trades, userEvents, candle for Hyperliquid):
        *   Call the method.
        *   Assert that the returned object is an instance of the correct Pydantic model (e.g., `BackpackRawWsSubscriptionRequest`, `HyperliquidRawWsSubscribeRequest`).
        *   Assert that the fields of the returned Pydantic model are populated correctly based on the input `topic` (and `wallet_address` for HL userEvents).
        *   Example (Backpack): `topic="depth.SOL_USDC"` -> model has `method="SUBSCRIBE"`, `params=["depth.SOL_USDC"]`.
        *   Example (Hyperliquid): `topic="l2Book:ETH"` -> model has `method="subscribe"`, `subscription` is `HyperliquidRawWsL2BookSubscriptionPayload` with `type="l2Book"` and `coin="ETH"`.
2.  **Test Exception Handling for Invalid/Unsupported Topics:**
    *   Pass malformed topic strings.
    *   Pass topic strings for types not supported by the exchange.
    *   For Hyperliquid `userEvents` topic, test calling with `wallet_address=None`.
    *   Use `pytest.raises(ValueError)` or `pytest.raises(APIError)` (as appropriate for the exceptions raised by the method) to assert that an exception is raised. Verify the error message if possible.
3.  **Test Private Stream Signature Placeholder (Backpack):**
    *   If the Backpack method uses placeholder signatures for private streams currently, ensure tests reflect that (e.g., `signature` field is `None` or contains expected placeholders).

### Part C: Unit Tests for `WebSocketManager.send_json()`

**Test File to Update/Augment:** `tests/unit/apis/connectivity/test_ws_manager.py` (Create if it doesn't exist or if tests are elsewhere).

1.  **Test with Valid Pydantic Model Input:**
    *   Mock `self._ws_connection.send_json()`.
    *   Create a sample Pydantic model instance (e.g., one of the new WS subscription request models).
    *   Call `ws_manager.send_json(pydantic_model_instance)`.
    *   Assert that `self._ws_connection.send_json()` was called once.
    *   Assert that the argument passed to the mocked `send_json` is a `dict` that results from `pydantic_model_instance.model_dump(by_alias=True, exclude_none=True)`.
2.  **Test Return Value:** Assert `send_json` returns `True` on successful mock call and `False` if `model_dump` or the underlying send fails (mock these failures).
3.  **No `dict` input test needed:** The signature now strictly requires `BaseModel`. A `TypeError` would occur naturally if a `dict` is passed, which Pydantic/mypy should catch before runtime in typed code.

### Part D: Re-verify `HttpClient.request` Pydantic Model Serialization

**Test File to Review/Augment:** `tests/unit/apis/connectivity/test_http_client.py`

1.  **Review Existing Tests:** Identify tests that pass a Pydantic `BaseModel` instance as the `data` argument to `HttpClient.request`.
2.  **Confirm `model_dump` Usage:** Ensure these tests implicitly or explicitly verify that `model_dump(by_alias=True, exclude_none=not serialize_none_as_null)` is effectively called.
    *   If mocks are used for `aiohttp.ClientSession.request`, check the `json` parameter passed to it.
3.  **Test `serialize_none_as_null` Behavior:**
    *   Create a Pydantic model with optional fields.
    *   Test `HttpClient.request` with `serialize_none_as_null=False` (default): ensure `None` fields are *omitted* from the JSON payload sent.
    *   Test `HttpClient.request` with `serialize_none_as_null=True`: ensure `None` fields are sent as `null` in the JSON payload.
4.  **Hyperliquid `_clean_order_type_fields`:**
    *   Based on Angel's findings from the previous task about the necessity of `_clean_order_type_fields` in `HttpClient`:
        *   If it **is** still necessary (e.g., for `serialize_none_as_null=True` scenarios where HL API rejects `{"limit": null, "market":{...}}`), ensure there's a test case that specifically verifies this cleaning logic works as intended.
        *   If it was found to be **redundant** (because `exclude_none=True` already handles omitting the `None` key from `orderType`), then no specific test for `_clean_order_type_fields` is needed beyond the general `model_dump` tests.

## 5. Files to Create/Modify
-   **Create:** `tests/unit/apis/backpack/models/test_bp_ws_payloads.py`
-   **Create:** `tests/unit/apis/hyperliquid/models/test_hl_ws_payloads.py`
-   **Modify:** `tests/unit/apis/backpack/test_bp_api.py`
-   **Modify:** `tests/unit/apis/hyperliquid/test_hl_api.py`
-   **Modify:** `tests/unit/apis/connectivity/test_ws_manager.py` (or create if structure dictates)
-   **Modify/Review:** `tests/unit/apis/connectivity/test_http_client.py`

## 6. Reporting
-   Confirm that `pytest tests/unit/apis/` runs **completely green**.
-   List all test files created and modified.
-   Provide a brief summary of new tests added and key changes to existing tests.
-   If `HttpClient._clean_order_type_fields` was reviewed, state its confirmed necessity or redundancy.
-   Output all created and modified test files.

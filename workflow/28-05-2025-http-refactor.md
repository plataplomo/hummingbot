# TASK: Integrate WS Subscription Models, Refactor WebSocketManager, and Verify HttpClient Serialization

## 1. Goal
Finalize the Pydantic integration for all outgoing API communications by:
1.  **Refactoring `_construct_subscription_payload` Methods:** Update these methods in `BackpackAPI` and `HyperliquidAPI` to instantiate and return the Pydantic models for WebSocket subscription requests (from `bp_ws_payloads.py` and `hl_ws_payloads.py`, which are now defined).
2.  **Refactoring `WebSocketManager.send_json()`:** Modify this method in `cyberdelta.apis.connectivity.ws_manager.py` to strictly accept only Pydantic `BaseModel` instances. It must serialize these models using `.model_dump(by_alias=True, exclude_none=True)` before sending. Remove any prior handling for `dict` inputs.
3.  **Verify `HttpClient.request` Serialization:** Briefly review and confirm that the existing Pydantic model serialization logic within `HttpClient.request` (for REST API request bodies) is robust and correctly uses `.model_dump(by_alias=True, exclude_none=not serialize_none_as_null)`.

## 2. Context and "Why"
With the WebSocket subscription request Pydantic models now defined, this task connects them to the API client logic and ensures `WebSocketManager` correctly serializes them. This, combined with verifying `HttpClient`'s handling of REST request models, completes our objective of using Pydantic for schema validation and serialization for all structured outgoing messages, enhancing type safety and reliability.

## 3. Project Rules Reminder
- Adhere strictly to all project rules.
- `_construct_subscription_payload` methods perform logic to populate the new Pydantic models.
- `WebSocketManager.send_json()` signature and logic must be updated for `BaseModel` input only.

## 4. Detailed Instructions

### Part A: Refactor `_construct_subscription_payload` Methods to Return Pydantic Models

#### A.1. Modify `cyberdelta/apis/backpack/bp_api.py`
   - **Locate `BackpackAPI._construct_subscription_payload` method.**
   - **Imports:** Import `BackpackRawWsSubscriptionRequest` from `..models.bp_ws_payloads`.
   - **Return Type:** Change annotation from `dict[str, Any] | None` to `BackpackRawWsSubscriptionRequest | None`.
   - **Logic:**
     - Based on input `topic: str`, determine `method_val: Literal["SUBSCRIBE", "UNSUBSCRIBE"]`, `params_val: list[RawBpNonEmptyStringMax128]`.
     - For private streams (e.g., `topic` starts with "account."), determine `signature_val`.
       - **Focus:** If retrieving actual signature components here is complex, for now, instantiate `BackpackRawWsSubscriptionRequest` with `signature=None` (as the model field is optional) and add a `logger.warning` or `TODO` comment about integrating real signature generation later. The priority is returning the correct Pydantic model structure.
     - Instantiate and return `BackpackRawWsSubscriptionRequest` with the determined values.
     - Return `None` if a valid payload cannot be formed (e.g., invalid topic format).

#### A.2. Modify `cyberdelta/apis/hyperliquid/hl_api.py`
   - **Locate `HyperliquidAPI._construct_subscription_payload` method.**
   - **Imports:** Import `HyperliquidRawWsSubscribeRequest` and its specific inner payload types (e.g., `HyperliquidRawWsL2BookSubscriptionPayload`) from `..models.hl_ws_payloads`.
   - **Return Type:** Change annotation from `dict[str, Any] | None` to `HyperliquidRawWsSubscribeRequest | None`.
   - **Logic:**
     - Parse the input `topic: str` (e.g., "l2Book:ETH", "userEvents", "candle:BTC:1m") to get `sub_type` and parameters like `coin`, `user`, `interval`.
     - If `sub_type` is "userEvents" and the `wallet_address` parameter to `_construct_subscription_payload` is `None`, log an error and return `None`.
     - Based on `sub_type`, instantiate the appropriate inner Pydantic subscription payload model (e.g., `HyperliquidRawWsL2BookSubscriptionPayload(type="l2Book", coin=parsed_coin)`).
     - If an inner payload model is successfully created, instantiate and return the top-level `HyperliquidRawWsSubscribeRequest(method="subscribe", subscription=inner_payload_model)`. (Handle "unsubscribe" method if applicable).
     - Return `None` if the topic is unparseable or required information for the inner payload is missing.

### Part B: Refactor `WebSocketManager.send_json()` to Strictly Use Pydantic Models

**File:** `cyberdelta/apis/connectivity/ws_manager.py`

1.  **Import `BaseModel` from `pydantic` at the top of the file.**
2.  **Update `send_json` Signature:**
    - Change `data: dict[str, Any]` to **`data: BaseModel`**. The method now *only* accepts Pydantic models.
3.  **Update Serialization Logic:**
    - Remove any `isinstance(data, dict)` checks or alternative handling for dictionaries.
    - Directly assume `data` is a `BaseModel` instance.
    - Serialize using `payload_to_send = data.model_dump(by_alias=True, exclude_none=True)`.
        - `by_alias=True` is crucial if your WS subscription Pydantic models use aliases.
        - `exclude_none=True` is standard for WebSocket messages to omit optional fields that are `None`. If an API specifically requires `null` for omitted optionals, this would need adjustment for that specific case, but it's rare for subscriptions.
    - The rest of the error handling for the actual send operation (`self._ws_connection.send_json`) remains.

    **Example Snippet:**
    ```python
    # Inside WebSocketManager.send_json method
    # from pydantic import BaseModel # Ensure import

    async def send_json(self, data: BaseModel) -> bool: # Strict BaseModel input
        if not self.is_connected or not self._ws_connection:
            self._logger.error(f"Cannot send JSON, WebSocket not connected to {self._ws_url}.")
            return False
        try:
            payload_to_send = data.model_dump(by_alias=True, exclude_none=True)
            self._logger.debug(f"[{self._exchange_name}] Sending WS JSON: {payload_to_send}")
            await self._ws_connection.send_json(payload_to_send)
            return True
        except asyncio.CancelledError: # Keep specific error handling
            self._logger.warning("Send JSON operation cancelled.")
            return False
        except ConnectionResetError:
            self._logger.error(
                f"Connection reset while trying to send JSON to {self._ws_url}. "
                "Marking as disconnected."
            )
            self._is_connected = False
            self._ws_connection = None # Or trigger reconnect logic
            return False
        except Exception as e: # Catch other errors including model_dump issues
            self._logger.error(f"[{self._exchange_name}] Error during WS send_json (serialize/send): {e}", exc_info=True)
            return False
    ```

### Part C: Verify `HttpClient.request` Serialization (Review Only)

**File to Review:** `cyberdelta/apis/connectivity/http_client.py`

1.  **Locate `HttpClient.request` method.**
2.  **Examine the Pydantic Model Serialization Block:**
    ```python
    if isinstance(request_data, BaseModel):
        json_payload = request_data.model_dump(
            by_alias=True, exclude_none=not serialize_none_as_null
        )
        if serialize_none_as_null and self.exchange_name == "hyperliquid": # Check if self.exchange_name exists
            # Ensure _clean_order_type_fields is available if self.exchange_name is used
            if hasattr(self, '_clean_order_type_fields'):
                 self._clean_order_type_fields(json_payload)
            else:
                 self._logger.warning("HttpClient: _clean_order_type_fields method not found, skipping special HL cleaning.")

    ```
3.  **Confirm:**
    *   `by_alias=True` is used.
    *   `exclude_none=not serialize_none_as_null` correctly handles the flag.
    *   **Hyperliquid `_clean_order_type_fields`:**
        *   This method exists in `HttpClient` to handle specific `orderType` formatting for Hyperliquid when `serialize_none_as_null=True`.
        *   **Question to Answer:** Given that `HyperliquidRequestBuilder` now aims to construct the `HyperliquidRawOrderType` with only one of `limit` or `market` populated (the other being `None`), and if `serialize_none_as_null=False` (meaning `exclude_none=True`) is the common case for Hyperliquid REST requests, is `_clean_order_type_fields` still strictly necessary? It might only be relevant if `serialize_none_as_null=True` is *ever* used for placing Hyperliquid orders *and* Hyperliquid's API errors if the "other" key (limit/market) is present with a `null` value (which `exclude_none=False` would produce).
        *   **Report your findings on whether `_clean_order_type_fields` seems redundant or if it serves a purpose for specific `serialize_none_as_null=True` scenarios with Hyperliquid orders.**

## 5. Files to Modify
-   `cyberdelta/apis/backpack/bp_api.py` (update `_construct_subscription_payload`)
-   `cyberdelta/apis/hyperliquid/hl_api.py` (update `_construct_subscription_payload`)
-   `cyberdelta/apis/connectivity/ws_manager.py` (update `send_json` signature and logic)
-   `cyberdelta/apis/connectivity/http_client.py` (review serialization logic, note findings on `_clean_order_type_fields`).

## 6. Testing Requirements
-   No new unit tests *from Angel* for this refactoring pass. Unit tests for these changes (testing `_construct_subscription_payload` returns Pydantic models, `WebSocketManager.send_json` serializes them, and `HttpClient` details) will be part of the *next major testing phase*.
-   Static analysis (`mypy --strict`, `ruff`) must pass for all modified files.

## 7. Reporting
-   Confirm successful completion of the refactoring.
-   List all modified files.
-   Summarize key changes made to `_construct_subscription_payload` methods and `WebSocketManager.send_json`.
-   Provide a concise report on the review of `HttpClient.request` serialization and your findings on the necessity of `_clean_order_type_fields` for Hyperliquid.
-   Output the changed files.
# TASK: Align Hyperliquid Request Path with Architectural Principles (Business Logic in Services)

## 1. Goal
Refactor the Hyperliquid API request flow (`cyberdelta/apis/hyperliquid/`) to ensure strict adherence to our architectural principle of business logic residing in Service classes, while Raw Pydantic Request Models handle purely syntactic validation.
1.  **Hyperliquid Raw Request Payload Models** (e.g., in `hl_raw_api_request_payloads.py`, `hl_raw_order.py`):
    *   Verify and ensure these models perform **syntactic validation only**. This includes checking field types (using `Literal` for API enums, `RawHl...` common types), presence/optionality, and structure strictly according to Hyperliquid API specifications.
    *   **Remove ALL business logic validators** (e.g., `@model_validator`s checking conditional field requirements like "price for limit orders" or complex value constraints beyond basic format).
2.  **Hyperliquid Service Classes** (e.g., `HyperliquidTradingService`, `HyperliquidAccountService` in `cyberdelta/apis/hyperliquid/services/`):
    *   Implement all necessary **business logic pre-validation** for request formation within the relevant service methods.
    *   These methods accept core CyberDeltaEngine types (e.g., `Decimal`, `OrderSide`), validate the business intent and parameters (e.g., quantity positive, correct arguments for order type), and raise `ValueError` if pre-conditions fail.
3.  **`HyperliquidRequestBuilder` Methods** (`hl_request_builder.py`):
    *   These methods will now receive inputs that have *already been business-validated* by the Service layer.
    *   Their sole responsibility is to **translate and format** these core types into the raw API values (strings, API-specific `Literal` values, numeric string formats, integers) required by the Hyperliquid API, and then instantiate the (now purely syntactic) Raw Pydantic Request Payload model.
    *   Remove any remaining business logic pre-validation from builder methods.

## 2. Context and "Why"
This refactoring ensures consistency with the architectural pattern applied to Backpack. It clearly separates concerns: Services handle business rules and intent, RequestBuilders handle exchange-specific formatting and translation, and Raw Request Models validate the final syntactic structure against the external API contract. This improves maintainability, testability, and robustness.

**Reference Material for Angel:**
-   Hyperliquid API Documentation.
-   Existing Hyperliquid raw models in `cyberdelta/apis/hyperliquid/models/`.
-   `cyberdelta/apis/hyperliquid/models/common_raw_types.py` for `RawHl...` types.
-   The principles just applied to `BackpackRequestBuilder` and its services.

## 3. Project Rules Reminder
- Adhere strictly to all project rules, especially `RULE-ARCH-MODEL-DESIGN-V1`.
- **Raw Request Models:** Purely syntactic. Use `Literal` for API enum strings.
- **Service Methods:** Perform business pre-validation on core type inputs. Raise `ValueError` on failure.
- **RequestBuilder Methods:** Focus on translation/formatting, assuming business-valid inputs.

## 4. Detailed Instructions for Angel (Iterative Process)

**Files to Modify:**
-   Relevant Pydantic model files in `cyberdelta/apis/hyperliquid/models/` (e.g., `hl_raw_api_request_payloads.py`, `hl_raw_order.py`, `hl_raw_transfer_withdrawal.py`).
-   `cyberdelta/apis/hyperliquid/services/hl_trading_service.py`
-   `cyberdelta/apis/hyperliquid/services/hl_account_service.py`
-   `cyberdelta/apis/hyperliquid/hl_request_builder.py`

**General Workflow for each operation (e.g., place_order, cancel_order, withdraw, transfer):**

### Step A: Verify/Correct Raw Request Payload Model (in `cyberdelta/apis/hyperliquid/models/...`)
   1.  Locate the Pydantic model used for the request body (e.g., `HyperliquidRawPlaceOrderAction`, `HyperliquidApiL2UsdTransferRequest` which wraps `HyperliquidRawL2UsdTransferActionDetails`).
   2.  **Critically review and REMOVE any `@model_validator` or `@field_validator` methods performing business logic.** For example, `HyperliquidRawOrderType` had a validator `check_exclusive_order_type`; this logic (ensuring one of `limit` or `market` is provided for the `orderType` object) should be enforced by the builder when it constructs the `orderType` object passed to `HyperliquidRawPlaceOrderAction`. The raw model itself just defines that `limit` and `market` are optional fields within `HyperliquidRawOrderType`.
   3.  Ensure fields use `Literal` for Hyperliquid's specific string enum values (e.g., `tif: Literal["Gtc", "Ioc", "Alo"]` in `HyperliquidRawLimitOrderTypeDetails`) and appropriate `RawHl...` common types for other fields. Field optionality must strictly match the API's expectation for the raw payload.

### Step B: Refactor the Corresponding Service Method (e.g., `HyperliquidTradingService.place_order`)
   1.  **Inputs:** Confirm service methods accept core CyberDeltaEngine types.
   2.  **Add/Move Business Logic Pre-Validation:**
       *   At the beginning of the service method, implement/move all business rule checks.
       *   Example for `place_order` in `HyperliquidTradingService`:
           - Check `quantity > 0`.
           - If `order_type == OrderType.LIMIT`, check `price is not None and price > 0`.
           - If `order_type` involves a stop, check `stop_price is not None and stop_price > 0`.
           - Validate `asset_index` is valid (usually handled by `_get_asset_index_callable` which should raise if symbol not found).
           - Raise `ValueError` if any check fails.
       *   Example for `transfer` in `HyperliquidAccountService` (L2 USD Transfer):
           - Check `amount > 0`.
           - Check `destination_address` is a valid format (basic check, detailed by raw model).
           - Check `is_l2_transfer` (or similar logic based on account types) is appropriate.
   3.  **Call Builder:** After pre-validation, call the `HyperliquidRequestBuilder` method, passing the validated core type inputs.
   4.  **Call `_http_client_requester`:** Pass the Pydantic model from the builder, serialized via `.model_dump(by_alias=True, exclude_none=...)`.

### Step C: Refactor the Corresponding `HyperliquidRequestBuilder` Method
   1.  Locate the builder method (e.g., `build_place_order_payload`).
   2.  **Remove Business Logic Validation:** Delete any business logic checks (these are now in the Service).
   3.  **Focus on Mapping and Formatting:**
       *   Translate core enum inputs to raw Hyperliquid API values (e.g., `OrderSide.BUY` -> `isBuy=True`; `OrderType.MARKET` -> `limitPx="0"`, correct `orderType` object structure; `TimeInForce` -> API `tif` string like `"Gtc"`).
       *   Convert `Decimal` inputs to strings where the API expects string-numbers.
       *   Construct the `HyperliquidRawOrderType` object correctly based on `order_type` input (e.g., providing either a `limit` or `market` sub-object).
   4.  Ensure return type is the correct Raw Pydantic Request Model.

**Specific Hyperliquid Models/Builders to Review & Refactor:**

1.  **Place Order:**
    *   Models: `HyperliquidRawPlaceOrderAction`, `HyperliquidRawOrderType`, `HyperliquidRawLimitOrderTypeDetails`, `HyperliquidRawMarketOrderTypeDetails`, `HyperliquidRawTriggerDetails`.
        *   `HyperliquidRawOrderType`: Remove `check_exclusive_order_type` validator. Builder must ensure only one of `limit` or `market` is populated.
    *   Service: `HyperliquidTradingService.place_order`. Add pre-validation for `price` with `OrderType.LIMIT`, `stop_price` for stop orders, valid `asset_index`.
    *   Builder: `HyperliquidRequestBuilder.build_place_order_payload`. Remove business validation. Focus on mapping `OrderType` to the correct `HyperliquidRawOrderType` structure (populating `limit` or `market` sub-object) and `limitPx` (e.g., "0" for market). Map `TimeInForce` to `tif` string for limit orders.

2.  **Cancel Order:**
    *   Model: `HyperliquidRawCancelOrderAction`. (Likely simple, check for any business validators).
    *   Service: `HyperliquidTradingService.cancel_order`. Add pre-validation for `order_id` (e.g., positive int), valid `asset_index`.
    *   Builder: `HyperliquidRequestBuilder.build_cancel_order_payload`. Focus on formatting `asset_index` and `oid`.

3.  **L2 USD Transfer:**
    *   Model: `HyperliquidApiL2UsdTransferRequest` (wraps `HyperliquidRawL2UsdTransferActionDetails` which wraps `HyperliquidRawL2UsdTransferPayload`).
        *   `HyperliquidRawL2UsdTransferPayload`: Ensure `amount` field's Pydantic type (`RawPositiveFiniteDecimalStr`) only checks string format; positivity should be builder/service concern. Correct this if `RawPositiveFiniteDecimalStr` itself implies value checking beyond format. **Clarification**: `RawPositiveFiniteDecimalStr` is a string format validator. The *builder/service* must ensure the *input Decimal* is positive. The model field itself is `str`.
    *   Service: `HyperliquidAccountService.transfer` (if it handles L2 USD transfers). Add pre-validation for `amount > 0`, valid `destination_address` format.
    *   Builder: `HyperliquidRequestBuilder.build_l2_usd_transfer_payload`. Ensure it takes `amount: Decimal`, validates `amount > 0`, then converts to string.

4.  **Withdrawal (ETH & Token):**
    *   Models: `HyperliquidApiEthWithdrawalRequest` (wraps `HyperliquidRawEthWithdrawalActionPayload`), `HyperliquidApiTokenWithdrawalRequest` (wraps `HyperliquidRawWithdrawalToL1ActionPayload`).
        *   Check `amount` fields: ensure they are string types validated by `RawPositiveFiniteDecimalStr` (or similar if HL expects non-string numbers directly).
    *   Service: `HyperliquidAccountService.withdraw`. Add pre-validation for `amount > 0`, valid `destination_address`.
    *   Builder: `HyperliquidRequestBuilder.build_withdrawal_payload`. Ensure it takes `amount: Decimal`, validates `amount > 0`, then converts to string if payload model field is string.

5.  **Review all other request payload definitions** in `hl_raw_api_request_payloads.py`, `hl_raw_order.py`, `hl_raw_transfer_withdrawal.py`, etc., and their corresponding builder methods in `hl_request_builder.py` for adherence to these principles.

## 5. WebSocket Subscription Payloads (Phase C Preview - For Awareness)
   - The methods `HyperliquidAPI._construct_subscription_payload` currently return `dict`.
   - In a subsequent task (Phase C), these will be refactored to return Pydantic models (e.g., `HyperliquidRawWsSubscriptionRequest`) defined in `hl_raw_api_request_payloads.py` (or a new WS-specific file). `WebSocketManager.send_json()` will then be updated to serialize these. You don't need to implement this part now, but ensure your current refactoring doesn't conflict with this future step.

## 6. Files to Modify
-   `cyberdelta/apis/hyperliquid/models/` (various files, especially those defining request structures like `hl_raw_api_request_payloads.py`, `hl_raw_order.py`, `hl_raw_transfer_withdrawal.py`)
-   `cyberdelta/apis/hyperliquid/services/hl_trading_service.py`
-   `cyberdelta/apis/hyperliquid/services/hl_account_service.py`
-   `cyberdelta/apis/hyperliquid/hl_request_builder.py`

## 7. Testing Requirements
-   No new unit tests from Angel yet. This will be the focus of the *next* task (Phase C, part 4).
-   Static analysis (`mypy --strict`, `ruff`) must pass.

## 8. Reporting
-   Confirm successful completion.
-   List all modified files.
-   Summarize key changes, especially detailing where business logic validation was moved from models/builders to Service methods for key operations.
-   Output the changed files.
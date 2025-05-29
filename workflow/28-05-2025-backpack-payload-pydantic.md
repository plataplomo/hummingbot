
# TASK: Define All Backpack Raw Request Pydantic Models and Update BackpackRequestBuilder

## 1. Goal
1.  Systematically identify all request body schemas for Backpack API's POST, PUT, DELETE, and PATCH operations as defined in the provided `openapi_backpack.json`.
2.  For each identified request schema, define a corresponding Pydantic model in **new files**: `cyberdelta/apis/backpack/models/bp_raw_request_*.py` (group them by payload logic).
3.  Refactor all relevant methods in `cyberdelta/apis/backpack/bp_request_builder.py` that construct these request bodies. These methods must be updated to:
    a.  Accept input parameters using CyberDeltaEngine's core enums (from `cyberdelta.core.models.enums`, e.g., `OrderSide`, `OrderType`) and standard Python types (`Decimal`, `str`, `bool`).
    b.  Implement the logic to map these core enum inputs and other parameters to the raw string/enum values required by the Backpack API (as determined from `openapi_backpack.json`).
    c.  Instantiate and return the newly defined Pydantic request payload model instance, populated with these raw API values.

## 2. Context and "Why"
This task addresses **Pydantic Point 1 (API Request Payloads/Parameters)** for the Backpack exchange. It's critical for ensuring data sent *to* Backpack is pre-validated against its schema. `BackpackRequestBuilder` will become the explicit translation layer from our internal types to Backpack's raw API request values. This replaces the previous approach of builders returning raw dictionaries.

**You have `openapi_backpack.json` available in the root folder.** This is your primary source of truth for request schemas. Use `grep` or other search tools to find correct schemas. This file is 10k lines long and will fill your context too fast (80k tokens), be careful. 
    - **Discovery:** Use your analysis capabilities (e.g., searching for `requestBody` under `paths`, or schema definitions under `components.schemas` that are referenced by request bodies like `OrderExecutePayload`, `OrderCancelPayload`) to identify all necessary request payload structures.
    - **Guidance by Example:** Refer to existing Raw Pydantic models in `cyberdelta/apis/hyperliquid/models/hl_raw_*.py` and `cyberdelta/apis/backpack/models/*` (for response models) to understand the expected style, use of aliases, common raw types, and model configuration.

## 3. Project Rules Reminder
- Adhere strictly to all project rules: `RULE-STATIC-ANALYSIS-V3`, `RULE-RUNTIME-SAFETY-V3`, `RULE-NO-SILENCING-V4`, `RULE-CONFIG-INTEGRITY-V3`, `RULE-ARCH-MODEL-DESIGN-V1`.
- New Pydantic models for request payloads must be "Raw" models:
    - Mirror `openapi_backpack.json` schemas for request bodies precisely.
    - Use `Field(..., alias="jsonKeyName")` if Python attribute names differ from JSON keys found in the spec.
    - Leverage common raw types from `cyberdelta/apis/backpack/models/bp_common_raw_types.py` (e.g., `RawBpParsableFiniteDecimalString`, `RawBpNonEmptyStringMax64`, `RawBpUint32`) for validating other field types.
    - All new request payload models must have `model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)`.
- `BackpackRequestBuilder` Methods:
    - **Inputs:** Ensure method parameters use `cyberdelta.core.models.enums` (e.g., import `OrderSide as CoreOrderSide`) and standard Python types (`Decimal`, `str`, `bool`).
    - **Utils:** ensure using utils from `cyberdelta.utils.parsing`
    - **Logic:** Implement the mapping from these core types to the raw API values/types required by the fields of the new Pydantic request payload models. For example, map `CoreOrderSide.BUY` to the Backpack API's representation (e.g., `"Bid"`). Convert `Decimal` inputs to strings for fields typed as `RawBpParsableFiniteDecimalString`.
    - **Return Type:** Must be the new Pydantic request payload model instance.

## 4. Detailed Sub-Steps (Iterative: Model Definition -> Builder Update)

You will create/update the files:
- `cyberdelta/apis/backpack/models/bp_raw_*.py` (for new models)
- `cyberdelta/apis/backpack/bp_request_builder.py` (for method updates)

For each relevant Backpack API operation that involves a request body:

**A. Identify the Schema & Define the Pydantic Model:**
   1.  Locate the operation (e.g., POST `/api/v1/order`) in `openapi_backpack.json`.
   2.  Find its `requestBody` definition and the `$ref` to its schema in `#/components/schemas/` (e.g., `OrderExecutePayload`).
   3.  In `bp_raw_api_request_payloads.py`, define a Pydantic model (e.g., `BackpackRawOrderExecuteRequest`) mirroring this schema.
       -   Determine field names, types, optionality, and aliases from the schema.
       -   If the schema specifies enum values for a string field, create a corresponding `Enum` or use `Literal`.
       -   Apply relevant `RawBp...` types from `bp_common_raw_types.py` for validation.
       -   Remember about our strict rules that RAW models don't do business logic! Only basic validation.

**B. Update the Corresponding `BackpackRequestBuilder` Method:**
   1.  Locate the builder method that constructs the payload for this operation (e.g., `build_place_order_payload`).
   2.  Change its return type annotation to the Pydantic model defined in step A.1.
   3.  Ensure its input parameters use core CyberDeltaEngine enums and types (e.g., `side: CoreOrderSide`, `quantity: Decimal`).
   4.  Implement the mapping logic from these input types to the raw values/types expected by the fields of the Pydantic request model.
   5.  Collect all mapped arguments into a dictionary. Filter out entries where the value is `None` if the corresponding Pydantic model field is optional and should be omitted if not provided (this allows Pydantic model field defaults to apply if any, or simply omits the field).
   6.  Instantiate and return the Pydantic request model using these processed arguments: `return BackpackRaw<Action>Request(**final_payload_args)`.
   7.  Add `import logging; logger = logging.getLogger(__name__)` to the builder if warnings (e.g., for type mappings) are needed.

**Specific Endpoints/Payloads to Address (Minimum List - discover others as needed):**

1.  **Place Order:**
    *   OpenAPI Path & Method: POST `/api/v1/order`
    *   Schema Name: `OrderExecutePayload`
    *   Builder Method: `build_place_order_payload`
    *   Notes: Pay attention to mapping core `OrderType` (which includes stop/take-profit variants) to Backpack's simpler `BpApiRequestOrderType` (`"Market"`, `"Limit"`). Log warnings if input `order_type` implies trigger functionality (like `CoreOrderType.STOP_MARKET`) that isn't directly supported by basic fields in `OrderExecutePayload` schema (SL/TP fields are mentioned in changelog; verify their presence in the *current* payload schema).

2.  **Cancel Single Order:**
    *   OpenAPI Path & Method: DELETE `/api/v1/order`
    *   Schema Name: `OrderCancelPayload`
    *   Builder Method: `build_cancel_order_payload`
    *   Notes: Ensure validator for "one of `orderId` or `clientId`".

3.  **Cancel All Orders (Per Symbol):**
    *   OpenAPI Path & Method: DELETE `/api/v1/orders`
    *   Schema Name: `OrderCancelAllPayload`
    *   Builder Method: `build_cancel_all_orders_payload`
    *   Notes: OpenAPI schema indicates `symbol` is required. Refactor builder input `symbol: str | None` to `symbol: str`. Input `order_type_filter: str | None` for the builder should map to the API's `orderType` field (which is `BpApiCancelOrderType | None`).

4.  **Request Withdrawal:**
    *   OpenAPI Path & Method: POST `/wapi/v1/capital/withdrawals`
    *   Schema Name: `AccountWithdrawalPayload`
    *   Builder Method: `build_withdraw_payload`
    *   Notes: Define `BpApiBlockchain` and `BpApiAsset` enums with *all* values from the spec. Carefully map input `network` and `asset` strings to these enums. Check handling of `addressTag` (if present in schema).

5.  **Update Account Settings:**
    *   OpenAPI Path & Method: PATCH `/api/v1/account`
    *   Schema Name: `UpdateAccountSettingsRequest`
    *   Builder Method: `build_update_account_settings_payload` (create if it doesn't exist).

6.  **Internal Transfer (Custom, not in OpenAPI directly for POST body):**
    *   Builder Method: `build_internal_transfer_payload`
    *   Existing Output: `{"symbol": "USDC", "quantity": "100", "fromAccount": "SPOT", "toAccount": "FUTURES", "clientId": "..."}`
    *   Action: Define `BackpackRawInternalTransferRequest(BaseModel)` based on this existing structure. Update builder input `amount_str: str` to `amount: Decimal`.

7.  **(If used/planned) Borrow/Lend Execute:**
    *   OpenAPI Path & Method: POST `/api/v1/borrowLend`
    *   Schema Name: `BorrowLendExecutePayload`
    *   Builder Method: (Create if needed) `build_borrow_lend_execute_payload`

8.  **(If used/planned) RFQ Quote Submit:**
    *   OpenAPI Path & Method: POST `/api/v1/rfq/quote`
    *   Schema Name: `QuotePayload`
    *   Builder Method: (Create if needed) `build_rfq_quote_payload`

## 5. File Management
   - Ensure `cyberdelta/apis/backpack/models/__init__.py` exports the new `bp_raw_api_request_payloads.py` module or its contents if you organize models into multiple files.
   - Add `__init__.py` to `cyberdelta/apis/backpack/models/` if it's missing.

## 6. Testing Requirements
   - No new unit tests are required from Angel for *defining* these models or *refactoring* the builder methods.
   - Human Lead will update unit tests that use `BackpackRequestBuilder` methods to expect Pydantic models as return types.
   - Static analysis (`mypy --strict`, `ruff`) must pass for all modified and new files.

## 7. Reporting
   - Confirm successful completion of all model definitions and builder updates for the specified payloads.
   - List all created/modified files.
   - Specifically list all new Pydantic models defined for request payloads.
   - Output the changed/new files.
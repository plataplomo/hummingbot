
**We WILL use the EXACT SAME PATTERN for Backpack request payload models as we used for Hyperliquid.** This means:

1.  **Primary use of `Literal` types** in the Pydantic request payload models for fields where the API expects a fixed set of specific strings (e.g., `orderType: Literal["Market", "Limit"]`).
2.  **Leverage `Annotated` types from `bp_common_raw_types.py`** for other common validation needs (e.g., `RawBpParsableFiniteDecimalString`, `RawBpNonEmptyStringMax64`, `RawBpUint32`, `RawBpStrictBool`). These common types might internally use `validate_str_field`, `parse_decimal_value`, `validate_enum_field` (against a `set` of strings if a field has many possible string values not suitable for `Literal`).
3.  **Avoid creating new, redundant `BpApi<Name>Enum` classes in `bp_raw_request_*.py` if `Literal` or existing common raw types can achieve the same strict validation based on `openapi_backpack.json`.** New Enums should only be for truly new, complex enum concepts not covered.

# TASK: Define Backpack Raw Request Pydantic Models (Consistent with HL Patterns) & Update Builder

## 1. Goal
1.  Systematically identify all request body schemas for Backpack API's POST, PUT, DELETE, and PATCH operations as defined in `openapi_backpack.json`.
2.  For each identified request schema, define a corresponding Pydantic model in a **new files**: `cyberdelta/apis/backpack/models/bp_raw_request_*.py` (group in 2-3 models files based on logic)
    **Crucially, these models MUST follow the established validation patterns used for Hyperliquid raw request models:**
    *   Employ `typing.Literal` for fields where `openapi_backpack.json` specifies a fixed set of string values (e.g., `orderType: Literal["Market", "Limit"]`).
    *   Utilize existing `Annotated` types from `cyberdelta/apis/backpack/models/bp_common_raw_types.py` (e.g., `RawBpParsableFiniteDecimalString`, `RawBpNonEmptyStringMax64`, `RawBpUint32`, `RawBpStrictBool`) for common field validations.
    *   **Avoid creating new `BpApi<Name>Enum` classes if `Literal` or common raw types suffice.**
    *   Models must strictly validate only the external contract (syntax, basic types, formats, field presence/optionality as per `openapi_backpack.json`) and **MUST NOT contain business logic validators** (e.g., "price can be negative if it's a valid Decimal").
3.  Refactor methods in `cyberdelta/apis/backpack/bp_request_builder.py` to:
    a.  Accept input parameters using CyberDeltaEngine's core enums (from `cyberdelta.core.models.enums`) and standard Python types (`Decimal`, `str`, `bool`).
    b.  Implement the logic to map these core inputs to the raw string values required by the Backpack API (which will be validated by the `Literal` or common raw types in the Pydantic request models).
    c.  Instantiate and return the newly defined Pydantic request payload models.

## 2. Context and "Why"
This task addresses **Pydantic Point 1 (API Request Payloads/Parameters)** for Backpack, ensuring consistency with Hyperliquid's established raw request model patterns. Raw Pydantic models at this boundary validate the *syntactic correctness* of outgoing data. Business rules for request formation belong in the `RequestBuilder` *before* raw model instantiation.

**Reference `openapi_backpack.json` for Backpack's expected request schemas.**
**Emulate validation patterns from `cyberdelta/apis/hyperliquid/models/` (especially request payloads and common types) and use `cyberdelta/apis/backpack/models/bp_common_raw_types.py`.**

## 3. Project Rules Reminder
- Adhere strictly to all project rules.
- **Raw Request Payload Models:**
    - Mirror `openapi_backpack.json` schemas (fields, types, optionality, `alias`).
    - **Validation Strategy:**
        - **Use `Literal["Val1", "Val2"]` for fields with fixed API string values.**
        - Use `RawBp...` types from `bp_common_raw_types.py` for other fields.
        - **NO business logic `@model_validator`s.**
    - Config: `model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)`.
- **`BackpackRequestBuilder` Methods:**
    - Inputs: Use `cyberdelta.core.models.enums` and standard Python types.
    - Logic: Perform business rule checks (e.g., price required for LIMIT). Map core types to raw API string values. Instantiate Raw Pydantic model.
    - Return Type: The new Pydantic request payload model instance.

## 4. Detailed Sub-Steps (Iterative: Model Definition & Validation -> Builder Update)

**Files for new models:** `cyberdelta/apis/backpack/models/bp_raw_request_*.py` (Create if it doesn't exist).
   - Add `__init__.py` to `cyberdelta/apis/backpack/models/` if missing. Ensure new module is exported in `cyberdelta/apis/backpack/models/__init__.py`.
**File for builder updates:** `cyberdelta/apis/backpack/bp_request_builder.py`.

**Common Imports for `bp_raw_request_*.py`:**
```python
from typing import Literal # For Literal types
from pydantic import BaseModel, ConfigDict, Field # No model_validator needed for raw models here

from cyberdelta.apis.backpack.models.bp_common_raw_types import (
    RawBpUint32, RawBpOptionalStrictBool, RawBpParsableFiniteDecimalString,
    RawBpNonEmptyStringMax64, RawBpNonEmptyStringMax32, RawBpNonEmptyStringMax128
    # Add others like RawBpSideString (if it defines "Bid"/"Ask"), RawBpTimeInForceString (if "GTC"/"IOC"/"FOK")
    # if these are better than direct Literal. For simple fixed sets, Literal is fine.
)
```

---

### Sub-step 4.A: `OrderExecutePayload` (Placing an Order)

#### 4.A.1. Define `BackpackRawOrderExecuteRequest` Model (in `bp_raw_api_request_payloads.py`)
   - Schema: `#/components/schemas/OrderExecutePayload`.
   - **Fields using `Literal` and common raw types:**
     ```python
     class BackpackRawOrderExecuteRequest(BaseModel):
         orderType: Literal["Market", "Limit"] # As per OrderTypeEnum in OpenAPI
         side: Literal["Bid", "Ask"]           # As per Side enum in OpenAPI
         symbol: RawBpNonEmptyStringMax64

         clientId: RawBpUint32 | None = Field(default=None)
         postOnly: RawBpOptionalStrictBool | None = Field(default=None)
         price: RawBpParsableFiniteDecimalString | None = Field(default=None)
         quantity: RawBpParsableFiniteDecimalString | None = Field(default=None)
         quoteQuantity: RawBpParsableFiniteDecimalString | None = Field(default=None)
         reduceOnly: RawBpOptionalStrictBool | None = Field(default=None)
         selfTradePrevention: Literal["RejectTaker", "RejectMaker", "RejectBoth"] | None = Field(default=None) # As per SelfTradePrevention in OpenAPI
         timeInForce: Literal["GTC", "IOC", "FOK"] | None = Field(default=None) # As per TimeInForce in OpenAPI

         # SL/TP Fields: Add as optional if schema confirms, using Literal for triggerBy if applicable.
         # e.g., stopLossTriggerPrice: RawBpParsableFiniteDecimalString | None = Field(default=None)
         # e.g., stopLossTriggerBy: Literal["LastPrice", "MarkPrice", "IndexPrice"] | None = Field(default=None)

         model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

         # NO @model_validator for business logic (e.g., price required for LIMIT).
         # Builder handles this.
     ```

#### 4.A.2. Update `BackpackRequestBuilder.build_place_order_payload`
   - Inputs: Use `OrderSide`, `OrderType`, `TimeInForce`, `Decimal`.
   - **Logic:**
     - **Perform business logic validation first (e.g., price required for LIMIT CoreOrderType). Raise `ValueError` if invalid.**
     - Map core enums to specific **strings** for `Literal` fields:
       - `OrderSide.BUY` -> `"Bid"`; `OrderSide.SELL` -> `"Ask"`
       - `OrderType.LIMIT` -> `"Limit"`; `OrderType.MARKET` -> `"Market"`
       - `TimeInForce.GTC` -> `"GTC"`; etc.
     - Convert `Decimal` to `str`. Convert `client_order_id: str` to `int`.
   - Return: `BackpackRawOrderExecuteRequest` instance.

---

### Sub-step 4.B: `OrderCancelPayload` (Cancelling a Single Order)

#### 4.B.1. Define `BackpackRawOrderCancelRequest` Model
   - Schema: `#/components/schemas/OrderCancelPayload`.
   - Fields:
     - `symbol: RawBpNonEmptyStringMax64`
     - `orderId: RawBpNonEmptyStringMax64 | None = Field(default=None)`
     - `clientId: RawBpUint32 | None = Field(default=None)`
   - **NO business logic `@model_validator` here.**

#### 4.B.2. Update `BackpackRequestBuilder.build_cancel_order_payload`
   - Inputs: `symbol: str`, `order_id: str | None`, `client_order_id: str | None`.
   - **Logic:**
     - **Business logic:** Ensure one of `order_id` or `client_order_id` is provided, not both. Raise `ValueError` if not.
     - Convert `client_order_id: str` to `int`.
   - Return: `BackpackRawOrderCancelRequest`.

---

### Sub-step 4.C: `OrderCancelAllPayload` (Cancelling All Orders)

#### 4.C.1. Define `BackpackRawOrderCancelAllRequest` Model
   - Schema: `#/components/schemas/OrderCancelAllPayload`.
   - Fields:
     - `symbol: RawBpNonEmptyStringMax64` (Required)
     - `orderType: Literal["RestingLimitOrder", "ConditionalOrder"] | None = Field(default=None)` (As per `CancelOrderTypeEnum` in OpenAPI)

#### 4.C.2. Update `BackpackRequestBuilder.build_cancel_all_orders_payload`
   - Inputs: `symbol: str` (mandatory), `order_type_filter: str | None = None`.
   - Logic: If `order_type_filter` is provided, validate it's one of the allowed Literal strings. If not, log warning and pass `None` (or raise `ValueError` if strict).
   - Return: `BackpackRawOrderCancelAllRequest`.

---

### Sub-step 4.D: `AccountWithdrawalPayload` (Requesting a Withdrawal)

#### 4.D.1. Define `BackpackRawAccountWithdrawalRequest` Model
   - Schema: `#/components/schemas/AccountWithdrawalPayload`.
   - Fields:
     - `address: RawBpNonEmptyStringMax128`
     - `blockchain: Literal["Arbitrum", "Base", ..., "Solana", "Ethereum", "XRP"]` (Populate with ALL values from OpenAPI `Blockchain` enum)
     - `quantity: RawBpParsableFiniteDecimalString`
     - `symbol: Literal["BTC", "ETH", ..., "USDC"]` (Populate with ALL values from OpenAPI `Asset` enum)
     - Optional fields like `clientId`, `twoFactorToken` with `RawBp...` types.
     - `addressTag` if present in this specific schema.
   - **NO `@model_validator` for quantity positivity here.** Builder handles this.

#### 4.D.2. Update `BackpackRequestBuilder.build_withdraw_payload`
   - Inputs: `asset: str` (core asset name), `amount: Decimal`, `address: str`, `network: str | None`, `tag: str | None`, etc.
   - **Logic:**
     - **Business logic:** Ensure `amount` is positive. Raise `ValueError` if not.
     - Map input `network` (string) to one of the `Literal` values for `blockchain`. Raise `ValueError` if mapping fails.
     - Map input `asset` (string) to one of the `Literal` values for `symbol`. Raise `ValueError` if mapping fails.
     - Convert `amount: Decimal` to string.
   - Return: `BackpackRawAccountWithdrawalRequest`.

---

### Subsequent Sub-steps (4.E through 4.H and beyond):
   - **For each remaining payload (`UpdateAccountSettingsRequest`, custom `InternalTransferPayload`, etc.):**
     1.  **Define the `BackpackRaw<ActionName>Request` Model:**
         -   Strictly use OpenAPI schema for fields, types, optionality.
         -   Employ `Literal` for API enum strings.
         -   Use `RawBp...` common types for other fields.
         -   **NO business logic `@model_validator`s.**
     2.  **Update/Create the `BackpackRequestBuilder` method:**
         -   Inputs: Core enums and Python types.
         -   **Logic:** Implement business rule pre-validation. Map inputs to raw API string values. Instantiate and return the Pydantic model.

## 5. Reporting
   - Confirm successful completion of all model definitions and builder updates.
   - List all created/modified files.
   - Specifically list all new Pydantic models defined for request payloads.
   - Output the changed/new files.
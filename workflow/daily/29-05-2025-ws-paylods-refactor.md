# TASK: Define Pydantic Models for WebSocket Subscription Request Payloads (Schema-Driven)

## 1. Goal
Create Pydantic models to strictly represent the JSON request payloads used for subscribing (and unsubscribing) to WebSocket streams for both Backpack and Hyperliquid exchanges. These models will be used by the `_construct_subscription_payload` methods in their respective API clients.

**Specific Deliverables:**
1.  A new file: `cyberdelta/apis/backpack/models/bp_ws_payloads.py` containing Pydantic models for Backpack WebSocket subscription requests, derived **directly from `openapi_backpack.json`**.
2.  A new file: `cyberdelta/apis/hyperliquid/models/hl_ws_payloads.py` containing Pydantic models for Hyperliquid WebSocket subscription requests, based on the **explicit schemas provided below**.
3.  Update `cyberdelta/apis/backpack/models/__init__.py` and `cyberdelta/apis/hyperliquid/models/__init__.py` to export the contents of these new files. Create these `__init__.py` files if they don't exist.

## 2. Context and "Why"
This is the first sub-step of "Phase C" in our Pydantic integration. Defining Pydantic models for outgoing WebSocket subscription messages provides schema validation, clarity, and type safety. These models validate the *structure and basic types* of the subscription messages we send, based on official or provided schemas.

## 3. Project Rules Reminder
- Adhere strictly to all project rules.
- **Raw Models Only:** Models must be "Raw," strictly reflecting the schemas.
- **Syntactic Validation Only:** NO business logic validators.
- **Model Configuration:** `model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)`.
- Use `Literal` for fixed string values and appropriate `RawBp...` or `RawHl...` types from `..._common_raw_types.py`.

## 4. Detailed Instructions for Model Definitions

### 4.1. Create `cyberdelta/apis/backpack/models/bp_ws_payloads.py`

   - **Schema Source:** Angel, you must **search `openapi_backpack.json` (provided in your context) for the exact schema of WebSocket subscription and unsubscription requests.** Look under the "Streams" tag, specifically the "Usage -> Subscribing" section, for the JSON structure.
     - Pay attention to the `method` field (`"SUBSCRIBE"` or `"UNSUBSCRIBE"`).
     - Pay attention to the `params` field (an array of stream name strings).
     - Pay attention to the optional `signature` field for private streams, which is an array/tuple of four strings: `["<verifying key>", "<signature>", "<timestamp>", "<window>"]`.
   - **Required Imports:** `Literal` from `typing`; `BaseModel`, `ConfigDict`, `Field` from `pydantic`; relevant `RawBp...` types from `.bp_common_raw_types` (e.g., `RawBpNonEmptyStringMax128` for stream names, and types appropriate for base64 encoded keys/signatures, and stringified timestamps/windows for the signature tuple elements).

   - **Define `BackpackRawWsSubscriptionRequest(BaseModel)` based on your findings:**
     - `method: Literal["SUBSCRIBE", "UNSUBSCRIBE"]` (or as specified in the schema).
     - `params: list[RawBpNonEmptyStringMax128]` (or other appropriate string type based on stream name examples).
     - `signature: tuple[<Type1>, <Type2>, <Type3>, <Type4>] | None = Field(default=None)`
       - Replace `<TypeN>` with appropriate `RawBp...String...` types from `bp_common_raw_types.py` suitable for base64 encoded Ed25519 keys/signatures, and string representations of millisecond timestamps and windows. For example, key/signature might need `RawBpNonEmptyStringMax255` (or similar for base64 length), while timestamp/window strings might use `RawBpNonEmptyStringMax64`.

### 4.2. Create `cyberdelta/apis/hyperliquid/models/hl_ws_payloads.py`

   - **Schema Source:** Use the **explicit schemas provided below** for Hyperliquid WebSocket subscription requests.
   - **Required Imports:** `Literal`, `Union` from `typing`; `BaseModel`, `ConfigDict`, `Field` from `pydantic`; relevant `RawHl...` types from `..common_raw_types` (e.g., `RawHlAssetString64HL`, `RawHlLaxEthereumAddressStrHL`, `RawHlTimeframeString`). If `RawHlTimeframeString` isn't in `hl_common_raw_types.py`, define it there first as `Annotated[str, BeforeValidator(lambda v: validate_str_field(v, field_name="timeframe_string", max_length=8, allow_empty=False))]`.

   - **Define Inner Subscription Payload Detail Models (These go into the `"subscription"` field):**
     ```python
     # In cyberdelta/apis/hyperliquid/models/hl_ws_payloads.py

     class HyperliquidRawWsL2BookSubscriptionPayload(BaseModel):
         type: Literal["l2Book"]
         coin: RawHlAssetString64HL # From hl_common_raw_types
         model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

     class HyperliquidRawWsTradesSubscriptionPayload(BaseModel):
         type: Literal["trades"]
         coin: RawHlAssetString64HL
         model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

     class HyperliquidRawWsUserEventsSubscriptionPayload(BaseModel):
         type: Literal["userEvents"]
         user: RawHlLaxEthereumAddressStrHL # From hl_common_raw_types
         model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

     class HyperliquidRawWsCandleSubscriptionPayload(BaseModel):
         type: Literal["candle"]
         coin: RawHlAssetString64HL
         interval: RawHlTimeframeString # Ensure this type exists or is defined
         model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

     # Add other inner payload types if Hyperliquid supports more (e.g., "allMids")
     # class HyperliquidRawWsAllMidsSubscriptionPayload(BaseModel):
     #     type: Literal["allMids"]
     #     model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
     ```

   - **Define Top-Level `HyperliquidRawWsSubscribeRequest(BaseModel)`:**
     ```python
     # In cyberdelta/apis/hyperliquid/models/hl_ws_payloads.py

     class HyperliquidRawWsSubscribeRequest(BaseModel):
         method: Literal["subscribe", "unsubscribe"] # Hyperliquid uses lowercase
         subscription: Union[
             HyperliquidRawWsL2BookSubscriptionPayload,
             HyperliquidRawWsTradesSubscriptionPayload,
             HyperliquidRawWsUserEventsSubscriptionPayload,
             HyperliquidRawWsCandleSubscriptionPayload
             # Add HyperliquidRawWsAllMidsSubscriptionPayload if defined
         ]
         model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
     ```

### 4.3. Update `__init__.py` Files
   - In `cyberdelta/apis/backpack/models/__init__.py`, add `from .bp_ws_payloads import *` (or list specific models like `BackpackRawWsSubscriptionRequest`).
   - In `cyberdelta/apis/hyperliquid/models/__init__.py`, add `from .hl_ws_payloads import *` (or list specific models like `HyperliquidRawWsSubscribeRequest` and its inner payload types).

## 5. Testing Requirements
-   No unit tests are required from Angel for *this specific sub-step* of model definition. Tests will be created in a subsequent sub-step.
-   Static analysis (`mypy --strict`, `ruff`) must pass for all new and modified files.

## 6. Reporting
-   Confirm successful creation of the new Pydantic models and updates to `__init__.py` files.
-   Output the contents of the new files:
    - `cyberdelta/apis/backpack/models/bp_ws_payloads.py`
    - `cyberdelta/apis/backpack/models/__init__.py`
    - `cyberdelta/apis/hyperliquid/models/hl_ws_payloads.py`
    - `cyberdelta/apis/hyperliquid/models/__init__.py`
-   If `RawHlTimeframeString` was newly defined, also output `cyberdelta/apis/hyperliquid/models/common_raw_types.py`.

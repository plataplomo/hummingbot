
**Prompt for AI Coder (Angel): Model Backpack WebSocket Signature Components**

**Project:** CyberDeltaEngine
**Context:** We are enhancing the robustness and clarity of our authentication components by using Pydantic models for structured data. The `BackpackEd25519Authenticator.get_ws_subscription_signature_tuple` method currently returns a raw tuple for WebSocket signature components.

**Goal:**
Define a Pydantic model, `BackpackWsSignatureComponents`, to represent the components of a Backpack WebSocket subscription signature. Refactor `BackpackEd25519Authenticator`, `BackpackAPI`, `BackpackWsMessageRouter`, and `BackpackRawWsSubscriptionRequest` to use this new model.

**Why:**
Using a Pydantic model provides named fields, improves type safety, enhances readability, and aligns with our project's "Model Architecture V1" rule for using Pydantic models at component boundaries.

**What to do (Step-by-Step):**

1.  **Define `BackpackWsSignatureComponents` Model:**
    *   **Location:** Create this model within a relevant file, e.g., `cyberdelta/apis/backpack/models/bp_auth_models.py` (create if it doesn't exist) or alongside `BackpackRawWsSubscriptionRequest` in `cyberdelta/apis/backpack/models/bp_ws_payloads.py`. Let's choose `bp_ws_payloads.py` for now to keep WS related request/component models together.
    *   **Fields:**
        *   `api_key: RawBpNonEmptyStringMax255` (using `RawBpNonEmptyStringMax255` from `bp_common_raw_types.py` for consistency and validation)
        *   `timestamp: RawBpNonEmptyStringMax64`
        *   `window: RawBpNonEmptyStringMax64`
        *   `signature: RawBpNonEmptyStringMax255`
    *   **Config:** `model_config = ConfigDict(extra="forbid", frozen=True)`

2.  **Update `BackpackEd25519Authenticator.get_ws_subscription_signature_tuple`:**
    *   **File:** `cyberdelta/apis/backpack/bp_auth.py`
    *   **Class:** `BackpackEd25519Authenticator`
    *   **Method:** Rename to `get_ws_subscription_signature_components`.
    *   **Return Type:** Change from `tuple[str, str, str, str]` to `BackpackWsSignatureComponents`.
    *   **Logic:** Instantiate and return `BackpackWsSignatureComponents` instead of the raw tuple.

3.  **Update `BackpackRawWsSubscriptionRequest` Model:**
    *   **File:** `cyberdelta/apis/backpack/models/bp_ws_payloads.py`
    *   **Class:** `BackpackRawWsSubscriptionRequest`
    *   **Field:** `signature`
    *   **Change Type Hint:** From `tuple[RawBpNonEmptyStringMax255, RawBpNonEmptyStringMax255, RawBpNonEmptyStringMax64, RawBpNonEmptyStringMax64] | None` to `BackpackWsSignatureComponents | None`.
    *   **Serialization:** Pydantic's `model_dump(by_alias=True)` will handle serializing `BackpackWsSignatureComponents` into the list format expected by Backpack's WebSocket API if the `signature` field is appropriately defined as a list of strings in the Pydantic model *or* if we customize serialization. For Backpack, the `signature` field in the final JSON is `["key", "timestamp", "window", "sig"]`.
        *   **Crucial Detail for `BackpackRawWsSubscriptionRequest`:** The `signature` field in `BackpackRawWsSubscriptionRequest` represents the *final JSON structure*. So, if `BackpackWsSignatureComponents` is used as its type, a `@field_serializer` or a custom root serializer for `BackpackRawWsSubscriptionRequest` will be needed to transform the `BackpackWsSignatureComponents` object into the required `list[str]` for the actual JSON payload.
        *   **Alternative for `BackpackRawWsSubscriptionRequest`:** Keep `signature: tuple[str, str, str, str] | None` in `BackpackRawWsSubscriptionRequest` and handle the conversion from `BackpackWsSignatureComponents` to this tuple in the `BackpackWsMessageRouter`. This might be simpler. **Let's go with this alternative.**

4.  **Update `BackpackWsMessageRouter.construct_subscription_payload`:**
    *   **File:** `cyberdelta/apis/backpack/bp_ws_message_router.py`
    *   **Class:** `BackpackWsMessageRouter`
    *   **Method:** `construct_subscription_payload`
    *   **Parameter:** Change `signature_components_tuple: tuple[str, str, str, str] | None` to `signature_components: BackpackWsSignatureComponents | None`.
    *   **Logic:** If `signature_components` is provided, convert it to the `tuple[str, str, str, str]` format expected by `BackpackRawWsSubscriptionRequest`.
        ```python
        signature_val_tuple: tuple[str, str, str, str] | None = None
        if signature_components:
            signature_val_tuple = (
                signature_components.api_key,
                signature_components.timestamp,
                signature_components.window,
                signature_components.signature,
            )
        # ...
        return BackpackRawWsSubscriptionRequest(
            # ...
            signature=signature_val_tuple # This now matches the tuple type in BackpackRawWsSubscriptionRequest
        )
        ```

5.  **Update Call Site in `BackpackAPI._construct_subscription_payload`:**
    *   **File:** `cyberdelta/apis/backpack/bp_api.py`
    *   **Class:** `BackpackAPI`
    *   **Method:** `_construct_subscription_payload`
    *   **Logic:**
        *   The call to the authenticator will now be `self._bp_authenticator.get_ws_subscription_signature_components(...)` and will return `BackpackWsSignatureComponents | None`.
        *   Pass this `BackpackWsSignatureComponents` object (or `None`) directly to `self._bp_ws_router.construct_subscription_payload(...)`.

6.  **Testing Requirements:**
    *   Update unit tests for `BackpackEd25519Authenticator` to verify it returns the new `BackpackWsSignatureComponents` model.
    *   Update unit tests for `BackpackWsMessageRouter` to ensure it correctly handles the `BackpackWsSignatureComponents` input and constructs the correct `BackpackRawWsSubscriptionRequest`.
    *   Ensure WebSocket subscription tests for `BackpackAPI` still pass, verifying the correct JSON payload is generated.

7.  **Static Analysis and Reporting:**
    *   Run static analysis (Mypy, Pylint, Ruff) after changes and report any new warnings/errors.

---

**3. EIP-712 Structures in `HyperliquidEip712Authenticator`**

*   **Current:** Python dictionaries (`_domain_template`, `_agent_typed_data_message_types`).
*   **Goal:** Define Pydantic models for these EIP-712 structures to improve internal robustness.
*   **Why:** Catches structural errors (typos, incorrect nesting, wrong types for `name` or `type` within the EIP-712 definitions) at development time. Enhances maintainability.
*   **Impact:** `HyperliquidEip712Authenticator` will instantiate these models in `__init__` and then convert them to dictionaries using `model_dump()` when preparing the `structured_data_to_sign` for `eth_account.messages.encode_typed_data`.

---
**Prompt for AI Coder (Angel): Model EIP-712 Structures in Hyperliquid Authenticator**

**Project:** CyberDeltaEngine
**Context:** We are improving the internal robustness of the `HyperliquidEip712Authenticator`. Currently, the EIP-712 domain and message type definitions (`_domain_template`, `_agent_typed_data_message_types`) are raw Python dictionaries.

**Goal:**
Define Pydantic models to represent the EIP-712 domain and message type structures used for Hyperliquid Agent signatures. Refactor `HyperliquidEip712Authenticator` to use these models for defining its EIP-712 structures, converting them to dictionaries only when passing to `encode_typed_data`.

**Why:**
Using Pydantic models for these complex nested structures provides:
*   **Schema Validation:** Catches typos, incorrect field names, or structural errors in the EIP-712 definitions at development time.
*   **Type Safety:** Ensures correct types for names and type strings within the EIP-712 definitions.
*   **Clarity and Maintainability:** Makes the EIP-712 structures more explicit and easier to understand and modify.
This is an internal robustness enhancement.

**What to do (Step-by-Step):**

1.  **Define EIP-712 Type Definition Model:**
    *   **Location:** Create these models in a new file like `cyberdelta/apis/hyperliquid/models/hl_eip712_models.py` or within `hl_auth.py` if preferred for co-location (let's choose `hl_eip712_models.py`).
    *   **Model:** `EIP712TypeField`
        *   `name: str`
        *   `type: str` (e.g., "string", "uint256", "bytes32", "Agent")
        *   `model_config = ConfigDict(extra="forbid", frozen=True)`

2.  **Define EIP-712 Domain Model:**
    *   **Model:** `EIP712DomainStructure` (not the data, but the structure definition for `types.EIP712Domain`)
        *   `EIP712Domain: list[EIP712TypeField]` (This represents the `types["EIP712Domain"]` part)
    *   **Model:** `EIP712DomainData` (for the actual domain data)
        *   `name: str`
        *   `version: str`
        *   `chain_id: int = Field(alias="chainId")`
        *   `verifying_contract: str = Field(alias="verifyingContract")`
        *   `model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)`

3.  **Define Agent Message Structure Model:**
    *   **Model:** `AgentMessageStructure` (represents `types["Agent"]`)
        *   `Agent: list[EIP712TypeField]`

4.  **Define Full EIP-712 Types Model:**
    *   **Model:** `EIP712Types`
        *   `EIP712Domain: list[EIP712TypeField]`
        *   `Agent: list[EIP712TypeField]`
        *   `model_config = ConfigDict(extra="forbid", frozen=True)`

5.  **Update `HyperliquidEip712Authenticator`:**
    *   **File:** `cyberdelta/apis/hyperliquid/hl_auth.py`
    *   **Class:** `HyperliquidEip712Authenticator`
    *   **Modify `__init__`:**
        *   Instead of defining `_domain_template` and `_agent_typed_data_message_types` as raw dicts, instantiate your new Pydantic models here.
            ```python
            # Example for domain template
            self._domain_data_model = EIP712DomainData(
                name="Hyperliquid",
                version="1",
                chainId=chain_id, # from constructor
                verifyingContract="0x0000000000000000000000000000000000000000"
            )

            # Example for types
            self._eip712_types_model = EIP712Types(
                EIP712Domain=[
                    EIP712TypeField(name="name", type="string"),
                    # ... other EIP712Domain fields
                ],
                Agent=[
                    EIP712TypeField(name="source", type="string"),
                    EIP712TypeField(name="connectionId", type="bytes32"),
                ]
            )
            ```
    *   **Modify `prepare_request`:**
        *   When constructing `structured_data_to_sign`:
            ```python
            structured_data_to_sign = {
                "domain": self._domain_data_model.model_dump(by_alias=True), # Use model_dump
                "message": {
                    "source": "a",
                    "connectionId": connection_id_bytes,
                },
                "primaryType": "Agent",
                "types": self._eip712_types_model.model_dump(by_alias=True), # Use model_dump
            }
            ```

6.  **Testing Requirements:**
    *   Ensure existing unit tests for `HyperliquidEip712Authenticator.prepare_request` still pass. The generated signature should be identical.
    *   Add tests for the new Pydantic models (`EIP712TypeField`, `EIP712DomainData`, `EIP712Types`) to verify they correctly validate expected EIP-712 structures and reject invalid ones.

7.  **Static Analysis and Reporting:**
    *   Run static analysis (Mypy, Pylint, Ruff) after changes and report any new warnings/errors.


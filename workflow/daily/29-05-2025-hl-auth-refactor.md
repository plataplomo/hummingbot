# TASK (S5 REFINED III - FINAL ARCHITECTURE): Implement Correct Base Auth Flow & Backpack ED25519

## 1. Goal
Implement the definitive, architecturally sound authentication flow. This ensures base API layers (`ExchangeAPI`, `HttpClient`, `IAuthenticator`) are truly generic, and all exchange-specific authentication logic (including request body manipulation like Hyperliquid's `_clean_order_type_fields` or Backpack's `instruction` derivation) is encapsulated within the respective concrete authenticator's `prepare_request` method.

**Key Changes & Enforcements:**
1.  **`AuthenticatedRequestComponents`:** Pydantic `BaseModel` (in `authenticator_interface.py`).
2.  **`IAuthenticator.prepare_request`:** Signature `(..., data: dict[str, Any] | None, ...)` returns Pydantic `AuthenticatedRequestComponents`. Receives data as a dictionary (post-`model_dump`).
3.  **`HttpClient.request`:** Signature `(..., data: dict[str, Any] | None, ...)`. If signed, passes this `data` dict to `authenticator.prepare_request`. Uses the `data` field from the returned `AuthenticatedRequestComponents` for the actual HTTP call's JSON body.
4.  **`ExchangeAPI._request`:**
    *   Signature takes `data: BaseModel | dict[str, Any] | None`.
    *   If `data` is a `BaseModel`, it calls `data_dict = data.model_dump(by_alias=True, exclude_none=not serialize_none_as_null)`.
    *   This `data_dict` (or the original `data` if it was already a dict, or `None`) is then passed to `self._http_client.request`.
    *   **The `_authenticate` abstract method is REMOVED from `ExchangeAPI`.**
5.  **`BackpackEd25519Authenticator(IAuthenticator)`:**
    *   `__init__`: Stores keys, `INSTRUCTION_MAP`.
    *   `prepare_request(...)`: Implements full ED25519 REST signing. Internally derives `instruction`. Uses input `data` (dict) to form signable payload. Returns `AuthenticatedRequestComponents` (with `X-` headers; `data` field in returned components is the input `data` dict).
    *   `get_ws_subscription_signature_tuple(...)`: Helper for WS.
6.  **`HyperliquidEip712Authenticator.prepare_request(...)` (Implied Review):**
    *   Ensure it receives `data: dict[str, Any] | None`.
    *   It must perform the Hyperliquid-specific `_clean_order_type_fields` logic on this `data` dictionary *if needed due to `serialize_none_as_null` behavior and API quirks,* before returning `AuthenticatedRequestComponents`.
7.  **`BackpackAPI` (and its Services):** Uses generic `ExchangeAPI._request`. Services pass Pydantic request models to `self._request`.
8.  **WebSocket Auth for Backpack:** `BackpackWsMessageRouter` does *not* take authenticator. `BackpackAPI._construct_subscription_payload` calls `self._bp_authenticator.get_ws_subscription_signature_tuple()` and passes components to router.

## 2. Context and "Why"
This architecture makes `ExchangeAPI` and `HttpClient` truly generic. All exchange-specific logic for preparing the final `AuthenticatedRequestComponents` (including deriving instructions or cleaning the data payload dict) resides within the concrete authenticator's `prepare_request` method. This is the cleanest separation.

## 3. Project Rules Reminder
- Adhere strictly to all project rules. `cryptography` for ED25519.

## 4. Detailed Instructions

### Part A: Update Base Authentication Interface & Generic HTTP Layers

**File 1: `cyberdelta/apis/base/authenticator_interface.py`**
1.  **`AuthenticatedRequestComponents`:** Pydantic `BaseModel` (fields: `headers: Mapping[str, str]`, `params: dict[str, Any] | None`, `data: dict[str, Any] | None`).
2.  **`IAuthenticator.prepare_request` Signature:**
    ```python
    @abstractmethod
    async def prepare_request(
        self,
        method: str,
        path: str, # Relative path
        params: dict[str, Any] | None,
        data: dict[str, Any] | None, # EXPECTS a dictionary (already model_dumped by ExchangeAPI._request)
        headers: Mapping[str, Any] | None # Initial headers
    ) -> AuthenticatedRequestComponents: # RETURNS Pydantic model
        pass
    ```

**File 2: `cyberdelta/apis/connectivity/http_client.py` (`HttpClient`)**
1.  **`request` Method Signature:**
    *   `data: dict[str, Any] | None = None`. No `BaseModel` type here for `data`.
    *   No `auth_kwargs`.
2.  **Logic for Calling Authenticator:**
    ```python
    # Inside HttpClient.request, if is_signed and authenticator:
    auth_components_model = await authenticator.prepare_request(
        method=method,
        path=endpoint_path, # Ensure this is the relative path if authenticators expect it
        params=params,      # params is already dict | None
        data=data,          # data is already dict | None here
        headers=dict(request_headers_mutable_copy) # Pass a mutable copy
    )
    # Use the fields from the Pydantic model for the actual aiohttp request
    final_headers_for_aiohttp.update(auth_components_model.headers)
    final_params_for_aiohttp = auth_components_model.params
    final_data_for_aiohttp = auth_components_model.data # This is the dict to send as JSON
    # ... then make aiohttp call with final_headers_for_aiohttp, final_params_for_aiohttp, json=final_data_for_aiohttp
    ```

**File 3: `cyberdelta/apis/base/exchange_api.py` (`ExchangeAPI`)**
1.  **REMOVE the `_authenticate` abstract method and any calls to it.**
2.  **Refactor `_request` Method:**
    *   **Signature:** `data: BaseModel | dict[str, Any] | None = None`.
    *   **Logic:**
        ```python
        # Inside ExchangeAPI._request
        # ... (url joining) ...

        data_dict_for_http_client: dict[str, Any] | None
        if isinstance(data, BaseModel):
            data_dict_for_http_client = data.model_dump(
                by_alias=True, exclude_none=not self.serialize_none_as_null # Use attribute if it exists, or param
            )
        elif isinstance(data, dict) or data is None:
            data_dict_for_http_client = data
        else:
            raise TypeError(f"ExchangeAPI._request 'data' param must be BaseModel, dict, or None. Got {type(data)}")

        # NO _clean_order_type_fields here. That's authenticator's job if needed on data_dict.

        # ... (Rate Limiting logic) ...

        response_content, status_code, _processed_headers, response_headers_dict = \
            await self._http_client.request(
                method=method,
                endpoint_path=request_url, # full URL
                params=params,
                data=data_dict_for_http_client, # Pass the DICT or None
                headers=headers, # Initial headers
                authenticator=self._authenticator, # Pass the exchange's authenticator instance
                is_signed=is_signed, # Pass the original is_signed flag
                serialize_none_as_null=self.serialize_none_as_null # Pass flag to HttpClient
            )
        # ... (Error mapping, update rate limit headers) ...
        return response_content, status_code, response_headers_dict
        ```
    *   The `serialize_none_as_null` attribute on `ExchangeAPI` needs to be added (or passed to `_request`). For now, assume it's passed to `_request`.

### Part B: Implement `BackpackEd25519Authenticator`

**File:** `cyberdelta/apis/backpack/bp_auth.py`
1.  Implement `BackpackEd25519Authenticator(IAuthenticator)`:
    *   `__init__`: Stores B64 public/private keys, initializes `cryptography` private key, defines and stores `self.INSTRUCTION_MAP`.
    *   `async def prepare_request(self, method: str, path: str, params: dict | None, data: dict | None, headers: Mapping | None) -> AuthenticatedRequestComponents:`
        *   Derives `instruction` from `method` & `path` using `self.INSTRUCTION_MAP` (implement path template matching robustly).
        *   Constructs signable string using `instruction`, `params`/`data` (which are dicts), generates timestamp/window.
        *   Signs, returns `AuthenticatedRequestComponents` with `X-` headers. `data` field in returned components is the input `data` dict.
    *   `get_ws_subscription_signature_tuple(...)`: As before.

### Part C: Review & Align `HyperliquidEip712Authenticator`

**File:** `cyberdelta/apis/hyperliquid/hl_auth.py`
1.  **Review `prepare_request` method:**
    *   It now receives `data: dict[str, Any] | None` (the result of `model_dump()` from `ExchangeAPI._request`).
    *   **It is now responsible for the `_clean_order_type_fields` logic if that cleaning is still necessary due to `serialize_none_as_null=True` behavior for HL.**
        ```python
        # Inside HyperliquidEip712Authenticator.prepare_request
        # ... after other preparations, before returning AuthenticatedRequestComponents ...
        final_data_for_payload = data # data is already a dict
        if final_data_for_payload and kwargs.get("serialize_none_as_null_for_hl_order_type_cleaning") is True: # Need a way to know if cleaning is contextually needed
            self._clean_order_type_fields_static(final_data_for_payload) # Assuming a static helper

        return AuthenticatedRequestComponents(..., data=final_data_for_payload)
        ```
        *(The flag `serialize_none_as_null_for_hl_order_type_cleaning` is hypothetical. `serialize_none_as_null` is known by `ExchangeAPI._request`. The authenticator doesn't know this flag. This implies `_clean_order_type_fields` must be done in `ExchangeAPI._request` if it depends on `serialize_none_as_null` and is HL specific, or the authenticator must always clean if it's an invariant HL need.)*
        **Decision:** `_clean_order_type_fields` is an API-specific data manipulation. If `ExchangeAPI._request` does `model_dump(exclude_none=True)` (when `serialize_none_as_null=False`), then `None`s are already gone. If `exclude_none=False` (when `serialize_none_as_null=True`), then `None`s are `null`. If HL *always* needs the keys absent rather than null, then cleaning should happen in `HyperliquidEip712Authenticator.prepare_request` on the `data` dict it receives. Assume `serialize_none_as_null` is **not** passed to authenticator; it must decide based on `data` content.

### Part D: Refactor `BackpackAPI`

**File:** `cyberdelta/apis/backpack/bp_api.py`
1.  **Remove `_request` and `_authenticate` overrides.** Uses generic `ExchangeAPI._request`.
2.  **Service Calls:** Ensure methods like `place_order` (if in `BackpackAPI` or its services) call `self._request(..., data=pydantic_raw_request_model, is_signed=True)`.
3.  **`_construct_subscription_payload`:** Calls `self._bp_authenticator.get_ws_subscription_signature_tuple()` and passes components to `self._bp_ws_router.construct_subscription_payload(...)`.

### Part E: Refactor `BackpackWsMessageRouter`
1.  **`__init__`:** Does **NOT** take authenticator.
2.  **`construct_subscription_payload` Signature:** `(..., signature_components_tuple: tuple[str, str, str, str] | None = None) -> BackpackRawWsSubscriptionRequest:`

### Part F: Update `BackpackAPIComponentsFactory`
1.  Instantiates `BackpackEd25519Authenticator`.

## 5. Files to Modify (Key Files)
-   `cyberdelta/apis/base/authenticator_interface.py`
-   `cyberdelta/apis/connectivity/http_client.py`
-   `cyberdelta/apis/base/exchange_api.py`
-   `cyberdelta/apis/backpack/bp_auth.py`
-   `cyberdelta/apis/backpack/bp_api.py`
-   `cyberdelta/apis/backpack/services/*_service.py` (call sites of `self._request`)
-   `cyberdelta/apis/backpack/bp_ws_message_router.py`
-   `cyberdelta/apis/backpack/bp_api_components_factory.py`
-   `cyberdelta/apis/hyperliquid/hl_auth.py` (Review `prepare_request` for `_clean_order_type_fields` logic).

## 6. Testing: THE VERY NEXT STEP.

## 7. Reporting
-   Confirm changes. Output files. Explain final auth flow.

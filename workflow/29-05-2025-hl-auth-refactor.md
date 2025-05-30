# TASK (S5 REFINED - FOUNDATIONAL AUTH REFACTOR): Correct Base API Auth Flow & Backpack ED25519

## 1. Goal
Perform a foundational refactoring of the authentication flow in the base API classes (`ExchangeAPI`, `HttpClient`, `IAuthenticator`) and then implement the correct, unified ED25519 authentication for Backpack (REST & WS) consistent with this refined base architecture.

**Key Changes:**
1.  **`AuthenticatedRequestComponents`:** Ensure it's a Pydantic `BaseModel` in `authenticator_interface.py`.
2.  **`IAuthenticator.prepare_request`:** Signature remains generic: `(..., data: dict | None, ...)` and returns Pydantic `AuthenticatedRequestComponents`.
3.  **`HttpClient.request`:** Takes `data: dict | None`. If `is_signed` and `authenticator` provided, it calls `authenticator.prepare_request` with this `data` dict.
4.  **`ExchangeAPI._request`:**
    *   Signature takes `data: BaseModel | None`.
    *   If `data` is a `BaseModel`, it calls `model_dump()` to get a `dict`.
    *   It then calls `self._http_client.request` passing the `data` (now a `dict | None`), `is_signed`, and `self._authenticator`.
    *   **The `_authenticate` abstract method is REMOVED from `ExchangeAPI` and all its subclasses.**
5.  **`BackpackEd25519Authenticator(IAuthenticator)`:**
    *   `__init__`: Takes B64 public/private keys, initializes internal `INSTRUCTION_MAP`.
    *   `prepare_request(...)`: Implements full ED25519 REST signing. It internally derives the `instruction` from `method` and `path` (relative path) using its `INSTRUCTION_MAP`. It uses the input `data` (which will be a `dict`) for the signable payload if applicable. Returns `AuthenticatedRequestComponents`.
    *   `get_ws_subscription_signature_tuple(...)`: Helper for WS signature components.
6.  **`BackpackAPI`:**
    *   **NO longer overrides `_request` or `_authenticate` for REST auth.** It uses the generic `ExchangeAPI._request`. Its service methods will call `self._request(..., is_signed=True, data=raw_request_model)`.
    *   `_construct_subscription_payload`: Calls its authenticator's WS method and passes components to the router.
7.  **`BackpackWsMessageRouter`:** `construct_subscription_payload` takes signature tuple. `__init__` does NOT take authenticator.
8.  **`BackpackAPIComponentsFactory`:** Instantiates `BackpackEd25519Authenticator`.

## 2. Context and "Why"
This is the definitive architecture. `HttpClient` always calls `IAuthenticator.prepare_request` for signed REST. Exchange-specific logic (like Backpack's `instruction` derivation) is fully encapsulated within its concrete authenticator's `prepare_request`. `ExchangeAPI` is a generic orchestrator. This ensures consistency and modularity.

## 3. Project Rules Reminder
- Adhere strictly to all project rules. Use `cryptography`.

## 4. Detailed Instructions

### Part A: Update Base Authentication Interface & Generic HTTP Layers

**File 1: `cyberdelta/apis/base/authenticator_interface.py`**
1.  **`AuthenticatedRequestComponents`:** Ensure it's a Pydantic `BaseModel`:
    ```python
    class AuthenticatedRequestComponents(BaseModel):
        headers: Mapping[str, str]
        params: dict[str, Any] | None = None
        data: dict[str, Any] | None = None # This is the dict passed to aiohttp
        model_config = ConfigDict(extra="forbid", frozen=True)
    ```
2.  **`IAuthenticator.prepare_request` Signature (No `auth_context`):**
    ```python
    @abstractmethod
    async def prepare_request(
        self,
        method: str,
        path: str, # Relative path
        params: dict[str, Any] | None,
        data: dict[str, Any] | None, # Request body as a dictionary
        headers: Mapping[str, Any] | None # Initial headers (mutable mapping expected by impl)
    ) -> AuthenticatedRequestComponents: # Returns Pydantic model
        pass
    ```

**File 2: `cyberdelta/apis/connectivity/http_client.py` (`HttpClient`)**
1.  **`request` Method Signature:**
    *   `data` parameter should be `data: dict[str, Any] | None = None`. (It receives a dict from `ExchangeAPI._request` after `model_dump`).
    *   No `auth_kwargs`.
2.  **Logic for Calling Authenticator:**
    ```python
    # Inside HttpClient.request, if is_signed and authenticator:
    auth_components_model = await authenticator.prepare_request(
        method=method,
        path=endpoint_path, # Ensure this is the relative path if authenticator expects it
        params=request_params if request_params else None,
        data=data, # data is already a dict here
        headers=dict(request_headers_mutable_copy)
    )
    # Use auth_components_model.headers, auth_components_model.params, auth_components_model.data
    # for the aiohttp call.
    # final_data_for_aiohttp = auth_components_model.data
    ```

**File 3: `cyberdelta/apis/base/exchange_api.py` (`ExchangeAPI`)**
1.  **REMOVE the `_authenticate` abstract method.**
2.  **Refactor `_request` Method:**
    *   **Signature:** `data: BaseModel | dict[str, Any] | None = None`. (Still allow dict here for flexibility if some internal calls don't use Pydantic models, though ideally they should).
    *   **Logic:**
        ```python
        # Inside ExchangeAPI._request
        # ... (url joining, initial headers preparation) ...

        data_for_http_client: dict[str, Any] | None
        if isinstance(data, BaseModel):
            # Ensure serialize_none_as_null is available or use a fixed default like exclude_none=True
            # serialize_none_as_null is a param of _request
            data_for_http_client = data.model_dump(
                by_alias=True, exclude_none=not serialize_none_as_null
            )
            # Handle Hyperliquid _clean_order_type_fields if applicable (moved from HttpClient)
            if self.exchange_name == "hyperliquid" and data_for_http_client and \
               serialize_none_as_null and hasattr(self, "_clean_order_type_fields"):
                self._clean_order_type_fields(data_for_http_client)
        elif isinstance(data, dict) or data is None:
            data_for_http_client = data
        else:
            raise TypeError(f"Unsupported type for 'data' parameter in ExchangeAPI._request: {type(data)}")

        # ... (Rate Limiting logic) ...
        
        response_content, status_code, _processed_headers, response_headers_dict = \
            await self._http_client.request(
                method=method,
                endpoint_path=request_url, # full URL
                params=params,
                data=data_for_http_client, # dict or None
                headers=headers, # Initial headers
                authenticator=self._authenticator, # Pass the exchange's authenticator instance
                is_signed=is_signed, # Crucial: pass the original is_signed flag
                # serialize_none_as_null NOT needed by HttpClient.request, already handled
            )
        # ... (Error mapping from self.error_mapper, update rate limit headers) ...
        return response_content, status_code, response_headers_dict
        ```
    *   Add `_clean_order_type_fields` to `ExchangeAPI` if it's generic enough, or keep it specific to `HyperliquidAPI` by having `HyperliquidAPI` override `_request` *only* to add that cleaning step after calling `super()._request` but before the data is passed to `_http_client.request` (this is getting complex again). **Simpler: `ExchangeAPI._request` does the model_dump. If specific cleaning is needed, the subclass can override `_request`, call `super()._request` to get the `data_for_http_client` dict, clean it, then call `self._http_client.request` itself.**
    **Decision for `_clean_order_type_fields`:** Keep it in `HttpClient` for now, as it's a post-`model_dump` data manipulation specific to how `aiohttp` might send JSON vs. what Hyperliquid expects. It's an API quirk adjustment.

### Part B: Implement `BackpackEd25519Authenticator`

**File:** `cyberdelta/apis/backpack/bp_auth.py`
1.  **Implement `BackpackEd25519Authenticator(IAuthenticator)` as per "TASK (CRITICAL REVISED II)" Prompt (Part A), ensuring:**
    *   `__init__` stores keys and `INSTRUCTION_MAP`.
    *   `prepare_request` correctly derives `instruction` from `method` and `path` (relative path, e.g., `/api/v1/order`) using `INSTRUCTION_MAP`. This may require a helper function for path template matching if paths contain variables (e.g., `/api/v1/order/{order_id}`). Start with exact matches and simple prefix matches for now.
    *   `prepare_request` constructs the full signable string, signs, and returns `AuthenticatedRequestComponents` (Pydantic model) with all `X-` headers.
    *   `get_ws_subscription_signature_tuple` is implemented for WS.

### Part C: Refactor `BackpackAPI`

**File:** `cyberdelta/apis/backpack/bp_api.py`
1.  **Remove any `_request` or `_authenticate` override.** It now fully uses the generic `ExchangeAPI._request`.
2.  **Ensure Service Calls to `self._request` are Correct:**
    *   When service methods (e.g., `BackpackTradingService.place_order`) call `self._request(...)` (which resolves to `ExchangeAPI._request`):
        *   They pass `is_signed=True`.
        *   The `data` argument they pass is the Pydantic model instance returned by `BackpackRequestBuilder` (e.g., `BackpackRawOrderExecuteRequest`). `ExchangeAPI._request` will handle `model_dump()`.
        *   **No `auth_context` or `instruction` is passed from service to `self._request`.** This is now encapsulated within `BackpackEd25519Authenticator.prepare_request`.
3.  **`_construct_subscription_payload` Method (WS Auth):**
    *   This method needs access to `self._bp_authenticator` to call `get_ws_subscription_signature_tuple`.
    *   It then passes the resulting `signature_components_tuple` to `self._bp_ws_router.construct_subscription_payload(topic, signature_components_tuple=...)`.
    *   Returns the Pydantic model from the router.

### Part D: Refactor `BackpackWsMessageRouter`

**File:** `cyberdelta/apis/backpack/bp_ws_message_router.py`
1.  **`__init__`:** Does **NOT** take the authenticator.
2.  **`construct_subscription_payload` Signature:** `def construct_subscription_payload(self, topic: str, signature_components_tuple: tuple[str, str, str, str] | None = None) -> BackpackRawWsSubscriptionRequest:`
3.  **Logic:** Uses `signature_components_tuple` to populate `BackpackRawWsSubscriptionRequest.signature`.

### Part E: Update `BackpackAPIComponentsFactory`
1.  Ensures `create_authenticator` instantiates `BackpackEd25519Authenticator` correctly.

## 5. Files to Modify
-   `cyberdelta/apis/base/authenticator_interface.py`
-   `cyberdelta/apis/connectivity/http_client.py` (Ensure `data` passed to `authenticator.prepare_request` is a dict)
-   `cyberdelta/apis/base/exchange_api.py` (Remove `_authenticate`, simplify `_request`)
-   `cyberdelta/apis/backpack/bp_auth.py` (Implement `BackpackEd25519Authenticator`)
-   `cyberdelta/apis/backpack/bp_api.py` (Remove overrides, update WS sub payload construction)
-   `cyberdelta/apis/backpack/services/*.py` (Ensure they pass Pydantic models as `data` to `self._request`)
-   `cyberdelta/apis/backpack/bp_ws_message_router.py`
-   `cyberdelta/apis/backpack/bp_api_components_factory.py`

## 6. Testing: THE VERY NEXT STEP.

## 7. Reporting
-   Confirm changes. Output files. Highlight `BackpackEd25519Authenticator.prepare_request` instruction logic.
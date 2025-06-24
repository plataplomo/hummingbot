# TASK (CRITICAL CLEANUP): Eradicate HMAC and Fully Implement ED25519 for Backpack Authentication

## 1. Goal
Ensure that all authentication mechanisms for the Backpack exchange within the `cyberdelta/apis/backpack/` directory exclusively use the new ED25519 signing process via `BackpackEd25519Authenticator`. This means completely removing any existing HMAC-SHA256 related logic or classes for Backpack.

## 2. Context and "Why"
It has been identified that remnants or primary use of HMAC-SHA256 authentication might still exist in the Backpack API client code, particularly in `bp_auth.py` or how authentication is invoked. The official Backpack documentation mandates **ED25519 for ALL signed operations (REST and WebSocket private streams)**. Using HMAC is incorrect and will lead to authentication failures. This task is to perform a thorough cleanup and ensure full adoption of the ED25519 standard as implemented in the (to be correctly defined) `BackpackEd25519Authenticator`.

## 3. Project Rules Reminder
- Adherence to all project rules is paramount.
- This is a correction and cleanup task. The focus is on removing incorrect implementations and ensuring the correct one is used everywhere for Backpack.

## 4. Detailed Instructions

### 4.1. File: `cyberdelta/apis/backpack/bp_auth.py`
   1.  **COMPLETE REMOVAL of `BackpackHmacAuthenticator`:**
       *   Delete the entire class definition for `BackpackHmacAuthenticator`.
       *   Ensure no import statements reference it (e.g., from `hashlib`, `hmac` if they were specific to it and not used by ED25519 logic).
   2.  **CONFIRM `BackpackEd25519Authenticator` Implementation:**
       *   This class **must** be present and correctly implemented as per the "S5 - DEFINITIVE" prompt (using `cryptography` library, with its internal `INSTRUCTION_MAP` for REST, and methods `prepare_request` for REST and `get_ws_subscription_signature_tuple` for WS).
       *   The `prepare_request` method in `BackpackEd25519Authenticator` (which implements `IAuthenticator.prepare_request`) is the one called by `HttpClient` for any signed REST request. It *must* derive the `instruction` internally from the `method` and `path` arguments it receives.

### 4.2. File: `cyberdelta/apis/backpack/bp_api_components_factory.py`
   1.  **`create_authenticator` Method:**
       *   This method **must** instantiate and return `BackpackEd25519Authenticator`.
       *   It should pass the `api_key` (Base64 public ED25519 key) and `api_secret` (Base64 private ED25519 key) from `ExchangeSecretsConfig` to the `BackpackEd25519Authenticator` constructor.
       *   Ensure there are no references or instantiation logic for `BackpackHmacAuthenticator`.

### 4.3. File: `cyberdelta/apis/backpack/bp_api.py`
   1.  **`__init__` Method:**
       *   Ensure `self._bp_authenticator` is assigned an instance of `BackpackEd25519Authenticator` (via the factory).
   2.  **REST API Calls (via `self._request` which is `ExchangeAPI._request`):**
       *   All signed REST calls made from service methods (e.g., `BackpackTradingService.place_order` calling `self._request`) must pass `is_signed=True`.
       *   `ExchangeAPI._request` will then use `self._bp_authenticator` (which is `BackpackEd25519Authenticator`) and call its `prepare_request` method. The `instruction` derivation logic is *inside* `BackpackEd25519Authenticator.prepare_request`.
       *   There should be **NO special handling** for `instruction` or manual header creation related to authentication within `BackpackAPI` methods that call `self._request` for REST. The generic flow handles it.
   3.  **WebSocket Subscription (`_construct_subscription_payload` method):**
       *   This method **must** use `self._bp_authenticator.get_ws_subscription_signature_tuple()` to get the signature components for private streams.
       *   It then **must** delegate to `self._bp_ws_router.construct_subscription_payload(topic, signature_components_tuple=...)`, passing these components.

### 4.4. File: `cyberdelta/apis/backpack/bp_ws_message_router.py`
    1. **`__init__` method:** It should **NOT** take an authenticator instance. The signature components for WS payloads are passed directly to its `construct_subscription_payload` method.
    2. **`construct_subscription_payload` method:** Its signature must be `def construct_subscription_payload(self, topic: str, signature_components_tuple: tuple[str, str, str, str] | None = None) -> BackpackRawWsSubscriptionRequest:`. It uses the provided `signature_components_tuple` to populate the `signature` field of the Pydantic model.

### 4.5. Search and Destroy
    *   Angel, please search the entire `cyberdelta/apis/backpack/` directory for any remaining imports, references, or usage of "HMAC", `BackpackHmacAuthenticator`, or HMAC-specific signing logic related to Backpack. These must all be removed or replaced with the ED25519 mechanism.

## 5. Key Architectural Points to Enforce (Reiteration of S5)
    - `HttpClient.request` makes a generic call to `IAuthenticator.prepare_request`.
    - `BackpackEd25519Authenticator.prepare_request` contains all logic for REST:
        - Derives `instruction` from `method` and `path`.
        - Builds the full string to sign.
        - Signs with ED25519.
        - Returns `AuthenticatedRequestComponents` with `X-` headers.
    - `BackpackAPI` does not override `_request` or `_authenticate` for REST auth. It uses the generic `ExchangeAPI._request`.
    - For WS, `BackpackAPI._construct_subscription_payload` gets signature components from its authenticator's dedicated WS method and passes them to the router, which builds the Pydantic payload.

## 6. Files to Modify (Primarily)
-   `cyberdelta/apis/backpack/bp_auth.py` (Complete replacement of HMAC with ED25519)
-   `cyberdelta/apis/backpack/bp_api_components_factory.py`
-   `cyberdelta/apis/backpack/bp_api.py` (Ensure it uses generic `_request`, and `_construct_subscription_payload` correctly uses authenticator's WS method and router)
-   `cyberdelta/apis/backpack/bp_ws_message_router.py` (Ensure `__init__` does not take authenticator, `construct_subscription_payload` takes signature tuple).

## 7. Testing Requirements
-   The immediate next step after this will be comprehensive testing. No new tests from Angel for this cleanup.
-   Static analysis (`mypy --strict`, `ruff`) must pass.

## 8. Reporting
-   Confirm removal of all HMAC logic for Backpack and full implementation of ED25519 via `BackpackEd25519Authenticator`.
-   List all modified files.
-   Output the changed files, especially `bp_auth.py`, `bp_api.py`, and `bp_ws_message_router.py`.


# Code Review Report: 02 - API Clients

**Report Date:** 2025-04-14
**Reviewer:** Angel (AI Assistant)
**Project:** CyberDeltaEngine
**Version Target:** v0.0.1

## 1. Overview

This section reviews the API client implementations responsible for interacting with the target exchanges (Hyperliquid and Backpack). It covers the base class defining the common interface and the specific implementations for each exchange.

## 2. `ExchangeAPI` Base Class (`apis/base.py`)

*   **Purpose:** Defines a crucial abstract base class (`ExchangeAPI`) establishing a standardized interface for all exchange-specific clients. This ensures consistency in how the rest of the application interacts with different exchanges.
*   **Key Features:**
    *   **Abstract Interface:** Defines numerous abstract methods (`@abstractmethod`) that *must* be implemented by subclasses. This includes core functionalities like authentication (`_authenticate`), fetching data (`get_balances`, `get_positions`, `get_ticker`, etc.), order management (`place_order`, `cancel_order`, `get_order_status`), and WebSocket message parsing (`parse_ticker_message`, `parse_orderbook_message`, etc.).
    *   **Centralized REST Request Logic (`_request`):** Provides a robust, shared method for handling HTTP requests. It integrates:
        *   `aiohttp.ClientSession` for asynchronous requests.
        *   Hook for subclass-specific authentication (`_authenticate`).
        *   Built-in `RateLimiter` (token bucket implementation) configurable per endpoint/method type.
        *   Basic retry logic for transient errors (timeouts, rate limits, server errors).
        *   Standardized error handling via `_map_error_response`, converting exchange errors into common `APIError` / `APIErrorCode`.
    *   **WebSocket Framework:** Offers a structure for managing WebSocket connections (`_connect_ws`, `_reconnect_ws`, `_ws_listener`) and message routing based on handlers registered by subscribers. Subclasses must implement the specific parsing logic.
    *   **Standardized Errors:** Defines `APIError` and `APIErrorCode` to simplify error handling in components using the API clients.
*   **Strengths:** Excellent use of abstraction to enforce consistency and reduce code duplication. Centralizes complex but common logic like rate limiting, retries, and basic request structure. Standardized error handling is a major plus for robustness.
*   **Weaknesses/Concerns:** The effectiveness relies entirely on the quality and completeness of the implementations in the concrete subclasses. The base WebSocket framework is generic; subclasses need to handle exchange-specific pings/pongs, subscription confirmations, and potential quirks.

*   **Code Snippet (Standardized Error):**
    ```python
    # apis/base.py L93-L129
    class APIError(Exception):
        # ... (init method with code, http_status, exchange_code etc.) ...
        def __init__( # ... parameters ... ):
            # ... assigns parameters ...
            full_message = f"{message}"
            # ... adds context like HTTP status, exchange code ...
            super().__init__(full_message)

        @property
        def is_retryable(self) -> bool:
            # ... checks self.code against retryable error codes ...
    ```

## 3. `HyperliquidAPI` (`apis/hyperliquid.py`)

*   **Implementation Status:** Appears largely complete, implementing most abstract methods from `ExchangeAPI`.
*   **Authentication:** Correctly implements EIP-712 signing using `web3.py` and `eth_account`. Handles nonce management and includes validation logic for the provided wallet address against the private key. Includes a mock authentication path for testing public endpoints without a private key.
*   **REST Endpoints:** Implements methods for fetching balances, positions, orders, market data (ticker, order book, trades), and funding rates. Also includes order placement and cancellation.
*   **WebSocket:** Implements subscription methods (`subscribe_to_ticker`, etc.) and specific message parsing logic (`parse_ticker_message`, etc.) tailored to Hyperliquid's JSON format, converting data to internal models. Uses the `websockets` library (explicitly the legacy client).
*   **Error Handling:** Implements `_map_error_response` to translate errors found in Hyperliquid response bodies into standard `APIErrorCode`s.
*   **Strengths:** Provides a functional interface to Hyperliquid. Authentication appears correctly implemented. Data parsing logic exists for key types.
*   **Weaknesses/Concerns:**
    *   **Legacy Websockets:** Uses `websockets.legacy.client`. Migrating to the current `websockets` library is recommended for long-term support and potential features/fixes.
    *   **Error Mapping:** The `_map_error_response` logic seems basic and might not cover all Hyperliquid error cases comprehensively. Requires testing against actual exchange errors.
    *   **Rate Limit Headers:** It's unclear if Hyperliquid provides rate limit information in response headers and if `_update_rate_limit_from_headers` is implemented to utilize it.

*   **Code Snippet (EIP-712 Authentication - Signing):**
    ```python
    # apis/hyperliquid.py L204-L239 (Simplified)
    # ... (Inside _authenticate) ...
    # Construct EIP-712 typed data structure
    eip712_domain = {"name": "Hyperliquid", "version": "1", ...}
    eip712_types = {"Agent": [...], ...}
    structured_data_to_sign = {
        "types": eip712_types,
        "primaryType": "Agent",
        "domain": eip712_domain,
        "message": {
            "source": "a", # Or 'b' based on context
            "connectionId": connection_hash, # Calculated connection hash
            "timestamp": timestamp_str,
            "nonce": nonce_str,
            # Include payload hash if method is POST/PUT
        },
    }
    # Sign the data
    encoded_data = encode_typed_data(full_message=structured_data_to_sign)
    signed_message = self._account.sign_message(encoded_data)
    signature = signed_message.signature.hex()
    # Return headers with signature, timestamp, nonce
    return {"headers": {"X-HL-Signature": signature, ...}, ...}
    ```

## 4. `BackpackAPI` (`apis/backpack.py`)

*   **Implementation Status:** Implements the necessary structure and key methods required by `ExchangeAPI`.
*   **Authentication:** Correctly implements HMAC-SHA256 signing (`_sign_request`) based on Backpack's documented requirements (timestamp, method, body/params).
*   **REST Endpoints:** Provides implementations for fetching balances, positions, orders, market data, funding rates, placing orders, and cancelling orders.
*   **WebSocket:** Follows the base class structure for WebSocket connection and subscription management. Specific message parsing methods are needed (defined in base, implemented per exchange).
*   **Error Handling:** Implements `_map_error_response` to translate common Backpack errors (inferred from status code or simple string matching in the body) into `APIErrorCode`s.
*   **Strengths:** Provides a functional interface to Backpack. Authentication mechanism matches documentation. Covers essential REST endpoints.
*   **Weaknesses/Concerns:**
    *   **WS Message Parsing:** Concrete implementations for `parse_ticker_message`, `parse_orderbook_message`, `parse_trade_message`, `parse_fill_message` etc., are crucial for real-time updates but are not shown in the reviewed `backpack.py` code (they are defined in the base class but need implementation here). The strategy likely relies heavily on these.
    *   **Funding Rate Reliability:** The accuracy and timeliness of funding rate data obtained via `get_funding_rate` (likely REST polling) are critical. Delays could impact strategy performance. If Backpack offers funding rates via WS, that stream should be implemented.
    *   **Error Mapping:** The current `_map_error_response` relies on basic checks. Backpack might have more detailed error codes or structures that could be mapped for more granular error handling. Needs testing against real-world errors.
    *   **WS Ping/Pong:** Explicit handling for WebSocket ping/pong or keepalive mechanisms required by Backpack might be necessary within the listener loop but isn't explicitly shown.

*   **Code Snippet (HMAC Authentication):**
    ```python
    # apis/backpack.py L127-L132 (Inside _sign_request)
    # Create signature
    signature = hmac.new(
        self._api_secret.encode("utf-8"),
        signature_payload.encode("utf-8"), # Contains timestamp + request data
        hashlib.sha256,
    ).hexdigest()
    # Return headers with key, timestamp, signature
    ```

## 5. Overall Assessment

The API client architecture is well-designed, leveraging a base class for consistency. Both `HyperliquidAPI` and `BackpackAPI` provide the necessary implementations for authentication and core REST actions. Key areas for attention are ensuring complete and robust WebSocket message parsing (especially for Backpack), verifying comprehensive error mapping, confirming the reliability of funding rate data acquisition, and considering upgrades (like moving Hyperliquid off the legacy websockets client).
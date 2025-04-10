# Code Report: CyberDeltaEngine - API Clients

## 1. Overview

The API client layer provides a standardized interface for interacting with different cryptocurrency exchanges. It abstracts away exchange-specific details, allowing core components like the `DataHandler` and `ExecutionHandler` to work with exchanges consistently.

## 2. Base Class (`cyberdelta/apis/base.py`)

**Purpose**: Defines the abstract base class `ExchangeAPI` that all specific exchange clients must inherit from.

**Key Responsibilities**:
- Defining common methods for fetching data (tickers, order books, trades, funding rates, balances, positions) and executing actions (place order, cancel order).
- Providing common infrastructure for:
    - Asynchronous HTTP requests (`aiohttp`).
    - WebSocket connection management (connect, reconnect, listen, subscribe).
    - Rate limiting using a token bucket implementation (`RateLimiter`).
    - Standardized error handling (`APIError`, `APIErrorCode`).
    - Request signing and authentication (via abstract `_authenticate` method).

**Code Snippet (`ExchangeAPI._request`)**:
```python
    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        signed: bool = False,
        retry_count: int = 3,
        timeout: float = 30.0
    ) -> Any:
        """
        Make an asynchronous HTTP request to the exchange API with rate limiting and retries.
        Handles authentication, error mapping, and JSON response parsing.
        """
        if not self._session:
            await self._initialize_session() # Ensure session exists

        full_url = f"{self.rest_endpoint}{path}"
        request_headers = self.default_headers.copy()
        if headers:
            request_headers.update(headers)

        auth_details = None
        if signed:
            try:
                # Call the subclass implementation for authentication
                auth_details = await self._authenticate(method, path, params, data)
                if auth_details.get('headers'):
                    request_headers.update(auth_details['headers'])
                # Potentially update params/data based on auth needs
                params = auth_details.get('params', params)
                data = auth_details.get('data', data)
            except Exception as e:
                logger.error(f"[{self.exchange_name}] Authentication error: {e}")
                raise APIError("Authentication failed", code=APIErrorCode.AUTHENTICATION_FAILED, original_exception=e)

        # Acquire rate limit token
        rate_limiter = await self._get_rate_limiter(method, path)
        wait_time = await rate_limiter.acquire()
        if wait_time > 0.1:
             logger.warning(f"[{self.exchange_name}] Rate limit delay: {wait_time:.2f}s for {method} {path}")

        # Prepare request data based on method
        request_kwargs = {
            "params": params,
            "headers": request_headers,
            "timeout": aiohttp.ClientTimeout(total=timeout)
        }
        if method in ("POST", "PUT", "DELETE") and data:
             request_kwargs["json"] = data # Send data as JSON

        # Retry loop
        for attempt in range(retry_count + 1):
            try:
                async with self._session.request(method, full_url, **request_kwargs) as response:
                    # Update rate limits from response headers if available
                    self._update_rate_limit_from_headers(response.headers, method, path)

                    # Check status code and handle response
                    if 200 <= response.status < 300:
                        try:
                            return await response.json()
                        except aiohttp.ContentTypeError:
                             # Handle non-JSON success responses if necessary
                            return await response.text()
                    else:
                        # Handle error responses
                        error_body = await response.text()
                        error_data = {}
                        try:
                            error_data = json.loads(error_body)
                        except json.JSONDecodeError:
                            pass # Body wasn't valid JSON

                        api_error = self._map_error_response(response.status, error_body, error_data)

                        # Check if retryable
                        if api_error.is_retryable and attempt < retry_count:
                            retry_delay = api_error.retry_after or (2 ** attempt)
                            logger.warning(
                                f"[{self.exchange_name}] Request failed (Attempt {attempt+1}/{retry_count+1}): {api_error}. Retrying in {retry_delay:.2f}s..."
                            )
                            await asyncio.sleep(retry_delay)
                            continue # Go to next attempt
                        else:
                            logger.error(f"[{self.exchange_name}] Request failed: {api_error}")
                            raise api_error

            except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                error_code = APIErrorCode.CONNECTION_ERROR if isinstance(e, aiohttp.ClientConnectionError) else APIErrorCode.TIMEOUT
                if attempt < retry_count:
                    retry_delay = 2 ** attempt
                    logger.warning(
                        f"[{self.exchange_name}] Request connection/timeout error (Attempt {attempt+1}/{retry_count+1}): {e}. Retrying in {retry_delay:.2f}s..."
                    )
                    await asyncio.sleep(retry_delay)
                    continue # Go to next attempt
                else:
                    logger.error(f"[{self.exchange_name}] Request failed after {retry_count+1} attempts: {e}")
                    raise APIError(str(e), code=error_code, original_exception=e)
            except Exception as e:
                # Catch any other unexpected errors
                logger.error(f"[{self.exchange_name}] Unexpected error during request: {e}", exc_info=True)
                raise APIError(f"Unexpected request error: {e}", code=APIErrorCode.UNKNOWN, original_exception=e)

        # Should not be reached if retry_count >= 0
        raise APIError("Request failed after all retries", code=APIErrorCode.UNKNOWN)
```

## 3. Hyperliquid API Client (`cyberdelta/apis/hyperliquid.py`)

**Purpose**: Implements the `ExchangeAPI` interface specifically for the Hyperliquid exchange.

**Key Responsibilities**:
- Providing Hyperliquid-specific REST endpoint and WebSocket URL.
- Implementing `_authenticate` using Hyperliquid's signing mechanism (likely ECDSA based on documentation).
- Parsing Hyperliquid-specific WebSocket message formats for tickers, order books, user fills, etc.
- Mapping Hyperliquid API responses to the standardized `cyberdelta.core.models` (e.g., `Ticker`, `Order`, `Position`).
- Handling Hyperliquid-specific error codes and mapping them to `APIErrorCode`.
- Implementing specific subscription messages for Hyperliquid WebSocket topics.

**Code Snippet (`HyperliquidAPI._authenticate`)**:
```python
    async def _authenticate(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Authenticate API request for Hyperliquid. Requires EIP-712 signing.
        (Note: Actual implementation depends on hyperliquid-python-sdk or manual EIP-712)
        """
        if not self.wallet or not self.account:
            raise APIError("Hyperliquid wallet/account required for signing", APIErrorCode.AUTHENTICATION_FAILED)

        # Timestamp in milliseconds
        timestamp = int(time.time() * 1000)

        # Construct payload based on Hyperliquid requirements
        # This is a simplified example; actual signing is complex
        payload = {
            # EIP-712 domain and message structure...
            "timestamp": timestamp,
            # ... other required fields ...
        }

        # Sign the payload using the wallet
        # signature = self.wallet.sign_eip712(...) # Requires EIP-712 library
        signature = "0xExampleSignature" # Placeholder

        # Construct the authentication data for the request body or headers
        # Hyperliquid typically uses a specific structure in the POST body
        auth_data = {
            "action": {
                "type": "exchange", # Or other action type
                "payload": payload,
                "signature": signature,
                "nonce": timestamp # Nonce often based on timestamp
            }
        }

        # Return structure expected by base class (modify data)
        return {
            "headers": {},
            "params": params,
            "data": auth_data # Authentication is part of the request body
        }
```

## 4. Backpack API Client (`cyberdelta/apis/backpack.py`)

**Purpose**: Implements the `ExchangeAPI` interface specifically for the Backpack exchange.

**Key Responsibilities**:
- Providing Backpack-specific REST endpoint and WebSocket URL.
- Implementing `_authenticate` using Backpack's HMAC-SHA256 signing mechanism.
- Parsing Backpack-specific WebSocket message formats (`topic`/`data` structure).
- Mapping Backpack API responses to the standardized `cyberdelta.core.models`.
- Handling Backpack-specific error codes and mapping them to `APIErrorCode`.
- Implementing specific subscription messages (`op`/`channel`) for Backpack WebSocket topics.

**Code Snippet (`BackpackAPI._sign_request`)**:
```python
    def _sign_request(self, method: str, path: str, params: Optional[Dict] = None, data: Optional[Dict] = None) -> Dict[str, Any]:
        """Sign requests for Backpack using HMAC-SHA256."""
        if not self._api_key or not self._api_secret:
            raise APIError("Backpack API key and secret required for signed requests.", APIErrorCode.AUTHENTICATION_FAILED)

        timestamp = str(int(time.time() * 1000))
        window = str(self.config.get('recv_window', 5000))

        # Create signature payload based on Backpack requirements
        # Instruction: Signature Message Structure
        # instruction={instruction_name}&timestamp={timestamp}&window={window}
        # + URI Encoded Query String Parameters (alphabetical order)
        # + URI Encoded JSON Body (if present)
        instruction_name = "" # Needs to be determined based on the operation
        # Example: For placing an order, it might be determined by the path or a specific header
        # This part needs clarification based on Backpack's specific instructions per endpoint.
        # Assuming a generic 'api' instruction for signed requests
        instruction_name = "api"

        signature_payload = f"instruction={instruction_name}&timestamp={timestamp}&window={window}"

        if params:
            query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
            signature_payload += "&" + query_string

        if data:
            # Ensure data is encoded consistently, typically compact JSON without spaces
            body_string = json.dumps(data, separators=(',', ':'))
            signature_payload += "&" + body_string

        # Create signature
        signature = hmac.new(
            base64.b64decode(self._api_secret),
            signature_payload.encode('utf-8'),
            hashlib.sha256
        ).digest()

        encoded_signature = base64.b64encode(signature).decode('utf-8')

        # Return headers and potentially modified params/data
        return {
            "headers": {
                "X-API-Key": self._api_key,
                "X-Timestamp": timestamp,
                "X-Window": window,
                "X-Signature": encoded_signature
            },
            "params": params,
            "data": data
        }
```

## 5. Error Handling (`cyberdelta/apis/errors.py` and `base.py`)

**Purpose**: Standardizes error reporting across different exchanges.

- `APIErrorCode` (Enum): Defines common error types (e.g., `AUTHENTICATION_FAILED`, `RATE_LIMITED`, `INSUFFICIENT_FUNDS`).
- `APIError` (Exception): Custom exception class holding standardized code, original HTTP status/message, and retry information.
- `_map_error_response` (Method in `ExchangeAPI`): Abstract method (though often implemented in base with common logic) responsible for translating exchange-specific error responses into standardized `APIError` objects. Subclasses can override or extend this.

This layered approach allows the core engine to handle errors consistently, regardless of which exchange originated the error. 
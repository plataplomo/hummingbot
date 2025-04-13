# CyberDeltaEngine: Code Review Report (v0.0.1) - API Clients

This section details the structure and implementation of the API clients responsible for interacting with the target exchanges (Hyperliquid and Backpack).

## 1. Base Class (`cyberdelta/apis/base.py`)

*   **Responsibility:**
    *   Define a standard `ExchangeAPI` abstract base class (ABC) enforcing a common interface for all exchange-specific implementations.
    *   Provide shared, reusable functionality: HTTP request handling (`aiohttp`), rate limiting, standardized error hierarchy, basic WebSocket connection management framework.
*   **Key Components & Implementation:**
    *   **`ExchangeAPI(ABC)`:** Defines abstract methods for essential exchange operations:
        *   Authentication: `_authenticate`, `_sign_request` (placeholders)
        *   REST Endpoints: `get_balances`, `get_positions`, `get_open_orders`, `create_order`, `cancel_order`, `get_ticker`, `get_order_book`, `get_funding_rate`, etc.
        *   WebSocket: `connect_websocket`, `subscribe`, `unsubscribe`, `_ws_listener`, `_reconnect_ws`, `_route_ws_message` (framework for subclasses).
        *   Parsing: `parse_ticker`, `parse_order_book`, `parse_balance`, `parse_position`, `parse_order`, `parse_trade`, `parse_funding_rate`, etc. (to convert exchange data to internal `core.models`).
    *   **`APIError(Exception)`:** Custom base exception for API-related issues. Includes attributes like `exchange_name`, `original_error`, `code` (standardized `APIErrorCode`), `status_code`, `retryable`.
    *   **`APIErrorCode(Enum)`:** Standardized error codes (e.g., `AUTHENTICATION_ERROR`, `INVALID_SYMBOL`, `INSUFFICIENT_FUNDS`, `RATE_LIMIT_EXCEEDED`, `NETWORK_ERROR`, `EXCHANGE_ERROR`, `ORDER_NOT_FOUND`).
    *   **`RateLimiter`:** Token bucket implementation (`refill_rate`, `max_tokens`) used within `_request` to prevent exceeding exchange limits.
    *   **`_request(method, endpoint, params, data, authenticate)`:** Core HTTP request function:
        *   Uses `aiohttp.ClientSession`.
        *   Waits for rate limiter token.
        *   Calls authentication hook (`_sign_request`) if `authenticate=True`.
        *   Executes the request.
        *   Handles basic retries for `NETWORK_ERROR` or specific `status_code`s (e.g., 5xx).
        *   Calls `_map_error_response` on non-2xx responses to translate exchange errors into standard `APIError`.
    *   **`_map_error_response(response, status_code, text)`:** Abstract method for subclasses to implement exchange-specific error translation.
    *   **WebSocket Framework:** Provides `connect_websocket` wrapper, `_ws_listener` task loop, reconnection logic (`_reconnect_ws`), basic subscription management, and message routing (`_route_ws_message`) to be implemented by subclasses.

*   **Code Snippet (Conceptual `_request` method):**
    ```python
    # cyberdelta/apis/base.py (Conceptual)
    class BaseAPIClient(ExchangeAPI):
        # ... __init__ with session, rate_limiter, logger, etc. ...

        async def _request(self, method: str, endpoint: str, authenticate: bool = False, params: dict | None = None, data: dict | None = None) -> Any:
            await self.rate_limiter.wait_for_token()
            request_kwargs = {}
            headers = self._get_default_headers()

            if authenticate:
                # Ensure authentication details (API key, secret) are available
                if not self.api_key or not self.secret_key:
                    raise APIError(self.exchange_name, APIErrorCode.AUTHENTICATION_ERROR, "API key or secret not configured")
                # Sign request modifies params, data, or headers
                headers, params, data = self._sign_request(method, endpoint, params, data, headers)

            request_kwargs["params"] = params
            request_kwargs["data"] = json.dumps(data) if data and method in ["POST", "PUT"] else None # Adjust based on content type
            if not request_kwargs["data"] and data:
                 request_kwargs["params"] = data # For GET/DELETE with body-like data in params

            request_kwargs["headers"] = headers
            url = self.base_url + endpoint

            # Simplified retry logic here
            for attempt in range(self.max_retries + 1):
                try:
                    async with self.session.request(method, url, **request_kwargs) as response:
                        status_code = response.status
                        text = await response.text()

                        if 200 <= status_code < 300:
                            try:
                                return await response.json() # Assume JSON response
                            except aiohttp.ContentTypeError:
                                return text # Return raw text if not JSON

                        # Map errors for non-success codes
                        mapped_error = self._map_error_response(response, status_code, text)
                        if mapped_error.retryable and attempt < self.max_retries:
                             self.logger.warning(f"Retrying {method} {url} after error (Attempt {attempt+1}/{self.max_retries}): {mapped_error}")
                             await asyncio.sleep(self.retry_delay * (2**attempt)) # Exponential backoff
                             continue
                        else:
                             raise mapped_error # Raise non-retryable or final retry error

                except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                    error = APIError(self.exchange_name, APIErrorCode.NETWORK_ERROR, f"Network error: {e}", original_error=e, retryable=True)
                    if attempt < self.max_retries:
                        self.logger.warning(f"Retrying {method} {url} after network error (Attempt {attempt+1}/{self.max_retries}): {error}")
                        await asyncio.sleep(self.retry_delay * (2**attempt))
                        continue
                    else:
                         raise error # Raise final network error
                except Exception as e:
                    # Catch unexpected errors during request
                    raise APIError(self.exchange_name, APIErrorCode.UNKNOWN_ERROR, f"Unexpected error during request: {e}", original_error=e)
            # Should not be reached if max_retries >= 0
            raise APIError(self.exchange_name, APIErrorCode.UNKNOWN_ERROR, "Request failed after max retries")

        @abstractmethod
        def _map_error_response(self, response: aiohttp.ClientResponse, status_code: int, text: str) -> APIError:
             # Subclasses must implement this
             pass

        @abstractmethod
        def _sign_request(self, method: str, endpoint: str, params: dict | None, data: dict | None, headers: dict) -> tuple[dict, dict | None, dict | None]:
             # Subclasses must implement this
             pass
    ```

*   **Observations & Strengths:**
    *   Enforces consistency across exchange implementations via the ABC.
    *   Centralizes crucial cross-cutting concerns: rate limiting, basic retries, error standardization.
    *   Reduces boilerplate code in subclasses.
*   **Concerns & Areas for Improvement:**
    *   **Error Mapping Burden:** Subclasses bear the significant responsibility of accurately implementing `_map_error_response`. Incomplete mapping leads to unhandled errors.
    *   **WebSocket Generality:** The base WebSocket framework might be too generic; specific exchanges often have unique WS protocols (e.g., message formats, subscription methods, ping/pong handling) requiring substantial overrides in subclasses.
    *   **Authentication Complexity:** Authentication logic (`_sign_request`) varies significantly and can be complex (e.g., EIP-712, HMAC signatures).
*   **Recommendations:**
    *   **Subclass Testing:** Emphasize rigorous testing of subclass implementations, especially `_map_error_response` and parsing methods.
    *   **WS Enhancements (Optional):** Consider adding more specific WS helper methods in the base class *if* common patterns emerge across exchanges (e.g., a standard way to handle JSON RPC responses if multiple exchanges use it), but avoid over-engineering.
    *   **Clear Documentation:** Ensure abstract methods and expected behaviors are clearly documented in the base class docstrings.

## 2. Hyperliquid API (`cyberdelta/apis/hyperliquid.py`)

*   **Responsibility:** Implement `ExchangeAPI` for the Hyperliquid DEX.
*   **Implementation Details:**
    *   **Authentication:** Implements `_sign_request` using EIP-712 signatures required by Hyperliquid's API (likely using `eth_account` library).
    *   **Endpoints:** Implements methods (`get_balances`, `get_positions`, `create_order`, etc.) targeting Hyperliquid's specific Info and Exchange API endpoints (e.g., `/info`, `/exchange`).
    *   **Parsing:** Contains `parse_*` methods (e.g., `parse_position`, `parse_order`, `parse_funding_rate`) to convert Hyperliquid JSON responses into `cyberdelta.core.models` objects. **Crucially, ensures conversion of stringified numbers to `Decimal` for financial values.**
    *   **WebSocket:** Implements `connect_websocket`, `subscribe` (using Hyperliquid's subscription format), and `_route_ws_message` to handle Hyperliquid's JSON RPC-based WebSocket stream for order updates, fills, user events, L2 books, etc.
    *   **Error Mapping:** Implements `_map_error_response` to translate Hyperliquid error strings (e.g., "Invalid order size", "Insufficient margin") into standardized `APIError` exceptions with appropriate `APIErrorCode`.

*   **Code Snippet (Conceptual Parsing & Decimal Handling):**
    ```python
    # cyberdelta/apis/hyperliquid.py (Conceptual)
    from decimal import Decimal
    from cyberdelta.core.models import Position, Balance, Order, Ticker, FundingRate, OrderStatus, OrderSide
    # ... other imports

    class HyperliquidAPI(BaseAPIClient):
        # ... __init__, _sign_request, endpoint methods ...

        def parse_position(self, data: dict) -> Position:
             # Example: Assumes Hyperliquid position data format
             position_data = data.get('position', {})
             asset_info = data.get('assetInfo', {})
             try:
                 entry_px_str = position_data.get("entryPx")
                 szi_str = position_data.get("szi")
                 unrealized_pnl_str = position_data.get("unrealizedPnl")
                 mark_px_str = asset_info.get("markPx")
                 liquidation_px_str = position_data.get("liquidationPx")

                 if not entry_px_str or not szi_str: # Required fields
                      raise ValueError("Missing entryPx or szi in position data")

                 size_decimal = Decimal(szi_str)
                 side = OrderSide.BUY if size_decimal > Decimal(0) else OrderSide.SELL

                 return Position(
                     symbol=asset_info.get("name"), # Assuming internal symbol mapping needed elsewhere
                     side=side,
                     size=abs(size_decimal), # Store size as positive
                     entry_price=Decimal(entry_px_str),
                     unrealized_pnl=Decimal(unrealized_pnl_str) if unrealized_pnl_str else None,
                     mark_price=Decimal(mark_px_str) if mark_px_str else None,
                     liquidation_price=Decimal(liquidation_px_str) if liquidation_px_str else None,
                     # ... map other fields ...
                 )
             except (InvalidOperation, TypeError, ValueError) as e:
                 self.logger.error(f"Error parsing Hyperliquid position data: {data}. Error: {e}")
                 raise APIError(self.exchange_name, APIErrorCode.PARSING_ERROR, f"Failed to parse position: {e}", original_error=e)

        def parse_funding_rate(self, data: dict) -> FundingRate:
             # Example: Assuming data structure from funding rate endpoint
             try:
                 # Adjust keys based on actual Hyperliquid response structure
                 symbol = data.get("coin") # Or similar
                 rate_str = data.get("fundingRate") # Or predicted rate key?
                 predicted_rate_str = data.get("predictedFundingRate")
                 next_funding_time_ms = data.get("nextFundingTime")

                 return FundingRate(
                     symbol=symbol,
                     funding_rate=Decimal(rate_str) if rate_str else None,
                     predicted_rate=Decimal(predicted_rate_str) if predicted_rate_str else None,
                     next_funding_time=next_funding_time_ms,
                     # ... other fields ...
                 )
             except (InvalidOperation, TypeError, ValueError) as e:
                 self.logger.error(f"Error parsing Hyperliquid funding rate data: {data}. Error: {e}")
                 raise APIError(self.exchange_name, APIErrorCode.PARSING_ERROR, f"Failed to parse funding rate: {e}", original_error=e)

        def _map_error_response(self, response: aiohttp.ClientResponse, status_code: int, text: str) -> APIError:
            # Example mapping
            message = text # Default message
            error_code = APIErrorCode.EXCHANGE_ERROR # Default
            retryable = False
            try:
                # Hyperliquid might return JSON error object or just string
                error_data = json.loads(text)
                if isinstance(error_data, dict):
                    message = error_data.get('error', message)
                elif isinstance(error_data, str):
                    message = error_data
            except json.JSONDecodeError:
                pass # Use raw text

            if status_code == 429 or "Rate limit exceeded" in message:
                error_code = APIErrorCode.RATE_LIMIT_EXCEEDED
                retryable = True # Typically retry rate limits
            elif status_code == 401 or "Invalid signature" in message:
                 error_code = APIErrorCode.AUTHENTICATION_ERROR
            elif status_code == 400:
                 if "Insufficient margin" in message:
                     error_code = APIErrorCode.INSUFFICIENT_FUNDS
                 elif "Invalid order size" in message or "Invalid price" in message:
                      error_code = APIErrorCode.INVALID_ORDER_PARAMETERS
                 else:
                      error_code = APIErrorCode.BAD_REQUEST
            # ... more specific mappings ...

            return APIError(self.exchange_name, error_code, message, status_code=status_code, retryable=retryable)

    ```

*   **Observations & Strengths:**
    *   Implements the necessary complex EIP-712 authentication.
    *   Provides coverage for essential REST and WebSocket functionalities.
*   **Concerns & Areas for Improvement:**
    *   **Decimal Conversion Rigor:** High risk if string numbers aren't converted to `Decimal` immediately and consistently in *all* `parse_*` methods. Requires thorough validation (`decimal.mdc`).
    *   **WebSocket Parsing:** Hyperliquid's WS stream can be verbose. Ensure the parsing in `_route_ws_message` is robust against variations and efficiently extracts needed information (orders, fills, etc.) into standard models.
    *   **Error Mapping Completeness:** Verify mapping covers all common operational errors (margin issues, order validation errors, liquidation events if applicable, etc.).
*   **Recommendations:**
    *   **Mandatory Decimal Audit:** Conduct a specific code review pass focusing *only* on ensuring all financial data from Hyperliquid (prices, sizes, PnL, rates) is parsed into `Decimal` objects correctly.
    *   **Test WebSocket Parsing:** Implement tests that simulate various Hyperliquid WebSocket messages (including potential edge cases or malformed ones) to ensure `_route_ws_message` and subsequent parsing are resilient.
    *   **Refine Error Mapping:** Review Hyperliquid documentation and potentially add more specific error mappings based on common failure modes.

## 3. Backpack API (`cyberdelta/apis/backpack.py`)

*   **Responsibility:** Implement `ExchangeAPI` for the Backpack CEX.
*   **Implementation Details:**
    *   **Authentication:** Implements `_sign_request` using HMAC-SHA256 signatures as required by Backpack (passing timestamp, window, signed parameters).
    *   **Endpoints:** Implements methods targeting Backpack's REST API v1 endpoints (e.g., `/api/v1/capital`, `/api/v1/balance`, `/api/v1/order`, `/api/v1/ticker`, `/api/v1/depth`, `/api/v1/klines`, `/api/v1/funding`).
    *   **Parsing:** Contains `parse_*` methods to convert Backpack JSON responses (which often use string representations for numbers) into `core.models` objects, ensuring `Decimal` conversion.
    *   **WebSocket:** Implements `connect_websocket`, `subscribe` (using Backpack's stream names like `ticker`, `depth`, `kline`, `account`, `orders`, `fills`), and `_route_ws_message` to handle Backpack's topic-based WebSocket stream.
    *   **Error Mapping:** Implements `_map_error_response` for Backpack's specific error codes (e.g., `10001` for parameter error, `20001` for auth error).
*   **Code Snippet (Conceptual Authentication & Funding Rate):**
    ```python
    # cyberdelta/apis/backpack.py (Conceptual)
    import hmac
    import hashlib
    import time
    from urllib.parse import urlencode
    from decimal import Decimal

    class BackpackAPI(BaseAPIClient):
        # ... __init__ ...

        def _sign_request(self, method: str, endpoint: str, params: dict | None, data: dict | None, headers: dict) -> tuple[dict, dict | None, dict | None]:
            if not self.api_key or not self.secret_key:
                 raise APIError(self.exchange_name, APIErrorCode.AUTHENTICATION_ERROR, "API key/secret missing")

            timestamp = int(time.time() * 1000)
            window = 5000 # Example receive window
            # Combine params and data for signature base string (adjust based on Backpack spec)
            query_params = params or {}
            body_params = data or {}
            all_params = {**query_params, **body_params}

            # Format depends on GET vs POST/PUT/DELETE
            if method == "GET" or method == "DELETE":
                 # Signature base often only includes query params for GET/DELETE
                 signature_base = urlencode(sorted(query_params.items())) if query_params else ""
                 payload_to_sign = f"timestamp={timestamp}&window={window}"
                 if signature_base:
                      payload_to_sign += f"&{signature_base}"
                 params = query_params # Ensure params are set for GET
                 data = None
            else: # POST, PUT
                 # Signature base often includes body params for POST/PUT
                 signature_base = urlencode(sorted(body_params.items())) if body_params else ""
                 payload_to_sign = f"timestamp={timestamp}&window={window}"
                 if signature_base:
                      payload_to_sign += f"&{signature_base}"
                 data = body_params # Ensure data is set for POST/PUT body
                 params = None

            signature = hmac.new(
                self.secret_key.encode('utf-8'),
                payload_to_sign.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()

            headers['X-Timestamp'] = str(timestamp)
            headers['X-Window'] = str(window)
            headers['X-API-Key'] = self.api_key
            headers['X-Signature'] = signature
            headers['Content-Type'] = 'application/json; charset=utf-8' # Usually required

            # Return potentially modified headers, params, data
            return headers, params, data

        async def get_funding_rate(self, symbol: str, **kwargs) -> FundingRate | None:
            # Backpack might not have a single "current" funding rate endpoint easily accessible.
            # Often requires fetching history and taking the latest, or using a WebSocket stream if available.
            exchange_symbol = self.symbol_mapper.get_exchange_symbol(symbol, self.exchange_name)
            try:
                # Option 1: Fetch recent history
                # params = {"symbol": exchange_symbol, "limit": 1} # Fetch only the latest
                # response = await self._request("GET", "/api/v1/funding", params=params, authenticate=True)
                # if response and isinstance(response, list) and len(response) > 0:
                #     return self.parse_funding_rate(response[0])

                # Option 2: Check if a dedicated current rate endpoint exists (unlikely for many CEXs)

                # Option 3: Rely solely on WebSocket stream (if Backpack provides one for funding)
                # self.logger.warning(f"Fetching current funding rate via REST not directly supported by Backpack API mock. Rely on WebSocket.")
                # Need to check if funding rate is pushed via WS
                # For now, return None or fetch from DataHandler cache filled by WS
                cached_rate = self.data_handler.get_funding_rate(self.exchange_name, symbol) # Requires DataHandler ref
                if cached_rate: return cached_rate

                self.logger.warning(f"Could not retrieve current funding rate for {symbol} on Backpack.")
                return None

            except APIError as e:
                self.logger.error(f"API Error getting funding rate for {symbol} on {self.exchange_name}: {e}")
                raise e
            except Exception as e:
                self.logger.error(f"Error getting funding rate for {symbol} on {self.exchange_name}: {e}")
                raise APIError(self.exchange_name, APIErrorCode.EXCHANGE_ERROR, f"Failed to get funding rate: {e}")

        def parse_funding_rate(self, data: dict) -> FundingRate:
             # Example assumes format from /fundingHistory endpoint
             try:
                 # Ensure keys match actual Backpack response
                 symbol = data.get('symbol')
                 rate_str = data.get('fundingRate')
                 time_ms = data.get('fundingTime') # Timestamp of the funding event

                 return FundingRate(
                     symbol=symbol,
                     funding_rate=Decimal(rate_str) if rate_str else None,
                     timestamp=time_ms,
                     # Backpack history might not include predicted rate or next time
                     predicted_rate=None,
                     next_funding_time=None,
                 )
             except (InvalidOperation, TypeError, ValueError) as e:
                 self.logger.error(f"Error parsing Backpack funding rate data: {data}. Error: {e}")
                 raise APIError(self.exchange_name, APIErrorCode.PARSING_ERROR, f"Failed to parse funding rate: {e}")

        def _map_error_response(self, response: aiohttp.ClientResponse, status_code: int, text: str) -> APIError:
            # Example mapping based on potential Backpack error format
            message = text
            error_code = APIErrorCode.EXCHANGE_ERROR
            retryable = False
            original_code = None

            try:
                error_data = json.loads(text)
                if isinstance(error_data, dict):
                    original_code = error_data.get('code')
                    message = error_data.get('msg', message)

                    if original_code == 20001 or original_code == 20002: # Example Auth Errors
                        error_code = APIErrorCode.AUTHENTICATION_ERROR
                    elif original_code == 10001: # Example Param Error
                         error_code = APIErrorCode.BAD_REQUEST
                    elif original_code == 10016: # Example Insufficient Balance
                         error_code = APIErrorCode.INSUFFICIENT_FUNDS
                    elif original_code == 30005: # Example Order Not Found
                         error_code = APIErrorCode.ORDER_NOT_FOUND
                    # ... more Backpack specific codes ...

            except json.JSONDecodeError:
                 pass # Use raw text

            # Handle generic HTTP status codes too
            if status_code == 429:
                 error_code = APIErrorCode.RATE_LIMIT_EXCEEDED
                 retryable = True
            elif status_code == 401:
                 error_code = APIErrorCode.AUTHENTICATION_ERROR
            elif status_code >= 500:
                 error_code = APIErrorCode.EXCHANGE_UNAVAILABLE
                 retryable = True

            return APIError(self.exchange_name, error_code, message, status_code=status_code, retryable=retryable, original_code=original_code)

    ```

*   **Observations & Strengths:**
    *   Implements standard HMAC authentication.
    *   Covers necessary endpoints for balances, orders, market data.
*   **Concerns & Areas for Improvement:**
    *   **Decimal Conversion:** Same high risk as Hyperliquid; ensure *all* `parse_*` methods convert string numbers to `Decimal` rigorously.
    *   **Funding Rate Availability (CRITICAL RISK):** Backpack might not provide a reliable real-time funding rate via REST or WebSocket suitable for low-latency arbitrage. Using historical data (`/fundingHistory`) is likely insufficient. **This needs immediate verification.** Does Backpack have a WebSocket stream for predicted or current rates? If not, the strategy might be unviable or require estimation.
    *   **WebSocket Data:** Confirm that the WebSocket implementation correctly subscribes to and parses *all* required streams, especially user-specific data like order updates (`orders`, `fills`) and account balance changes (`account`).
    *   **Timestamp Consistency:** Standardize handling of timestamps from REST/WebSocket, converting to UTC `datetime`.
    *   **Error Mapping:** Requires thorough mapping of Backpack's numeric error codes.
*   **Recommendations:**
    *   **Mandatory Decimal Audit:** Review all Backpack `parse_*` methods for correct `Decimal` conversion.
    *   **INVESTIGATE FUNDING RATE SOURCE:** **Highest priority.** Determine definitively how to get timely (near real-time or predictive) funding rate data from Backpack. Check WebSocket documentation thoroughly. If unavailable, the project's core premise for Backpack is at risk.
    *   **Validate WebSocket User Streams:** Test `account`, `orders`, and `fills` WebSocket streams to ensure `PortfolioTracker` receives correct, real-time updates.
    *   **Complete Error Mapping:** Consult Backpack API documentation to map all relevant error codes in `_map_error_response`.
    *   **Standardize Timestamps:** Ensure consistent UTC `datetime` conversion for all timestamps.

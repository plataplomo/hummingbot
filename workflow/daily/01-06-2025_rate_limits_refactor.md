**Prompt 1 for Angel (Phase 1: Core Generic Components & Hyperliquid Config Setup)**
```markdown
**Objective:** Phase 1 Rate Limiting Refactor - Implement Core Generic Rate Limiting Primitives and Hyperliquid Configuration Models.

**Context & Rationale:**
This is the **first phase** of a major refactor to implement a new "Rate Limit Strategy" pattern. This pattern will allow each exchange integration to manage its own specific rate limiting logic via a dedicated strategy object, while core components like `ExchangeAPI` remain generic.

**In this phase, we will:**
1.  Enhance the basic `TokenBucketRateLimiterRuntime` to consume a variable number of tokens.
2.  Update our Pydantic configuration models (`ExchangeSpecificConfig`) to include new, detailed rate limit parameters specifically for Hyperliquid (e.g., IP weight limits, weights for different `/info` request types, and settings for an address-based action safety net). Backpack's simple `rate_limit_per_minute` will remain for its `SimpleTokenBucketStrategy`.
3.  **Remove the existing central `RateLimiterService` and its config `RateLimiterConfig`**, as rate limiting will now be more decentralized and managed by strategy objects owned by each `ExchangeAPI` instance.
4.  Modify the base `ExchangeAPI.__init__` to accept an optional `RateLimitStrategy` instance. If none is provided (e.g., by BackpackAPI), it will create a default `SimpleTokenBucketStrategy` using the exchange's `rate_limit_per_minute` config.
5.  Fix the "double token acquisition" bug by removing all rate limiting logic from `HttpClient.request`. The base `ExchangeAPI._request` will call the `acquire_permission` method of its `self.rate_limit_strategy`.

**Affected Files & Detailed Instructions:**

**I. `cyberdelta/apis/rate_limiter.py` (`TokenBucketRateLimiterRuntime`):**
    1.  **Modify `acquire` Method:**
        *   Change signature from: `async def acquire(self) -> float:`
        *   To: `async def acquire(self, tokens_to_consume: int = 1) -> float:`
        *   Update internal logic to correctly use `tokens_to_consume` when checking available tokens, decrementing tokens, and calculating `wait_time` if tokens are insufficient (i.e., `wait_time = (tokens_to_consume - self.tokens) / self.rate`).

**II. `cyberdelta/config/config_models.py`:**
    1.  **New Model `AddressActionSafetyNetConfig`:**
        ```python
        from pydantic import BaseModel, ConfigDict, Field

        class AddressActionSafetyNetConfig(BaseModel):
            model_config = ConfigDict(extra="forbid", frozen=True)
            rate_per_minute: int = Field(..., gt=0, description="Client-side safety net rate for address-based actions, in actions per minute.")
            # Optional: bucket_size_factor: int = Field(default=2, ge=1, description="Factor to calculate bucket size from rate_per_second for this limiter.")
        ```
    2.  **Modify `ExchangeSpecificConfig`:**
        *   Keep existing `rate_limit_per_minute: int | None = Field(default=None, description="For simple exchanges: total requests per minute.")` (Make it optional, Backpack will use it, HL will not).
        *   Add new **optional** fields specifically for Hyperliquid's detailed configuration (ensure they are `None` by default so other exchanges don't need to define them):
            *   `ip_weight_limit_per_minute: int | None = Field(default=None, gt=0, description="Hyperliquid: Total IP weight budget per minute (e.g., 1200).")`
            *   `info_request_type_ip_weights: dict[str, int] | None = Field(default=None, description="Hyperliquid: IP weights for /info request types. Keys are API 'type' strings.")`
            *   `default_info_weight: int | None = Field(default=None, ge=1, description="Hyperliquid: Default IP weight for unlisted /info types.")`
            *   `exchange_action_base_ip_weight: int | None = Field(default=None, ge=1, description="Hyperliquid: Base IP weight for one /exchange action (formula applies on top).")`
            *   `address_action_safety_net: AddressActionSafetyNetConfig | None = Field(default=None, description="Hyperliquid: Config for address action safety net limiter.")`
        *   Update/add `@model_validator` `check_hyperliquid_specific_configs` to ensure all these new Hyperliquid-specific fields are present and valid if `self.exchange_name == ExchangeName.HYPERLIQUID`. If it's Backpack, ensure `rate_limit_per_minute` is present.

**III. `config/config.yaml` (Example Update for Hyperliquid & Backpack):**
    *   **Hyperliquid Section:**
        ```yaml
        hyperliquid:
          # ... (exchange_name, api_base_url, etc.) ...
          # REMOVE old rate_limit_per_minute for Hyperliquid
          ip_weight_limit_per_minute: 1140
          info_request_type_ip_weights:
            l2Book: 2                 # Level 2 Order Book
            allMids: 2                # All Mid Prices
            # ... add other HL info types and their weights, with comments ...
            userRole: 60
          default_info_weight: 20
          exchange_action_base_ip_weight: 1
          address_action_safety_net:
            rate_per_minute: 300
        ```
    *   **Backpack Section:**
        ```yaml
        backpack:
          # ... (exchange_name, api_base_url, etc.) ...
          rate_limit_per_minute: 120 # Backpack continues to use this simple config
        ```

**IV. `cyberdelta/apis/connectivity/rate_limiter_service.py` (`RateLimiterService`) & `cyberdelta/apis/models/rate_limiter_config.py` (`RateLimiterConfig`):**
    1.  **DELETE `RateLimiterService` class and the file `rate_limiter_service.py`.**
    2.  **DELETE `RateLimiterConfig` model and the file `rate_limiter_config.py`.**
    *   **Rationale:** With the "Rate Limit Strategy" pattern, each `ExchangeAPI` subclass (via its specific `RateLimitStrategy` instance) will manage its own `TokenBucketRateLimiterRuntime` instances. The central `RateLimiterService` is no longer needed.

**V. `cyberdelta/apis/connectivity/http_client.py` (`HttpClient.request`):**
    1.  **Modify `request` Method:**
        *   Remove all parameters and logic related to rate limiting (e.g., `rate_limiter_service`, `limiter`, `request_weight`).
        *   Remove any `limiter.acquire()` calls.
        *   The method should now only focus on preparing and executing the `aiohttp.ClientSession.request` call with the provided `method, endpoint_path, params, data, headers, timeout`.

**VI. `cyberdelta/apis/base/exchange_api.py` (`ExchangeAPI`):**
    1.  **Modify `__init__`:**
        *   Add a new parameter: `rate_limit_strategy: RateLimitStrategy | None = None` (import `RateLimitStrategy` from its new location, which will be defined in Prompt 2).
        *   Store it: `self.rate_limit_strategy = rate_limit_strategy`.
        *   **Remove `self._rate_limiter_service` attribute and its instantiation.**
        *   **Default Strategy Creation:** If `rate_limit_strategy` is `None` when `ExchangeAPI.__init__` is called (meaning the subclass like `BackpackAPI` didn't provide a specific one):
            *   Read `self.exchange_config.rate_limit_per_minute` (ensure `self.exchange_config` is available, it's set from `config` arg).
            *   Calculate `rate_s` and `bucket_size`.
            *   Create `default_limiter_primitive = TokenBucketRateLimiterRuntime(rate=rate_s, bucket_size=bucket)`.
            *   Set `self.rate_limit_strategy = SimpleTokenBucketStrategy(limiter=default_limiter_primitive, default_request_weight=1)` (import `SimpleTokenBucketStrategy` from its new location, defined in Prompt 2).
            *   Log that a default simple strategy is being used.
    2.  **Modify `_request` Method:**
        *   Remove the `_rate_limiting_handled: bool = False` parameter.
        *   **Rate Limiting Step (New):**
            *   Prepare `data_payload_dict` from the input `data` argument.
            *   Create `request_context = {"exchange_name": self.exchange_name, "method": method, "endpoint": endpoint, "action_payload": data_payload_dict, "request_weight": request_weight, "endpoint_group": endpoint_group}`.
            *   If `self.rate_limit_strategy` exists:
                *   `await self.rate_limit_strategy.prepare_and_acquire(request_context)`.
        *   The rest of the `_request` method (authentication for generic exchanges if `is_signed` and `self._authenticator` is present, URL construction, calling `self._http_client.request(...)`, error mapping using `self.error_mapper`) remains largely the same. The call to `self._http_client.request` will no longer pass limiter arguments.

**Testing Requirements:**
*   Unit tests for `TokenBucketRateLimiterRuntime.acquire` with variable `tokens_to_consume`.
*   Pydantic models in `config_models.py` validate correctly. `config.yaml` loads into `AppSettings`.
*   `ExchangeSpecificConfig`'s validator correctly enforces new Hyperliquid fields and Backpack's `rate_limit_per_minute`.
*   `HttpClient.request` has no rate limiting logic.
*   `ExchangeAPI.__init__` correctly sets up `self.rate_limit_strategy` (either passed-in or default `SimpleTokenBucketStrategy`).
*   `ExchangeAPI._request` correctly calls `self.rate_limit_strategy.prepare_and_acquire()`.
*   **Crucially:** Existing tests for Backpack (which uses the simple rate limit) **must continue to pass** using the new `ExchangeAPI._request` flow with the default `SimpleTokenBucketStrategy`.

**Project Rules Adherence:** Static Analysis V3, No Silencing V4. Full type hinting.
```

---

**Prompt 2 for Angel (Phase 2: Implementing Rate Limit Strategy Pattern and Hyperliquid-Specific Logic)**

```markdown
**Objective:** Phase 2 Rate Limiting Refactor - Implement the "Rate Limit Strategy" Pattern and Hyperliquid's Specific Strategy.

**Context & Rationale:**
This phase builds on the foundational changes from Phase 1. We will now define the `RateLimitStrategy` interface and implement two concrete strategies:
1.  `SimpleTokenBucketStrategy`: For exchanges like Backpack with a single, simple rate limit. This will be used by default in `ExchangeAPI.__init__` if a subclass doesn't provide a specific strategy.
2.  `HyperliquidRateLimitStrategy`: For Hyperliquid, encapsulating its dual-limiter logic (IP weights and address-based action counts). This strategy will use an internal `HyperliquidRequestWeighter` utility.

`HyperliquidAPI` will then be updated to instantiate and use `HyperliquidRateLimitStrategy`.

**Affected Files & Key Changes:**

**I. New File: `cyberdelta/apis/base/rate_limit_strategy_interface.py` (or similar interfaces file)**
    1.  **Define `RateLimitStrategy` (ABC/Protocol):**
        ```python
        from abc import ABC, abstractmethod
        from typing import Any, Dict, Optional

        class RateLimitStrategy(ABC):
            @abstractmethod
            async def prepare_and_acquire(self, request_context: Dict[str, Any]) -> Dict[str, Any] | None:
                """
                Prepares for and acquires necessary rate limit tokens/permissions.
                Can optionally modify and return the request data payload if needed
                (e.g., to inject a rate-limit specific nonce, though not used by HL/BP REST).
                Should raise APIError(code=RATE_LIMITED) if acquisition times out or fails.

                Args:
                    request_context: Dict containing details like 'method', 'endpoint',
                                     'action_payload', 'exchange_name', 'request_weight'.
                Returns:
                    Optionally, a modified action_payload dict, or None if no modifications.
                """
                pass
        ```

**II. New File: `cyberdelta/apis/base/simple_rate_limit_strategy.py`**
    1.  **Implement `SimpleTokenBucketStrategy(RateLimitStrategy)`:**
        ```python
        from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime # Primitive
        # Import RateLimitStrategy interface

        class SimpleTokenBucketStrategy(RateLimitStrategy):
            def __init__(self, limiter: TokenBucketRateLimiterRuntime, default_request_weight: int = 1):
                self.limiter = limiter
                self.default_request_weight = default_request_weight

            async def prepare_and_acquire(self, request_context: Dict[str, Any]) -> None:
                cost = request_context.get("request_weight", self.default_request_weight)
                if cost > 0: # Only acquire if cost is positive
                    await self.limiter.acquire(tokens_to_consume=cost)
                return None # Does not modify data payload
        ```

**III. New File: `cyberdelta/apis/hyperliquid/hl_request_weighter.py`**
    1.  **Implement `HyperliquidRequestWeighter` Class:**
        *   `__init__(self, hl_exchange_config: ExchangeSpecificConfig)`: Stores the Hyperliquid-specific section of `AppSettings.exchanges`.
        *   `get_ip_weight(self, endpoint: str, action_payload: dict | None) -> int`:
            *   Calculates IP weight:
                *   If `endpoint == "/exchange"`: `batch_length = len(action_payload["actions"])` (default 1), returns `self.hl_exchange_config.exchange_action_base_ip_weight + (batch_length // 40)`.
                *   Else if `endpoint == "/info"`: `api_type = action_payload.get("type")`, looks up in `self.hl_exchange_config.info_request_type_ip_weights`, falls back to `self.hl_exchange_config.default_info_weight`.
                *   Else: returns `self.hl_exchange_config.default_info_weight` (log warning).
        *   `get_address_action_count(self, endpoint: str, action_payload: dict | None) -> int`:
            *   If `endpoint == "/exchange"`: returns `len(action_payload["actions"])` (default 1).
            *   Else: returns 0.

**IV. New File: `cyberdelta/apis/hyperliquid/hl_rate_limit_strategy.py`**
    1.  **Implement `HyperliquidRateLimitStrategy(RateLimitStrategy)`:**
        ```python
        from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
        from cyberdelta.config.config_models import ExchangeSpecificConfig # For typing hl_exchange_config
        from cyberdelta.apis.base.rate_limit_strategy_interface import RateLimitStrategy
        from .hl_request_weighter import HyperliquidRequestWeighter # Local import

        class HyperliquidRateLimitStrategy(RateLimitStrategy):
            def __init__(self, hl_exchange_config: ExchangeSpecificConfig):
                self._request_weighter = HyperliquidRequestWeighter(hl_exchange_config)

                # IP Weight Limiter
                ip_rate_rpm = hl_exchange_config.ip_weight_limit_per_minute
                ip_rate_rps = ip_rate_rpm / 60.0
                ip_bucket = max(1, int(ip_rate_rps * 2)) # Example bucket factor
                self._ip_weight_limiter = TokenBucketRateLimiterRuntime(rate=ip_rate_rps, bucket_size=ip_bucket)

                # Address Action Count Limiter (Safety Net)
                aa_rate_rpm = hl_exchange_config.address_action_safety_net.rate_per_minute
                aa_rate_rps = aa_rate_rpm / 60.0
                aa_bucket = max(1, int(aa_rate_rps * 2)) # Example bucket factor
                self._address_action_limiter = TokenBucketRateLimiterRuntime(rate=aa_rate_rps, bucket_size=aa_bucket)

            async def prepare_and_acquire(self, request_context: Dict[str, Any]) -> None:
                endpoint = request_context["endpoint"]
                action_payload = request_context.get("action_payload") # Can be None for GETs

                ip_cost = self._request_weighter.get_ip_weight(endpoint, action_payload)
                address_action_cost = self._request_weighter.get_address_action_count(endpoint, action_payload)

                if ip_cost > 0:
                    await self._ip_weight_limiter.acquire(tokens_to_consume=ip_cost)

                if address_action_cost > 0: # Only for /exchange actions
                    await self._address_action_limiter.acquire(tokens_to_consume=address_action_cost)

                return None # Does not modify data payload
        ```

**V. `cyberdelta/apis/hyperliquid/hl_api.py` (`HyperliquidAPI`):**
    1.  **Modify `__init__`:**
        *   Import `HyperliquidRateLimitStrategy`.
        *   Create `hl_strategy = HyperliquidRateLimitStrategy(self.exchange_config)`.
        *   In `super().__init__(...)`, pass `rate_limit_strategy=hl_strategy`.
        *   (Authenticator setup remains, to be passed to `super` as `authenticator=self._hl_authenticator`).
    2.  **Remove `_request` Override (if any existed from previous refactors that made it call `_http_client` directly).** `HyperliquidAPI` will now use the base `ExchangeAPI._request` method, which will polymorphically call `HyperliquidRateLimitStrategy.prepare_and_acquire()`.

**VI. `cyberdelta/apis/backpack/bp_api.py` (`BackpackAPI`):**
    1.  **Modify `__init__`:**
        *   Import `SimpleTokenBucketStrategy` and `TokenBucketRateLimiterRuntime`.
        *   Calculate Backpack's simple rate/bucket from `self.exchange_config.rate_limit_per_minute`.
        *   Create `bp_limiter_primitive = TokenBucketRateLimiterRuntime(rate=bp_rate_s, bucket_size=bp_bucket)`.
        *   Create `bp_strategy = SimpleTokenBucketStrategy(limiter=bp_limiter_primitive, default_request_weight=1)`.
        *   In `super().__init__(...)`, pass `rate_limit_strategy=bp_strategy`.
    2.  **Remove `_request` Override (if any).** It will use the base `ExchangeAPI._request`.

**Testing Requirements:**
*   Unit tests for `SimpleTokenBucketStrategy.prepare_and_acquire`.
*   Unit tests for `HyperliquidRequestWeighter` (all calculation branches).
*   Unit tests for `HyperliquidRateLimitStrategy.prepare_and_acquire` (verify it calls its internal limiters with correct costs from weighter).
*   `HyperliquidAPI.__init__` correctly instantiates and passes `HyperliquidRateLimitStrategy` to `super`.
*   `BackpackAPI.__init__` correctly instantiates and passes `SimpleTokenBucketStrategy` to `super`.
*   End-to-end tests for Hyperliquid (as in Phase 1 Prompt 3) must pass, confirming the new strategy pattern correctly respects IP weights and address action counts.
*   End-to-end tests for Backpack must pass, confirming its simple rate limiting still works via `SimpleTokenBucketStrategy` called by base `ExchangeAPI._request`.

**Project Rules Adherence:** Static Analysis V3, No Silencing V4. Full type hinting.
```

---
**Prompt 3 for Angel (Phase 3 - Error Handling & WebSocket Limits - Future Enhancement, Lower Priority)**

This prompt remains the same as in the previous plan, as it addresses downstream concerns not directly altered by the core "Rate Limit Strategy" pattern implementation for REST API calls. It focuses on:
1.  Enhancing `HyperliquidErrorMapper` for address-based limit errors.
2.  Implementing robust IP Ban state management (e.g., by adding `trigger_ip_ban` to `TokenBucketRateLimiterRuntime` and having `ExchangeAPI._request` call it on specific errors).
3.  Preparing for WebSocket message sending rate limits in `WebSocketManager` if future use cases require sending many commands over WS.


Okay, Human Lead. Here is the detailed, contextual prompt for Angel covering Phase 3, which focuses on enhancing error handling for rate limits (especially IP bans and specific address-based limit messages) and preparing for potential WebSocket command rate limiting.

This phase assumes Phase 1 and Phase 2 (implementing the "Rate Limit Strategy" pattern for REST API calls) are complete and functional.

**Prompt 3 for Angel (Phase 3 - Error Handling & WebSocket Limits)**

**Objective:** Phase 3 Rate Limiting Refactor - Enhance Hyperliquid Error Handling and Prepare for WebSocket Command Rate Limiting.

**Context & Rationale:**
With the "Rate Limit Strategy" pattern implemented for REST API calls (Phases 1 & 2), this phase focuses on improving robustness and future-proofing:
1.  **Enhanced Error Mapping:** `HyperliquidErrorMapper` will be updated to better identify specific error messages from Hyperliquid that might indicate an *address-based* rate limit has been hit (distinct from a general IP ban), potentially allowing for more nuanced retry logic (e.g., respecting the "one request every 10 seconds" fallback).
2.  **IP Ban State Management:** Implement a mechanism so that if a general IP ban is detected (e.g., persistent HTTP 403s from Hyperliquid after hitting IP weight limits), all REST requests to Hyperliquid from that IP are paused for the documented ban duration (e.g., ~65 seconds). This will likely involve adding state to the Hyperliquid-specific `TokenBucketRateLimiterRuntime` instance that manages IP weights.
3.  **WebSocket Command Rate Limiting (Preparation):** For future use cases where we might send commands (not just subscriptions) at a high frequency over WebSocket to Hyperliquid, we will add the configuration and basic infrastructure for a rate limiter within `WebSocketManager` specifically for *outgoing* messages to Hyperliquid.

**Affected Files & Key Changes:**

1.  `cyberdelta/apis/hyperliquid/hl_errors_mapper.py` (`HyperliquidErrorMapper`)
2.  `cyberdelta/apis/rate_limiter.py` (`TokenBucketRateLimiterRuntime`)
3.  `cyberdelta/apis/base/exchange_api.py` (`ExchangeAPI._request` - minor change to call ban trigger)
4.  `cyberdelta/apis/hyperliquid/hl_rate_limit_strategy.py` (`HyperliquidRateLimitStrategy` - minor change to interact with ban trigger on its IP limiter)
5.  `cyberdelta/config/config_models.py` (`ExchangeSpecificConfig` - for new WS rate limit config)
6.  `config/config.yaml` (for new WS rate limit config)
7.  `cyberdelta/apis/connectivity/ws_manager.py` (`WebSocketManager` - for WS send limiter)

**Prerequisites:**
*   Successful completion of Phase 1 and Phase 2 for REST API rate limiting using the "Rate Limit Strategy" pattern.
*   `HyperliquidRateLimitStrategy` correctly manages its internal `_ip_weight_limiter` and `_address_action_limiter`.

**Detailed Instructions:**

**I. Enhance `HyperliquidErrorMapper` (`apis/hyperliquid/hl_errors_mapper.py`):**
    1.  **Research & Identify Specific Error Messages:**
        *   Investigate (if possible via logs or community info) if Hyperliquid returns distinct error messages when the *address-based* action limit is hit, especially if it indicates the "one request every 10 seconds" fallback mode.
    2.  **Update Error Categorization:**
        *   In `_categorize_hyperliquid_error` (or `map_string_error` / `map_exchange_error`):
            *   Add logic (e.g., new regex patterns or string checks) to detect these specific address-based rate limit error messages.
            *   If detected, map them to `APIErrorCode.RATE_LIMITED`.
            *   Crucially, if the error message implies the "one request every 10 seconds" state, the `APIError` generated by the mapper should include `retry_after=10.5` (or similar, with a small buffer). This allows the retry logic in `ExchangeAPI` or higher layers to respect this specific cooldown.
    3.  **Distinguish from IP Ban:** Ensure that general IP ban errors (e.g., a persistent HTTP 403 that isn't a specific "address limit" message) are still mapped in a way that can trigger the IP ban mechanism (Task II).

**II. Implement IP Ban State Management:**
    1.  **Modify `TokenBucketRateLimiterRuntime` (`apis/rate_limiter.py`):**
        *   Add new instance attribute: `is_ip_banned_until: float | None = None` (stores `time.monotonic()` value).
        *   **Modify `acquire` Method:**
            *   At the very beginning (inside the lock):
                ```python
                if self.is_ip_banned_until is not None:
                    now_mono = time.monotonic()
                    if now_mono < self.is_ip_banned_until:
                        wait_time_for_ban = self.is_ip_banned_until - now_mono
                        # Release lock while sleeping
                        self.lock.release()
                        try:
                            logger.warning(f"IP ban active for rate limiter. Waiting {wait_time_for_ban:.2f}s.")
                            await asyncio.sleep(wait_time_for_ban)
                        finally:
                            await self.lock.acquire() # Re-acquire lock
                        # After waiting, clear the ban state and proceed to token acquisition
                        self.is_ip_banned_until = None
                    else: # Ban duration has passed
                        self.is_ip_banned_until = None
                ```
            *   The rest of the `acquire` logic (token refilling, checking `tokens_to_consume`) follows.
        *   **New Method: `async def trigger_ip_ban(self, duration_seconds: float) -> None`:**
            *   `async with self.lock:`
            *   `self.is_ip_banned_until = time.monotonic() + duration_seconds`
            *   `logger.critical(f"Rate limiter IP BAN triggered for {duration_seconds:.1f}s.")`

    2.  **Modify `HyperliquidRateLimitStrategy` (`apis/hyperliquid/hl_rate_limit_strategy.py`):**
        *   **Modify `prepare_and_acquire`:**
            *   It already calls `acquire` on `self._ip_weight_limiter` and `self._address_action_limiter`. This part is fine.
            *   **Add error handling around these `acquire` calls specifically to detect if an IP ban was respected.** The `acquire` method of `TokenBucketRateLimiterRuntime` now handles the sleeping for an active ban. This strategy method doesn't need to change *its* calls to `acquire`, but the system needs to know how to *trigger* the ban on the correct limiter.

    3.  **Modify `ExchangeAPI._request` (or its error handling part) (`apis/base/exchange_api.py`):**
        *   **In the `except HttpRequestFailedError as e_http_failed:` block (or similar for other critical, persistent errors like repeated timeouts):**
            *   If `self.exchange_name == ExchangeName.HYPERLIQUID.value` AND the error strongly indicates an IP ban (e.g., persistent HTTP 403 with a rate-limit-like message from `HyperliquidErrorMapper`, or after multiple `APIErrorCode.RATE_LIMITED` retries have failed):
                *   Get the Hyperliquid IP weight limiter instance:
                    *   If `self.rate_limit_strategy` is `HyperliquidRateLimitStrategy`, it would be `self.rate_limit_strategy._ip_weight_limiter`. (This requires making `_ip_weight_limiter` accessible, perhaps via a method on the strategy, or `ExchangeAPI` needs to know the conventional name if the strategy fetched it from `RateLimiterService`).
                    *   **Simpler if using the "Rate Limit Strategy" pattern:** `HyperliquidRateLimitStrategy` itself could have a method `trigger_ip_ban_on_main_pool(duration_seconds)`.
                *   Call `await chosen_ip_limiter_instance.trigger_ip_ban(65.0)`.
        *   This logic needs to be carefully placed to ensure it targets the correct limiter instance responsible for Hyperliquid's IP-based limits.

**III. Prepare for WebSocket Command Rate Limiting (Future - Basic Setup):**
    1.  **Modify `ExchangeSpecificConfig` (`config/config_models.py`):**
        *   Add an optional field for Hyperliquid:
            `websocket_send_rate_per_minute: int | None = Field(default=None, gt=0, description="Hyperliquid: Max outgoing WS messages (commands) per minute.")`
        *   Update validator to check it if Hyperliquid.
    2.  **Modify `config.yaml` (Hyperliquid Section):**
        *   Add `websocket_send_rate_per_minute: 1800` (Example: 30 msg/sec, slightly under HL's 2000/min documented general WS message limit).
    3.  **Modify `WebSocketManager` (`apis/connectivity/ws_manager.py`):**
        *   **In `__init__`:**
            *   Accept an optional `outgoing_message_limiter: TokenBucketRateLimiterRuntime | None = None` parameter.
            *   Store it as `self._outgoing_message_limiter`.
        *   **In `send_json()` (and `send_text()` if we add it):**
            *   `if self._outgoing_message_limiter:`
            *   `    await self._outgoing_message_limiter.acquire(1)`
            *   Then proceed to send the message.
    4.  **Modify `HyperliquidAPI.__init__` (`apis/hyperliquid/hl_api.py`):**
        *   When it instantiates `WebSocketManager` (if it does so directly, or if `ExchangeAPI.__init__` does):
            *   If `self.exchange_config.websocket_send_rate_per_minute` is set for Hyperliquid:
                *   Create a `TokenBucketRateLimiterRuntime` instance based on this rate.
                *   Pass this instance as `outgoing_message_limiter` to the `WebSocketManager` constructor.
    *   **Rationale:** This sets up the infrastructure. Actual use depends on sending commands over WS, which is not our primary mode for Hyperliquid actions currently.

**Testing Requirements:**
*   **`HyperliquidErrorMapper`:** Unit tests for new error string mappings for address-based limits, ensuring `retry_after` is set correctly.
*   **IP Ban Logic in `TokenBucketRateLimiterRuntime`:**
    *   Unit tests for `trigger_ip_ban()` and `acquire()`: verify `acquire()` sleeps if `is_ip_banned_until` is active, and clears the ban after waiting.
*   **Integration of IP Ban Triggering:**
    *   Mock API responses in `HttpClient.request` to simulate an IP ban condition (e.g., persistent 403).
    *   Verify that `ExchangeAPI._request` correctly identifies this and calls `trigger_ip_ban()` on the appropriate Hyperliquid IP weight limiter.
*   **WebSocket Sending Limiter (if fully implemented beyond config):**
    *   Unit tests for `WebSocketManager.send_json()` to ensure it acquires from its limiter if one is configured.

**Project Rules Adherence:** Static Analysis V3, No Silencing V4. Full type hinting.



Here is the full sequence of prompts:

---

**Overall Goal:** Refactor the API client layer to ensure `ExchangeAPI` is truly exchange-agnostic, especially regarding rate limit error handling. `retry_after` information parsed from error messages will be made available via `APIError`, and the decision to act upon it (e.g., by informing the `RateLimitStrategy`) will reside in higher-level application code that uses the `ExchangeAPI` instance. The Hyperliquid IP ban logic will be moved out of `ExchangeAPI._request` and into Hyperliquid-specific components.

---

**Sub-Prompt 1: Enhance `BackpackErrorMapper` to Parse `retry-after` (Re-issue of V5 with updated context)**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** BP_RL_P1_MAPPER_REFINED
**Task:** Enhance `BackpackErrorMapper` to Parse `retry-after` Durations from Error Messages

**1. Goal:**
Modify `BackpackErrorMapper.map_exchange_error` to accurately parse `retry-after` durations (in seconds) from Backpack Exchange's error response messages when a rate limit error occurs. This parsed duration must be populated into the `APIError.retry_after` field of the returned `APIError` object.

**2. Why This Is Important:**
Backpack Exchange does not use standard `Retry-After` HTTP headers. Parsing hints from error messages (e.g., "Retry after X seconds") and populating `APIError.retry_after` makes this exchange-specific delay information programmatically available. This `APIError.retry_after` value can be used by higher-level application logic (that calls `ExchangeAPI` methods) to make informed decisions, such as instructing the specific `RateLimitStrategy` to pause or logging the directive. This task *only* concerns the parsing and population by the mapper.

**3. File to Modify:**
*   `cyberdelta/apis/backpack/bp_error_mapper.py` (specifically the `map_exchange_error` method).

**4. Detailed Steps & Implementation Guidance:**
   *   **Locate Method:** The `map_exchange_error` method within `BackpackErrorMapper`.
   *   **Identify Rate Limit Conditions:** The method already identifies rate limit conditions and maps them to `APIErrorCode.RATE_LIMITED`. Ensure this is robust.
   *   **Implement `retry-after` Parsing Logic:**
      *   Execute this parsing *only if* the resolved `api_error_code_enum` is `APIErrorCode.RATE_LIMITED`.
      *   Initialize `parsed_retry_after_seconds: float | None = None`.
      *   Use the `effective_exchange_message` variable as the text source.
      *   Use the `re` module (case-insensitive) to search for patterns:
          *   Primary: `r"Retry after (\d+) seconds?"`
          *   Secondary: `r"try again in (\d+) ms"`, `r"please wait (\d+)s"`, `r"wait for (\d+) milliseconds"`, `r"wait (\d+) seconds?"`
      *   Capture numeric value and unit. Convert to `float` seconds.
      *   Handle parsing errors gracefully; `parsed_retry_after_seconds` should remain `None` if no valid hint is found. The first successful parse should be used.
   *   **Logging for `retry-after` Parsing:**
      *   When `api_error_code_enum == APIErrorCode.RATE_LIMITED`, log `effective_exchange_message` at `DEBUG` ("Attempting to parse retry_after...").
      *   If `parsed_retry_after_seconds` is set, log at `INFO` ("Parsed `retry_after`... X.Y seconds.").
      *   If parsing attempted but no hint found, log at `DEBUG` ("No parsable `retry_after`...").
   *   **`APIError` Construction:**
      *   Populate `APIError(..., retry_after=parsed_retry_after_seconds, ...)`.
      *   Ensure `code` is `APIErrorCode.RATE_LIMITED.value` for this path.

**5. Testing Requirements:**
    *   **Unit Tests for `BackpackErrorMapper.map_exchange_error`:**
        *   Test with "Retry after 30 seconds" -> `retry_after == 30.0`.
        *   Test with "Try again in 1500 ms." -> `retry_after == 1.5`.
        *   Test with "Rate limit exceeded." (no time) -> `retry_after is None`.
        *   Test with `error_body="Wait for 10000 milliseconds"` -> `retry_after == 10.0`.
        *   Test with non-rate limit error -> `retry_after is None`.
        *   Test with malformed retry info (e.g., "Retry after twenty seconds.") -> `retry_after is None`.
        *   Ensure `APIError.code == APIErrorCode.RATE_LIMITED.value` when expected.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Model Architecture V1, Code Clarity.
```

---
**Sub-Prompt 2: Update `BackpackAPI` Documentation (Re-issue of V5 with updated context)**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** BP_RL_P2_DOCS_REFINED
**Task:** Update Documentation in `BackpackAPI` for Rate Limit Handling and `retry_after` Information

**1. Goal:**
Update docstrings and comments in `cyberdelta/apis/backpack/bp_api.py` to accurately reflect:
    a) That rate limiting for Backpack is primarily static, based on configuration.
    b) That `BackpackErrorMapper` now parses `retry_after` from error messages, making this information available in `APIError` for potential use by higher-level application logic or monitoring, not for direct use by `HttpClient` or basic rate limit strategies to alter their immediate behavior.

**2. Why This Is Important:**
Clear documentation is vital for maintainability and for understanding how Backpack's rate limiting is handled, especially its reliance on static configuration and the informational nature of the parsed `retry_after`.

**3. File to Modify:**
*   `cyberdelta/apis/backpack/bp_api.py`

**4. Detailed Steps & Implementation Guidance:**

   *   **Update `BackpackAPI` Class Docstring:**
      *   In the rate limiting section, state:
          *   Proactive rate limiting uses `SimpleTokenBucketStrategy` based on `rate_limit_per_minute` from `ExchangeSpecificConfig`.
          *   Dynamic adjustment of this strategy from response headers is not performed (Backpack does not provide the necessary headers).
          *   When rate limit errors occur, `BackpackErrorMapper` parses `retry_after` durations from the error message body, populating `APIError.retry_after`. This value is informational and can be used by higher-level application logic (e.g., to inform the `RateLimitStrategy` instance to pause or for monitoring). `HttpClient`'s internal retry mechanism and the `SimpleTokenBucketStrategy` do not directly consume this `APIError.retry_after` to modify their behavior without higher-level intervention.

   *   **Update `BackpackAPI._update_rate_limit_from_headers` Method Docstring:**
      *   Reiterate it's a no-op for dynamic limiter adjustments due to lack of Backpack headers. State that `retry-after` hints are parsed from error *message bodies* by the mapper for informational purposes.

   *   **Review `BackpackAPI.__init__` Method Comments:**
      *   Ensure comments emphasize the static configuration of `SimpleTokenBucketStrategy`.

**5. Testing Requirements:**
*   Manual review of documentation by Human Lead.

**6. Project Rules Adherence:**
*   Static Analysis V3, Code Clarity.
```

---
**Sub-Prompt 3: Define `handle_exchange_retry_after` in `RateLimitStrategy` Interface**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** CORE_RL_P3_RL_INTERFACE_ABSTRACT_METHOD
**Task:** Add Abstract `handle_exchange_retry_after` Method to `RateLimitStrategy` Interface and Update Concrete Strategies

**1. Goal:**
1.  Add a new **abstract** asynchronous method `handle_exchange_retry_after` to the `RateLimitStrategy` interface in `cyberdelta/apis/base/rate_limit_strategy_interface.py`.
2.  Add placeholder (`pass`) implementations of this new abstract method to the existing concrete strategy classes: `SimpleTokenBucketStrategy` (in `cyberdelta/apis/base/simple_rate_limit_strategy.py`) and `HyperliquidRateLimitStrategy` (in `cyberdelta/apis/hyperliquid/hl_rate_limit_strategy.py`) to ensure they remain instantiable and compliant with the updated interface.

**2. Why This Is Important:**
This new abstract method provides a standardized way for higher-level application logic (after catching an `APIError` from `ExchangeAPI`) to inform a specific `RateLimitStrategy` instance about an exchange-advised `retry-after` delay. Making it abstract ensures all strategies acknowledge this potential interaction point. The pass-through implementations maintain current behavior for existing strategies until they are specifically enhanced.

**3. Files to Modify:**
*   `cyberdelta/apis/base/rate_limit_strategy_interface.py`
*   `cyberdelta/apis/base/simple_rate_limit_strategy.py`
*   `cyberdelta/apis/hyperliquid/hl_rate_limit_strategy.py`

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Modify `RateLimitStrategy` Interface:**
      *   In `cyberdelta/apis/base/rate_limit_strategy_interface.py`, add to the `RateLimitStrategy` class:
        ```python
        @abstractmethod
        async def handle_exchange_retry_after(self, duration_seconds: float, request_context: dict[str, Any]) -> None:
            """
            Handles an explicit 'retry_after' directive received from the
            exchange, typically called by higher-level application logic after an
            APIError with retry_after information has been caught.

            Concrete strategies MUST implement this method. Implementations can use
            this duration to, for example, temporarily pause token acquisition
            or adjust internal state. If a strategy cannot or should not utilize
            this information, it can implement this method with 'pass'.

            Args:
                duration_seconds: The exchange-advised delay in seconds.
                request_context: Context of the request that was rate-limited.
            """
            pass
        ```
      *   Ensure necessary imports (`abstractmethod`, `Any`) are present.

   **4.2. Update `SimpleTokenBucketStrategy`:**
      *   In `cyberdelta/apis/base/simple_rate_limit_strategy.py`, add to `SimpleTokenBucketStrategy`:
        ```python
        async def handle_exchange_retry_after(self, duration_seconds: float, request_context: dict[str, Any]) -> None:
            """Reacts to an exchange-advised retry_after directive.
            For this simple strategy, it means temporarily pausing its limiter.
            """
            # Note: The prompt for BackpackRateLimitStrategy will have it call self.limiter.trigger_ip_ban().
            # For the base SimpleTokenBucketStrategy, if it's directly used and needs to react,
            # it would also call trigger_ip_ban(). If it's meant to be a truly simple base
            # that doesn't react, 'pass' is acceptable. Let's implement the pause.
            if hasattr(self, 'limiter') and hasattr(self.limiter, 'trigger_ip_ban'):
                # Log the action being taken by this specific strategy
                logger = logging.getLogger(__name__) # Get logger if not already available as self.logger
                logger.info(
                    f"SimpleTokenBucketStrategy for {request_context.get('exchange_name', 'N/A')}: "
                    f"Received exchange-advised retry_after of {duration_seconds:.2f}s. "
                    f"Triggering temporary pause on its limiter."
                )
                await self.limiter.trigger_ip_ban(duration_seconds)
            else:
                # This case implies incorrect setup or that the limiter doesn't support banning
                pass
        ```
      *   Add `from typing import Any` and ensure logging is set up or `logger` is available.

   **4.3. Update `HyperliquidRateLimitStrategy`:**
      *   In `cyberdelta/apis/hyperliquid/hl_rate_limit_strategy.py`, add to `HyperliquidRateLimitStrategy`:
        ```python
        async def handle_exchange_retry_after(self, duration_seconds: float, request_context: dict[str, Any]) -> None:
            """
            Handles exchange-advised retry_after directives.
            Hyperliquid's primary rate limit feedback mechanism is an IP ban (403 error),
            which is handled by trigger_ip_ban_on_main_pool. If Hyperliquid were to
            provide explicit retry-after durations in other rate limit error messages,
            this method could be used to trigger a similar ban on the appropriate limiter pool.
            For now, this implementation will call trigger_ip_ban_on_main_pool, assuming any
            explicit retry-after from HL implies a general backoff is needed.
            """
            logger = logging.getLogger(__name__) # Get logger
            logger.info(
                f"HyperliquidRateLimitStrategy: Received exchange-advised retry_after of "
                f"{duration_seconds:.2f}s for {request_context.get('exchange_name', 'Hyperliquid')}. "
                f"Applying as a temporary IP ban on the main pool."
            )
            await self.trigger_ip_ban_on_main_pool(duration_seconds)
            # If more granular control based on request_context (e.g., endpoint_group) is needed
            # for different limiter pools within HyperliquidRateLimitStrategy,
            # that logic would be added here.
        ```
      *   Add `from typing import Any` and ensure logging is set up or `logger` is available.

**5. Testing Requirements:**
*   Ensure existing tests for strategies still pass. The classes must be instantiable.
*   Unit test the `handle_exchange_retry_after` method in `SimpleTokenBucketStrategy` by mocking its `limiter` and verifying `trigger_ip_ban` is called.
*   Unit test the `handle_exchange_retry_after` method in `HyperliquidRateLimitStrategy` by mocking its `_ip_weight_limiter` (or whatever it uses for `trigger_ip_ban_on_main_pool`) and verifying the correct call.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
```

---
**Sub-Prompt 4: Create and Implement `BackpackRateLimitStrategy` (Re-issue of V5 with updated context)**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** BP_RL_P4_BP_STRATEGY_REFINED
**Task:** Create and Implement `BackpackRateLimitStrategy` to Utilize `handle_exchange_retry_after`

**1. Goal:**
Create a new `BackpackRateLimitStrategy` class in `cyberdelta/apis/backpack/bp_rate_limit_strategy.py`. This class will inherit from `SimpleTokenBucketStrategy` and **override** the `handle_exchange_retry_after` method. The overridden method will call `self.limiter.trigger_ip_ban(duration_seconds)` to effectively pause the underlying token bucket for the exchange-advised duration.

**2. Why This Is Important:**
This provides a Backpack-specific rate limiting strategy that directly acts upon `retry_after` information parsed from Backpack's error messages (by `BackpackErrorMapper`). It ensures that after an explicit "wait" directive from Backpack, subsequent requests are appropriately delayed by the rate limiting layer itself, aligning with our architecture where strategies handle rate limit logic.

**3. Files to Create/Modify:**
*   **Create New File:** `cyberdelta/apis/backpack/bp_rate_limit_strategy.py`
*   **Modify (for import/use):** `cyberdelta/apis/backpack/bp_api.py` (to instantiate and use this new strategy).
*   **Modify (add to `__all__`):** `cyberdelta/apis/backpack/__init__.py`

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Create `BackpackRateLimitStrategy` (in `bp_rate_limit_strategy.py`):**
      *   Imports:
          ```python
          import logging # For logger
          from typing import Any
          from cyberdelta.apis.base.simple_rate_limit_strategy import SimpleTokenBucketStrategy
          # TokenBucketRateLimiterRuntime might be needed for __init__ type hint if not inferred
          from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime 
          
          logger = logging.getLogger(__name__) # Module-level logger
          ```
      *   Class Definition: `class BackpackRateLimitStrategy(SimpleTokenBucketStrategy):`
      *   **Constructor (`__init__`):**
          *   Accept `limiter: TokenBucketRateLimiterRuntime` and `default_request_weight: int = 1`.
          *   Call `super().__init__(limiter, default_request_weight)`.
      *   **Implement `async def handle_exchange_retry_after(self, duration_seconds: float, request_context: dict[str, Any]) -> None`:**
          *   This overrides the method from the `RateLimitStrategy` interface (implemented in `SimpleTokenBucketStrategy` perhaps as `pass` or with basic logging).
          *   Log at `INFO` level: f"BackpackRateLimitStrategy for {request_context.get('exchange_name', 'Backpack')}: Received exchange-advised retry_after of {duration_seconds:.2f}s. Triggering temporary pause on limiter."
          *   Call `await self.limiter.trigger_ip_ban(duration_seconds)`.

   **4.2. Update `cyberdelta/apis/backpack/__init__.py`:**
      *   Add `BackpackRateLimitStrategy` to `__all__`.
      *   Add `from .bp_rate_limit_strategy import BackpackRateLimitStrategy`.

   **4.3. Modify `BackpackAPI.__init__` (in `cyberdelta/apis/backpack/bp_api.py`):**
      *   Import the new `BackpackRateLimitStrategy`:
          ```python
          from cyberdelta.apis.backpack.bp_rate_limit_strategy import BackpackRateLimitStrategy
          ```
      *   Instead of `SimpleTokenBucketStrategy`, instantiate `BackpackRateLimitStrategy`:
          ```python
          # bp_limiter_primitive = TokenBucketRateLimiterRuntime(...) # This is already created
          bp_strategy = BackpackRateLimitStrategy(
              limiter=bp_limiter_primitive, default_request_weight=1
          )
          ```
      *   Pass this `bp_strategy` to `super().__init__(..., rate_limit_strategy=bp_strategy, ...)`.

**5. Testing Requirements:**
*   **Unit Tests for `BackpackRateLimitStrategy.handle_exchange_retry_after`:**
    *   Instantiate `BackpackRateLimitStrategy` with a mocked `TokenBucketRateLimiterRuntime`.
    *   Call `await strategy.handle_exchange_retry_after(duration_seconds=10.5, request_context={"exchange_name": "backpack"})`.
    *   Assert `mocked_limiter.trigger_ip_ban` was called once with `10.5`.
*   Verify `BackpackAPI` can be instantiated with this new strategy.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
```

---
**Sub-Prompt 5: Refactor `ExchangeAPI._request` to be Truly Agnostic and Remove Hyperliquid IP Ban Logic**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** CORE_RL_P5_EXCHANGE_API_AGNOSTIC_REFACTOR
**Task:** Refactor `ExchangeAPI._request` for True Agnosticism: Remove `handle_exchange_retry_after` Call and Hyperliquid IP Ban Logic

**1. Goal:**
Modify the `ExchangeAPI._request` method in `cyberdelta/apis/base/exchange_api.py` to ensure it is **truly exchange-agnostic**. This involves:
    1.  Removing the direct call to `self.rate_limit_strategy.handle_exchange_retry_after(...)` that was added in a previous iteration. The responsibility to call this method on the strategy now lies with higher-level application code that catches the `APIError`.
    2.  Removing the Hyperliquid-specific IP ban detection logic (403 status + RATE_LIMITED code).

**2. Why This Is Important:**
This refactoring is critical to maintain a clean and consistent architecture where `ExchangeAPI` serves as a generic base class. It should not make decisions based on the content of `APIError.retry_after` nor contain logic specific to any single exchange. The `APIError` it raises (potentially containing `retry_after`) will be handled by the calling code.

**3. File to Modify:**
*   `cyberdelta/apis/base/exchange_api.py` (specifically the `_request` method)

**4. Detailed Steps & Implementation Guidance:**

   *   **Locate `_request` Method Error Handling:** Find the `try...except HttpRequestFailedError as e_http_failed:` block within the `async def _request(...)` method.
   *   **Remove `handle_exchange_retry_after` Call:**
      *   Delete the entire `if self.rate_limit_strategy and mapped_error.code == APIErrorCode.RATE_LIMITED.value ...:` block that calls `await self.rate_limit_strategy.handle_exchange_retry_after(...)`.
      *   After `mapped_error = self.error_mapper.map_exchange_error(...)` is called, the method should simply proceed to `raise mapped_error from e_http_failed`.
   *   **Remove Hyperliquid-Specific IP Ban Logic:**
      *   Delete the code block:
        ```python
        # if ( # This entire block should be removed
        #     self.exchange_name == "hyperliquid" 
        #     and mapped_error.code == APIErrorCode.RATE_LIMITED.value
        #     and e_http_failed.http_status == 403
        # ):
        #     # ... logic to call trigger_ip_ban_on_main_pool ...
        ```
   *   **Resulting Logic:** The `except HttpRequestFailedError` block should simplify to:
      ```python
      except HttpRequestFailedError as e_http_failed:
          logger.warning(...) # Existing logging for HTTP failure
          parsed_error_data = # ... existing logic to parse e_http_failed.exchange_message ...
          
          mapped_error = self.error_mapper.map_exchange_error(
              status_code=e_http_failed.http_status or 500,
              error_body=e_http_failed.exchange_message or "",
              error_data=parsed_error_data,
              request_path=request_url,
              original_exception=e_http_failed,
          )
          # Simply raise the mapped error. No further inspection or calls based on its content here.
          raise mapped_error from e_http_failed
      ```

**5. Testing Requirements:**
*   **Review Existing Tests:** Ensure tests for `ExchangeAPI._request` correctly reflect that it no longer calls `handle_exchange_retry_after` and does not have Hyperliquid-specific logic.
*   Tests should confirm that `APIError`s (potentially with `retry_after` populated by the specific mapper) are correctly propagated upwards.
*   **New/Modified Tests (if applicable):** Tests for higher-level components that *use* `ExchangeAPI` will eventually need to demonstrate correct handling of the `APIError` and subsequent calls to `RateLimitStrategy.handle_exchange_retry_after` *at that higher level*. This is outside the scope of this immediate refactoring of `ExchangeAPI._request`.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
```

---
**Sub-Prompt 6: Relocate Hyperliquid IP Ban Logic to `HyperliquidErrorMapper`**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** HL_RL_P6_HL_MAPPER_IP_BAN
**Task:** Relocate Hyperliquid IP Ban Detection Logic to `HyperliquidErrorMapper`

**1. Goal:**
Modify `HyperliquidErrorMapper.map_exchange_error` to detect the specific conditions indicating a Hyperliquid IP ban (HTTP 403 status + rate limit error message/code) and map this to a distinct, new `APIErrorCode` (e.g., `IP_BAN_SUSPECTED`).

**2. Why This Is Important:**
The generic `ExchangeAPI._request` method should not contain logic specific to Hyperliquid's IP ban error signature. By moving this detection into `HyperliquidErrorMapper`, we keep the error interpretation exchange-specific, and the resulting `APIError` can carry a more precise code that higher-level application logic (calling HyperliquidAPI) can use to trigger appropriate actions (like informing `HyperliquidRateLimitStrategy` to activate its ban).

**3. Files to Modify:**
*   `cyberdelta/apis/hyperliquid/hl_errors_mapper.py`
*   `cyberdelta/apis/models/api_error_codes.py` (to add the new `APIErrorCode`)

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Add New `APIErrorCode` (in `api_error_codes.py`):**
      *   Add a new enum member to `APIErrorCode`, for example:
          ```python
          IP_BAN_SUSPECTED = 119 # Or another appropriate number in a suitable range
          ```
      *   Ensure the enum remains correctly structured and valued.

   **4.2. Enhance `HyperliquidErrorMapper.map_exchange_error` (in `hl_errors_mapper.py`):**
      *   **Locate Method:** The `map_exchange_error` method.
      *   **Priority Check for IP Ban:** *Before* the existing general error categorization logic (or as a high-priority part of it), add a specific check for the IP ban condition:
          *   If `status_code == 403` AND the error (derived from `error_data` or `error_body`) is categorized by `_categorize_hyperliquid_error` as `HyperliquidAPIErrorCategory.RATE_LIMIT_EXCEEDED` (or if the `error_body` contains typical rate limit messages that Hyperliquid uses for IP bans):
              *   Set `api_error_code_enum = APIErrorCode.IP_BAN_SUSPECTED`.
              *   The `effective_message` should still be the original error message from Hyperliquid.
              *   No `retry_after` is typically provided by Hyperliquid for IP bans in a parsable way; the ban duration (e.g., 60-65s) is usually known from experience/community. `APIError.retry_after` would likely remain `None` unless explicitly parsed.
              *   Construct and return the `APIError` with this specific code.
      *   **Existing Logic:** If the IP ban condition is not met, the method should proceed with its existing logic for categorizing and mapping other Hyperliquid errors.
      *   **Logging:** Add a `WARNING` or `CRITICAL` log message when an `IP_BAN_SUSPECTED` error is mapped.

**5. Testing Requirements:**
*   **Unit Tests for `HyperliquidErrorMapper.map_exchange_error`:**
    *   Test Case (IP Ban): Input: `status_code=403`, `error_body="Your IP has been rate limited for 1 minute..."` (or similar known Hyperliquid IP ban message pattern).
        *   Verify: Returned `APIError` has `code == APIErrorCode.IP_BAN_SUSPECTED.value`, `http_status == 403`.
    *   Test Case (Normal 403, Not Rate Limit): Input: `status_code=403`, `error_body="Forbidden action."`.
        *   Verify: Returned `APIError` has `code == APIErrorCode.AUTHENTICATION_FAILED.value` (or as per existing mapping for general 403), *not* `IP_BAN_SUSPECTED`.
    *   Test Case (Normal Rate Limit, Not 403): Input: `status_code=429`, `error_body="Rate limit exceeded"`.
        *   Verify: Returned `APIError` has `code == APIErrorCode.RATE_LIMITED.value`, *not* `IP_BAN_SUSPECTED`.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Model Architecture V1, Code Clarity.
```

---

This full set of prompts systematically refactors the system:
1.  `BackpackErrorMapper` extracts `retry_after`.
2.  `BackpackAPI` documentation reflects the informational nature of this.
3.  `RateLimitStrategy` interface gets the `handle_exchange_retry_after` abstract method, and existing strategies get `pass` implementations.
4.  `BackpackRateLimitStrategy` is created to *meaningfully implement* `handle_exchange_retry_after`.
5.  `ExchangeAPI._request` is cleaned of *all* exchange-specific rate limit reaction logic and the direct call to `handle_exchange_retry_after`.
6.  Hyperliquid's specific IP ban logic is moved from `ExchangeAPI._request` to its own `HyperliquidErrorMapper`.

The overall system will then rely on higher-level code (that calls `ExchangeAPI` methods) to catch an `APIError`, check if `retry_after` is present or if the code is `IP_BAN_SUSPECTED`, and then decide to call `the_api_instance.rate_limit_strategy.handle_exchange_retry_after(...)` or `the_hl_api_instance.rate_limit_strategy.trigger_ip_ban_on_main_pool(...)` respectively.

This achieves the desired agnosticism in `ExchangeAPI` and places exchange-specific reactions within exchange-specific components or strategy layers.
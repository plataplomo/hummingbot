

**Overall Goal:** Enhance Backpack API client to parse `retry-after` from error messages and integrate this information into the `RateLimitStrategy` layer for architecturally consistent handling of exchange-advised delays.

---

**Sub-Prompt 1: Enhance `BackpackErrorMapper` to Parse `retry-after`**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** BP_RL_P1_MAPPER
**Task:** Enhance `BackpackErrorMapper` to Parse `retry-after` Durations from Error Messages

**1. Goal:**
Modify `BackpackErrorMapper.map_exchange_error` to parse `retry-after` durations (in seconds) from Backpack Exchange's error response messages when a rate limit error occurs. This parsed duration must be populated into the `APIError.retry_after` field of the returned `APIError` object.

**2. Why This Is Important:**
Backpack Exchange does not use standard `Retry-After` HTTP headers. Parsing hints from error messages (e.g., "Retry after X seconds") and populating `APIError.retry_after` makes this information programmatically available for other system components (like the `RateLimitStrategy`) to use for more compliant and efficient retry behavior.

**3. File to Modify:**
*   `cyberdelta/apis/backpack/bp_error_mapper.py` (specifically the `map_exchange_error` method).

**4. Detailed Steps & Implementation Guidance:**
   *   **Locate Method:** The `map_exchange_error` method within `BackpackErrorMapper`.
   *   **Identify Rate Limit Conditions:** The method already identifies rate limit conditions (e.g., HTTP 429, specific error codes like "TOO_MANY_REQUESTS") and maps them to `APIErrorCode.RATE_LIMITED`. This logic should be sound.
   *   **Implement `retry-after` Parsing Logic:**
      *   This parsing logic should execute *only if* the resolved `api_error_code_enum` (the internal error code determined by the mapper) is `APIErrorCode.RATE_LIMITED`.
      *   Initialize a variable `parsed_retry_after_seconds: float | None = None`.
      *   The text source for parsing is the `effective_exchange_message` variable within `map_exchange_error`.
      *   Use the `re` (regular expression) module to search `effective_exchange_message` for patterns. Prioritize the pattern observed in community SDKs, then add robust fallbacks (all searches should be case-insensitive):
          *   **Primary Regex:** `r"Retry after (\d+) seconds?"` (optional 's' at the end of seconds)
          *   **Secondary Regexes (if primary fails):**
              *   `r"try again in (\d+) ms"`
              *   `r"please wait (\d+)s"`
              *   `r"wait for (\d+) milliseconds"`
              *   `r"wait (\d+) seconds?"` (optional 's')
      *   The regex should capture the numeric value. If a unit like "ms" or "milliseconds" is part of the pattern or typically accompanies the number, ensure your logic correctly identifies it.
      *   **Unit Conversion:** If milliseconds are detected/parsed, convert the numeric value to `float` seconds (e.g., `1500 ms` becomes `1.5`). If seconds are detected, use the numeric value directly as `float`.
      *   **Error Handling in Parsing:** If parsing fails for any pattern (e.g., regex doesn't match, captured group is not a valid number), `parsed_retry_after_seconds` should remain `None`. The overall error mapping process must not crash due to an unparseable `retry-after` hint.
      *   If a valid delay is successfully parsed and converted to seconds, assign it to `parsed_retry_after_seconds`. If multiple patterns could match, the first successful parse should be used.
   *   **Logging for `retry-after` Parsing:**
      *   If `api_error_code_enum == APIErrorCode.RATE_LIMITED`, log the `effective_exchange_message` at `DEBUG` level with a message like "Attempting to parse retry_after from Backpack rate limit message: '{message_text}'".
      *   If `parsed_retry_after_seconds` is successfully set, log at `INFO` level: "Parsed `retry_after` from Backpack message: {value} seconds."
      *   If parsing is attempted but no valid hint is found, log at `DEBUG` level: "No parsable `retry_after` information found in Backpack rate limit message."
   *   **`APIError` Construction:**
      *   When constructing the `APIError` instance at the end of `map_exchange_error`, ensure the `retry_after` parameter is populated with the final value of `parsed_retry_after_seconds`.
      *   The `code` field of the `APIError` should be `APIErrorCode.RATE_LIMITED.value` if this parsing path was taken. Other fields should be populated as per existing logic.

**5. Testing Requirements:**
    *   **Unit Tests for `BackpackErrorMapper.map_exchange_error`:**
        *   Test Case (Primary Regex - Seconds): `status_code=429`, `error_data={"message": "Retry after 30 seconds"}`. Verify `APIError.retry_after == 30.0`.
        *   Test Case (Milliseconds): `status_code=429`, `error_data={"message": "Try again in 1500 ms."}`. Verify `APIError.retry_after == 1.5`.
        *   Test Case (No Retry Info): `status_code=429`, `error_data={"message": "Rate limit exceeded."}`. Verify `APIError.retry_after is None`.
        *   Test Case (Error Body String): `status_code=429`, `error_body="Wait for 10000 milliseconds then try again"`, `error_data=None`. Verify `APIError.retry_after == 10.0`.
        *   Test Case (Non-Rate Limit Error): `status_code=400`, `error_data={"message": "Bad request."}`. Verify `APIError.retry_after is None`.
        *   Test Case (Malformed Retry - Text Number): `status_code=429`, `error_data={"message": "Retry after twenty seconds."}`. Verify `APIError.retry_after is None`.
        *   Ensure all tests check `APIError.code == APIErrorCode.RATE_LIMITED.value` when a rate limit is expected.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Model Architecture V1, Code Clarity.
*   Comment regex patterns and complex parsing logic.
```

---
**Sub-Prompt 2: Update Backpack API Documentation**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** BP_RL_P2_DOCS
**Task:** Update Documentation in `BackpackAPI` for Rate Limit Handling

**1. Goal:**
Update docstrings and comments in `cyberdelta/apis/backpack/bp_api.py` to accurately reflect the current rate limiting mechanism for Backpack, including the new parsing of `retry-after` from error messages by `BackpackErrorMapper`.

**2. Why This Is Important:**
Clear documentation ensures maintainability and correct understanding of how rate limiting is handled for the Backpack client, especially given its reliance on static configuration and error message parsing rather than standard HTTP headers.

**3. File to Modify:**
*   `cyberdelta/apis/backpack/bp_api.py`

**4. Detailed Steps & Implementation Guidance:**

   *   **Update `BackpackAPI` Class Docstring:**
      *   Locate the main docstring for the `BackpackAPI` class.
      *   In the section discussing rate limiting (or add one if it's sparse), ensure the following points are clearly stated:
          *   Proactive rate limiting is managed by a `SimpleTokenBucketStrategy` initialized using the static `rate_limit_per_minute` from the exchange-specific configuration (`ExchangeSpecificConfig`).
          *   The client does not dynamically adjust this token bucket's parameters based on response headers, as Backpack Exchange does not appear to provide the necessary standard headers (e.g., `X-RateLimit-Remaining`, `X-RateLimit-Reset`).
          *   When rate limit errors (e.g., HTTP 429 or specific Backpack error codes like "TOO_MANY_REQUESTS") are encountered, the `BackpackErrorMapper` (modified in task BP_RL_P1_MAPPER) now attempts to parse `retry-after` durations from the error message body.
          *   If a `retry-after` duration is successfully parsed, it is populated in the `APIError.retry_after` field (in seconds). This information is then available for consumption by the `RateLimitStrategy` layer (via `ExchangeAPI._request`) or other higher-level system components to inform more precise retry or pause behavior.

   *   **Update `BackpackAPI._update_rate_limit_from_headers` Method Docstring:**
      *   Locate the docstring for the `_update_rate_limit_from_headers` method.
      *   Ensure it clearly states:
          *   "Backpack Exchange does not provide standard or known non-standard HTTP response headers that detail remaining rate limits or explicit `Retry-After` durations for dynamic adjustment of client-side rate limiters."
          *   "Therefore, this method remains a no-op concerning dynamic adjustment of the `SimpleTokenBucketStrategy`."
          *   "Hints for `retry-after` delays are primarily parsed from error message *bodies* by `BackpackErrorMapper` when a rate limit error occurs."
      *   Verify the `logger.debug` call within this method accurately reflects this (e.g., logs that no actionable headers were found for dynamic rate limit adjustment).

   *   **Review `BackpackAPI.__init__` Method Comments:**
      *   Locate the `__init__` method of `BackpackAPI`.
      *   Near the instantiation of `SimpleTokenBucketStrategy`, ensure comments clearly state that its parameters (e.g., `rate`, `bucket_size`) are derived from the static `exchange_config.rate_limit_per_minute`.

**5. Testing Requirements:**
*   This task is primarily documentation. Manual review by Human Lead is sufficient. Ensure the updated comments and docstrings are clear, accurate, and reflect the actual implementation after BP_RL_P1_MAPPER is complete.

**6. Project Rules Adherence:**
*   Static Analysis V3 (for any minor code changes, like log messages).
*   Code Clarity: Documentation must be clear and unambiguous.
```

---

Sub-Prompt 3 (Revised): Extend RateLimitStrategy Interface with Optional Method
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** BP_RL_P3_RL_INTERFACE_OPTIONAL
**Task:** Extend `RateLimitStrategy` Interface with an Optional Method for Exchange-Advised Delays

**1. Goal:**
Add a new, **optional** asynchronous method to the `RateLimitStrategy` abstract base class (`cyberdelta/apis/base/rate_limit_strategy_interface.py`). This method will allow `ExchangeAPI._request` to inform the strategy about explicit `retry-after` directives received from an exchange, but strategies are not required to implement specific logic for it.

**2. Why This Is Important:**
This provides a consistent, non-breaking architectural hook for communicating exchange-advised delays. Strategies that can benefit from this information (like the upcoming `BackpackRateLimitStrategy`) can override the method, while existing strategies remain unaffected.

**3. File to Modify:**
*   `cyberdelta/apis/base/rate_limit_strategy_interface.py`

**4. Detailed Steps & Implementation Guidance:**

   *   **Add New Optional Method:**
      *   In the `RateLimitStrategy` class, define a new `async` method (NOT an `@abstractmethod`):
        ```python
        # In cyberdelta/apis/base/rate_limit_strategy_interface.py
        from abc import ABC, abstractmethod # Keep ABC, abstractmethod for other methods
        from typing import Any # Ensure Any is imported

        class RateLimitStrategy(ABC):
            # ... existing abstract methods like prepare_and_acquire ...

            async def handle_exchange_retry_after(self, duration_seconds: float, request_context: dict[str, Any]) -> None:
                """
                Optional method for strategies to react to an explicit 'retry_after'
                directive received from the exchange after a request has failed with
                a rate limit error.

                The base implementation does nothing (`pass`). Subclasses should override
                this method if they can make use of this information (e.g., to
                temporarily pause token acquisition or adjust internal state).

                Args:
                    duration_seconds: The exchange-advised delay in seconds.
                    request_context: Context of the request that was rate-limited,
                                     containing details like 'exchange_name', 'method',
                                     'endpoint', 'endpoint_group'.
                """
                pass # Default implementation does nothing
        ```
      *   Ensure the method is asynchronous (`async def`).
      *   The docstring should clearly indicate that this is an optional method for subclasses to override and that the base implementation is a no-op.

**5. Testing Requirements:**
*   No specific unit tests are required for this interface change itself, as it's a non-abstract method with a default `pass` implementation. Tests for `ExchangeAPI._request` (in a later step) will verify it can call this method without error on any strategy.

**6. Project Rules Adherence:**
*   Static Analysis V3.
*   No Silencing V4.
*   Code Clarity: Ensure the docstring for the new method is clear about its purpose, parameters, and optional nature for implementers.
```

---
**Sub-Prompt 4: Create and Implement `BackpackRateLimitStrategy`**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** BP_RL_P4_BP_STRATEGY
**Task:** Create and Implement `BackpackRateLimitStrategy` to Handle Exchange-Advised Delays

**1. Goal:**
Create a new `BackpackRateLimitStrategy` class that inherits from `SimpleTokenBucketStrategy`. Implement the `handle_exchange_retry_after` method in this new class to make the underlying `TokenBucketRateLimiterRuntime` pause token acquisition for the duration specified by Backpack's `retry-after` directive.

**2. Why This Is Important:**
This makes Backpack's rate limiting architecturally consistent. The strategy becomes responsible for reacting to explicit delays advised by the exchange, ensuring that subsequent requests correctly honor these waits. This leverages the `retry_after` information parsed by `BackpackErrorMapper` (Task BP_RL_P1_MAPPER) and the new interface method from Task BP_RL_P3_RL_INTERFACE.

**3. Files to Create/Modify:**
*   **Create New File:** `cyberdelta/apis/backpack/bp_rate_limit_strategy.py`
*   **Modify (for import/use):** `cyberdelta/apis/backpack/bp_api.py` (to use the new strategy)
*   **Modify (potentially, if not already importing `SimpleTokenBucketStrategy`):** `cyberdelta/apis/backpack/__init__.py` (if `bp_rate_limit_strategy.py` needs to import from `cyberdelta.apis.base.simple_rate_limit_strategy`)

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Create `BackpackRateLimitStrategy` (in `bp_rate_limit_strategy.py`):**
      *   Import necessary classes:
          ```python
          from cyberdelta.apis.base.simple_rate_limit_strategy import SimpleTokenBucketStrategy
          from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime # If type hinting constructor
          from typing import Any # For request_context
          ```
      *   Define the class: `class BackpackRateLimitStrategy(SimpleTokenBucketStrategy):`
      *   **Constructor (`__init__`):**
          *   It should accept the same arguments as `SimpleTokenBucketStrategy` (i.e., `limiter: TokenBucketRateLimiterRuntime`, `default_request_weight: int = 1`).
          *   Call `super().__init__(limiter, default_request_weight)`.
          *   No additional state is likely needed for this specific task.
      *   **Implement `async def handle_exchange_retry_after(self, duration_seconds: float, request_context: dict[str, Any]) -> None`:**
          *   This method overrides the one from the (to-be-updated) `RateLimitStrategy` interface.
          *   Log at `INFO` level: f"BackpackRateLimitStrategy: Received exchange-advised retry_after of {duration_seconds:.2f}s. Triggering temporary pause on limiter for exchange: {request_context.get('exchange_name', 'N/A')}."
          *   Call `await self.limiter.trigger_ip_ban(duration_seconds)`.
             *   The `self.limiter` is the `TokenBucketRateLimiterRuntime` instance inherited from `SimpleTokenBucketStrategy`.
             *   `trigger_ip_ban` will cause subsequent calls to `self.limiter.acquire()` (which happens inside `prepare_and_acquire`) to wait until the "ban" duration expires.
          *   The `request_context` can be used for more granular logging if desired, but the simple strategy applies the ban to its single global limiter.

   **4.2. Modify `BackpackAPI.__init__` (in `cyberdelta/apis/backpack/bp_api.py`):**
      *   Import the new `BackpackRateLimitStrategy`:
          ```python
          from cyberdelta.apis.backpack.bp_rate_limit_strategy import BackpackRateLimitStrategy
          ```
      *   Instead of directly instantiating `SimpleTokenBucketStrategy` for `bp_strategy`, instantiate `BackpackRateLimitStrategy`:
          ```python
          # Old:
          # bp_strategy = SimpleTokenBucketStrategy(
          #     limiter=bp_limiter_primitive, default_request_weight=1
          # )

          # New:
          bp_strategy = BackpackRateLimitStrategy(
              limiter=bp_limiter_primitive, default_request_weight=1
          )
          ```
      *   Pass this `bp_strategy` to the `super().__init__()` call.

**5. Testing Requirements:**
*   **Unit Tests for `BackpackRateLimitStrategy.handle_exchange_retry_after`:**
    *   Create a `BackpackRateLimitStrategy` instance with a mocked `TokenBucketRateLimiterRuntime` (the `limiter` attribute).
    *   Call `await strategy.handle_exchange_retry_after(duration_seconds=10.5, request_context={"exchange_name": "backpack"})`.
    *   Assert that `mocked_limiter.trigger_ip_ban` was called once with `10.5`.
*   **Integration Test Snippet (Conceptual - for `BackpackAPI` if testing its rate limiting behavior):**
    *   Simulate `BackpackAPI._request` making a call that results in a 429 error, where `BackpackErrorMapper` returns an `APIError` with `retry_after = 5.0`.
    *   Ensure that the `BackpackRateLimitStrategy` instance associated with the `BackpackAPI` has its `handle_exchange_retry_after` method called with `5.0`.
    *   Verify that a subsequent call to `BackpackAPI._request` (via `prepare_and_acquire` on the strategy) blocks for approximately 5 seconds. This might require careful mocking of `time.monotonic()` and `asyncio.sleep()` within the `TokenBucketRateLimiterRuntime`.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
```

---
**Sub-Prompt 5: Modify `ExchangeAPI._request` to Utilize `handle_exchange_retry_after`**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** BP_RL_P5_API_REQUEST
**Task:** Modify `ExchangeAPI._request` to Call `RateLimitStrategy.handle_exchange_retry_after`

**1. Goal:**
Update the `ExchangeAPI._request` method (in `cyberdelta/apis/base/exchange_api.py`) to call the new `handle_exchange_retry_after` method on its `RateLimitStrategy` instance when a rate limit error with a populated `retry_after` value is caught from the exchange.

**2. Why This Is Important:**
This change completes the architectural integration of exchange-advised `retry-after` delays. It ensures that when an exchange explicitly tells us to wait (via an error message parsed into `APIError.retry_after`), this information is passed to the rate limiting strategy, allowing the strategy to enforce this specific delay for subsequent requests. This respects our new `RateLimitStrategy` interface and its implementations.

**3. File to Modify:**
*   `cyberdelta/apis/base/exchange_api.py`

**4. Detailed Steps & Implementation Guidance:**

   *   **Locate `_request` Method:** Find the `async def _request(...)` method within the `ExchangeAPI` class.
   *   **Locate Error Handling Block:** Inside this method, find the `try...except HttpRequestFailedError as e_http_failed:` block. This is where errors from `self._http_client.request()` are caught and mapped.
   *   **After Error Mapping:**
      *   Inside this `except HttpRequestFailedError` block, after `mapped_error = self.error_mapper.map_exchange_error(...)` is called and you have the `mapped_error` (which is an `APIError` instance):
      *   Add a check:
          ```python
          if (
              self.rate_limit_strategy
              and mapped_error.code == APIErrorCode.RATE_LIMITED.value
              and mapped_error.retry_after is not None
              and mapped_error.retry_after > 0
          ):
              logger.info(
                  f"[{self.exchange_name}] Exchange advised retry_after: "
                  f"{mapped_error.retry_after:.2f}s for {method} {endpoint}. "
                  f"Informing rate limit strategy."
              )
              request_context_for_strategy = {
                  "exchange_name": self.exchange_name,
                  "method": method,
                  "endpoint": endpoint, # This is the relative path
                  "endpoint_group": endpoint_group,
                  # Add other relevant context if available and useful for the strategy
              }
              try:
                  await self.rate_limit_strategy.handle_exchange_retry_after(
                      duration_seconds=mapped_error.retry_after,
                      request_context=request_context_for_strategy,
                  )
              except Exception as e_strat_handle:
                  logger.error(
                      f"[{self.exchange_name}] Error calling "
                      f"rate_limit_strategy.handle_exchange_retry_after: {e_strat_handle}",
                      exc_info=True
                  )
          # The existing `raise mapped_error from e_http_failed` should follow this new block.
          ```
   *   **Consider Placement Relative to `HttpClient` Retries:**
        *   The `HttpClient` has its own internal retry loop. The call to `handle_exchange_retry_after` should ideally happen *before* the `HttpClient` itself decides to retry based on its own exponential backoff for this *specific failed request*.
        *   The `mapped_error` is raised from `_request` and typically caught by `HttpClient`'s retry loop if the error is deemed retryable by `HttpClient`.
        *   The modification above is correctly placed: when `_request` *first* processes the `HttpRequestFailedError` and maps it, it informs its strategy. If `HttpClient` then decides to retry *that same failed request*, subsequent calls to `_request` (and thus `prepare_and_acquire`) will encounter the strategy that has now been "paused" due to the `retry_after`.
        *   No changes are needed to the `HttpClient`'s own retry logic for *this* task.

**5. Testing Requirements:**
*   **Integration Test for `ExchangeAPI._request` (Conceptual - harder to unit test this specific interaction directly without a full API mock):**
    *   Mock an `ExchangeAPI` subclass (e.g., a simplified `MockBackpackAPI`).
    *   Mock its `_http_client.request()` to raise an `HttpRequestFailedError` that, when mapped by a mocked `ErrorMapper`, produces an `APIError` with `code=RATE_LIMITED` and `retry_after=5.0`.
    *   Mock the `rate_limit_strategy` attached to this `MockBackpackAPI` (e.g., an instance of `BackpackRateLimitStrategy` with a mocked `handle_exchange_retry_after` method).
    *   Call `await mock_api_instance._request(...)`.
    *   Assert that `mock_rate_limit_strategy.handle_exchange_retry_after` was called with `duration_seconds=5.0` and an appropriate `request_context`.
*   **Existing tests for `HttpClient`'s retry behavior should continue to pass.** This change influences how the *strategy* behaves for *subsequent* requests, not directly how `HttpClient` retries the *current* failed one.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
*   Ensure the `request_context_for_strategy` dictionary passed to `handle_exchange_retry_after` is well-defined and contains useful information for the strategy.
```

### **High-Level Context: Battle-Testing the API Client Layer with Comprehensive Integration Tests**

**Current State & Problem:**

Our project has successfully established a clean API client architecture with distinct `Service`, `ResponseHandler`, and `Mapper` layers. We've also created an environment-aware test configuration system.

However, our current integration tests are insufficient. They operate at a very low level, making direct `aiohttp` calls to exchange endpoints and only performing superficial checks on the raw JSON responses. As I've diagnosed:

*   **They don't test our code:** The tests completely bypass our `ExchangeAPI` classes, services, handlers, and mappers. They are effectively just "ping" tests for the live exchanges.
*   **They don't validate the full data pipeline:** A successful HTTP 200 response tells us nothing about whether our code can correctly parse, validate, and transform that response into the reliable, standardized **Internal Domain Models** (like `Order`, `Ticker`, `SpotBalance`) that the rest of our application depends on.
*   **They are brittle and hard to maintain:** Any change in our API client logic would not be caught by these tests.

**The Strategic Goal: From "Health Check" to "Full-Stack Validation"**

The objective of this phase is to elevate our integration tests from simple health checks to a robust suite that **validates the entire data processing pipeline for every single API endpoint**.

This means for any given public API method, like `api.get_balances()`, a corresponding integration test must:

1.  **Initiate the Call** through our client's public interface (`api.get_balances()`).
2.  **Record the Real Interaction:** Use `pytest-recording` to capture the *actual* underlying HTTP request our client makes and the *actual* response the exchange returns. This becomes our "ground truth" cassette.
3.  **Validate the End-to-End Result:** Assert that the final output of the `api.get_balances()` call is a perfectly formed dictionary of `SpotBalance` internal models, with all fields correctly typed (`Decimal`, `datetime`, etc.) and populated.

By doing this, a single integration test implicitly validates:
*   The **Service** correctly calls the request method.
*   The **RequestBuilder** (if used by the service) correctly constructs the request.
*   The **Authenticator** (for private endpoints later) correctly signs the request.
*   The **ResponseHandler** correctly validates the raw JSON against its Pydantic model.
*   The **Mapper** correctly transforms the raw model into the final internal model.

**Architectural Approach:**

*   **Leverage Existing Infrastructure:** We will use the recently created environment-aware fixtures (`active_hl_config`, `active_bp_config`, etc.) and the DI-enabled API fixtures (`hl_api_for_test_env`, `bp_api_for_test_env`).
*   **Systematic Coverage:** We will methodically add a cassette-based integration test for every public method on both `HyperliquidAPI` and `BackpackAPI`. We'll start with public/unauthenticated endpoints and then move to private/authenticated ones.
*   **Focus on the Internal Model:** The "finish line" for every test is the validation of the `cyberdelta.core.models` object. This is the contract our API layer provides to the rest of the application, and these tests will enforce that contract.

**Why This is the Right Next Step:**

Before we can trust our business logic (strategies, risk management), we must have absolute confidence that the data flowing into it is correct, consistent, and reliable. This comprehensive suite of integration tests provides that confidence. It creates a fast, deterministic, and self-contained validation layer for our entire API package, allowing us to build the core engine on a foundation we know is solid.


**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** API_INTEGRATION_P3_FULL_PUBLIC_REFACTOR
**Task:** Systematically Refactor All Public Endpoint Integration Tests for Both Hyperliquid and Backpack to Use Cassettes and Validate the Full Data Pipeline to Internal Models.

**1. Goal:**
Perform a complete refactoring of all public endpoint integration tests for both the `HyperliquidAPI` and `BackpackAPI` clients. The current tests are making direct, low-level HTTP calls and only verifying raw responses. This is incorrect. The new tests must:
    a. Call the public methods on our `HyperliquidAPI` and `BackpackAPI` instances (e.g., `api.get_ticker()`, `api.get_order_book()`).
    b. Use `pytest-recording` (`vcr`) to record and play back the underlying network interactions made by our API clients.
    c. Validate the *entire* internal data pipeline by asserting that the API methods return correctly populated **Internal Domain Models** (e.g., `core.models.Ticker`, `core.models.OrderBook`), not just a successful HTTP status or raw JSON.

**2. Why This Is Important (Overall Testing Strategy Context):**
This refactoring is the most critical step in battle-testing our API client layer. It moves us from simple "API health checks" to true integration tests that validate our software stack. By testing the full flow from the public API method call to the final, mapped Internal Domain Model, we gain high confidence in our request builders, response handlers, mappers, and service layer logic. This creates a fast, deterministic, and comprehensive regression suite that verifies our code works as expected against real-world (recorded) exchange data structures. It also eliminates hardcoded test logic by leveraging our new configuration fixtures.

**3. Files to Search and Refactor:**
*   `tests/integration/apis/hyperliquid/test_hl_market_data_public_endpoints.py`
*   `tests/integration/apis/backpack/test_bp_market_data_public_endpoints.py`

**4. Detailed Steps & Implementation Guidance (Apply to Both Exchanges):**

   **4.1. General Refactoring Pattern:**
      *   For every test in the target files, you will replace the direct `aiohttp.ClientSession` calls.
      *   Instead, each test function will receive the appropriate API client fixture: `hl_api_for_test_env` for Hyperliquid tests, and `bp_api_for_test_env` for Backpack tests.
      *   The test will then call the corresponding public method on the API instance provided by the fixture.
      *   The assertions will be rewritten to validate the `Internal Domain Model` that the public method returns.

   **4.2. Hyperliquid Public Endpoint Test Refactoring (`test_hl_market_data_public_endpoints.py`):**
      *   **Analyze existing tests:** Review tests like `test_hyperliquid_info_meta_and_asset_ctxs_public_endpoint`, `test_hyperliquid_info_l2_book_public_endpoint`, etc.
      *   **Refactor each test:**
          *   **Example for `l2Book`:** The test `test_hyperliquid_info_l2_book_public_endpoint` currently calls `session.post` with `{"type": "l2Book", "coin": "BTC"}`.
              *   **Refactor:** The test should now use the `hl_api_for_test_env` fixture. The action should be `order_book = await api.get_order_book("BTC")`.
              *   **New Asserts:**
                  *   `assert isinstance(order_book, OrderBook)`.
                  *   `assert order_book.symbol == "BTC"`.
                  *   `assert isinstance(order_book.bids[0][0], Decimal)`.
                  *   `assert order_book.bids[0][1] > Decimal(0)`.
          *   **Example for `metaAndAssetCtxs`:** The `get_ticker()` and `get_funding_rates()` methods on our `HyperliquidAPI` use this underlying endpoint. You should create tests for these public methods.
              *   **Refactor/Create `test_hl_get_ticker_integration`:**
                  *   Call `ticker = await api.get_ticker("ETH")`.
                  *   Assert `isinstance(ticker, Ticker)`, `ticker.symbol == "ETH"`, `ticker.price > 0`.
          *   **Apply this pattern** for all other public endpoints covered in the `info.py` SDK file (e.g., `recentTrades`, `candleSnapshot`, `fundingHistory`). Create new test functions for each corresponding `HyperliquidAPI` public method.

   **4.3. Backpack Public Endpoint Test Refactoring (`test_bp_market_data_public_endpoints.py`):**
      *   **Analyze existing tests:** Review tests like `test_backpack_public_markets_endpoint`, `test_backpack_public_ticker_endpoint`, etc.
      *   **Refactor each test:**
          *   **Example for `markets`:** The test `test_backpack_public_markets_endpoint` currently calls `/api/v1/markets`. The equivalent high-level method might be `get_all_tickers`.
              *   **Refactor:** Use the `bp_api_for_test_env` fixture. Call `all_tickers = await api.get_all_tickers()`.
              *   **New Asserts:** Assert the result is `dict[str, Ticker]`, check for a known symbol like "SOL_USDC", and validate the structure of the `Ticker` object.
          *   **Example for `depth`:** The test `test_backpack_public_depth_endpoint` currently calls `/api/v1/depth`.
              *   **Refactor:** Use `bp_api_for_test_env`. Call `order_book = await api.get_order_book("SOL_USDC")`.
              *   **New Asserts:** `assert isinstance(order_book, OrderBook)`, `assert order_book.symbol == "SOL_USDC"`, etc.
      *   **Apply this pattern** for all other Backpack public endpoints identified from their OpenAPI spec (e.g., `assets`, `klines`, `trades`).
      *   **Remove Low-Value Tests:** Tests for endpoints like `/ping`, `/time`, and `/status` do not test our mapping pipeline. They can be removed as they don't validate our application logic. Our focus is on endpoints that return structured data we must process into internal models.

   **4.4. Cassette Management:**
      *   When you refactor a test, you must **delete the old cassette file** associated with the old low-level `aiohttp` test.
      *   Run the refactored test with `pytest --vcr-record=once` to generate a **new cassette** that records the interaction made by our *actual API client*.
      *   Hyperliquid tests should be recorded against **testnet** (our fixture default).
      *   Backpack tests will be recorded against **mainnet**.

**5. Expected Outcome:**
    *   The `test_hl_market_data_public_endpoints.py` and `test_bp_market_data_public_endpoints.py` files are completely refactored.
    *   They no longer contain any direct `aiohttp` calls.
    *   All tests use the `hl_api_for_test_env` or `bp_api_for_test_env` fixtures.
    *   All tests assert against the final `Internal Domain Model` (e.g., `Ticker`, `OrderBook`, `Candle`).
    *   The `tests/cassettes/` directory is updated with new cassettes recorded from our API clients' interactions.

**6. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.
*   All tests must be marked with `@pytest.mark.integration` and `@pytest.mark.vcr`.


**Analysis of Existing Tests:**

*   I see `tests/integration/apis/hyperliquid/test_hl_market_data_public_endpoints.py`. This is a great start! It already covers `metaAndAssetCtxs`, `l2Book`, and `allMids`.
*   I also see `tests/integration/apis/backpack/test_bp_market_data_public_endpoints.py`, which covers `markets`.
*   We need to expand these files to be comprehensive and add tests for any missing endpoints.

**Source of Truth for Endpoints:**

1.  **Backpack:** We will primarily use `openapi_backpack.json` to identify all public (non-authenticated) GET endpoints.
2.  **Hyperliquid:** We will use a combination of:
    *   `openapi_hl.json` (our reverse-engineered spec).
    *   Web search results to confirm endpoint types if needed.
    *   https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api Hyperliquid documentation

---
**Prompt for Angel: Comprehensive Cassette-Based Integration Testing for All Public API Endpoints**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** API_INTEGRATION_P3_FULL_COVERAGE
**Task:** Implement Comprehensive Cassette-Based Integration Tests for All Public Endpoints of Hyperliquid and Backpack

**1. Goal:**
Achieve 100% integration test coverage for all public, unauthenticated API endpoints for both Hyperliquid and Backpack. Each test must use `pytest-recording` (`vcr`) to record and play back real API interactions. The tests must validate not only a successful HTTP response but also the basic structure of the returned data, ensuring our data ingestion pipeline is robust.

**2. Why This Is Important:**
This creates a fast, reliable, and deterministic test suite that acts as a contract between our API clients and the live exchanges. It will catch regressions, API changes, and validate our `ResponseHandler` logic for every public endpoint. This is a critical step before building business logic on top of the API layer.

**3. Files to Modify/Create:**

*   **Hyperliquid:** `tests/integration/apis/hyperliquid/test_hl_market_data_integration.py` (expand this file).
*   **Backpack:** `tests/integration/apis/backpack/test_bp_market_data_public_endpoints.py` (expand this file).
*   **Fixtures:** Use existing fixtures from `tests/integration/apis/*/conftest.py` (e.g., `active_hl_config`, `active_bp_config`). No new fixtures should be needed.

**4. Detailed Steps & Implementation Guidance - Part 1: Hyperliquid Public Endpoints**

   **4.1. Identify All Hyperliquid Public Endpoints:**
      *   Your primary source is the `info.py` file from the SDK. Identify all methods that make a POST request to `/info` with a different `type` in the payload and do not require authentication beyond a user address (which is public information).
      *   Examples to ensure are covered: `meta`, `metaAndAssetCtxs`, `allMids`, `l2Book`, `recentTrades`, `candleSnapshot`, `fundingHistory`, `tokenDetails`, `spotMeta`, `perpDexs`, etc.
      *   Also include user-context public endpoints that take a public address as an argument, like `openOrders`, `userFills`, `clearinghouseState`. Use a known public address for these recordings (e.g., `0x0000000000000000000000000000000000000000`).

   **4.2. Implement Tests in `test_hl_market_data_integration.py`:**
      *   For each identified public endpoint type:
          *   Create a new `async def` test function (e.g., `test_hyperliquid_info_funding_history_endpoint`).
          *   Decorate it with `@pytest.mark.integration` and `@pytest.mark.vcr`. You can use `@pytest.mark.parametrize` to group similar tests if desired.
          *   Use the `active_hl_config` fixture to get the correct testnet URL.
          *   Construct the correct POST payload (e.g., `{"type": "fundingHistory", "coin": "ETH", ...}`).
          *   Use `aiohttp.ClientSession` to make the POST request.
          *   **Assert Response Status:** `assert response.status == 200`.
          *   **Assert Data Structure:**
              *   Parse the JSON response: `data = await response.json()`.
              *   Assert the top-level type (e.g., `assert isinstance(data, list)` or `assert isinstance(data, dict)`).
              *   Assert the presence of key fields based on your knowledge of the API response (e.g., for `fundingHistory`, assert that items in the list are dicts and contain keys like `"coin"`, `"fundingRate"`, `"time"`).
              *   Assert that lists are not empty for endpoints that should return data (e.g., `assert len(data) > 0`).

   **4.3. Record Cassettes:**
      *   Run pytest for this file against the Hyperliquid **testnet**. The fixtures are set up to default to testnet.
      *   Command: `pytest tests/integration/apis/hyperliquid/test_hl_market_data_integration.py --vcr-record=once -s`
      *   Verify new YAML cassette files are created under `tests/cassettes/`.

---

**5. Detailed Steps & Implementation Guidance - Part 2: Backpack Public Endpoints**

   **5.1. Identify All Backpack Public Endpoints:**
      *   Your primary source is the `openapi_backpack.json` file. Systematically go through all `paths` and identify every **GET** request that does *not* have a `"security"` requirement defined.
      *   Examples to ensure are covered:
          *   `/api/v1/ping`
          *   `/api/v1/time`
          *   `/api/v1/status`
          *   `/api/v1/markets`
          *   `/api/v1/ticker` (needs `symbol` param)
          *   `/api/v1/tickers`
          *   `/api/v1/depth` (needs `symbol` param)
          *   `/api/v1/klines` (needs `symbol` and `interval` params)
          *   `/api/v1/trades` (needs `symbol` param)
          *   `/api/v1/assets`
          *   ...and any others found in the spec.

   **5.2. Implement Tests in `test_bp_market_data_public_endpoints.py`:**
      *   For each identified public endpoint:
          *   Create a new `async def` test function (e.g., `test_backpack_public_time_endpoint`).
          *   Decorate with `@pytest.mark.integration` and `@pytest.mark.vcr`.
          *   Use the `active_bp_config` fixture to get the mainnet URL.
          *   Use `aiohttp.ClientSession` to make the GET request, including any necessary query parameters (e.g., `params={"symbol": "SOL_USDC"}`).
          *   **Assert Response Status:** `assert response.status == 200`.
          *   **Assert Data Structure:**
              *   If JSON, parse and assert basic structure (e.g., is it a list? a dict? does it have expected keys?).
              *   If plain text (like `/api/v1/time`), assert that the response text is not empty.

   **5.3. Record Cassettes:**
      *   **CAUTION:** These tests will hit the live Backpack **mainnet**.
      *   Run pytest for this file: `pytest tests/integration/apis/backpack/test_bp_market_data_public_endpoints.py --vcr-record=once -s`.
      *   Verify new YAML cassette files are created.

**6. General Requirements for All New Tests:**
    *   **Use Fixtures:** Do not hardcode URLs. Use `active_hl_config.active_api_base_url` and `active_bp_config.active_api_base_url`.
    *   **Keep it Simple:** The goal is to verify the endpoint is reachable and returns data in the expected *shape*. We are not testing the full `Service -> Mapper -> Internal Model` pipeline here, just the raw HTTP interaction. Deep validation of the data content will happen in other tests that *use* these cassettes.
    *   **Clarity:** Name test functions clearly, e.g., `test_ENDPOINT_NAME_endpoint`.

**7. Project Rules Adherence:**
*   Static Analysis V3, Runtime Safety V3, No Silencing V4, Code Clarity.


This is **Phase 3: Implement Cassette-Based Integration Testing.**

**Overall Plan for Phase 3:**

1.  **Setup `pytest-recording`:** Install the library and set up basic VCR.py configuration, including cassette directory and sensitive data filtering.
2.  **Convert Key Public Endpoint Tests:** Start by converting existing integration tests for public, unauthenticated endpoints to use cassettes. This will establish the pattern.
3.  **Convert Key Authenticated Endpoint Tests:** Gradually convert integration tests for private, authenticated endpoints, paying close attention to request matching and filtering of sensitive authentication data.
4.  **Develop New Cassette-Based Tests for Untested Endpoints:** Systematically go through each public method in `HyperliquidAPI` and `BackpackAPI` that makes a network call and ensure it has at_least one integration test using a cassette that covers:
    *   A successful response.
    *   Common error responses (e.g., 404 Not Found, 400 Invalid Params, 429 Rate Limited).
5.  **Focus on "Raw -> Internal Model" Pipeline:** For tests covering data retrieval (tickers, order books, balances, positions, etc.), ensure assertions validate the structure and key values of the *final Internal Domain Model* returned by the API client method, thus testing the full chain (Service -> Handler -> Mapper).

**Breaking Down Phase 3 into Actionable Sub-Prompts for Angel:**

---
**Sub-Prompt 3.1: Initial `pytest-recording` Setup and Configuration**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** CASSETTE_P3A_SETUP
**Task:** Install and Configure `pytest-recording` with Basic VCR.py Settings

**1. Goal:**
1.  Add `pytest-recording` to the project's dependencies.
2.  Create an initial VCR.py configuration in `tests/conftest.py` (the root conftest) to define the cassette library directory, default record mode, basic request matchers, and essential filters for sensitive data that might appear in any request/response (even for public endpoints initially).

**2. Why This Is Important:**
This sets up the foundational infrastructure for cassette-based testing across the project, ensuring recordings are stored consistently and common sensitive data (like potential IP addresses or user agent details if we don't want them recorded) can be filtered globally.

**3. Files to Modify/Create:**
*   `pyproject.toml` (or `requirements.txt` if used)
*   `tests/conftest.py` (Create or update)

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Add Dependency:**
      *   Add `pytest-recording` to the appropriate section in `pyproject.toml` (e.g., `[tool.poetry.group.dev.dependencies]`) or your `requirements-dev.txt`.
      *   Run `poetry lock && poetry install` or `pip install -r requirements-dev.txt`.

   **4.2. Configure VCR.py in `tests/conftest.py`:**
      *   Import `vcr` at the top.
      *   Define a VCR configuration fixture or directly configure the default VCR instance. Using a fixture is often cleaner.
        ```python
        # In tests/conftest.py
        import pytest
        import vcr
        import os

        @pytest.fixture(scope='module') # Or 'session' if preferred for global VCR settings
        def vcr_config():
            return {
                "filter_headers": [
                    # Add any global headers to filter even for public endpoints,
                    # e.g., 'User-Agent' if it's too revealing or changes often.
                    # Specific auth headers will be added in later tasks/prompts.
                    ('User-Agent', 'CyberDeltaEngine-Test-Suite'), # Example placeholder
                ],
                "filter_query_parameters": [
                    # Add any global query params to filter (less common for public)
                ],
                # Default matchers: method and URI (scheme, host, port, path, query)
                # We might need to customize this later for authenticated requests.
                "match_on": ['method', 'scheme', 'host', 'port', 'path', 'query'],
                "cassette_library_dir": "tests/cassettes",
                # Default record mode. 'once' is safe: records if cassette missing, replays otherwise.
                # 'new_episodes' records new interactions if cassette exists but doesn't match.
                # Use 'none' to ensure no new recordings if cassettes should be complete.
                # For development, 'once' or 'new_episodes' is common.
                # For CI, 'none' is often preferred after initial recording.
                "record_mode": os.environ.get("VCR_RECORD_MODE", "once"), 
                "decode_compressed_response": True, # Useful for gzipped responses
            }

        # Optional: If you want to make VCR globally available with this config
        # This can be done by configuring the default VCR instance.
        # However, using the vcr fixture provided by pytest-recording and passing
        # vcr_config to it in tests is often more explicit.
        # For pytest-recording, it often picks up a vcr_config fixture automatically.
        # Refer to pytest-recording docs for the best way to apply module/session-wide config.
        # A common pattern is to define a vcr fixture that uses this config:
        
        # @pytest.fixture(scope='module')
        # def vcr(vcr, vcr_config): # vcr here is the one from pytest-recording
        #     # This way of configuring might depend on pytest-recording's exact API
        #     # The primary way is often just defining vcr_config.
        #     # Let's assume defining vcr_config is sufficient for pytest-recording to pick up.
        #     # If not, we may need to configure the default VCR instance more directly
        #     # or use the vcr fixture with custom_patches.
        #     # For now, just define vcr_config. pytest-recording should find it.
        #     return vcr 
        ```
      *   **Cassette Directory:** Ensure `tests/cassettes/` is created (or will be created by VCR.py). Add this directory to `.gitignore`.
      *   **Record Mode:** Set a sensible default record mode (e.g., `once`). This can be overridden via environment variable `VCR_RECORD_MODE` or pytest command-line options provided by `pytest-recording`.
      *   **Initial Filters:** Start with filtering common headers like `User-Agent` if you want to anonymize it or make it constant for tests. We will add more specific auth header filters later.

**5. Testing Requirements:**
*   After this setup, running pytest should not fail due to VCR.py configuration issues.
*   A simple test that makes an external HTTP call (even to a public site like `httpbin.org`) using `requests` or `aiohttp` and marked with the `vcr` fixture should generate a cassette in `tests/cassettes/`.

**6. Project Rules Adherence:**
*   Static Analysis V3, Code Clarity.
```

---
**Sub-Prompt 3.2: Convert a Simple Public Endpoint Integration Test to Use Cassettes**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** CASSETTE_P3B_PUBLIC_ENDPOINT_TEST
**Task:** Refactor a Simple Public Hyperliquid API Integration Test to Use `pytest-recording`

**1. Goal:**
Select one simple, public, unauthenticated Hyperliquid API integration test (e.g., fetching `meta` or `allMids` from `tests/integration/apis/hyperliquid/test_hl_api_integration.py` or a similar service test) and refactor it to use `pytest-recording` for request/response recording and playback.

**2. Why This Is Important:**
This serves as a proof-of-concept for using cassettes, establishes the pattern for other tests, and immediately improves the speed and reliability of the chosen test.

**3. File to Modify:**
*   The selected Hyperliquid integration test file (e.g., `tests/integration/apis/hyperliquid/test_hl_api_integration.py` or a relevant service integration test file).

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Select Test Case:**
      *   Choose a test that calls a public `HyperliquidAPI` method resulting in a POST to `/info` (e.g., `api.market_data_service.get_all_asset_contexts_raw()` which calls `/info` with `{"type": "metaAndAssetCtxs"}` or `api.market_data_service.get_all_mids()`).
      *   Ensure this test currently uses the `hl_api_with_di` fixture (or similar) that provides a `HyperliquidAPI` instance configured for testnet (via `active_hl_config`).

   **4.2. Apply `vcr` Fixture:**
      *   Add the `vcr` pytest fixture to the test function's parameters.
        ```python
        # Example:
        # import pytest # Already there if using pytest.mark.integration
        # pytestmark = pytest.mark.integration # If at module level

        # @pytest.mark.integration # Or at function level
        async def test_get_all_mids_integration(hl_api_with_di, vcr): # Add vcr fixture
            api = hl_api_with_di()
            # ... rest of the test making the API call ...
            # with vcr.use_cassette('hl_all_mids.yaml'): # Optional: explicit naming
            all_mids_data = await api.market_data_service.get_all_mids() 
            # ... assertions ...
        ```
      *   `pytest-recording` often names cassettes automatically based on the test function name. Explicit naming with `vcr.use_cassette()` is also an option for more control.

   **4.3. Record Cassette:**
      *   Ensure your local `config.yaml` points Hyperliquid to testnet and `is_mainnet_environment: false`.
      *   Delete any pre-existing cassette for this test if you want a fresh recording.
      *   Run `pytest path/to/your/test_file.py::test_function_name -s --vcr-record=once` (or `rewrite`).
      *   Verify a YAML cassette file is generated in `tests/cassettes/your_test_module_name/test_function_name.yaml`.
      *   Inspect the cassette: check it contains the request and response. Ensure no sensitive data is present (though for public endpoints, this is less of a concern yet).

   **4.4. Verify Playback:**
      *   Run the same test again: `pytest path/to/your/test_file.py::test_function_name -s`.
      *   It should now pass much faster and without making a real network call (VCR.py will log this).

**5. Testing Requirements for This Task:**
*   The refactored test must pass when run with `pytest-recording` in both record and playback modes.
*   The generated cassette file should be present and look reasonable.

**6. Project Rules Adherence:**
*   Static Analysis V3, Code Clarity.
```

---
**Sub-Prompt 3.3: Implement Robust Sensitive Data Filtering for Cassettes**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** CASSETTE_P3C_SENSITIVE_DATA_FILTERS
**Task:** Implement Comprehensive Sensitive Data Filtering for `pytest-recording` Cassettes

**1. Goal:**
Enhance the VCR.py configuration in `tests/conftest.py` to include robust filters for all known sensitive data, especially authentication headers and any sensitive information that might appear in request/response bodies for **authenticated** endpoints of both Hyperliquid and Backpack.

**2. Why This Is Important:**
Cassettes must **never** contain real API keys, private keys, signatures, or personally identifiable information. This is critical for security and for being able to commit cassettes to version control.

**3. File to Modify:**
*   `tests/conftest.py`

**4. Detailed Steps & Implementation Guidance:**

   **4.1. Review Authentication Mechanisms:**
      *   **Hyperliquid:** EIP-712 signatures. The request body for `/exchange` contains `action`, `nonce`, and `signature` (with `r`, `s`, `v`). The signature itself is sensitive. The `nonce` changes.
      *   **Backpack:** ED25519 signatures. Uses headers: `X-API-Key` (public key, less sensitive but good to filter for consistency), `X-Timestamp` (changes), `X-Window` (changes), `X-Signature` (highly sensitive).

   **4.2. Implement `before_record_request` and `before_record_response` Hooks in `tests/conftest.py`:**
      *   These hooks allow modification of requests and responses before they are saved to the cassette.
      *   Refer to VCR.py documentation for `add_filter`, `filter_headers`, `filter_query_parameters`, `filter_post_data_parameters` (for form data), and how to use `before_record_request/response` for more complex body filtering.

      ```python
      # In tests/conftest.py
      # import vcr # Already imported
      # import json # If parsing/modifying JSON bodies

      def filter_hl_signature(request_or_response):
          # For Hyperliquid /exchange POST requests, the signature is in the JSON body.
          if isinstance(request_or_response.body, bytes):
              try:
                  body_str = request_or_response.body.decode('utf-8')
                  data = json.loads(body_str)
                  if 'signature' in data and isinstance(data['signature'], dict):
                      data['signature']['r'] = "0xFILTERED_R"
                      data['signature']['s'] = "0xFILTERED_S"
                      data['signature']['v'] = 0 # Or some placeholder int
                  if 'nonce' in data: # Nonce changes, can make matching hard if not filtered
                      data['nonce'] = "FILTERED_NONCE" 
                  request_or_response.body = json.dumps(data).encode('utf-8')
              except (UnicodeDecodeError, json.JSONDecodeError):
                  pass # Not a JSON body we can parse and filter this way
          return request_or_response

      # More specific filter for Backpack headers
      def filter_backpack_auth_headers(headers_dict):
          headers_dict_copy = headers_dict.copy() # Operate on a copy
          if 'X-API-Key' in headers_dict_copy:
              headers_dict_copy['X-API-Key'] = 'FILTERED_BACKPACK_API_KEY'
          if 'X-Signature' in headers_dict_copy:
              headers_dict_copy['X-Signature'] = 'FILTERED_BACKPACK_SIGNATURE'
          if 'X-Timestamp' in headers_dict_copy:
              headers_dict_copy['X-Timestamp'] = 'FILTERED_TIMESTAMP'
          # X-Window is usually small and fixed, maybe less critical to filter, but can be added
          return headers_dict_copy

      @pytest.fixture(scope='module')
      def vcr_config():
          return {
              "filter_headers": [
                  ('User-Agent', 'CyberDeltaEngine-Test-Suite/1.0'),
                  # General Authorization header filter
                  ('Authorization', 'FILTERED_AUTHORIZATION_HEADER'), 
                  # Backpack specific headers filtering can also be done here if simple,
                  # or use before_record_request for more control.
                  ('X-API-Key', 'FILTERED_BACKPACK_API_KEY'),
                  ('X-Signature', 'FILTERED_BACKPACK_SIGNATURE'),
                  ('X-Timestamp', 'FILTERED_TIMESTAMP'),
              ],
              "filter_query_parameters": [
                  ('api_key', 'FILTERED_QUERY_API_KEY'),
                  ('signature', 'FILTERED_QUERY_SIGNATURE'),
                  ('timestamp', 'FILTERED_QUERY_TIMESTAMP'),
              ],
              "before_record_request": [
                  filter_hl_signature, 
                  # If Backpack headers need more complex logic than filter_headers provides
                  # lambda r: setattr(r, 'headers', filter_backpack_auth_headers(r.headers)) or r
              ],
              "before_record_response": [
                  # Add filters for sensitive data in response bodies if any.
                  # E.g., filter_user_account_ids_in_response_body,
              ],
              "match_on": [
                  'method', 'scheme', 'host', 'port', 'path', 'query',
                  # For POST requests, matching on body can be important, but
                  # needs to be done carefully if parts of the body (like nonce/signature)
                  # are filtered or change. VCR.py has body matchers.
                  # 'body' # Add this if needed, but be aware of dynamic content.
              ],
              "cassette_library_dir": "tests/cassettes",
              "record_mode": os.environ.get("VCR_RECORD_MODE", "once"),
              "decode_compressed_response": True,
          }
      ```
   *   **Hyperliquid Specifics:** Filter the `r`, `s`, `v` components of the `signature` and the `nonce` in the JSON body of `/exchange` requests.
   *   **Backpack Specifics:** Filter `X-API-Key`, `X-Signature`, `X-Timestamp` headers.
   *   **General:** Consider filtering any session cookies or other potentially sensitive headers/body parts.

**5. Testing Requirements for This Task:**
*   Create a simple test that makes an *authenticated* call (e.g., Backpack get balances, or Hyperliquid get open orders).
*   Record the cassette.
*   **Manually inspect the generated YAML cassette file** to ensure that all sensitive data (API keys, signatures, nonces, timestamps in auth headers) has been replaced with placeholder values (e.g., "FILTERED_SIGNATURE").
*   Ensure the test passes when run in playback mode using the filtered cassette.

**6. Project Rules Adherence:**
*   Static Analysis V3, Code Clarity. Security is paramount here.
```

---
**Sub-Prompt 3.4 (Iterative): Convert Remaining Integration Tests to Use Cassettes**
```markdown
**Project:** CyberDeltaEngine
**AI Mentor:** Toly
**Task ID:** CASSETTE_P3D_CONVERT_REMAINING_TESTS
**Task:** Incrementally Convert All Hyperliquid and Backpack Integration Tests to Use `pytest-recording`

**1. Goal:**
Systematically go through all integration tests in `tests/integration/apis/hyperliquid/` and `tests/integration/apis/backpack/` (including their subdirectories) and refactor them to use `pytest-recording` with the configured sensitive data filters.

**2. Why This Is Important:**
This will make our entire suite of API integration tests fast, reliable, deterministic, and capable of running offline, significantly improving the development and CI/CD cycle.

**3. Files to Modify:**
*   All `test_*.py` files within `tests/integration/apis/hyperliquid/` and `tests/integration/apis/backpack/`.

**4. Detailed Steps & Implementation Guidance (Iterative Approach):**

   *   **Prioritize Public Endpoints First:** Start with tests for public, unauthenticated endpoints in both Hyperliquid and Backpack. These are simpler as they don't involve complex auth filtering.
   *   **Then Tackle Authenticated Endpoints:**
      *   For each test:
          1.  Add the `vcr` fixture to the test function.
          2.  Run the test once with an appropriate record mode (e.g., `pytest --vcr-record=new_episodes ...`) to generate the cassette. Use testnet for Hyperliquid and mainnet (with care, or specific test accounts if possible) for Backpack.
          3.  **Critically inspect the generated cassette YAML file:** Ensure all sensitive authentication data (keys, secrets, signatures, nonces, timestamps used in auth) and any PII in request/response bodies are correctly filtered to placeholder values by the filters defined in `tests/conftest.py`. If not, refine the filters in `conftest.py` and re-record.
          4.  Run the test again to ensure it plays back correctly from the filtered cassette.
          5.  Commit both the test file changes and the new/updated cassette file.
   *   **Cover Success and Error Cases:** For each logical API endpoint, try to create/convert tests that record:
      *   A successful response (e.g., 200 OK with expected data).
      *   Common error responses (e.g., 400 Bad Request, 401 Unauthorized, 404 Not Found, 429 Too Many Requests, 500 Server Error). This will test your `ErrorMapper` logic against real (recorded) error structures.
   *   **"Raw -> Internal Model" Pipeline:** For data-retrieval endpoints, ensure your tests assert not just that the call succeeds, but that the API client method correctly returns the expected Internal Domain Model, properly mapped from the raw cassette data.

**5. Testing Requirements for This Task:**
*   All converted integration tests must pass using recorded cassettes.
*   Cassette files must be committed and must not contain any sensitive data.
*   The overall test suite should run significantly faster when playing back from cassettes.

**6. Project Rules Adherence:**
*   Static Analysis V3, Code Clarity. Diligence in inspecting cassettes for filtered data is key.

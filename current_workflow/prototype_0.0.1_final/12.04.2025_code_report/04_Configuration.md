# CyberDeltaEngine: Code Review Report (v0.0.1) - Configuration and Secrets

This section details the approach to managing application configuration (`config.yaml`) and sensitive credentials (`secrets.yaml` or environment variables).

## 1. Configuration Management (`cyberdelta/utils/config.py`, `config.yaml`)

*   **Responsibility:** Load, validate, and provide access to application settings defined primarily in `config.yaml`, allowing overrides via environment variables.
*   **Utility Class (`cyberdelta/utils/config.py:Config`)**:
    *   **Loading:** Initializes by loading one or more YAML files (e.g., `config.yaml`, potentially environment-specific overlays like `config.dev.yaml`).
    *   **Environment Overrides:** Supports overriding YAML values with environment variables. A common pattern is `CONFIG__SECTION__SUBSECTION__PARAM=value` mapping to `config['section']['subsection']['param']`.
    *   **Access:** Provides dictionary-like access with added dot-notation support (e.g., `config.get('risk_manager.max_total_exposure_usd', default=...)`).
    *   **Merging:** Can merge multiple configuration sources.
    *   **Type Handling:** Loads values using standard YAML types (string, int, float, bool, list, dict). Does **not** perform automatic type validation or conversion beyond basic YAML parsing (e.g., does not automatically convert numeric strings to `Decimal`).

*   **Code Snippet (`Config` class usage - Conceptual):**
    ```python
    # cyberdelta/utils/config.py (Illustrative parts)
    import yaml
    import os
    from box import Box # Example using python-box for dot notation access

    class Config:
        def __init__(self, *config_files: str):
            self._data = Box() # Using Box for attribute-style access
            for file_path in config_files:
                if os.path.exists(file_path):
                    try:
                        with open(file_path, 'r') as f:
                            yaml_data = yaml.safe_load(f)
                            if yaml_data:
                                # Merge dictionaries deeply
                                self._data.merge_update(yaml_data)
                    except Exception as e:
                         print(f"Warning: Could not load config file {file_path}: {e}") # Use logger ideally
            self._load_env_vars()

        def _load_env_vars(self, prefix="CONFIG__"):
            # Simplified: Iterates os.environ, finds keys with prefix,
            # parses section__subsection__key, sets value in self._data
            for key, value in os.environ.items():
                if key.startswith(prefix):
                    parts = key[len(prefix):].lower().split('__')
                    target = self._data
                    for part in parts[:-1]:
                        target = target.setdefault(part, Box())
                    # Attempt basic type casting based on value format
                    try:
                         if value.lower() in ['true', 'false']:
                             target[parts[-1]] = value.lower() == 'true'
                         elif '.' in value:
                             target[parts[-1]] = float(value) # Or Decimal if needed
                         else:
                              target[parts[-1]] = int(value)
                    except ValueError:
                         target[parts[-1]] = value # Store as string if cast fails

        def get(self, key: str, default: Any = None) -> Any:
            # Box handles dot notation access implicitly
            try:
                value = self._data.get(key)
                return value if value is not None else default
            except AttributeError:
                 # Handle case where intermediate key doesn't exist
                 # Using Box directly often avoids this need vs manual traversal
                 keys = key.split('.')
                 val = self._data
                 for k in keys:
                     if isinstance(val, dict) and k in val:
                         val = val[k]
                     else:
                         return default
                 return val

        # Allow direct attribute access e.g., config.risk_manager.max_drawdown_pct
        def __getattr__(self, name):
            return getattr(self._data, name)

        def __getitem__(self, key):
             return self._data[key]

    # cyberdelta/main.py (Usage)
    # config = Config('config.yaml', 'config.prod.yaml') # Load base and overlay
    # risk_limit = config.get('risk_manager.max_total_exposure_usd', '10000')
    # # Or using attribute access if using Box or similar
    # risk_limit_attr = config.risk_manager.max_total_exposure_usd
    ```

*   **Configuration File (`config.yaml`) Structure & Examples:**
    *   Organized into sections matching major components or concerns.
    *   **Crucially needs inline comments** explaining each parameter, its expected type, units, and purpose.
    *   **Example Structure:**
        ```yaml
        # config.yaml - Main Application Configuration

        # General application settings
        application:
          log_level: "INFO" # DEBUG, INFO, WARNING, ERROR
          state_file_path: "./cyberdelta_state.json" # Path for saving application state

        # Settings for the DataHandler component
        data_handler:
          websocket_reconnect_delay_seconds: 5.0 # Initial delay before WS reconnect
          max_reconnect_attempts: 10 # Max attempts before giving up on WS
          # Data staleness thresholds in seconds
          staleness_thresholds:
            ticker: 10.0
            orderbook: 5.0
            funding_rate: 600.0 # Allow older funding rate? Depends on strategy

        # Exchange-specific settings
        exchanges:
          hyperliquid:
            enabled: true
            base_rest_url: "https://api.hyperliquid.xyz"
            base_ws_url: "wss://api.hyperliquid.xyz/ws"
            # Rate limits (requests per second, or tokens per interval)
            rate_limits:
              info: { limit: 10, period: 1 } # Example: 10 req/sec for /info
              exchange: { limit: 5, period: 1 } # Example: 5 req/sec for /exchange
            # Min order sizes, tick sizes, etc. (can be fetched dynamically too)
            min_order_sizes:
              BTC-PERP: "0.001"
            tick_sizes:
              BTC-PERP: "0.1"

          backpack:
            enabled: true
            base_rest_url: "https://api.backpack.exchange"
            base_ws_url: "wss://ws.backpack.exchange"
            rate_limits:
              default: { limit: 20, period: 1 } # Example: 20 req/sec global default
            # Use API key/secret names matching secrets file/env vars
            api_key_name: "BACKPACK_API_KEY"
            secret_key_name: "BACKPACK_SECRET_KEY"
            # Optional: Subaccount name if needed
            # subaccount: "my_trading_subaccount"

        # Strategy configurations (keyed by strategy name)
        strategies:
          funding_rate_arbitrage: # Matches strategy class name or identifier
            enabled: true
            cycle_interval_seconds: 5.0 # How often to check for opportunities
            target_symbol: "BTC-PERP" # Internal canonical symbol
            perp_exchange: "hyperliquid"
            other_exchange: "backpack"
            min_funding_differential_pct: "0.0001" # 0.01% - Use string for Decimal conversion later
            min_profit_threshold_usd: "0.50" # Min est. USD profit per trade
            # Cost parameters (use strings for Decimal)
            fee_rate_perp: "0.0005" # 0.05%
            fee_rate_other: "0.0010" # 0.10%
            slippage_calculation_depth_usd: "1000"
            opportunity_cooldown_seconds: 30

        # Risk Manager configuration
        risk_manager:
          max_total_exposure_usd: "50000"
          max_position_size_usd: "10000"
          max_trade_size_usd: "5000"
          max_drawdown_pct_global: "0.10" # 10%
          sizing_method: "kelly"
          kelly_fraction: "0.05"

        # Execution Handler configuration
        execution_handler:
          default_order_type: "LIMIT" # MARKET or LIMIT
          limit_order_slippage_bps: 5 # Basis points away from reference price for limit orders
          max_retries: 3
          retry_delay_seconds: 1.0
          compensation_mode: "NONE" # Or MARKET/LIMIT if partial fill compensation needed

        # Safety systems configuration
        validation:
          circuit_breaker:
            enabled: true
            # Global triggers
            max_global_api_error_rate: { count: 10, interval_seconds: 60 }
            max_global_ws_disconnect_rate: { count: 5, interval_seconds: 60 }
            # Per-exchange triggers
            exchanges:
              hyperliquid:
                max_api_error_rate: { count: 5, interval_seconds: 60 }
                max_ws_disconnect_rate: { count: 3, interval_seconds: 60 }
              backpack:
                # ... specific limits for backpack ...
          position_reconciliation:
            enabled: true
            check_interval_seconds: 300 # Check every 5 minutes
            # Thresholds for triggering alerts (e.g., percentage difference)
            discrepancy_threshold_pct: "0.01" # 1% difference in position size

        # Symbol mapping utility configuration
        symbol_mapper:
          # Define internal canonical symbols and their exchange-specific representations
          mappings:
            BTC-PERP:
              hyperliquid: "BTC-PERP"
              backpack: "PERP-BTC" # Example difference
            SOL-USD:
              hyperliquid: null # Not available
              backpack: "SOL-USD"
          # Default quote currency if needed for conversions
          default_quote_currency: "USD"

        ```

*   **Observations & Strengths:**
    *   Centralizes application settings in a readable format (YAML).
    *   `Config` class provides a consistent loading mechanism with environment override capability.
*   **Concerns & Areas for Improvement:**
    *   **Type Safety & Validation:** Major weakness. Relies on manual type conversions (`Decimal()`, `float()`, `int()`) and key checking throughout the code. Typos in keys or incorrect value formats in `config.yaml` are not caught at load time.
    *   **Implicit Defaults:** Defaults specified in `.get(key, default)` calls are scattered across the codebase, making it hard to see the effective default configuration.
    *   **Structure Maintainability:** Deeply nested structures can become hard to manage.
*   **Recommendations:**
    *   **Adopt Pydantic (HIGHLY RECOMMENDED):**
        *   Define Pydantic models mirroring the `config.yaml` structure.
        *   Use Pydantic's features for type validation, required fields, default values, and even custom validators.
        *   Load YAML into Pydantic models during startup. This provides immediate validation and a typed configuration object for use throughout the application, significantly reducing runtime errors and improving clarity.
    *   **Document `config.yaml` Thoroughly:** Add inline comments explaining *every* parameter, its purpose, expected type, and units.
    *   **Review Structure:** Periodically review the YAML structure for clarity and consistency. Consolidate related parameters where logical.

## 2. Secrets Management (`secrets.yaml`, `.env`, Environment Variables)

*   **Responsibility:** Securely manage sensitive credentials like API keys and private keys, keeping them separate from the main configuration and version control.
*   **Common Approaches:**
    1.  **Environment Variables:** Secrets loaded directly from the environment (e.g., `os.environ.get("HYPERLIQUID_API_KEY")`). Standard practice, especially for containerized deployments.
    2.  **`.env` File:** Uses a `.env` file (added to `.gitignore`) loaded via `python-dotenv` library during development. Variables are then accessed via `os.environ`.
    3.  **`secrets.yaml` File:** A dedicated YAML file (added to `.gitignore`) for secrets, loaded separately or merged carefully by the `Config` class or a dedicated `SecretsManager`.
*   **Current Implementation:** Appears API clients receive secrets via a `secrets` dictionary or directly access config for key *names* (e.g., `BACKPACK_API_KEY`). The loading likely happens in `main.py` using environment variables or potentially a simple secrets loader.

*   **Example (`.env` format):**
    ```dotenv
    # .env - Local Development Secrets (Add to .gitignore!)
    
    # Hyperliquid (Assuming private key for EIP-712)
    HYPERLIQUID_WALLET_ADDRESS="0x..."
    HYPERLIQUID_PRIVATE_KEY="0x..."
    
    # Backpack (Standard API Key/Secret)
    BACKPACK_API_KEY="your_backpack_api_key_here"
    BACKPACK_SECRET_KEY="your_backpack_secret_key_here"
    
    # Optional: Logging API keys, Database credentials, etc.
    # SENTRY_DSN="..."
    ```
*   **Example (`secrets.yaml` format - if used):**
    ```yaml
    # secrets.yaml - Local Development Secrets (Add to .gitignore!)
    
    api_keys:
      hyperliquid:
        wallet_address: "0x..."
        private_key: "0x..." # Be extremely careful with private keys
      backpack:
        api_key: "your_backpack_api_key_here"
        secret_key: "your_backpack_secret_key_here"
    
    # other_secrets:
    #   database_password: "..."
    ```

*   **Code Snippet (Loading Secrets in `main.py` - Conceptual):**
    ```python
    # cyberdelta/main.py (Conceptual Loading)
    import os
    from dotenv import load_dotenv # If using .env

    def load_secrets():
        # Option 1: Using .env for local dev
        load_dotenv() # Load variables from .env into os.environ

        secrets = {
            "hyperliquid": {
                "address": os.environ.get("HYPERLIQUID_WALLET_ADDRESS"),
                "private_key": os.environ.get("HYPERLIQUID_PRIVATE_KEY"),
            },
            "backpack": {
                "api_key": os.environ.get("BACKPACK_API_KEY"),
                "secret_key": os.environ.get("BACKPACK_SECRET_KEY"),
            }
        }
        # Basic validation
        if not secrets["hyperliquid"]["private_key"]:
             print("Error: HYPERLIQUID_PRIVATE_KEY not found in environment.")
             # Handle error appropriately - exit?
        if not secrets["backpack"]["api_key"] or not secrets["backpack"]["secret_key"]:
             print("Error: BACKPACK_API_KEY or BACKPACK_SECRET_KEY not found.")
             # Handle error appropriately

        return secrets

    # --- In main function ---
    # config = Config('config.yaml')
    # secrets = load_secrets()
    # hl_client = HyperliquidAPI(config, secrets=secrets["hyperliquid"])
    # bp_client = BackpackAPI(config, secrets=secrets["backpack"])
    ```

*   **Observations & Strengths:**
    *   Separates sensitive data from general configuration.
    *   Follows standard practices (env vars, `.env`).
*   **Concerns & Areas for Improvement:**
    *   **Validation:** Loading secrets directly from `os.environ` lacks validation at startup; missing secrets might only cause errors later when an API client tries to use them.
    *   **Clarity:** The exact method of loading and passing secrets to components should be clear and consistent.
*   **Recommendations:**
    *   **Centralize Loading & Validation:** Implement a simple `load_secrets()` function (as in the snippet) or a dedicated `SecretsManager` class called early in `main.py`. This function should attempt to load all required secrets and perform basic validation (check for presence), failing fast if critical secrets are missing.
    *   **Confirm `.gitignore`:** Re-verify that `.env`, `secrets.yaml`, or any other files containing secrets are definitely in `.gitignore`.
    *   **Production:** For production deployments, rely on secure environment variable injection mechanisms provided by the deployment platform (e.g., Kubernetes Secrets, Docker secrets, cloud provider secret managers) rather than committing `.env` files.

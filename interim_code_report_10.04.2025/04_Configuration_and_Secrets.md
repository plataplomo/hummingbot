# Code Report: CyberDeltaEngine - Configuration and Secrets Management

## 1. Overview

Configuration and secrets management is crucial for security and flexibility. The system uses separate managers for general configuration (`ConfigManager`) and sensitive API keys/secrets (`SecretsManager`).

## 2. Configuration Manager (`cyberdelta/config/config_manager.py`)

**Purpose**: Handles loading, validation, and access to general application configuration from YAML files.

**Key Features**:
- Loads configuration from specified file paths or default locations.
- Supports environment variable overrides for config file paths (`CYBERDELTA_CONFIG_PATH`).
- Provides dot notation access to nested configuration values (e.g., `config.get("exchanges.hyperliquid.enabled")`).
- Handles default values gracefully.
- Includes basic validation for the presence of critical sections.
- Centralized configuration object (`config`) accessible throughout the application.

**Code Snippet (`ConfigManager.load`)**:
```python
    def load(self) -> None:
        """Load configuration from the specified file path."""
        try:
            with open(self.config_path, 'r') as f:
                self._config_data = yaml.safe_load(f)
            if not isinstance(self._config_data, dict):
                 logger.error(f"Configuration file {self.config_path} is not a valid dictionary.")
                 self._config_data = {}
                 return # Keep empty config if invalid

            logger.info(f"Loaded configuration from {self.config_path}")
            self._validate_config()

        except FileNotFoundError:
            logger.warning(f"Configuration file not found at {self.config_path}. Using defaults.")
            self._config_data = {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file {self.config_path}: {e}")
            self._config_data = {}
        except Exception as e:
            logger.error(f"Unexpected error loading configuration: {e}")
            self._config_data = {}
```

**Example `config.yaml` Structure**:
```yaml
general:
  log_level: INFO
  state_file: "state.json"
  state_backup_directory: "state_backups"
  state_backup_count: 5
  state_save_interval: 60 # seconds
  safe_mode: true # If true, disables actual trade execution

exchanges:
  hyperliquid:
    enabled: true
    rest_endpoint: "https://api.hyperliquid.xyz"
    ws_endpoint: "wss://api.hyperliquid.xyz/ws"
    rate_limits: # Example structure
      default: { rate: 10, bucket: 20 } # requests/sec
      orderPlacement: { rate: 5, bucket: 10 }
    symbols: ["BTC", "ETH"]
    websocket:
        ping_interval: 30
        reconnect_delay: 5
        max_reconnect_delay: 60
  backpack:
    enabled: true
    rest_endpoint: "https://api.backpack.exchange"
    ws_endpoint: "wss://ws.backpack.exchange"
    recv_window: 5000 # For signature generation
    symbols: ["BTC_USDC", "ETH_USDC"]
    # ... other backpack specific settings

strategies:
  hl_perp_bp_spot:
    enabled: true
    symbols:
      hl_symbol: "BTC"
      bp_symbol: "BTC_USDC"
    params:
      min_funding_differential: 0.0001 # 0.01%
      min_profit_threshold: 1.0 # USD
      min_spread: 0.0002 # Minimum price spread
      check_interval: 600 # seconds
  hl_perp_bp_perp:
    enabled: false # Example disabled strategy
    symbols:
      hl_symbol: "ETH"
      bp_symbol: "ETH-PERP"
    params:
      min_funding_diff: 0.0002
      # ... other params

risk_manager:
  max_total_exposure_usd: 10000
  max_position_size_usd: 5000
  min_position_size_usd: 100
  kelly_fraction: 0.1 # Example Kelly sizing parameter
  # ... other risk parameters

validation:
  circuit_breaker:
    enabled: true
    # ... specific breaker configurations ...
  position_reconciliation:
    threshold: 0.01 # 1% difference threshold
    check_interval: 3600
    auto_correct: false
  funding_rate_validation:
    enabled: true
    # ... validation parameters ...

# ... other sections like monitoring, notifications etc.
```

## 3. Secrets Manager (`cyberdelta/config/secrets_manager.py`)

**Purpose**: Handles secure loading and access to sensitive information like API keys and secrets, keeping them separate from the main codebase and configuration file.

**Key Features**:
- Loads secrets from a dedicated YAML file (`secrets.yaml` by default).
- **Security**: Secrets file is intended to be stored *outside* the Git repository.
- Supports loading secrets path via environment variable (`CYBERDELTA_SECRETS_PATH`).
- Provides fallback locations (e.g., `~/.cyberdelta/secrets.yaml`).
- Provides simple dictionary-like access (`secrets.get("BACKPACK_API_KEY")`).
- Centralized secrets object (`secrets`) accessible where needed.

**Code Snippet (`SecretsManager._find_secrets_file`)**:
```python
    def _find_secrets_file(self) -> Optional[str]:
        """Find the secrets file path based on environment variable or default locations."""
        # 1. Check environment variable
        env_path = os.environ.get("CYBERDELTA_SECRETS_PATH")
        if env_path and os.path.exists(env_path):
            logger.info(f"Using secrets file from CYBERDELTA_SECRETS_PATH: {env_path}")
            return env_path

        # 2. Check default location in user's home directory
        home_path = os.path.expanduser("~/.cyberdelta/secrets.yaml")
        if os.path.exists(home_path):
            logger.info(f"Using secrets file from default location: {home_path}")
            return home_path

        # 3. Check for secrets.yaml in the project root (development only - discourage this)
        #    This might require adjusting the base path depending on where the code runs.
        #    For simplicity, let's assume a specific project structure or use relative paths carefully.
        dev_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'secrets.yaml'))
        if os.path.exists(dev_path):
             logger.warning(f"Using secrets file from project root: {dev_path}. Consider moving it outside the source tree.")
             return dev_path

        logger.error("Secrets file not found. Set CYBERDELTA_SECRETS_PATH or place secrets.yaml in ~/.cyberdelta/")
        return None
```

**Example `secrets.yaml` Structure**:
```yaml
# WARNING: Store this file securely and outside your Git repository.
# Example structure - Replace with your actual keys

HYPERLIQUID_API_KEY: "your_hyperliquid_api_key_here"
HYPERLIQUID_API_SECRET: "your_hyperliquid_api_secret_here"
# Or potentially wallet private key for EIP-712 signing if applicable
# HYPERLIQUID_WALLET_PRIVATE_KEY: "0x..."

BACKPACK_API_KEY: "your_backpack_api_key_base64"
BACKPACK_API_SECRET: "your_backpack_api_secret_base64"

# Other secrets (e.g., notification service keys)
TELEGRAM_BOT_TOKEN: "your_telegram_token"
```

## 4. Security Considerations

- `secrets.yaml` **must not** be committed to version control. The path is included in `.gitignore`.
- Access to secrets is restricted; only components that require them (e.g., `APIClient` subclasses) should access the `secrets` object.
- Consider using environment variables or a dedicated secrets management service (like HashiCorp Vault) for production deployments instead of a file.

This separation ensures that sensitive credentials are not accidentally exposed in the codebase or general configuration files. 
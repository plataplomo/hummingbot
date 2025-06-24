# Test Configuration System

This document explains how configurations are loaded into tests in the CyberDeltaEngine testing environment. The system has been completely transformed from hardcoded values to a centralized, file-based configuration system.

## Configuration Flow Architecture

```
tests/config/test_config.yaml     →  ConfigManager    →  test_app_settings (fixture)
                                                        ↓
tests/config/test_secrets.yaml    →  SecretsManager   →  test_secrets_config (fixture)
                                                        ↓
                                  Environment Variables → hl_test_environment (fixture)
                                                        ↓
                                    Processed through → active_hl_config, active_bp_config
                                                        ↓
                                       Used by tests → hl_api_for_test_env, bp_api_for_test_env
```

## Core Configuration Loading

### 1. Main Configuration Fixtures (tests/conftest.py)

The main configuration loading happens in the root conftest.py:

```python
@pytest.fixture(scope="session")
def test_app_settings() -> AppSettings:
    """Load AppSettings from tests/config/test_config.yaml using ConfigManager."""
    test_config_file_path = Path(__file__).parent / "config" / "test_config.yaml"

    if not test_config_file_path.exists():
        pytest.skip(f"Test config file not found: {test_config_file_path}")

    try:
        return ConfigManager.load_from_file(test_config_file_path)
    except Exception as e:
        pytest.fail(f"Failed to load test AppSettings from {test_config_file_path}: {e}")

@pytest.fixture(scope="session")
def test_secrets_config() -> SecretsConfig:
    """Load SecretsConfig from tests/config/test_secrets.yaml using SecretsManager."""
    test_secrets_file_path = Path(__file__).parent / "config" / "test_secrets.yaml"

    if not test_secrets_file_path.exists():
        pytest.skip(f"Test secrets file not found: {test_secrets_file_path}")

    try:
        return SecretsManager.load_from_file(test_secrets_file_path)
    except Exception as e:
        pytest.fail(f"Failed to load test SecretsConfig from {test_secrets_file_path}: {e}")
```

### 2. Environment-Aware Configuration

The system respects environment variables for flexible testing:

```python
@pytest.fixture(scope="session")
def hl_test_environment() -> str:
    """Determine Hyperliquid test environment (mainnet/testnet).

    Defaults to 'testnet' but can be overridden with CYBERDELTA_TEST_ENV_HL environment variable.
    """
    return os.environ.get("CYBERDELTA_TEST_ENV_HL", "testnet")

@pytest.fixture(scope="session")
def active_hl_config(hl_test_environment: str) -> ExchangeSpecificConfig:
    """Environment-aware ExchangeSpecificConfig fixture for Hyperliquid.

    Configures the exchange for mainnet or testnet based on hl_test_environment.
    """
    is_mainnet_env_flag = hl_test_environment == "mainnet"

    return ExchangeSpecificConfig.model_validate({
        "exchange_name": ExchangeName.HYPERLIQUID,
        "api_base_url_mainnet": "https://api.hyperliquid.xyz",
        "ws_url_mainnet": "wss://api.hyperliquid.xyz/ws",
        "api_base_url_testnet": "https://api.hyperliquid-testnet.xyz",
        "ws_url_testnet": "wss://api.hyperliquid-testnet.xyz/ws",
        "is_mainnet_environment": is_mainnet_env_flag,
        # ... other config fields
    })
```

## Exchange-Specific Configuration Fixtures

### Hyperliquid Configuration

```python
@pytest.fixture(scope="session")
def active_hl_config(test_app_settings: AppSettings, hl_test_environment: str) -> ExchangeSpecificConfig:
    """Provide ExchangeSpecificConfig for Hyperliquid from test configuration."""
    hl_config_from_file = test_app_settings.exchanges["hyperliquid"]
    return hl_config_from_file.model_copy(
        update={"is_mainnet_environment": hl_test_environment == "mainnet"}
    )

@pytest.fixture(scope="session")
def active_hl_secrets(test_secrets_config: SecretsConfig) -> PrivateKeyAuthSecrets:
    """Provide PrivateKeyAuthSecrets for Hyperliquid from test secrets."""
    secrets = test_secrets_config.exchanges["hyperliquid"]
    if not isinstance(secrets, PrivateKeyAuthSecrets):
        pytest.fail("Hyperliquid secrets in test_secrets.yaml are not PrivateKeyAuthSecrets type.")
    return secrets
```

### Backpack Configuration

```python
@pytest.fixture(scope="session")
def active_bp_config(test_app_settings: AppSettings) -> ExchangeSpecificConfig:
    """Provide ExchangeSpecificConfig for Backpack from test configuration."""
    return test_app_settings.exchanges["backpack"]

@pytest.fixture(scope="session")
def active_bp_secrets(test_secrets_config: SecretsConfig) -> ApiKeyAuthSecrets:
    """Provide ApiKeyAuthSecrets for Backpack from test secrets."""
    secrets = test_secrets_config.exchanges["backpack"]
    if not isinstance(secrets, ApiKeyAuthSecrets):
        pytest.fail("Backpack secrets in test_secrets.yaml are not ApiKeyAuthSecrets type.")
    return secrets
```

## API Client Fixtures with Dependency Injection

Tests receive fully configured API clients through dependency injection:

### Integration Test API Fixtures

```python
@pytest.fixture
def hl_api_for_test_env(
    active_hl_config: ExchangeSpecificConfig,
    active_hl_secrets: PrivateKeyAuthSecrets,
) -> HyperliquidAPI:
    """Create HyperliquidAPI instance for integration tests.

    Uses configuration from test_config.yaml and test_secrets.yaml.
    For cassette recording/playback, this uses real components.
    """
    return HyperliquidAPI(
        exchange_config=active_hl_config,
        exchange_secrets=active_hl_secrets,
    )

@pytest.fixture
def bp_api_for_test_env(
    active_bp_config: ExchangeSpecificConfig,
    active_bp_secrets: ApiKeyAuthSecrets,
) -> BackpackAPI:
    """Create BackpackAPI instance for integration tests."""
    return BackpackAPI(
        exchange_config=active_bp_config,
        exchange_secrets=active_bp_secrets,
    )
```

### Unit Test API Fixtures (with Dependency Injection)

```python
@pytest.fixture
def hl_api_with_di(
    active_hl_config: ExchangeSpecificConfig,
    active_hl_secrets: PrivateKeyAuthSecrets,
    # ... mock dependencies
) -> Callable[..., HyperliquidAPI]:
    """Create HyperliquidAPI instances with all dependencies injected for testing."""
    def _create_api(
        config: ExchangeSpecificConfig | None = None,
        secrets: PrivateKeyAuthSecrets | None = None,
        **overrides: MagicMock,
    ) -> HyperliquidAPI:
        # Use active fixtures as defaults
        if config is None:
            config = active_hl_config
        if secrets is None:
            secrets = active_hl_secrets

        return HyperliquidAPI(
            exchange_config=config,
            exchange_secrets=secrets,
            # ... injected dependencies
        )
    return _create_api
```

## How Tests Use the Configuration

### Before Refactoring (Hardcoded)

```python
def test_old_way():
    # ❌ HARDCODED - Hard to maintain, brittle
    config = create_test_exchange_config(
        api_base_url="https://api.hyperliquid-testnet.xyz",  # HARDCODED!
        ws_url="wss://api.hyperliquid-testnet.xyz/ws"        # HARDCODED!
    )
    secrets = PrivateKeyAuthSecrets(
        private_key=SecretStr("0x" + "1" * 64)               # HARDCODED!
    )
    api = HyperliquidAPI(exchange_config=config, exchange_secrets=secrets)
```

### After Refactoring (Standardized)

```python
def test_new_way_with_fixtures(
    active_hl_config: ExchangeSpecificConfig,  # ✅ From file-based config
    active_hl_secrets: PrivateKeyAuthSecrets,  # ✅ From file-based secrets
):
    """Test using individual fixtures."""
    api = HyperliquidAPI(
        exchange_config=active_hl_config,
        exchange_secrets=active_hl_secrets,
    )
    # Test logic here...

def test_new_way_with_api_client(hl_api_for_test_env: HyperliquidAPI):
    """Test using pre-configured API client - RECOMMENDED approach."""
    # ✅ API client is already configured with correct environment
    await hl_api_for_test_env.get_market_data(...)

def test_with_dependency_injection(hl_api_with_di: Callable[..., HyperliquidAPI]):
    """Test using dependency injection for unit tests."""
    # ✅ Clean, testable, mockable
    api = hl_api_with_di()
    # Test logic here...
```

## Environment Switching

You can easily switch environments using environment variables:

```bash
# Test against testnet (default)
pytest tests/integration/

# Test against mainnet
CYBERDELTA_TEST_ENV_HL=mainnet pytest tests/integration/

# Mix environments for different exchanges
CYBERDELTA_TEST_ENV_HL=mainnet CYBERDELTA_TEST_ENV_BP=testnet pytest tests/

# Use with specific test files
CYBERDELTA_TEST_ENV_HL=mainnet pytest tests/integration/apis/hyperliquid/test_hl_market_data_public_endpoints.py
```

## Configuration File Structure

### tests/config/test_config.yaml

```yaml
# Test environment configuration
# Copy from tests/config/test_config.yaml.example and customize

exchanges:
  hyperliquid:
    exchange_name: "hyperliquid"
    api_base_url_mainnet: "https://api.hyperliquid.xyz"
    ws_url_mainnet: "wss://api.hyperliquid.xyz/ws"
    api_base_url_testnet: "https://api.hyperliquid-testnet.xyz"
    ws_url_testnet: "wss://api.hyperliquid-testnet.xyz/ws"
    is_mainnet_environment: false  # Default to testnet for safety
    chain_id: 1337
    rate_limit_per_minute: 300
    symbols:
      BTC: "BTC"
      ETH: "ETH"
    # Hyperliquid-specific rate limiting
    ip_weight_limit_per_minute: 1200
    info_request_type_ip_weights:
      l2Book: 2
      allMids: 2
      meta: 2
      userRole: 60
      clearinghouseState: 10
      openOrders: 1
    default_info_weight: 20
    exchange_action_base_ip_weight: 1
    address_action_safety_net:
      rate_per_minute: 300
    websocket_send_rate_per_minute: 1800

  backpack:
    exchange_name: "backpack"
    api_base_url_mainnet: "https://api.backpack.exchange"
    ws_url_mainnet: "wss://ws.backpack.exchange"
    api_base_url_testnet: null  # Backpack doesn't have testnet
    ws_url_testnet: null
    is_mainnet_environment: true  # Backpack only has mainnet
    rate_limit_per_minute: 120
    symbols:
      SOL_USDC: "SOL_USDC"
      BTC_USDC: "BTC_USDC"
```

### tests/config/test_secrets.yaml

```yaml
# Test secrets configuration
# Copy from tests/config/test_secrets.yaml.example and populate with real values

exchanges:
  hyperliquid:
    private_key: "0x1234567890123456789012345678901234567890123456789012345678901234"
    passphrase: null
    private_key_testnet: "0x5678901234567890123456789012345678901234567890123456789012345678"
    testnet_seed_passphrase: null

  backpack:
    api_key: "your_backpack_api_key_here"
    api_secret: "your_backpack_api_secret_here"
```

## Key Benefits of This System

### 🎯 Single Source of Truth
- All test configuration centralized in dedicated files
- No hardcoded URLs scattered across test files
- Easy to update all tests by changing configuration files

### 🔄 Environment Awareness
- Easy switching between mainnet/testnet via environment variables
- Default to safe environments (testnet for Hyperliquid)
- Per-exchange environment control

### 🧪 Test Isolation
- Each test gets clean, configured components via dependency injection
- No shared state between tests
- Proper fixture scoping for performance

### 📁 Maintainability
- Configuration changes don't require code changes
- Clear separation between test logic and test configuration
- Documented configuration structure

### 🔒 Security
- Secrets properly managed and separated from configuration
- No hardcoded credentials in test files
- Easy to manage different secrets for different environments

### ⚡ Performance
- Session-scoped fixtures cache configuration loading
- No repeated file I/O during test execution
- Efficient dependency injection

### 🛡️ Reliability
- Proper error handling and validation
- Clear error messages when configuration is missing or invalid
- Fail-fast approach for configuration issues

## Migration from Old System

The refactoring eliminated these problematic patterns:

```python
# ❌ OLD: Hardcoded helper functions (REMOVED)
def simple_hl_config() -> dict[str, Any]:
    return {"api_base_url": "https://api.hyperliquid-testnet.xyz"}

def create_test_exchange_config() -> ExchangeSpecificConfig:
    return ExchangeSpecificConfig.model_validate({...})

# ❌ OLD: Direct API instantiation (REMOVED)
api = HyperliquidAPI(
    exchange_config=ExchangeSpecificConfig.model_validate({
        "api_base_url": "https://hardcoded.url",  # HARDCODED!
    }),
    exchange_secrets=PrivateKeyAuthSecrets(...)
)
```

All tests now use the standardized fixture system for clean, maintainable, and reliable configuration management.

## Troubleshooting

### Configuration File Not Found
```
pytest.skip: Test config file not found: /path/to/test_config.yaml
```
**Solution:** Copy `tests/config/test_config.yaml.example` to `tests/config/test_config.yaml` and customize.

### Invalid Configuration
```
pytest.fail: Failed to load test AppSettings: ValidationError
```
**Solution:** Check your YAML syntax and ensure all required fields are present and properly typed.

### Environment Variable Issues
```bash
# Check current environment settings
echo $CYBERDELTA_TEST_ENV_HL

# Reset to default
unset CYBERDELTA_TEST_ENV_HL
```

### Secrets Loading Issues
**Solution:** Ensure `tests/config/test_secrets.yaml` exists and contains valid credentials for your test environment.

## Future Enhancements

- Integration with CI/CD for environment-specific test configuration
- Automatic environment detection based on available credentials
- Configuration validation and schema enforcement
- Dynamic configuration reloading for development workflows

# Environment Loading Flow After Boolean Refactor

## Overview
The `is_mainnet_environment` boolean has been replaced with `EnvironmentType` enum, providing type-safe environment configuration.

## Configuration Flow

### 1. Configuration File (`test_config.yaml`)
```yaml
exchanges:
  hyperliquid:
    # Environment type - explicit testnet selection (safer than boolean)
    environment_type: "testnet"

    # Mainnet URLs
    api_base_url_mainnet: "https://api.hyperliquid.xyz"
    ws_url_mainnet: "wss://api.hyperliquid.xyz/ws"

    # Testnet URLs
    api_base_url_testnet: "https://api.hyperliquid-testnet.xyz"
    ws_url_testnet: "wss://api.hyperliquid-testnet.xyz/ws"
```

### 2. Pydantic Model (`ExchangeSpecificConfig`)
```python
# In config_models.py
class ExchangeSpecificConfig(BaseModel):
    # Environment type (replaces dangerous boolean is_mainnet_environment)
    environment_type: EnvironmentType = Field(
        default=EnvironmentType.MAINNET,
        description="Environment type for mainnet/testnet selection"
    )

    @property
    def active_api_base_url(self) -> HttpUrl:
        """Return the active API base URL based on environment setting."""
        if self.environment_type == EnvironmentType.MAINNET:
            return self.api_base_url_mainnet
        return self.api_base_url_testnet
```

### 3. EnvironmentType Enum
```python
# In enums/environment.py
class EnvironmentType(Enum):
    MAINNET = "mainnet"  # Production environment with real funds
    TESTNET = "testnet"  # Test environment for development

    @property
    def is_production(self) -> bool:
        return self == EnvironmentType.MAINNET
```

### 4. Test Fixtures Loading
```python
# In tests/fixtures/config_fixtures.py
@pytest.fixture(scope="session")
def active_hl_config(test_app_settings: AppSettings) -> ExchangeSpecificConfig:
    """Loads from test_config.yaml"""
    hl_config = test_app_settings.exchanges["hyperliquid"]
    # Config already has environment_type set from YAML
    return hl_config
```

### 5. API Initialization
```python
# In hyperliquid/hl_api.py
class HyperliquidAPI:
    def __init__(self, exchange_config: ExchangeSpecificConfig):
        # URL Selection based on environment
        if exchange_config.environment_type.is_production:
            self.active_api_base_url = str(exchange_config.api_base_url_mainnet)
        else:
            self.active_api_base_url = str(exchange_config.api_base_url_testnet)
```

## Benefits Over Boolean Approach

1. **Type Safety**: Can't accidentally pass `True`/`False` to unrelated functions
2. **Explicit Intent**: `EnvironmentType.TESTNET` is clearer than `is_mainnet_environment=False`
3. **Extensibility**: Easy to add new environments (e.g., `STAGING`, `DEV`)
4. **Configuration Validation**: Pydantic validates the enum value from YAML
5. **No Negative Logic**: No more confusing `not is_mainnet_environment`

## Testing Environment Override

Tests can override the environment via environment variable:
```bash
CYBERDELTA_TEST_ENV_HL=mainnet pytest tests/
```

Or the fixture system respects the config file setting by default.

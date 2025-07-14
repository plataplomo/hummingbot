# Configuration Guide

## Overview

The CyberDeltaEngine uses a structured configuration system with a focus on security, clarity, and flexibility. This guide explains the configuration architecture and how to set up your environment properly.

## Configuration Files

The system uses two main configuration files:

1. **`config.yaml`**: Contains non-sensitive configuration settings.
2. **`secrets.yaml`**: Contains sensitive information like API keys and credentials.

### Security Architecture

A key security feature is that `secrets.yaml` is **never** stored in the repository. Instead:

- Secrets are stored in a secure location **outside** the Git repository structure.
- The `SecretsManager` class loads secrets from this external location.
- Example files are provided in the repository for reference only.

## Configuration Locations

### `config.yaml` Locations (in order of precedence)

1. Path specified by the `CYBERDELTA_CONFIG_PATH` environment variable
2. `./config.yaml` (current working directory)
3. `./config/config.yaml`
4. `/cyberdelta/config/config.yaml` (inside the package)

### `secrets.yaml` Locations (in order of precedence)

1. Path specified by the `CYBERDELTA_SECRETS_PATH` environment variable
2. `~/.cyberdelta/secrets.yaml` (in user's home directory)
3. `/etc/cyberdelta/secrets.yaml`
4. `/opt/cyberdelta/secrets.yaml`

## Setting Up Your Environment

### Step 1: Create the secrets file

```bash
# Create directory for secrets (outside repository)
mkdir -p ~/.cyberdelta

# Copy the example secrets file and edit with your actual credentials
cp cyberdelta/config/secrets.yaml.example ~/.cyberdelta/secrets.yaml
```

Then edit `~/.cyberdelta/secrets.yaml` with your actual API keys and credentials.

### Step 2: Set up the configuration file

```bash
# Copy the example config file to your working directory
cp cyberdelta/config/config.yaml.example config.yaml
```

Edit `config.yaml` to customize the behavior of the system.

### Step 3 (Optional): Set environment variables

For production environments, you may want to set the environment variables:

```bash
export CYBERDELTA_SECRETS_PATH="/path/to/your/secrets.yaml"
export CYBERDELTA_CONFIG_PATH="/path/to/your/config.yaml"
```

These can also be set in your `.env` file:

```
CYBERDELTA_SECRETS_PATH=/path/to/your/secrets.yaml
CYBERDELTA_CONFIG_PATH=/path/to/your/config.yaml
```

## Configuration Structure

### General Settings

```yaml
general:
  log_level: INFO
  safe_mode: true
  state_file: "data/state.json"
  # ...
```

| Parameter | Description | Default |
|-----------|-------------|---------|
| `log_level` | Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) | INFO |
| `safe_mode` | Start in read-only mode (no trading) | true |
| `state_file` | Path to state persistence file | data/state.json |

### Exchange Configuration

```yaml
exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.hyperliquid.xyz"
    # ...

  backpack:
    enabled: true
    # ...
```

### Strategy Configuration

```yaml
strategies:
  hl_perp_bp_spot:
    enabled: true
    symbols:
      hl_symbol: "BTC"
      bp_symbol: "BTC_USDC"
    params:
      funding_threshold: 0.0001
      # ...
```

### Risk Management (Enhanced Configuration)

The risk management system uses a comprehensive configuration structure with direct Pydantic integration:

```yaml
risk:
  enabled: true
  
  # Global risk limits (applied to all positions)
  global:
    max_position_usd: "5000.0"        # Maximum position size in USD
    max_total_exposure_usd: "25000.0" # Maximum total portfolio exposure
  
  # Risk checker configuration
  checkers:
    # Enable/disable individual checkers
    enable_required_fields: true
    enable_profitability: true
    enable_price_sanity: true
    enable_volatility: true
    enable_funding_rate: true
    enable_circuit_breaker: true
    enable_balance: true
    
    # Checker thresholds (all Decimal types for precision)
    thresholds:
      min_profitability: "0.001"           # 0.1% minimum profit threshold
      max_price_deviation: "0.1"           # 10% maximum price deviation
      max_price_spread: "0.05"             # 5% maximum spread
      min_price: "0.0001"                  # Minimum valid price
      max_price: "100000"                  # Maximum valid price
      outlier_z_score_threshold: 3.0       # Z-score for outlier detection
      max_funding_rate: "0.01"             # 1% maximum funding rate
      max_funding_rate_spread: "0.005"     # 0.5% maximum funding rate spread
      max_volatility: "0.2"                # 20% maximum volatility
      min_volatility: "0.001"              # 0.1% minimum volatility
      min_balance_ratio: "0.1"             # 10% minimum balance ratio
      min_funding_rate: "-0.01"            # -1% minimum funding rate (negative)
      max_funding_rate_volatility: "0.002" # 0.2% maximum funding rate volatility
      min_funding_rate_confidence: "0.7"   # 70% minimum confidence
    
    # Pipeline configuration
    fail_fast: true                     # Stop on first failure
    max_concurrent_checks: 5            # Maximum parallel checks
    check_timeout_seconds: 5.0          # Timeout per check
    funding_rate_lookback_hours: 24     # Hours of funding rate history
    volatility_lookback_hours: 24       # Hours of volatility history
    include_fees_in_profitability: true # Include trading fees
    enable_outlier_detection: true      # Enable outlier detection
    check_both_exchanges: true          # Check both sides of trade
  
  # Position sizing configuration
  sizing:
    method: "simple"                    # "simple" or "kelly"
    
    # Simple sizing parameters
    simple_method: "fixed_fraction"     # "fixed_fraction" or "fixed_usd"
    simple_fixed_fraction: "0.02"      # 2% of capital per position
    simple_fixed_usd: "1000"           # Fixed USD amount per position
    
    # Kelly criterion parameters (when method="kelly")
    kelly_multiplier: "0.25"           # 25% of Kelly fraction
    kelly_max_allocation: "0.1"        # 10% maximum allocation
    kelly_min_allocation: "0.01"       # 1% minimum allocation
    kelly_risk_free_rate: 0.02         # 2% annual risk-free rate
    
    # Position limits
    min_position_size: "100"           # Minimum position size in USD
    max_position_size: "10000"         # Maximum position size in USD
    max_leverage: "5.0"                # Maximum leverage
    max_portfolio_allocation: "0.5"    # 50% maximum portfolio allocation
    total_capital: "100000"            # Total available capital
    
    # Volatility adjustment
    min_volatility: "0.001"            # Minimum volatility for sizing
    max_volatility_bound: "1.0"        # Maximum volatility bound
    volatility_lookback_hours: 24      # Hours for volatility calculation
    
    # Validation factors
    enable_validation_factors: true    # Enable position size validation
    enable_volatility_adjustment: true # Adjust for volatility
    enable_spread_adjustment: true     # Adjust for spread
    base_validation_factor: "0.8"      # Base validation factor (80%)
    sizing_timeout_seconds: 10.0       # Timeout for sizing operations
  
  # System configuration
  log_level: "INFO"                   # Risk system log level
  log_all_checks: false               # Log all check results
  log_performance_metrics: true       # Log performance metrics
  max_concurrent_checks: 10           # System-wide concurrent checks
  max_concurrent_sizing: 5            # System-wide concurrent sizing
  
  # Backward compatibility fields (preserved during migration)
  use_simple_sizing_path: true        # Legacy simple sizing flag
  simple_sizing_method: "fixed_fraction" # Legacy sizing method
  simple_fixed_fraction: "0.1"       # Legacy fixed fraction
  simple_fixed_usd_size: "10.0"      # Legacy fixed USD size
```

#### Risk Configuration Presets

The system supports predefined risk configuration presets:

- **Conservative**: Higher profitability thresholds, lower volatility limits, reduced leverage
- **Moderate**: Balanced settings (default behavior)
- **Aggressive**: Lower profitability thresholds, higher volatility limits, increased leverage

Presets can be applied using the `ConfigurationMigrator.apply_preset()` method.

## Using Configuration in Code

### Loading Configuration

The enhanced configuration system uses Pydantic models for type safety and validation:

```python
from cyberdelta.config import AppSettings

# Load complete application settings
app_settings = AppSettings.model_validate_json(config_json)

# Access configuration values with full type safety
log_level = app_settings.general.log_level
max_position = app_settings.risk.global_risk.max_position_usd
profitability_threshold = app_settings.risk.checkers.thresholds.min_profitability

# Access nested configuration
if app_settings.risk.checkers.enable_profitability:
    threshold = app_settings.risk.checkers.thresholds.min_profitability
    
# Check if Kelly sizing is enabled
if app_settings.risk.sizing.method == "kelly":
    kelly_multiplier = app_settings.risk.sizing.kelly_multiplier
```

### Risk Manager Integration

```python
from cyberdelta.core.risk.orchestrator.risk_manager_factory import RiskManagerFactory

# Create risk manager from configuration
risk_manager = RiskManagerFactory.create_risk_manager(
    app_settings=app_settings,
    portfolio_tracker=portfolio_tracker,
    circuit_breaker_system=circuit_breaker,
    funding_rate_validator=funding_validator,
)

# Create risk manager with preset
conservative_manager = RiskManagerFactory.create_from_preset(
    preset_name="conservative",
    base_app_settings=app_settings,
    portfolio_tracker=portfolio_tracker,
)
```

### Configuration Migration

For legacy configurations, use the migration utility:

```python
from cyberdelta.core.risk.config.migration import ConfigurationMigrator

# Migrate legacy dict-based configuration
legacy_config = {
    "min_profitability_threshold": 0.002,
    "max_volatility": 0.3,
    "kelly_multiplier": 0.25,
    "use_simple_sizing_path": True,
}

enhanced_config = ConfigurationMigrator.migrate_legacy_config(legacy_config)

# Apply preset to existing configuration
preset_config = ConfigurationMigrator.apply_preset(
    base_config=app_settings.model_dump(),
    preset_name="conservative"
)
```

### Configuration Validation

The enhanced configuration system provides comprehensive validation:

1. **Type Safety**: All configuration values are validated using Pydantic models with strict typing
2. **Range Validation**: Numeric values are checked against valid ranges (e.g., percentages 0-1)
3. **Cross-Field Validation**: Related fields are validated together (e.g., min < max values)
4. **Required Fields**: Essential parameters are enforced at load time
5. **Decimal Precision**: Financial values use `Decimal` type for precision arithmetic

```python
# Example validation errors you might see:
# ValueError: min_profitability must be less than max_price_spread
# ValueError: kelly_min_allocation must be less than kelly_max_allocation
# ValidationError: max_position_usd must be greater than 0
```

### Configuration Architecture Changes

The refactored risk management system introduces several architectural improvements:

#### Direct Pydantic Integration
- **Before**: Dict-based configuration with runtime type conversion
- **After**: Direct Pydantic model access with compile-time type safety

#### Clean Break Design
- Eliminates intermediate `ConfigBridge` abstraction
- Direct access to configuration values through `app_settings.risk.*`
- Type-safe configuration access patterns

#### Enhanced Type Safety
- All checkers inherit from `TypedBaseChecker` with AppSettings access
- Position sizers inherit from `TypedBaseSizer` with direct configuration
- Protocol-based dependency injection for external systems

#### Configuration Migration Support
- Automatic migration from legacy dict-based configurations
- Backward compatibility preservation during transition
- Support for configuration presets (conservative/moderate/aggressive)

## Best Practices

### Security
1. **Never** commit your `secrets.yaml` file to version control
2. Always set `safe_mode: true` initially and only disable after thorough testing
3. Check the logs for configuration warnings or errors on startup

### Risk Configuration
4. **Use Decimal strings** for all financial values to ensure precision (e.g., `"0.001"` not `0.001`)
5. **Start with conservative settings** and gradually adjust based on performance
6. **Test configuration changes** in a safe environment before production
7. **Monitor position sizes** against global risk limits to prevent overexposure

### Development Workflow
8. **Use configuration presets** for consistent risk profiles across environments
9. **Leverage migration utilities** when updating legacy configurations
10. **Validate configurations** early using Pydantic model validation
11. **Use type hints** when accessing configuration values for better IDE support

### Performance
12. **Cache AppSettings** instances rather than repeatedly parsing configuration
13. **Use fail_fast: true** in development to catch configuration issues quickly
14. **Set appropriate timeouts** for checker and sizing operations

### Example Configuration Workflow

```python
# 1. Load and validate configuration
try:
    app_settings = AppSettings.model_validate_json(config_json)
except ValidationError as e:
    logger.error(f"Configuration validation failed: {e}")
    raise

# 2. Apply preset if needed
if use_preset:
    config_dict = ConfigurationMigrator.apply_preset(
        base_config=app_settings.model_dump(),
        preset_name="conservative"
    )
    app_settings = AppSettings.model_validate(config_dict)

# 3. Create risk manager
risk_manager = RiskManagerFactory.create_risk_manager(
    app_settings=app_settings,
    portfolio_tracker=portfolio_tracker,
)

# 4. Validate risk limits
assert app_settings.risk.global_risk.max_position_usd > 0
assert app_settings.risk.sizing.max_position_size <= app_settings.risk.global_risk.max_position_usd
```

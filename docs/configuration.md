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

### Risk Management

```yaml
risk:
  global:
    max_position_usd: 1000.0
    # ...
  
  strategies:
    hl_perp_bp_spot:
      max_position_usd: 1000.0
      # ...
```

## Using Configuration in Code

### Loading Configuration

```python
from cyberdelta.config import config, secrets

# Access configuration values
log_level = config.get('general.log_level', 'INFO')
max_position = config.get('risk.global.max_position_usd', 1000.0)

# Access secrets
api_key = secrets.get('exchanges.hyperliquid.api_key')
```

### Configuration Validation

The system validates your configuration on load and will:

1. Check that all required sections are present
2. Verify that essential parameters have valid values
3. Warn about potentially conflicting or duplicated parameters

## Best Practices

1. **Never** commit your `secrets.yaml` file to version control
2. Use the most specific configuration parameters for your use case
3. Always set `safe_mode: true` initially and only disable after thorough testing
4. Check the logs for configuration warnings or errors on startup 
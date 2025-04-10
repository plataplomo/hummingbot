# Configuration Refactoring Guide - CyberDeltaEngine

**Note (Post-Critic Feedback Aug 6):** The critic review on Aug 6 identified that the current `config.yaml` **does not adhere to this guide**, calling it a "DISASTER" due to bloat, duplicates, and out-of-scope parameters. **Mandate #1** is to **immediately refactor `config.yaml`** to be clean, lean, consolidated, and strictly follow the principles outlined below.

## 1. Overview

This guide provides instructions for refactoring the configuration system of the CyberDeltaEngine (`CyberDeltaEngine`) project. The goal is to move from the current potentially cluttered `config.yaml` to a clean, validated, and secure configuration structure.

## Critical Issues Identified by Critic

The Gemini critic identified several serious configuration issues that must be addressed immediately:

1. **Security Vulnerability**: `secrets.yaml` is incorrectly placed inside the source tree
2. **Configuration Bloat**: `config.yaml` contains duplicated settings and out-of-scope parameters
3. **Poor Configuration Structure**: Lack of clear hierarchies and conflicting parameters

## 1. Secrets Management Refactoring

### Current Problem
- `secrets.yaml` is located inside the source code tree (`/cyberdelta/config/`)
- This creates a significant security risk as it could be accidentally committed to Git
- Comment-based protection ("IMPORTANT: Do not commit") is inadequate

### Required Changes

#### 1.1 Move Secrets Out of Source Tree
- Relocate secrets to a path **outside** the Git repository structure
- Create a dedicated directory (`~/.cyberdelta/secrets/`)

#### 1.2 Use Environment Variables for Secrets Path
- Add environment variable `CYBERDELTA_KEYS_PATH` to specify the secrets location
- Implement fallback paths if environment variable is not set

#### 1.3 Implementation Plan

```python
# secrets_manager.py
import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional

class SecretsManager:
    """Manages loading of secrets from secure location outside source tree"""
    
    def __init__(self):
        self.secrets: Dict[str, Any] = {}
        self.secrets_loaded = False
    
    def load_secrets(self) -> bool:
        """
        Load secrets from the configured location outside the source tree.
        
        Returns:
            bool: True if secrets were loaded successfully, False otherwise
        """
        # Get secrets path from environment or use default fallbacks
        secrets_path = self._get_secrets_path()
        
        if not secrets_path.exists():
            print(f"Secrets file not found at {secrets_path}")
            return False
        
        try:
            with open(secrets_path, 'r') as f:
                self.secrets = yaml.safe_load(f)
            self.secrets_loaded = True
            return True
        except Exception as e:
            print(f"Error loading secrets: {e}")
            return False
    
    def _get_secrets_path(self) -> Path:
        """
        Get the path to the secrets file from environment variable or default locations
        
        Returns:
            Path: The path to the secrets file
        """
        # Try environment variable first
        env_path = os.environ.get('CYBERDELTA_SECRETS_PATH')
        if env_path:
            return Path(env_path)
        
        # Try default locations in order of preference
        home_dir = Path.home()
        default_paths = [
            home_dir / '.cyberdelta' / 'secrets.yaml',
            Path('/etc/cyberdelta/secrets.yaml'),
            Path('/opt/cyberdelta/secrets.yaml')
        ]
        
        for path in default_paths:
            if path.exists():
                return path
        
        # Return the first default path as fallback
        return default_paths[0]
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a secret value by key
        
        Args:
            key: The key to look up
            default: Default value if key doesn't exist
            
        Returns:
            The secret value or default
        """
        if not self.secrets_loaded:
            self.load_secrets()
        
        # Support nested keys with dot notation (e.g., "exchanges.hyperliquid.api_key")
        keys = key.split('.')
        value = self.secrets
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
```

#### 1.4 Migration Process

1. Create the new directory outside the Git repository
2. Move `secrets.yaml` to the new location
3. Update the code to use the new `SecretsManager` class
4. Add `.env` file support for development environments
5. Document the new secrets location in README.md and SETUP.md
6. Update deployment documentation with environment variable requirements

## 2. Configuration Cleanup

### Current Problem
- `config.yaml` is bloated with:
  - Duplicate sections and conflicting parameters
  - Out-of-scope features not needed for Prototype 0.0.1
  - Confusing hierarchy with overlapping parameter names

### Required Changes

#### 2.1 Strip Down Configuration to v0.0.1 Essentials

Remove ALL parameters related to:
- Backtesting features
- Data storage options (parquet/sqlite)
- Strategy params for unused strategies (Moving Average/RSI/Bollinger)
- Complex risk management (VaR beyond simple limits)
- Trailing stops and complex order types
- Any other features explicitly out of scope for v0.0.1

#### 2.2 Create Clear, Focused Hierarchy

Reorganize configuration into a clean, logical hierarchy:

```yaml
# Simplified config.yaml structure
general:
  log_level: INFO
  safe_mode: true  # Start in safe mode (read-only)

exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.hyperliquid.xyz"
    ws_url: "wss://api.hyperliquid.xyz/ws"
    rate_limit_per_minute: 120
    
  backpack:
    enabled: true
    api_base_url: "https://api.backpack.exchange"
    ws_url: "wss://ws.backpack.exchange"
    rate_limit_per_minute: 120

strategies:
  hl_perp_bp_spot:
    enabled: true
    symbols:
      hl_symbol: "BTC"
      bp_symbol: "BTC_USDC"
    params:
      funding_threshold: 0.0001  # 0.01% min funding rate
      min_spread: 0.0002  # 0.02% max price spread
      min_profit_usd: 1.0  # Minimum profit to execute
    
  hl_perp_bp_perp:
    enabled: false  # Disabled by default until primary strategy is proven
    symbols:
      hl_symbol: "BTC"
      bp_symbol: "BTC-PERP"
    params:
      min_funding_diff: 0.0002  # 0.02% min funding differential
      max_basis_spread: 0.005  # 0.5% max basis spread
      min_profit_usd: 2.0  # Higher min profit for riskier strategy

risk:
  global:
    max_position_usd: 1000.0  # Maximum position size in USD
    max_total_exposure_usd: 5000.0  # Maximum total exposure
    max_portfolio_leverage: 2.0  # Maximum leverage across portfolio
  
  strategies:
    hl_perp_bp_spot:
      max_position_usd: 1000.0
      max_leverage: 3.0
    
    hl_perp_bp_perp:
      max_position_usd: 500.0  # More conservative
      max_leverage: 1.5  # More conservative

circuit_breakers:
  enabled: true
  global:
    failure_threshold: 3
    reset_timeout: 300  # 5 minutes
  
  hyperliquid:
    failure_threshold: 3  # Exchange-specific settings
  
  backpack:
    failure_threshold: 3  # Exchange-specific settings

validation:
  funding_rates:
    enabled: true
    max_error_threshold: 0.0005  # 0.05% max tolerated error
    check_interval_seconds: 300  # 5 minutes
  
  positions:
    enabled: true
    reconcile_interval_seconds: 600  # 10 minutes
    max_discrepancy_pct: 0.05  # 5% max discrepancy
```

#### 2.3 Database Credentials Handling

- Move all real DB credentials to `secrets.yaml`
- Keep only placeholder DB configs in `config.yaml`
- Ensure DB connection string construction combines config + secrets

#### 2.4 Implementation Changes

Update the configuration loader to:
1. Validate configuration against a schema
2. Check for and warn about duplicate/conflicting values
3. Apply defaults in a consistent manner
4. Log loading success/failure

```python
# config_manager.py
import os
import yaml
import jsonschema
from pathlib import Path
from typing import Dict, Any, Optional

class ConfigManager:
    """Manages loading and validation of configuration"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config: Dict[str, Any] = {}
        self.config_path = config_path or self._get_default_config_path()
        self.loaded = False
    
    def load(self) -> bool:
        """
        Load configuration from file
        
        Returns:
            bool: True if config was loaded successfully
        """
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            
            # Validate configuration against schema
            self._validate_config()
            self.loaded = True
            return True
        except Exception as e:
            print(f"Error loading configuration: {e}")
            return False
    
    def _get_default_config_path(self) -> str:
        """Get default configuration path"""
        # Check environment variable first
        env_path = os.environ.get('CYBERDELTA_CONFIG_PATH')
        if env_path:
            return env_path
        
        # Look in standard locations
        default_paths = [
            os.path.join(os.getcwd(), 'config.yaml'),
            os.path.join(os.getcwd(), 'config', 'config.yaml'),
        ]
        
        for path in default_paths:
            if os.path.exists(path):
                return path
        
        return default_paths[0]  # Return first default as fallback
    
    def _validate_config(self) -> None:
        """Validate configuration against schema"""
        # For v0.0.1, perform basic validation
        required_sections = ['general', 'exchanges', 'strategies', 'risk']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Check for minimum exchange configuration
        exchanges = self.config.get('exchanges', {})
        if not exchanges.get('hyperliquid', {}).get('enabled', False):
            raise ValueError("Hyperliquid exchange must be enabled")
        if not exchanges.get('backpack', {}).get('enabled', False):
            raise ValueError("Backpack exchange must be enabled")
            
        # Additional validation can be added as needed
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a configuration value by key path
        
        Args:
            key_path: Dot-separated path to config value
            default: Default value if key doesn't exist
            
        Returns:
            Configuration value or default
        """
        if not self.loaded:
            self.load()
        
        # Handle dot notation for nested keys
        keys = key_path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
```

## 3. Integration with Application

### 3.1 Main Class Integration

Update the main application to use the refactored configuration:

```python
from config_manager import ConfigManager
from secrets_manager import SecretsManager

class CyberDeltaEngine:
    """Main application class"""
    
    def __init__(self):
        # Load configuration and secrets from secured locations
        self.config_manager = ConfigManager()
        self.secrets_manager = SecretsManager()
        
        # Initialize other components with config and secrets
        if not self.config_manager.load():
            raise RuntimeError("Failed to load configuration")
        
        if not self.secrets_manager.load_secrets():
            raise RuntimeError("Failed to load secrets")
            
        # Continue with initialization...
```

### 3.2 Component Access to Config/Secrets

Components should access configuration and secrets through the managers:

```python
# Good pattern for components
class SomeComponent:
    def __init__(self, config_manager, secrets_manager):
        self.config = config_manager
        self.secrets = secrets_manager
        
        # Access config values
        self.log_level = self.config.get('general.log_level', 'INFO')
        
        # Access secrets
        self.api_key = self.secrets.get('exchanges.hyperliquid.api_key')
```

## 4. Documentation

### 4.1 Configuration Documentation

Create a detailed configuration reference document:

```markdown
# Configuration Reference

## Overview
This document describes the configuration structure for CyberDeltaEngine.

## File Structure
- `config.yaml`: Contains non-sensitive configuration settings.
- `secrets.yaml`: Contains sensitive information (API keys, credentials).

## Secrets Management
Secrets MUST be stored outside the repository in a secure location:
- Default: `~/.cyberdelta/secrets.yaml`
- Environment variable: `CYBERDELTA_SECRETS_PATH`

## Configuration Parameters

### General Settings
- `general.log_level`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `general.safe_mode`: Start in safe mode (read-only)

... (document all configuration parameters)
```

### 4.2 Example Files

Create example configuration files with placeholders and comments:

```yaml
# config.yaml.example
general:
  log_level: INFO
  safe_mode: true  # Start in safe mode (read-only)

exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.hyperliquid.xyz"
    # Additional parameters...
```

```yaml
# secrets.yaml.example
exchanges:
  hyperliquid:
    api_key: "your_api_key_here"
    api_secret: "your_api_secret_here"
  
  backpack:
    api_key: "your_api_key_here"
    api_secret: "your_api_secret_here"
```

## 5. Implementation Timeline

1. **Immediate**:
   - Move `secrets.yaml` to secure location outside repository
   - Create and populate `.gitignore` to include `secrets.yaml` and other sensitive files
   
2. **Day 1**:
   - Implement `SecretsManager` class
   - Refactor configuration to remove duplication and out-of-scope parameters
   
3. **Day 2**:
   - Implement `ConfigManager` class with validation
   - Update all components to use the new configuration and secrets managers
   
4. **Day 3**:
   - Create documentation and example files
   - Perform end-to-end testing of configuration loading 
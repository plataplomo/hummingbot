# CyberDeltaEngine

A cross-exchange delta-neutral trading system optimized for funding rate arbitrage between cryptocurrency exchanges.

## Overview

CyberDeltaEngine is designed to identify and exploit funding rate differentials between exchanges while maintaining delta-neutral positions. The primary strategy in v0.0.1 is a Hyperliquid-Perp vs Backpack-Spot approach that:

1. Takes positions on Hyperliquid perpetual contracts
2. Hedges with opposite positions in Backpack spot markets
3. Profits from funding rate payments while maintaining delta neutrality

## Key Features

- **Cross-Exchange Arbitrage**: Exploits funding rate differentials between exchanges
- **Delta-Neutral Trading**: Maintains hedged positions to minimize directional risk
- **Risk Management**: Implements position sizing, risk controls, and maximum exposure limits
- **Validation Systems**: Tracks prediction accuracy and validates funding rates and positions
- **Robust State Management**: Reliable state persistence with backup and recovery mechanisms
- **Balance Monitoring**: Tracks balances across exchanges and alerts on low balances
- **Extensible Architecture**: Modular design with clear separation of concerns
- **Safety Systems**: Comprehensive safety measures including position reconciliation and circuit breakers

## System Architecture

```
┌─────────────────┐      ┌──────────────────┐      ┌────────────────┐
│                 │      │                  │      │                │
│   Data Handler  │◄────►│  Signal Generator│◄────►│  Risk Manager  │
│                 │      │                  │      │                │
└───────┬─────────┘      └──────────────────┘      └────────┬───────┘
        │                                                   │
        │                   ┌──────────────┐               │
        │                   │  Validation  │               │
        │                   │  Systems     │               │
        │                   └──────┬───────┘               │
        │                          │                       │
┌───────▼─────────┐               │                ┌───────▼────────┐
│                 │               │                │                │
│  API Clients    │◄─────────────►│◄───────────────│ Execution      │
│  - Hyperliquid  │               │                │ Handler        │
│  - Backpack     │               │                │                │
│                 │               │                │                │
└───────┬─────────┘               │                └────────┬───────┘
        │                         │                         │
        │                         │                         │
┌───────▼─────────┐              │                 ┌────────▼───────┐
│                 │              │                 │                │
│  Portfolio      │◄─────────────┘                 │ Balance        │
│  Tracker        │                                │ Monitor        │
│                 │                                │                │
└─────────────────┘                                └────────────────┘
```

## Core Components

- **Data Handler**: Collects and processes market data from exchanges
- **Portfolio Tracker**: Tracks positions, balances, and orders across exchanges
- **Signal Generator**: Identifies funding rate arbitrage opportunities
- **Risk Manager**: Evaluates and sizes trading opportunities based on risk parameters
- **Execution Handler**: Executes trades on exchanges with error handling
- **Balance Monitor**: Monitors exchange balances and generates alerts
- **State Manager**: Provides reliable state persistence with backup and recovery
- **Validation Systems**: Comprehensive safety systems for monitoring and protection

## Strategy Implementation

The primary strategy for v0.0.1 is the `FundingRateArbitrageStrategy` which:

1. Monitors funding rates on Hyperliquid perpetual contracts
2. Calculates Net Funding Differential (NFD)
3. Estimates trading costs and expected profits
4. Calculates basis volatility for risk assessment
5. Implements a utility function for opportunity ranking
6. Takes delta-neutral positions across exchanges (perp on Hyperliquid, spot on Backpack)
7. Monitors and rebalances positions as needed

## Validation and Safety Systems

The system incorporates robust validation and safety measures:

1. **Funding Rate Validation**: Tracks funding rate predictions against actual payments
   - Records predictions from multiple sources (API, model, historical)
   - Stores actual funding payments received
   - Calculates accuracy metrics (RMSE, MAE, bias)
   - Generates validation reports for ongoing improvement

2. **Position Reconciliation**: Verifies position consistency between:
   - Exchange API-reported positions
   - Fill history-derived positions
   - Local state tracking
   - Automatic or manual correction of discrepancies
   - Historical tracking of reconciliation issues

3. **Circuit Breaker System**: Implements safety cutoffs when:
   - Market volatility exceeds thresholds
   - Portfolio drawdown exceeds limits
   - API errors exceed thresholds
   - Market liquidity falls below minimums
   - Automatic recovery testing when conditions normalize

## Installation

### Prerequisites

- Python 3.10+
- pip (Python package manager)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/CyberDeltaEngine.git
cd CyberDeltaEngine
```

2. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure settings:
   - Copy `config/config.example.yaml` to `config/config.yaml`
   - Copy `config/secrets.example.yaml` to `config/secrets.yaml`
   - Update the configuration files with your exchange API keys and preferences

## Usage

Run the bot with default settings:
```bash
python cyberdelta/main.py
```

Custom configuration:
```bash
python cyberdelta/main.py --config path/to/config.yaml --log-level DEBUG
```

Run in dry-run mode (no live trading):
```bash
python cyberdelta/main.py --dry-run
```

## Configuration

The system uses a secure, validated configuration approach with two main files:

### Configuration Files

1. **config.yaml**: Contains non-sensitive settings like strategy parameters and exchange URLs
2. **secrets.yaml**: Contains sensitive information like API keys (stored securely outside the repository)

### Secure Secrets Management

For security, secrets are stored **outside** the Git repository:

- Default location: `~/.cyberdelta/secrets.yaml`
- Custom location: Set via `CYBERDELTA_SECRETS_PATH` environment variable

**IMPORTANT: Never add secrets.yaml to version control!**

### Example Configuration

```yaml
# General settings
general:
  log_level: INFO
  safe_mode: true  # Start in safe mode (read-only)
  state_file: "data/state.json"
  state_backup_directory: "data/state_backups"
  state_save_interval: 300  # seconds
  state_backup_count: 5     # Number of previous state files to keep

# Exchange configuration
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

# Strategy configuration
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

# Validation and safety systems configuration
validation:
  funding_rate_validator:
    enabled: true
    accuracy_threshold: 0.0005  # Maximum tolerated prediction error
    alert_on_threshold: true    # Alert when threshold is exceeded
    data_retention_days: 90     # Days to keep validation data
    
  position_reconciliation:
    enabled: true
    threshold: 0.05            # 5% discrepancy threshold
    auto_correct: false        # Whether to auto-correct discrepancies
    check_interval: 3600       # Seconds between checks
    
  circuit_breaker:
    enabled: true
    exchanges:
      hyperliquid:
        volatility:
          threshold: 0.05      # 5% volatility threshold
          lookback_periods: 12 # Number of periods to consider
        drawdown:
          threshold: 0.10      # 10% drawdown threshold
        api_errors:
          threshold: 3         # Number of errors before tripping
          window_seconds: 60   # Time window for errors
```

### Example Secrets File

```yaml
# CyberDeltaEngine Secrets Configuration
# 
# IMPORTANT: DO NOT STORE REAL SECRETS IN THE REPOSITORY
# Store this file at: ~/.cyberdelta/secrets.yaml

# Exchange credentials
exchanges:
  # HyperLiquid exchange credentials
  hyperliquid:
    api_key: "YOUR_HYPERLIQUID_API_KEY"
    api_secret: "YOUR_HYPERLIQUID_API_SECRET"
    private_key: "YOUR_HYPERLIQUID_PRIVATE_KEY"  # If applicable

  # Backpack exchange credentials
  backpack:
    api_key: "YOUR_BACKPACK_API_KEY"
    api_secret: "YOUR_BACKPACK_API_SECRET"
```

### Configuration Usage

Access configuration in code:

```python
from cyberdelta.config import config, secrets

# Access configuration with dot notation
log_level = config.get('general.log_level', 'INFO')
hyperliquid_url = config.get('exchanges.hyperliquid.api_base_url')

# Access secrets securely
api_key = secrets.get('exchanges.hyperliquid.api_key')
```

### Environment Variables

Environment variables can be used to override configuration:

- `CYBERDELTA_CONFIG_PATH`: Custom path to config.yaml
- `CYBERDELTA_SECRETS_PATH`: Custom path to secrets.yaml
- `CYBERDELTA_LOG_LEVEL`: Override logging level
- `CYBERDELTA_SAFE_MODE`: Enable/disable safe mode (true/false)

## Development Status

The CyberDeltaEngine is under active development with the following progress:

- Phase 1 (Configuration Security & Cleanup): ✅ **COMPLETED**
- Phase 2 (Fix & Expand Test Suite): ✅ **COMPLETED**
- Phase 3 (Implement Safety Systems): ✅ **COMPLETED**
- Phase 4 (Core Strategy Implementation): 🟡 **IN PROGRESS**
- Phase 5 (Experimental Strategy & Additional Features): ⏱️ **PLANNED**

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
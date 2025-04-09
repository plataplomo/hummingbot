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
- **Robust State Management**: Reliable state persistence with backup and recovery mechanisms
- **Balance Monitoring**: Tracks balances across exchanges and alerts on low balances
- **Extensible Architecture**: Modular design with clear separation of concerns

## System Architecture

```
┌─────────────────┐      ┌──────────────────┐      ┌────────────────┐
│                 │      │                  │      │                │
│   Data Handler  │◄────►│  Signal Generator│◄────►│  Risk Manager  │
│                 │      │                  │      │                │
└───────┬─────────┘      └──────────────────┘      └────────┬───────┘
        │                                                   │
        │                                                   │
        │                                                   │
┌───────▼─────────┐                                ┌────────▼───────┐
│                 │                                │                │
│  API Clients    │                                │ Execution      │
│  - Hyperliquid  │◄──────────────────────────────►│ Handler        │
│  - Backpack     │                                │                │
│                 │                                │                │
└───────┬─────────┘                                └────────┬───────┘
        │                                                   │
        │                                                   │
┌───────▼─────────┐                                ┌────────▼───────┐
│                 │                                │                │
│  Portfolio      │◄──────────────────────────────►│ Balance        │
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

## Strategy Implementation

The primary strategy for v0.0.1 is the `FundingRateArbitrageStrategy` which:

1. Monitors funding rates on Hyperliquid perpetual contracts
2. Calculates Net Funding Differential (NFD)
3. Estimates trading costs and expected profits
4. Calculates basis volatility for risk assessment
5. Implements a utility function for opportunity ranking
6. Takes delta-neutral positions across exchanges (perp on Hyperliquid, spot on Backpack)
7. Monitors and rebalances positions as needed

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

The bot is configured through YAML files:

### config.yaml
Contains general settings, strategy parameters, and exchange configurations.

```yaml
# General settings
general:
  log_level: INFO
  state_file: state.json
  state_backup_directory: state_backups
  state_backup_count: 5
  check_interval: 60  # seconds

# Exchange configuration
exchanges:
  hyperliquid:
    enabled: true
    base_url: https://api.hyperliquid.xyz
    ws_url: wss://api.hyperliquid.xyz/ws
    symbols:
      - BTC-PERP
      - ETH-PERP
      - SOL-PERP
  
  backpack:
    enabled: true
    base_url: https://api.backpack.exchange
    ws_url: wss://ws.backpack.exchange
    symbols:
      - BTC_USDC
      - ETH_USDC
      - SOL_USDC

# Strategy parameters
strategy:
  symbols:
    - BTC-PERP
    - ETH-PERP
    - SOL-PERP
  funding_rate:
    min_funding_differential: 0.0001  # Minimum funding rate differential to consider
    min_profit_threshold: 5.0  # Minimum expected profit in USD
    risk_aversion: 1.0  # Risk aversion parameter for utility function
    rebalance_threshold: 0.05  # 5% threshold for rebalancing

# Risk parameters
risk:
  max_position_size: 1000.0  # USD
  max_total_exposure: 5000.0  # USD
  kelly_fraction: 0.5  # Conservative Kelly criterion
  max_collateral_per_exchange: 0.8  # 80% max on any exchange
  max_leverage: 5.0  # Maximum allowed leverage
```

### secrets.yaml
Contains sensitive API keys and credentials. Never commit this file to version control.

```yaml
# API Secrets
secrets:
  hyperliquid:
    WALLET_PRIVATE_KEY: "your_private_key_here"
  
  backpack:
    API_KEY: "your_api_key_here"
    API_SECRET: "your_api_secret_here"
```

## Development

### Project Structure

```
cyberdelta/
├── apis/
│   ├── base.py              # Base ExchangeAPI abstract class
│   ├── hyperliquid.py       # Hyperliquid API implementation
│   ├── backpack.py          # Backpack API implementation
│   └── errors.py            # API-related exceptions
├── core/
│   ├── data_handler.py      # Market data management
│   ├── portfolio_tracker.py # Position and balance tracking
│   ├── signal_generator.py  # Funding rate opportunity detection
│   ├── risk_manager.py      # Risk assessment and sizing
│   ├── execution_handler.py # Order execution
│   ├── balance_monitor.py   # Balance monitoring
│   ├── strategy.py          # Base Strategy abstract class
│   ├── engine.py            # Trading engine
│   └── types.py             # Data models and types
├── strategies/
│   ├── funding_rate_arbitrage.py  # Funding rate arbitrage strategy
│   └── ma_crossover.py            # Sample moving average strategy
├── utils/
│   ├── config.py            # Configuration handling
│   ├── logging_config.py    # Logging setup
│   ├── state_manager.py     # State persistence
│   └── constants.py         # Constant values
├── config/
│   ├── config.yaml          # Main configuration
│   └── secrets.yaml         # API keys and secrets
├── tests/                   # Test suite
├── main.py                  # Entry point
└── requirements.txt         # Dependencies
```

### Running Tests

Run unit tests:
```bash
python -m unittest discover -s cyberdelta/tests
```

## Disclaimer

This software is for educational purposes only. Trading cryptocurrencies involves significant risk and you should only risk capital you are willing to lose. The authors assume no responsibility for financial losses incurred through the use of this software.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
# Implementation Guide - CyberDeltaEngine Prototype 0.0.1

This document provides a practical, step-by-step approach to implementing the CyberDeltaEngine Prototype 0.0.1, focusing on core functionality and following the "simplicity first" principle.

## 1. Project Setup

### 1.1 Environment Setup

1. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install core dependencies:
```bash
pip install -r requirements.txt
```

3. Create a basic project structure:
```
cyberdelta/
├── apis/
│   ├── __init__.py
│   ├── base.py              # Base ExchangeAPI abstract class
│   ├── hyperliquid.py       # Hyperliquid API implementation
│   ├── backpack.py          # Backpack API implementation
│   └── errors.py            # API-related exceptions
├── core/
│   ├── __init__.py
│   ├── data_handler.py      # Market data management
│   ├── portfolio_tracker.py # Position and balance tracking
│   ├── signal_generator.py  # Funding rate opportunity detection
│   ├── risk_manager.py      # Risk assessment and sizing
│   ├── execution_handler.py # Order execution
│   ├── balance_monitor.py   # Balance monitoring (renamed from collateral_manager)
│   └── models.py            # Data models (Order, Position, etc.)
├── utils/
│   ├── __init__.py
│   ├── config.py            # Configuration handling
│   ├── logging_config.py    # Logging setup
│   └── state_manager.py     # State persistence
├── config/
│   ├── __init__.py
│   ├── settings.py          # Settings loader
│   └── config.yaml          # Configuration file
├── tests/
│   ├── unit/                # Unit tests
│   ├── integration/         # Integration tests
│   └── failure/             # Failure scenario tests
├── main.py                  # Main entry point
└── requirements.txt
```

### 1.2 Configuration Setup

1. Create a basic `config.yaml` file:
```yaml
# General settings
general:
  log_level: INFO
  state_file: state.json
  state_backup_directory: state_backups  # Directory for state backups
  state_save_interval: 300  # seconds
  state_backup_count: 5     # Number of previous state files to keep

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
    rate_limits:
      default_rate: 1.0
      default_bucket: 5
      endpoints:
        "POST:/exchange": { rate: 5.0, bucket: 10 }
        "POST:/info": { rate: 10.0, bucket: 20 }
  
  backpack:
    enabled: true
    base_url: https://api.backpack.exchange
    ws_url: wss://ws.backpack.exchange
    symbols:
      - BTC_USDC
      - ETH_USDC
      - SOL_USDC
    rate_limits:
      default_rate: 1.0
      default_bucket: 5
      endpoints:
        "GET:/api/v1/ticker": { rate: 10.0, bucket: 20 }

# Strategy parameters
strategy:
  funding_rate:
    min_funding_differential: 0.0001  # Minimum difference to consider
    min_profit_threshold: 5.0  # Min USD profit after fees
    funding_sample_period: 3600  # seconds
    funding_sample_count: 24  # Number of samples to keep
    rebalance_threshold: 0.05  # 5% threshold for rebalancing

# Risk parameters
risk:
  max_position_size: 1000.0  # USD
  max_total_exposure: 5000.0  # USD
  kelly_fraction: 0.5  # Conservative Kelly criterion
  max_collateral_per_exchange: 0.8  # 80% max on any exchange
  max_leverage: 5.0  # Maximum allowed leverage
  min_liquidation_buffer: 0.2  # 20% buffer from liquidation price

# Execution parameters
execution:
  max_slippage: 0.002  # 0.2% max allowed slippage
  max_retries: 3  # Maximum retry attempts for failed API calls
  retry_delay_base: 1.0  # Base delay for exponential backoff (seconds)
  circuit_breaker:
    loss_threshold: 0.05  # 5% of total capital
    failed_trades: 3  # Number of consecutive failed trades

# Balance parameters
balance:
  min_usdc_balance: 50.0  # Minimum USDC balance to maintain
  low_balance_threshold: 100.0  # Threshold for low balance alerts
```

2. Create a secure way to handle API secrets (`.env` file or secure storage)

## 2. Implementation Order

### 2.1 Phase 1: Core Components

1. **API Clients**:
   - Implement the base `ExchangeAPI` abstract class
   - Implement concrete API clients for Hyperliquid and Backpack
   - Focus on core methods: get_funding_rates, get_balances, place_order
   - Implement comprehensive error handling and rate limiting

2. **Data Handler**:
   - Implement WebSocket connections for real-time data
   - Implement market data collection and storage
   - Implement data validation and normalization
   - Add data timestamp tracking and staleness detection

3. **Portfolio Tracker**:
   - Implement balance tracking across exchanges
   - Implement position tracking with P&L calculation
   - Implement order status tracking
   - Add regular reconciliation with exchange data

4. **State Manager**:
   - Implement atomic state persistence with validation
   - Implement state restoration with integrity checks
   - Implement state backup rotation
   - Add corruption detection and recovery mechanisms

5. **Basic Main Loop**:
   - Implement component initialization
   - Implement simple run loop
   - Implement graceful shutdown
   - Add signal handling for clean termination

### 2.2 Phase 2: Strategy Components

6. **Signal Generator**:
   - Implement funding rate differential calculation
   - Implement basic opportunity identification
   - Implement opportunity ranking
   - Add signal validation and anomaly detection

7. **Risk Manager**:
   - Implement concrete position sizing logic
   - Implement exposure checks with hard limits
   - Implement liquidation risk assessment
   - Enforce maximum leverage constraints

8. **Execution Handler**:
   - Implement sequential order execution with atomicity handling
   - Implement comprehensive error recovery for multi-leg trades
   - Implement circuit breakers for critical failures
   - Add detailed execution state tracking

9. **Balance Monitor**:
   - Implement balance checking with alerts
   - Implement threshold monitoring
   - Add detailed balance change logging

### 2.3 Phase 3: Testing and Refinement

10. **Unit Testing**:
    - Implement API client tests with mocked responses
    - Test critical calculation functions
    - Test state persistence mechanisms
    
11. **Integration Testing**:
    - Test data flow between components
    - Test complete execution flow with small values
    - Test state persistence and recovery
    
12. **Failure Scenario Testing**:
    - Implement tests for network failures
    - Test recovery mechanisms
    - Verify checksum validation works
    - Test backup rotation and recovery
    - Verify alerts are generated for corruption

## 3. Critical Implementation Details

### 3.1 API Client Implementation

Focus on implementing the authentication and core API methods correctly:

1. **Authentication**:
   - For Hyperliquid: Implement EIP-712 signing using library
   - For Backpack: Implement ED25519 signing according to docs
   - Always validate authentication success

2. **Error Handling**:
   - Map HTTP error codes to specific exception types
   - Implement retries with exponential backoff
   - Handle specific error responses from each exchange
   - Log detailed error information with context
   - Add network failure detection and recovery
   - Implement connection timeouts and circuit breakers

3. **Rate Limiting**:
   - Implement token bucket algorithm for limiting requests
   - Configure rate limits for each endpoint based on exchange docs
   - Add dynamic rate limit adjustment based on response headers
   - Implement proper waiting when limits are approached

### 3.2 Data Handler Implementation

Focus on reliable data collection and validation:

1. **WebSocket Management**:
   - Implement reconnection logic with exponential backoff
   - Handle sudden disconnections gracefully
   - Implement keep-alive mechanism (ping/pong)
   - Buffer messages to prevent data loss during reconnection
   - Validate all incoming data before processing

2. **Data Validation**:
   - Check data against expected schema
   - Implement timestamp tracking for all data points
   - Detect and handle stale data (older than threshold)
   - Normalize data from different sources into consistent formats
   - Add anomaly detection for extreme values

### 3.3 Portfolio Tracker Implementation

Focus on accurate position and balance tracking:

1. **State Management**:
   - Implement thread-safe state updates with proper locking
   - Keep atomic state snapshots for consistent views
   - Add change tracking to detect unexpected modifications
   - Implement proper event ordering for causally related updates

2. **Reconciliation**:
   - Implement periodic reconciliation with exchange data (every N minutes)
   - Calculate and log discrepancies between local and exchange state
   - Add automatic correction of minor discrepancies
   - Generate alerts for significant state inconsistencies
   - Maintain audit log of all reconciliation actions

### 3.4 State Manager Implementation

Focus on reliable state persistence and recovery:

1. **Atomic Persistence**:
   - Write state to temporary file first
   - Calculate checksum for the state data
   - Verify write completed successfully
   - Rename to target file (atomic file system operation)
   - Add file locking to prevent concurrent modifications

2. **State Backup**:
   - Keep N previous state files in rotation
   - Store backups with timestamps in filenames
   - Implement garbage collection for old backups
   - Add periodically scheduled deep backups

3. **Recovery Mechanism**:
   - Validate checksums when loading state
   - If primary state is corrupted, try backup files in order
   - Log detailed information about recovery attempts
   - Add recovery verification steps
   - Implement emergency mode when all state files are corrupted

### 3.5 Execution Handler Implementation

Focus on reliable order execution and recovery:

1. **Atomic Execution**:
   - Implement multi-leg trade execution with atomicity handling
   - Track execution state of each leg explicitly
   - If one leg fails, attempt compensation strategy:
     a. Try to reverse already executed legs
     b. Log detailed alerts for manual intervention if reversal fails
     c. Update portfolio state to reflect partial execution
   - Maintain timeout-based safety mechanisms

2. **Circuit Breakers**:
   - Implement loss-based circuit breakers (stop if losses exceed threshold)
   - Add failure count circuit breakers (stop after N consecutive failures)
   - Implement market volatility circuit breakers
   - Add exchange-specific circuit breakers (e.g., API errors)
   - Create alerts for all circuit breaker activations

## 4. Testing Approach

### 4.1 Unit Testing

1. **API Client Testing**:
   - Create mock HTTP responses for all API endpoints
   - Test authentication mechanisms with known inputs/outputs
   - Test error handling with various error responses
   - Test rate limiting behavior under load
   - Verify data parsing and normalization

2. **Component Testing**:
   - Test each component in isolation with mocked dependencies
   - Verify calculations with known inputs/outputs
   - Test state transitions and edge cases
   - Verify thread safety with concurrent operations
   - Test logging and error handling

### 4.2 Integration Testing

1. **Component Integration**:
   - Test interaction between components
   - Verify data flow through the system
   - Test end-to-end execution paths
   - Verify state consistency across components
   - Test configuration integration

2. **Live API Testing**:
   - Test against exchange APIs with minimal operations
   - Verify auth mechanism works with real exchanges
   - Test WebSocket connectivity with real data
   - Validate rate limiting works in practice
   - Verify data formats match expectations

### 4.3 Failure Testing

1. **Network Failures**:
   - Simulate connection drops during critical operations
   - Test reconnection mechanisms
   - Verify retry logic works correctly
   - Test timeout handling
   - Verify circuit breakers activate appropriately

2. **State Corruption**:
   - Deliberately corrupt state files
   - Test recovery mechanisms
   - Verify checksum validation works
   - Test backup rotation and recovery
   - Verify alerts are generated for corruption

3. **Execution Failures**:
   - Simulate partial fills for orders
   - Test order cancellation during execution
   - Simulate one leg of a trade failing
   - Test compensation mechanisms
   - Verify portfolio state remains consistent
   - Test circuit breaker activation

4. **API Failures**:
   - Simulate various API error responses
   - Test handling of rate limit errors
   - Simulate authentication failures
   - Test handling of unexpected response formats
   - Verify system degrades gracefully

### 4.4 Manual Testing

1. **Testnet Testing**:
   - Deploy on testnets if available
   - Run complete trading cycles with small values
   - Verify all components function correctly
   - Test failure scenarios manually

2. **Controlled Mainnet Testing**:
   - Run with minimal values on mainnet
   - Monitor all operations closely
   - Verify execution and state management
   - Test emergency stop mechanisms

## 5. Implementation Roadmap

### Week 1: Foundation
- Project setup
- API client implementation
- Basic WebSocket connection
- State manager implementation

### Week 2: Core Components
- Data handler implementation
- Portfolio tracker implementation
- Basic signal generator
- Initial risk manager

### Week 3: Execution Flow
- Execution handler with atomicity
- Balance monitor implementation
- Integration of components
- Basic end-to-end testing

### Week 4: Testing and Refinement
- Unit and integration testing
- Failure scenario testing
- Bug fixes and improvements
- Documentation
- Controlled mainnet testing

## 6. Implementation Tips

1. **Start Small**: Begin with a single pair on each exchange
2. **Manual Verification**: Verify API responses manually before automating
3. **Fail Early**: Implement strict validation to catch issues early
4. **Log Everything**: Keep detailed logs with context for debugging
5. **State Validation**: Verify state consistency after every significant operation
6. **Atomic Operations**: Design all critical operations to be atomic or compensable
7. **Defense in Depth**: Add multiple layers of protection against failures
8. **Assume APIs Will Lie**: Always validate responses, never trust exchange data blindly
9. **Expect Disconnections**: Design all networking code to handle disconnections gracefully
10. **Test Unhappy Paths**: Focus testing on failure scenarios, not just the happy path
11. **Security First**: Secure API keys and never commit secrets to version control 
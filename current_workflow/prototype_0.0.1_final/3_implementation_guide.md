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
   - **For Backpack specifically**:
     - Implement aggressive error recovery and retry mechanisms
     - Add explicit validation layers for all critical responses
     - Implement multi-source data reconciliation
     - Add comprehensive logging for all API interactions
     - Start with read-only operations during validation phase

2. **Data Handler**:
   - Implement WebSocket connections for real-time data
   - Implement market data collection and storage
   - Implement data validation and normalization
   - Add data timestamp tracking and staleness detection
   - **For Backpack specifically**:
     - Implement separate validation tracking for funding rate calculations
     - Create a dedicated module to compare calculated vs actual funding payments
     - Store all raw API responses for post-analysis
     - Implement the Hyperliquid-Perp vs Backpack-Spot strategy as primary approach

3. **Portfolio Tracker**:
   - Implement balance tracking across exchanges
   - Implement position tracking with P&L calculation
   - Implement order status tracking
   - Add regular reconciliation with exchange data
   - **For Backpack specifically**:
     - Implement multi-source position verification
     - Use order fills as a cross-check against reported positions
     - Add automatic alerts for any position discrepancies
     - Implement "safe mode" triggers for inconsistent data

4. **State Manager**:
   - Implement atomic state persistence with validation
   - Implement state restoration with integrity checks
   - Implement state backup rotation
   - Add corruption detection and recovery mechanisms
   - **Enhanced for reliability**:
     - Implement detailed execution state tracking
     - Add explicit tracking of partial trade execution state
     - Store comprehensive recovery metadata
     - Implement transaction logging for all state changes

5. **Basic Main Loop**:
   - Implement component initialization
   - Implement simple run loop
   - Implement graceful shutdown
   - Add signal handling for clean termination
   - **Enhanced for safety**:
     - Implement per-exchange circuit breakers and safe mode
     - Add progressive trading starting with minimal sizes
     - Implement strict initialization validation for all components
     - Add separate validation loops for experimental features

### 2.2 Phase 2: Strategy Components

6. **Signal Generator**:
   - Implement funding rate differential calculation
   - Implement basic opportunity identification
   - Implement opportunity ranking
   - Add signal validation and anomaly detection
   - **For 0.0.1 Focus**:
     - Prioritize the Hyperliquid-Perp vs Backpack-Spot strategy initially
     - Implement separate experimental tracking for Backpack-Perp strategies
     - Add extensive logging and validation for all signal generation steps
     - Implement signal strength thresholds specific to each exchange

7. **Risk Manager**:
   - ~~Implement concrete position sizing logic~~ Implement simplified fixed-fraction position sizing
   - Implement exposure checks with hard limits
   - ~~Implement liquidation risk assessment~~ Implement conservative maximum leverage limits
   - Enforce maximum leverage constraints
   - **Simplified for 0.0.1**:
     - Focus on implementing hard caps first:
       - Maximum USD per position
       - Maximum total exposure percentage
       - Maximum leverage limits
       - Maximum exchange concentration limits
     - Use simple fixed fraction sizing (e.g., 5% of capital)
     - Defer complex Kelly criterion and VaR calculations
     - Implement stricter limits for Backpack specifically

8. **Execution Handler**:
   - Implement sequential order execution with atomicity handling
   - Implement comprehensive error recovery for multi-leg trades
   - Implement circuit breakers for critical failures
   - Add detailed execution state tracking
   - **Enhanced for robustness**:
     - Implement exchange-specific circuit breakers
     - Add critical alerts and automatic trading pause for failed reversal orders
     - Implement detailed state recording for manual intervention
     - For Backpack specifically, implement additional verification steps
     - Start with smaller sizes on Backpack until validation metrics meet targets

9. **Balance Monitor**:
   - Implement balance checking with alerts
   - Implement threshold monitoring
   - Add detailed balance change logging
   - **Enhanced for safety**:
     - Implement "safe mode" when balance discrepancies are detected
     - Add verification of expected vs actual balance changes after trades
     - Implement progressive exposure limits based on validation metrics
     - Add reconciliation between portfolio tracker and balance monitor

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
   - **For Backpack specifically**:
     - Test with a wider range of error conditions and edge cases
     - Validate the funding rate calculation logic with known examples
     - Test position reconstruction from multiple data sources
     - Verify reconciliation mechanisms work correctly

2. **Component Testing**:
   - Test each component in isolation with mocked dependencies
   - Verify calculations with known inputs/outputs
   - Test state transitions and edge cases
   - Verify thread safety with concurrent operations
   - Test logging and error handling
   - **Enhanced for reliability**:
     - Test state persistence with corrupted/incomplete data
     - Verify recovery mechanisms function correctly
     - Test component behavior during network interruptions
     - Verify circuit breakers activate appropriately

3. **Signal Generator Testing**:
   - Test NFD calculation with known funding rates
   - Verify basis volatility calculation with historical data
   - Test expected profit calculation with various cost scenarios
   - Validate utility function calculation against expected rankings
   - Test signal filtering logic with edge cases
   - Verify signal anomaly detection with outlier data
   - **For Hybrid Strategies**:
     - Test the Hyperliquid-Perp vs Backpack-Spot strategy specifically
     - Validate profit calculations with realistic fees and spreads
     - Test signal generation with historical data from both exchanges

4. **Risk Manager Testing**:
   - ~~Test Kelly position sizing with various profit/risk scenarios~~
   - Test fixed-fraction position sizing with various capital levels
   - Verify position constraints are applied correctly
   - ~~Test VaR calculation with known portfolio compositions~~
   - ~~Validate dynamic VaR adjustment with changing market volatility~~
   - Test exposure limit enforcement across exchanges
   - Verify leverage checks with margin requirements
   - **For 0.0.1 Simplified Approach**:
     - Test hard caps with various portfolio configurations
     - Verify exchange-specific limits are applied correctly
     - Test behavior when multiple limits apply simultaneously
     - Validate that stricter limits apply for Backpack-related trades

### 4.2 Integration Testing

1. **Component Integration**:
   - Test interaction between components
   - Verify data flow through the system
   - Test end-to-end execution paths
   - Verify state consistency across components
   - Test configuration integration
   - **For multi-exchange operations**:
     - Test with both exchanges active simultaneously
     - Verify isolation of exchange-specific issues
     - Test failover mechanisms when one exchange has problems
     - Verify circuit breakers don't affect unrelated exchanges

2. **Live API Testing**:
   - Test against exchange APIs with minimal operations
   - Verify auth mechanism works with real exchanges
   - Test WebSocket connectivity with real data
   - Validate rate limiting works in practice
   - Verify data formats match expectations
   - **For Backpack specifically**:
     - Start with read-only operations only
     - Implement extensive validation before any trading
     - Maintain parallel tracking of expected vs actual results
     - Implement progressive testing from minimal operations to full functionality

### 4.3 Failure Testing

1. **Network Failures**:
   - Simulate connection drops during critical operations
   - Test reconnection mechanisms
   - Verify retry logic works correctly
   - Test timeout handling
   - Verify circuit breakers activate appropriately
   - **For multi-exchange scenarios**:
     - Test with failures affecting only one exchange
     - Verify operations continue for unaffected exchanges
     - Test recovery when connectivity is restored

2. **State Corruption**:
   - Deliberately corrupt state files
   - Test recovery mechanisms
   - Verify checksum validation works
   - Test backup rotation and recovery
   - Verify alerts are generated for corruption
   - **Enhanced Validation**:
     - Test recovery from partial corruption
     - Verify transaction log can be used for recovery
     - Test with various corruption patterns
     - Validate that no invalid state is ever used for trading

3. **Execution Failures**:
   - Simulate partial fills for orders
   - Test order cancellation during execution
   - Simulate one leg of a trade failing
   - Test compensation mechanisms
   - Verify portfolio state remains consistent
   - Test circuit breaker activation
   - **Enhanced for robustness**:
     - Test scenarios where compensation orders also fail
     - Verify trading stops for affected pairs when reversal fails
     - Test manual intervention recovery process
     - Validate alerts are generated with appropriate urgency

4. **API Failures**:
   - Simulate various API error responses
   - Test handling of rate limit errors
   - Simulate authentication failures
   - Test handling of unexpected response formats
   - Verify system degrades gracefully
   - **For Backpack specifically**:
     - Test with Backpack-specific error scenarios
     - Verify "safe mode" activates appropriately
     - Test reconciliation mechanisms during API inconsistency
     - Validate fallback to alternative data sources when appropriate

### 4.4 Manual Testing

1. **Testnet Testing**:
   - Deploy on testnets if available
   - Run complete trading cycles with small values
   - Verify all components function correctly
   - Test failure scenarios manually
   - **Progressive Deployment**:
     - Start with Hyperliquid-only operations
     - Add Backpack read-only operations next
     - Finally add Backpack trading with minimal sizes

2. **Controlled Mainnet Testing**:
   - Run with minimal values on mainnet
   - Monitor all operations closely
   - Verify execution and state management
   - Test emergency stop mechanisms
   - **Progressive Rollout**:
     - Begin with Hyperliquid-only operations
     - Add Backpack spot price data collection
     - Implement Hyperliquid-Perp vs Backpack-Spot with small sizes
     - Only proceed to Backpack-Perp strategies after extensive validation

### 4.5 Funding Rate Model Validation

A critical aspect of the funding rate arbitrage strategy is validating that our funding rate calculations match the actual behavior of exchanges.

1. **Historical Data Analysis**:
   - Collect historical funding rate data from Hyperliquid and Backpack APIs
   - Compare actual funding payments with predicted values from our models
   - Calculate error metrics (RMSE, MAE) for model accuracy
   - Identify systematic biases or patterns in prediction errors
   - **For Backpack specifically**:
     - Maintain separate validation metrics for direct API vs calculated approach
     - Implement continuous comparison between calculated and actual funding
     - Set specific accuracy thresholds before relying on calculations

2. **Formula Calibration**:
   - Calibrate dampening factors and other parameters in funding rate formulas
   - Adjust estimation models based on observed behavior
   - Implement exchange-specific tweaks to improve accuracy
   - Document exchange-specific quirks and adjustments
   - **For Backpack specifically**:
     - Start with conservative estimates that err on the side of caution
     - Gradually refine parameters as more data is collected
     - Maintain multiple model versions and compare performance

3. **Market Condition Impact**:
   - Test funding rate models during different market conditions (high/low volatility)
   - Verify formulas perform consistently during market stress
   - Identify market conditions where models may be less reliable
   - Implement conditional adjustments for different market regimes
   - **For cross-exchange strategies**:
     - Analyze correlation between exchanges during various market conditions
     - Identify scenarios where correlations break down
     - Implement additional safeguards for volatile periods

4. **Live Validation Pipeline**:
   - Create a continuous validation process that compares predicted vs actual funding
   - Implement automatic alerts for significant prediction errors
   - Track model performance over time with metrics dashboards
   - Establish thresholds for when models need recalibration
   - **For experimental features**:
     - Implement shadow mode tracking with no actual trading
     - Compare potential profits vs actual results
     - Gradually promote validated approaches to active use

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

## 7. Strategy Testing Specifics

For testing the funding rate arbitrage strategy, we'll focus on key validation steps:

### 7.1 Funding Rate Prediction Accuracy

The ability to accurately predict funding rates is critical for opportunity assessment:

```python
# Example validation routine pseudocode
def validate_funding_predictions(historical_data, prediction_model, days=30):
    """
    Validate funding rate prediction accuracy against historical data.
    
    Parameters:
    - historical_data: DataFrame with actual funding rates
    - prediction_model: Function that predicts funding rates
    - days: Number of days to test
    
    Returns:
    - Accuracy metrics
    """
    predictions = []
    actuals = []
    
    for day in range(days):
        # Get data up to this point
        training_data = historical_data[:day]
        
        # Predict next funding rate
        predicted = prediction_model(training_data)
        predictions.append(predicted)
        
        # Record actual funding rate
        actual = historical_data[day]
        actuals.append(actual)
    
    # Calculate error metrics
    rmse = calculate_rmse(predictions, actuals)
    mae = calculate_mae(predictions, actuals)
    
    return {
        "rmse": rmse,
        "mae": mae,
        "predictions": predictions,
        "actuals": actuals
    }
```

### 7.2 NFD Strategy Testing

Testing the Net Funding Differential strategy with historical data:

```python
# Example NFD strategy backtest pseudocode
def backtest_nfd_strategy(historical_data, params):
    """
    Backtest NFD strategy with historical data.
    
    Parameters:
    - historical_data: DataFrame with price and funding rate data
    - params: Strategy parameters (thresholds, position sizing, etc.)
    
    Returns:
    - Strategy performance metrics
    """
    portfolio = initialize_portfolio(params["initial_capital"])
    trades = []
    
    for timestamp, data in historical_data.iterrows():
        # Calculate NFD
        nfd = data["funding_rate_exchange_A"] - data["funding_rate_exchange_B"]
        
        # Calculate basis volatility
        basis_vol = calculate_rolling_volatility(
            historical_data["basis"][:timestamp], 
            params["volatility_window"]
        )
        
        # Calculate costs
        costs = estimate_trading_costs(data, params)
        
        # Calculate expected profit
        expected_profit = nfd * params["position_size"] - costs
        
        # Calculate utility
        utility = expected_profit - params["lambda"] * basis_vol**2
        
        # Trading logic
        if abs(nfd) > params["min_nfd_threshold"] and utility > params["min_utility"]:
            # Execute trade
            trade = execute_simulated_trade(portfolio, data, nfd, params)
            trades.append(trade)
        
        # Apply funding payments
        apply_funding_payments(portfolio, data, params)
        
        # Update portfolio value
        update_portfolio_value(portfolio, data)
    
    # Calculate performance metrics
    metrics = calculate_performance_metrics(portfolio, trades)
    
    return {
        "portfolio": portfolio,
        "trades": trades,
        "metrics": metrics
    }
```

These testing frameworks will be implemented to validate both the mathematical models and the strategy implementation. For Prototype 0.0.1, we'll focus on basic validation with historical data, expanding to more sophisticated testing in future versions. 
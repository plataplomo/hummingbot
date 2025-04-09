# CyberDeltaEngine - Core Architecture

## System Overview

CyberDeltaEngine is a cross-exchange delta-neutral trading system optimized for funding rate arbitrage. The system connects to multiple cryptocurrency exchanges (currently Hyperliquid, Backpack, with plans for Paradex), monitors funding rates, and executes delta-neutral strategies based on funding rate differentials.

## Core Components

### 1. Exchange Integration Layer

The exchange integration layer consists of:

1. **API Clients**: Low-level implementations for each exchange API
   - `HyperliquidAPI` - Uses EIP-712 signatures for authentication
   - `BackpackAPI` - Uses ED25519 signatures for authentication
   - `ParadexAPI` (planned) - Will use wallet signatures

2. **Exchange Adapters**: Higher-level abstraction that provides a unified interface
   - `HyperliquidAdapter` - Adapts Hyperliquid's unique API structure
     - Handles hourly funding rate payments
     - Supports HIP-1 and HIP-2 (Hyperliquidity) token standards
   - `BackpackAdapter` - Adapts Backpack's API conventions
     - Handles microsecond precision in WebSocket events vs millisecond in REST
     - Implements the new WebSocket protocol and stream formats
   - Each adapter handles exchange-specific logic, error handling, and data normalization

### 2. Core Strategy Layer

1. **Funding Rate Arbitrage**:
   - Monitors funding rates across exchanges
   - Identifies opportunities where funding rate differentials exceed thresholds
   - Calculates optimal position sizes using Kelly criterion
   - Manages entry and exit timing for positions

2. **Funding Rate Calculation**:
   - **Direct Calculation**: Uses exchange-provided funding rates when available
   - **Mark-Index Calculation**: Calculates funding rates from mark/index price differentials
   - **Cross-Exchange Calculation**: Implements spot vs perp on different exchanges
   - **Validation System**: Compares predicted vs actual funding payments

3. **Position Management**:
   - Tracks open positions across exchanges
   - Manages hedged positions for delta-neutrality
   - Implements risk controls including stop-loss and maximum exposure
   - Uses WebSocket streams for real-time position updates

4. **Risk Management**:
   - Dynamic position sizing based on volatility
   - Portfolio-level risk controls
   - Defensive error handling for all exchange operations
   - Exchange-specific risk parameters

### 3. Data Collection and Analysis

1. **Market Data Collection**:
   - Historical funding rates
   - Price data for supported assets
   - Order book depth and liquidity metrics
   - Real-time WebSocket streams with proper reconnection handling

2. **Performance Analysis**:
   - Strategy performance tracking
   - Parameter optimization feedback loop
   - Risk-adjusted return calculations
   - Funding rate prediction validation

### 4. System Infrastructure

1. **Configuration System**:
   - YAML-based configuration
   - Environment variables for secrets
   - Feature flags for controlled rollout
   - Exchange-specific parameters

2. **Logging and Monitoring**:
   - Comprehensive logging with configurable verbosity
   - Performance metrics collection
   - Error reporting and alerting
   - API response validation and discrepancy detection

3. **Execution Engine**:
   - Asynchronous execution using Python asyncio
   - Rate-limiting for API compliance
   - Fault-tolerant operation with retry mechanisms
   - Atomic transaction handling for multi-leg trades

## Data Flow

1. **Initialization**:
   - Load configuration and secrets
   - Connect to exchange APIs
   - Initialize exchange adapters
   - Establish WebSocket connections

2. **Market Data Ingestion**:
   - Poll funding rates from all exchanges 
   - Subscribe to price updates via WebSockets
   - Collect historical data for analysis
   - Maintain time-series data with proper timestamp handling

3. **Strategy Execution**:
   - Identify arbitrage opportunities
   - Calculate position sizes and expected returns
   - Execute trades with appropriate risk controls
   - Implement circuit breakers for safety

4. **Position Monitoring**:
   - Track position performance
   - Apply stop-loss or take-profit rules
   - Close positions when conditions change
   - Multi-source position verification

## Implementation Details

### Key Technologies

1. **Python 3.13+** as the core language
2. **Asyncio** for non-blocking I/O operations
3. **ED25519 and EIP-712 Signatures** for exchange authentication
4. **Pandas/NumPy** for data analysis
5. **SQLite/PostgreSQL** for data persistence (planned)

### Design Patterns

1. **Adapter Pattern** for exchange-specific implementations
2. **Strategy Pattern** for various trading algorithms
3. **Repository Pattern** for data access
4. **Factory Pattern** for creating exchange-specific components
5. **Circuit Breaker Pattern** for fault tolerance

### Exchange-Specific Considerations

1. **Hyperliquid**:
   - Hourly funding payments (vs 8-hour for most exchanges)
   - EIP-712 signature authentication
   - Support for HIP-2 (Hyperliquidity) which provides automated market making
   - Mark price is based on impact bid/ask prices

2. **Backpack**:
   - ED25519 signature authentication
   - Microsecond precision in WebSocket events (milliseconds in REST)
   - New WebSocket API format (.stream instead of @stream)
   - Updated order ID format (no longer timestamp-based)
   - Funding rate API endpoints for rates, predicted rates, and history

## Future Enhancements

1. **Expansion to Additional Exchanges**:
   - Support for more derivative exchanges
   - Cross-exchange funding rate arbitrage

2. **Advanced Strategies**:
   - Multi-leg arbitrage strategies
   - Market making with funding rate bias
   - Volatility-based strategies
   - HIP-2 Hyperliquidity integration

3. **Infrastructure Improvements**:
   - Distributed execution for higher throughput
   - Real-time monitoring dashboard
   - Automated parameter optimization 
   - ML-enhanced funding rate prediction 
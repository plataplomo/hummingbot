# Implementation Roadmap - Prototype 0.0.1

This document outlines specific implementation tasks for each component, including function details, API endpoints, and algorithmic changes needed to complete the prototype 0.0.1 milestone.

## 1. Hyperliquid API Client

### Current State
Basic structure exists with placeholder methods in `apis/hyperliquid.py`.

### Required Implementation

#### Authentication & Signing
```python
def _sign_request(self, method: str, endpoint: str, data: dict) -> dict:
    """Sign a request using the wallet private key."""
    # 1. Create an EIP-712 compatible message
    # 2. Sign the message using eth_account.Account.sign_message
    # 3. Return the signature as part of the request payload
```

#### Market Data Endpoints
```python
async def fetch_ticker(self, symbol: str) -> models.Ticker:
    """Fetch current ticker data for a symbol."""
    # Implement using POST to /info endpoint
    
async def fetch_orderbook(self, symbol: str, depth: int = 20) -> models.OrderBook:
    """Fetch order book for a symbol."""
    # Implement using POST to /info endpoint
    
async def fetch_funding_rate(self, symbol: str) -> float:
    """Fetch current funding rate for a symbol."""
    # Fully implement using correct POST to /info endpoint
```

#### Account Endpoints
```python
async def get_balances(self) -> list[models.Balance]:
    """Fetch current user balances."""
    # Implement using POST to /info endpoint with signing
    
async def get_positions(self) -> list[models.Position]:
    """Fetch current user positions."""
    # Implement using POST to /info endpoint with signing
    
async def place_order(self, order: models.Order) -> str:
    """Place an order on the exchange."""
    # Implement using POST to /exchange endpoint with signing
    
async def cancel_order(self, order_id: str) -> bool:
    """Cancel an order on the exchange."""
    # Implement using POST to /exchange endpoint with signing
```

#### WebSocket Implementation
```python
async def _setup_user_ws(self):
    """Set up user-specific WebSocket connection."""
    # 1. Connect to user-specific WS endpoint
    # 2. Sign the connection
    # 3. Subscribe to user events
    
async def _handle_orderbook_message(self, msg: dict):
    """Parse orderbook WebSocket message."""
    # Implement proper parsing logic for the WS format
    
async def _handle_ticker_message(self, msg: dict):
    """Parse ticker WebSocket message."""
    # Implement proper parsing logic for the WS format
    
async def _handle_trade_message(self, msg: dict):
    """Parse trade WebSocket message."""
    # Implement proper parsing logic for the WS format
    
async def _handle_user_update(self, msg: dict):
    """Parse user update WebSocket message."""
    # Implement proper parsing logic for user events
```

## 2. Data Handler

### Current State
Basic structure in `core/data_handler.py` with placeholder message handling.

### Required Implementation

#### WebSocket Management
```python
async def subscribe_to_streams(self):
    """Subscribe to all required WebSocket streams."""
    # 1. Subscribe to correct topics for each exchange
    # 2. Set up error handling and reconnection logic
    
async def _process_message(self, exchange: str, message: dict):
    """Process incoming WebSocket messages."""
    # 1. Implement proper message routing based on type
    # 2. Update internal data structures
    # 3. Notify any waiting components
```

#### Data Access Methods
```python
def get_ticker(self, exchange: str, symbol: str) -> Optional[models.Ticker]:
    """Get the latest ticker for a symbol on an exchange."""
    # Return from cached data with thread safety
    
def get_orderbook(self, exchange: str, symbol: str) -> Optional[models.OrderBook]:
    """Get the latest order book for a symbol on an exchange."""
    # Return from cached data with thread safety
    
def get_funding_rate(self, exchange: str, symbol: str) -> Optional[float]:
    """Get the latest funding rate for a symbol on an exchange."""
    # Return from cached data with thread safety
    
def get_last_price(self, exchange: str, symbol: str) -> Optional[float]:
    """Get the last traded price for a symbol on an exchange."""
    # Return from cached data with thread safety
```

## 3. Portfolio Tracker

### Current State
Basic structure in `core/portfolio_tracker.py` with placeholder update methods.

### Required Implementation

#### State Management
```python
async def load_initial_state(self):
    """Load initial state from exchanges."""
    # 1. Fetch balances from all exchanges
    # 2. Fetch positions from all exchanges
    # 3. Fetch open orders from all exchanges
    # 4. Update internal state dictionaries
    
async def _update_from_ws(self, exchange: str, message: dict):
    """Update state from WebSocket messages."""
    # 1. Parse message based on type (balance, position, order)
    # 2. Update relevant state dictionaries
    # 3. Calculate PnL if needed
```

#### Position Calculations
```python
def calculate_position_pnl(self, exchange: str, symbol: str) -> tuple[float, float]:
    """Calculate realized and unrealized PnL for a position."""
    # 1. Get current position details
    # 2. Get current market price from DataHandler
    # 3. Calculate unrealized PnL
    # 4. Return (realized_pnl, unrealized_pnl)
    
def calculate_portfolio_value(self) -> float:
    """Calculate total portfolio value across all exchanges."""
    # 1. Sum balance values
    # 2. Add unrealized PnL from positions
    # 3. Return total value
```

#### History Tracking
```python
def record_historical_state(self):
    """Record current state for historical tracking."""
    # 1. Calculate key metrics (portfolio value, position sizes)
    # 2. Add to historical data structure
    # 3. Implement pruning if needed
```

## 4. Signal Generator

### Current State
Basic structure in `core/signal_generator.py` with placeholder calculations.

### Required Implementation

#### NFD Calculation
```python
def _calculate_nfd(self, exchange1: str, exchange2: str, symbol: str) -> float:
    """Calculate Normalized Funding Delta between exchanges."""
    # 1. Get funding rates from both exchanges via DataHandler
    # 2. Apply normalization formula: (funding1 - funding2) / timeframe
    # 3. Return value as annualized percentage
```

#### Cost Estimation
```python
def _estimate_costs(
    self, exchange1: str, exchange2: str, symbol: str, size: float
) -> float:
    """Estimate execution costs for a potential opportunity."""
    # 1. Calculate exchange fees from config
    # 2. Estimate slippage using orderbook data
    # 3. Add additional fixed costs
    # 4. Return total estimated cost
```

#### Opportunity Ranking
```python
def _calculate_opportunity_utility(
    self, opportunity: models.ArbitrageOpportunity
) -> float:
    """Calculate utility score for ranking opportunities."""
    # 1. Apply the formula: U = NFD - (costs * adjustment)
    # 2. Adjust based on volatility and risk
    # 3. Return utility score
```

#### Market Data Integration
```python
def _fetch_market_data(self, exchange: str, symbol: str) -> dict:
    """Fetch required market data for signal generation."""
    # Replace direct API calls with DataHandler usage:
    # 1. Get funding rates via self.data_handler.get_funding_rate()
    # 2. Get orderbook via self.data_handler.get_orderbook()
    # 3. Get tickers via self.data_handler.get_ticker()
```

## 5. Risk Manager

### Current State
Basic structure in `core/risk_manager.py` with placeholder risk checks.

### Required Implementation

#### Portfolio Risk Calculation
```python
def _calculate_portfolio_var(self, confidence_level: float = 0.95) -> float:
    """Calculate Value at Risk for the current portfolio."""
    # 1. Get current positions from PortfolioTracker
    # 2. Fetch historical volatility data
    # 3. Calculate correlation matrix
    # 4. Apply VaR formula with normalization
    # 5. Return VaR value
```

#### Position Sizing
```python
def _calculate_position_size(
    self, opportunity: models.ArbitrageOpportunity
) -> float:
    """Calculate appropriate position size using Kelly criterion."""
    # 1. Get edge from opportunity NFD
    # 2. Get probability of success (estimated)
    # 3. Apply Kelly formula with fractional multiplier
    # 4. Apply maximum position constraints
    # 5. Return position size
```

#### Pre-Trade Checks
```python
async def _check_margin_requirements(
    self, exchange: str, symbol: str, size: float
) -> bool:
    """Check if margin requirements can be met for a trade."""
    # 1. Get current available margin from PortfolioTracker
    # 2. Calculate required margin for the position
    # 3. Check if sufficient margin exists
    # 4. Return boolean result
    
async def _check_execution_viability(
    self, opportunity: models.ArbitrageOpportunity
) -> bool:
    """Check if opportunity can be executed safely."""
    # 1. Check market liquidity via orderbook depth
    # 2. Check for unusual market conditions
    # 3. Check timing constraints
    # 4. Return boolean result
```

## 6. Execution Handler

### Current State
Basic structure in `core/execution_handler.py` with sequential execution.

### Required Implementation

#### Parallel Execution
```python
async def _execute_parallel_orders(
    self, opportunity: models.ArbitrageOpportunity
) -> dict:
    """Execute orders on both exchanges in parallel."""
    # 1. Prepare orders for both exchanges
    # 2. Start both order placements concurrently with asyncio.gather
    # 3. Implement timeout handling
    # 4. Return execution results
```

#### Order Monitoring
```python
async def _monitor_orders(self, orders: dict) -> dict:
    """Monitor the status of placed orders."""
    # 1. Poll or listen to WebSocket for order updates
    # 2. Check for fills, partial fills, or rejections
    # 3. Implement timeout handling
    # 4. Return final status
```

#### Partial Fill Handling
```python
async def _handle_partial_fills(self, orders: dict) -> None:
    """Handle partial fills by adjusting or cancelling remaining orders."""
    # 1. Check how much was filled on each side
    # 2. Calculate imbalance
    # 3. Either adjust remaining order or place compensating order
    # 4. Update PortfolioTracker
```

#### Error Recovery
```python
async def _handle_execution_error(
    self, error_type: str, failed_orders: dict, filled_orders: dict
) -> None:
    """Handle errors during execution."""
    # 1. Determine error recovery strategy based on error_type
    # 2. For legging risk, place compensating orders
    # 3. For market errors, implement retry logic
    # 4. Log all recovery actions
```

## 7. Testing Infrastructure

### Current State
Minimal or no testing.

### Required Implementation

#### Unit Test Framework
```python
# tests/unit/test_hyperliquid_api.py
def test_sign_request():
    """Test that request signing works correctly."""
    # 1. Create mock private key
    # 2. Create sample request
    # 3. Call _sign_request
    # 4. Verify signature format is correct

# tests/unit/test_signal_generator.py
def test_nfd_calculation():
    """Test NFD calculation with mock data."""
    # 1. Create mock funding rates
    # 2. Call _calculate_nfd
    # 3. Verify result matches expected value

# tests/unit/test_risk_manager.py
def test_position_sizing():
    """Test position sizing with mock opportunities."""
    # 1. Create mock opportunity
    # 2. Call _calculate_position_size
    # 3. Verify size is within expected constraints
```

#### Integration Test Framework
```python
# tests/integration/test_data_handler.py
async def test_websocket_processing():
    """Test that WebSocket messages are processed correctly."""
    # 1. Create mock WebSocket client
    # 2. Send sample messages
    # 3. Verify DataHandler state is updated
    # 4. Verify data access methods return correct values

# tests/integration/test_execution_flow.py
async def test_opportunity_execution():
    """Test the flow from signal to execution."""
    # 1. Create mock opportunity
    # 2. Pass through risk assessment
    # 3. Execute via ExecutionHandler with mock API
    # 4. Verify PortfolioTracker is updated
```

#### Simulation Framework
```python
# tests/simulation/exchange_simulator.py
class ExchangeSimulator:
    """Simulates exchange behavior for testing."""
    # 1. Implement methods matching ExchangeAPI interface
    # 2. Add configurable latency and errors
    # 3. Implement realistic order matching logic
    # 4. Add market data simulation capability

# tests/simulation/market_data_replay.py
class MarketDataReplay:
    """Replays historical market data for testing."""
    # 1. Load data from files or database
    # 2. Implement time-based replay functionality
    # 3. Feed data to components via mocked interfaces
```

## Implementation Sequence

1. **Week 1 (April 8-14)**
   - Complete Hyperliquid API authentication and market data endpoints
   - Refine WebSocket connection and basic message parsing
   - Set up basic testing infrastructure

2. **Week 2 (April 15-21)**
   - Complete Portfolio Tracker with proper state management
   - Enhance Data Handler with efficient data structures
   - Implement core mathematical formulas in Signal Generator

3. **Week 3 (April 22-28)**
   - Implement VaR calculation and position sizing in Risk Manager
   - Develop parallel execution in Execution Handler
   - Add unit tests for core calculations

4. **Week 4 (April 29-May 5)**
   - Implement order monitoring and partial fill handling
   - Add error recovery strategies
   - Develop integration tests

5. **Week 5 (May 6-12)**
   - Build simulation framework for end-to-end testing
   - Implement basic adaptation loop metrics
   - Complete documentation updates

## Technical Debt & Future Considerations

1. **Hyperliquid API Client**
   - Need to handle nonce management for order submission
   - Consider implementing rate limiting with backoff
   - Track API key usage and constraints

2. **Data Handler**
   - Consider implementing a more efficient data structure for high-frequency updates
   - Add data archiving for historical analysis
   - Implement circuit breakers for bad data

3. **Risk Manager**
   - More sophisticated covariance calculation beyond initial implementation
   - Dynamic confidence level adjustment
   - Stress testing for extreme market conditions

4. **Execution Handler**
   - Need more advanced order types as exchanges support them
   - Cross-exchange order routing optimization
   - More sophisticated partial fill handling strategy

5. **Testing**
   - Need more comprehensive API response mocks 
   - Consider property-based testing for mathematical components
   - Add performance testing for high-throughput scenarios 
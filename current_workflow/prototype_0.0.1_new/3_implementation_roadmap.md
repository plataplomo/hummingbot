# Implementation Roadmap - Prototype 0.0.1

This document outlines the specific implementation tasks required to complete Prototype 0.0.1 of the CyberDeltaEngine. It details the functions that need to be implemented for each component, including specific API endpoints, algorithms, and data structures.

## Hyperliquid API Client

### Current State
- Basic API client structure defined
- Missing comprehensive endpoint implementation
- WebSocket client needs development

### Required Implementation

#### Authentication
```python
class HyperliquidAuth:
    def __init__(self, private_key: str):
        self.private_key = private_key
        self.address = self._derive_address()
    
    def _derive_address(self) -> str:
        # Implementation to derive ETH address from private key
        pass
    
    def sign_message(self, message: str) -> str:
        # Implementation to sign messages with private key
        pass
    
    def get_signature(self, timestamp: int, data: dict) -> str:
        # Implementation to generate signature for API calls
        pass
```

#### Market Data Endpoints
```python
class HyperliquidPublicAPI:
    def get_funding_rates(self) -> dict:
        # Endpoint: /info
        # Implementation to fetch current funding rates for all markets
        pass
    
    def get_order_book(self, symbol: str, depth: int = 10) -> dict:
        # Endpoint: /orderbook
        # Implementation to fetch order book data for a specific market
        pass
    
    def get_ticker(self, symbol: str = None) -> dict:
        # Endpoint: /ticker
        # Implementation to fetch ticker data for a specific market or all markets
        pass
    
    def get_recent_trades(self, symbol: str) -> list:
        # Endpoint: /trades
        # Implementation to fetch recent trades for a specific market
        pass
```

#### Account Endpoints
```python
class HyperliquidPrivateAPI:
    def __init__(self, auth: HyperliquidAuth):
        self.auth = auth
    
    def get_account_info(self) -> dict:
        # Endpoint: /user
        # Implementation to fetch account information including balances
        pass
    
    def get_positions(self) -> list:
        # Endpoint: /positions
        # Implementation to fetch current open positions
        pass
    
    def place_order(self, symbol: str, side: str, order_type: str, 
                   amount: float, price: float = None, reduce_only: bool = False) -> dict:
        # Endpoint: /order
        # Implementation to place a new order
        pass
    
    def cancel_order(self, order_id: str) -> bool:
        # Endpoint: /cancel
        # Implementation to cancel an existing order
        pass
    
    def cancel_all_orders(self, symbol: str = None) -> bool:
        # Endpoint: /cancelAll
        # Implementation to cancel all open orders
        pass
    
    def modify_order(self, order_id: str, price: float = None, amount: float = None) -> dict:
        # Endpoint: /modifyOrder
        # Implementation to modify an existing order
        pass
```

#### WebSocket Connection
```python
class HyperliquidWebSocket:
    def __init__(self, auth: HyperliquidAuth = None):
        self.auth = auth
        self.ws = None
    
    async def connect(self):
        # Implementation to establish WebSocket connection
        pass
    
    async def subscribe_to_funding(self, callback):
        # Implementation to subscribe to funding rate updates
        pass
    
    async def subscribe_to_orderbook(self, symbol: str, callback):
        # Implementation to subscribe to order book updates
        pass
    
    async def subscribe_to_user_updates(self, callback):
        # Implementation to subscribe to user account updates
        pass
    
    async def handle_message(self, message):
        # Implementation to process incoming WebSocket messages
        pass
    
    async def close(self):
        # Implementation to close WebSocket connection
        pass
```

## Data Handler

### Current State
- Basic structure defined
- Missing WebSocket integration
- Incomplete data access methods

### Required Implementation

```python
class DataHandler:
    def __init__(self, api_client):
        self.api_client = api_client
        self.websocket = None
        self.data_cache = {}
    
    async def initialize(self):
        # Implementation to set up WebSocket connections and initial data fetch
        self.websocket = HyperliquidWebSocket()
        await self.websocket.connect()
        
        # Subscribe to necessary data streams
        await self.websocket.subscribe_to_funding(self._handle_funding_update)
        # Additional subscriptions...
        
        # Initial data fetch
        await self.update_market_data()
    
    async def update_market_data(self):
        # Implementation to fetch and update market data
        funding_rates = await self.api_client.get_funding_rates()
        self.data_cache['funding_rates'] = self._process_funding_rates(funding_rates)
        # Additional data fetching...
    
    def _process_funding_rates(self, raw_funding_data):
        # Implementation to process raw funding rate data
        processed_data = {}
        # Processing logic...
        return processed_data
    
    async def _handle_funding_update(self, message):
        # Implementation to handle funding rate updates from WebSocket
        # Update data_cache with new information
        pass
    
    def get_funding_rates(self, symbols=None):
        # Implementation to retrieve funding rates from cache
        if symbols is None:
            return self.data_cache.get('funding_rates', {})
        else:
            return {s: r for s, r in self.data_cache.get('funding_rates', {}).items() if s in symbols}
    
    def get_ticker_data(self, symbol):
        # Implementation to retrieve ticker data from cache
        return self.data_cache.get('tickers', {}).get(symbol)
    
    def get_order_book(self, symbol):
        # Implementation to retrieve order book data from cache
        return self.data_cache.get('order_books', {}).get(symbol)
    
    async def close(self):
        # Implementation to clean up resources
        if self.websocket:
            await self.websocket.close()
```

## Portfolio Tracker

### Current State
- Basic structure defined
- Missing state management
- Incomplete position calculations

### Required Implementation

```python
class PortfolioTracker:
    def __init__(self, api_client):
        self.api_client = api_client
        self.positions = {}
        self.balances = {}
        self.position_history = []
        self.last_update_time = None
    
    async def initialize(self):
        # Implementation to fetch initial account state
        await self.update_portfolio()
    
    async def update_portfolio(self):
        # Implementation to fetch and update portfolio state
        account_info = await self.api_client.get_account_info()
        positions = await self.api_client.get_positions()
        
        self.balances = self._process_balances(account_info)
        self.positions = self._process_positions(positions)
        self.last_update_time = time.time()
        
        # Record position history
        self._record_position_history()
    
    def _process_balances(self, account_info):
        # Implementation to process account balance information
        processed_balances = {}
        # Processing logic...
        return processed_balances
    
    def _process_positions(self, positions_data):
        # Implementation to process position information
        processed_positions = {}
        # Processing logic...
        return processed_positions
    
    def _record_position_history(self):
        # Implementation to record position history
        history_entry = {
            'timestamp': self.last_update_time,
            'positions': copy.deepcopy(self.positions),
            'balances': copy.deepcopy(self.balances)
        }
        self.position_history.append(history_entry)
        
        # Limit history size
        if len(self.position_history) > 1000:
            self.position_history = self.position_history[-1000:]
    
    def get_position(self, symbol):
        # Implementation to retrieve position information for a specific market
        return self.positions.get(symbol)
    
    def get_balance(self, currency):
        # Implementation to retrieve balance information for a specific currency
        return self.balances.get(currency)
    
    def get_total_portfolio_value(self):
        # Implementation to calculate total portfolio value
        total_value = 0.0
        # Calculation logic...
        return total_value
    
    def get_position_history(self, symbol=None, start_time=None, end_time=None):
        # Implementation to retrieve position history filtered by symbol and time
        filtered_history = []
        # Filtering logic...
        return filtered_history
```

## Signal Generator

### Current State
- Basic structure defined
- Missing NFD calculation
- Missing opportunity ranking

### Required Implementation

```python
class SignalGenerator:
    def __init__(self, data_handler, portfolio_tracker):
        self.data_handler = data_handler
        self.portfolio_tracker = portfolio_tracker
        self.min_funding_threshold = 0.0001  # 0.01% minimum threshold
    
    async def generate_signals(self):
        # Implementation to generate trading signals based on current market data
        funding_rates = self.data_handler.get_funding_rates()
        
        # Calculate NFD (Net Funding Difference) for each market
        nfd_opportunities = self._calculate_nfd_opportunities(funding_rates)
        
        # Rank opportunities
        ranked_opportunities = self._rank_opportunities(nfd_opportunities)
        
        # Apply minimum threshold
        filtered_opportunities = [
            opp for opp in ranked_opportunities 
            if abs(opp['nfd']) >= self.min_funding_threshold
        ]
        
        return filtered_opportunities
    
    def _calculate_nfd_opportunities(self, funding_rates):
        # Implementation to calculate NFD for each market
        opportunities = []
        
        for symbol, rate in funding_rates.items():
            # Calculate NFD for current market
            nfd = self._calculate_single_nfd(symbol, rate)
            
            if nfd != 0:
                opportunities.append({
                    'symbol': symbol,
                    'nfd': nfd,
                    'funding_rate': rate,
                    'estimated_cost': self._estimate_trade_cost(symbol)
                })
        
        return opportunities
    
    def _calculate_single_nfd(self, symbol, rate):
        # Implementation to calculate NFD for a single market
        # NFD calculation logic...
        return rate  # Simplified calculation
    
    def _estimate_trade_cost(self, symbol):
        # Implementation to estimate trading cost for a specific market
        # Cost estimation logic including fees, slippage, etc.
        estimated_cost = 0.0005  # Simplified 0.05% cost estimate
        return estimated_cost
    
    def _rank_opportunities(self, opportunities):
        # Implementation to rank opportunities by NFD adjusted for estimated costs
        for opp in opportunities:
            opp['adjusted_nfd'] = opp['nfd'] - opp['estimated_cost']
        
        # Sort by adjusted NFD in descending order
        ranked = sorted(opportunities, key=lambda x: abs(x['adjusted_nfd']), reverse=True)
        return ranked
```

## Risk Manager

### Current State
- Basic structure defined
- Missing risk calculations
- Missing position sizing

### Required Implementation

```python
class RiskManager:
    def __init__(self, portfolio_tracker, config):
        self.portfolio_tracker = portfolio_tracker
        self.config = config
        self.max_position_size = config.get('max_position_size', 0.1)  # 10% of portfolio
        self.max_total_exposure = config.get('max_total_exposure', 0.5)  # 50% of portfolio
    
    def evaluate_opportunity(self, opportunity, market_data):
        # Implementation to evaluate risk for a specific opportunity
        symbol = opportunity['symbol']
        direction = 1 if opportunity['nfd'] > 0 else -1
        
        # Get current position
        current_position = self.portfolio_tracker.get_position(symbol)
        
        # Calculate risk metrics
        portfolio_value = self.portfolio_tracker.get_total_portfolio_value()
        current_exposure = self._calculate_total_exposure(portfolio_value)
        available_risk = self.max_total_exposure - current_exposure
        
        # Calculate appropriate position size
        suggested_size = self._calculate_position_size(
            opportunity, market_data, portfolio_value, available_risk
        )
        
        # Apply risk constraints
        final_size = self._apply_risk_constraints(
            suggested_size, symbol, current_position, portfolio_value
        )
        
        return {
            'symbol': symbol,
            'direction': direction,
            'size': final_size,
            'current_position': current_position,
            'risk_metrics': {
                'portfolio_value': portfolio_value,
                'current_exposure': current_exposure,
                'available_risk': available_risk,
                'max_position_allowed': portfolio_value * self.max_position_size
            }
        }
    
    def _calculate_total_exposure(self, portfolio_value):
        # Implementation to calculate total current exposure
        total_exposure = 0.0
        for symbol, position in self.portfolio_tracker.positions.items():
            total_exposure += abs(position['size'] * position['entry_price']) / portfolio_value
        return total_exposure
    
    def _calculate_position_size(self, opportunity, market_data, portfolio_value, available_risk):
        # Implementation to calculate appropriate position size
        base_size = portfolio_value * self.max_position_size * 0.5  # Start with 50% of max
        
        # Adjust size based on NFD (higher NFD = larger position)
        nfd_factor = min(abs(opportunity['nfd']) * 100, 1.0)  # Scale NFD to 0-1
        adjusted_size = base_size * nfd_factor
        
        # Adjust for available risk
        if available_risk <= 0:
            return 0.0
        
        risk_ratio = min(available_risk / self.max_total_exposure, 1.0)
        risk_adjusted_size = adjusted_size * risk_ratio
        
        return risk_adjusted_size
    
    def _apply_risk_constraints(self, suggested_size, symbol, current_position, portfolio_value):
        # Implementation to apply risk constraints
        max_size = portfolio_value * self.max_position_size
        
        # Ensure we don't exceed maximum position size
        final_size = min(suggested_size, max_size)
        
        # Account for existing position
        if current_position:
            current_size = abs(current_position['size'])
            if current_size + final_size > max_size:
                final_size = max(0, max_size - current_size)
        
        return final_size
    
    def pre_trade_check(self, trade_plan):
        # Implementation to perform final checks before executing a trade
        # Return True if the trade passes all checks, False otherwise
        
        symbol = trade_plan['symbol']
        size = trade_plan['size']
        
        # Check if the trade size is significant
        if size <= 0:
            return False
        
        # Check market conditions (liquidity, volatility)
        # Additional checks...
        
        return True
```

## Execution Handler

### Current State
- Basic structure defined
- Missing order management
- Missing error handling

### Required Implementation

```python
class ExecutionHandler:
    def __init__(self, api_client, portfolio_tracker):
        self.api_client = api_client
        self.portfolio_tracker = portfolio_tracker
        self.active_orders = {}
        self.execution_history = []
    
    async def execute_trade_plan(self, trade_plan):
        # Implementation to execute a trade plan
        symbol = trade_plan['symbol']
        direction = trade_plan['direction']
        size = trade_plan['size']
        
        # Determine order parameters
        side = 'buy' if direction > 0 else 'sell'
        order_type = 'market'  # For Prototype 0.0.1, use market orders for simplicity
        
        try:
            # Place order
            order_result = await self.api_client.place_order(
                symbol=symbol,
                side=side,
                order_type=order_type,
                amount=size
            )
            
            # Record order
            order_id = order_result['order_id']
            self.active_orders[order_id] = {
                'symbol': symbol,
                'side': side,
                'size': size,
                'status': 'pending',
                'created_at': time.time()
            }
            
            # Monitor order
            await self._monitor_order(order_id)
            
            # Update portfolio
            await self.portfolio_tracker.update_portfolio()
            
            # Record execution
            self._record_execution(trade_plan, order_result, 'success')
            
            return {
                'success': True,
                'order_id': order_id,
                'details': order_result
            }
            
        except Exception as e:
            # Handle execution error
            self._record_execution(trade_plan, str(e), 'failed')
            
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _monitor_order(self, order_id, timeout=60):
        # Implementation to monitor order status until filled or timeout
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Check order status
                order_status = await self._check_order_status(order_id)
                
                # Update active order record
                if order_id in self.active_orders:
                    self.active_orders[order_id]['status'] = order_status['status']
                
                # If order is filled or canceled, break
                if order_status['status'] in ['filled', 'canceled']:
                    # Handle order completion
                    if order_status['status'] == 'filled':
                        await self._handle_order_filled(order_id, order_status)
                    break
                
                # Wait before next check
                await asyncio.sleep(1)
                
            except Exception as e:
                # Log error and continue monitoring
                print(f"Error monitoring order {order_id}: {e}")
                await asyncio.sleep(1)
        
        # Handle timeout if order is still active
        if order_id in self.active_orders and self.active_orders[order_id]['status'] not in ['filled', 'canceled']:
            await self._handle_order_timeout(order_id)
    
    async def _check_order_status(self, order_id):
        # Implementation to check order status
        # This is a placeholder - actual implementation would call the API
        pass
    
    async def _handle_order_filled(self, order_id, order_status):
        # Implementation to handle filled order
        if order_id in self.active_orders:
            # Record fill details
            self.active_orders[order_id]['fill_price'] = order_status.get('average_price')
            self.active_orders[order_id]['fill_time'] = time.time()
            
            # Update portfolio state
            await self.portfolio_tracker.update_portfolio()
    
    async def _handle_order_timeout(self, order_id):
        # Implementation to handle order timeout
        try:
            # Cancel the order
            await self.api_client.cancel_order(order_id)
            
            # Update active order record
            if order_id in self.active_orders:
                self.active_orders[order_id]['status'] = 'canceled'
                self.active_orders[order_id]['canceled_at'] = time.time()
        except Exception as e:
            # Log error
            print(f"Error canceling timed-out order {order_id}: {e}")
    
    def _record_execution(self, trade_plan, result, status):
        # Implementation to record execution details
        execution_record = {
            'timestamp': time.time(),
            'trade_plan': copy.deepcopy(trade_plan),
            'result': result,
            'status': status
        }
        self.execution_history.append(execution_record)
        
        # Limit history size
        if len(self.execution_history) > 1000:
            self.execution_history = self.execution_history[-1000:]
    
    def get_execution_history(self, symbol=None, start_time=None, end_time=None):
        # Implementation to retrieve execution history filtered by symbol and time
        filtered_history = []
        # Filtering logic...
        return filtered_history
```

## Testing Infrastructure

### Current State
- Minimal testing structure
- Missing unit tests
- Missing simulation framework

### Required Implementation

#### Unit Tests

```python
# Example unit tests for the HyperliquidAPI client
def test_derive_address():
    # Test that address derivation works correctly
    auth = HyperliquidAuth(test_private_key)
    assert auth.address == expected_address

def test_sign_message():
    # Test that message signing works correctly
    auth = HyperliquidAuth(test_private_key)
    signature = auth.sign_message("test message")
    assert verify_signature(signature, "test message", auth.address)

# Example unit tests for the SignalGenerator
def test_nfd_calculation():
    # Test that NFD calculation works correctly
    data_handler = MockDataHandler(test_funding_rates)
    portfolio_tracker = MockPortfolioTracker()
    signal_generator = SignalGenerator(data_handler, portfolio_tracker)
    
    opportunities = signal_generator._calculate_nfd_opportunities(test_funding_rates)
    
    # Verify calculation results
    assert len(opportunities) == len(test_funding_rates)
    for opp in opportunities:
        symbol = opp['symbol']
        assert opp['nfd'] == test_funding_rates[symbol]
```

#### Integration Tests

```python
# Example integration test for data flow
async def test_data_flow():
    # Test that data flows correctly from API to DataHandler to SignalGenerator
    api_client = MockHyperliquidAPI(test_funding_response)
    data_handler = DataHandler(api_client)
    portfolio_tracker = MockPortfolioTracker()
    signal_generator = SignalGenerator(data_handler, portfolio_tracker)
    
    await data_handler.initialize()
    opportunities = await signal_generator.generate_signals()
    
    # Verify that opportunities were generated from the test data
    assert len(opportunities) > 0
    for opp in opportunities:
        assert opp['symbol'] in test_funding_response['markets']

# Example integration test for execution flow
async def test_execution_flow():
    # Test that trade execution flow works correctly
    api_client = MockHyperliquidAPI(test_order_response)
    portfolio_tracker = PortfolioTracker(api_client)
    execution_handler = ExecutionHandler(api_client, portfolio_tracker)
    
    # Create a trade plan
    trade_plan = {
        'symbol': 'BTC-PERP',
        'direction': 1,
        'size': 0.1
    }
    
    # Execute the trade
    result = await execution_handler.execute_trade_plan(trade_plan)
    
    # Verify the execution result
    assert result['success'] == True
    assert result['order_id'] in execution_handler.active_orders
    
    # Verify that the portfolio was updated
    assert portfolio_tracker.last_update_time is not None
```

#### Simulation Framework

```python
class TradingSimulation:
    def __init__(self, config, market_data_source):
        self.config = config
        self.market_data_source = market_data_source
        self.mock_api = MockHyperliquidAPI()
        self.data_handler = DataHandler(self.mock_api)
        self.portfolio_tracker = PortfolioTracker(self.mock_api)
        self.signal_generator = SignalGenerator(self.data_handler, self.portfolio_tracker)
        self.risk_manager = RiskManager(self.portfolio_tracker, config)
        self.execution_handler = ExecutionHandler(self.mock_api, self.portfolio_tracker)
        
        self.simulation_results = {
            'trades': [],
            'portfolio_value_history': [],
            'metrics': {}
        }
    
    async def run_simulation(self, start_time, end_time, time_step=60):
        # Implementation to run a simulation over a time period
        current_time = start_time
        
        # Initialize components
        await self.portfolio_tracker.initialize()
        await self.data_handler.initialize()
        
        while current_time <= end_time:
            # Update market data for the current time
            await self._update_market_data(current_time)
            
            # Generate signals
            opportunities = await self.signal_generator.generate_signals()
            
            # Evaluate and execute opportunities
            for opp in opportunities:
                # Risk evaluation
                trade_plan = self.risk_manager.evaluate_opportunity(opp, self.market_data_source.get_market_data(current_time))
                
                # Pre-trade check
                if self.risk_manager.pre_trade_check(trade_plan):
                    # Execute trade
                    execution_result = await self.execution_handler.execute_trade_plan(trade_plan)
                    
                    # Record trade
                    self._record_trade(current_time, trade_plan, execution_result)
            
            # Record portfolio state
            self._record_portfolio_state(current_time)
            
            # Advance time
            current_time += time_step
        
        # Calculate simulation metrics
        self._calculate_metrics()
        
        return self.simulation_results
    
    async def _update_market_data(self, timestamp):
        # Implementation to update market data for the current timestamp
        market_data = self.market_data_source.get_market_data(timestamp)
        self.mock_api.update_market_data(market_data)
        await self.data_handler.update_market_data()
    
    def _record_trade(self, timestamp, trade_plan, execution_result):
        # Implementation to record a trade in the simulation results
        trade_record = {
            'timestamp': timestamp,
            'trade_plan': copy.deepcopy(trade_plan),
            'execution_result': copy.deepcopy(execution_result)
        }
        self.simulation_results['trades'].append(trade_record)
    
    def _record_portfolio_state(self, timestamp):
        # Implementation to record portfolio state in the simulation results
        portfolio_value = self.portfolio_tracker.get_total_portfolio_value()
        portfolio_record = {
            'timestamp': timestamp,
            'portfolio_value': portfolio_value,
            'positions': copy.deepcopy(self.portfolio_tracker.positions),
            'balances': copy.deepcopy(self.portfolio_tracker.balances)
        }
        self.simulation_results['portfolio_value_history'].append(portfolio_record)
    
    def _calculate_metrics(self):
        # Implementation to calculate performance metrics
        # Calculate metrics like total return, Sharpe ratio, max drawdown, etc.
        portfolio_values = [record['portfolio_value'] for record in self.simulation_results['portfolio_value_history']]
        
        if len(portfolio_values) >= 2:
            # Calculate basic metrics
            self.simulation_results['metrics']['initial_value'] = portfolio_values[0]
            self.simulation_results['metrics']['final_value'] = portfolio_values[-1]
            self.simulation_results['metrics']['total_return'] = (portfolio_values[-1] / portfolio_values[0]) - 1
            self.simulation_results['metrics']['total_trades'] = len(self.simulation_results['trades'])
            
            # Additional metrics...
```

## Implementation Sequence

The following implementation sequence is recommended:

### Week 1: Foundation
1. Complete Hyperliquid API authentication
2. Implement key REST API endpoints
3. Set up basic WebSocket client
4. Develop unit tests for API client

### Week 2: Data Layer
1. Enhance Data Handler with WebSocket integration
2. Implement data caching and processing
3. Develop Portfolio Tracker state management
4. Create unit tests for Data Layer components

### Week 3: Strategy and Risk
1. Implement NFD calculation in Signal Generator
2. Develop opportunity ranking algorithm
3. Implement Risk Manager calculations
4. Create unit tests for Strategy and Risk components

### Week 4: Execution and Integration
1. Implement Execution Handler with order management
2. Develop error handling and recovery mechanisms
3. Create integration tests for the complete flow
4. Begin building simulation framework

### Week 5: Testing and Finalization
1. Complete simulation framework
2. Run end-to-end tests
3. Fix bugs and optimize performance
4. Finalize documentation and prepare for release

## Technical Debt and Future Considerations

### Hyperliquid API Client
- Nonce management for authentication
- Rate limiting and backoff strategy
- Error handling and retry logic

### Data Handler
- Improved data structures for efficient access
- Data validation and sanitization
- Historical data storage

### Portfolio Tracker
- More sophisticated PnL calculations
- Advanced portfolio metrics
- Historical performance analysis

### Signal Generator
- Multiple signal types beyond funding arbitrage
- Signal weighting and combination
- Market impact modeling

### Risk Manager
- Dynamic risk allocation
- Correlation-based position sizing
- Volatility-adjusted risk limits

### Execution Handler
- Smart order routing
- Advanced order types
- Partial fill handling

### Testing Infrastructure
- Expanded test coverage
- Performance testing
- Stress testing

By following this implementation roadmap, we will deliver a functioning Prototype 0.0.1 that can identify funding arbitrage opportunities, make risk-adjusted trading decisions, and execute trades on the Hyperliquid exchange. 
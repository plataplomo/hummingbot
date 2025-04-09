# Test Implementation Plan

## Critical Issues Identified by Critic

The Gemini critic identified testing as a critical risk:

> "Testing Status: The report openly admits tests are incomplete, logically flawed, and lack coverage/failure scenarios. This remains a CRITICAL RISK. A trading bot without solid tests is just a complicated way to donate money to the market."

## Required Testing Implementation

### 1. Core Component Unit Tests

#### 1.1 API Client Tests

```python
# test_hyperliquid_api.py
import pytest
import asyncio
from unittest.mock import patch, MagicMock
from cyberdelta.exchanges.hyperliquid_api import HyperliquidAPI

class TestHyperliquidAPI:
    @pytest.fixture
    def api_client(self):
        config = MagicMock()
        secrets = MagicMock()
        secrets.get.return_value = "test_api_key"
        return HyperliquidAPI(config, secrets)
    
    @patch('cyberdelta.exchanges.hyperliquid_api.aiohttp.ClientSession')
    async def test_get_funding_rate(self, mock_session, api_client):
        # Setup mock response
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json.return_value = {
            "funding_rates": [{"symbol": "BTC", "rate": 0.0001, "timestamp": 1625097600000}]
        }
        
        mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_resp
        
        # Test the method
        result = await api_client.get_funding_rate("BTC")
        
        # Assertions
        assert result is not None
        assert result["rate"] == 0.0001
        assert "timestamp" in result
        
        # Verify the correct URL was called with proper parameters
        mock_session.return_value.__aenter__.return_value.get.assert_called_once()
        call_args = mock_session.return_value.__aenter__.return_value.get.call_args[0][0]
        assert "funding" in call_args
        assert "BTC" in call_args
    
    @patch('cyberdelta.exchanges.hyperliquid_api.aiohttp.ClientSession')
    async def test_get_funding_rate_network_error(self, mock_session, api_client):
        # Setup mock to raise an exception
        mock_session.return_value.__aenter__.return_value.get.side_effect = Exception("Network error")
        
        # Test the method with exception handling
        with pytest.raises(Exception) as exc_info:
            await api_client.get_funding_rate("BTC")
        
        # Verify the exception was properly handled
        assert "Network error" in str(exc_info.value)
```

#### 1.2 Data Handler Tests

```python
# test_data_handler.py
import pytest
from unittest.mock import patch, MagicMock
from cyberdelta.data.data_handler import DataHandler
from cyberdelta.models.market_data import MarketData

class TestDataHandler:
    @pytest.fixture
    def data_handler(self):
        config = MagicMock()
        api_clients = {
            "hyperliquid": MagicMock(),
            "backpack": MagicMock()
        }
        return DataHandler(config, api_clients)
    
    async def test_process_funding_rate(self, data_handler):
        # Mock the funding rate data
        funding_data = {
            "symbol": "BTC",
            "rate": 0.0001,
            "timestamp": 1625097600000
        }
        
        # Mock the API client method
        data_handler.api_clients["hyperliquid"].get_funding_rate.return_value = funding_data
        
        # Setup observer
        observer = MagicMock()
        data_handler.register_observer(observer)
        
        # Call the method
        await data_handler.fetch_and_process_funding_rates("hyperliquid", "BTC")
        
        # Verify observer was notified with correct data
        observer.on_market_data.assert_called_once()
        market_data = observer.on_market_data.call_args[0][0]
        assert isinstance(market_data, MarketData)
        assert market_data.symbol == "BTC"
        assert market_data.exchange == "hyperliquid"
        assert market_data.data_type == "funding_rate"
        assert market_data.timestamp == 1625097600000
        assert market_data.data["rate"] == 0.0001
```

#### 1.3 Portfolio Tracker Tests

```python
# test_portfolio_tracker.py
import pytest
from unittest.mock import patch, MagicMock
from cyberdelta.portfolio.portfolio_tracker import PortfolioTracker

class TestPortfolioTracker:
    @pytest.fixture
    def portfolio_tracker(self):
        config = MagicMock()
        api_clients = {
            "hyperliquid": MagicMock(),
            "backpack": MagicMock()
        }
        return PortfolioTracker(config, api_clients)
    
    async def test_update_position(self, portfolio_tracker):
        # Initialize with empty positions
        portfolio_tracker.positions = {}
        
        # Test updating a new position
        await portfolio_tracker.update_position("hyperliquid", "BTC", 0.5, 50000.0, "long")
        
        # Verify the position was added
        assert "hyperliquid" in portfolio_tracker.positions
        assert "BTC" in portfolio_tracker.positions["hyperliquid"]
        assert portfolio_tracker.positions["hyperliquid"]["BTC"]["size"] == 0.5
        assert portfolio_tracker.positions["hyperliquid"]["BTC"]["entry_price"] == 50000.0
        assert portfolio_tracker.positions["hyperliquid"]["BTC"]["side"] == "long"
        
        # Test updating an existing position
        await portfolio_tracker.update_position("hyperliquid", "BTC", 1.0, 52000.0, "long")
        
        # Verify the position was updated with weighted average entry price
        assert portfolio_tracker.positions["hyperliquid"]["BTC"]["size"] == 1.0
        assert 50000.0 < portfolio_tracker.positions["hyperliquid"]["BTC"]["entry_price"] < 52000.0
```

#### 1.4 Risk Manager Tests

```python
# test_risk_manager.py
import pytest
from unittest.mock import patch, MagicMock
from cyberdelta.risk.risk_manager import RiskManager
from cyberdelta.models.trade_opportunity import ArbitrageOpportunity, SizedOpportunity

class TestRiskManager:
    @pytest.fixture
    def risk_manager(self):
        config = MagicMock()
        config.get.return_value = 1000.0  # Default max position size
        
        portfolio_tracker = MagicMock()
        portfolio_tracker.get_total_capital.return_value = 10000.0
        portfolio_tracker.get_total_exposure.return_value = 2000.0
        
        return RiskManager(config, portfolio_tracker)
    
    async def test_size_opportunity_within_limits(self, risk_manager):
        # Create a test opportunity
        opportunity = ArbitrageOpportunity(
            timestamp=1625097600000,
            perp_exchange="hyperliquid",
            perp_symbol="BTC",
            spot_exchange="backpack",
            spot_symbol="BTC_USDC",
            expected_return=0.001,  # 0.1%
            perp_price=50000.0,
            spot_price=50050.0,
            perp_side="short",
            spot_side="long",
            metadata={}
        )
        
        # Size the opportunity
        sized_opp = await risk_manager.size_opportunity(opportunity)
        
        # Verify
        assert sized_opp is not None
        assert sized_opp.position_size_usd <= 1000.0  # Should not exceed max position size
        assert sized_opp.perp_qty > 0
        assert sized_opp.spot_qty > 0
    
    async def test_size_opportunity_exceeds_limits(self, risk_manager):
        # Mock portfolio tracker to show high exposure
        risk_manager.portfolio_tracker.get_total_exposure.return_value = 9500.0  # 95% of capital
        
        # Create a test opportunity
        opportunity = ArbitrageOpportunity(
            timestamp=1625097600000,
            perp_exchange="hyperliquid",
            perp_symbol="BTC",
            spot_exchange="backpack",
            spot_symbol="BTC_USDC",
            expected_return=0.001,  # 0.1%
            perp_price=50000.0,
            spot_price=50050.0,
            perp_side="short",
            spot_side="long",
            metadata={}
        )
        
        # Size the opportunity
        sized_opp = await risk_manager.size_opportunity(opportunity)
        
        # Verify
        assert sized_opp is None  # Should be rejected due to exposure limits
```

### 2. Key Integration Tests

#### 2.1 Funding Rate Arbitrage Signal Generation

```python
# test_funding_rate_strategy.py
import pytest
from unittest.mock import patch, MagicMock
from cyberdelta.strategies.funding_rate_strategy import HLPerpBPSpotStrategy
from cyberdelta.models.market_data import MarketData

class TestFundingRateStrategy:
    @pytest.fixture
    def strategy(self):
        config = MagicMock()
        config.get.return_value = {
            "funding_threshold": 0.0001,
            "min_spread": 0.0002
        }
        
        symbols = {
            "hl_symbol": "BTC",
            "bp_symbol": "BTC_USDC"
        }
        
        return HLPerpBPSpotStrategy(config, "funding_arb", symbols)
    
    async def test_generate_signal_positive_funding(self, strategy):
        # Prepare market data
        funding_data = MarketData(
            symbol="BTC",
            exchange="hyperliquid",
            data_type="funding_rate",
            timestamp=1625097600000,
            data={"rate": 0.0005}  # Positive funding rate (0.05%)
        )
        
        # Mock internal data state
        strategy._get_latest_hl_price = MagicMock(return_value=50000.0)
        strategy._get_latest_bp_price = MagicMock(return_value=50010.0)
        
        # Process the funding data
        signal = await strategy.process_data(funding_data)
        
        # Verify signal is generated and correct
        assert signal is not None
        assert signal.strategy_name == "funding_arb"
        assert signal.perp_exchange == "hyperliquid"
        assert signal.perp_symbol == "BTC"
        assert signal.spot_exchange == "backpack"
        assert signal.spot_symbol == "BTC_USDC"
        assert signal.perp_side == "SELL"  # Short on positive funding
        assert signal.spot_side == "BUY"   # Long spot as hedge
        assert signal.expected_return > 0
    
    async def test_generate_signal_below_threshold(self, strategy):
        # Prepare market data with funding rate below threshold
        funding_data = MarketData(
            symbol="BTC",
            exchange="hyperliquid",
            data_type="funding_rate",
            timestamp=1625097600000,
            data={"rate": 0.00005}  # 0.005%, below 0.01% threshold
        )
        
        # Process the funding data
        signal = await strategy.process_data(funding_data)
        
        # Verify no signal is generated
        assert signal is None
```

#### 2.2 Execution Flow Tests

```python
# test_execution_handler.py
import pytest
from unittest.mock import patch, MagicMock
from cyberdelta.execution.execution_handler import ExecutionHandler
from cyberdelta.models.sized_opportunity import SizedOpportunity

class TestExecutionHandler:
    @pytest.fixture
    def execution_handler(self):
        config = MagicMock()
        api_clients = {
            "hyperliquid": MagicMock(),
            "backpack": MagicMock()
        }
        portfolio_tracker = MagicMock()
        circuit_breaker_manager = MagicMock()
        
        return ExecutionHandler(config, api_clients, portfolio_tracker, circuit_breaker_manager)
    
    async def test_execute_perp_spot_opportunity_success(self, execution_handler):
        # Create a sized opportunity
        opportunity = SizedOpportunity(
            timestamp=1625097600000,
            perp_exchange="hyperliquid",
            perp_symbol="BTC",
            spot_exchange="backpack",
            spot_symbol="BTC_USDC",
            expected_return=0.001,
            perp_price=50000.0,
            spot_price=50050.0,
            perp_side="short",
            spot_side="long",
            position_size_usd=500.0,
            perp_qty=0.01,
            spot_qty=0.01,
            metadata={}
        )
        
        # Mock successful order executions
        execution_handler.api_clients["hyperliquid"].place_order.return_value = {
            "order_id": "hl123",
            "status": "filled",
            "fill_price": 50000.0,
            "fill_qty": 0.01
        }
        
        execution_handler.api_clients["backpack"].place_order.return_value = {
            "order_id": "bp123",
            "status": "filled",
            "fill_price": 50050.0,
            "fill_qty": 0.01
        }
        
        # Execute the opportunity
        result = await execution_handler.execute_opportunity(opportunity)
        
        # Verify successful execution
        assert result["success"] is True
        assert result["perp_order_id"] == "hl123"
        assert result["spot_order_id"] == "bp123"
        
        # Verify portfolio was updated
        execution_handler.portfolio_tracker.update_position.assert_called()
    
    async def test_execute_perp_spot_opportunity_first_leg_failure(self, execution_handler):
        # Create a sized opportunity
        opportunity = SizedOpportunity(
            timestamp=1625097600000,
            perp_exchange="hyperliquid",
            perp_symbol="BTC",
            spot_exchange="backpack",
            spot_symbol="BTC_USDC",
            expected_return=0.001,
            perp_price=50000.0,
            spot_price=50050.0,
            perp_side="short",
            spot_side="long",
            position_size_usd=500.0,
            perp_qty=0.01,
            spot_qty=0.01,
            metadata={}
        )
        
        # Mock first leg failure
        execution_handler.api_clients["hyperliquid"].place_order.side_effect = Exception("API Error")
        
        # Execute the opportunity
        result = await execution_handler.execute_opportunity(opportunity)
        
        # Verify execution failed
        assert result["success"] is False
        assert "API Error" in result["error"]
        
        # Verify circuit breaker was triggered
        execution_handler.circuit_breaker_manager.get_circuit_breaker.return_value.record_failure.assert_called_once()
        
        # Verify second leg was never attempted
        execution_handler.api_clients["backpack"].place_order.assert_not_called()
```

### 3. Validation and Circuit Breaker Tests

#### 3.1 Funding Rate Validator Tests

```python
# test_funding_rate_validator.py
import pytest
import time
from unittest.mock import patch, MagicMock
from cyberdelta.validation.funding_rate_validator import FundingRateValidator

class TestFundingRateValidator:
    @pytest.fixture
    def validator(self):
        config = MagicMock()
        config.get.return_value = ":memory:"  # Use in-memory SQLite
        return FundingRateValidator(config)
    
    def test_record_prediction(self, validator):
        # Record a prediction
        validator.record_prediction("hyperliquid", "BTC", 0.0001, "api")
        
        # Verify it was stored
        conn = validator._get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM funding_predictions WHERE exchange = ? AND symbol = ?", 
                      ("hyperliquid", "BTC"))
        result = cursor.fetchone()
        conn.close()
        
        assert result is not None
        assert result[3] == "hyperliquid"  # exchange
        assert result[4] == "BTC"  # symbol
        assert result[5] == 0.0001  # rate
    
    def test_record_payment(self, validator):
        # Record an actual payment
        validator.record_payment("hyperliquid", "BTC", 0.0001, 0.5, 10.0)
        
        # Verify it was stored
        conn = validator._get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM funding_payments WHERE exchange = ? AND symbol = ?", 
                      ("hyperliquid", "BTC"))
        result = cursor.fetchone()
        conn.close()
        
        assert result is not None
        assert result[3] == "hyperliquid"  # exchange
        assert result[4] == "BTC"  # symbol
        assert result[5] == 0.0001  # rate
        assert result[6] == 0.5  # payment amount
    
    def test_calculate_metrics(self, validator):
        # Insert test data
        timestamp = int(time.time() * 1000)
        validator.record_prediction("hyperliquid", "BTC", 0.0001, "api")
        validator.record_payment("hyperliquid", "BTC", 0.00012, 0.6, 10.0)
        
        # Calculate metrics
        metrics = validator.calculate_metrics("hyperliquid", "BTC", 1)
        
        # Verify metrics
        assert metrics["count"] > 0
        assert "rmse" in metrics
        assert "mae" in metrics
        assert "bias" in metrics
```

#### 3.2 Circuit Breaker Tests

```python
# test_circuit_breaker.py
import pytest
import time
from cyberdelta.circuit_breakers.circuit_breaker import CircuitBreaker

class TestCircuitBreaker:
    @pytest.fixture
    def circuit_breaker(self):
        return CircuitBreaker(
            name="test_breaker",
            failure_threshold=3,
            reset_timeout=1,  # 1 second for faster testing
            half_open_max_calls=2,
            success_threshold=2
        )
    
    def test_initial_state(self, circuit_breaker):
        assert circuit_breaker.get_state() == CircuitBreaker.CLOSED
        assert circuit_breaker.allow_request() is True
    
    def test_failure_threshold(self, circuit_breaker):
        # Record failures up to threshold
        for i in range(2):
            circuit_breaker.record_failure(f"Test failure {i}")
            assert circuit_breaker.get_state() == CircuitBreaker.CLOSED
        
        # One more failure should trip the breaker
        circuit_breaker.record_failure("Final failure")
        assert circuit_breaker.get_state() == CircuitBreaker.OPEN
        assert circuit_breaker.allow_request() is False
    
    def test_reset_timeout(self, circuit_breaker):
        # Trip the breaker
        for i in range(3):
            circuit_breaker.record_failure(f"Test failure {i}")
        
        assert circuit_breaker.get_state() == CircuitBreaker.OPEN
        assert circuit_breaker.allow_request() is False
        
        # Wait for reset timeout
        time.sleep(1.1)  # Just over the reset_timeout
        
        # Should be in half-open state now
        assert circuit_breaker.allow_request() is True
        assert circuit_breaker.get_state() == CircuitBreaker.HALF_OPEN
    
    def test_half_open_success(self, circuit_breaker):
        # Trip the breaker
        for i in range(3):
            circuit_breaker.record_failure(f"Test failure {i}")
        
        # Wait for reset timeout
        time.sleep(1.1)
        
        # First request in half-open state
        assert circuit_breaker.allow_request() is True
        circuit_breaker.record_success()
        
        # Still in half-open state
        assert circuit_breaker.get_state() == CircuitBreaker.HALF_OPEN
        
        # Second success should close the circuit
        assert circuit_breaker.allow_request() is True
        circuit_breaker.record_success()
        
        # Circuit should be closed now
        assert circuit_breaker.get_state() == CircuitBreaker.CLOSED
```

### 4. Test Implementation Approach

#### 4.1 Testing Priority Order

1. **Fix existing tests first**
   - Correct logical errors in current test suite
   - Ensure all tests pass reliably

2. **Basic component unit tests**
   - API Clients
   - Data Handler
   - Portfolio Tracker
   - Risk Manager

3. **Integration tests for core workflows**
   - Signal generation
   - Risk assessment
   - Order execution

4. **Safety system tests**
   - Validation
   - Circuit breakers
   - Error handling

#### 4.2 Test Coverage Requirements

- **API Interface Tests**: 100% coverage for API interface methods
- **Data Processing**: 90%+ coverage for data handlers
- **Risk Management**: 95%+ coverage for risk rules
- **Execution Logic**: 95%+ coverage including failure scenarios
- **Circuit Breakers**: 100% coverage for all state transitions
- **Validation**: 90%+ coverage for validators

#### 4.3 Testing Patterns

- **Dependency Injection**: Mock external dependencies
- **Fixture-Based Setup**: Standardize test fixtures
- **Parameterized Tests**: Test boundary conditions systematically
- **Async Testing**: Proper async fixture handling
- **Integration Fixtures**: Shared setup for integration tests
- **Failure Injection**: Simulate API failures, timeouts, errors

#### 4.4 Test Execution

- Run unit tests on every commit
- Trigger integration tests on PR and before deployment
- Use GitHub Actions for CI/CD integration
- Generate coverage reports

## Implementation Timeline

1. **Day 1**: Fix existing tests to correct logical errors
2. **Day 2-3**: Implement unit tests for API clients and data components
3. **Day 4-5**: Implement unit tests for risk management and execution components
4. **Day 6-7**: Implement integration tests for core workflows
5. **Day 8-9**: Implement tests for validation and circuit breaker systems
6. **Day 10**: Finalize test coverage and generate reports 
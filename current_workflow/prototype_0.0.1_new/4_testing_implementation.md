# Testing Implementation - Prototype 0.0.1

This document outlines the specific testing approach, infrastructure setup, and test cases required for the CyberDeltaEngine prototype 0.0.1.

## 1. Testing Infrastructure Setup

### 1.1 Core Testing Tools

```python
# requirements-dev.txt
pytest==7.4.0
pytest-asyncio==0.21.1
pytest-mock==3.11.1
pytest-cov==4.1.0
aioresponses==0.7.4
websockets==11.0.3
```

### 1.2 Project Test Structure

```
tests/
├── conftest.py                 # Shared fixtures
├── mock_data/                  # Mock API responses
│   ├── hyperliquid_responses/
│   │   ├── market_data.json
│   │   ├── account_data.json
│   │   └── orders.json
│   └── websocket_streams/
│       ├── orderbook_updates.json
│       └── funding_updates.json
├── unit/                       # Unit tests
│   ├── api/
│   │   └── test_hyperliquid.py
│   ├── data/
│   │   └── test_data_handler.py
│   ├── signal/
│   │   └── test_signal_generator.py
│   ├── risk/
│   │   └── test_risk_manager.py
│   ├── execution/
│   │   └── test_execution_handler.py
│   └── portfolio/
│       └── test_portfolio_tracker.py
├── integration/                # Integration tests
│   ├── test_data_to_signal.py
│   ├── test_signal_to_risk.py
│   └── test_risk_to_execution.py
└── simulation/                 # Simulation tests
    ├── scenarios/
    │   ├── funding_arbitrage.py
    │   └── error_conditions.py
    └── test_full_workflow.py
```

### 1.3 Test Configuration Setup

```python
# conftest.py
import pytest
import json
import os
import asyncio
from unittest.mock import MagicMock, AsyncMock

# Load mock data
@pytest.fixture
def mock_market_data():
    with open('tests/mock_data/hyperliquid_responses/market_data.json', 'r') as f:
        return json.load(f)

@pytest.fixture
def mock_account_data():
    with open('tests/mock_data/hyperliquid_responses/account_data.json', 'r') as f:
        return json.load(f)

# Mock API client
@pytest.fixture
def mock_hyperliquid_api():
    mock_api = AsyncMock()
    mock_api.fetch_funding_rate.return_value = 0.0001
    mock_api.fetch_orderbook.return_value = {
        "bids": [{"price": 30000, "size": 1.5}],
        "asks": [{"price": 30010, "size": 2.0}]
    }
    return mock_api

# Event loop
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
```

## 2. Unit Testing Implementation

### 2.1 API Client Tests

```python
# tests/unit/api/test_hyperliquid.py
import pytest
import aioresponses
from aiohttp import ClientSession
import json

from app.api.hyperliquid import HyperliquidAPI

@pytest.fixture
def mock_responses():
    with aioresponses.aioresponses() as m:
        yield m

@pytest.mark.asyncio
async def test_fetch_funding_rate(mock_responses):
    # Arrange
    api = HyperliquidAPI(api_key="test_key", api_secret="test_secret")
    mock_funding_response = {"data": {"fundingRate": 0.0001}}
    mock_responses.get(
        "https://api.hyperliquid.xyz/info",
        status=200,
        payload=mock_funding_response
    )
    
    # Act
    result = await api.fetch_funding_rate("BTC-PERP")
    
    # Assert
    assert result == 0.0001

@pytest.mark.asyncio
async def test_authentication(mock_responses):
    # Arrange
    api = HyperliquidAPI(api_key="test_key", api_secret="test_secret")
    mock_responses.post(
        "https://api.hyperliquid.xyz/exchange",
        status=200,
        payload={"success": True}
    )
    
    # Act
    # Call method that requires authentication
    result = await api.place_order(
        symbol="BTC-PERP",
        side="buy",
        size=1.0,
        price=30000,
        order_type="limit"
    )
    
    # Assert
    # Verify the request was made with correct authentication
    request_info = mock_responses.requests[("POST", "https://api.hyperliquid.xyz/exchange")][0]
    request_body = json.loads(request_info.kwargs["data"])
    assert "signature" in request_body
    assert request_body["action"]["type"] == "order"
```

### 2.2 Data Handler Tests

```python
# tests/unit/data/test_data_handler.py
import pytest
from unittest.mock import AsyncMock, patch

from app.data.data_handler import DataHandler

@pytest.mark.asyncio
async def test_get_funding_rate(mock_hyperliquid_api):
    # Arrange
    data_handler = DataHandler(api_client=mock_hyperliquid_api)
    
    # Act
    funding_rate = await data_handler.get_funding_rate("BTC-PERP")
    
    # Assert
    assert funding_rate == 0.0001
    mock_hyperliquid_api.fetch_funding_rate.assert_called_once_with("BTC-PERP")

@pytest.mark.asyncio
async def test_websocket_subscription():
    # Arrange
    mock_api = AsyncMock()
    data_handler = DataHandler(api_client=mock_api)
    callback = AsyncMock()
    
    # Act
    await data_handler.subscribe("orderbook", "BTC-PERP", callback)
    
    # Assert
    mock_api.subscribe_to_orderbook.assert_called_once_with("BTC-PERP", data_handler._process_orderbook_update)
```

### 2.3 Signal Generator Tests

```python
# tests/unit/signal/test_signal_generator.py
import pytest
from unittest.mock import AsyncMock, patch

from app.signal.signal_generator import SignalGenerator

@pytest.mark.asyncio
async def test_calculate_nfd():
    # Arrange
    data_handler = AsyncMock()
    data_handler.get_funding_rate.side_effect = [0.0001, -0.0003]  # Returns for long, short
    signal_generator = SignalGenerator(data_handler=data_handler)
    
    # Act
    nfd = await signal_generator.calculate_nfd("BTC-PERP")
    
    # Assert
    assert nfd == 0.0004  # 0.0001 - (-0.0003)
    assert data_handler.get_funding_rate.call_count == 2

@pytest.mark.asyncio
async def test_rank_opportunities():
    # Arrange
    data_handler = AsyncMock()
    signal_generator = SignalGenerator(data_handler=data_handler)
    signal_generator.calculate_nfd = AsyncMock(side_effect=[0.0004, 0.0002, 0.0006])
    
    # Act
    ranked = await signal_generator.rank_opportunities(["BTC-PERP", "ETH-PERP", "SOL-PERP"])
    
    # Assert
    assert ranked[0]["symbol"] == "SOL-PERP"
    assert ranked[0]["nfd"] == 0.0006
    assert ranked[1]["symbol"] == "BTC-PERP"
    assert ranked[2]["symbol"] == "ETH-PERP"
```

### 2.4 Risk Manager Tests

```python
# tests/unit/risk/test_risk_manager.py
import pytest
from unittest.mock import AsyncMock, patch

from app.risk.risk_manager import RiskManager

@pytest.mark.asyncio
async def test_calculate_position_size():
    # Arrange
    portfolio_tracker = AsyncMock()
    portfolio_tracker.get_total_equity.return_value = 10000
    risk_manager = RiskManager(portfolio_tracker=portfolio_tracker)
    signal = {"symbol": "BTC-PERP", "nfd": 0.0004, "confidence": 0.8}
    
    # Act
    size = await risk_manager.calculate_position_size(signal)
    
    # Assert
    assert size > 0
    # Verify position size follows risk rules (example: max 5% of equity)
    assert size <= 500  # 5% of 10000

@pytest.mark.asyncio
async def test_check_trade_viability():
    # Arrange
    portfolio_tracker = AsyncMock()
    portfolio_tracker.get_total_equity.return_value = 10000
    portfolio_tracker.get_position_size.return_value = 400  # Already have 400 in position
    risk_manager = RiskManager(portfolio_tracker=portfolio_tracker)
    trade = {"symbol": "BTC-PERP", "size": 200, "side": "buy"}
    
    # Act
    result = await risk_manager.check_trade_viability(trade)
    
    # Assert
    assert result is True  # 400 + 200 = 600, which is 6% of equity, still acceptable
```

### 2.5 Execution Handler Tests

```python
# tests/unit/execution/test_execution_handler.py
import pytest
from unittest.mock import AsyncMock, patch

from app.execution.execution_handler import ExecutionHandler

@pytest.mark.asyncio
async def test_execute_signal():
    # Arrange
    api = AsyncMock()
    api.place_order.return_value = {"id": "order123", "status": "open"}
    
    risk_manager = AsyncMock()
    risk_manager.calculate_position_size.return_value = 0.5
    risk_manager.check_trade_viability.return_value = True
    
    execution_handler = ExecutionHandler(api_client=api, risk_manager=risk_manager)
    signal = {"symbol": "BTC-PERP", "action": "buy", "price": 30000}
    
    # Act
    result = await execution_handler.execute_signal(signal)
    
    # Assert
    assert result["id"] == "order123"
    api.place_order.assert_called_once_with(
        symbol="BTC-PERP",
        side="buy",
        size=0.5,
        price=30000,
        order_type="limit"
    )

@pytest.mark.asyncio
async def test_execute_signal_risk_rejection():
    # Arrange
    api = AsyncMock()
    risk_manager = AsyncMock()
    risk_manager.check_trade_viability.return_value = False
    
    execution_handler = ExecutionHandler(api_client=api, risk_manager=risk_manager)
    signal = {"symbol": "BTC-PERP", "action": "buy", "price": 30000}
    
    # Act
    result = await execution_handler.execute_signal(signal)
    
    # Assert
    assert "error" in result
    assert "risk check failed" in result["error"]
    api.place_order.assert_not_called()
```

## 3. Integration Testing Implementation

### 3.1 Data to Signal Flow Test

```python
# tests/integration/test_data_to_signal.py
import pytest
from unittest.mock import AsyncMock, patch

from app.data.data_handler import DataHandler
from app.signal.signal_generator import SignalGenerator

@pytest.mark.asyncio
async def test_data_to_signal_flow():
    # Arrange
    mock_api = AsyncMock()
    mock_api.fetch_funding_rate.side_effect = [0.0001, -0.0003]  # Long, short rates
    
    data_handler = DataHandler(api_client=mock_api)
    signal_generator = SignalGenerator(data_handler=data_handler)
    
    # Act
    opportunities = await signal_generator.generate_signals()
    
    # Assert
    assert len(opportunities) > 0
    assert "symbol" in opportunities[0]
    assert "nfd" in opportunities[0]
    assert "action" in opportunities[0]
```

### 3.2 Signal to Risk Flow Test

```python
# tests/integration/test_signal_to_risk.py
import pytest
from unittest.mock import AsyncMock, patch

from app.signal.signal_generator import SignalGenerator
from app.risk.risk_manager import RiskManager
from app.portfolio.portfolio_tracker import PortfolioTracker

@pytest.mark.asyncio
async def test_signal_to_risk_flow():
    # Arrange
    data_handler = AsyncMock()
    portfolio_tracker = PortfolioTracker(api_client=AsyncMock())
    portfolio_tracker.get_total_equity = AsyncMock(return_value=10000)
    
    signal_generator = SignalGenerator(data_handler=data_handler)
    signal_generator.generate_signals = AsyncMock(return_value=[
        {"symbol": "BTC-PERP", "action": "buy", "nfd": 0.0004, "confidence": 0.8}
    ])
    
    risk_manager = RiskManager(portfolio_tracker=portfolio_tracker)
    
    # Act
    signals = await signal_generator.generate_signals()
    sized_trades = []
    
    for signal in signals:
        size = await risk_manager.calculate_position_size(signal)
        trade = {**signal, "size": size}
        viable = await risk_manager.check_trade_viability(trade)
        if viable:
            sized_trades.append(trade)
    
    # Assert
    assert len(sized_trades) > 0
    assert sized_trades[0]["size"] > 0
```

### 3.3 Risk to Execution Flow Test

```python
# tests/integration/test_risk_to_execution.py
import pytest
from unittest.mock import AsyncMock, patch

from app.risk.risk_manager import RiskManager
from app.execution.execution_handler import ExecutionHandler
from app.portfolio.portfolio_tracker import PortfolioTracker

@pytest.mark.asyncio
async def test_risk_to_execution_flow():
    # Arrange
    api_client = AsyncMock()
    api_client.place_order.return_value = {"id": "order123", "status": "open"}
    
    portfolio_tracker = PortfolioTracker(api_client=api_client)
    portfolio_tracker.get_total_equity = AsyncMock(return_value=10000)
    
    risk_manager = RiskManager(portfolio_tracker=portfolio_tracker)
    execution_handler = ExecutionHandler(api_client=api_client, risk_manager=risk_manager)
    
    trade = {"symbol": "BTC-PERP", "action": "buy", "price": 30000, "size": 0.5}
    
    # Act
    result = await execution_handler.execute_signal(trade)
    
    # Assert
    assert "id" in result
    assert result["id"] == "order123"
```

## 4. Simulation Testing Implementation

### 4.1 Funding Arbitrage Scenario

```python
# tests/simulation/scenarios/funding_arbitrage.py
import json
import os

class FundingArbitrageScenario:
    def __init__(self):
        self.market_data = self._load_market_data()
        self.account_data = self._load_account_data()
        self.events = self._generate_events()
    
    def _load_market_data(self):
        with open('tests/mock_data/hyperliquid_responses/market_data.json', 'r') as f:
            return json.load(f)
    
    def _load_account_data(self):
        with open('tests/mock_data/hyperliquid_responses/account_data.json', 'r') as f:
            return json.load(f)
    
    def _generate_events(self):
        """Generate a sequence of market events for simulation"""
        return [
            {"type": "funding_update", "symbol": "BTC-PERP", "rate": 0.0001, "timestamp": 1000},
            {"type": "funding_update", "symbol": "BTC-PERP", "rate": 0.0002, "timestamp": 2000},
            {"type": "orderbook_update", "symbol": "BTC-PERP", "data": {...}, "timestamp": 3000},
            {"type": "funding_update", "symbol": "BTC-PERP", "rate": 0.0003, "timestamp": 4000},
            # More events...
        ]
    
    async def run(self, engine):
        """Run the scenario against the engine"""
        for event in self.events:
            await self._process_event(engine, event)
    
    async def _process_event(self, engine, event):
        """Process a single event in the simulation"""
        if event["type"] == "funding_update":
            await engine.data_handler.process_funding_update(event)
        elif event["type"] == "orderbook_update":
            await engine.data_handler.process_orderbook_update(event)
        # Process other event types...
```

### 4.2 Full Workflow Test

```python
# tests/simulation/test_full_workflow.py
import pytest
import asyncio
from unittest.mock import AsyncMock, patch

from app.main import CyberDeltaEngine
from tests.simulation.scenarios.funding_arbitrage import FundingArbitrageScenario

@pytest.mark.asyncio
async def test_full_workflow_simulation():
    # Arrange
    # Create mocked components
    api_client = AsyncMock()
    data_handler = AsyncMock()
    signal_generator = AsyncMock()
    risk_manager = AsyncMock()
    execution_handler = AsyncMock()
    portfolio_tracker = AsyncMock()
    
    # Create engine with mocked components
    engine = CyberDeltaEngine(
        api_client=api_client,
        data_handler=data_handler,
        signal_generator=signal_generator,
        risk_manager=risk_manager,
        execution_handler=execution_handler,
        portfolio_tracker=portfolio_tracker
    )
    
    # Create scenario
    scenario = FundingArbitrageScenario()
    
    # Configure mocks to respond to scenario events
    signal_generator.generate_signals.return_value = [
        {"symbol": "BTC-PERP", "action": "buy", "price": 30000, "nfd": 0.0004}
    ]
    
    risk_manager.check_trade_viability.return_value = True
    risk_manager.calculate_position_size.return_value = 0.5
    
    execution_handler.execute_signal.return_value = {"id": "order123", "status": "filled"}
    
    # Act
    await engine.initialize()
    # Run scenario
    await scenario.run(engine)
    
    # Assert
    # Verify flow through components
    assert signal_generator.generate_signals.called
    assert risk_manager.calculate_position_size.called
    assert execution_handler.execute_signal.called
    assert portfolio_tracker.update_positions.called
```

## 5. CI/CD Integration

### 5.1 GitHub Actions Workflow

```yaml
# .github/workflows/test.yml
name: Run Tests

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
        pip install -r requirements-dev.txt
    
    - name: Run unit tests
      run: |
        pytest tests/unit/ -v --cov=app
    
    - name: Run integration tests
      run: |
        pytest tests/integration/ -v
    
    - name: Run simulation tests
      run: |
        pytest tests/simulation/ -v
    
    - name: Generate coverage report
      run: |
        pytest --cov=app --cov-report=xml
    
    - name: Upload coverage to Codecov
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
```

## 6. Test Data Generation Scripts

### 6.1 Mock Response Generator

```python
# scripts/generate_mock_data.py
import json
import os
import random
from datetime import datetime, timedelta

def generate_market_data():
    """Generate mock market data responses"""
    symbols = ["BTC-PERP", "ETH-PERP", "SOL-PERP", "ARB-PERP", "MATIC-PERP"]
    
    market_data = {}
    for symbol in symbols:
        base_price = random.uniform(
            100 if symbol != "BTC-PERP" else 30000,
            500 if symbol != "BTC-PERP" else 40000
        )
        
        market_data[symbol] = {
            "price": base_price,
            "funding_rate": random.uniform(-0.0005, 0.0005),
            "volume_24h": random.uniform(100000000, 1000000000),
            "open_interest": random.uniform(50000000, 500000000)
        }
    
    # Create directory if it doesn't exist
    os.makedirs("tests/mock_data/hyperliquid_responses", exist_ok=True)
    
    # Write to file
    with open("tests/mock_data/hyperliquid_responses/market_data.json", "w") as f:
        json.dump(market_data, f, indent=2)

def generate_account_data():
    """Generate mock account data responses"""
    account_data = {
        "equity": random.uniform(9000, 11000),
        "available_balance": random.uniform(8000, 9000),
        "margin_used": random.uniform(1000, 2000),
        "positions": [
            {
                "symbol": "BTC-PERP",
                "size": random.uniform(0.1, 0.5),
                "entry_price": random.uniform(30000, 40000),
                "mark_price": random.uniform(30000, 40000),
                "unrealized_pnl": random.uniform(-500, 500)
            },
            {
                "symbol": "ETH-PERP",
                "size": random.uniform(1, 5),
                "entry_price": random.uniform(1800, 2200),
                "mark_price": random.uniform(1800, 2200),
                "unrealized_pnl": random.uniform(-300, 300)
            }
        ]
    }
    
    # Create directory if it doesn't exist
    os.makedirs("tests/mock_data/hyperliquid_responses", exist_ok=True)
    
    # Write to file
    with open("tests/mock_data/hyperliquid_responses/account_data.json", "w") as f:
        json.dump(account_data, f, indent=2)

if __name__ == "__main__":
    generate_market_data()
    generate_account_data()
    print("Mock data generated successfully!")
```

## 7. Testing Success Criteria

| Component | Coverage Target | Test Types | Success Metrics |
|-----------|----------------|------------|----------------|
| API Client | 90% | Unit + Integration | All endpoints tested, authentication verified |
| Data Handler | 85% | Unit + Integration | Data processing correctness, event handling |
| Signal Generator | 90% | Unit + Integration | NFD calculation accuracy, opportunity ranking |
| Risk Manager | 95% | Unit + Integration | Position sizing correctness, risk limit enforcement |
| Execution Handler | 95% | Unit + Integration + Simulation | Order placement, monitoring, error handling |
| Portfolio Tracker | 85% | Unit + Integration | Position/balance tracking accuracy |
| Main Orchestrator | 80% | Integration + Simulation | Component coordination, error recovery |

## 8. Implementation Timeline

| Week | Testing Focus | Deliverables |
|------|--------------|--------------|
| Week 1 | - Setup pytest infrastructure<br>- Create basic unit tests for API client<br>- Generate mock data | - Working test framework<br>- Initial API client tests<br>- Mock response fixtures |
| Week 2 | - Expand unit tests for all components<br>- Create initial integration tests<br>- Implement CI pipeline | - Component unit tests<br>- Basic integration tests<br>- GitHub Actions workflow |
| Week 3 | - Complete unit test coverage<br>- Enhance integration tests<br>- Begin simulation framework | - Full unit test coverage<br>- Comprehensive integration tests<br>- Simulation test structure |
| Week 4 | - Develop detailed simulation scenarios<br>- Test end-to-end workflows<br>- Stress testing | - Funding arbitrage simulation<br>- End-to-end test cases<br>- Performance/stress tests |
| Week 5 | - Fix identified issues<br>- Enhance test documentation<br>- Optimize test performance | - Bug fixes<br>- Comprehensive test documentation<br>- Optimized test execution | 
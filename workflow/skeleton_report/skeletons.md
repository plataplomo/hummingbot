# Project File Tree

```
.
├── .env.example
├── .gitignore
├── .roomodes
├── config.yaml
├── cyberdelta/
│   ├── __init__.py
│   ├── apis/
│   │   ├── __init__.py
│   │   ├── backpack.py
│   │   ├── base.py
│   │   ├── errors.py
│   │   └── hyperliquid.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── config.yaml
│   │   ├── config.yaml.example
│   │   ├── config_manager.py
│   │   ├── secrets.yaml.example
│   │   ├── secrets_manager.py
│   │   └── settings.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── backtesting/
│   │   │   └── results.py
│   │   ├── backtesting.py
│   │   ├── balance_monitor.py
│   │   ├── data_handler.py
│   │   ├── data_manager.py
│   │   ├── engine.py
│   │   ├── execution/
│   │   │   ├── __init__.py
│   │   │   └── synchronized_order_submission.py
│   │   ├── execution_handler.py
│   │   ├── models.py
│   │   ├── portfolio_tracker.py
│   │   ├── results.py
│   │   ├── risk_manager.py
│   │   ├── signal_generator.py
│   │   ├── signal_queue.py
│   │   ├── strategy.py
│   │   ├── strategy_manager.py
│   │   ├── symbol_mapper.py
│   │   └── trade_executor.py
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── dashboard_integration.py
│   │   ├── performance_metrics.py
│   │   ├── performance_tracker.py
│   │   ├── persistence.py
│   │   ├── real_time_dashboard.py
│   │   └── simplified_performance_tracker.py
│   ├── strategies/
│   │   └── funding_rate_arbitrage.py
│   ├── testing/
│   │   └── data_generation.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── constants.py
│   │   ├── logging_config.py
│   │   ├── serialization.py
│   │   └── state_manager.py
│   ├── validation/
│   │   ├── __init__.py
│   │   ├── circuit_breaker.py
│   │   ├── funding_data.py
│   │   ├── funding_rate_validator.py
│   │   ├── multi_tier_funding_provider.py
│   │   └── position_reconciliation.py
│   └── visualization/
│       ├── performance_visualizer.py
│       └── simplified_visualizer.py
├── cyberdelta.egg-info/
├── docs/
│   ├── api_integration/
│   ├── component_details/
│   ├── configuration.md
│   ├── database_plan.md
│   ├── diagrams/
│   │   ├── architecture.mermaid
│   │   ├── database_integration.mermaid
│   │   └── frontend_architecture.mermaid
│   └── frontend_plan.md
├── examples/
│   └── config_example.py
├── main.py
├── mypy_ruff_error_count.sh
├── pyproject.toml
├── pyproject.toml.bak
├── README.md
├── requirements.txt
├── scripts/
│   └── merge_test_directories.py
├── setup.py
├── test_adapters.py
├── tests/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── test_config_consistency.py
│   ├── conftest.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── execution/
│   │   │   ├── __init__.py
│   │   │   └── test_synchronized_order_submission.py
│   │   ├── test_data_manager.py
│   │   ├── test_execution_handler.py
│   │   ├── test_portfolio_tracker.py
│   │   ├── test_signal_generator.py
│   │   └── test_signal_queue.py
│   ├── failure/
│   ├── integration/
│   │   ├── __init__.py
│   │   ├── conftest.py
│   │   ├── mocks/
│   │   │   └── mock_exchange.py
│   │   ├── test_backtesting.py
│   │   ├── test_core_workflow.py
│   │   ├── test_failure_scenarios.py
│   │   └── test_safety_systems.py
│   ├── run_api_tests.sh
│   ├── strategies/
│   │   ├── __init__.py
│   │   └── test_funding_rate_arbitrage.py
│   ├── test_config.py
│   ├── unit/
│   │   ├── conftest.py
│   │   ├── core/
│   │   │   └── test_symbol_mapper.py
│   │   ├── risk/
│   │   │   ├── __init__.py
│   │   │   ├── conftest.py
│   │   │   ├── test_rm_constraints.py
│   │   │   ├── test_rm_controls.py
│   │   │   ├── test_rm_dependencies.py
│   │   │   ├── test_rm_sizing_simple.py
│   │   │   ├── test_rm_sizing_standard.py
│   │   │   └── test_rm_validation.py
│   │   ├── test_backpack_api.py
│   │   ├── test_backtest_engine.py
│   │   ├── test_config_example.py
│   │   ├── test_config_security.py
│   │   ├── test_data_handler.py
│   │   ├── test_execution_handler.py
│   │   ├── test_hyperliquid_api.py
│   │   ├── test_performance_visualizer.py
│   │   ├── test_portfolio_tracker.py
│   │   ├── test_position_sizing_integration.py
│   │   ├── test_signal_queue.py
│   │   ├── test_simplified_visualizer.py
│   │   └── test_strategy_manager.py
│   └── validation/
│       ├── __init__.py
│       ├── test_circuit_breaker.py
│       ├── test_funding_rate_validator.py
│       ├── test_multi_tier_funding_provider.py
│       └── test_position_reconciliation.py
└── workflow/
    ├── 01_Current_Status_Summary.md
    ├── 02_Current_Context_and_Focus.md
    ├── 03_Active_Workflow_and_Rules.md
    ├── 04_Immediate_Next_Steps.md
    ├── 05_Future_Plans_and_Deferred_Items.md
    ├── 14.04.2025_code_report_Angel/
    │   ├── 00_Overview_and_Architecture.md
    │   ├── 01_Core_Engine_Components.md
    │   ├── 02_API_Clients.md
    │   ├── 03_Strategies.md
    │   ├── 04_Configuration_and_Secrets.md
    │   ├── 05_Safety_Systems.md
    │   ├── 06_Testing_Strategy.md
    │   ├── 07_Workflow_and_Progress.md
    │   ├── 08_Code_Quality_and_Style.md
    │   ├── 09_Dependencies_and_Environment.md
    │   └── 10_Recommendations.md
    ├── 14.04.2025_code_report_Junior/
    ├── skeleton_report/
    │   ├── relationships.md
    │   └── skeletons.md
    └── summary.md
```

# Code Skeletons

## cyberdelta/apis/backpack.py
```python
class BackpackAPI(ExchangeAPI):
    """API Client for Backpack Exchange."""
    def __init__(self, api_config: dict[str, Any], secrets: dict[str, str | None]) -> None:
        pass
    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        pass
    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        """Subscribe to a Backpack WebSocket topic."""
        pass
    # ... other methods truncated for brevity ...
    def _map_error_response(self, response_data: dict[str, Any] | str, status_code: int) -> APIError:
        """Map Backpack error responses to generic APIErrorCode."""
        pass
```

## cyberdelta/apis/base.py
```python
class APIErrorCode(Enum):
    pass

class APIError(Exception):
    """
    Custom exception for API-related errors.

    Attributes:
        message (str): A human-readable error message.
        code (APIErrorCode | None): A specific error code enum value, if applicable.
        status_code (int | None): The HTTP status code, if applicable.
        exchange (str | None): The name of the exchange where the error occurred.
        retry_after (int | None): Seconds to wait before retrying, if suggested by API.
        details (dict[str, Any] | None): Additional details about the error.
    """
    def __init__(self, message: str, code: APIErrorCode | None = None, status_code: int | None = None, exchange: str | None = None, retry_after: int | None = None, details: dict[str, Any] | None = None) -> None:
        pass
    @property
    def is_retryable(self) -> bool:
        pass

class RateLimiter:
    """
    Simple token bucket rate limiter.

    Attributes:
        rate (float): Tokens added per second.
        bucket_size (int): Maximum number of tokens in the bucket.
    """
    def __init__(self, rate: float, bucket_size: int) -> None:
        pass
    async def acquire(self) -> float:
        pass

class ExchangeAPI(ABC):
    """
    Abstract Base Class for exchange API clients.

    Handles common functionality like rate limiting, WebSocket connections,
    request signing (via subclasses), and error mapping.
    """
    def __init__(self, api_config: dict[str, Any], secrets: dict[str, str | None]) -> None:
        pass
    # ... other methods truncated ...
    @abstractmethod
    def _generate_client_order_id(self) -> str:
        ...
```

## cyberdelta/apis/hyperliquid.py
```python
class HyperliquidAPI(ExchangeAPI):
    """API Client for Hyperliquid DEX."""
    # ... methods truncated ...
    pass
```

## cyberdelta/config/config_manager.py
```python
class ConfigManager:
    """Manages loading, validation, and access to the application configuration."""
    def __init__(self, config_path: str | None = None):
        pass
    # ... methods truncated ...
```

## cyberdelta/config/secrets_manager.py
```python
class SecretsManager:
    """Manages loading and accessing sensitive secrets (e.g., API keys)."""
    def __init__(self) -> None:
        pass
    # ... methods truncated ...
```

## cyberdelta/core/data_handler.py
```python
class DataHandler:
    """
    Manages data feeds from multiple exchanges, including historical data collection
    (via REST) and real-time updates (via WebSocket).

    Acts as an observable, notifying registered observers (like strategies or the UI)
    of new MarketData updates.
    """
    # ... methods truncated ...
    pass
```

## cyberdelta/core/engine.py
```python
class Engine:
    """
    The main trading engine orchestrating strategies and data flow.

    This simplified version focuses on managing strategies and processing market data.
    It receives market data and passes it to relevant strategies.
    """
    def __init__(self, name: str = "CyberDeltaEngine") -> None:
        pass
    # ... methods truncated ...
```

## cyberdelta/core/execution_handler.py
```python
class TradeExecution:
    """
    Represents the state and results of executing a single arbitrage opportunity.

    Tracks the opportunity details, orders placed, fill information, status,
    and any errors encountered.
    """
    # ... methods truncated ...
    pass

class ExecutionHandler:
    """
    Handles the execution of sized arbitrage opportunities.

    Coordinates order placement across exchanges, manages retries,
    handles partial fills, calculates PnL for filled trades,
    and interacts with the Circuit Breaker system.
    """
    # ... methods truncated ...
    pass
```

## cyberdelta/core/portfolio_tracker.py
```python
class PortfolioTracker:
    """
    Tracks account balances, positions, and orders across multiple exchanges.

    Provides methods to update state based on trades, API fetches, and order updates.
    Calculates overall portfolio value, exposure, and PnL.
    """
    # ... methods truncated ...
    pass
```

## cyberdelta/core/risk_manager.py
```python
class SizedOpportunity:
    """
    Represents an arbitrage opportunity that has been sized by the RiskManager.
    Includes the original opportunity and the calculated trade sizes for each leg.
    """
    # ... methods truncated ...
    pass

class RiskManager:
    """
    Evaluates arbitrage opportunities and determines appropriate trade sizes.

    Considers factors like:
    - Configured risk parameters (max exposure, leverage, drawdown).
    - Kelly criterion (optional, based on historical volatility/returns).
    - Portfolio constraints (total capital, available balance).
    - Circuit breaker status.
    - Funding rate validation metrics.
    """
    # ... methods truncated ...
    pass
```

## cyberdelta/core/signal_generator.py
```python
class SignalGenerator:
    """
    Analyzes market data (tickers, funding rates) from DataHandler
    to identify potential arbitrage opportunities.

    Calculates relevant metrics like basis volatility and estimated slippage.
    """
    # ... methods truncated ...
    pass
```

## cyberdelta/core/signal_queue.py
```python
class PrioritySignalQueue:
    """
    Manages a queue of trade signals with prioritization and expiration.

    Handles adding signals, retrieving the highest priority signal, cleaning
    expired signals, and interacting with circuit breakers.
    """
    # ... methods truncated ...
    pass
```

## cyberdelta/core/strategy.py
```python
class Strategy(ABC):
    """
    Abstract Base Class for all trading strategies.

    Defines the interface for strategies, including processing data,
    managing parameters, and state.
    """
    # ... methods truncated ...
    pass
```

## cyberdelta/core/strategy_manager.py
```python
class StrategyManager:
    """
    Manages multiple trading strategies.

    Handles registration, enabling/disabling, and routing market data
    to the appropriate strategies based on symbols.
    Collects signals generated by strategies.
    """
    # ... methods truncated ...
    pass
```

## cyberdelta/strategies/funding_rate_arbitrage.py
```python
class FundingRateArbitrageStrategy(Strategy):
    """
    Implements a funding rate arbitrage strategy.

    Identifies opportunities based on funding rate differences between exchanges
    for the same underlying asset (represented by a symbol pair like BTC-USD_PERP/BTC-USD_SPOT
    or BTC-PERP/BTC-SPOT if using internal symbols).

    Generates entry signals when profitable arbitrage is detected and rebalance signals
    when positions need adjustment.
    """
    # ... methods truncated ...
    pass
```

## examples/config_example.py
```python
def main():
    pass
def create_example_files():
    """Create example configuration and secrets files in the correct locations."""
    pass
def run_benchmark(config_path, secrets_path):
    pass
```

## tests/unit/core/test_symbol_mapper.py
```python
def test_symbol_mapper_init_success() -> None:
    pass
def test_symbol_mapper_init_missing_exchanges_key() -> None:
    pass
# ... other test functions ...
```
## main.py
```python
async def shutdown(app_state: dict[str, Any]) -> None:
    """Perform graceful shutdown using cancellation token."""
    pass
def get_api_client(exchange_name: str, api_config: dict[str, Any], state_manager: StateManager) -> HyperliquidAPI | BackpackAPI:
    """Factory function to create API clients."""
    pass
async def main() -> None:
    """Main application entry point."""
    pass
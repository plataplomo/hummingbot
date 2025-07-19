# Portfolio Integration Examples

This directory contains comprehensive examples demonstrating how to use the refactored portfolio system.

## Overview

The portfolio system has been completely refactored from a monolithic `PortfolioTracker` class into a modular, maintainable architecture with the following key components:

- **Managers**: BalanceManager, PositionManager, OrderManager, PortfolioStateManager
- **Calculators**: PnL calculators, exposure calculators, performance calculators
- **Services**: Price service, symbol service, currency converter, validation, resilience
- **Infrastructure**: Event system, concurrency management, caching, monitoring

## Integration Examples

### 1. Basic Integration Example (`integration_example.py`)

Demonstrates the core functionality of the portfolio system:

```bash
python cyberdelta/core/portfolio/integration_example.py
```

**Features demonstrated:**
- Complete system initialization using the factory pattern
- Trade processing with validation and resilience
- Balance and position management
- Event-driven architecture
- P&L and exposure calculations
- Portfolio summary generation
- Resilience patterns (circuit breakers, retries, fallbacks)
- Validation and consistency checks
- Health monitoring and cleanup

### 2. Advanced Integration Example (`examples/portfolio/advanced_integration_example.py`)

Shows sophisticated usage patterns and custom strategies:

```bash
python examples/portfolio/advanced_integration_example.py
```

**Features demonstrated:**
- Custom portfolio rebalancing strategy
- Advanced risk management and monitoring
- Automated risk limit enforcement
- Performance analysis and reporting
- Integration of resilience and validation middleware
- Complex portfolio allocation algorithms
- Emergency risk reduction protocols

## Key Architecture Components

### Factory Pattern
```python
from cyberdelta.core.portfolio.factory import PortfolioComponentFactory

# Create complete portfolio system
portfolio_manager = PortfolioComponentFactory.create_portfolio_state_manager(
    config=config,
    symbol_mapper=symbol_mapper,
    api_clients=api_clients,
)
```

### Event-Driven Architecture
```python
# Events are automatically published for all portfolio changes
await event_dispatcher.register_handler(TradeProcessedEvent, trade_handler)
await event_dispatcher.register_handler(BalanceUpdatedEvent, balance_handler)
```

### Resilience Patterns
```python
# All operations support circuit breakers, retries, and fallbacks
result = await resilience_service.execute_with_resilience(
    service_name="trade_processing",
    func=portfolio_manager.process_trade,
    trade=trade,
)
```

### Validation System
```python
# Comprehensive data validation
validation_result = await validation_service.validate_trade(trade)
if not validation_result.is_valid:
    # Handle validation issues
    for issue in validation_result.issues:
        logger.warning("validation_issue", issue=issue.message)
```

### Risk Management
```python
# Real-time exposure monitoring
exposure_result = await portfolio_manager.calculate_portfolio_exposure("USD")
currency_exposure = await portfolio_manager.calculate_currency_exposure("USD")
```

## Configuration

The system supports comprehensive configuration:

```python
config = {
    "resilience": {
        "retry_max_attempts": 3,
        "circuit_breaker_failure_threshold": 5,
        "health_check_interval": 30.0,
    },
    "validation": {
        "trade_validation": {
            "min_price": "0.01",
            "max_price": "1000000",
            "strict_mode": True,
        },
    },
    "exposure": {
        "concentration_threshold": 0.3,
        "leverage_warning_threshold": 5.0,
    },
}
```

## Key Benefits

### 1. **Modular Architecture**
- Each component has a single responsibility
- Easy to test, maintain, and extend
- Loose coupling between components

### 2. **Fault Tolerance**
- Circuit breakers prevent cascading failures
- Automatic retry mechanisms with exponential backoff
- Graceful degradation with fallback handlers

### 3. **Data Integrity**
- Comprehensive validation of all data
- Consistency checks across components
- Business rule enforcement

### 4. **Observability**
- Complete event tracking for all changes
- Health monitoring and alerting
- Performance metrics and statistics

### 5. **Scalability**
- Async/await throughout for high concurrency
- Efficient caching and batching
- Proper resource management

## Usage Patterns

### Basic Usage
```python
# Initialize system
portfolio_manager = PortfolioComponentFactory.create_portfolio_state_manager(config)
await portfolio_manager.initialize()

# Process trades
await portfolio_manager.process_trade(trade)

# Get portfolio summary
summary = await portfolio_manager.get_portfolio_summary()
```

### Advanced Usage with Middleware
```python
# Create middleware for advanced patterns
resilience_middleware = ResilienceMiddleware(resilience_service)
validation_middleware = ValidationMiddleware(validation_service)

# Apply middleware to methods
@resilience_middleware.resilient("trade_processing")
@validation_middleware.validate_trade()
async def process_trade_with_middleware(trade):
    return await portfolio_manager.process_trade(trade)
```

### Custom Strategy Implementation
```python
class CustomPortfolioStrategy:
    def __init__(self, portfolio_manager, resilience_service, validation_service):
        self.portfolio_manager = portfolio_manager
        # ... setup middleware and services
    
    async def execute_custom_strategy(self):
        # Implement custom logic using all portfolio components
        pass
```

## Error Handling

The system provides structured error handling:

```python
try:
    await portfolio_manager.process_trade(trade)
except ValidationError as e:
    logger.error("validation_failed", validation_result=e.validation_result)
except ServiceUnavailableError as e:
    logger.error("service_unavailable", service=e.service_name)
except Exception as e:
    logger.error("unexpected_error", error=str(e))
```

## Testing

The system is designed to be easily testable:

```python
# Use test configuration
test_config = PortfolioComponentFactory.create_test_configuration()
portfolio_manager = PortfolioComponentFactory.create_portfolio_state_manager(test_config)

# Mock external dependencies
mock_api_clients = {"exchange1": MockExchangeAPI()}
portfolio_manager = PortfolioComponentFactory.create_portfolio_state_manager(
    config=test_config,
    api_clients=mock_api_clients,
)
```

## Performance Considerations

- **Batching**: Use batch operations for multiple trades/updates
- **Caching**: Price and symbol data is cached with configurable TTL
- **Concurrency**: Proper locking prevents race conditions
- **Resource Management**: Automatic cleanup of stale data

## Monitoring and Alerts

The system provides comprehensive monitoring:

```python
# Health status
health_status = resilience_service.get_resilience_status()

# Validation statistics
validation_stats = validation_service.get_validation_statistics()

# Component status
manager_stats = portfolio_manager.get_manager_stats()
```

## Next Steps

1. **Run the examples** to see the system in action
2. **Review the configuration options** for your specific needs
3. **Implement custom strategies** using the provided patterns
4. **Add monitoring and alerting** for production deployment
5. **Create comprehensive tests** for your specific use cases

For more detailed documentation, see the individual component files and their docstrings.
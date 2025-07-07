# Using Parametrized Fixtures in CyberDeltaEngine Tests

This document demonstrates the proper usage of parametrized fixtures from `conftest.py`.

## Available Parametrized Fixtures

The following fixtures in `conftest.py` support parametrization with scenarios: "default", "large", and "minimal":

- `sample_orders` - Sample orders organized by exchange
- `sample_positions` - Sample derivative positions organized by exchange
- `sample_balances_state` - Sample spot balances organized by exchange

## Usage Examples

### 1. Basic Parametrized Test

Use `@pytest.mark.parametrize` with the scenario name to test different data sets:

```python
@pytest.mark.parametrize("scenario,expected_count", [
    ("default", 3),  # default scenario has 3 positions
    ("large", 5),    # large scenario has 5 positions
    ("minimal", 1),  # minimal scenario has 1 position
])
def test_get_all_positions(
    self,
    portfolio_tracker: PortfolioTracker,
    scenario: str,
    expected_count: int,
) -> None:
    """Test retrieving all positions across all exchanges with different scenarios."""
    # Create scenario-specific positions
    from tests.unit.core.conftest import create_sample_positions, populate_nested_dict

    sample_positions = create_sample_positions(scenario)
    populate_nested_dict(portfolio_tracker.positions, sample_positions)

    all_positions = portfolio_tracker.get_all_positions()
    assert len(all_positions) == expected_count
```

### 2. Using Indirect Parametrization

When you need the fixture itself to be parametrized based on the scenario:

```python
@pytest.mark.parametrize("sample_orders,sample_balances_state", [("default", "default")], indirect=True)
def test_update_order(
    self,
    portfolio_tracker: PortfolioTracker,
    sample_orders: ExchangeOrders,
    sample_balances_state: ExchangeBalances,
    now: datetime,
) -> None:
    """Test updating an existing order with specific scenario data."""
    # The fixtures will be automatically created with the "default" scenario
    # sample_orders will contain the default order data
    # sample_balances_state will contain the default balance data

    # Populate the portfolio tracker with test data
    populate_nested_dict(portfolio_tracker.orders, sample_orders)
    populate_nested_dict(portfolio_tracker.balances, sample_balances_state)

    # Your test logic here...
```

### 3. Testing Multiple Scenarios Automatically

The parametrized fixtures will automatically run your test with all scenarios:

```python
def test_process_orders(
    self,
    portfolio_tracker: PortfolioTracker,
    sample_orders: ExchangeOrders,  # This will be parametrized automatically
) -> None:
    """Test order processing with different order sets."""
    # This test will run 3 times automatically:
    # 1. With sample_orders[default] - 3 orders
    # 2. With sample_orders[large] - 5 orders
    # 3. With sample_orders[minimal] - 1 order

    populate_nested_dict(portfolio_tracker.orders, sample_orders)

    # Test will automatically verify behavior with different data volumes
    order_count = sum(len(orders) for orders in portfolio_tracker.orders.values())
    assert order_count > 0
```

### 4. Combining Multiple Parametrized Fixtures

You can combine multiple parametrized fixtures to test interactions:

```python
@pytest.mark.parametrize(
    "sample_orders,sample_positions,sample_balances_state",
    [
        ("default", "default", "default"),
        ("large", "large", "large"),
        ("minimal", "minimal", "minimal"),
    ],
    indirect=True
)
def test_portfolio_state_consistency(
    self,
    portfolio_tracker: PortfolioTracker,
    sample_orders: ExchangeOrders,
    sample_positions: ExchangePositions,
    sample_balances_state: ExchangeBalances,
) -> None:
    """Test portfolio state consistency across different data volumes."""
    # Populate all data
    populate_nested_dict(portfolio_tracker.orders, sample_orders)
    populate_nested_dict(portfolio_tracker.positions, sample_positions)
    populate_nested_dict(portfolio_tracker.balances, sample_balances_state)

    # Verify consistency
    assert len(portfolio_tracker.orders) > 0
    assert len(portfolio_tracker.positions) > 0
    assert len(portfolio_tracker.balances) > 0
```

## Data Creation Functions

You can also use the data creation functions directly for custom scenarios:

```python
from tests.unit.core.conftest import (
    create_sample_orders,
    create_sample_positions,
    create_sample_balances,
)

def test_custom_scenario(self, portfolio_tracker: PortfolioTracker) -> None:
    """Test with custom data scenario."""
    # Create custom data
    custom_orders = create_sample_orders("large")
    custom_positions = create_sample_positions("minimal")
    custom_balances = create_sample_balances("default")

    # Mix and match scenarios as needed
    populate_nested_dict(portfolio_tracker.orders, custom_orders)
    populate_nested_dict(portfolio_tracker.positions, custom_positions)
    populate_nested_dict(portfolio_tracker.balances, custom_balances)
```

## Benefits of Parametrized Fixtures

1. **DRY Principle**: No duplicate fixture definitions across test files
2. **Consistent Test Data**: All tests use the same base data scenarios
3. **Easy Scaling**: Add new scenarios in one place (conftest.py)
4. **Better Coverage**: Automatically test edge cases (minimal) and stress cases (large)
5. **Type Safety**: Type aliases provide clear signatures

## Migration Guide

When updating existing tests to use parametrized fixtures:

1. Remove local fixture definitions for `sample_orders`, `sample_positions`, `sample_balances_state`
2. Import type aliases: `from .conftest import ExchangeBalances, ExchangeOrders, ExchangePositions`
3. Add parametrize decorators where you want specific scenarios
4. Use `populate_nested_dict` helper to populate portfolio tracker state
5. Update assertions to match the actual data in each scenario

## Scenario Data Reference

### Default Scenario
- Orders: 3 orders across exchanges (2 on hyperliquid, 1 on backpack)
- Positions: 3 positions (BTC and ETH on hyperliquid, ETH on backpack)
- Balances: USDC balances on both exchanges

### Large Scenario
- Orders: 5 orders with various types and statuses
- Positions: 5 positions including some with unrealized PnL
- Balances: Multiple assets including BTC and ETH

### Minimal Scenario
- Orders: 1 simple order
- Positions: 1 basic position
- Balances: 1 USDC balance

Always check the actual data in `conftest.py` for exact values when writing assertions.

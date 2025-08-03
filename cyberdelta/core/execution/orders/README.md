# Market Order Execution Module

This module provides programmatic market order functionality for CyberDeltaEngine, implementing market orders using aggressive IoC (Immediate-or-Cancel) limit orders since exchanges like Hyperliquid don't provide native market order endpoints.

## Overview

The market order module consists of:

- **MarketOrder**: Main executor class for market orders
- **MarketOrderService**: Service for calculating aggressive prices
- **MarketOrderConfig**: Configuration model with validation
- **MarketOrderMetrics**: Monitoring and observability
- **Error Hierarchy**: Custom exceptions for specific failure modes

## Installation

The market order module is included in CyberDeltaEngine. Import the components:

```python
from cyberdelta.core.execution.orders import (
    MarketOrder,
    MarketOrderService,
    MarketOrderConfig,
    MarketOrderMetrics,
    MarketOrderError,
    InsufficientLiquidityError,
    PriceDeviationError,
)
```

## Quick Start

### Basic Usage

```python
from decimal import Decimal
from cyberdelta.core.execution.orders import MarketOrder, MarketOrderService, MarketOrderConfig
from cyberdelta.models import OrderSide
from cyberdelta.core.signal_generator import SignalGenerator

# Initialize components
config = MarketOrderConfig(
    default_slippage_pct=Decimal("0.001"),  # 0.1%
    max_slippage_pct=Decimal("0.05"),        # 5% max
)

# Create service with optional signal generator for better slippage estimation
signal_generator = SignalGenerator(config=signal_config)
market_order_service = MarketOrderService(
    exchange_api=exchange_api,
    signal_generator=signal_generator,
    config=config
)

# Create market order executor
market_order = MarketOrder(
    exchange_api=exchange_api,
    market_order_service=market_order_service,
    config=config
)

# Execute a market buy order
try:
    order = await market_order.execute_market_order(
        symbol="BTC",
        side=OrderSide.BUY,
        quantity=Decimal("0.1"),
        max_slippage=Decimal("0.01")  # Optional: 1% max slippage override
    )

    if order.status == OrderStatus.FILLED:
        print(f"Market order filled at {order.price}")
    elif order.status == OrderStatus.PARTIALLY_FILLED:
        print(f"Partial fill: {order.quantity_filled}/{order.quantity}")
    else:
        print("Market order failed - no fill")

except InsufficientLiquidityError as e:
    print(f"Not enough liquidity: {e}")
except PriceDeviationError as e:
    print(f"Price deviation too high: {e}")
except MarketOrderError as e:
    print(f"Market order failed: {e}")
```

### Advanced Usage with Retry

```python
# Execute with retry logic for partial fills
order = await market_order.execute_market_order_with_retry(
    symbol="ETH",
    side=OrderSide.SELL,
    quantity=Decimal("10"),
    max_retries=2,  # Retry up to 2 times for partial fills
    client_order_id="MY_ORDER_001"
)
```

## Configuration

### MarketOrderConfig Options

```python
config = MarketOrderConfig(
    # Slippage settings
    default_slippage_pct=Decimal("0.001"),      # Default 0.1%
    max_slippage_pct=Decimal("0.05"),           # Max 5%

    # Price deviation limits
    max_price_deviation_pct=Decimal("0.10"),    # Max 10% from reference

    # Liquidity requirements
    min_liquidity_ratio=Decimal("2.0"),         # Need 2x order size in book

    # Symbol-specific overrides
    slippage_by_symbol={
        "BTC": Decimal("0.005"),                # 0.5% for BTC
        "ETH": Decimal("0.005"),                # 0.5% for ETH
        "SOL": Decimal("0.01"),                 # 1% for SOL
        "default": Decimal("0.02"),             # 2% for others
    },

    # Other settings
    enabled=True,                               # Enable/disable market orders
    use_all_mids_for_reference=False,          # Use AllMids endpoint
    order_timeout_seconds=10,                   # Order placement timeout
)
```

## Monitoring

### Using MarketOrderMetrics

```python
from cyberdelta.core.execution.orders import MarketOrderMetrics
import time

# Initialize metrics
metrics = MarketOrderMetrics(max_history=1000)

# Execute order with timing
start_time = time.time()
order = await market_order.execute_market_order(
    symbol="BTC",
    side=OrderSide.BUY,
    quantity=Decimal("1")
)
execution_time_ms = (time.time() - start_time) * 1000

# Record metrics
metrics.record_execution(
    symbol="BTC",
    side=OrderSide.BUY,
    requested_qty=Decimal("1"),
    filled_qty=order.quantity_filled or Decimal("0"),
    expected_price=Decimal("50000"),  # Your expected price
    actual_price=order.price,
    expected_slippage=config.get_slippage_for_symbol("BTC"),
    status=order.status,
    execution_time_ms=execution_time_ms
)

# Get statistics
symbol_stats = metrics.get_symbol_stats("BTC")
print(f"BTC fill rate: {symbol_stats['avg_fill_rate']:.1f}%")
print(f"BTC avg slippage: {symbol_stats['avg_actual_slippage']:.3f}")

overall_stats = metrics.get_overall_stats()
print(f"Overall success rate: {overall_stats['success_rate']:.1f}%")
```

## Integration with Trading Strategies

### Example: Delta-Neutral Strategy

```python
class DeltaNeutralStrategy:
    def __init__(self, long_exchange: ExchangeAPI, short_exchange: ExchangeAPI):
        # Initialize market order executors for each exchange
        self.long_market_order = self._create_market_order_executor(long_exchange)
        self.short_market_order = self._create_market_order_executor(short_exchange)

    def _create_market_order_executor(self, exchange: ExchangeAPI) -> MarketOrder:
        config = MarketOrderConfig()
        service = MarketOrderService(exchange, config=config)
        return MarketOrder(exchange, service, config)

    async def execute_arbitrage(self, symbol: str, size: Decimal):
        # Execute market orders on both exchanges
        tasks = [
            self.long_market_order.execute_market_order(
                symbol=symbol,
                side=OrderSide.BUY,
                quantity=size
            ),
            self.short_market_order.execute_market_order(
                symbol=symbol,
                side=OrderSide.SELL,
                quantity=size
            )
        ]

        long_order, short_order = await asyncio.gather(*tasks)

        # Check execution
        if long_order.status == OrderStatus.FILLED and short_order.status == OrderStatus.FILLED:
            print(f"Arbitrage executed: Long @ {long_order.price}, Short @ {short_order.price}")
        else:
            print("Arbitrage failed - incomplete fills")
```

## Error Handling

### Specific Error Types

```python
try:
    order = await market_order.execute_market_order(...)

except InsufficientLiquidityError as e:
    # Not enough liquidity in order book
    print(f"Symbol: {e.symbol}")
    print(f"Requested: {e.requested_quantity}")
    print(f"Available: {e.available_quantity}")

except PriceDeviationError as e:
    # Price deviation exceeds configured limits
    print(f"Symbol: {e.symbol}")
    print(f"Aggressive price: {e.aggressive_price}")
    print(f"Reference price: {e.reference_price}")
    print(f"Deviation: {e.deviation_pct:.2%}")

except MarketOrderError as e:
    # General market order error
    print(f"Market order failed: {e}")
```

## Best Practices

1. **Always Set Reasonable Slippage Limits**
   - Use symbol-specific slippage configurations
   - Monitor actual vs expected slippage

2. **Check Liquidity Before Large Orders**
   ```python
   order_book = await exchange_api.get_order_book(symbol)
   liquidity_ratio = market_order_service.calculate_liquidity_ratio(
       order_book, side, quantity
   )
   if liquidity_ratio < 1.5:
       print("Warning: Low liquidity")
   ```

3. **Use Metrics for Optimization**
   - Track fill rates by symbol
   - Analyze slippage patterns
   - Adjust configurations based on data

4. **Handle Partial Fills**
   - Use retry logic for important orders
   - Consider breaking large orders into smaller chunks

5. **Test in Paper Trading First**
   - Validate configurations
   - Understand exchange behavior
   - Fine-tune slippage estimates

## Limitations

1. **Not True Market Orders**: Uses aggressive IoC limit orders
2. **Slippage Risk**: Actual slippage may exceed estimates
3. **Liquidity Dependent**: Won't fill if insufficient liquidity
4. **Exchange Specific**: Behavior varies by exchange

## Troubleshooting

### Common Issues

1. **"Market orders are not supported by Hyperliquid API"**
   - This is expected - use the MarketOrder component instead
   - The API error message directs you to this module

2. **InsufficientLiquidityError**
   - Check order book depth before placing large orders
   - Consider reducing order size or using limit orders

3. **High Slippage**
   - Review and adjust symbol-specific slippage configs
   - Check market volatility
   - Consider using smaller order sizes

4. **Timeouts**
   - Increase `order_timeout_seconds` in config
   - Check exchange connectivity
   - Monitor exchange status

## Contributing

To contribute to the market order module:

1. Add tests for new functionality
2. Update documentation
3. Follow existing code patterns
4. Ensure type safety with Pydantic models

## License

This module is part of CyberDeltaEngine and follows the same license terms.

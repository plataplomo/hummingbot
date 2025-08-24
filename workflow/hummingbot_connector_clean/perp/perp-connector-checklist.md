# Perpetual Connector v2.1 Developer Checklist

## Overview
This guide provides a comprehensive checklist for developing perpetual futures connectors that integrate with Hummingbot's V2 strategies. Perpetual connectors handle derivatives trading with leverage, position management, and funding rates.

## Key Differences from Spot Connectors

Perpetual connectors have additional requirements:
- **Position Management**: Track and manage open positions
- **Leverage**: Support for leverage configuration
- **Funding Rates**: Handle funding fee payments
- **Position Modes**: Support one-way or hedge position modes
- **Collateral Management**: Handle margin and collateral requirements

## File Structure

### Main Connector Files
```
hummingbot/hummingbot/connector/derivative/connector_name_perpetual/
  __init__.py
  connector_name_perpetual_api_order_book_data_source.py
  connector_name_perpetual_api_user_stream_data_source.py
  connector_name_perpetual_auth.py
  connector_name_perpetual_constants.py
  connector_name_perpetual_derivative.py  # Note: 'derivative' instead of 'exchange'
  connector_name_perpetual_order_book.py
  connector_name_perpetual_utils.py
  connector_name_perpetual_web_utils.py
  dummy.pxd
  dummy.pyx
```

### Test Files
```
hummingbot/test/hummingbot/connector/derivative/connector_name_perpetual/
  __init__.py
  test_connector_name_perpetual_api_order_book_data_source.py
  test_connector_name_perpetual_api_user_stream_data_source.py
  test_connector_name_perpetual_auth.py
  test_connector_name_perpetual_derivative.py
  test_connector_name_perpetual_order_book.py
  test_connector_name_perpetual_utils.py
  test_connector_name_perpetual_web_utils.py
```

## Class Inheritance

### Main Exchange Class
```python
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.perpetual_trading import PerpetualTrading

class ConnectorNamePerpetualDerivative(ExchangeBase, PerpetualTrading):
    """
    Multiple inheritance from ExchangeBase and PerpetualTrading
    PerpetualTrading provides perpetual-specific functionality
    """
    pass
```

## API Requirements Checklist

### Public REST Endpoints (Required)
All spot connector requirements PLUS:
- **GET FUNDING INFO**: Current funding rate and next payment time
- **GET MARKET INFO**: Including contract specifications

### Private REST Endpoints (Required)
All spot connector requirements PLUS:
- **CHECK POSITIONS**: REST API endpoint to check user positions
  - Example: https://binance-docs.github.io/apidocs/futures/en/#position-information-v2-user_data
- **CONFIGURE LEVERAGE**: REST API endpoint to configure leverage
  - Example: https://binance-docs.github.io/apidocs/futures/en/#change-initial-leverage-trade
- **GET FUNDING HISTORY**: Historical funding payments
- **SET POSITION MODE**: Change between one-way and hedge modes (if supported)

### WebSocket Channels (Required)
All spot connector requirements PLUS:
- **POSITION UPDATES**: Real-time position changes
- **FUNDING RATE UPDATES**: Funding rate changes
- **LIQUIDATION EVENTS**: Position liquidation notifications

## PerpetualTrading Methods to Implement

Methods specific to perpetual trading functionality:

### From PerpetualDerivativePyBase
1. `funding_fee_poll_interval` - Interval for polling funding fees
2. `supported_position_modes` - List of supported position modes (One-way, Hedge)
3. `get_buy_collateral_token` - Token used as collateral for long positions
4. `get_sell_collateral_token` - Token used as collateral for short positions

### Position Management
```python
# Logic to get the current leverage from the exchange and to set it in the exchange
async def set_leverage(self, trading_pair: str, leverage: int) -> dict:
    pass

# Provide the supported position modes and change them in the exchange
async def set_position_mode(self, position_mode: PositionMode) -> dict:
    pass

# Method to get the current funding information
async def get_funding_info(self, trading_pair: str) -> FundingInfo:
    pass

# Logic to keep positions status updated
async def _update_positions(self) -> None:
    pass
```

## ExchangePyBase Methods (Same as Spot)

All the same methods from spot connectors PLUS additional perpetual-specific implementations:

1. `authenticator`
2. `name`
3. `rate_limits_rules`
4. `domain`
5. `client_order_id_max_length`
6. `client_order_id_prefix`
7. `trading_rules_request_path`
8. `trading_pairs_request_path`
9. `check_network_request_path`
10. `trading_pairs`
11. `is_cancel_request_in_exchange_synchronous`
12. `is_trading_required`
13. `supported_order_types`
14. `_is_request_exception_related_to_time_synchronizer`
15. `_create_web_assistants_factory`
16. `_create_order_book_data_source`
17. `_create_user_stream_data_source`
18. `_get_fee`
19. `_place_order`
20. `_place_cancel`
21. `_format_trading_rules`
22. `_status_polling_loop_fetch_updates`
23. `_update_trading_fees`
24. `_user_stream_event_listener`
25. `_all_trade_updates_for_order`
26. `_request_order_status`
27. `_update_balances`
28. `_initialize_trading_pair_symbols_from_exchange_info`
29. `_get_last_traded_price`

## Implementation Steps

### 1. Order Book Data Source
```python
# connector_name_perpetual_api_order_book_data_source.py

# Copy the Bybit Perpetual connector order book data source file.
# Replace Bybit for `connector_name` with the first letter in uppercase.
# Replace bybit for `connector_name`.
# Replace the `HEARTBEAT_TIME_INTERVAL` with the appropriate value.
```

### 2. User Stream Data Source
```python
# connector_name_perpetual_api_user_stream_data_source.py

# Copy the Bybit perpetual connector user stream data source file.
# Replace Bybit for `connector_name` with the first letter in uppercase.
# Replace bybit for `connector_name`.
# Replace the `HEARTBEAT_TIME_INTERVAL` with the appropriate value.
# Check if you need `LISTEN_KEY_KEEP_ALIVE_INTERVAL`.
```

### 3. Main Derivative Class
The main class must inherit from both `ExchangeBase` and `PerpetualTrading`:

```python
class ConnectorNamePerpetualDerivative(ExchangeBase, PerpetualTrading):
    def __init__(self, ...):
        super().__init__(...)
        # Perpetual specific initializations
        self._funding_info = {}
        self._positions = {}
        self._leverage = {}
```

## Funding Information Handling

### OrderBookTrackerDataSource Extension
For perpetual connectors, the `OrderBookTrackerDataSource` also maintains funding information:

```python
class OrderBookTrackerDataSource:
    async def listen_for_funding_info(self):
        """
        Listen to funding rate updates from WebSocket
        """
        pass

    async def get_funding_info(self, trading_pair: str):
        """
        Fetch current funding info via REST API
        """
        pass
```

## Position Tracking

Perpetual connectors must track open positions:

```python
# In the main derivative class
async def _update_positions(self):
    """
    Regularly update position status
    Called from status polling loop
    """
    positions = await self._request_positions()
    for position in positions:
        # Update internal position tracking
        self._positions[position.trading_pair] = position
```

## Supported Features

### Order Types
Typical supported order types:
- `LIMIT`
- `LIMIT_MAKER`
- `MARKET`

### Position Modes
- **One-way**: Single position per trading pair (long OR short)
- **Hedge**: Separate long and short positions per trading pair

## Testing Requirements

### Additional Test Cases for Perpetuals

Beyond spot connector tests, add:

1. **Position Management Tests**
   - Opening positions
   - Closing positions
   - Position updates via WebSocket
   - Position liquidation handling

2. **Leverage Tests**
   - Setting leverage
   - Getting current leverage
   - Leverage limits validation

3. **Funding Rate Tests**
   - Funding info updates
   - Funding payment calculations
   - Historical funding data

4. **Position Mode Tests**
   - Switching between one-way and hedge modes
   - Mode-specific order placement

## Example Connectors for Reference

Study these existing perpetual connectors:

### Centralized Exchanges
- **Binance Perpetual**: `binance_perpetual`
  - Location: `hummingbot/connector/derivative/binance_perpetual/`
  - Supports: One-way and Hedge modes

- **Bybit Perpetual**: `bybit_perpetual`
  - Location: `hummingbot/connector/derivative/bybit_perpetual/`
  - Good reference for implementation patterns

- **OKX Perpetual**: `okx_perpetual`
  - Supports advanced position management

- **Gate.io Perpetual**: `gate_io_perpetual`
  - Clear implementation structure

### Decentralized Exchanges
- **dYdX v4**: `dydx_v4_perpetual`
  - WebSocket-based
  - One-way position mode only

- **Hyperliquid**: `hyperliquid_perpetual`
  - Modern implementation
  - One-way position mode

- **Derive**: `derive_perpetual`
  - WebSocket connection
  - Includes leverage calculations

## Key Implementation Considerations

### 1. Margin and Collateral
- Track available margin
- Monitor margin ratio
- Handle margin calls
- Implement position size calculations based on leverage

### 2. Funding Payments
- Track funding timestamps
- Calculate funding fees
- Update user balance after funding payments
- Emit funding payment events

### 3. Position Lifecycle
- Opening positions (with leverage)
- Modifying positions (add/reduce)
- Closing positions (market/limit)
- Forced liquidations

### 4. Risk Management
- Position limits
- Leverage limits
- Margin requirements
- Auto-deleveraging handling

## Configuration

### Global Configuration
Add to `conf_global_TEMPLATE.yml`:

```yaml
connector_name_perpetual_api_key: ""
connector_name_perpetual_api_secret: ""
```

### Testnet Support
Many perpetual exchanges offer testnet environments:

```yaml
connector_name_perpetual_testnet_api_key: ""
connector_name_perpetual_testnet_api_secret: ""
```

## Common Pitfalls to Avoid

1. **Not handling position updates properly**: Positions can change due to funding, liquidations, or partial fills
2. **Incorrect leverage calculations**: Different exchanges calculate leverage differently
3. **Missing funding rate updates**: Can lead to incorrect P&L calculations
4. **Not distinguishing between reduce-only and regular orders**
5. **Improper position mode handling**: Mixing one-way and hedge mode logic

## Debugging Tips

1. **Use testnet first**: Most perpetual exchanges provide testnet environments
2. **Log position updates**: Track all position changes for debugging
3. **Monitor funding payments**: Ensure funding is correctly applied
4. **Test liquidation scenarios**: Use high leverage on testnet to trigger liquidations
5. **Verify margin calculations**: Cross-check with exchange UI

## Submission Requirements

Same as spot connectors:

1. **Complete connector folder** with all required files
2. **Pass Developer and QA Checklists**
3. **Unit test coverage**
4. **Documentation PR** to hummingbot-site
5. **Inline comments** for complex logic

## Resources

- [Perp Connector v2.1 Notion Template](https://hummingbot-foundation.notion.site/Perp-Connector-v2-1-57d8391eb54c40929f77067355fd551e)
- [Perp Connector QA Checklist](/developers/connectors/test-perp/)
- [Building Connectors Guide](/developers/connectors/)
- Example implementations in `hummingbot/connector/derivative/`

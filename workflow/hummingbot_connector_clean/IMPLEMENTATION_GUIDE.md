# Hummingbot Connector Implementation Guide

## Overview

This guide provides step-by-step instructions for implementing new connectors for Hummingbot. We cover both **Spot** and **Perpetual (Derivative)** connectors.

## Connector Types

### 1. **CLOB Spot Connectors**
- WebSocket-based connectors to exchange's spot order book markets
- Located in: `hummingbot/connector/exchange/`
- Examples: Binance, OKX, Kraken

### 2. **CLOB Perp Connectors**
- WebSocket-based connectors to exchange's perpetual futures markets
- Located in: `hummingbot/connector/derivative/`
- Convention: Names end with `_perpetual`
- Examples: binance_perpetual, hyperliquid_perpetual, dydx_v4_perpetual

## Development Standards

### Architecture Principles
1. **WebSocket-first**: All connectors must use WebSocket for real-time data
2. **Type Safety**: Use proper type hints and domain types
3. **No Hardcoding**: All values from configuration or API
4. **Fail Fast**: No silent failures or fallbacks
5. **Test Coverage**: Comprehensive unit tests required

### Key Classes and Inheritance

#### Spot Connectors
```python
from hummingbot.connector.exchange_base import ExchangeBase

class ConnectorNameExchange(ExchangeBase):
    pass
```

#### Perpetual Connectors
```python
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.perpetual_trading import PerpetualTrading

class ConnectorNamePerpetualDerivative(ExchangeBase, PerpetualTrading):
    pass
```

## Implementation Workflow

### Phase 1: Research & Planning

1. **Study Exchange API Documentation**
   - REST endpoints
   - WebSocket channels
   - Authentication methods
   - Rate limits
   - Trading rules

2. **Review Reference Implementations**
   - For Spot: Study Binance connector
   - For Perp: Study Bybit Perpetual or Binance Perpetual

3. **Create Connector Structure**
   - Copy template files from reference connector
   - Rename all files and classes appropriately

### Phase 2: Core Implementation

#### Step 1: Constants & Configuration
```python
# connector_name_constants.py
DEFAULT_DOMAIN = "api.exchange.com"
REST_URLS = {DEFAULT_DOMAIN: "https://api.exchange.com"}
WSS_URLS = {DEFAULT_DOMAIN: "wss://stream.exchange.com"}

RATE_LIMITS = [
    RateLimit(limit_id=CONSTANTS.SNAPSHOT_ENDPOINT, limit=10, time_interval=60),
    # Add all rate limits
]
```

#### Step 2: Authentication
```python
# connector_name_auth.py
class ConnectorNameAuth(AuthBase):
    def add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        # Add authentication to request parameters
        pass

    def header_for_authentication(self) -> Dict[str, str]:
        # Return headers with authentication
        pass

    def _generate_signature(self, data: str) -> str:
        # Generate request signature
        pass
```

#### Step 3: Web Utils
```python
# connector_name_web_utils.py
def build_api_factory() -> WebAssistantsFactory:
    # Create API factory with or without time synchronizer
    pass

def get_current_server_time() -> float:
    # Get server time for synchronization
    pass
```

#### Step 4: Order Book Data Source
```python
# connector_name_api_order_book_data_source.py
class ConnectorNameAPIOrderBookDataSource(OrderBookTrackerDataSource):
    async def listen_for_order_book_diffs(self, output: asyncio.Queue):
        # Stream order book updates via WebSocket
        pass

    async def listen_for_order_book_snapshots(self, output: asyncio.Queue):
        # Get order book snapshots
        pass

    async def listen_for_trades(self, output: asyncio.Queue):
        # Stream trade data
        pass
```

#### Step 5: User Stream Data Source
```python
# connector_name_api_user_stream_data_source.py
class ConnectorNameAPIUserStreamDataSource(UserStreamTrackerDataSource):
    async def listen_for_user_stream(self, output: asyncio.Queue):
        # Stream user updates (orders, balances, positions)
        pass
```

#### Step 6: Main Exchange/Derivative Class
Implement all required methods from ExchangePyBase:
- Trading operations: `_place_order`, `_place_cancel`
- Market data: `_get_last_traded_price`
- Account data: `_update_balances`
- Trading rules: `_format_trading_rules`
- Event handling: `_user_stream_event_listener`

### Phase 3: Testing

#### Unit Tests Structure
Each component needs comprehensive tests:

1. **Authentication Tests**
   - REST authentication
   - WebSocket authentication
   - Signature generation

2. **Order Book Tests**
   - Snapshot retrieval
   - Diff processing
   - Trade stream handling

3. **User Stream Tests**
   - Connection handling
   - Event processing
   - Reconnection logic

4. **Exchange/Derivative Tests**
   - Order placement
   - Order cancellation
   - Balance updates
   - Trading rules
   - Fee calculations

#### Test Implementation Pattern
```python
class TestConnectorNameExchange(TestCase):
    def setUp(self):
        # Setup mocks and test instance
        pass

    @aioresponses()
    def test_place_order_successfully(self, mock_api):
        # Mock API responses
        # Execute order placement
        # Assert expected behavior
        pass
```

### Phase 4: Additional Requirements for Perpetuals

#### Position Management
```python
async def _update_positions(self):
    """Update open positions from exchange"""
    pass

async def set_leverage(self, trading_pair: str, leverage: int):
    """Set leverage for a trading pair"""
    pass

async def set_position_mode(self, position_mode: PositionMode):
    """Switch between one-way and hedge modes"""
    pass
```

#### Funding Rate Handling
```python
async def get_funding_info(self, trading_pair: str) -> FundingInfo:
    """Get current funding rate and payment time"""
    pass

async def _update_funding_rates(self):
    """Update funding rates periodically"""
    pass
```

## Quality Assurance Checklist

### Code Quality
- [ ] No hardcoded values
- [ ] All strings use constants or enums
- [ ] Proper error handling (fail fast)
- [ ] Comprehensive logging
- [ ] Type hints on all methods
- [ ] Docstrings on public methods

### Functionality
- [ ] Connect with valid API keys
- [ ] Handle invalid/expired keys gracefully
- [ ] Place buy/sell orders (limit and market)
- [ ] Cancel orders
- [ ] Track order status updates
- [ ] Update balances correctly
- [ ] Handle rate limits
- [ ] Reconnect on disconnection

### Testing
- [ ] Unit test coverage > 80%
- [ ] Integration tests pass
- [ ] Manual testing completed
- [ ] Testnet verification done

### Documentation
- [ ] Inline code comments
- [ ] Configuration examples
- [ ] API requirements documented
- [ ] Known limitations listed

## Common Pitfalls & Solutions

### 1. WebSocket Message Handling
**Problem**: Missing or incorrectly parsed messages
**Solution**: Log raw messages during development, validate against exchange docs

### 2. Order Status Tracking
**Problem**: Orders stuck in pending state
**Solution**: Implement proper status polling and WebSocket event handling

### 3. Rate Limiting
**Problem**: Getting banned or throttled
**Solution**: Implement proper rate limiters with safety margins

### 4. Decimal Precision
**Problem**: Order rejections due to precision issues
**Solution**: Use exchange's tick_size and step_size from trading rules

### 5. Time Synchronization
**Problem**: Authentication failures due to time drift
**Solution**: Implement time synchronizer if required by exchange

## Submission Process

1. **Complete Implementation**
   - All required files present
   - Tests passing
   - Documentation complete

2. **Create Pull Request**
   - Target: `development` branch
   - Include:
     - Connector code
     - Unit tests
     - Documentation updates

3. **Submit Proposal**
   - Create New Connector Proposal (NCP)
   - Post in Hummingbot NCP Snapshot
   - Include HBOT stake if required

## Resources

### Documentation
- [Spot Connector v2.1 Template](https://hummingbot-foundation.notion.site/Spot-Connector-v2-1-1cc43830938445c9974f43ef861d59f1)
- [Perp Connector v2.1 Template](https://hummingbot-foundation.notion.site/Perp-Connector-v2-1-57d8391eb54c40929f77067355fd551e)
- [Hummingbot Developer Docs](https://docs.hummingbot.org/developers/)

### Reference Implementations
- **Best Spot Reference**: `hummingbot/connector/exchange/binance/`
- **Best Perp Reference**: `hummingbot/connector/derivative/binance_perpetual/`
- **Modern Implementation**: `hummingbot/connector/derivative/hyperliquid_perpetual/`

### Community
- [Discord](https://discord.gg/hummingbot)
- [Github Discussions](https://github.com/hummingbot/hummingbot/discussions)
- [Foundation Site](https://hummingbot.org)

## Final Notes

- **Start with testnet**: Most exchanges provide test environments
- **Use existing patterns**: Don't reinvent the wheel
- **Ask for help**: Community and maintainers are helpful
- **Test thoroughly**: Financial software requires high reliability
- **Document edge cases**: Help future maintainers

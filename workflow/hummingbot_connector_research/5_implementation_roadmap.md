# Backpack Connector Implementation Roadmap

## Overview

This roadmap outlines the step-by-step implementation of a Hummingbot connector for Backpack exchange, leveraging CyberDelta's existing API implementation.

## Phase 1: Foundation (Day 1-2)

### 1.1 Environment Setup
- [ ] Switch to hummingbot-dev worktree
- [ ] Verify conda environment with Python 3.10
- [ ] Install Hummingbot dependencies
- [ ] Set up development environment

```bash
cd /workspaces/CyberDeltaEngine/worktrees/hummingbot-dev
conda activate hummingbot
./install
./compile
```

### 1.2 Create Connector Structure
- [ ] Create connector directory structure
- [ ] Add __init__.py files
- [ ] Create placeholder files

```bash
mkdir -p hummingbot/connector/exchange/backpack
touch hummingbot/connector/exchange/backpack/{__init__.py,backpack_exchange.py,backpack_auth.py,backpack_constants.py,backpack_utils.py,backpack_web_utils.py}
```

### 1.3 Import CyberDelta API
- [ ] Add CyberDelta to Python path
- [ ] Test import of BackpackAPI
- [ ] Verify authentication works

```python
import sys
sys.path.append('/workspaces/CyberDeltaEngine')
from cyberdelta.apis.backpack import BackpackAPI
```

## Phase 2: Core Implementation (Day 3-5)

### 2.1 Constants and Configuration
```python
# backpack_constants.py
DEFAULT_DOMAIN = "backpack"
REST_URL = "https://api.backpack.exchange"
WSS_URL = "wss://ws.backpack.exchange"

# Endpoints
EXCHANGE_INFO_URL = "/api/v1/markets"
ORDER_URL = "/api/v1/order"
BALANCE_URL = "/api/v1/capital"

# Rate Limits
RATE_LIMITS = [
    RateLimit(limit_id=ORDER_URL, limit=30, time_interval=1),
    RateLimit(limit_id=BALANCE_URL, limit=10, time_interval=1),
]

MAX_ORDER_ID_LEN = 36
BROKER_ID = "HMB"
```

### 2.2 Authentication Wrapper
```python
# backpack_auth.py
class BackpackAuth(AuthBase):
    def __init__(self, api_key: str, api_secret: str):
        # Initialize CyberDelta authenticator
        self._cd_auth = BackpackEd25519Authenticator(api_key, api_secret)

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        # Wrap CyberDelta auth
        pass
```

### 2.3 Main Exchange Class - Minimal
```python
# backpack_exchange.py - Start with required properties only
class BackpackExchange(ExchangePyBase):
    @property
    def name(self) -> str:
        return "backpack"

    @property
    def authenticator(self) -> AuthBase:
        return self._auth

    # Implement all required abstract properties
    # Raise NotImplementedError for complex methods initially
```

### 2.4 Utility Functions
```python
# backpack_utils.py
def split_trading_pair(trading_pair: str) -> Tuple[str, str]:
    """BTC-USDC -> (BTC, USDC)"""
    return tuple(trading_pair.split("-"))

def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    """Convert Hummingbot format to Backpack format"""
    # Backpack uses underscore: BTC_USDC
    return hb_trading_pair.replace("-", "_")
```

## Phase 3: Order Operations (Day 6-8)

### 3.1 Order Placement
```python
async def _place_order(
    self,
    order_id: str,
    trading_pair: str,
    amount: Decimal,
    order_type: OrderType,
    is_buy: bool,
    price: Optional[Decimal] = None,
) -> str:
    """
    Minimal implementation:
    1. Convert types
    2. Call CyberDelta API
    3. Track order
    4. Emit event
    5. Return exchange order ID
    """
    # Implementation using wrapper pattern
```

### 3.2 Order Cancellation
```python
async def _cancel(self, trading_pair: str, order_id: str) -> bool:
    """
    Minimal implementation:
    1. Get exchange order ID from mapping
    2. Call CyberDelta cancel
    3. Update tracking
    4. Emit event
    """
```

### 3.3 Balance Updates
```python
async def _update_balances(self):
    """
    Minimal implementation:
    1. Call CyberDelta get_account_summary
    2. Update self._account_balances
    3. Update self._account_available_balances
    """
```

## Phase 4: Data Sources (Day 9-11)

### 4.1 User Stream Data Source (Priority)
```python
# backpack_api_user_stream_data_source.py
class BackpackAPIUserStreamDataSource(UserStreamTrackerDataSource):
    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Minimal: Just balance and order updates
        """
        # Mock implementation initially
        while True:
            await asyncio.sleep(30)
            # Push mock balance update
            output.put_nowait({"type": "balance_update"})
```

### 4.2 Order Book Data Source (Can be minimal)
```python
# backpack_api_order_book_data_source.py
class BackpackAPIOrderBookDataSource(OrderBookTrackerDataSource):
    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        """
        Minimal: Return empty order book
        """
        return OrderBook()

    # Other methods can raise NotImplementedError initially
```

## Phase 5: Integration (Day 12-13)

### 5.1 Trading Rules
```python
async def _format_trading_rules(self, exchange_info: Dict[str, Any]) -> List[TradingRule]:
    """
    Parse Backpack market info into TradingRule objects
    Minimal: Set reasonable defaults
    """
    rules = []
    for market in exchange_info.get("markets", []):
        rules.append(TradingRule(
            trading_pair=market["symbol"],
            min_order_size=Decimal("0.001"),
            max_order_size=Decimal("10000"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.001"),
        ))
    return rules
```

### 5.2 Web Utils
```python
# backpack_web_utils.py
def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    """Create web assistants factory"""
    # Minimal implementation
    return WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
    )
```

## Phase 6: Testing (Day 14-15)

### 6.1 Unit Tests
```python
# test/hummingbot/connector/exchange/backpack/test_backpack_exchange.py
class TestBackpackExchange(unittest.TestCase):
    def test_name_property(self):
        exchange = BackpackExchange(...)
        self.assertEqual(exchange.name, "backpack")

    # Test each implemented method
```

### 6.2 Integration Test with Testnet
```python
# Manual test script
async def test_connector():
    exchange = BackpackExchange(
        api_key="testnet_key",
        api_secret="testnet_secret",
        trading_pairs=["SOL-USDC"],
    )

    # Test balance query
    await exchange._update_balances()
    print(f"Balances: {exchange._account_balances}")

    # Test order placement
    order_id = get_new_client_order_id(is_buy=True, trading_pair="SOL-USDC")
    exchange_order_id = await exchange._place_order(
        order_id=order_id,
        trading_pair="SOL-USDC",
        amount=Decimal("0.01"),
        order_type=OrderType.LIMIT,
        is_buy=True,
        price=Decimal("100.0")
    )
    print(f"Placed order: {exchange_order_id}")
```

## Phase 7: Hummingbot Integration (Day 16-17)

### 7.1 Register Connector
```python
# hummingbot/connector/connector_status.py
CONNECTOR_STATUS = {
    "backpack": ConnectorStatus(
        name="backpack",
        status=ConnectorStatusType.YELLOW,  # Beta status
        details="Community contributed connector"
    )
}
```

### 7.2 Add to Factory
```python
# hummingbot/client/config/config_helpers.py
CONNECTOR_FACTORY = {
    "backpack": "hummingbot.connector.exchange.backpack.backpack_exchange.BackpackExchange"
}
```

### 7.3 Test with Hummingbot CLI
```bash
./start
>>> connect backpack
>>> balance
>>> create
```

## Phase 8: WebSocket Implementation (Day 18-20)

### 8.1 Real User Stream
- [ ] Implement WebSocket connection
- [ ] Handle authentication
- [ ] Process order updates
- [ ] Process balance updates
- [ ] Handle reconnection

### 8.2 Order Book Stream (Optional)
- [ ] Subscribe to order book updates
- [ ] Process snapshots
- [ ] Process incremental updates
- [ ] Maintain order book state

## Minimal Working Connector Checklist

### Required for Basic Trading:
- [x] Main exchange class with properties
- [x] Order placement method
- [x] Order cancellation method
- [x] Balance update method
- [x] Auth wrapper
- [x] Constants file
- [x] Utils for trading pair conversion
- [x] Minimal user stream (can be polling)
- [x] Minimal order book (can return empty)
- [x] Trading rules (can use defaults)

### Can Be Implemented Later:
- [ ] Real-time WebSocket updates
- [ ] Order book maintenance
- [ ] Trade history
- [ ] Funding payments (for perps)
- [ ] Advanced order types
- [ ] Position tracking (for perps)

## Implementation Priority

### Week 1: Get It Working
1. **Day 1-2**: Setup and structure
2. **Day 3-5**: Core wrapper implementation
3. **Day 6-7**: Order operations

### Week 2: Make It Reliable
1. **Day 8-9**: Data sources
2. **Day 10-11**: Integration
3. **Day 12-13**: Testing

### Week 3: Make It Production-Ready
1. **Day 14-15**: WebSocket implementation
2. **Day 16-17**: Error handling
3. **Day 18-19**: Performance optimization
4. **Day 20**: Documentation

## Success Metrics

### Milestone 1: "Hello World" (Day 5)
- [ ] Can import connector
- [ ] Can connect with API keys
- [ ] Can query balance

### Milestone 2: "First Trade" (Day 10)
- [ ] Can place limit order
- [ ] Can cancel order
- [ ] Can see order status

### Milestone 3: "Basic Bot" (Day 15)
- [ ] Can run with pure market making strategy
- [ ] Orders appear on exchange
- [ ] Balance updates work

### Milestone 4: "Production Ready" (Day 20)
- [ ] WebSocket updates working
- [ ] Proper error handling
- [ ] All tests passing
- [ ] Documentation complete

## Risk Mitigation

### Technical Risks:
1. **Python version compatibility**
   - Solution: Test early in Python 3.10

2. **CyberDelta API changes**
   - Solution: Pin CyberDelta version

3. **WebSocket format differences**
   - Solution: Start with polling, add WS later

### Schedule Risks:
1. **Unknown Hummingbot internals**
   - Solution: Study existing connectors
   - Fallback: Ask Hummingbot community

2. **Testing delays**
   - Solution: Use testnet early
   - Have backup test accounts

## Development Tips

### Start Simple:
```python
# First version - just make it work
async def _place_order(...):
    # Hardcode everything
    return "fake_order_id"

# Second version - add CyberDelta
async def _place_order(...):
    result = await self._cd_api.place_order(...)
    return result.order_id

# Third version - add proper error handling
async def _place_order(...):
    try:
        result = await self._cd_api.place_order(...)
        # Add tracking, events, etc.
        return result.order_id
    except Exception as e:
        # Handle errors
```

### Debug Strategy:
1. Add extensive logging
2. Use Python debugger in hummingbot-dev
3. Test each method in isolation
4. Use testnet for all testing

### Common Pitfalls:
1. Don't forget to track orders
2. Always emit events
3. Handle None/null values
4. Use Decimal for all amounts
5. Test error paths

## Conclusion

This roadmap provides a pragmatic path to implementing a working Backpack connector for Hummingbot. By leveraging CyberDelta's existing implementation and following a wrapper pattern, we can deliver a functional connector in approximately 3 weeks:

- **Week 1**: Basic functionality
- **Week 2**: Integration and testing
- **Week 3**: Production readiness

The key is to start simple, test early, and iterate quickly. With CyberDelta's robust API layer already built, the main challenge is translating between the two architectures, which is a well-defined problem with clear solutions.

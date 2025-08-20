# Hummingbot Connector File Structure

## Standard Connector Directory Structure

Based on analysis of existing connectors (Hyperliquid, Binance, etc.), here's the required file structure for a Backpack connector:

```
hummingbot/connector/exchange/backpack/
├── __init__.py
├── backpack_exchange.py                    # Main connector class
├── backpack_api_order_book_data_source.py  # Order book data handling
├── backpack_api_user_stream_data_source.py # User stream (account updates)
├── backpack_auth.py                        # Authentication/signing
├── backpack_constants.py                   # Exchange constants
├── backpack_utils.py                       # Utility functions
├── backpack_web_utils.py                   # Web/HTTP utilities
└── dummy.pxd                               # Cython placeholder (optional)
└── dummy.pyx                               # Cython placeholder (optional)
```

## Detailed File Responsibilities

### 1. `__init__.py`
```python
# Empty or minimal imports
from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange

__all__ = ["BackpackExchange"]
```

### 2. `backpack_exchange.py` - Main Connector Class
```python
"""
Primary connector implementation
- Inherits from ExchangePyBase
- Implements abstract methods
- Manages order lifecycle
- Handles balance updates
- Coordinates all components
"""

Key responsibilities:
- Order placement/cancellation
- Balance tracking
- Trading rules management
- Event generation
- WebSocket coordination
```

### 3. `backpack_api_order_book_data_source.py` - Market Data
```python
"""
Handles order book data from REST and WebSocket
- Inherits from OrderBookTrackerDataSource
- Manages order book snapshots
- Processes incremental updates
- Handles market data subscriptions
"""

Key methods:
- listen_for_subscriptions()
- listen_for_trades()
- listen_for_order_book_diffs()
- listen_for_order_book_snapshots()
```

### 4. `backpack_api_user_stream_data_source.py` - Account Updates
```python
"""
Handles user-specific WebSocket streams
- Inherits from UserStreamTrackerDataSource
- Manages authentication for private streams
- Processes account updates, order updates, fills
"""

Key methods:
- listen_for_user_stream()
- _connected_websocket_assistant()
- _subscribe_channels()
- _authenticate()
```

### 5. `backpack_auth.py` - Authentication Module
```python
"""
Handles request signing and authentication
- Inherits from AuthBase (Hummingbot's auth interface)
- Implements Ed25519 signing (wraps CyberDelta's auth)
- Manages API keys and secrets
"""

Key methods:
- rest_authenticate()
- ws_authenticate()
- get_headers()
```

### 6. `backpack_constants.py` - Exchange Constants
```python
"""
All exchange-specific constants
- API endpoints
- WebSocket URLs
- Rate limits
- Error messages
- Trading pair formats
"""

Example:
DEFAULT_DOMAIN = "backpack"
REST_URL = "https://api.backpack.exchange"
WSS_URL = "wss://ws.backpack.exchange"

# Endpoints
EXCHANGE_INFO_URL = "/api/v1/markets"
TICKER_PRICE_CHANGE_URL = "/api/v1/ticker/24hr"
ORDER_URL = "/api/v1/order"

# Rate Limits
RATE_LIMITS = [
    RateLimit(limit_id=ORDER_URL, limit=30, time_interval=1),
    ...
]
```

### 7. `backpack_utils.py` - Utility Functions
```python
"""
Helper functions for the connector
- Trading pair conversions
- Symbol mappings
- Data transformations
"""

Functions:
- split_trading_pair()  # "BTC-USDC" -> ("BTC", "USDC")
- convert_to_exchange_trading_pair()  # Hummingbot -> Exchange format
- convert_from_exchange_trading_pair()  # Exchange -> Hummingbot format
- get_new_client_order_id()
```

### 8. `backpack_web_utils.py` - Web/HTTP Utilities
```python
"""
HTTP client configuration and helpers
- WebAssistants factory
- Request/response helpers
- Error handling utilities
"""

Functions:
- build_api_factory()
- get_current_server_time()
- api_request()
```

### 9. `dummy.pxd` and `dummy.pyx` - Cython Files (Optional)
```python
"""
Placeholder Cython files
- Required if connector uses Cython
- Can be empty for pure Python implementation
- Used for performance optimization
"""
```

## File Naming Conventions

### Must Follow:
1. **Prefix**: All files must start with `backpack_`
2. **Snake Case**: Use snake_case for all file names
3. **Descriptive**: Names should clearly indicate purpose

### Standard Suffixes:
- `_exchange.py` - Main connector
- `_auth.py` - Authentication
- `_constants.py` - Constants
- `_utils.py` - Utilities
- `_web_utils.py` - Web utilities
- `_api_order_book_data_source.py` - Order book data
- `_api_user_stream_data_source.py` - User stream

## Import Structure

### Standard Imports Pattern:
```python
# System imports
import asyncio
from decimal import Decimal
from typing import Dict, List, Optional

# Hummingbot core imports
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder

# Connector-specific imports
from hummingbot.connector.exchange.backpack import (
    backpack_constants as CONSTANTS,
    backpack_web_utils as web_utils,
)
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth

# CyberDelta imports (for wrapper)
from cyberdelta.apis.backpack import BackpackAPI
from cyberdelta.models import Order as CyberDeltaOrder
```

## Configuration Files

### Additional Required Files (in Hummingbot root):

```
hummingbot/templates/
└── conf_fee_overrides_TEMPLATE.yml
    # Add Backpack fee configuration

hummingbot/connector/
└── connector_status.py
    # Register Backpack connector status
```

## Test Structure

```
test/hummingbot/connector/exchange/backpack/
├── __init__.py
├── test_backpack_exchange.py
├── test_backpack_api_order_book_data_source.py
├── test_backpack_api_user_stream_data_source.py
├── test_backpack_auth.py
├── test_backpack_utils.py
└── test_backpack_web_utils.py
```

## Integration Points

### 1. Connector Registration
Location: `hummingbot/connector/connector_status.py`
```python
backpack = ConnectorStatus(
    name="backpack",
    status=ConnectorStatusType.GREEN,  # or YELLOW during development
    details="Community contributed connector"
)
```

### 2. Factory Registration
Location: `hummingbot/client/config/config_helpers.py`
```python
# Add to connector factory
"backpack": "hummingbot.connector.exchange.backpack.backpack_exchange.BackpackExchange"
```

### 3. Configuration Template
Location: `hummingbot/client/config/`
- Add Backpack-specific configuration parameters
- API key/secret handling
- Testnet configuration

## File Size Guidelines

Based on Hummingbot patterns:
- **Main connector**: 800-1500 lines
- **Data sources**: 300-600 lines each
- **Auth module**: 100-200 lines
- **Constants**: 50-150 lines
- **Utils**: 100-300 lines

## Code Style Requirements

### Must Follow:
1. **Type Hints**: All functions must have type hints
2. **Docstrings**: Google-style docstrings for all classes/methods
3. **Logging**: Use HummingbotLogger
4. **Async**: All I/O operations must be async
5. **Error Handling**: Comprehensive try/except blocks

### Example Method Structure:
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
    Place an order on Backpack exchange.

    Args:
        order_id: Client-generated order ID
        trading_pair: Trading pair (e.g., "BTC-USDC")
        amount: Order amount
        order_type: LIMIT, MARKET, or LIMIT_MAKER
        is_buy: True for buy, False for sell
        price: Order price (required for limit orders)

    Returns:
        Exchange order ID

    Raises:
        Exception: On order placement failure
    """
    # Implementation
```

## Directory Location in CyberDelta

For our wrapper approach:
```
CyberDeltaEngine/
└── worktrees/
    └── hummingbot-dev/
        └── hummingbot/
            └── connector/
                └── exchange/
                    └── backpack/  # Our connector here
```

This structure ensures compatibility with Hummingbot's discovery mechanism while keeping our code organized within the CyberDelta project structure.

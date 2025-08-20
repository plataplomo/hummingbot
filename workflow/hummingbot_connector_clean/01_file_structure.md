# Hummingbot Backpack Connector - File Structure

## Overview
This document outlines the standard file structure for a Hummingbot exchange connector, specifically for the Backpack exchange implementation. The structure follows Hummingbot's established patterns for Python 3.10 compatibility.

## Directory Structure

```
hummingbot/connector/exchange/backpack/
├── __init__.py
├── backpack_exchange.py                    # Main exchange connector class
├── backpack_auth.py                        # Ed25519 authentication implementation
├── backpack_constants.py                   # API endpoints, rate limits, constants
├── backpack_utils.py                       # Helper functions for trading pairs
├── backpack_web_utils.py                   # HTTP/WebSocket factory builders
├── backpack_api_order_book_data_source.py  # OrderBook WebSocket handler
├── backpack_api_user_stream_data_source.py # Private WebSocket handler
└── backpack_order_book.py                  # Custom OrderBook class (if needed)
```

## File Purposes

### Core Files (Required)

#### `__init__.py`
- Empty file to make the directory a Python package
- No imports needed

#### `backpack_exchange.py`
- **Purpose**: Main connector implementation inheriting from `ExchangePyBase`
- **Key responsibilities**:
  - Order placement and cancellation
  - Balance and position tracking
  - Trading rules management
  - Fee calculation
  - Order status updates
- **Size**: ~800-1200 lines typically

#### `backpack_auth.py`
- **Purpose**: Handle Ed25519 signature authentication
- **Key responsibilities**:
  - Sign REST API requests
  - Generate WebSocket authentication messages
  - Manage API keys and secrets
- **Size**: ~150-200 lines

#### `backpack_constants.py`
- **Purpose**: Define all exchange-specific constants
- **Contents**:
  ```python
  # API URLs
  REST_URL = "https://api.backpack.exchange/"
  WS_PUBLIC_URL = "wss://ws.backpack.exchange/"
  WS_PRIVATE_URL = "wss://ws.backpack.exchange/"

  # Endpoints
  EXCHANGE_INFO_URL = "api/v1/exchange_info"
  ORDER_URL = "api/v1/order"

  # Rate Limits
  RATE_LIMITS = [
      RateLimit(limit_id=ORDER_URL, limit=10, time_interval=1),
      # ... more limits
  ]

  # Other constants
  BROKER_ID = "HBOT"
  MAX_ORDER_ID_LEN = 32
  ```
- **Size**: ~100-150 lines

#### `backpack_utils.py`
- **Purpose**: Trading pair conversion utilities
- **Key functions**:
  - `split_trading_pair()` - Split "BTC-USDC" into base and quote
  - `convert_from_exchange_trading_pair()` - Convert exchange format to Hummingbot format
  - `convert_to_exchange_trading_pair()` - Convert Hummingbot format to exchange format
- **Size**: ~50-100 lines

#### `backpack_web_utils.py`
- **Purpose**: Create web assistants for API communication
- **Key functions**:
  - `build_api_factory()` - Create WebAssistantsFactory with auth and throttler
  - `get_current_server_time()` - Helper for time synchronization
- **Size**: ~100-150 lines

### Data Source Files (Required)

#### `backpack_api_order_book_data_source.py`
- **Purpose**: Handle public WebSocket streams for order book updates
- **Inherits from**: `OrderBookTrackerDataSource`
- **Key methods**:
  - `listen_for_order_book_diffs()` - WebSocket subscription to orderbook updates
  - `listen_for_order_book_snapshots()` - REST API calls for snapshots
  - `listen_for_trades()` - WebSocket subscription to trade streams
- **Size**: ~200-300 lines

#### `backpack_api_user_stream_data_source.py`
- **Purpose**: Handle private WebSocket streams for account updates
- **Inherits from**: `UserStreamTrackerDataSource`
- **Key methods**:
  - `listen_for_user_stream()` - Main WebSocket listener for account events
  - `_authenticate_websocket()` - Send authentication message
  - Parse balance updates, order updates, trade fills
- **Size**: ~200-300 lines

### Optional Files

#### `backpack_order_book.py`
- **Purpose**: Custom OrderBook implementation if exchange has special requirements
- **When needed**: Only if default OrderBook class doesn't handle exchange specifics
- **Size**: ~100-150 lines if needed

## Test Structure

```
test/hummingbot/connector/exchange/backpack/
├── __init__.py
├── test_backpack_exchange.py
├── test_backpack_auth.py
├── test_backpack_api_order_book_data_source.py
└── test_backpack_api_user_stream_data_source.py
```

## Import Organization

Standard import order for all files:

```python
# Standard library imports
import asyncio
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

# Third-party imports
from async_timeout import timeout

# Hummingbot core imports
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder

# Local imports (within backpack module)
from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
```

## Naming Conventions

### File Names
- All lowercase with underscores
- Prefix with `backpack_`
- Descriptive suffix: `_exchange`, `_auth`, `_constants`, etc.

### Class Names
- PascalCase
- Prefix with `Backpack`
- Examples: `BackpackExchange`, `BackpackAuth`, `BackpackAPIOrderBookDataSource`

### Constants
- ALL_CAPS with underscores
- Grouped logically in `backpack_constants.py`

### Methods
- snake_case for all methods
- Private methods prefix with underscore: `_process_order_update()`
- Async methods should be clearly async: `async def place_order()`

## Size Guidelines

Total connector size: ~2500-3500 lines of code

Breakdown:
- Main exchange file: 35-40% of total
- Data sources combined: 25-30% of total
- Auth: 5-10% of total
- Constants and utils: 10-15% of total
- Tests: Should aim for 80%+ coverage

## Dependencies

### Required Hummingbot modules:
- `hummingbot.connector.exchange_py_base`
- `hummingbot.connector.client_order_tracker`
- `hummingbot.connector.trading_rule`
- `hummingbot.core.api_throttler`
- `hummingbot.core.data_type.*`
- `hummingbot.core.web_assistant`

### External dependencies (Python 3.10 compatible):
- `aiohttp` - For HTTP requests
- `websockets` or `aiohttp` - For WebSocket connections
- `cryptography` or `nacl` - For Ed25519 signing
- No Pydantic v2 (not compatible with Python 3.10)
- No match/case statements (Python 3.10 doesn't support)

## File Creation Order

When implementing, create files in this order:
1. `backpack_constants.py` - Define all constants first
2. `backpack_auth.py` - Authentication is needed by everything
3. `backpack_utils.py` - Helper functions
4. `backpack_web_utils.py` - Web factory setup
5. `backpack_api_order_book_data_source.py` - Public data
6. `backpack_api_user_stream_data_source.py` - Private data
7. `backpack_exchange.py` - Main implementation using all above
8. Tests for each component

This structure ensures each file has its dependencies available when created.

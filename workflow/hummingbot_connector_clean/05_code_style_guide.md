# Code Style Guide - Python 3.10 vs 3.13 Differences

## Overview
This document highlights critical code style differences between Python 3.10 (Hummingbot) and Python 3.13 (CyberDelta), ensuring compatibility and proper patterns for the Backpack connector.

## Python Version Compatibility

### ❌ DON'T Use Python 3.11+ Features

#### Structural Pattern Matching (match/case)
```python
# ❌ WRONG - Python 3.10 doesn't support match/case
match order_status:
    case "NEW":
        return OrderState.OPEN
    case "FILLED":
        return OrderState.FILLED
    case "CANCELLED":
        return OrderState.CANCELED
    case _:
        return OrderState.FAILED

# ✅ CORRECT - Use if/elif for Python 3.10
if order_status == "NEW":
    return OrderState.OPEN
elif order_status == "FILLED":
    return OrderState.FILLED
elif order_status == "CANCELLED":
    return OrderState.CANCELED
else:
    return OrderState.FAILED
```

#### Exception Groups and except*
```python
# ❌ WRONG - Python 3.11+ feature
try:
    await asyncio.gather(*tasks)
except* ConnectionError as e:
    handle_connection_errors(e.exceptions)

# ✅ CORRECT - Traditional exception handling
try:
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, ConnectionError):
            handle_connection_error(result)
except Exception as e:
    handle_other_error(e)
```

#### Self Type Hints
```python
# ❌ WRONG - Python 3.11+ Self type
from typing import Self

class BackpackExchange:
    def copy(self) -> Self:
        return BackpackExchange(...)

# ✅ CORRECT - Use TypeVar for Python 3.10
from typing import TypeVar

T = TypeVar("T", bound="BackpackExchange")

class BackpackExchange:
    def copy(self: T) -> T:
        return type(self)(...)
```

### ❌ DON'T Use Python 3.12+ Features

#### Type Parameter Syntax
```python
# ❌ WRONG - Python 3.12+ generic syntax
def process_order[T](order: T) -> T:
    return order

# ✅ CORRECT - Use TypeVar for Python 3.10
from typing import TypeVar

T = TypeVar("T")

def process_order(order: T) -> T:
    return order
```

#### f-string Improvements
```python
# ❌ WRONG - Python 3.12+ f-string features
error_msg = f"{order_id=}"  # Debug format
formatted = f"{value:.{precision}f}"  # Nested expressions

# ✅ CORRECT - Python 3.10 compatible
error_msg = f"order_id={order_id}"
formatted = f"{value:.2f}" if precision == 2 else str(round(value, precision))
```

## Type Hints Compatibility

### Union vs | Operator
```python
# ❌ WRONG - Using | operator (works in 3.10 but less compatible)
from typing import Optional
def get_price() -> float | None:
    pass

# ✅ CORRECT - Use Union/Optional for clarity
from typing import Optional, Union
def get_price() -> Optional[float]:
    pass

def process_value(val: Union[str, int]) -> str:
    pass
```

### Generic Type Aliases
```python
# ❌ WRONG - Python 3.12+ type alias syntax
type OrderDict = Dict[str, Any]

# ✅ CORRECT - Python 3.10 type alias
from typing import Dict, Any, TypeAlias

OrderDict: TypeAlias = Dict[str, Any]
# Or simply:
OrderDict = Dict[str, Any]
```

## Data Classes and Models

### No Pydantic v2
```python
# ❌ WRONG - Pydantic v2 (requires Python 3.11+)
from pydantic import BaseModel, Field, computed_field

class Order(BaseModel):
    symbol: str
    quantity: Decimal = Field(gt=0)

    @computed_field
    @property
    def notional(self) -> Decimal:
        return self.quantity * self.price

# ✅ CORRECT - Use dictionaries or dataclasses
from dataclasses import dataclass
from decimal import Decimal

@dataclass
class Order:
    symbol: str
    quantity: Decimal
    price: Decimal

    @property
    def notional(self) -> Decimal:
        return self.quantity * self.price

    def __post_init__(self):
        if self.quantity <= 0:
            raise ValueError("Quantity must be positive")
```

### Simple Data Structures
```python
# ❌ WRONG - Over-engineered with models
class BalanceUpdate(BaseModel):
    asset: str
    free: Decimal
    locked: Decimal

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

# ✅ CORRECT - Simple dictionary handling
def parse_balance_update(data: Dict[str, Any]) -> Dict[str, Decimal]:
    return {
        "asset": data["asset"],
        "free": Decimal(data["free"]),
        "locked": Decimal(data["locked"]),
        "total": Decimal(data["free"]) + Decimal(data["locked"])
    }
```

## Async/Await Patterns

### Task Groups (Python 3.11+)
```python
# ❌ WRONG - Python 3.11+ TaskGroup
async with asyncio.TaskGroup() as tg:
    task1 = tg.create_task(fetch_balance())
    task2 = tg.create_task(fetch_orders())

# ✅ CORRECT - Use gather or create_task
tasks = [
    asyncio.create_task(fetch_balance()),
    asyncio.create_task(fetch_orders())
]
results = await asyncio.gather(*tasks)

# Or with error handling:
results = await asyncio.gather(*tasks, return_exceptions=True)
```

### Timeout Context Manager
```python
# ❌ WRONG - Python 3.11+ asyncio.timeout
async with asyncio.timeout(30):
    response = await fetch_data()

# ✅ CORRECT - Use async_timeout for Python 3.10
from async_timeout import timeout

async with timeout(30):
    response = await fetch_data()
```

## String Operations

### removeprefix/removesuffix
```python
# These are available in Python 3.9+ so they're OK to use
symbol = "BTC-USDC"
base = symbol.removesuffix("-USDC")  # OK in Python 3.10

# But for compatibility with older code:
base = symbol[:-5] if symbol.endswith("-USDC") else symbol
```

## Import Style

### Hummingbot Import Order
```python
# ✅ CORRECT - Hummingbot standard import order
# 1. Standard library
import asyncio
import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

# 2. Third-party libraries
import aiohttp
from async_timeout import timeout

# 3. Hummingbot imports
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

# 4. Local module imports
from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth
```

## Naming Conventions

### Variables and Functions
```python
# ✅ CORRECT - snake_case for everything
trading_pair = "BTC-USDC"
order_id = get_new_client_order_id()

async def place_limit_order():
    pass

def calculate_order_amount():
    pass

# ❌ WRONG - camelCase (not Python convention)
tradingPair = "BTC-USDC"
orderId = getNewClientOrderId()
```

### Constants
```python
# ✅ CORRECT - ALL_CAPS for constants
MAX_ORDER_ID_LENGTH = 32
DEFAULT_TIMEOUT = 30.0
BROKER_ID = "HBOT"

# ❌ WRONG - Mixed case constants
MaxOrderIdLength = 32
DefaultTimeout = 30.0
```

### Private Methods
```python
# ✅ CORRECT - Single underscore for internal methods
class BackpackExchange:
    def _process_balance_update(self, data: Dict):
        """Internal method"""
        pass

    async def _api_request(self, method: str, path: str):
        """Internal API helper"""
        pass

# ❌ WRONG - Double underscore (name mangling)
def __process_balance_update(self, data: Dict):
    pass
```

## Error Handling

### Simple and Direct
```python
# ✅ CORRECT - Simple error handling
try:
    response = await self._api_post("/api/v1/order", params)
    if "orderId" not in response:
        raise ValueError(f"Invalid response: {response}")
    return response["orderId"]
except aiohttp.ClientError as e:
    self.logger().error(f"Network error placing order: {e}")
    raise
except Exception as e:
    self.logger().error(f"Unexpected error: {e}", exc_info=True)
    raise

# ❌ WRONG - Over-engineered error handling
try:
    response = await self._api_post("/api/v1/order", params)
except NetworkException as e:
    self._handle_network_error(e)
    self._circuit_breaker.record_failure()
    raise OrderPlacementError from e
except ValidationException as e:
    self._metrics.record_validation_error()
    raise
```

## Logging Style

### Hummingbot Logger Pattern
```python
# ✅ CORRECT - Use class logger
class BackpackExchange(ExchangePyBase):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    async def place_order(self):
        self.logger().info(f"Placing order: {order_id}")
        try:
            # ... order logic
            self.logger().info(f"Order placed successfully: {order_id}")
        except Exception as e:
            self.logger().error(f"Failed to place order {order_id}: {e}")

# ❌ WRONG - Module-level logger
logger = get_logger(__name__)  # CyberDelta style

async def place_order(self):
    logger.info("Placing order")  # Not Hummingbot pattern
```

## Testing Style

### Simple Test Structure
```python
# ✅ CORRECT - Straightforward test
class TestBackpackExchange(unittest.TestCase):
    def setUp(self):
        self.exchange = BackpackExchange(
            client_config_map=mock_config,
            backpack_api_key="test_key",
            backpack_api_secret="test_secret",
            trading_pairs=["BTC-USDC"]
        )

    @patch("aiohttp.ClientSession.request")
    async def test_place_order(self, mock_request):
        mock_request.return_value.__aenter__.return_value.json = AsyncMock(
            return_value={"orderId": "12345"}
        )

        order_id = await self.exchange._place_order(
            order_id="TEST001",
            trading_pair="BTC-USDC",
            amount=Decimal("0.1"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("45000")
        )

        self.assertEqual(order_id[0], "12345")
```

## Common Pitfalls to Avoid

### 1. Complex Type Hierarchies
```python
# ❌ WRONG - CyberDelta style type complexity
T = TypeVar("T", bound=BaseModel)
U = TypeVar("U", bound=BaseModel)

class GenericMapper(Generic[T, U], Protocol):
    def map(self, source: T) -> U: ...

# ✅ CORRECT - Keep it simple
def map_order_to_dict(order: InFlightOrder) -> Dict[str, Any]:
    return {
        "id": order.client_order_id,
        "symbol": order.trading_pair,
        "quantity": str(order.amount)
    }
```

### 2. Service Layer Abstraction
```python
# ❌ WRONG - Service-oriented architecture
class OrderService:
    def __init__(self, api_client, mapper, validator):
        self.api_client = api_client
        self.mapper = mapper
        self.validator = validator

# ✅ CORRECT - Direct implementation
class BackpackExchange:
    async def _place_order(self, ...):
        # Direct API call, no service layer
        response = await self._api_post(...)
        return response["orderId"]
```

### 3. Circular Imports
```python
# ❌ WRONG - Circular dependency risk
# In backpack_exchange.py
from .backpack_auth import BackpackAuth
# In backpack_auth.py
from .backpack_exchange import BackpackExchange

# ✅ CORRECT - One-way dependencies
# backpack_auth.py has no imports from exchange
# backpack_exchange.py imports from auth
```

## Summary Checklist

Before committing code, verify:

- [ ] No match/case statements
- [ ] No Python 3.11+ features
- [ ] No Pydantic models
- [ ] Simple dictionary-based data handling
- [ ] Using Optional[] instead of | None
- [ ] Using async_timeout instead of asyncio.timeout
- [ ] Snake_case naming throughout
- [ ] Single underscore for private methods
- [ ] Direct error handling without complex hierarchies
- [ ] No service layer abstractions
- [ ] Simple, flat import structure
- [ ] Hummingbot logger pattern
- [ ] No circular dependencies

Following these guidelines ensures the connector works with Hummingbot's Python 3.10 environment while maintaining clean, readable code.

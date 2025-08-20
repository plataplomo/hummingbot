# Restructure Proposal: Submodule and Adapter Layer

## Option 1: Move Submodule to `cyberdelta/hummingbot`

### Current Structure
```
CyberDeltaEngine/
├── vendor/
│   └── hummingbot/  # Current submodule location
└── cyberdelta/
    └── apis/
```

### Proposed Structure
```
CyberDeltaEngine/
├── cyberdelta/
│   ├── apis/           # CyberDelta APIs
│   └── hummingbot/     # Moved submodule here
│       └── (hummingbot source)
```

### Commands to Move Submodule
```bash
# 1. Remove old submodule (preserving the code)
git submodule deinit -f vendor/hummingbot
git rm -f vendor/hummingbot
rm -rf .git/modules/vendor/hummingbot

# 2. Add submodule in new location
git submodule add https://github.com/plataplomo/hummingbot.git cyberdelta/hummingbot
git submodule update --init --recursive

# 3. Update .gitmodules
git add .gitmodules cyberdelta/hummingbot
git commit -m "Move hummingbot submodule to cyberdelta/hummingbot"
```

### ❌ Problems with This Approach

1. **Import Confusion**: `cyberdelta.hummingbot` looks like part of CyberDelta
2. **Namespace Pollution**: Mixing external and internal code
3. **CI/CD Complexity**: Build processes need to handle submodule inside package
4. **Package Distribution**: Can't pip install with submodule inside

## Option 2: Keep Submodule External + Separate Adapter Package (RECOMMENDED)

### Proposed Complete Structure

```
CyberDeltaEngine/
├── cyberdelta/                         # Core CyberDelta package
│   ├── __init__.py
│   ├── apis/
│   │   ├── backpack/                   # Existing Backpack API
│   │   └── base/
│   └── models/
│
├── vendor/                              # External dependencies (keep here)
│   └── hummingbot/                      # Submodule stays here
│       └── (hummingbot source)
│
├── hummingbot-connectors/               # NEW: Connector implementations
│   ├── README.md
│   ├── pyproject.toml                   # Separate package config
│   ├── setup.py
│   ├── requirements.txt
│   │
│   ├── src/
│   │   └── cyberdelta_connectors/      # Package namespace
│   │       ├── __init__.py
│   │       │
│   │       ├── backpack/                # Backpack connector
│   │       │   ├── __init__.py
│   │       │   ├── connector.py         # Main BackpackExchange class
│   │       │   ├── auth.py             # Auth wrapper
│   │       │   ├── constants.py        # Backpack constants
│   │       │   ├── utils.py            # Utilities
│   │       │   ├── web_utils.py        # Web helpers
│   │       │   ├── order_book_data_source.py
│   │       │   └── user_stream_data_source.py
│   │       │
│   │       ├── adapters/                # Shared adapter layer
│   │       │   ├── __init__.py
│   │       │   ├── base_adapter.py     # Base adapter class
│   │       │   ├── cyberdelta_bridge.py # Bridge to CyberDelta
│   │       │   ├── type_converter.py   # Type conversions
│   │       │   └── event_bus.py        # Event handling
│   │       │
│   │       └── interfaces/              # Protocol definitions
│   │           ├── __init__.py
│   │           ├── api_protocol.py     # API interface
│   │           ├── ws_protocol.py      # WebSocket interface
│   │           └── adapter_protocol.py # Adapter interface
│   │
│   └── tests/
│       ├── __init__.py
│       ├── unit/
│       │   ├── test_backpack_connector.py
│       │   ├── test_adapter.py
│       │   └── test_type_converter.py
│       └── integration/
│           └── test_backpack_integration.py
│
└── worktrees/
    └── hummingbot-dev/                  # Development worktree
        └── hummingbot/
            └── connector/
                └── exchange/
                    └── backpack/         # Symlink to connector
```

### Adapter Layer Details

#### `hummingbot-connectors/src/cyberdelta_connectors/adapters/cyberdelta_bridge.py`
```python
"""
Single point of contact with CyberDelta API.
ALL CyberDelta imports happen here and nowhere else.
"""
import sys
import asyncio
from typing import Optional, Any, Dict
from pathlib import Path

# Add CyberDelta to path
CYBERDELTA_PATH = Path(__file__).parent.parent.parent.parent.parent / "cyberdelta"
sys.path.insert(0, str(CYBERDELTA_PATH))

# ALL CyberDelta imports in this file only
from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator
from cyberdelta.apis.models.service_args.trading import PlaceOrderArgs, CancelOrderArgs
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import BackpackSecrets

from ..interfaces.adapter_protocol import IAdapterBridge


class CyberDeltaBridge(IAdapterBridge):
    """
    Bridge to CyberDelta API.
    This is the ONLY class that imports from CyberDelta.
    """

    def __init__(self, api_key: str, api_secret: str):
        # Initialize CyberDelta API
        config = ExchangeSpecificConfig(
            name="backpack",
            api_base_url="https://api.backpack.exchange",
            ws_url="wss://ws.backpack.exchange",
        )
        secrets = BackpackSecrets(api_key=api_key, api_secret=api_secret)

        self._api = BackpackAPI(config, secrets)
        self._update_queue = asyncio.Queue()

    async def place_order(self, **kwargs) -> Dict[str, Any]:
        """Place order through CyberDelta"""
        args = PlaceOrderArgs(**kwargs)
        result = await self._api.place_order(args)
        # Convert to dict for transport
        return {
            "order_id": result.order_id,
            "client_order_id": result.client_order_id,
            "status": result.status.value,
        }

    async def get_updates(self) -> Optional[Dict[str, Any]]:
        """Get updates without callbacks"""
        try:
            return await asyncio.wait_for(
                self._update_queue.get(),
                timeout=0.1
            )
        except asyncio.TimeoutError:
            return None
```

#### `hummingbot-connectors/src/cyberdelta_connectors/backpack/connector.py`
```python
"""
Backpack connector for Hummingbot.
NO CyberDelta imports here!
"""
from decimal import Decimal
from typing import Optional, Dict, List

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.data_type.common import OrderType, TradeType

from ..adapters.cyberdelta_bridge import CyberDeltaBridge
from ..adapters.type_converter import TypeConverter


class BackpackExchange(ExchangePyBase):
    """
    Hummingbot connector for Backpack exchange.
    Uses CyberDeltaBridge for all exchange operations.
    """

    def __init__(
        self,
        client_config_map,
        api_key: str,
        api_secret: str,
        trading_pairs: Optional[List[str]] = None,
    ):
        super().__init__(client_config_map)

        # Initialize bridge (only class that imports CyberDelta)
        self._bridge = CyberDeltaBridge(api_key, api_secret)
        self._converter = TypeConverter()
        self._trading_pairs = trading_pairs or []

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        order_type: OrderType,
        is_buy: bool,
        price: Optional[Decimal] = None,
    ) -> str:
        """Place order through bridge"""
        # Convert types
        cd_params = self._converter.hb_to_cd_order_params(
            trading_pair=trading_pair,
            amount=amount,
            order_type=order_type,
            is_buy=is_buy,
            price=price,
            client_order_id=order_id,
        )

        # Call bridge (no direct CyberDelta import)
        result = await self._bridge.place_order(**cd_params)

        # Process result
        return result["order_id"]
```

### Installation and Development Setup

#### `hummingbot-connectors/pyproject.toml`
```toml
[project]
name = "cyberdelta-hummingbot-connectors"
version = "0.1.0"
description = "Hummingbot connectors using CyberDelta APIs"
requires-python = ">=3.10,<3.11"  # Hummingbot compatibility

dependencies = [
    # Don't depend on cyberdelta package directly
    # It will be available via sys.path manipulation
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-asyncio>=0.21",
    "pytest-mock>=3.10",
]

[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"
```

#### Development Workflow

1. **Install in Development Mode**
```bash
cd hummingbot-connectors
pip install -e .
```

2. **Create Symlink in Hummingbot Worktree**
```bash
cd worktrees/hummingbot-dev/hummingbot/connector/exchange
ln -s ../../../../../hummingbot-connectors/src/cyberdelta_connectors/backpack backpack
```

3. **Run Tests**
```bash
cd hummingbot-connectors
pytest tests/
```

## Why Option 2 is Better

### ✅ Advantages

1. **Clean Separation**: Three distinct codebases
   - CyberDelta core (`cyberdelta/`)
   - Hummingbot vendor (`vendor/hummingbot/`)
   - Connectors (`hummingbot-connectors/`)

2. **No Circular Risk**: Bridge pattern ensures one-way dependencies
   ```
   Connector → Bridge → CyberDelta
   Connector → Hummingbot
   (No path from CyberDelta back to Connector)
   ```

3. **Easy Testing**: Each component can be tested independently

4. **Flexible Deployment**:
   - Can pip install connector into any Hummingbot instance
   - Can develop without modifying vendor code
   - Can version independently

5. **Clear Ownership**:
   - `cyberdelta/`: Your trading engine
   - `vendor/`: External dependency
   - `hummingbot-connectors/`: Your connector implementations

### ❌ Why Not Move Submodule Inside

1. **Package Pollution**: External code inside your package
2. **Import Confusion**: `from cyberdelta.hummingbot` looks wrong
3. **Git Complexity**: Submodules inside packages cause issues
4. **Distribution Problems**: Can't pip install with submodules

## Implementation Plan

### Phase 1: Create Connector Package Structure
```bash
# Create directories
mkdir -p hummingbot-connectors/src/cyberdelta_connectors/{backpack,adapters,interfaces}
mkdir -p hummingbot-connectors/tests/{unit,integration}

# Initialize package
cd hummingbot-connectors
touch src/cyberdelta_connectors/__init__.py
touch pyproject.toml
```

### Phase 2: Implement Bridge Layer
1. Create `cyberdelta_bridge.py` with all CyberDelta imports
2. Create `type_converter.py` for type mappings
3. Create protocol interfaces

### Phase 3: Implement Connector
1. Create `connector.py` using only bridge
2. Create data sources
3. Add Hummingbot-specific files

### Phase 4: Integration
1. Symlink or install into Hummingbot
2. Test with Hummingbot CLI
3. Verify no circular dependencies

## Conclusion

**Recommendation**: Keep submodule in `vendor/` and create a separate `hummingbot-connectors/` package with a clean adapter layer. This provides:

- Complete separation of concerns
- No circular dependency risk
- Clean, maintainable architecture
- Flexibility for future connectors

The adapter/bridge pattern ensures CyberDelta never needs to know about Hummingbot, eliminating circular dependency risk completely.

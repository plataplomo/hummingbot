# Complete Hummingbot Setup Guide with CyberDelta Integration

## Overview
This guide shows how to set up Hummingbot with your existing CyberDelta Backpack API code using `uv` and `venv` instead of conda.

## Important Note: Python Version Requirements
Hummingbot requires Python 3.10 or 3.11. Using `uv` with `venv` is possible but requires extra setup compared to conda.

## Directory Structure

```bash
~/trading_workspace/
├── hummingbot/                    # Cloned Hummingbot
├── CyberDeltaEngine/              # Your existing repo
└── backpack-connector/            # Connector wrapper
```

## Step 1: Install UV and Python 3.10

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Python 3.10 using uv
uv python install 3.10

# Verify installation
uv python list
```

## Step 2: Clone Hummingbot

```bash
# Create workspace
mkdir -p ~/trading_workspace
cd ~/trading_workspace

# Clone Hummingbot
git clone https://github.com/hummingbot/hummingbot.git
cd hummingbot

# Checkout stable version
git checkout v1.24.0  # Latest stable
```

## Step 3: Setup Hummingbot with UV/Venv

```bash
cd ~/trading_workspace/hummingbot

# Create venv with Python 3.10
uv venv --python 3.10

# Activate venv
source .venv/bin/activate  # On Linux/Mac
# or
.venv\Scripts\activate  # On Windows

# Install Hummingbot dependencies with uv
uv pip install -r requirements.txt

# Install Hummingbot in development mode
uv pip install -e .

# Verify installation
python -c "import hummingbot; print('Hummingbot installed successfully')"
```

## Step 4: Create Connector Wrapper Structure

### Recommended Approach: Monorepo with Hummingbot as Dependency

```bash
# Your CyberDeltaEngine structure
CyberDeltaEngine/
├── cyberdelta/
│   └── apis/
│       └── backpack/           # Your existing API
├── hummingbot_connectors/      # NEW directory
│   └── backpack/
│       ├── __init__.py
│       ├── pyproject.toml     # Using modern Python packaging
│       └── src/
│           └── backpack_connector/
│               ├── __init__.py
│               └── backpack_exchange.py
└── scripts/
    └── setup_hummingbot_uv.sh
```

## Step 5: Create the Connector Package

### Create pyproject.toml for the connector

```toml
# CyberDeltaEngine/hummingbot_connectors/backpack/pyproject.toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "hummingbot-backpack-connector"
version = "0.1.0"
description = "Backpack exchange connector for Hummingbot"
requires-python = ">=3.10,<3.12"
dependencies = [
    # Don't include hummingbot here as it's already installed
    # Don't include cyberdelta as we'll use path imports
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-asyncio>=0.21",
    "black>=23.0",
    "ruff>=0.1",
]

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.package-data]
"*" = ["*.json", "*.yml"]
```

### Create the Minimal Connector Wrapper

```python
# CyberDeltaEngine/hummingbot_connectors/backpack/src/backpack_connector/backpack_exchange.py
"""
Minimal Backpack connector wrapper for Hummingbot.
Most methods return NotImplementedError initially - implement as needed.
"""

import sys
from pathlib import Path
from decimal import Decimal
from typing import Optional, Dict, Any, List
import asyncio

# Add CyberDelta to Python path
cyber_root = Path(__file__).parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(cyber_root))

# Import YOUR EXISTING CODE - no changes needed!
from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.bp_ws_init import BackpackAPIWs

# Import Hummingbot base classes
try:
    from hummingbot.connector.exchange_py_base import ExchangePyBase
    from hummingbot.core.data_type.common import OrderType, TradeType
    from hummingbot.core.data_type.in_flight_order import InFlightOrder
    from hummingbot.core.data_type.trade_fee import TokenAmount
except ImportError as e:
    print(f"Error importing Hummingbot: {e}")
    print("Make sure Hummingbot is installed in the current environment")
    raise


class BackpackExchange(ExchangePyBase):
    """
    Minimal Backpack connector - only implements essential methods.
    Everything else raises NotImplementedError.
    """

    @classmethod
    def name(cls) -> str:
        return "backpack"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        trading_pairs: List[str] = None,
        trading_required: bool = True
    ):
        super().__init__()

        # USE YOUR EXISTING API - NO CHANGES!
        self._api = BackpackAPI(
            api_key=api_key,
            api_secret=api_secret
        )

        # Optional: WebSocket for later
        # self._ws_api = BackpackAPIWs(api_key, api_secret)

        self._trading_pairs = trading_pairs or []
        self._trading_required = trading_required

        # Required by Hummingbot
        self._last_poll_timestamp = 0
        self._in_flight_orders = {}

    # ============================================
    # MINIMAL REQUIRED IMPLEMENTATIONS
    # ============================================

    async def _update_balances(self):
        """REQUIRED: Update account balances"""
        try:
            # USE YOUR EXISTING METHOD
            balances = await self._api.get_balances()

            # Simple translation to Hummingbot format
            for balance in balances:
                asset = balance.get("currency", "")
                free = Decimal(str(balance.get("available", 0)))
                total = Decimal(str(balance.get("total", 0)))

                self._account_balances[asset] = total
                self._account_available_balances[asset] = free

        except Exception as e:
            self.logger().error(f"Error updating balances: {e}")

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        order_type: OrderType,
        is_buy: bool,
        price: Optional[Decimal] = None,
    ) -> str:
        """REQUIRED: Place an order"""
        try:
            # Map Hummingbot order type to your format
            if order_type == OrderType.MARKET:
                bp_order_type = "Market"
            elif order_type == OrderType.LIMIT:
                bp_order_type = "Limit"
            else:
                bp_order_type = "Limit"  # Default fallback

            # USE YOUR EXISTING ORDER METHOD
            result = await self._api.place_order(
                symbol=trading_pair,
                side="Buy" if is_buy else "Sell",
                order_type=bp_order_type,
                quantity=str(amount),
                price=str(price) if price else None,
                client_order_id=order_id
            )

            # Return the exchange order ID
            return result.get("id", order_id)

        except Exception as e:
            self.logger().error(f"Error placing order: {e}")
            raise

    async def _cancel(self, trading_pair: str, order_id: str) -> bool:
        """REQUIRED: Cancel an order"""
        try:
            # USE YOUR EXISTING CANCEL METHOD
            result = await self._api.cancel_order(
                symbol=trading_pair,
                order_id=order_id
            )
            return bool(result.get("success", False))

        except Exception as e:
            self.logger().error(f"Error cancelling order {order_id}: {e}")
            return False

    async def _update_order_status(self):
        """REQUIRED: Update status of in-flight orders"""
        try:
            # Minimal implementation - just check open orders
            for trading_pair in self._trading_pairs:
                # USE YOUR EXISTING METHOD
                open_orders = await self._api.get_open_orders(symbol=trading_pair)

                # Update in-flight orders
                for order_data in open_orders:
                    client_order_id = order_data.get("clientOrderId")
                    if client_order_id in self._in_flight_orders:
                        # Update order status
                        order = self._in_flight_orders[client_order_id]
                        # Basic status update - expand as needed
                        if order_data.get("status") == "FILLED":
                            self._process_trade_fill(order_data)

        except Exception as e:
            self.logger().error(f"Error updating order status: {e}")

    def ready(self) -> bool:
        """REQUIRED: Check if connector is ready"""
        # Minimal check - just return True for now
        return True

    async def check_network(self) -> None:
        """REQUIRED: Check network connectivity"""
        # Minimal implementation
        try:
            # USE YOUR EXISTING METHOD
            await self._api.get_server_time()
        except Exception as e:
            self.logger().error(f"Network check failed: {e}")
            raise

    # ============================================
    # NOT IMPLEMENTED YET - Return errors for now
    # ============================================

    async def _trading_rules(self) -> Dict[str, Any]:
        """TODO: Implement trading rules"""
        raise NotImplementedError("Trading rules not implemented yet")

    async def _update_trading_fees(self):
        """TODO: Implement fee updates"""
        raise NotImplementedError("Trading fees not implemented yet")

    async def get_order_book(self, trading_pair: str) -> Dict[str, Any]:
        """TODO: Implement order book fetching"""
        raise NotImplementedError("Order book not implemented yet")

    async def listen_for_trades(self):
        """TODO: Implement trade stream listening"""
        raise NotImplementedError("Trade stream not implemented yet")

    async def listen_for_order_book_diffs(self):
        """TODO: Implement order book diff stream"""
        raise NotImplementedError("Order book diff stream not implemented yet")

    # Additional required properties and methods...

    @property
    def status_dict(self) -> Dict[str, bool]:
        """Required property for Hummingbot UI"""
        return {
            "symbols_mapping_initialized": True,
            "order_books_initialized": False,
            "account_balance": len(self._account_balances) > 0,
            "trading_rule_initialized": False,
        }
```

## Step 6: Setup Script Using UV

```bash
#!/bin/bash
# CyberDeltaEngine/scripts/setup_hummingbot_uv.sh

set -e  # Exit on error

echo "Setting up Hummingbot with UV and Venv..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi

# Install Python 3.10 if not available
echo "Ensuring Python 3.10 is available..."
uv python install 3.10

# Get the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
WORKSPACE_ROOT="$(dirname "$PROJECT_ROOT")"

# Clone Hummingbot if not exists
if [ ! -d "$WORKSPACE_ROOT/hummingbot" ]; then
    echo "Cloning Hummingbot..."
    cd "$WORKSPACE_ROOT"
    git clone https://github.com/hummingbot/hummingbot.git
    cd hummingbot
    git checkout v1.24.0
else
    echo "Hummingbot already cloned"
fi

# Setup Hummingbot venv
cd "$WORKSPACE_ROOT/hummingbot"
if [ ! -d ".venv" ]; then
    echo "Creating Hummingbot virtual environment..."
    uv venv --python 3.10
fi

# Activate venv
source .venv/bin/activate

# Install Hummingbot dependencies
echo "Installing Hummingbot dependencies..."
uv pip install -r requirements.txt
uv pip install -e .

# Install CyberDelta connector
echo "Installing Backpack connector..."
cd "$PROJECT_ROOT/hummingbot_connectors/backpack"
uv pip install -e .

# Create test script
cat > test_connector.py << 'EOF'
#!/usr/bin/env python3
"""Test script for Backpack connector"""

import asyncio
import sys
from decimal import Decimal
from pathlib import Path

# Ensure Hummingbot is in path
hummingbot_path = Path(__file__).parent.parent.parent.parent / "hummingbot"
if hummingbot_path.exists():
    sys.path.insert(0, str(hummingbot_path))

from backpack_connector.backpack_exchange import BackpackExchange

async def test():
    print("Testing Backpack connector...")

    # Replace with your test API keys
    connector = BackpackExchange(
        api_key="YOUR_TEST_API_KEY",
        api_secret="YOUR_TEST_API_SECRET",
        trading_pairs=["SOL-USDC"]
    )

    print("Testing network connectivity...")
    await connector.check_network()
    print("✓ Network check passed")

    print("\nTesting balance fetch...")
    await connector._update_balances()
    print(f"✓ Balances: {connector._account_balances}")

    print("\nConnector test completed successfully!")

if __name__ == "__main__":
    asyncio.run(test())
EOF

echo ""
echo "✅ Setup complete!"
echo ""
echo "To test your connector:"
echo "  1. cd $PROJECT_ROOT/hummingbot_connectors/backpack"
echo "  2. source $WORKSPACE_ROOT/hummingbot/.venv/bin/activate"
echo "  3. python test_connector.py"
echo ""
echo "To run Hummingbot:"
echo "  1. cd $WORKSPACE_ROOT/hummingbot"
echo "  2. source .venv/bin/activate"
echo "  3. python bin/hummingbot.py"
```

## Step 7: Alternative - Using UV with pyproject.toml for Everything

If you want to manage everything with `uv` and modern Python packaging:

```toml
# CyberDeltaEngine/pyproject.toml (root)
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "cyberdelta"
version = "0.1.0"
requires-python = ">=3.10,<3.12"

[project.optional-dependencies]
hummingbot = [
    "hummingbot @ git+https://github.com/hummingbot/hummingbot.git@v1.24.0"
]

[tool.uv]
dev-dependencies = [
    "pytest>=7.0",
    "pytest-asyncio>=0.21",
]

[tool.uv.sources]
hummingbot = { git = "https://github.com/hummingbot/hummingbot.git", tag = "v1.24.0" }
```

Then install with:
```bash
# Install everything with uv
uv pip install -e ".[hummingbot]"
```

## Step 8: Docker Alternative (If UV/Venv Issues Persist)

If you encounter issues with UV/Venv, here's a Docker approach:

```dockerfile
# Dockerfile.hummingbot-backpack
FROM hummingbot/hummingbot:latest

# Install uv in Docker
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:${PATH}"

# Copy your CyberDelta code
COPY CyberDeltaEngine /opt/cyberdelta

# Install connector
WORKDIR /opt/cyberdelta/hummingbot_connectors/backpack
RUN uv pip install -e .

# Set working directory
WORKDIR /opt/hummingbot

# Entry point
CMD ["python", "bin/hummingbot.py"]
```

Build and run:
```bash
docker build -f Dockerfile.hummingbot-backpack -t hummingbot-backpack .
docker run -it hummingbot-backpack
```

## Comparison: UV/Venv vs Conda for Hummingbot

### UV/Venv Approach
**Pros:**
- ✅ Consistent with your CyberDelta tooling
- ✅ Modern Python packaging
- ✅ Faster dependency resolution
- ✅ No conda environment conflicts

**Cons:**
- ❌ Hummingbot not officially tested with UV
- ❌ Some C dependencies might need manual installation
- ❌ Less community support for issues

### Conda Approach (Hummingbot Default)
**Pros:**
- ✅ Official Hummingbot support
- ✅ Handles C dependencies automatically
- ✅ Well-documented troubleshooting

**Cons:**
- ❌ Different from your UV workflow
- ❌ Slower environment creation
- ❌ Potential conflicts with UV environments

## Recommendation

1. **Try UV/Venv first** - It should work for most cases
2. **Fall back to Conda** if you encounter C dependency issues
3. **Use Docker** for production deployment

## Quick Start Commands

```bash
# 1. Run setup script
cd CyberDeltaEngine
chmod +x scripts/setup_hummingbot_uv.sh
./scripts/setup_hummingbot_uv.sh

# 2. Activate environment
source ../hummingbot/.venv/bin/activate

# 3. Test connector
cd hummingbot_connectors/backpack
python test_connector.py

# 4. Run Hummingbot
cd ../../../hummingbot
python bin/hummingbot.py
```

## Troubleshooting

### Issue: C Dependencies Failed
```bash
# Install system dependencies (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install -y build-essential python3-dev

# For Mac
brew install python@3.10
```

### Issue: Import Errors
```bash
# Ensure correct Python version
python --version  # Should be 3.10.x or 3.11.x

# Reinstall with correct Python
uv venv --python 3.10 --recreate
```

### Issue: Hummingbot Not Finding Connector
```python
# In Hummingbot, manually register:
from backpack_connector.backpack_exchange import BackpackExchange
from hummingbot.connector.connector_base import ConnectorBase

ConnectorBase.add_connector("backpack", BackpackExchange)
```

## Next Steps

1. **Week 1**: Get basic connector working with test trades
2. **Week 2**: Implement WebSocket data feeds
3. **Week 3**: Add full order management
4. **Week 4**: Build orchestrator layer on top

This setup gives you:
- Full control over your code
- No public forking needed
- Easy development workflow with UV
- Path to production deployment

# Final Setup Guide: Submodule + Public Fork + Worktrees

## Overview
This guide describes the chosen approach for integrating Hummingbot with CyberDeltaEngine using:
- Your public fork as a submodule
- Worktrees for development
- Thin wrapper connector that imports from private CyberDelta code

## Directory Structure You'll Create
```bash
CyberDeltaEngine/                    # Your private main repo
├── cyberdelta/
│   └── apis/
│       └── backpack/                # Your existing API code
├── vendor/
│   └── hummingbot/                  # Your PUBLIC fork as submodule
└── worktrees/
    └── hummingbot-dev/              # Worktree for development
```

## Step 1: Fork Hummingbot on GitHub
1. Go to https://github.com/hummingbot/hummingbot
2. Click Fork → Create public fork
3. Name it whatever you want (e.g., `hummingbot` or `hummingbot-fork`)

## Step 2: Add Your Fork as Submodule
```bash
cd /workspaces/CyberDeltaEngine

# Add YOUR fork (not original) as submodule
git submodule add https://github.com/YOUR_USERNAME/hummingbot.git vendor/hummingbot

# Go into submodule
cd vendor/hummingbot

# Add upstream remote for future updates from official Hummingbot
git remote add upstream https://github.com/hummingbot/hummingbot.git
git fetch upstream

# Go back to main repo
cd ../..

# Commit the submodule addition
git add .gitmodules vendor/hummingbot
git commit -m "Add Hummingbot fork as submodule"
git push
```

## Step 3: Create Worktree for Development
```bash
# Go to the submodule
cd /workspaces/CyberDeltaEngine/vendor/hummingbot

# Create worktree branch for your connector development
git worktree add -b backpack-connector ../../worktrees/hummingbot-dev

# Now you have a separate working directory
cd ../../worktrees/hummingbot-dev

# Verify you're on the right branch
git branch  # Should show backpack-connector
```

## Step 4: Setup Python Environment in Worktree
```bash
cd /workspaces/CyberDeltaEngine/worktrees/hummingbot-dev

# Create venv (or use conda if you prefer)
python3.10 -m venv .venv
source .venv/bin/activate

# Install Hummingbot dependencies
pip install -r requirements.txt

# Install Hummingbot in development mode
pip install -e .
```

## Step 5: Create Your Connector in Worktree
```bash
cd /workspaces/CyberDeltaEngine/worktrees/hummingbot-dev

# Create connector directory
mkdir -p hummingbot/connector/exchange/backpack

# Create the wrapper file
vim hummingbot/connector/exchange/backpack/backpack_exchange.py
```

### Your Connector File Structure
```python
# hummingbot/connector/exchange/backpack/backpack_exchange.py
"""
Thin wrapper connector for Backpack exchange.
This file is PUBLIC - contains no proprietary logic.
All implementation delegates to private CyberDelta code.
"""

import sys
from pathlib import Path
from decimal import Decimal
from typing import Optional, List, Dict, Any

# Add CyberDelta to path (go up from worktree to main repo)
cyber_root = Path(__file__).parent.parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(cyber_root))

# Import private implementation
from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.bp_ws_init import BackpackAPIWs

# Import Hummingbot base classes
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.data_type.common import OrderType, TradeType

class BackpackExchange(ExchangePyBase):
    """
    Backpack exchange connector for Hummingbot.
    Delegates all operations to CyberDelta's BackpackAPI.
    """

    def __init__(self, api_key: str, api_secret: str, trading_pairs: List[str] = None):
        super().__init__()
        # Use existing CyberDelta implementation
        self._api = BackpackAPI(api_key=api_key, api_secret=api_secret)
        self._trading_pairs = trading_pairs or []

    async def _update_balances(self):
        """Delegate to CyberDelta implementation"""
        result = await self._api.get_balances()
        for balance in result:
            self._account_balances[balance["currency"]] = Decimal(balance["available"])

    async def _place_order(self, order_id: str, trading_pair: str,
                          amount: Decimal, order_type: OrderType,
                          is_buy: bool, price: Optional[Decimal] = None) -> str:
        """Delegate to CyberDelta implementation"""
        return await self._api.place_order(
            symbol=trading_pair,
            side="Buy" if is_buy else "Sell",
            quantity=str(amount),
            price=str(price) if price else None
        )

    # Additional required methods...
```

## Step 6: Test Your Setup
```bash
# In worktree with venv activated
cd /workspaces/CyberDeltaEngine/worktrees/hummingbot-dev
source .venv/bin/activate

# Test Python imports
python -c "from cyberdelta.apis.backpack import BackpackAPI; print('CyberDelta OK')"
python -c "from hummingbot.connector.exchange.backpack import BackpackExchange; print('Connector OK')"

# Run Hummingbot
python bin/hummingbot.py
```

## Step 7: Commit Your Connector to Fork
```bash
# In worktree
cd /workspaces/CyberDeltaEngine/worktrees/hummingbot-dev

# Add your connector files
git add hummingbot/connector/exchange/backpack/
git commit -m "Add Backpack exchange connector"

# Push to YOUR fork (public)
git push origin backpack-connector
```

## Step 8: Update Submodule Reference in Main Repo
```bash
# Go back to main repo
cd /workspaces/CyberDeltaEngine

# The submodule now points to your new commit
cd vendor/hummingbot
git fetch origin
git checkout backpack-connector

# Go back and update the submodule reference
cd ../..
git add vendor/hummingbot
git commit -m "Update Hummingbot submodule to include Backpack connector"
git push
```

## Daily Workflow

### Working on Your API
```bash
cd /workspaces/CyberDeltaEngine
vim cyberdelta/apis/backpack/bp_api.py
git add . && git commit -m "Update API" && git push
```

### Working on Connector
```bash
cd /workspaces/CyberDeltaEngine/worktrees/hummingbot-dev
vim hummingbot/connector/exchange/backpack/backpack_exchange.py
git add . && git commit -m "Update connector"
git push origin backpack-connector
```

### Updating from Upstream Hummingbot
```bash
cd /workspaces/CyberDeltaEngine/vendor/hummingbot
git fetch upstream
git checkout master
git merge upstream/master
git push origin master

# Rebase your connector branch
cd ../../worktrees/hummingbot-dev
git rebase master
git push --force origin backpack-connector
```

### Running Hummingbot
```bash
cd /workspaces/CyberDeltaEngine/worktrees/hummingbot-dev
source .venv/bin/activate
python bin/hummingbot.py
```

## Important Path Relationships

```
When you're in: /workspaces/CyberDeltaEngine/worktrees/hummingbot-dev
- To get to CyberDelta code: ../../cyberdelta/
- To get to submodule: ../../vendor/hummingbot/
- Your connector: ./hummingbot/connector/exchange/backpack/

The worktree is a full Hummingbot checkout linked to vendor/hummingbot
Changes in worktree can be pushed to your fork
Submodule in vendor/ tracks which commit your main repo uses
```

## Key Points to Remember

1. **Worktree** = Where you actually edit and test
2. **Submodule** = Reference to specific commit in your fork
3. **Fork** = Your public Hummingbot with thin connector wrapper
4. **Main repo** = Your private CyberDeltaEngine with actual implementation

5. The connector file has NO sensitive code, just imports from CyberDelta
6. When you push worktree changes, they go to your public fork
7. The submodule reference in your main repo stays private

## Architecture Benefits

This setup provides:
- **Complete privacy** for your implementation
- **Public contribution** possibility (can PR connector to Hummingbot)
- **Clean separation** between wrapper and implementation
- **Version control** for both Hummingbot version and your changes
- **Fast development** with worktree for immediate testing
- **Easy updates** from upstream Hummingbot

## Troubleshooting

### If imports fail in worktree
```python
# Debug path issues
import sys
from pathlib import Path
print(f"Current file: {__file__}")
print(f"Cyber root should be: {Path(__file__).parent.parent.parent.parent.parent.parent.parent}")
print(f"Python path: {sys.path}")
```

### If submodule gets out of sync
```bash
# In main repo
git submodule update --init --recursive
```

### If worktree branch diverges
```bash
# In worktree
git fetch origin
git rebase origin/backpack-connector
```

## Next Steps

1. Implement minimal connector methods (just enough to test)
2. Test with paper trading
3. Add WebSocket support for real-time data
4. Implement full connector functionality
5. Consider contributing back to Hummingbot (optional)

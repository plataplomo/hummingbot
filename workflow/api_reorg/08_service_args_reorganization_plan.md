# 08. Service Args Reorganization Plan - Clean Break Refactor

## Executive Summary
The `/cyberdelta/apis/models/service_args/common.py` file (991 lines) needs reorganization to match the domain-based structure used in the API protocol layers. This document outlines a **clean break refactor** with no backward compatibility.

## Current State
- **File**: `/cyberdelta/apis/models/service_args/common.py`
- **Lines**: 991
- **Models**: 24 generic + helper functions
- **Issues**:
  - File too large (nearly 1000 lines)
  - Mixed functional domains
  - Internal-only models mixed with public API

## Proposed Structure

Mirror the organization pattern from API protocol layers:

```
/cyberdelta/apis/models/service_args/
├── __init__.py              # NO re-exports - keep empty or minimal
├── common.py                # Base utilities ONLY
├── account.py               # Account domain args
├── trading.py               # Trading domain args
├── market_data.py           # Market data domain args
├── internal.py              # Internal-only args
├── backpack.py              # Backpack-specific (existing)
└── hyperliquid.py           # Hyperliquid-specific (existing)
```

## Model Distribution

### Following API Protocol Domain Patterns

#### `account.py` (matches AccountRequestBuilderProtocol domain)
```python
# Models that correspond to account operations
- TransferArgs              # Internal fund transfers
- WithdrawArgs              # Withdrawal operations
- UpdateAccountSettingsArgs # Account settings updates
```

#### `trading.py` (matches TradingRequestBuilderProtocol domain)
```python
# Models that correspond to trading operations
- PlaceOrderArgs           # Order placement
- CancelOrderArgs          # Order cancellation
- CancelAllOrdersArgs      # Cancel all orders
- GetOrderArgs             # Get order details
- GetOrderStatusArgs       # Alias for GetOrderArgs
- GetAllOpenOrdersArgs     # Get open orders
- GetOrderHistoryArgs      # Order history
- GetTradeHistoryArgs      # Trade/fill history
```

#### `market_data.py` (matches MarketDataRequestBuilderProtocol domain)
```python
# Models that correspond to market data queries
- GetMarketDataArgs              # Candles/OHLCV
- GetFundingRatesArgs            # Current funding rates
- GetHistoricalFundingRatesArgs  # Historical funding
- GetMarketArgs                  # Single market metadata
- GetMarketsArgs                 # All markets metadata
- GetTickerArgs                  # Ticker data
- GetOrderBookArgs               # Order book
- GetAllMidsArgs                 # Mid prices
- GetL2BookArgs                  # L2 order book
- GetRecentTradesArgs            # Recent public trades
```

#### `internal.py` (internal validation models)
```python
# Models marked as "INTERNAL USE ONLY"
- GetMaxBorrowQuantityArgs      # Max borrow limits
- GetMaxOrderQuantityArgs       # Max order limits
- GetMaxWithdrawalQuantityArgs  # Max withdrawal limits
```

#### `common.py` (keep only shared utilities)
```python
# Only truly shared components
- validate_api_str_field()      # String validation helper
- Common imports and constants
# NO MODEL CLASSES IN THIS FILE
```

## Clean Break Implementation

### Phase 1: Create New Structure
1. Create new module files with proper organization
2. Move models to appropriate modules based on domain
3. **DELETE** the original `common.py` after extracting utilities

### Phase 2: Update ALL Imports (Single Atomic Change)
1. Find and replace ALL imports across the entire codebase
2. **NO backward compatibility** - clean break
3. All imports must use the new explicit paths
4. No re-exports in `__init__.py`

### Phase 3: Verify and Cleanup
1. Run all tests to ensure nothing is broken
2. Run linters (mypy, ruff, pyright) to catch any missed imports
3. Commit as a single atomic refactor

## Example Import Changes (Clean Break)

**Before:**
```python
from cyberdelta.apis.models.service_args.common import (
    PlaceOrderArgs,      # Trading
    TransferArgs,        # Account
    GetMarketDataArgs,   # Market Data
)
```

**After (NO compatibility layer):**
```python
# MUST use explicit imports from new locations
from cyberdelta.apis.models.service_args.trading import PlaceOrderArgs
from cyberdelta.apis.models.service_args.account import TransferArgs
from cyberdelta.apis.models.service_args.market_data import GetMarketDataArgs

# NO re-exports in __init__.py - force explicit imports
```

## Import Update Script

```bash
# Example sed commands for updating imports
# Update trading imports
find . -name "*.py" -exec sed -i 's/from cyberdelta.apis.models.service_args.common import.*PlaceOrderArgs/from cyberdelta.apis.models.service_args.trading import PlaceOrderArgs/g' {} \;

# Update account imports
find . -name "*.py" -exec sed -i 's/from cyberdelta.apis.models.service_args.common import.*TransferArgs/from cyberdelta.apis.models.service_args.account import TransferArgs/g' {} \;

# ... etc for all models
```

## Benefits of Clean Break

1. **No Technical Debt**: No compatibility layers to maintain
2. **Clear Intent**: Explicit imports show exactly where models come from
3. **Forced Migration**: All code updated at once, no lingering old imports
4. **Simpler Code**: No re-export logic in `__init__.py`
5. **Better IDE Support**: Direct imports are easier for IDEs to track

## What We're NOT Doing

- ❌ Creating backward compatibility imports
- ❌ Re-exporting in `__init__.py`
- ❌ Supporting old import paths
- ❌ Gradual migration

## Success Criteria

1. All models moved to domain-specific modules
2. `common.py` contains ONLY utilities (no model classes)
3. All imports updated to use explicit paths
4. All tests pass
5. Zero import errors from linters

## Conclusion

This clean break reorganization brings service args in line with the API protocol layer's domain-based structure. By avoiding backward compatibility, we ensure:

- Clean, explicit imports
- No technical debt
- Clear module responsibilities
- Consistent architecture

The refactor should be done as a single atomic commit to minimize disruption.

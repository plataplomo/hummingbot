# Backpack Exchange Balance System Documentation

## Executive Summary

Backpack Exchange implements an **auto-lending** feature (`autoLend`) that automatically lends user funds to earn yield while keeping them available for trading. When enabled:
- The spot balance endpoint (`/api/v1/capital`) returns all zeros (funds are moved to lending pool)
- The collateral endpoint (`/api/v1/capital/collateral`) shows the lent balances
- The account settings endpoint (`/api/v1/account`) indicates if `autoLend` is enabled

**Critical Business Logic**: 
- When `autoLend=true`: True Balance = Spot Balance + Collateral Balance (though spot is usually 0)
- When `autoLend=false`: True Balance = Spot Balance (collateral may still show values)
- **Always query BOTH endpoints and sum the balances for complete accuracy**

## Overview

Backpack Exchange has a unique balance system that features **automatic lending (auto-lending)** where user funds are automatically lent out to earn yield while remaining available for trading. This creates a situation where the traditional spot balance endpoint may show zero balances even when users have funds in their account.

## Key Concepts

### 1. Auto-Lend Feature
- **Setting Name**: `autoLend` (accessible via `/api/v1/account` endpoint)
- **Default Behavior**: When enabled, funds are automatically lent out to earn yield
- **Trading Availability**: Lent funds remain fully available for trading
- **Yield Generation**: Users earn interest on their lent balances
- **Unified Account**: Spot and margin trading use the same pool of funds

### 2. Three Critical Endpoints

#### `/api/v1/account` (Account Settings)
Returns account configuration including:
- `autoLend`: Boolean indicating if auto-lending is enabled
- `autoBorrowSettlements`: Auto-borrow for settlements
- `autoRepayBorrows`: Auto-repay borrowed amounts
- `autoRealizePnl`: Auto-realize PnL

Example response:
```json
{
  "autoLend": true,
  "autoBorrowSettlements": true,
  "autoRepayBorrows": true,
  "autoRealizePnl": true,
  "borrowLimit": "2500000",
  "spotMakerFee": "8",
  "spotTakerFee": "10",
  // ... other fields
}
```

### 3. Two Balance Endpoints

#### `/api/v1/capital` (Spot Balance Endpoint)
Returns three fields for each asset:
- `available`: Funds immediately available (not in orders)
- `locked`: Funds locked in open orders
- `staked`: Funds that are staked (NOTE: This field is misleading - shows "0" even when funds are auto-staked)

**Important**: When auto-staking is enabled, this endpoint returns all zeros even if you have funds!

Example response with auto-staking enabled:
```json
{
  "SOL": {"available": "0", "locked": "0", "staked": "0"},
  "USDC": {"available": "0", "locked": "0", "staked": "0"}
}
```

#### `/api/v1/capital/collateral` (Collateral Endpoint)
Returns the true balance information including auto-staked funds:
- `totalQuantity`: Total amount of the asset (includes lent amounts)
- `lendQuantity`: Amount currently lent/staked
- `availableQuantity`: Amount available for immediate use
- `openOrderQuantity`: Amount locked in open orders

Example response showing actual balances:
```json
{
  "collateral": [
    {
      "symbol": "USDC",
      "totalQuantity": "10.6524044577",
      "lendQuantity": "10.6524044577",
      "availableQuantity": "0",
      "openOrderQuantity": "0",
      "collateralValue": "10.6524044577",
      "collateralWeight": "1"
    },
    {
      "symbol": "SOL",
      "totalQuantity": "0.0699302874",
      "lendQuantity": "0.0699302874",
      "availableQuantity": "0",
      "openOrderQuantity": "0",
      "collateralValue": "9.630096779592616743",
      "collateralWeight": "0.95"
    }
  ],
  "netEquity": "20.282501237292616743",
  "assetsValue": "20.282501237292616743"
}
```

## Real-World Example

Based on testing with a real account containing ~$20 ($10.65 USDC + ~$10 worth of SOL):

### Account Settings Response:
```json
{
  "autoLend": true,  ← Auto-lending is ENABLED
  "autoBorrowSettlements": true,
  "autoRepayBorrows": true,
  "autoRealizePnl": true
}
```

### Spot Balance Response (with autoLend=true):
```
USDC Spot: available=$0, locked=$0, staked=$0  → total=$0
SOL Spot: available=$0, locked=$0, staked=$0   → total=$0
```

### Collateral Response (with autoLend=true):
```
USDC IN COLLATERAL:
- Total Quantity: 10.6524044577
- Lend Quantity: 10.6524044577  ← All funds are lent out
- Available Quantity: 0          ← Shows 0 but can still be used for trading
- Collateral Value: $10.6524044577

SOL IN COLLATERAL:
- Total Quantity: 0.0699302874
- Lend Quantity: 0.0699302874   ← All SOL is also lent out
- Available Quantity: 0
- Collateral Value: $9.630096779592616743
```

### TRUE Balance Calculation:
```
USDC True Balance = Spot Total + Collateral Total
                  = $0 + $10.6524044577
                  = $10.6524044577 ✓

SOL True Balance = Spot Total + Collateral Total
                 = 0 + 0.0699302874
                 = 0.0699302874 SOL ✓

Total Account Value = $10.6524044577 + $9.630096779592616743
                    = $20.282501237292616743 ✓
```

## Implementation Considerations

### 1. Auto-Lending Detection Logic

```python
async def is_autolending_enabled(api: BackpackAPI) -> bool:
    """Check if auto-lending is enabled for the account."""
    # Get account settings
    account_data = await api.get_account_settings()  # /api/v1/account
    return account_data.get("autoLend", False)
```

### 2. Balance Calculation Logic

When calculating total balance for a Backpack account:
```python
# First, check autoLend status
account_data = await api.get_account_settings()  # /api/v1/account
auto_lend_enabled = account_data.get("autoLend", False)

# Get balances from BOTH endpoints
spot_balances = await api.get_balances()  # /api/v1/capital
collateral_data = await api.get_collateral()  # /api/v1/capital/collateral

# Build complete balance picture
balances = {}
for symbol, spot_balance in spot_balances.items():
    balances[symbol] = {
        "spot_total": spot_balance.total_quantity,
        "spot_available": spot_balance.available_quantity,
        "spot_locked": spot_balance.locked_quantity,
        "spot_staked": spot_balance.staked_quantity  # Usually 0 with autoLend
    }

# Add collateral data
for asset in collateral_data["collateral"]:
    symbol = asset["symbol"]
    if symbol not in balances:
        balances[symbol] = {}
    
    balances[symbol].update({
        "collateral_total": Decimal(asset["totalQuantity"]),
        "lend_quantity": Decimal(asset["lendQuantity"]),
        "collateral_available": Decimal(asset["availableQuantity"]),
        "open_orders": Decimal(asset["openOrderQuantity"])
    })

# Calculate TRUE total balance
for symbol, data in balances.items():
    if auto_lend_enabled:
        # When autoLend is true, the TRUE balance is the SUM of spot + collateral
        # because spot endpoint doesn't include lent amounts
        true_total = data.get("spot_total", 0) + data.get("collateral_total", 0)
        # In practice, spot_total is usually 0 when autoLend=true
        # So true_total ≈ collateral_total
    else:
        # When autoLend is false, both endpoints should show similar totals
        # Use spot as primary source
        true_total = data.get("spot_total", 0)
    
    data["true_total_balance"] = true_total
```

### 3. Recommended Approach

For accurate balance information:
1. **Always query ALL three endpoints**:
   - `/api/v1/account` - Get autoLend status
   - `/api/v1/capital` - Get spot balances
   - `/api/v1/capital/collateral` - Get collateral/lending balances

2. **Calculate true balance**:
   - True Balance = Spot Balance + Collateral Balance
   - This works regardless of autoLend status
   - When autoLend=true, spot is usually 0, so true balance ≈ collateral
   - When autoLend=false, collateral may still have values (e.g., from manual lending)

3. **Never rely on a single endpoint**:
   - The spot endpoint alone will miss lent funds
   - The collateral endpoint alone might miss unlent spot balances
   - Always sum both for complete picture

### 4. Key Fields Mapping

| Purpose | Spot Endpoint Field | Collateral Endpoint Field |
|---------|-------------------|-------------------------|
| Total Balance | `available + locked + staked` | `totalQuantity` |
| Available for Trading | `available` | `totalQuantity - openOrderQuantity` |
| In Open Orders | `locked` | `openOrderQuantity` |
| Lent/Staked | `staked` (unreliable) | `lendQuantity` |

## Testing Implications

### 1. Test Data Considerations
- Cassettes recorded with auto-staking enabled will show zero balances in spot endpoint
- Must use collateral endpoint responses to verify actual balances
- Test assertions should account for this behavior

### 2. Balance Validation
```python
# Don't do this:
assert spot_balance.total_quantity > 0  # Fails with auto-staking!

# Do this instead:
if account has auto-staking enabled:
    assert collateral_data['totalQuantity'] > 0
else:
    assert spot_balance.total_quantity > 0
```

### 3. Integration Test Strategy
1. Check both endpoints
2. If spot shows all zeros but collateral shows positive, auto-staking is enabled
3. Use collateral data for assertions when auto-staking is detected

## Key Findings

### 1. Auto-Lending Detection ✅ SOLVED
- **Endpoint**: `/api/v1/account` returns `autoLend` boolean field
- **Direct Detection**: No need to infer from balance comparisons
- **Account Settings**: Full visibility into all auto-* features

### 2. API Behavior Clarifications
- The `staked` field in spot balance endpoint is separate from auto-lending
- When `autoLend=true`, funds are in the lending pool but spot endpoint shows zeros
- The collateral endpoint always shows true balances regardless of autoLend setting
- `lendQuantity` in collateral response shows amount earning yield

### 3. Edge Cases and Important Notes
- **Manual Lending**: Even with `autoLend=false`, users can manually lend, so always check both endpoints
- **Partial Lending**: Some funds might be in spot, some in lending - sum both
- **Open Orders**: Check `openOrderQuantity` in collateral data for funds locked in orders
- **Available for Trading**: When lent, funds show as `availableQuantity=0` but are still tradeable
- **Precision**: Use Decimal type for all calculations to avoid floating-point errors

## CyberDelta Implementation Status

### Current Implementation
1. **`get_balances()`**: Uses only the spot endpoint (`/api/v1/capital`), which returns zeros when auto-staking is enabled
2. **`get_account_summary()`**: Has enhanced implementation that uses collateral endpoint (`/api/v1/capital/collateral`) via `_get_enhanced_account_info()`
3. **Collateral Support**: Already implemented in `_fetch_raw_collateral()` method

### Recommendations for Improvement

1. **Enhanced Balance Retrieval**: 
   - Modify `get_balances()` to optionally check collateral endpoint when spot shows all zeros
   - Add a parameter like `use_collateral_fallback=True` to enable this behavior
   - Return SpotBalance with `lend_quantity` populated in `bp_details`

2. **Auto-Staking Detection**: 
   - Add helper method `is_autostaking_enabled()` that compares spot vs collateral data
   - Cache this status to avoid repeated checks

3. **Test Updates**: 
   - Update balance tests to use `get_account_summary()` for accurate balance data when cassettes show zero spot balances
   - Add specific tests for auto-staking scenario

4. **Documentation**: 
   - Add docstring warnings to `get_balances()` about auto-staking behavior
   - Recommend using `get_account_summary()` for accurate balance information

5. **SpotBalance Enhancement**:
   - The `BackpackSpotBalanceDetails` model already has `lend_quantity` field
   - Populate this from collateral endpoint data when available

## Practical Implementation Example

```python
async def get_true_balances(api: BackpackAPI) -> dict[str, Decimal]:
    """Get true balances by combining spot and collateral data."""
    # 1. Get account settings
    account_data = await api._get_raw_account_summary()  # /api/v1/account
    auto_lend = account_data.auto_lend
    
    # 2. Get spot balances
    spot_balances = await api.get_balances()  # /api/v1/capital
    
    # 3. Get collateral data
    collateral_raw = await api._fetch_raw_collateral()  # /api/v1/capital/collateral
    
    # 4. Combine both sources
    true_balances = {}
    
    # Add spot balances
    for symbol, spot_balance in spot_balances.items():
        true_balances[symbol] = spot_balance.total_quantity
    
    # Add collateral balances
    if collateral_raw and collateral_raw.collateral:
        for asset in collateral_raw.collateral:
            symbol = asset.symbol
            collateral_total = asset.total_quantity
            
            if symbol in true_balances:
                # Sum spot + collateral
                true_balances[symbol] += collateral_total
            else:
                # Asset only in collateral
                true_balances[symbol] = collateral_total
    
    # Log for debugging
    logger.info(f"AutoLend enabled: {auto_lend}")
    for symbol, balance in true_balances.items():
        if balance > 0:
            logger.info(f"{symbol}: {balance}")
    
    return true_balances
```

## References

- Backpack API Documentation: Balance endpoint shows `staked` field but doesn't reflect auto-lending
- Real account testing confirms spot + collateral summation is required
- Auto-lending moves funds between endpoints but doesn't change total balance
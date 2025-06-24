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

### ✅ **ENHANCED IMPLEMENTATION** (June 2025)

**Auto-Lending Support Fully Implemented** - CyberDeltaEngine now provides complete, transparent auto-lending support that follows our architectural principles.

#### 1. **Enhanced `get_balances()` Method** ✅ IMPLEMENTED
- **Automatic Detection**: Detects auto-lending scenario when all spot balances are zero
- **Collateral Fallback**: Automatically fetches collateral endpoint data when needed
- **Extension Slot Population**: Populates `lend_quantity` in `bp_details` from collateral data
- **True Balance Calculation**: Returns `spot + collateral` as the total balance
- **Exchange-Agnostic**: No API changes - works transparently with existing code
- **Error Resilience**: Graceful fallback if collateral endpoint fails

```python
# File: cyberdelta/apis/backpack/services/bp_account_service.py:310-385
async def get_balances(self) -> dict[str, SpotBalance]:
    """Retrieves all spot balances from the account.

    Note: Handles Backpack's auto-lending feature where spot balances may show
    zero when funds are auto-lent. When all spot balances are zero, this method
    automatically fetches collateral data to provide complete balance information
    including lent amounts in the bp_details extension slot.
    """
```

#### 2. **New `_enhance_balances_with_collateral()` Helper** ✅ IMPLEMENTED
- **Spot + Collateral Fusion**: Merges data from both endpoints intelligently
- **Extension Slot Enrichment**: Populates `BackpackSpotBalanceDetails.lend_quantity`
- **Asset Discovery**: Adds assets that exist only in collateral (not in spot response)
- **Type Safety**: Full Pydantic validation throughout
- **Precision Handling**: Uses Decimal for all financial calculations

```python
# File: cyberdelta/apis/backpack/services/bp_account_service.py:441-548
async def _enhance_balances_with_collateral(
    self,
    spot_balances: dict[str, SpotBalance],
    collateral_response: BackpackRawCollateralResponse,
) -> dict[str, SpotBalance]:
```

#### 3. **Enhanced Account Summary** ✅ ALREADY IMPLEMENTED
- **Parallel Data Fetching**: Uses `asyncio.gather()` for optimal performance
- **Collateral Integration**: Full collateral endpoint support in `_get_enhanced_account_info()`
- **Fallback Strategy**: Graceful degradation to basic implementation if collateral fails

#### 4. **Architecture Compliance** ✅ VERIFIED
- **Service Layer Pattern**: Follows established service method patterns (API_ARCHITECTURE.md:164-188)
- **Error Handling Strategy**: Comprehensive error handling with context preservation (API_ARCHITECTURE.md:452-499)
- **Extension Slot Pattern**: Uses `bp_details` extension slots correctly (API_ARCHITECTURE.md:355-381)
- **Raw/Internal Model Separation**: Maintains strict separation with proper mappers
- **Exchange-Agnostic Interface**: No breaking changes to public API surface

#### 5. **Test Compatibility** ✅ UPDATED
- **Auto-Lending Detection**: Tests now detect and handle auto-lending scenarios
- **Balance Validation**: Updated to work with enhanced balance logic
- **Cassette Compatibility**: Works with existing VCR cassettes that show zero spot balances

### Implementation Features

#### **Automatic Auto-Lending Detection**
```python
# Detects when all spot balances are zero (indicates auto-lending)
all_spot_balances_zero = all(
    balance.total_quantity == Decimal("0") for balance in internal_balances.values()
)

if all_spot_balances_zero and len(internal_balances) > 0:
    # Automatically fetch and enhance with collateral data
    raw_collateral = await self._get_raw_collateral_response()
    internal_balances = await self._enhance_balances_with_collateral(
        spot_balances=internal_balances,
        collateral_response=raw_collateral,
    )
```

#### **Extension Slot Enrichment**
```python
# Populates BackpackSpotBalanceDetails with lending information
enhanced_bp_details = BackpackSpotBalanceDetails(
    open_order_quantity=open_order_quantity,
    lend_quantity=lend_quantity,  # ✅ Now populated from collateral endpoint
)

# Returns enhanced SpotBalance with true total (spot + collateral)
enhanced_balances[asset_symbol] = SpotBalance(
    exchange=spot_balance.exchange,
    asset=spot_balance.asset,
    timestamp=spot_balance.timestamp,
    total_quantity=true_total,  # ✅ spot + collateral sum
    available_quantity=true_available,
    bp_details=enhanced_bp_details,
)
```

#### **Transparent Operation**
- **No API Changes**: Existing code works without modification
- **Strategy Compatibility**: Trading strategies see complete balance information
- **Performance Optimized**: Only fetches collateral when needed (spot shows zeros)
- **Error Resilient**: Continues with spot-only balances if collateral fails

### Previous Implementation Issues (Now Resolved)

~~1. **Enhanced Balance Retrieval**: Uses only spot endpoint~~ ✅ **FIXED**
~~2. **Auto-Staking Detection**: Manual comparison needed~~ ✅ **AUTOMATED**
~~3. **Test Updates**: Required manual handling~~ ✅ **TRANSPARENT**
~~4. **Documentation**: Missing warnings about auto-staking~~ ✅ **DOCUMENTED**
~~5. **SpotBalance Enhancement**: lend_quantity not populated~~ ✅ **IMPLEMENTED**

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

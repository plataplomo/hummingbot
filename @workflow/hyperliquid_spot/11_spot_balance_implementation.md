# Hyperliquid Spot Balance Implementation Analysis

## Current Balance API Structure (v0.0.1)

### API Endpoint: `get_balances()`
- **Returns**: `dict[str, SpotBalance]`
- **Source**: Derives from Hyperliquid's `/info` endpoint (`clearinghouseState`)
- **Current Implementation**: Returns **total** balances across all account types (spot + perp combined)

### Key Finding: Current Implementation Limitation

**IMPORTANT**: This analysis is based on our current v0.0.1 implementation, not a definitive statement about Hyperliquid's API capabilities.

Our current implementation **does not** separate spot vs perp account balances:

```python
# Current API response structure:
{
    "USDC": SpotBalance(
        total_quantity=Decimal("1000.0"),    # Combined spot + perp
        available_quantity=Decimal("950.0"),  # Combined available
        # No individual spot_balance or perp_balance fields
    )
}
```

## Raw API Data Structure Analysis (Current v0.0.1 Models)

**Note**: These are our current Pydantic models - they may not fully represent all data available in Hyperliquid's actual API responses.

### `HyperliquidRawClearinghouseState`
```python
class HyperliquidRawClearinghouseState(BaseModel):
    asset_positions: list[HyperliquidRawAssetPosition]
    margin_summary: HyperliquidRawMarginSummary
    cross_margin_summary: HyperliquidRawMarginSummary
    # No separate spot/perp balance fields
```

### `HyperliquidRawMarginSummary`
```python
class HyperliquidRawMarginSummary(BaseModel):
    account_value: RawFiniteDecimalStr        # Total account value
    total_margin_used: RawFiniteDecimalStr    # Used margin
    total_ntl_pos: RawFiniteDecimalStr        # Net position value
    total_raw_usd: RawFiniteDecimalStr        # Raw USD balance
    # Note: 'total_raw_usd' might be the combined balance
```

### `HyperliquidRawAssetPosition`
```python
class HyperliquidRawAssetPosition(BaseModel):
    position: HyperliquidRawPositionInfo
    type: str | None  # Could indicate spot vs perp
    # Individual balances not separated here either
```

## Current Test Issue

### Problem with Current Test
```python
# Current test assumes all funds are in perp account:
initial_amount = await self._get_initial_usdc_balance(hl_api_for_test_env)

# Step 2: Move all funds from perp to spot
await self._execute_transfer_and_validate(hl_api_for_test_env, "perp", "spot", initial_amount)
```

**Issue**: `initial_amount` is the **total** balance (spot + perp), but we're trying to transfer it all **from perp only**. This will fail if funds are distributed across both accounts.

## Current Implementation Limitations

### What We Know About Our v0.0.1 Implementation:
1. **Current Mapping**: Our balance mapper returns combined spot + perp balances
2. **Transfer Mechanism Works**: Internal transfers between spot ↔ perp are functional
3. **Current Models**: Don't expose individual spot vs perp balances separately

### What We DON'T Know About Hyperliquid's API:
- **Actual API Capability**: Whether Hyperliquid's raw response includes individual account balances
- **Missing Fields**: Our models might not capture all available data fields
- **Alternative Endpoints**: Whether separate endpoints exist for spot-only or perp-only balances

### Current Implementation Constraints:
- **Cannot determine account distribution**: No way to know how much is in spot vs perp with current code
- **Transfer amounts must be conservative**: Cannot assume total balance equals single account balance
- **Test strategy needs adjustment**: Must use smaller, known amounts rather than total balance

## Proposed Solutions

### Option 1: Conservative Test Amounts
```python
# Use small fixed amounts instead of total balance
test_amount = Decimal("1.0")  # Small test amount
if initial_total_balance < test_amount * 2:
    pytest.skip("Insufficient balance for transfer testing")
```

### Option 2: Sequential Small Transfers
```python
# Start with tiny amounts to establish state
small_amount = Decimal("0.1")
# Try perp→spot first, if it fails, try spot→perp
# Then do the reverse to validate both directions
```

### Option 3: Error-Tolerant Testing
```python
# Expect some transfers to fail due to insufficient balance
# Test that the API properly reports failures vs successes
try:
    transfer_result = await api.transfer(args)
    # Validate successful transfer
except APIError as e:
    # Validate proper error handling for insufficient balance
```

## Recommended Test Strategy

Based on this analysis, the test should:

1. **Use small, fixed amounts** (e.g., 1-10 USDC) rather than total balance
2. **Test both directions** (spot→perp and perp→spot) with tolerance for failures
3. **Focus on API functionality** rather than perfect balance state management
4. **Validate error handling** for insufficient balance scenarios
5. **Ensure no net balance change** after successful round-trip transfers

## Future Enhancement Possibilities

### Potential API Extension
If individual account balances are needed, could extend `HyperliquidSpotBalanceDetails`:

```python
class HyperliquidSpotBalanceDetails(BaseModel):
    spot_balance: Decimal | None = None      # Future: spot-only balance
    perp_balance: Decimal | None = None      # Future: perp-only balance
    # Currently empty - no individual account data available
```

### Alternative: Separate Balance Queries
Could implement separate methods if Hyperliquid provides individual account endpoints:
```python
async def get_spot_balances(self) -> dict[str, SpotBalance]:
    # If/when Hyperliquid provides spot-only balance endpoint

async def get_perp_balances(self) -> dict[str, SpotBalance]:
    # If/when Hyperliquid provides perp-only balance endpoint
```

## Conclusion

The current v0.0.1 balance implementation has limitations in exposing individual spot vs perp account balances. However, this may be due to:

1. **Incomplete modeling** - Our Pydantic models might not capture all available API data
2. **Missing endpoints** - We might not be using the right API endpoints for individual balances
3. **Actual API limitation** - Hyperliquid's API might genuinely only provide total balances

**Immediate Recommendation**: Update the test to use conservative amounts and focus on transfer functionality validation rather than total balance movements.

**Future Investigation Needed**:
- Review Hyperliquid's official API documentation for balance endpoints
- Examine raw API responses to identify any missed fields
- Research if alternative endpoints provide individual account balances
- Consider implementing separate balance queries if supported by the API

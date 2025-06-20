# Hyperliquid Perpetual vs Spot Trading Support Analysis

## Executive Summary

After a comprehensive analysis of the CyberDeltaEngine codebase and cross-referencing with Hyperliquid's official SDK, I've discovered that **Hyperliquid DOES support spot trading**, contrary to what the current test files suggest. The codebase needs to be updated to properly implement this functionality.

## Key Findings

### 1. API Implementation Status

#### Perpetual Trading (✅ Fully Supported in CyberDeltaEngine)
- Complete implementation in `/cyberdelta/apis/hyperliquid/`
- Full trading lifecycle support: place orders, cancel orders, query positions
- Market data: order books, trades, funding rates, candles
- Account management: balances, positions, PnL tracking
- WebSocket support for real-time data

#### Spot Trading (✅ Supported by Hyperliquid, ❌ Not Implemented in CyberDeltaEngine)
- **Hyperliquid API supports spot trading**
- CyberDeltaEngine incorrectly assumes it's not supported
- All spot-related tests are placeholders based on incorrect assumptions
- Infrastructure needs to be updated to support spot markets

### 2. Evidence from Official Hyperliquid SDK

The official Hyperliquid Python SDK includes spot trading examples:
```python
# From examples/basic_spot_order.py
PURR = "PURR/USDC"
OTHER_COIN = "@8"
OTHER_COIN_NAME = "KORILA/USDC"

# Place spot orders
order_result = exchange.order("PURR/USDC", True, 24, 0.5, {"limit": {"tif": "Gtc"}})
```

### 3. Spot Market Implementation Details

#### Asset Index System
The key differentiator between perp and spot markets is the asset index:
- **Perpetuals**: Start at index 0
- **Spot Assets**: Start at index 10,000

```python
# From the SDK
# spot assets start at 10000
for spot_info in spot_meta["universe"]:
    asset = spot_info["index"] + 10000
```

#### Symbol Naming Patterns
Spot markets use two naming conventions:

1. **Canonical Names**: `"PURR/USDC"`, `"KORILA/USDC"` - Human-readable format
2. **Index References**: `"@1"`, `"@2"`, `"@8"` - Direct index references

#### Spot Meta Response Structure
```json
{
  "universe": [
    {
      "tokens": [1, 0],  // [base_token_index, quote_token_index]
      "name": "PURR/USDC",
      "index": 0,
      "isCanonical": true
    },
    {
      "tokens": [2, 0],
      "name": "@1",
      "index": 1,
      "isCanonical": false
    }
  ],
  "tokens": [
    {
      "name": "USDC",
      "szDecimals": 8,
      "weiDecimals": 8
    },
    {
      "name": "PURR",
      "szDecimals": 5,
      "weiDecimals": 18
    }
  ]
}
```

### 4. Required API Endpoints for Spot

- `info.spot_meta()` - Get all spot market metadata
- `info.spot_meta_and_asset_ctxs()` - Get spot metadata with market contexts  
- `info.spot_user_state(address)` - Get user's spot balances
- `exchange.order()` - Same endpoint for both spot and perp orders

## Implementation Recommendations

### 1. Update Asset Index Resolution

Modify `/cyberdelta/apis/hyperliquid/hl_asset_indexer.py`:

```python
class HyperliquidAssetIndexResolver:
    SPOT_ASSET_OFFSET = 10000
    
    async def resolve_symbol_to_asset_index(self, symbol: str) -> int | None:
        # Check if it's a spot market reference
        if symbol.startswith("@"):
            # Direct index reference for spot
            spot_index = int(symbol[1:])
            return spot_index + self.SPOT_ASSET_OFFSET
        
        # Check if it's a canonical spot name (contains "/")
        if "/" in symbol:
            # Fetch spot meta and resolve
            spot_meta = await self._fetch_spot_meta()
            return self._resolve_spot_symbol(symbol, spot_meta)
        
        # Otherwise, it's a perpetual
        return await self._resolve_perp_symbol(symbol)
```

### 2. Add Spot Meta Models

Create new models in `/cyberdelta/apis/hyperliquid/models/`:

```python
# hl_raw_spot_meta.py
class HyperliquidRawSpotToken(BaseModel):
    name: str
    sz_decimals: int = Field(alias="szDecimals")
    wei_decimals: int = Field(alias="weiDecimals")

class HyperliquidRawSpotMarket(BaseModel):
    tokens: list[int]  # [base_index, quote_index]
    name: str
    index: int
    is_canonical: bool = Field(alias="isCanonical")

class HyperliquidRawSpotMeta(BaseModel):
    universe: list[HyperliquidRawSpotMarket]
    tokens: list[HyperliquidRawSpotToken]
```

### 3. Update Market Data Mapper

Modify `/cyberdelta/apis/hyperliquid/mappers/hl_market_data_mapper.py`:

```python
def _determine_market_type(self, asset_index: int) -> str:
    """Determine if market is spot or perpetual based on asset index."""
    if asset_index >= 10000:
        return "Spot"
    return "Perpetual"

def _transform_to_market(self, asset_def, asset_index: int):
    market_type = self._determine_market_type(asset_index)
    
    if market_type == "Spot":
        # Parse spot market specifics
        base_symbol, quote_symbol = self._parse_spot_symbol(asset_def.name)
    else:
        # Existing perp logic
        base_symbol = asset_def.name
        quote_symbol = "USD"
```

### 4. Implement Spot-Specific Services

Add methods to trading service:

```python
async def get_spot_balances(self) -> dict[str, SpotBalance]:
    """Get spot token balances."""
    spot_state = await self._get_spot_user_state()
    return self._mapper.transform_spot_balances(spot_state)

async def place_spot_order(self, args: PlaceOrderArgs) -> Order:
    """Place a spot market order."""
    # Validate it's a spot symbol
    if not self._is_spot_symbol(args.symbol):
        raise APIError("Symbol is not a spot market", APIErrorCode.INVALID_SYMBOL)
    
    # Use existing order placement logic
    return await self.place_order(args)
```

### 5. Update Symbol Validation

```python
def _is_spot_symbol(self, symbol: str) -> bool:
    """Check if symbol is a spot market."""
    return symbol.startswith("@") or "/" in symbol

def _validate_spot_order_params(self, args: PlaceOrderArgs):
    """Validate spot-specific order parameters."""
    # Spot orders don't support certain features
    if args.reduce_only:
        raise APIError("Spot orders cannot be reduce-only")
    if args.trigger_price:
        raise APIError("Spot markets don't support stop orders")
```

### 6. Test Updates

Update the spot tests to reflect actual functionality:

```python
@pytest.mark.asyncio
async def test_place_spot_order(hl_api_for_test_env):
    """Test placing a spot order on PURR/USDC market."""
    place_args = PlaceOrderArgs(
        symbol="PURR/USDC",  # Real spot market
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("100"),
        price=Decimal("0.5"),
        time_in_force=TimeInForce.GTC,
    )
    
    order = await hl_api_for_test_env.place_order(place_args)
    assert order.symbol == "PURR/USDC"
    assert order.market_type == "Spot"
```

### 7. Configuration Updates

Add spot market configuration:

```yaml
# config/hyperliquid.yaml
spot_markets:
  enabled: true
  canonical_symbols:
    - "PURR/USDC"
    - "KORILA/USDC"
  index_offset: 10000
```

## Migration Path

1. **Phase 1: Discovery**
   - Implement spot meta fetching
   - Add spot market detection logic
   - Update asset indexer

2. **Phase 2: Read Operations**
   - Implement spot balance queries
   - Add spot market data support
   - Update market type detection

3. **Phase 3: Trading**
   - Enable spot order placement
   - Add spot-specific validations
   - Update order status handling

4. **Phase 4: Testing**
   - Remove incorrect "not implemented" assumptions
   - Add real spot market tests
   - Verify with testnet spot markets

## Key Differences: Spot vs Perpetuals

| Feature | Perpetuals | Spot |
|---------|------------|------|
| Asset Index | 0-9999 | 10000+ |
| Symbol Format | `"BTC"`, `"ETH"` | `"PURR/USDC"`, `"@1"` |
| Quote Currency | Always USD | Variable (usually USDC) |
| Funding Rates | Yes | No |
| Leverage | Yes (up to max) | No (1x only) |
| Position Type | Long/Short | Asset ownership |
| Balance Type | USD collateral | Token balances |

## Conclusion

The current CyberDeltaEngine implementation incorrectly assumes Hyperliquid doesn't support spot trading. In reality, Hyperliquid has a full spot trading implementation with:
- Dedicated spot markets
- Token pair trading
- Separate balance system
- Same order execution flow as perpetuals

The codebase should be updated to:
1. Remove the "not implemented" placeholders
2. Add proper spot market detection
3. Implement spot-specific features
4. Update tests to use real spot markets

This will enable CyberDeltaEngine to fully leverage Hyperliquid's spot trading capabilities.
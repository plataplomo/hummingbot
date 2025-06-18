# Hyperliquid Market Orders Implementation Documentation (Improved)

## Overview

This document details the technical findings and implementation requirements for Hyperliquid market orders based on analysis of the official Hyperliquid SDK, API behavior, and CyberDeltaEngine's current implementation.

## Key Discovery: Market Orders are IoC Limit Orders

**Critical Finding**: Hyperliquid does NOT support true market orders. Instead, market orders are implemented as aggressive Immediate-or-Cancel (IoC) limit orders.

### Evidence from Official SDK

From the Hyperliquid Python SDK (`hyperliquid-python-sdk`):

```python
# HYPERLIQUID SDK CODE (REFERENCE ONLY - USES FLOATS!)
def market_open(
    self,
    name: str,
    is_buy: bool,
    sz: float,                    # ❌ FLOAT - UNACCEPTABLE FOR TRADING ENGINE
    px: Optional[float] = None,   # ❌ FLOAT - PRECISION LOSS RISK
    slippage: float = DEFAULT_SLIPPAGE,  # ❌ FLOAT - ROUNDING ERRORS
    cloid: Optional[Cloid] = None,
    builder: Optional[BuilderInfo] = None,
) -> Any:
    # Get aggressive Market Price
    px = self._slippage_price(name, is_buy, slippage, px)
    # Market Order is an aggressive Limit Order IoC
    return self.order(
        name, is_buy, sz, px, order_type={"limit": {"tif": "Ioc"}}, reduce_only=False, cloid=cloid, builder=builder
    )
```

**⚠️ CRITICAL WARNING**: The official Hyperliquid SDK uses `float` types, which is completely unacceptable for a professional trading engine due to precision loss and rounding errors.

**Key Quote**: `"Market Order is an aggressive Limit Order IoC"`

## CyberDeltaEngine Current Implementation Status

### What's Already Implemented ✅

1. **Request Builder Support** (`hl_request_builder.py:382-387`):
   - Already converts market orders to IoC limit orders
   - Expects aggressive price to be provided by service layer
   - Proper Decimal handling with `_decimal_to_wire_format` method

2. **Order Book Access** (`hl_market_data_service.py:407`):
   - `get_order_book()` method available for L2 book data
   - Returns OrderBook model with bids/asks as Decimal tuples

3. **Ticker Data** (`hl_market_data_service.py:246`):
   - `get_ticker()` method provides mark price via asset contexts

4. **Decimal Precision Infrastructure**:
   - All price/quantity handling uses Decimal throughout
   - Proper wire format conversion with 8 decimal places
   - Validation for finite values

### What's Currently Blocked ❌

1. **Service Layer Validation** (`hl_trading_service.py:554-560`):
   ```python
   if args.order_type == OrderType.MARKET:
       raise ValueError(
           f"[{current_method}] Market orders are not currently supported. "
           "Market order implementation requires integration with real-time "
           "market data service to calculate safe aggressive pricing. "
           "Use limit orders instead."
       )
   ```

2. **Missing Components**:
   - No `all_mids` endpoint implementation (for efficient mid-price fetching)
   - No slippage calculation service
   - No market order pricing configuration

## Technical Implementation Details

### 1. Order Type Structure

**WRONG** (causes 422 "Failed to deserialize" error):
```json
{
  "type": "order",
  "orders": [{
    "a": 3,
    "b": true,
    "p": "0",
    "s": "0.0001",
    "r": false,
    "t": {"market": {}}
  }],
  "grouping": "na"
}
```

**CORRECT** (IoC limit order):
```json
{
  "type": "order",
  "orders": [{
    "a": 3,
    "b": true,
    "p": "52500.25",
    "s": "0.0001", 
    "r": false,
    "t": {"limit": {"tif": "Ioc"}}
  }],
  "grouping": "na"
}
```

### 2. Aggressive Pricing Algorithm

The official SDK uses the following pricing strategy:

```python
# HYPERLIQUID SDK CODE (REFERENCE ONLY - USES FLOATS!)
def _slippage_price(
    self,
    name: str,
    is_buy: bool,
    slippage: float,              # ❌ FLOAT - PRECISION RISK
    px: Optional[float] = None,   # ❌ FLOAT - ROUNDING ERRORS
) -> float:                       # ❌ FLOAT - UNACCEPTABLE RETURN TYPE
    coin = self.info.name_to_coin[name]
    if not px:
        # Get midprice
        px = float(self.info.all_mids()[coin])  # ❌ FLOAT CONVERSION
    
    asset = self.info.coin_to_asset[coin]
    # spot assets start at 10000
    is_spot = asset >= 10_000
    
    # Calculate Slippage - FLOAT ARITHMETIC = PRECISION LOSS!
    px *= (1 + slippage) if is_buy else (1 - slippage)  # ❌ DANGEROUS
    # Round px to 5 significant figures and 6 decimals for perps, 8 decimals for spot
    return round(float(f"{px:.5g}"), (6 if not is_spot else 8) - self.info.asset_to_sz_decimals[asset])
```

**⚠️ FINANCIAL RISK WARNING**: This algorithm uses floating-point arithmetic throughout, causing:
- Precision loss in price calculations
- Rounding errors in slippage computation  
- Potential financial discrepancies

**Algorithm Logic** (must be reimplemented with Decimal precision):
- Gets current mid price from `all_mids()` endpoint
- Applies slippage: `(1 + slippage)` for buys, `(1 - slippage)` for sells
- Default slippage is typically 0.05 (5%)
- Different precision for perps vs spot assets

## MANDATORY: Decimal-Based Implementation

**FOR CYBERDELTAENGINE - ALL PRICING MUST USE DECIMAL**:

```python
async def calculate_aggressive_price(
    self,
    symbol: str,
    side: OrderSide,
    slippage: Decimal,                    # ✅ DECIMAL - EXACT PRECISION
    current_price: Decimal | None = None  # ✅ DECIMAL - NO ROUNDING ERRORS
) -> Decimal:                             # ✅ DECIMAL RETURN TYPE
    """Calculate aggressive market order price with exact precision."""
    
    if current_price is None:
        # Option 1: Use order book for more accurate pricing
        order_book = await self.market_data_service.get_order_book(symbol)
        if order_book:
            if side == OrderSide.BUY:
                # Use best ask for buy orders
                current_price = order_book.asks[0][0] if order_book.asks else None
            else:
                # Use best bid for sell orders
                current_price = order_book.bids[0][0] if order_book.bids else None
        
        # Option 2: Fall back to ticker mark price
        if current_price is None:
            ticker = await self.market_data_service.get_ticker(symbol)
            if ticker and ticker.price:
                current_price = ticker.price
            else:
                raise ValueError(f"Unable to get current price for {symbol}")
    
    # Exact decimal arithmetic - no precision loss
    if side == OrderSide.BUY:
        aggressive_price = current_price * (Decimal("1") + slippage)
    else:
        aggressive_price = current_price * (Decimal("1") - slippage)
    
    # Apply proper decimal rounding for exchange precision requirements
    return self.round_to_exchange_precision(aggressive_price, symbol)
```

## Proposed Implementation Approach

### Phase 1: Add AllMids Support (Optional but Efficient)
1. Add `HyperliquidRawAllMidsRequestPayload` to request builder
2. Implement `get_all_mids()` in market data service
3. Use for efficient batch price fetching

### Phase 2: Implement Market Order Pricing Service
```python
class HyperliquidMarketOrderPricingService:
    """Service for calculating aggressive prices for market orders."""
    
    def __init__(
        self,
        market_data_service: HyperliquidMarketDataService,
        config: MarketOrderConfig
    ):
        self._market_data = market_data_service
        self._config = config
    
    async def calculate_aggressive_price(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        max_slippage: Decimal | None = None
    ) -> Decimal:
        """Calculate aggressive price with safety checks."""
        
        # Get order book for accurate pricing
        order_book = await self._market_data.get_order_book(symbol)
        if not order_book:
            raise APIError(
                code=APIErrorCode.MARKET_DATA_UNAVAILABLE.value,
                message=f"Cannot calculate market order price: no order book for {symbol}"
            )
        
        # Check liquidity
        available_liquidity = self._calculate_available_liquidity(
            order_book, side, quantity
        )
        
        if available_liquidity < quantity:
            raise APIError(
                code=APIErrorCode.INSUFFICIENT_LIQUIDITY.value,
                message=f"Insufficient liquidity for {quantity} {symbol}"
            )
        
        # Calculate volume-weighted average price with slippage
        vwap = self._calculate_vwap(order_book, side, quantity)
        
        # Apply configured slippage
        slippage = self._get_slippage_for_symbol(symbol)
        if max_slippage:
            slippage = min(slippage, max_slippage)
        
        if side == OrderSide.BUY:
            aggressive_price = vwap * (Decimal("1") + slippage)
        else:
            aggressive_price = vwap * (Decimal("1") - slippage)
        
        # Validate price bounds
        self._validate_price_bounds(aggressive_price, symbol, side)
        
        return aggressive_price
```

### Phase 3: Update Trading Service
```python
# In hl_trading_service.py, replace the market order validation with:

if args.order_type == OrderType.MARKET:
    # Calculate aggressive price for market order
    pricing_service = self._get_market_order_pricing_service()
    
    try:
        aggressive_price = await pricing_service.calculate_aggressive_price(
            symbol=args.symbol,
            side=args.side,
            quantity=args.quantity,
            max_slippage=self._config.max_market_order_slippage
        )
        
        # Update args with calculated price
        args = args.model_copy(update={"price": aggressive_price})
        
        logger.info(
            f"Market order for {args.symbol} {args.side.value} "
            f"will use aggressive price: {aggressive_price}"
        )
        
    except APIError as e:
        logger.error(f"Failed to calculate market order price: {e}")
        raise
```

### Phase 4: Configuration System
```yaml
# config/market_orders.yaml
hyperliquid:
  market_orders:
    enabled: true
    default_slippage: "0.01"  # 1%
    max_slippage: "0.05"      # 5% safety limit
    
    slippage_by_symbol:
      BTC: "0.005"            # 0.5% for high liquidity
      ETH: "0.005"
      SOL: "0.01"
      default: "0.02"         # 2% for others
    
    price_bounds:
      max_deviation_from_mark: "0.10"  # 10% from mark price
      stale_price_threshold_seconds: 5
    
    liquidity_requirements:
      min_book_depth_multiple: 2.0  # Need 2x order size in book
      max_single_level_percentage: 0.5  # Max 50% from one level
```

## Testing Requirements

### Unit Tests
- [x] IoC limit order structure validation (already in codebase)
- [ ] Price calculation with different slippage values
- [ ] VWAP calculation with various order book depths
- [ ] Decimal precision preservation
- [ ] Configuration loading and symbol-specific slippage

### Integration Tests
- [ ] Real order book data fetching
- [ ] Market order execution with various symbols
- [ ] Slippage behavior under different market conditions
- [ ] Error handling for insufficient liquidity
- [ ] Price bound validation

### Safety Tests
- [ ] Maximum slippage enforcement
- [ ] Extreme market condition handling (wide spreads)
- [ ] Network failure during price fetching
- [ ] Stale price data detection

## Security Considerations

### Financial Risk Mitigation
1. **Hard Slippage Caps**: Never exceed configured maximum slippage
2. **Liquidity Validation**: Ensure sufficient order book depth
3. **Price Sanity Checks**: Validate against mark price bounds
4. **Decimal Precision**: All calculations use Decimal, never float

### Operational Safety
1. **Fail Fast**: No fallback prices - fail if market data unavailable
2. **Audit Trail**: Log all price calculations with full context
3. **Monitoring**: Track actual vs expected slippage
4. **Circuit Breakers**: Disable during extreme volatility

## Implementation Priority

### Immediate Actions
1. **DO NOT** remove the market order validation without implementing pricing
2. **DO NOT** use hardcoded or fallback prices
3. **DO NOT** copy the float-based SDK implementation

### Phase 1: Foundation (1-2 days)
1. Implement all_mids endpoint support (optional)
2. Create MarketOrderPricingService class
3. Add configuration system
4. Unit tests for pricing logic

### Phase 2: Integration (2-3 days)
1. Update trading service to use pricing service
2. Integration tests with real market data
3. Error handling and edge cases
4. Documentation updates

### Phase 3: Production Ready (1-2 days)
1. Performance optimization
2. Monitoring and alerting setup
3. Load testing with concurrent orders
4. Final security review

## Architecture Compliance

This implementation follows CyberDeltaEngine's architecture principles:

1. **Exchange-Agnostic Interface**: Market orders work through standard `PlaceOrderArgs`
2. **Service Layer Separation**: Pricing logic in dedicated service
3. **Decimal Precision**: All financial calculations use Decimal
4. **Error Handling**: Comprehensive APIError usage
5. **Configuration-Driven**: No hardcoded values
6. **Type Safety**: Full Pydantic validation

## References

- **Hyperliquid Python SDK**: Official implementation reference (with float warnings)
- **Hyperliquid API Documentation**: Order structure specifications
- **CyberDeltaEngine Architecture**: API_ARCHITECTURE.md, API_ARCHITECTURE_COMPREHENSIVE.md
- **Testing Security Rules**: TESTING_SECURITY_RULES.md
- **Internal Testing**: 422 error analysis and IoC limit order validation

## Notes

- Market orders are fundamentally IoC limit orders on Hyperliquid
- Safe implementation requires real-time market data (no fallbacks)
- Business logic must be configuration-driven, not hardcoded
- Financial safety is the top priority over convenience
- Current disabled state prevents accidental losses during development

## ⚠️ CRITICAL: Decimal Precision Requirements

**NEVER USE FLOAT IN TRADING ENGINE**:
- ❌ `float` causes precision loss and rounding errors
- ❌ Hyperliquid SDK uses floats (unacceptable for production)
- ✅ **ALL** financial calculations must use `Decimal`
- ✅ **ALL** prices, quantities, slippage must be `Decimal`
- ✅ **ALL** arithmetic operations must preserve exact precision

**Financial Precision Standards**:
```python
# ❌ WRONG - Float precision loss
price = 52345.67890123456789  # Lost precision!
slippage = price * 0.005      # Rounding errors!

# ✅ CORRECT - Exact decimal precision  
price = Decimal("52345.67890123456789")      # Exact precision preserved
slippage = price * Decimal("0.005")          # Exact arithmetic
aggressive_price = price * (Decimal("1") + slippage)  # No rounding errors
```

This is a **TRADING ENGINE** - financial precision is non-negotiable.
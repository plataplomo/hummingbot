# Hyperliquid Market Orders Implementation Documentation

## Overview

This document details the technical findings and implementation requirements for Hyperliquid market orders based on analysis of the official Hyperliquid SDK and API behavior.

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
def calculate_aggressive_price(
    self,
    symbol: str,
    side: OrderSide,
    slippage: Decimal,                    # ✅ DECIMAL - EXACT PRECISION
    current_price: Decimal | None = None  # ✅ DECIMAL - NO ROUNDING ERRORS
) -> Decimal:                             # ✅ DECIMAL RETURN TYPE
    """Calculate aggressive market order price with exact precision."""
    
    if current_price is None:
        # Get current mid price with Decimal precision
        current_price = await self.get_current_mid_price_decimal(symbol)
    
    # Exact decimal arithmetic - no precision loss
    if side == OrderSide.BUY:
        aggressive_price = current_price * (Decimal("1") + slippage)
    else:
        aggressive_price = current_price * (Decimal("1") - slippage)
    
    # Apply proper decimal rounding for exchange precision requirements
    return self.round_to_exchange_precision(aggressive_price, symbol)
```

## Error Analysis

### Original Problem
- **Error**: HTTP 422 "Failed to deserialize the JSON body into the target type"
- **Root Cause**: Using `{"market": {}}` order type structure
- **Solution**: Use `{"limit": {"tif": "Ioc"}}` structure

### Pricing Issues Encountered

#### Attempt 1: Price "0"
```json
"p": "0"
```
- **Problem**: Invalid aggressive pricing
- **Risk**: Unpredictable execution behavior

#### Attempt 2: Hardcoded Extreme Prices
```python
if is_buy:
    limit_px_wire = "1000000.0"  # DANGEROUS
else:
    limit_px_wire = "0.01"       # DANGEROUS
```
- **Problem**: Could cause massive financial losses
- **Risk**: No liquidity protection, unlimited slippage

#### Attempt 3: Hardcoded Slippage Rules
```python
if symbol in ["BTC", "ETH"]:
    base_slippage = Decimal("0.002")  # WRONG APPROACH
```
- **Problem**: Business logic hardcoded in service layer
- **Issue**: Not configurable, not data-driven

## Current Implementation Status

### What's Fixed
✅ **Order Structure**: Market orders now use `{"limit": {"tif": "Ioc"}}` format
✅ **API Compatibility**: No more 422 deserialization errors
✅ **Type Safety**: Proper Pydantic model validation

### What's NOT Implemented (Deliberately)
❌ **Market Order Execution**: Currently raises `ValueError` to prevent unsafe execution
❌ **Dynamic Pricing**: No real-time market data integration
❌ **Slippage Calculation**: No price impact analysis

### Current Behavior
```python
if args.order_type == OrderType.MARKET:
    raise ValueError(
        f"[{current_method}] Market orders are not currently supported. "
        "Market order implementation requires integration with real-time market data service "
        "to calculate safe aggressive pricing. Use limit orders instead."
    )
```

## Required Implementation Components

### 1. Market Data Service Integration
**Required**: Real-time access to:
- Current mid prices (`all_mids()` equivalent)
- Order book depth
- Recent trade data
- Asset metadata (perp vs spot, decimals)

### 2. Slippage Configuration System
```yaml
market_orders:
  default_slippage: 0.01  # 1%
  max_slippage: 0.05      # 5% safety limit
  slippage_by_asset:
    BTC: 0.005            # 0.5% for high liquidity
    ETH: 0.005
    default: 0.02         # 2% for others
```

### 3. Price Calculation Service (Decimal-Based)
```python
class MarketOrderPricingService:
    async def calculate_aggressive_price(
        self, 
        symbol: str, 
        side: OrderSide, 
        quantity: Decimal,           # ✅ DECIMAL - EXACT QUANTITY
        max_slippage: Decimal        # ✅ DECIMAL - PRECISE SLIPPAGE
    ) -> Decimal:                    # ✅ DECIMAL - EXACT PRICE
        """Calculate safe aggressive price with slippage limits using exact arithmetic."""
        
        # Get current mid price as Decimal (never float!)
        current_mid_price: Decimal = await self.get_mid_price_decimal(symbol)
        
        # Calculate dynamic slippage based on order size and liquidity
        calculated_slippage: Decimal = await self.calculate_dynamic_slippage(
            symbol, quantity, current_mid_price
        )
        
        # Enforce maximum slippage safety limit
        safe_slippage = min(calculated_slippage, max_slippage)
        
        # Apply slippage with exact decimal arithmetic
        if side == OrderSide.BUY:
            aggressive_price = current_mid_price * (Decimal("1") + safe_slippage)
        else:
            aggressive_price = current_mid_price * (Decimal("1") - safe_slippage)
            
        # Round to exchange-specific precision requirements
        return self.round_to_tick_size(aggressive_price, symbol)
```

### 4. Risk Management
- **Slippage Limits**: Hard caps on maximum slippage
- **Position Size Limits**: Prevent large market orders
- **Liquidity Checks**: Ensure sufficient order book depth
- **Price Bounds**: Sanity checks on calculated prices

## Architecture Requirements

### Service Layer Separation
```
MarketOrderService
├── PricingStrategy (configurable)
├── RiskManager (slippage limits)
├── MarketDataProvider (real-time prices)
└── OrderExecutor (IoC limit orders)
```

### Configuration-Driven
- No hardcoded symbols
- No hardcoded slippage values
- Configurable risk parameters
- Environment-specific settings

## Testing Requirements

### Unit Tests
- [ ] IoC limit order structure validation
- [ ] Price calculation with different slippage values
- [ ] Risk limit enforcement
- [ ] Configuration loading

### Integration Tests
- [ ] Real market data integration
- [ ] Order execution with various symbols
- [ ] Slippage behavior under different market conditions
- [ ] Error handling for edge cases

### Safety Tests
- [ ] Maximum slippage enforcement
- [ ] Extreme market condition handling
- [ ] Network failure scenarios
- [ ] Invalid market data handling

## Security Considerations

### Financial Risk Mitigation
1. **Hard Slippage Caps**: Never exceed configured maximum slippage
2. **Position Limits**: Prevent oversized market orders
3. **Sanity Checks**: Validate calculated prices against market bounds
4. **Circuit Breakers**: Disable market orders during high volatility

### Operational Safety
1. **Graceful Degradation**: Fall back to limit orders if pricing fails
2. **Monitoring**: Log all market order executions and slippage
3. **Alerting**: Notify on unusual slippage or pricing behavior
4. **Audit Trail**: Complete record of pricing decisions

## Implementation Priority

### Phase 1: Foundation
1. Market data service integration
2. Basic slippage calculation
3. Configuration system
4. Risk limits

### Phase 2: Production Ready
1. Advanced pricing strategies
2. Comprehensive testing
3. Monitoring and alerting
4. Documentation and training

### Phase 3: Optimization
1. Dynamic slippage based on market conditions
2. Order book depth analysis
3. Performance optimization
4. Advanced risk management

## References

- **Hyperliquid Python SDK**: Official implementation reference
- **Hyperliquid API Documentation**: Order structure specifications
- **Internal Testing**: 422 error analysis and IoC limit order validation
- **Risk Management**: CyberDeltaEngine security requirements

## Notes

- Market orders are fundamentally IoC limit orders on Hyperliquid
- Safe implementation requires real-time market data
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
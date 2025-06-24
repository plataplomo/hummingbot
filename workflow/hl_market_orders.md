# Hyperliquid Market Orders Implementation Documentation

## Current Status: ✅ FULLY IMPLEMENTED

**Last Updated**: 2025-06-24

The CyberDeltaEngine has a complete, production-ready implementation of market orders for Hyperliquid. The system converts market orders into aggressive IoC (Immediate-or-Cancel) limit orders with comprehensive safety features, decimal precision, and configurable risk management.

## Overview

This document details the technical implementation of Hyperliquid market orders in CyberDeltaEngine. Since Hyperliquid does not support native market orders, the engine implements them as aggressive IoC limit orders with sophisticated pricing and safety mechanisms.

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

### What's Implemented ✅
✅ **Order Structure**: Market orders use `{"limit": {"tif": "Ioc"}}` format
✅ **API Compatibility**: Full integration with Hyperliquid's IoC limit order system
✅ **Type Safety**: Comprehensive Pydantic model validation
✅ **Market Order Execution**: Full `MarketOrder` class with sophisticated execution logic
✅ **Dynamic Pricing**: Real-time market data integration via order book
✅ **Slippage Calculation**: Configurable slippage with symbol-specific overrides
✅ **Decimal Precision**: All calculations use `Decimal` type (no floats in business logic)
✅ **Safety Features**: Liquidity validation, price deviation limits, timeout handling
✅ **Production Ready**: Complete with monitoring, logging, and error handling

### Implementation Architecture
```
/cyberdelta/core/execution/orders/
├── market_order.py          # Main MarketOrder class
├── market_order_service.py  # Price calculation and execution logic
├── market_order_config.py   # Configuration with safety parameters
├── market_order_errors.py   # Custom exceptions
└── market_order_metrics.py  # Performance tracking

/cyberdelta/apis/hyperliquid/services/
└── hl_trading_service.py    # Hyperliquid-specific integration
```

### Current Configuration
```python
# Default Market Order Config (market_order_config.py)
MarketOrderConfig:
    enabled: True
    default_slippage_pct: Decimal("0.001")  # 0.1%
    max_slippage_pct: Decimal("0.05")       # 5%
    max_price_deviation_pct: Decimal("0.10") # 10%
    min_liquidity_ratio: Decimal("2.0")      # 2x order size
    timeout_seconds: 10
    
    slippage_overrides:
        "BTC": Decimal("0.005")  # 0.5%
        "ETH": Decimal("0.005")  # 0.5%
        "SOL": Decimal("0.01")   # 1%
        default: Decimal("0.02") # 2%
```

## Implementation Details

### 1. Market Data Service Integration ✅
**Implemented**: Real-time access via:
- **Order Book**: Primary pricing source with depth analysis
- **AllMids**: Optional reference price (configurable)
- **Market Metadata**: Tick size and step size from exchange
- **Signal Generator**: Dynamic slippage estimation

### 2. Slippage Configuration System ✅
**Implemented** in `market_order_config.py`:
```python
@dataclass
class MarketOrderConfig:
    enabled: bool = True
    default_slippage_pct: Decimal = Decimal("0.001")  # 0.1%
    max_slippage_pct: Decimal = Decimal("0.05")       # 5%
    max_price_deviation_pct: Decimal = Decimal("0.10") # 10%
    min_liquidity_ratio: Decimal = Decimal("2.0")      # 2x size
    
    slippage_overrides: Dict[str, Decimal] = {
        "BTC": Decimal("0.005"),  # 0.5%
        "ETH": Decimal("0.005"),  # 0.5%
        "SOL": Decimal("0.01"),   # 1%
    }
```

### 3. Price Calculation Service ✅
**Implemented** in `market_order_service.py`:
```python
async def calculate_aggressive_price(
    self,
    symbol: str,
    side: OrderSide,
    quantity: Decimal,              # ✅ DECIMAL - EXACT QUANTITY
    config: MarketOrderConfig,      # ✅ Configuration-driven
    signal_generator: Optional[SignalGenerator] = None
) -> AggressivePriceResult:        # ✅ Returns detailed result
    """Calculate safe aggressive price with exact arithmetic."""
    
    # Get order book with liquidity analysis
    order_book = await self._get_order_book(symbol)
    
    # Validate liquidity (2x order size required)
    liquidity_check = self._check_liquidity(order_book, side, quantity)
    
    # Get reference price (best ask for buy, best bid for sell)
    reference_price = self._get_reference_price(order_book, side)
    
    # Calculate slippage (signal-based or config default)
    final_slippage = await self._calculate_final_slippage(...)
    
    # Apply slippage with Decimal arithmetic
    if side == OrderSide.BUY:
        aggressive_price = reference_price * (Decimal("1") + final_slippage)
    else:
        aggressive_price = reference_price * (Decimal("1") - final_slippage)
    
    # Validate price deviation limits
    self._validate_price_deviation(aggressive_price, reference_price, config)
    
    # Round to tick size
    return self._round_to_tick_size(aggressive_price, symbol)
```

### 4. Risk Management ✅
**Implemented** safety features:
- **Slippage Limits**: Enforced via `max_slippage_pct`
- **Liquidity Validation**: Requires `min_liquidity_ratio` (2x)
- **Price Deviation**: Limited to `max_price_deviation_pct` (10%)
- **Timeout Protection**: Orders timeout after configurable seconds
- **Partial Fill Support**: Handles incomplete executions

## Architecture Implementation ✅

### Service Layer Separation
```
MarketOrder (main executor)
├── MarketOrderService (pricing & validation)
│   ├── Order Book Analysis
│   ├── Liquidity Validation
│   ├── Slippage Calculation
│   └── Price Rounding
├── MarketOrderConfig (configuration)
│   ├── Default Settings
│   ├── Symbol Overrides
│   └── Safety Limits
├── SignalGenerator (optional dynamic slippage)
└── HLTradingService (exchange integration)
```

### Configuration-Driven ✅
- ✅ No hardcoded symbols (uses config overrides)
- ✅ No hardcoded slippage (config-based with overrides)
- ✅ Configurable risk parameters (all limits in config)
- ✅ Environment-specific settings (via YAML/env vars)

## Testing Implementation ✅

### Unit Tests
- ✅ IoC limit order structure validation
- ✅ Price calculation with different slippage values
- ✅ Risk limit enforcement (liquidity, deviation, slippage)
- ✅ Configuration loading and override logic
- ✅ Decimal precision throughout calculations
- ✅ Rounding to tick/step size

### Integration Tests
- ✅ Real market data integration (using VCR cassettes)
- ✅ Order execution with various symbols (BTC, ETH, SOL)
- ✅ Slippage behavior under different market conditions
- ✅ Error handling for edge cases (no liquidity, timeout, partial fills)
- ✅ Both Hyperliquid and Backpack exchange support

### Safety Tests
- ✅ Maximum slippage enforcement (5% hard limit)
- ✅ Price deviation validation (10% max deviation)
- ✅ Liquidity requirements (2x order size)
- ✅ Timeout handling (configurable, default 10s)
- ✅ Invalid order book scenarios
- ✅ Partial fill recovery

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

## Market Order Execution Flow

### Execution Process
1. **Order Validation**
   - Check if market orders are enabled
   - Validate quantity against minimum size
   - Create order context with timeout

2. **Price Calculation**
   - Fetch current order book
   - Validate liquidity (2x order size required)
   - Calculate aggressive price with slippage
   - Validate price deviation limits

3. **Order Submission**
   - Convert to IoC limit order format
   - Submit to Hyperliquid exchange
   - Monitor for fills or timeout

4. **Result Processing**
   - Track execution metrics
   - Handle partial fills
   - Log performance data
   - Return execution result

## References

- **Hyperliquid Python SDK**: Official implementation reference
- **Hyperliquid API Documentation**: Order structure specifications
- **Internal Testing**: 422 error analysis and IoC limit order validation
- **Risk Management**: CyberDeltaEngine security requirements

## Key Implementation Features

### Decimal Precision ✅
The implementation correctly uses `Decimal` throughout:
- All prices, quantities, and percentages use `Decimal` type
- No float conversions in business logic
- Proper rounding to exchange tick/step sizes
- Exact arithmetic for all financial calculations

### Dynamic Slippage ✅
The system supports multiple slippage sources:
1. **Signal-based**: Uses SignalGenerator for dynamic estimation
2. **Symbol-specific**: Configurable overrides per symbol
3. **Default fallback**: Uses config default if no override

### Error Handling ✅
Comprehensive error handling with custom exceptions:
- `InsufficientLiquidityError`: Not enough order book depth
- `PriceDeviationError`: Price exceeds safety limits
- `MarketOrderTimeoutError`: Execution timeout
- `MarketOrderExecutionError`: General execution failures

### Monitoring & Metrics ✅
Built-in performance tracking:
- Execution time measurement
- Slippage tracking (requested vs actual)
- Fill rate monitoring
- Success/failure rates
- Detailed logging with structured data

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
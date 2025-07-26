# 03. Duplicate Protocol Interfaces - Deep Code Research Report

## Executive Summary
Six protocol interfaces are defined separately for each exchange with nearly identical purposes but different method signatures. These protocols define transformation interfaces for converting API-specific data to internal domain models. While the protocols serve the same conceptual purpose, their methods are exchange-specific due to different raw model types.

## File Locations

### 1. Backpack: `/cyberdelta/apis/backpack/protocols/mapper_protocols.py`
- **Lines**: 367
- **Protocols**: 11 mapper protocols
- **Imports**: Backpack-specific raw models

### 2. Hyperliquid: `/cyberdelta/apis/hyperliquid/protocols/mapper_protocols.py`
- **Lines**: 500+
- **Protocols**: 9 mapper protocols
- **Imports**: Hyperliquid-specific raw models

## Protocol Comparison

### 1. BalanceMapperProtocol
**Purpose**: Transform balance data to SpotBalance models

**Backpack Methods**:
- `transform_balance_data_to_spot_balance(asset, total_balance, available_balance)`
- `transform_raw_balance_to_internal(asset_symbol, raw: BackpackRawBalance)`
- `create_balance_from_collateral(symbol, collateral_data, exchange_name)`

**Hyperliquid Methods**:
- `transform_raw_clearinghouse_state_to_spot_balances(raw_state: HyperliquidRawClearinghouseState)`
- `transform_raw_balance_to_internal(asset_symbol, raw_user_state: HyperliquidRawClearinghouseState)`

**Analysis**: Different method signatures due to different API structures

### 2. PositionMapperProtocol
**Purpose**: Transform position data to DerivativePosition models

**Backpack Methods**:
- `transform_raw_position_to_internal(raw: BackpackRawPosition)`
- `transform_ws_position_update_to_internal_position(raw_position_update: BackpackRawPositionUpdate)`

**Hyperliquid Methods**:
- `transform_raw_clearinghouse_state_to_derivative_positions(clearinghouse_data: HyperliquidRawClearinghouseState)`
- `transform_ws_position_update_to_internal_position(raw_position_update: HyperliquidRawWsPositionUpdateEvent)`

**Analysis**: Similar patterns but different raw types

### 3. AccountSummaryMapperProtocol
**Purpose**: Transform account data to MarginAccountSummary models

**Backpack Methods**:
- `transform_raw_account_summary_to_internal(raw_settings, spot_balances_raw, derivative_positions_raw)`
- `transform_enhanced_account_data_to_margin_summary(raw_collateral, raw_settings, raw_positions)`
- `transform_account_settings_update_to_internal(args, exchange_name)`

**Hyperliquid Methods**:
- `transform_raw_summary_to_internal(raw_summary: HyperliquidRawClearinghouseState)`
- `transform_raw_clearinghouse_state_to_margin_summary(clearinghouse_data: HyperliquidRawClearinghouseState)`

**Analysis**: Backpack has more granular methods

### 4. OrderMapperProtocol
**Purpose**: Transform order data to Order models

**Backpack Methods**:
- `transform_order_data_to_internal(order_id, symbol, side, order_type, status, quantity, price, ...)`
- `transform_raw_order_to_internal(raw_order: BackpackRawOrder)`
- `transform_ws_order_update_to_internal_order(raw_order_update: BackpackRawOrderUpdate)`

**Hyperliquid Methods**:
- `transform_raw_order_to_internal(raw_order: HyperliquidRawOrder)`
- `transform_raw_fill_to_internal(raw_fill: HyperliquidRawUserFill)`
- `transform_raw_historical_order_to_internal(raw_historical_order, trigger)`
- `transform_raw_simple_open_order_to_internal(raw_simple_order)`
- `transform_ws_order_update_to_internal_order(raw_order, trigger)`

**Analysis**: Hyperliquid has more order types

### 5. TickerMapperProtocol
**Purpose**: Transform ticker data to Ticker models

**Backpack Methods**:
- `transform_raw_ticker_to_internal(raw_ticker: BackpackRawTicker, symbol_override)`
- `transform_ws_ticker_event_to_internal(raw_ticker: BackpackRawTickerEvent)`

**Hyperliquid Methods**:
- `transform_raw_ticker_to_internal(raw_ticker: HyperliquidRawAssetCtx)`

**Analysis**: Different raw types, Hyperliquid lacks WebSocket method

### 6. OrderBookMapperProtocol
**Purpose**: Transform order book data to OrderBook models

**Backpack Methods**:
- `transform_raw_order_book_to_internal(symbol, raw_book: BackpackRawOrderBook)`
- `transform_ws_depth_event_to_internal(symbol, raw_depth: BackpackRawDepthUpdateEvent)`

**Hyperliquid Methods**:
- `transform_raw_order_book_to_internal(raw_order_book: HyperliquidRawL2Book)`
- `transform_raw_public_trade_to_internal(raw_trade: HyperliquidRawPublicTrade)`
- `transform_ws_book_update_to_internal(raw: HyperliquidRawWsBookUpdate)`
- `transform_ws_trade_event_to_internal(raw: HyperliquidRawWsTradeEvent)`

**Analysis**: Hyperliquid mixes trade methods in OrderBook protocol

## Additional Protocols

### Backpack-Only Protocols
1. **TransactionMapperProtocol** - Fill/trade transformations
2. **TransferMapperProtocol** - Transfer/withdrawal transformations
3. **CandleMapperProtocol** - Candle/kline transformations
4. **FundingRateMapperProtocol** - Funding rate transformations
5. **MarketMapperProtocol** - Market configuration transformations
6. **MarketDataMapperProtocol** - Base for market data mappers

### Hyperliquid-Only Protocols
1. **TradeMapperProtocol** - Trade-specific transformations
2. **CandleMapperProtocol** - Candle transformations
3. **FundingRateMapperProtocol** - Funding rate transformations
4. **MarketMapperProtocol** - Market transformations
5. **MidPricesMapperProtocol** - Mid price transformations

## Architecture Analysis

### Current Issues
1. **Conceptual Duplication**: Same transformation concepts defined separately
2. **Inconsistent Organization**: Trade methods mixed into OrderBook protocol (Hyperliquid)
3. **Missing Abstractions**: No shared interface despite similar purposes
4. **Maintenance Overhead**: Changes to transformation patterns must be made twice

### Key Differences
1. **Raw Model Types**: Each exchange has unique raw model structures
2. **Method Granularity**: Backpack tends to have more granular methods
3. **Protocol Hierarchy**: Backpack uses MarketDataMapperProtocol base class
4. **WebSocket Support**: Not all protocols have WebSocket methods in both

## Recommendations

### Option 1: Generic Protocol Interfaces
Create generic protocols using type parameters:
```python
# /cyberdelta/apis/base/protocols/mapper_protocols.py
from typing import Protocol, TypeVar, Generic

TRawBalance = TypeVar('TRawBalance')
TRawPosition = TypeVar('TRawPosition')

class BalanceMapperProtocol(Generic[TRawBalance], Protocol):
    def transform_raw_balance_to_internal(
        self, raw: TRawBalance
    ) -> SpotBalance:
        ...
```

### Option 2: Abstract Base Protocols
Define abstract interfaces that exchanges extend:
```python
# /cyberdelta/apis/base/protocols/mapper_protocols.py
class BaseBalanceMapperProtocol(Protocol):
    """Abstract balance mapper interface"""

    def transform_to_spot_balance(
        self, raw_data: Any
    ) -> SpotBalance:
        """Transform any raw balance data to SpotBalance"""
        ...

# Exchange-specific
class BackpackBalanceMapperProtocol(BaseBalanceMapperProtocol):
    def transform_raw_balance_to_internal(
        self, raw: BackpackRawBalance
    ) -> SpotBalance:
        ...
```

### Option 3: Keep Separate but Document Pattern
1. Create documentation explaining the mapper pattern
2. Establish naming conventions for consistency
3. Create a protocol registry to track all mappers
4. Use automated testing to ensure pattern compliance

### Recommended Approach: Option 2 + Documentation
1. **Create Base Abstract Protocols**:
   - Define common transformation patterns
   - Use `Any` for raw types in base protocols
   - Document expected behavior

2. **Exchange-Specific Extensions**:
   - Inherit from base protocols
   - Add exchange-specific methods
   - Use proper type hints for raw models

3. **Benefits**:
   - Clear conceptual hierarchy
   - Shared documentation
   - Type safety maintained
   - Flexibility for exchange differences

## Implementation Impact

### Files to Update
- Create: `/cyberdelta/apis/base/protocols/mapper_protocols.py`
- Update: Both exchange mapper protocol files to inherit from base
- No changes needed to actual mapper implementations

### Migration Risk
- **Low Risk**: Protocols are interfaces only
- **Backward Compatible**: Existing code continues to work
- **Gradual Migration**: Can update one protocol at a time

## Code Metrics

### Current State
- **Duplicate Protocol Definitions**: 6 core protocols
- **Total Methods**: ~40 methods across both files
- **Conceptually Similar Methods**: ~20 pairs

### After Consolidation
- **Base Protocol Definitions**: 6 abstract protocols
- **Exchange Extensions**: 12 specific protocols (6 each)
- **Shared Documentation**: Single source of truth

## Conclusion
While the protocol interfaces appear duplicated, they serve exchange-specific purposes with different method signatures due to unique raw model types. Rather than forcing complete unification, creating abstract base protocols would provide conceptual unity while maintaining type safety and flexibility for exchange-specific implementations. This approach reduces conceptual duplication without sacrificing the benefits of strong typing.

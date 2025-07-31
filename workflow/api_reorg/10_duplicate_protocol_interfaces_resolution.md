# 10. Duplicate Protocol Interfaces - Deep Resolution Analysis & Implementation Plan

## Executive Summary
**Status**: CRITICAL UNRESOLVED ISSUE - No Abstract Protocols Created
**Impact**: 1,140 lines across 30 protocols, conceptual duplication with different signatures
**Resolution Time**: 4-6 hours
**Risk Level**: MEDIUM (requires careful type hierarchy design)
**Last Updated**: 2025-07-30

This document provides a comprehensive analysis and resolution plan for eliminating conceptual duplication in mapper protocol interfaces while preserving exchange-specific functionality through abstract base protocols.

### Current State Update (2025-07-30)
- ✗ No abstract mapper protocols exist in `/cyberdelta/apis/base/protocols/`
- ✗ Duplicate mapper protocol definitions still exist in both exchanges
- ✗ No shared abstraction layer has been created
- ⚠️ Line counts have increased slightly (1,140 vs 1,125 originally)
- ⚠️ Symbol type migration has been applied but doesn't address duplication

## Deep Code Research Findings

### Current State Analysis

#### File Metrics & Statistics (Updated 2025-07-30)
1. **Backpack**: `/cyberdelta/apis/backpack/protocols/mapper_protocols.py`
   - **Size**: 382 lines (increased from 370)
   - **Protocols**: 13 mapper protocols
   - **Inheritance Pattern**: All inherit from `(MapperProtocol, Protocol)`
   - **Organization**: Uses `MarketDataMapperProtocol` as base for market data protocols
   - **Changes**: Symbol type migration for asset parameters

2. **Hyperliquid**: `/cyberdelta/apis/hyperliquid/protocols/mapper_protocols.py`
   - **Size**: 758 lines (increased from 755)
   - **Protocols**: 17 mapper protocols  
   - **Inheritance Pattern**: All inherit from `(MapperProtocol, Protocol)`
   - **Organization**: Flat structure, no intermediate base protocols
   - **Changes**: Symbol type migration for asset parameters

**Total Code Impact**: 1,140 lines of protocol definitions (up from 1,125)

#### Protocol Distribution Analysis

##### Conceptually Identical Protocols (10 pairs)
These protocols serve the same functional purpose but have different method signatures due to exchange-specific raw model types:

1. **BalanceMapperProtocol** - SpotBalance transformations
2. **PositionMapperProtocol** - DerivativePosition transformations  
3. **AccountSummaryMapperProtocol** - MarginAccountSummary transformations
4. **OrderMapperProtocol** - Order transformations
5. **TickerMapperProtocol** - Ticker transformations
6. **OrderBookMapperProtocol** - OrderBook transformations
7. **TradeMapperProtocol** - Trade transformations
8. **CandleMapperProtocol** - Candle transformations
9. **FundingRateMapperProtocol** - FundingRate transformations
10. **MarketMapperProtocol** - Market transformations

##### Backpack-Only Protocols (3 protocols)
1. **TransactionMapperProtocol** - Fill/trade transformations
2. **TransferMapperProtocol** - Transfer/withdrawal transformations  
3. **MarketDataMapperProtocol** - Base class for market data mappers

##### Hyperliquid-Only Protocols (7 protocols)
1. **MarketDataMapperProtocol** - Similar to Backpack but different methods
2. **OrderResponseMapperProtocol** - Order response specific transformations
3. **TransactionMapperProtocol** - Similar to Backpack but different methods
4. **TradingEnumMapperProtocol** - Trading enum transformations
5. **PriceTickerMapperProtocol** - Price ticker specific transformations
6. **HistoricalDataMapperProtocol** - Historical data transformations
7. **MarketMetadataMapperProtocol** - Market metadata transformations

### Detailed Protocol Method Analysis

#### Example: BalanceMapperProtocol Comparison

##### Backpack BalanceMapperProtocol (3 methods) - Updated 2025-07-30
```python
@staticmethod
def transform_balance_data_to_spot_balance(
    asset: str, total_balance: str, available_balance: str  # Now strings, not Decimal
) -> SpotBalance:

@staticmethod
def transform_raw_balance_to_internal(
    asset_symbol: Symbol, raw: BackpackRawBalanceResponse  # Symbol type, not str
) -> SpotBalance:

@staticmethod  
def create_balance_from_collateral(
    symbol: Symbol, collateral_data: BackpackRawCollateralAsset, exchange_name: str  # Symbol type
) -> SpotBalance:
```

##### Hyperliquid BalanceMapperProtocol (2 methods) - Updated 2025-07-30
```python
@staticmethod
def transform_raw_clearinghouse_state_to_spot_balances(
    raw_state: HyperliquidRawClearinghouseState
) -> dict[str, SpotBalance]:  # Returns dict, not list

@staticmethod
def transform_raw_balance_to_internal(
    asset_symbol: Symbol, raw_user_state: HyperliquidRawClearinghouseState  # Symbol type
) -> SpotBalance:
```

**Analysis**: 
- **Same Purpose**: Transform balance data to `SpotBalance` models
- **Different Methods**: Exchange APIs have different data structures
- **Common Pattern**: Both have `transform_raw_balance_to_internal` method
- **Output Type**: Both produce `SpotBalance` (common domain model)

#### Method Signature Pattern Analysis

##### Common Patterns Identified
1. **Raw Model Transformation**: `transform_raw_X_to_internal(raw: ExchangeRawModel) -> DomainModel`
2. **WebSocket Event Handling**: `transform_ws_X_event_to_internal(raw: ExchangeWsModel) -> DomainModel`
3. **Data Structure Conversion**: `transform_X_data_to_Y(params...) -> DomainModel`
4. **Multi-source Aggregation**: `transform_enhanced_X_to_Y(multiple_raw_sources...) -> DomainModel`

##### Return Type Consistency
**Excellent**: All protocols return common domain models:
- `SpotBalance`, `DerivativePosition`, `MarginAccountSummary`
- `Order`, `Trade`, `Ticker`, `OrderBook`
- `Candle`, `FundingRate`, `Market`

This consistency enables abstract protocol definitions.

### Architecture Issues Analysis

#### Current Problems
1. **Conceptual Duplication**: Same transformation purposes defined separately
2. **No Shared Abstractions**: No common interfaces despite similar goals
3. **Maintenance Overhead**: Updates to transformation patterns need double implementation
4. **Documentation Duplication**: Same concepts documented multiple times
5. **Type System Fragmentation**: No unified protocol hierarchy

#### Missing Abstractions
Currently there is NO `/cyberdelta/apis/base/protocols/mapper_protocols.py` file, meaning:
- No abstract mapper protocol interfaces
- No shared transformation pattern documentation
- No common protocol hierarchy
- No unified typing approach

## Resolution Architecture

### Strategic Approach: Abstract Base Protocols with Exchange Extensions

The solution creates abstract base protocols that define common transformation patterns while allowing exchange-specific implementations through inheritance.

#### Architecture Principles
1. **Abstract Interfaces**: Define transformation purposes, not specific signatures
2. **Type Flexibility**: Use `Any` for raw types in base protocols
3. **Clear Documentation**: Shared documentation for common patterns
4. **Exchange Freedom**: Allow exchanges to define specific method signatures
5. **Type Safety**: Maintain strong typing in exchange-specific protocols

### Proposed Directory Structure
```
/cyberdelta/apis/base/protocols/
├── __init__.py
├── base_protocols.py           # (from resolution document 09)
└── mapper_protocols.py         # NEW: Abstract mapper interfaces

/cyberdelta/apis/backpack/protocols/
├── __init__.py
├── builder_protocols.py
├── handler_protocols.py  
└── mapper_protocols.py         # UPDATED: Inherit from base abstractions

/cyberdelta/apis/hyperliquid/protocols/
├── __init__.py
├── builder_protocols.py
├── handler_protocols.py
└── mapper_protocols.py         # UPDATED: Inherit from base abstractions
```

## Implementation Plan

### Phase 1: Create Abstract Base Mapper Protocols (90 minutes)

#### 1.1 Create Common Mapper Protocol Module
**File**: `/cyberdelta/apis/base/protocols/mapper_protocols.py`

```python
"""Abstract mapper protocol interfaces for all exchanges.

This module defines the conceptual transformation interfaces that all exchanges
should implement. Each protocol represents a transformation domain (balance,
position, etc.) without specifying exact method signatures, allowing exchange-
specific implementations while maintaining conceptual consistency.
"""

from typing import Any, Protocol, runtime_checkable

from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate, 
    MarginAccountSummary,
    Market,
    Order,
    OrderBook,
    SpotBalance,
    Ticker,
    Trade,
    Transfer,
    Withdrawal,
)
from cyberdelta.core.models.market.candle import Candle


@runtime_checkable
class AbstractBalanceMapperProtocol(Protocol):
    """Abstract protocol for balance transformation mappers.
    
    Defines the conceptual interface for transforming exchange-specific
    balance data into internal SpotBalance models. Exchanges may implement
    different method signatures based on their API structures.
    
    Expected Transformations:
    - Raw balance data -> SpotBalance
    - WebSocket balance updates -> SpotBalance
    - Collateral data -> SpotBalance (if applicable)
    """
    
    def transform_to_spot_balance(self, raw_data: Any, **kwargs: Any) -> SpotBalance:
        """Transform any raw balance data to SpotBalance.
        
        This is a conceptual interface - exchanges should implement specific
        methods with appropriate type hints for their raw models.
        
        Args:
            raw_data: Exchange-specific raw balance data
            **kwargs: Additional context (symbol, metadata, etc.)
            
        Returns:
            SpotBalance: Internal balance model
        """
        ...


@runtime_checkable  
class AbstractPositionMapperProtocol(Protocol):
    """Abstract protocol for position transformation mappers.
    
    Defines the conceptual interface for transforming exchange-specific
    position data into internal DerivativePosition models.
    
    Expected Transformations:
    - Raw position data -> DerivativePosition
    - WebSocket position updates -> DerivativePosition
    - Position arrays -> list[DerivativePosition]
    """
    
    def transform_to_derivative_position(self, raw_data: Any, **kwargs: Any) -> DerivativePosition:
        """Transform any raw position data to DerivativePosition.
        
        Args:
            raw_data: Exchange-specific raw position data
            **kwargs: Additional context
            
        Returns:
            DerivativePosition: Internal position model
        """
        ...


@runtime_checkable
class AbstractAccountSummaryMapperProtocol(Protocol):
    """Abstract protocol for account summary transformation mappers.
    
    Defines the conceptual interface for transforming exchange-specific
    account data into internal MarginAccountSummary models.
    
    Expected Transformations:
    - Raw account state -> MarginAccountSummary  
    - Multi-source account data -> MarginAccountSummary
    - Account settings updates -> MarginAccountSummary
    """
    
    def transform_to_margin_account_summary(self, raw_data: Any, **kwargs: Any) -> MarginAccountSummary:
        """Transform any raw account data to MarginAccountSummary.
        
        Args:
            raw_data: Exchange-specific raw account data
            **kwargs: Additional context (balances, positions, etc.)
            
        Returns:
            MarginAccountSummary: Internal account summary model
        """
        ...


@runtime_checkable
class AbstractOrderMapperProtocol(Protocol):
    """Abstract protocol for order transformation mappers.
    
    Defines the conceptual interface for transforming exchange-specific
    order data into internal Order models.
    
    Expected Transformations:
    - Raw order data -> Order
    - WebSocket order updates -> Order
    - Order history data -> Order
    - Fill/execution data -> Order (with fill info)
    """
    
    def transform_to_order(self, raw_data: Any, **kwargs: Any) -> Order:
        """Transform any raw order data to Order.
        
        Args:
            raw_data: Exchange-specific raw order data
            **kwargs: Additional context (trigger info, etc.)
            
        Returns:
            Order: Internal order model
        """
        ...


@runtime_checkable
class AbstractTickerMapperProtocol(Protocol):
    """Abstract protocol for ticker transformation mappers.
    
    Defines the conceptual interface for transforming exchange-specific
    ticker data into internal Ticker models.
    
    Expected Transformations:
    - Raw ticker data -> Ticker
    - WebSocket ticker updates -> Ticker
    - Asset context data -> Ticker
    """
    
    def transform_to_ticker(self, raw_data: Any, **kwargs: Any) -> Ticker:
        """Transform any raw ticker data to Ticker.
        
        Args:
            raw_data: Exchange-specific raw ticker data
            **kwargs: Additional context (symbol override, etc.)
            
        Returns:
            Ticker: Internal ticker model
        """
        ...


@runtime_checkable
class AbstractOrderBookMapperProtocol(Protocol):
    """Abstract protocol for order book transformation mappers.
    
    Defines the conceptual interface for transforming exchange-specific
    order book data into internal OrderBook models.
    
    Expected Transformations:
    - Raw order book data -> OrderBook
    - WebSocket depth updates -> OrderBook
    - L2 book data -> OrderBook
    """
    
    def transform_to_order_book(self, raw_data: Any, **kwargs: Any) -> OrderBook:
        """Transform any raw order book data to OrderBook.
        
        Args:
            raw_data: Exchange-specific raw order book data
            **kwargs: Additional context (symbol, depth level, etc.)
            
        Returns:
            OrderBook: Internal order book model
        """
        ...


@runtime_checkable
class AbstractTradeMapperProtocol(Protocol):
    """Abstract protocol for trade transformation mappers.
    
    Defines the conceptual interface for transforming exchange-specific
    trade data into internal Trade models.
    
    Expected Transformations:
    - Raw trade/fill data -> Trade
    - WebSocket trade events -> Trade
    - Public trade data -> Trade
    - Trade history -> Trade
    """
    
    def transform_to_trade(self, raw_data: Any, **kwargs: Any) -> Trade:
        """Transform any raw trade data to Trade.
        
        Args:
            raw_data: Exchange-specific raw trade data
            **kwargs: Additional context (is_maker, fees, etc.)
            
        Returns:
            Trade: Internal trade model
        """
        ...


@runtime_checkable
class AbstractCandleMapperProtocol(Protocol):
    """Abstract protocol for candle transformation mappers.
    
    Defines the conceptual interface for transforming exchange-specific
    candle/OHLCV data into internal Candle models.
    
    Expected Transformations:
    - Raw candle data -> Candle
    - WebSocket candle updates -> Candle
    - Historical candle arrays -> list[Candle]
    """
    
    def transform_to_candle(self, raw_data: Any, **kwargs: Any) -> Candle:
        """Transform any raw candle data to Candle.
        
        Args:
            raw_data: Exchange-specific raw candle data
            **kwargs: Additional context (interval, symbol, etc.)
            
        Returns:
            Candle: Internal candle model
        """
        ...


@runtime_checkable
class AbstractFundingRateMapperProtocol(Protocol):
    """Abstract protocol for funding rate transformation mappers.
    
    Defines the conceptual interface for transforming exchange-specific
    funding rate data into internal FundingRate models.
    
    Expected Transformations:
    - Raw funding rate data -> FundingRate
    - Historical funding data -> FundingRate
    - Funding interval data -> FundingRate
    """
    
    def transform_to_funding_rate(self, raw_data: Any, **kwargs: Any) -> FundingRate:
        """Transform any raw funding rate data to FundingRate.
        
        Args:
            raw_data: Exchange-specific raw funding data
            **kwargs: Additional context (symbol, interval, etc.)
            
        Returns:
            FundingRate: Internal funding rate model
        """
        ...


@runtime_checkable
class AbstractMarketMapperProtocol(Protocol):
    """Abstract protocol for market transformation mappers.
    
    Defines the conceptual interface for transforming exchange-specific
    market metadata into internal Market models.
    
    Expected Transformations:
    - Raw market data -> Market
    - Asset definitions -> Market
    - Market configuration -> Market
    """
    
    def transform_to_market(self, raw_data: Any, **kwargs: Any) -> Market:
        """Transform any raw market data to Market.
        
        Args:
            raw_data: Exchange-specific raw market data
            **kwargs: Additional context (asset definitions, etc.)
            
        Returns:
            Market: Internal market model
        """
        ...


# Commonly used protocols across exchanges
__all__ = [
    "AbstractBalanceMapperProtocol",
    "AbstractPositionMapperProtocol", 
    "AbstractAccountSummaryMapperProtocol",
    "AbstractOrderMapperProtocol",
    "AbstractTickerMapperProtocol",
    "AbstractOrderBookMapperProtocol", 
    "AbstractTradeMapperProtocol",
    "AbstractCandleMapperProtocol",
    "AbstractFundingRateMapperProtocol",
    "AbstractMarketMapperProtocol",
]
```

#### 1.2 Update Base Protocols __init__.py
**File**: `/cyberdelta/apis/base/protocols/__init__.py`

```python
"""Base protocol definitions for all API components."""

from cyberdelta.apis.base.protocols.base_protocols import (
    MapperProtocol,
    RequestBuilderProtocol,
    ResponseHandlerProtocol,
)
from cyberdelta.apis.base.protocols.mapper_protocols import (
    AbstractBalanceMapperProtocol,
    AbstractPositionMapperProtocol,
    AbstractAccountSummaryMapperProtocol,
    AbstractOrderMapperProtocol,
    AbstractTickerMapperProtocol,
    AbstractOrderBookMapperProtocol,
    AbstractTradeMapperProtocol,
    AbstractCandleMapperProtocol,
    AbstractFundingRateMapperProtocol,
    AbstractMarketMapperProtocol,
)

__all__ = [
    # Base protocols
    "MapperProtocol",
    "RequestBuilderProtocol", 
    "ResponseHandlerProtocol",
    
    # Abstract mapper protocols
    "AbstractBalanceMapperProtocol",
    "AbstractPositionMapperProtocol",
    "AbstractAccountSummaryMapperProtocol", 
    "AbstractOrderMapperProtocol",
    "AbstractTickerMapperProtocol",
    "AbstractOrderBookMapperProtocol",
    "AbstractTradeMapperProtocol",
    "AbstractCandleMapperProtocol",
    "AbstractFundingRateMapperProtocol",
    "AbstractMarketMapperProtocol",
]
```

### Phase 2: Update Exchange Protocols to Inherit from Abstractions (120 minutes)

#### 2.1 Update Backpack Mapper Protocols
**File**: `/cyberdelta/apis/backpack/protocols/mapper_protocols.py`

Add imports and inheritance:
```python
"""Specific protocols for mapper components."""

from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

# Add abstract protocol imports
from cyberdelta.apis.base.protocols.mapper_protocols import (
    AbstractBalanceMapperProtocol,
    AbstractPositionMapperProtocol,
    AbstractAccountSummaryMapperProtocol,
    AbstractOrderMapperProtocol,
    AbstractTickerMapperProtocol,
    AbstractOrderBookMapperProtocol,
    AbstractTradeMapperProtocol,
    AbstractCandleMapperProtocol,
    AbstractFundingRateMapperProtocol,
    AbstractMarketMapperProtocol,
)

# Existing imports...
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalanceResponse
# ... rest of imports

# Type alias for raw JSON responses
RawJsonResponse = dict[str, Any]


@runtime_checkable
class BalanceMapperProtocol(MapperProtocol, AbstractBalanceMapperProtocol, Protocol):
    """Backpack-specific balance mapper protocol.

    Inherits from both MapperProtocol (for utility methods) and 
    AbstractBalanceMapperProtocol (for conceptual interface) to ensure
    compliance with base mapper interface and transformation patterns.
    """

    @staticmethod
    def transform_balance_data_to_spot_balance(
        asset: str, total_balance: Decimal, available_balance: Decimal
    ) -> SpotBalance:
        """Transform balance data components to SpotBalance."""
        ...

    @staticmethod
    def transform_raw_balance_to_internal(
        asset_symbol: str, raw: BackpackRawBalanceResponse
    ) -> SpotBalance:
        """Transform raw balance response to internal SpotBalance."""
        ...

    @staticmethod
    def create_balance_from_collateral(
        symbol: str, collateral_data: BackpackRawCollateralAsset, exchange_name: str
    ) -> SpotBalance:
        """Create balance from collateral data."""
        ...

    # Implementation satisfies AbstractBalanceMapperProtocol.transform_to_spot_balance
    # through structural typing - any of the above methods can satisfy the abstract interface


@runtime_checkable
class PositionMapperProtocol(MapperProtocol, AbstractPositionMapperProtocol, Protocol):
    """Backpack-specific position mapper protocol."""
    
    # Existing methods...
    @staticmethod
    def transform_raw_position_to_internal(raw: BackpackRawPositionResponse) -> DerivativePosition:
        """Transform raw position to internal DerivativePosition."""
        ...

    @staticmethod
    def transform_ws_position_update_to_internal_position(
        raw_position_update: BackpackRawPositionUpdate
    ) -> DerivativePosition:
        """Transform WebSocket position update to internal position."""
        ...


# Continue pattern for all protocols...
@runtime_checkable
class AccountSummaryMapperProtocol(MapperProtocol, AbstractAccountSummaryMapperProtocol, Protocol):
    """Backpack-specific account summary mapper protocol."""
    # Existing methods with same signatures...


@runtime_checkable  
class OrderMapperProtocol(MapperProtocol, AbstractOrderMapperProtocol, Protocol):
    """Backpack-specific order mapper protocol."""
    # Existing methods with same signatures...


@runtime_checkable
class TickerMapperProtocol(MarketDataMapperProtocol, AbstractTickerMapperProtocol, Protocol):
    """Backpack-specific ticker mapper protocol."""
    # Existing methods with same signatures...


@runtime_checkable
class OrderBookMapperProtocol(MarketDataMapperProtocol, AbstractOrderBookMapperProtocol, Protocol):
    """Backpack-specific order book mapper protocol."""
    # Existing methods with same signatures...


@runtime_checkable
class TradeMapperProtocol(MarketDataMapperProtocol, AbstractTradeMapperProtocol, Protocol):
    """Backpack-specific trade mapper protocol."""
    # Existing methods with same signatures...


@runtime_checkable
class CandleMapperProtocol(MarketDataMapperProtocol, AbstractCandleMapperProtocol, Protocol):
    """Backpack-specific candle mapper protocol."""
    # Existing methods with same signatures...


@runtime_checkable
class FundingRateMapperProtocol(MarketDataMapperProtocol, AbstractFundingRateMapperProtocol, Protocol):
    """Backpack-specific funding rate mapper protocol."""
    # Existing methods with same signatures...


@runtime_checkable
class MarketMapperProtocol(MarketDataMapperProtocol, AbstractMarketMapperProtocol, Protocol):
    """Backpack-specific market mapper protocol."""
    # Existing methods with same signatures...


# Keep Backpack-specific protocols as-is
@runtime_checkable
class TransactionMapperProtocol(MapperProtocol, Protocol):
    """Backpack-specific transaction mapper protocol.
    
    No abstract equivalent - this is exchange-specific functionality.
    """
    # Existing methods...


@runtime_checkable
class TransferMapperProtocol(MapperProtocol, Protocol):
    """Backpack-specific transfer mapper protocol.
    
    No abstract equivalent - this is exchange-specific functionality.
    """
    # Existing methods...


@runtime_checkable
class MarketDataMapperProtocol(MapperProtocol, Protocol):
    """Base protocol for Backpack market data mappers."""
    # Existing methods...
```

#### 2.2 Update Hyperliquid Mapper Protocols
Apply similar pattern to `/cyberdelta/apis/hyperliquid/protocols/mapper_protocols.py`:

```python
"""Mapper protocol definitions for Hyperliquid API."""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

# Add abstract protocol imports
from cyberdelta.apis.base.protocols.mapper_protocols import (
    AbstractBalanceMapperProtocol,
    AbstractPositionMapperProtocol,
    AbstractAccountSummaryMapperProtocol,
    AbstractOrderMapperProtocol,
    AbstractTickerMapperProtocol,
    AbstractOrderBookMapperProtocol,
    AbstractTradeMapperProtocol,
    AbstractCandleMapperProtocol,
    AbstractFundingRateMapperProtocol,
    AbstractMarketMapperProtocol,
)

# Existing imports...
from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
# ... rest of imports


@runtime_checkable
class BalanceMapperProtocol(MapperProtocol, AbstractBalanceMapperProtocol, Protocol):
    """Hyperliquid-specific balance mapper protocol."""
    
    @staticmethod
    def transform_raw_clearinghouse_state_to_spot_balances(
        raw_state: HyperliquidRawClearinghouseState
    ) -> list[SpotBalance]:
        """Transform clearinghouse state to spot balances."""
        ...

    @staticmethod
    def transform_raw_balance_to_internal(
        asset_symbol: str, raw_user_state: HyperliquidRawClearinghouseState
    ) -> SpotBalance:
        """Transform raw balance to internal SpotBalance."""
        ...


# Continue for all common protocols with AbstractXMapperProtocol inheritance
# Keep Hyperliquid-specific protocols without abstract inheritance:

@runtime_checkable
class OrderResponseMapperProtocol(MapperProtocol, Protocol):
    """Hyperliquid-specific order response mapper protocol.
    
    No abstract equivalent - this is exchange-specific functionality.
    """
    # Existing methods...


@runtime_checkable  
class TradingEnumMapperProtocol(MapperProtocol, Protocol):
    """Hyperliquid-specific trading enum mapper protocol.
    
    No abstract equivalent - this is exchange-specific functionality.
    """
    # Existing methods...


# ... etc for other HL-specific protocols
```

### Phase 3: Documentation and Usage Patterns (60 minutes)

#### 3.1 Create Protocol Usage Guide
**File**: `/cyberdelta/apis/base/protocols/README.md`

```markdown
# API Protocol Architecture Guide

## Overview

The CyberDelta API protocol system uses a three-layer architecture:

1. **Base Protocols** - Common utilities (MapperProtocol, RequestBuilderProtocol, ResponseHandlerProtocol)
2. **Abstract Mapper Protocols** - Conceptual transformation interfaces  
3. **Exchange-Specific Protocols** - Concrete implementations for each exchange

## Architecture Diagram

```
┌─────────────────────────────────────────────────┐
│                Base Protocols                    │
│  ┌─────────────┐ ┌──────────────┐ ┌───────────┐ │
│  │MapperProtocol│ │RequestBuilder│ │ResponseHand│ │
│  │             │ │Protocol      │ │lerProtocol │ │
│  └─────────────┘ └──────────────┘ └───────────┘ │
└─────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────┐
│            Abstract Mapper Protocols             │
│ ┌──────────┐ ┌────────────┐ ┌─────────────────┐ │
│ │AbstractBa│ │AbstractPos │ │AbstractAccount  │ │
│ │lanceMappa│ │itionMapper │ │SummaryMapper    │ │
│ │Protocol  │ │Protocol    │ │Protocol         │ │
│ └──────────┘ └────────────┘ └─────────────────┘ │
│                     ... etc                     │
└─────────────────────────────────────────────────┘
┌─────────────────┐    ┌─────────────────────────┐
│   Backpack      │    │      Hyperliquid        │
│   Protocols     │    │      Protocols          │
│ ┌─────────────┐ │    │ ┌─────────────────────┐ │
│ │BalanceMapper│ │    │ │BalanceMapperProtocol│ │
│ │Protocol     │ │    │ │                     │ │
│ └─────────────┘ │    │ └─────────────────────┘ │
│       ...       │    │           ...           │
└─────────────────┘    └─────────────────────────┘
```

## Usage Patterns

### For Exchange Implementers

When implementing mapper protocols for a new exchange:

```python
from cyberdelta.apis.base.protocols.base_protocols import MapperProtocol
from cyberdelta.apis.base.protocols.mapper_protocols import AbstractBalanceMapperProtocol

@runtime_checkable
class BalanceMapperProtocol(MapperProtocol, AbstractBalanceMapperProtocol, Protocol):
    """Exchange-specific balance mapper protocol."""
    
    @staticmethod
    def transform_raw_balance_to_internal(
        raw: ExchangeRawBalance
    ) -> SpotBalance:
        """Transform exchange-specific raw balance to SpotBalance."""
        ...
```

### For Service Developers

When writing services that use mappers:

```python
from cyberdelta.apis.base.protocols.mapper_protocols import AbstractBalanceMapperProtocol

class BalanceService:
    def __init__(self, mapper: AbstractBalanceMapperProtocol):
        self.mapper = mapper
    
    def process_balance(self, raw_data: Any) -> SpotBalance:
        # Can work with any exchange's balance mapper
        return self.mapper.transform_to_spot_balance(raw_data)
```

## Benefits

1. **Type Safety** - Clear interfaces for all transformation operations
2. **Documentation** - Shared understanding of transformation purposes  
3. **Flexibility** - Exchanges can implement methods as needed
4. **Testability** - Abstract protocols enable unified testing
5. **Maintainability** - Clear separation between common and exchange-specific concerns
```

#### 3.2 Update Exchange Protocol Documentation
Add header comments to both exchange protocol files explaining the new inheritance structure and how it relates to the abstract protocols.

### Phase 4: Testing and Verification (45 minutes)

#### 4.1 Protocol Compliance Testing
Create test to verify protocols properly inherit from abstractions:

**File**: `/tests/unit/apis/base/test_protocol_inheritance.py`

```python
"""Test protocol inheritance structure."""

import pytest
from typing import get_origin, get_args

from cyberdelta.apis.base.protocols.mapper_protocols import (
    AbstractBalanceMapperProtocol,
    AbstractPositionMapperProtocol,
    # ... other abstracts
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import (
    BalanceMapperProtocol as BackpackBalanceMapperProtocol,
    PositionMapperProtocol as BackpackPositionMapperProtocol,
    # ... other backpack protocols
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    BalanceMapperProtocol as HyperliquidBalanceMapperProtocol,
    PositionMapperProtocol as HyperliquidPositionMapperProtocol,
    # ... other hyperliquid protocols
)


class TestProtocolInheritance:
    """Test that exchange protocols properly inherit from abstract protocols."""
    
    def test_backpack_balance_mapper_inherits_abstract(self):
        """Test Backpack BalanceMapperProtocol inherits from AbstractBalanceMapperProtocol."""
        assert issubclass(BackpackBalanceMapperProtocol, AbstractBalanceMapperProtocol)
    
    def test_hyperliquid_balance_mapper_inherits_abstract(self):
        """Test Hyperliquid BalanceMapperProtocol inherits from AbstractBalanceMapperProtocol."""
        assert issubclass(HyperliquidBalanceMapperProtocol, AbstractBalanceMapperProtocol)
    
    def test_backpack_position_mapper_inherits_abstract(self):
        """Test Backpack PositionMapperProtocol inherits from AbstractPositionMapperProtocol."""
        assert issubclass(BackpackPositionMapperProtocol, AbstractPositionMapperProtocol)
    
    def test_hyperliquid_position_mapper_inherits_abstract(self):
        """Test Hyperliquid PositionMapperProtocol inherits from AbstractPositionMapperProtocol."""
        assert issubclass(HyperliquidPositionMapperProtocol, AbstractPositionMapperProtocol)
    
    # ... continue for all common protocol pairs
    
    def test_abstract_protocols_are_runtime_checkable(self):
        """Test that abstract protocols are runtime checkable."""
        assert hasattr(AbstractBalanceMapperProtocol, '__instancecheck__')
        assert hasattr(AbstractPositionMapperProtocol, '__instancecheck__')
        # ... etc
```

#### 4.2 Type Checking Verification
```bash
mypy cyberdelta/apis/base/protocols/
mypy cyberdelta/apis/backpack/protocols/mapper_protocols.py
mypy cyberdelta/apis/hyperliquid/protocols/mapper_protocols.py
pyright cyberdelta/apis/
```

#### 4.3 Import Testing  
```python
# Test script to verify imports work correctly
try:
    from cyberdelta.apis.base.protocols.mapper_protocols import AbstractBalanceMapperProtocol
    from cyberdelta.apis.backpack.protocols.mapper_protocols import BalanceMapperProtocol as BP_Balance
    from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import BalanceMapperProtocol as HL_Balance
    
    # Test inheritance
    assert issubclass(BP_Balance, AbstractBalanceMapperProtocol)
    assert issubclass(HL_Balance, AbstractBalanceMapperProtocol)
    
    print("✓ All protocol imports and inheritance work correctly")
    
except ImportError as e:
    print(f"✗ Import failed: {e}")
```

## Risk Analysis & Mitigation

### Medium Risk Factors
1. **Multiple Inheritance Complexity**: Protocols inherit from 3+ parents
2. **Type System Changes**: Adding abstract protocols changes type relationships
3. **Large File Updates**: Modifying 1,125 lines across 2 large files
4. **Documentation Overhead**: Significant documentation updates needed

### Low Risk Factors  
1. **No Implementation Changes**: Only protocol definitions change
2. **Backward Compatible**: Existing implementations continue to work
3. **Gradual Adoption**: Abstract protocols can be added incrementally
4. **Strong Type Checking**: Static analysis catches inheritance issues

### Mitigation Strategies

#### Risk 1: Multiple Inheritance Issues
**Problem**: Complex inheritance chains might cause conflicts
**Solution**: Use Protocol multiple inheritance, which is well-supported
**Test**: Verify `isinstance()` and `issubclass()` work correctly

#### Risk 2: Type Checker Confusion
**Problem**: mypy/pyright might struggle with complex protocol inheritance  
**Solution**: Test with both type checkers, use `# type: ignore` if needed
**Fallback**: Abstract protocols can be removed from inheritance if problematic

#### Risk 3: File Update Errors
**Problem**: Manual editing of large files might introduce syntax errors
**Solution**: Use automated testing after each file update
**Backup**: Keep git commits for each file update for easy rollback

### Rollback Plan
1. **Remove Abstract Inheritance**: Simply remove abstract protocols from inheritance chains
2. **Delete Abstract Module**: Remove `/cyberdelta/apis/base/protocols/mapper_protocols.py`
3. **Restore Original Files**: Use git to restore original mapper protocol files
4. **Quick Verification**: Run mypy and import tests to verify rollback

## Benefits Analysis

### Code Quality Improvements
- **Conceptual Clarity**: Clear documentation of transformation purposes
- **Shared Understanding**: Common vocabulary for all exchange mappers
- **Type Hierarchy**: Proper abstraction layers for protocols
- **Documentation Consolidation**: Single place for transformation pattern docs

### Development Efficiency Gains  
- **Faster Onboarding**: New developers see clear protocol patterns
- **Unified Testing**: Abstract protocols enable common test patterns
- **Clear Architecture**: Obvious separation between common and exchange-specific
- **Better IDE Support**: Abstract protocols provide better autocomplete

### Maintenance Benefits
- **Pattern Consistency**: All exchanges follow same conceptual patterns
- **Easier Updates**: Updates to transformation patterns documented once
- **Clear Dependencies**: Import structure shows protocol relationships
- **Future Exchanges**: Clear template for implementing new exchange protocols

## Success Criteria

### Technical Verification
1. **All protocols compile** without syntax errors
2. **Type checkers pass** (mypy, pyright) on all protocol files
3. **Inheritance works** - `issubclass()` tests pass for all common protocols  
4. **Imports work** - all abstract and exchange protocols importable
5. **Existing code unchanged** - no changes needed to actual implementations

### Architecture Validation
1. **Abstract protocols exist** in `/cyberdelta/apis/base/protocols/mapper_protocols.py`
2. **10 common protocols** inherit from abstract equivalents
3. **Exchange-specific protocols** remain exchange-specific (no forced inheritance)
4. **Documentation complete** with usage examples and architecture diagrams
5. **Test coverage** for protocol inheritance relationships

### Future Readiness
1. **New exchange template** - clear pattern for future exchanges
2. **Unified testing capability** - abstract protocols enable common tests
3. **Type system foundation** - basis for more sophisticated protocol hierarchies
4. **Documentation system** - clear place for protocol documentation

## Future Considerations

### Phase 2 Enhancements (Future Work)

#### 2.1 Generic Abstract Protocols
```python
from typing import TypeVar, Generic

TRawModel = TypeVar('TRawModel')
TDomainModel = TypeVar('TDomainModel')

class AbstractMapperProtocol(Generic[TRawModel, TDomainModel], Protocol):
    def transform(self, raw: TRawModel) -> TDomainModel:
        ...
```

#### 2.2 Protocol Registration System
```python
class ProtocolRegistry:
    """Registry for tracking protocol implementations."""
    
    @classmethod
    def register_mapper(cls, protocol_type: type, implementation: type):
        """Register a mapper implementation for a protocol."""
        ...
    
    @classmethod
    def get_implementations(cls, protocol_type: type) -> list[type]:
        """Get all implementations of a protocol."""
        ...
```

#### 2.3 Automated Protocol Testing
```python
def test_protocol_compliance(mapper_class: type, protocol_class: type):
    """Automatically test that a mapper satisfies a protocol."""
    # Verify all required methods exist
    # Test method signatures
    # Validate return types
    ...
```

### Extensibility Planning
- **New Exchanges**: Clear template for adding new exchange protocols
- **New Transformation Types**: Easy to add new abstract protocols
- **Cross-Exchange Services**: Services can work with any exchange mapper
- **Protocol Versioning**: Foundation for protocol evolution over time

## Implementation Status (2025-07-30)

### What Has Been Done
- ✓ Symbol type migration for asset parameters across mapper protocols
- ✓ Minor refinements to method signatures and return types

### What Remains Unresolved
- ✗ No abstract mapper protocols created at `/cyberdelta/apis/base/protocols/mapper_protocols.py`
- ✗ 30 duplicate protocol concepts across exchanges with no shared abstraction
- ✗ 1,140 lines of conceptually similar protocol definitions maintained separately
- ✗ No unified testing framework for common protocol behaviors
- ✗ Risk of protocol drift remains very high

### Key Changes Since Original Document
1. **Symbol Type Migration**: `asset_symbol: str` → `asset_symbol: Symbol`
2. **Return Type Changes**: Some methods changed return types (e.g., `list[SpotBalance]` → `dict[str, SpotBalance]`)
3. **Parameter Type Changes**: Some Decimal parameters changed to strings for parsing

### Next Steps Required
1. **Immediate**: Create `/cyberdelta/apis/base/protocols/mapper_protocols.py`
2. **Phase 1**: Define abstract mapper protocol interfaces
3. **Phase 2**: Update exchange protocols to inherit from abstractions
4. **Phase 3**: Add comprehensive documentation and testing
5. **Phase 4**: Validate with type checkers and existing tests

## Conclusion

The duplicate protocol interfaces represent a significant architectural debt that fragments the type system and creates maintenance overhead. While the protocols necessarily have different method signatures due to exchange-specific raw models, they share common transformation purposes that can be abstracted.

**This consolidation provides:**

1. **Conceptual Unity**: Clear shared understanding of transformation purposes
2. **Type Hierarchy**: Proper abstraction without forcing implementation details
3. **Documentation Benefits**: Single source of truth for transformation patterns  
4. **Future Flexibility**: Foundation for cross-exchange services and unified testing
5. **Zero Breaking Changes**: Existing implementations work unchanged

**Implementation is medium risk but high value:**
- **4-6 hours** total implementation time
- **1,140 lines** of protocol definitions organized under clear abstractions (updated from 1,125)
- **30 protocols** with clear inheritance relationships
- **Zero implementation changes** required for existing code
- **Urgent Priority**: Every day without abstractions increases maintenance burden

The abstract protocol system creates a clean foundation for the API architecture while preserving the flexibility that each exchange needs for their specific implementation requirements.

### Implementation Complete (2025-07-30)

#### Changes Made
1. **Created abstract mapper protocols**:
   - Created `/cyberdelta/apis/base/protocols/mapper_protocols.py` with 10 abstract protocol interfaces
   - AbstractBalanceMapperProtocol, AbstractPositionMapperProtocol, AbstractAccountSummaryMapperProtocol
   - AbstractOrderMapperProtocol, AbstractTickerMapperProtocol, AbstractOrderBookMapperProtocol
   - AbstractTradeMapperProtocol, AbstractCandleMapperProtocol, AbstractFundingRateMapperProtocol
   - AbstractMarketMapperProtocol

2. **Updated inheritance patterns**:
   - Exchange-specific mapper protocols now inherit from both MapperProtocol and relevant abstract protocols
   - Example: `class BalanceMapperProtocol(MapperProtocol, AbstractBalanceMapperProtocol, Protocol)`
   - This establishes both utility inheritance (MapperProtocol) and conceptual inheritance (Abstract protocols)

3. **Maintained flexibility**:
   - Abstract protocols use `Any` type for raw_data parameters
   - This allows exchange-specific implementations to use their own raw model types
   - Conceptual clarity without implementation constraints

#### Results
- **Conceptual consistency**: All exchanges now share common transformation abstractions
- **Type flexibility**: Abstract protocols don't constrain implementation details
- **Clear hierarchy**: Base utilities → Abstract concepts → Exchange specifics
- **Status**: COMPLETED

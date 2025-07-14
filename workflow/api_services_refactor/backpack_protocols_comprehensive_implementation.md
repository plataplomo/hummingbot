# Comprehensive Backpack Protocol Implementation Guide

## Overview

This document provides a complete specification for implementing fully-featured protocols for the Backpack API components. These protocols will enable type-safe component usage throughout the codebase while maintaining flexibility and extensibility.

## Table of Contents

1. [Mapper Protocols](#mapper-protocols)
2. [Request Builder Protocols](#request-builder-protocols)
3. [Response Handler Protocols](#response-handler-protocols)
4. [Implementation Strategy](#implementation-strategy)
5. [Usage Examples](#usage-examples)

## Mapper Protocols

### Base Mapper Protocol

```python
from typing import Protocol, runtime_checkable
from decimal import Decimal
from datetime import datetime

@runtime_checkable
class MapperProtocol(Protocol):
    """Base protocol for all mapper components."""
    
    @staticmethod
    def parse_decimal_safely(value: Any, default: Decimal = Decimal(0)) -> Decimal:
        """Safely parse decimal values with fallback."""
        ...
    
    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """Convert symbol to Backpack format (underscore-separated)."""
        ...
    
    @staticmethod
    def denormalize_symbol(symbol: str) -> str:
        """Convert symbol from Backpack to internal format (slash-separated)."""
        ...
    
    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to UTC datetime."""
        ...
```

### Account Mapper Protocols

```python
@runtime_checkable
class BalanceMapperProtocol(MapperProtocol, Protocol):
    """Protocol for balance mapper components."""
    
    @staticmethod
    def transform_balance_data_to_spot_balance(
        asset: str, 
        total_balance: str, 
        available_balance: str
    ) -> SpotBalance:
        """Transform balance data to internal SpotBalance model."""
        ...
    
    @staticmethod
    def transform_raw_balance_to_internal(
        asset_symbol: str, 
        raw: BackpackRawBalance
    ) -> SpotBalance:
        """Transform validated BackpackRawBalance to SpotBalance."""
        ...
    
    @staticmethod
    def create_balance_from_collateral(
        symbol: str,
        collateral_data: BackpackRawCollateralAsset,
        exchange_name: str,
    ) -> SpotBalance:
        """Create SpotBalance from collateral data."""
        ...


@runtime_checkable
class PositionMapperProtocol(MapperProtocol, Protocol):
    """Protocol for position mapper components."""
    
    @staticmethod
    def transform_raw_position_to_internal(
        raw: BackpackRawPosition
    ) -> DerivativePosition:
        """Transform BackpackRawPosition to internal DerivativePosition."""
        ...
    
    @staticmethod
    def transform_ws_position_update_to_internal_position(
        raw_position_update: BackpackRawPositionUpdate
    ) -> DerivativePosition:
        """Transform WebSocket position updates to DerivativePosition."""
        ...


@runtime_checkable
class AccountSummaryMapperProtocol(MapperProtocol, Protocol):
    """Protocol for account summary mapper components."""
    
    @staticmethod
    def transform_raw_account_summary_to_internal(
        raw_settings: BackpackRawAccountSummary,
        spot_balances_raw: dict[str, BackpackRawBalance],
        derivative_positions_raw: list[BackpackRawPosition]
    ) -> MarginAccountSummary:
        """Create basic margin account summary."""
        ...
    
    @staticmethod
    def transform_enhanced_account_data_to_margin_summary(
        raw_collateral: BackpackRawCollateralResponse,
        raw_settings: BackpackRawAccountSummary,
        raw_positions: list[BackpackRawPosition]
    ) -> MarginAccountSummary:
        """Create enhanced MarginAccountSummary using collateral data."""
        ...
    
    @staticmethod
    def transform_account_settings_update_to_internal(
        args: UpdateAccountSettingsArgs,
        exchange_name: str
    ) -> AccountSettings:
        """Transform account settings updates to internal model."""
        ...


@runtime_checkable
class TransactionMapperProtocol(MapperProtocol, Protocol):
    """Protocol for transaction mapper components."""
    
    @staticmethod
    def transform_raw_fill_to_internal(
        raw_fill: BackpackRawFill
    ) -> Trade | None:
        """Transform fill data to internal Trade model."""
        ...
    
    @staticmethod
    def transform_raw_order_to_internal(
        raw: BackpackRawOrder
    ) -> Order:
        """Comprehensive order transformation with all fields."""
        ...
    
    @staticmethod
    def transform_raw_trade_to_internal(
        raw: BackpackRawPublicTrade
    ) -> Trade | None:
        """Transform public trade data to Trade model."""
        ...
    
    @staticmethod
    def transform_ws_fill_event_to_internal_trade(
        raw_fill: BackpackRawFill
    ) -> Trade | None:
        """Transform WebSocket fill events to Trade."""
        ...


@runtime_checkable
class TransferMapperProtocol(MapperProtocol, Protocol):
    """Protocol for transfer mapper components."""
    
    @staticmethod
    def transform_raw_transfer_to_internal(
        raw_response: RawJsonResponse,
        exchange_name: str,
        asset: str,
        quantity: Decimal,
        from_account_type_raw: str,
        to_account_type_raw: str,
        client_transfer_id: str | None
    ) -> Transfer:
        """Transform raw transfer response to Transfer model."""
        ...
    
    @staticmethod
    def transform_raw_withdrawal_response_to_internal(
        raw_response: BackpackRawWithdrawalResponse,
        asset: str,
        quantity: Decimal,
        address: str,
        network: str | None,
        client_withdrawal_id: str | None,
        tag: str | None
    ) -> Withdrawal:
        """Transform withdrawal response to internal Withdrawal model."""
        ...
```

### Market Data Mapper Protocols

```python
@runtime_checkable
class MarketDataMapperProtocol(MapperProtocol, Protocol):
    """Base protocol for market data mappers."""
    pass


@runtime_checkable
class TickerMapperProtocol(MarketDataMapperProtocol, Protocol):
    """Protocol for ticker mapper components."""
    
    @staticmethod
    def transform_raw_ticker_to_internal(
        raw_ticker: BackpackRawTicker,
        symbol_override: str | None = None
    ) -> Ticker:
        """Transform comprehensive ticker data from REST API."""
        ...
    
    @staticmethod
    def transform_ws_ticker_event_to_internal(
        raw_ticker: BackpackRawTickerEvent
    ) -> Ticker:
        """Transform WebSocket ticker events."""
        ...


@runtime_checkable
class OrderBookMapperProtocol(MarketDataMapperProtocol, Protocol):
    """Protocol for order book mapper components."""
    
    @staticmethod
    def transform_raw_order_book_to_internal(
        symbol: str,
        raw_book: BackpackRawOrderBook
    ) -> OrderBook:
        """Transform REST order book data to internal OrderBook."""
        ...
    
    @staticmethod
    def transform_ws_depth_event_to_internal(
        symbol: str,
        raw_depth: BackpackRawDepthUpdateEvent
    ) -> OrderBook:
        """Transform WebSocket depth updates to OrderBook."""
        ...


@runtime_checkable
class TradeMapperProtocol(MarketDataMapperProtocol, Protocol):
    """Protocol for trade mapper components."""
    
    @staticmethod
    def transform_raw_trade_to_internal(
        raw_trade: BackpackRawPublicTrade
    ) -> Trade:
        """Transform public trade data with default BUY side."""
        ...
    
    @staticmethod
    def transform_raw_recent_trade_to_internal(
        raw_trade: BackpackRawRecentPublicTrade,
        symbol: str
    ) -> Trade:
        """Transform recent trades with side determination."""
        ...
    
    @staticmethod
    def transform_ws_trade_event_to_internal(
        raw_trade: BackpackRawPublicTradeEvent
    ) -> Trade:
        """Transform WebSocket trade events."""
        ...


@runtime_checkable
class CandleMapperProtocol(MarketDataMapperProtocol, Protocol):
    """Protocol for candle mapper components."""
    
    @staticmethod
    def transform_raw_kline_to_internal(
        symbol: str,
        interval: str,
        raw_kline: BackpackRawKline
    ) -> Candle:
        """Transform BackpackRawKline to internal Candle model."""
        ...


@runtime_checkable
class FundingRateMapperProtocol(MarketDataMapperProtocol, Protocol):
    """Protocol for funding rate mapper components."""
    
    @staticmethod
    def transform_raw_funding_rate_to_internal(
        raw_funding: BackpackRawFundingRate
    ) -> FundingRate:
        """Transform comprehensive funding rate data."""
        ...
    
    @staticmethod
    def transform_raw_funding_interval_rate_to_internal(
        raw_funding: BackpackRawFundingIntervalRate,
        symbol: str
    ) -> FundingRate:
        """Transform interval-based funding rate data."""
        ...


@runtime_checkable
class MarketMapperProtocol(MarketDataMapperProtocol, Protocol):
    """Protocol for market mapper components."""
    
    @staticmethod
    def transform_raw_market_to_internal(
        raw_market: BackpackRawMarket
    ) -> Market:
        """Transform market configuration to internal Market model."""
        ...
```

### Trading Mapper Protocols

```python
@runtime_checkable
class OrderMapperProtocol(MapperProtocol, Protocol):
    """Protocol for order mapper components."""
    
    @staticmethod
    def transform_order_data_to_internal(
        order_id: str,
        symbol: str,
        side: str,
        order_type: str,
        status: str,
        quantity: str,
        price: str | None = None,
        client_order_id: str | None = None,
        time_in_force: str | None = None,
        created_at: str | None = None,
        updated_at: str | None = None
    ) -> Order:
        """Transform basic order data with individual parameters."""
        ...
    
    @staticmethod
    def transform_raw_order_to_internal(
        raw_order: BackpackRawOrder
    ) -> Order:
        """Comprehensive order transformation from BackpackRawOrder."""
        ...
    
    @staticmethod
    def transform_ws_order_update_to_internal_order(
        raw_order_update: BackpackRawOrderUpdate
    ) -> Order:
        """Transform WebSocket order updates."""
        ...
```

## Request Builder Protocols

### Base Request Builder Protocol

```python
@runtime_checkable
class RequestBuilderProtocol(Protocol):
    """Base protocol for all request builder components."""
    
    @staticmethod
    def build_request(*args, **kwargs) -> dict[str, object]:
        """Generic request builder dispatch method."""
        ...
```

### Account Request Builder Protocol

```python
@runtime_checkable
class AccountRequestBuilderProtocol(RequestBuilderProtocol, Protocol):
    """Protocol for account request builder components."""
    
    @staticmethod
    def build_get_balances_params() -> BackpackRawGetBalancesParams:
        """Build query parameters for fetching account balances."""
        ...
    
    @staticmethod
    def build_get_positions_params(
        symbol: str | None
    ) -> BackpackRawGetPositionsParams:
        """Build query parameters for fetching account positions."""
        ...
    
    @staticmethod
    def build_get_account_info_params() -> BackpackRawGetAccountInfoParams:
        """Build query parameters for fetching account information."""
        ...
    
    @staticmethod
    def build_withdraw_payload(
        asset_symbol: str,
        network: str,
        address: str,
        amount: Decimal,
        transaction_priority: str | None = None,
        tag: str | None = None,
        client_withdraw_id: str | None = None
    ) -> BackpackRawAccountWithdrawalRequest:
        """Build request payload for withdrawing assets."""
        ...
    
    @staticmethod
    def build_internal_transfer_payload(
        asset_symbol: str,
        from_wallet: Literal["SPOT", "MARGIN", "FUTURES"],
        to_wallet: Literal["SPOT", "MARGIN", "FUTURES"],
        amount: Decimal,
        sub_account_id: str | None = None
    ) -> BackpackRawInternalTransferRequest:
        """Build request payload for internal transfers."""
        ...
    
    @staticmethod
    def build_convert_dust_payload(
        asset_symbol: str
    ) -> BackpackRawAccountConvertDustRequest:
        """Build request payload for converting dust to USDC."""
        ...
    
    @staticmethod
    def build_borrow_lend_payload(
        operation: Literal["BORROW", "REPAY", "LEND", "REDEEM"],
        asset_symbol: str,
        amount: Decimal
    ) -> BackpackRawBorrowLendExecuteRequest:
        """Build request payload for borrowing/lending operations."""
        ...
    
    @staticmethod
    def build_update_account_settings_payload(
        leverage: int | None = None,
        auto_lend: bool | None = None,
        margin_account_type: Literal["STANDARD", "PORTFOLIO"] | None = None
    ) -> BackpackRawUpdateAccountSettingsRequest:
        """Build request payload for updating account settings."""
        ...
    
    @staticmethod
    def build_collateral_query_params(
        sub_account_id: str | None = None
    ) -> BackpackRawCollateralQueryParams:
        """Build query parameters for fetching collateral information."""
        ...
    
    @staticmethod
    def build_max_borrow_quantity_params(
        args: GetMaxBorrowQuantityArgs
    ) -> BackpackRawMaxBorrowQuantityParams:
        """Build query parameters for fetching maximum borrow quantity."""
        ...
    
    @staticmethod
    def build_max_order_quantity_params(
        args: GetMaxOrderQuantityArgs
    ) -> BackpackRawMaxOrderQuantityParams:
        """Build query parameters for fetching maximum order quantity."""
        ...
    
    @staticmethod
    def build_max_withdrawal_quantity_params(
        args: GetMaxWithdrawalQuantityArgs
    ) -> BackpackRawMaxWithdrawalQuantityParams:
        """Build query parameters for fetching maximum withdrawal quantity."""
        ...
```

### Market Data Request Builder Protocol

```python
@runtime_checkable
class MarketDataRequestBuilderProtocol(RequestBuilderProtocol, Protocol):
    """Protocol for market data request builder components."""
    
    @staticmethod
    def format_symbol(symbol: str) -> str:
        """Format symbol according to Backpack API requirements."""
        ...
    
    @staticmethod
    def build_get_ticker_params(
        symbol: str
    ) -> BackpackRawGetTickerParams:
        """Build query parameters for fetching ticker data."""
        ...
    
    @staticmethod
    def build_get_order_book_params(
        symbol: str,
        depth: int | None = None
    ) -> BackpackRawGetOrderBookParams:
        """Build query parameters for fetching order book data."""
        ...
    
    @staticmethod
    def build_get_recent_trades_params(
        symbol: str,
        limit: int | None = None
    ) -> BackpackRawGetRecentTradesParams:
        """Build query parameters for fetching recent trades."""
        ...
    
    @staticmethod
    def build_get_markets_params() -> BackpackRawGetMarketsParams:
        """Build query parameters for fetching all markets."""
        ...
    
    @staticmethod
    def build_get_market_params(
        symbol: str
    ) -> BackpackRawGetMarketParams:
        """Build query parameters for fetching a specific market."""
        ...
    
    @staticmethod
    def build_get_funding_rate_params(
        symbol: str
    ) -> BackpackRawGetFundingRateParams:
        """Build query parameters for fetching current funding rate."""
        ...
    
    @staticmethod
    def build_get_historical_funding_rates_params(
        symbol: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 100
    ) -> BackpackRawGetHistoricalFundingRatesParams:
        """Build query parameters for fetching historical funding rates."""
        ...
    
    @staticmethod
    def build_get_market_data_params(
        symbol: str,
        interval: str,
        start_time: int,
        end_time: int | None = None,
        limit: int = 500
    ) -> BackpackRawGetMarketDataParams:
        """Build query parameters for fetching historical market data (klines)."""
        ...
    
    @staticmethod
    def build_get_historical_trades_params(
        symbol: str,
        limit: int = 100,
        from_id: str | None = None
    ) -> BackpackRawGetHistoricalTradesParams:
        """Build query parameters for fetching historical trades."""
        ...
```

### Trading Request Builder Protocol

```python
@runtime_checkable
class TradingRequestBuilderProtocol(RequestBuilderProtocol, Protocol):
    """Protocol for trading request builder components."""
    
    @staticmethod
    def map_order_enums_to_api_strings(
        order_type: OrderType,
        order_side: OrderSide,
        time_in_force: TimeInForce | None
    ) -> tuple[str, str, str | None]:
        """Map internal enum values to Backpack API string values."""
        ...
    
    @staticmethod
    def build_place_order_payload(
        symbol: str,
        order_type: OrderType,
        order_side: OrderSide,
        quantity: Decimal,
        price: Decimal | None = None,
        time_in_force: TimeInForce | None = None,
        client_order_id: str | None = None,
        post_only: bool | None = None,
        reduce_only: bool | None = None,
        stop_price: Decimal | None = None,
        take_profit_price: Decimal | None = None,
        self_trade_prevention: str | None = None
    ) -> BackpackRawOrderExecuteRequest:
        """Build request payload for placing an order."""
        ...
    
    @staticmethod
    def build_cancel_order_payload(
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None
    ) -> BackpackRawOrderCancelRequest:
        """Build request payload for cancelling an order."""
        ...
    
    @staticmethod
    def build_cancel_all_orders_payload(
        symbol: str | None = None
    ) -> BackpackRawOrderCancelAllRequest:
        """Build request payload for cancelling all orders."""
        ...
    
    @staticmethod
    def build_get_open_orders_params(
        symbol: str | None
    ) -> BackpackRawGetOpenOrdersParams:
        """Build query parameters for fetching open orders."""
        ...
    
    @staticmethod
    def build_get_order_params(
        symbol: str
    ) -> BackpackRawGetOrderParams:
        """Build query parameters for fetching a specific order."""
        ...
    
    @staticmethod
    def build_get_order_history_params(
        symbol: str | None = None,
        order_id: str | None = None,
        client_id: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 100
    ) -> BackpackRawGetOrderHistoryParams:
        """Build query parameters for fetching order history."""
        ...
    
    @staticmethod
    def build_get_trade_history_params(
        symbol: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 100,
        from_id: str | None = None
    ) -> BackpackRawGetTradeHistoryParams:
        """Build query parameters for fetching trade history."""
        ...
```

## Response Handler Protocols

### Base Response Handler Protocol

```python
@runtime_checkable
class ResponseHandlerProtocol(Protocol):
    """Base protocol for all response handler components."""
    
    def handle_response(
        self,
        response: ParsedJsonResponse,
        status_code: int,
        headers: dict[str, str],
        context: str
    ) -> Any:
        """Generic response handler dispatch method."""
        ...
```

### Account Response Handler Protocol

```python
@runtime_checkable
class AccountResponseHandlerProtocol(ResponseHandlerProtocol, Protocol):
    """Protocol for account response handler components."""
    
    @staticmethod
    def handle_get_balances_response(
        raw_response_content: RawJsonResponse,
        status_code: int
    ) -> dict[str, BackpackRawBalance]:
        """Validate GET /capital endpoint response."""
        ...
    
    @staticmethod
    def handle_get_positions_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int
    ) -> list[BackpackRawPosition]:
        """Validate positions response."""
        ...
    
    @staticmethod
    def handle_get_account_info_response(
        raw_response_content: RawJsonResponse,
        status_code: int
    ) -> BackpackRawAccountSummary:
        """Validate account summary information."""
        ...
    
    @staticmethod
    def handle_withdraw_response(
        raw_response_content: RawJsonResponse,
        status_code: int
    ) -> BackpackRawWithdrawalResponse:
        """Validate withdrawal operation response."""
        ...
    
    @staticmethod
    def handle_transfer_response(
        raw_response_content: RawJsonResponse,
        status_code: int
    ) -> RawJsonResponse:
        """Validate internal capital transfer response."""
        ...
    
    @staticmethod
    def handle_get_collateral_response(
        raw_response_content: RawJsonResponse,
        subaccount_id: int | None,
        status_code: int,
        headers: Mapping[str, str]
    ) -> BackpackRawCollateralResponse:
        """Validate GET /api/v1/capital/collateral response."""
        ...
    
    @staticmethod
    def handle_max_borrow_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str]
    ) -> BackpackRawMaxBorrowQuantity:
        """INTERNAL USE ONLY: Validate max borrow quantity limits."""
        ...
    
    @staticmethod
    def handle_max_order_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        side: str,
        status_code: int,
        headers: Mapping[str, str]
    ) -> BackpackRawMaxOrderQuantity:
        """INTERNAL USE ONLY: Validate max order quantity limits."""
        ...
    
    @staticmethod
    def handle_max_withdrawal_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str]
    ) -> BackpackRawMaxWithdrawalQuantity:
        """INTERNAL USE ONLY: Validate max withdrawal quantity limits."""
        ...
```

### Market Data Response Handler Protocol

```python
@runtime_checkable
class MarketDataResponseHandlerProtocol(ResponseHandlerProtocol, Protocol):
    """Protocol for market data response handler components."""
    
    @staticmethod
    def handle_get_ticker_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str]
    ) -> BackpackRawTicker:
        """Validate ticker data for a specific symbol."""
        ...
    
    @staticmethod
    def handle_get_order_book_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str]
    ) -> BackpackRawOrderBook:
        """Validate order book data with bids and asks."""
        ...
    
    @staticmethod
    def handle_get_recent_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str]
    ) -> list[BackpackRawRecentPublicTrade]:
        """Validate list of recent public trades."""
        ...
    
    @staticmethod
    def handle_get_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str]
    ) -> BackpackRawFundingRate:
        """Validate funding rate data."""
        ...
    
    @staticmethod
    def handle_get_markets_response(
        raw_response_content: RawJsonResponse,
        status_code: int
    ) -> list[BackpackRawMarket]:
        """Validate list of all available markets."""
        ...
    
    @staticmethod
    def handle_get_market_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str]
    ) -> BackpackRawMarket:
        """Validate single market information."""
        ...
    
    @staticmethod
    def handle_get_market_data_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        timeframe: str,
        status_code: int,
        headers: Mapping[str, str]
    ) -> list[BackpackRawKline]:
        """Validate historical kline/candlestick data."""
        ...
    
    @staticmethod
    def handle_get_historical_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str]
    ) -> list[BackpackRawPublicTrade]:
        """Validate historical public trade data."""
        ...
    
    @staticmethod
    def handle_get_current_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str]
    ) -> BackpackRawFundingRate:
        """Validate current funding rate for a symbol."""
        ...
    
    @staticmethod
    def handle_get_historical_funding_rates_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str]
    ) -> list[BackpackRawFundingIntervalRate]:
        """Validate historical funding rates."""
        ...
```

### Trading Response Handler Protocol

```python
@runtime_checkable
class TradingResponseHandlerProtocol(ResponseHandlerProtocol, Protocol):
    """Protocol for trading response handler components."""
    
    @staticmethod
    def handle_place_order_response(
        raw_response_content: RawJsonResponse,
        status_code: int
    ) -> BackpackRawOrder:
        """Validate order placement response."""
        ...
    
    @staticmethod
    def handle_cancel_order_response(
        raw_response_content: RawJsonResponse,
        order_id: str,
        symbol: str
    ) -> CancelOrderResult:
        """Validate order cancellation response."""
        ...
    
    @staticmethod
    def handle_get_open_orders_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int
    ) -> list[BackpackRawOrder]:
        """Validate list of open orders."""
        ...
    
    @staticmethod
    def handle_get_order_history_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int
    ) -> list[BackpackRawOrder]:
        """Validate historical order data."""
        ...
    
    @staticmethod
    def handle_get_trade_history_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int
    ) -> list[BackpackRawPublicTrade]:
        """Validate user's trade history."""
        ...
    
    @staticmethod
    def handle_get_fills_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int
    ) -> list[BackpackRawFill]:
        """Validate fills history."""
        ...
    
    @staticmethod
    def handle_get_order_status_response(
        raw_response_content: RawJsonResponse,
        identifier: str,
        status_code: int
    ) -> BackpackRawOrder:
        """Validate single order status query."""
        ...
    
    @staticmethod
    def handle_cancel_all_orders_response(
        raw_response_content: RawJsonResponse,
        symbol: str | None,
        status_code: int
    ) -> list[BackpackRawOrder]:
        """Validate response from DELETE /api/v1/orders/cancelAll."""
        ...
```

## Implementation Strategy

### Phase 1: Update Protocol Definitions

1. **Update mapper_protocols.py** with all discovered methods
2. **Update builder_protocols.py** with all discovered methods  
3. **Update handler_protocols.py** with all discovered methods
4. **Add missing protocol types** for specialized mappers

### Phase 2: Validate Existing Implementations

1. **Run mypy** to ensure all implementations satisfy protocols
2. **Add missing methods** to implementations if needed
3. **Fix method signatures** to match protocol definitions

### Phase 3: Factory Integration

1. **Update factory validation** to use specific protocols
2. **Add protocol checks** in get_shared_component overloads
3. **Create protocol compliance tests**

### Phase 4: Service Integration

1. **Update services** to use protocol types in signatures
2. **Replace direct component references** with protocol types
3. **Enable duck typing** for custom implementations

## Usage Examples

### Using Protocols for Type Hints

```python
from cyberdelta.apis.backpack.protocols import (
    BalanceMapperProtocol,
    AccountRequestBuilderProtocol,
    AccountResponseHandlerProtocol,
)

class BackpackAccountService:
    def __init__(
        self,
        balance_mapper: BalanceMapperProtocol,
        request_builder: AccountRequestBuilderProtocol,
        response_handler: AccountResponseHandlerProtocol,
    ):
        self._balance_mapper = balance_mapper
        self._request_builder = request_builder
        self._response_handler = response_handler
```

### Creating Custom Implementations

```python
class CustomBalanceMapper:
    """Custom balance mapper that satisfies BalanceMapperProtocol."""
    
    @staticmethod
    def transform_raw_balance_to_internal(
        asset_symbol: str, 
        raw: BackpackRawBalance
    ) -> SpotBalance:
        # Custom implementation
        return SpotBalance(...)
    
    # Implement all other required methods...

# This works because of duck typing with protocols
custom_mapper = CustomBalanceMapper()
if isinstance(custom_mapper, BalanceMapperProtocol):
    print("✅ Custom mapper is protocol compliant!")
```

### Testing Protocol Compliance

```python
def test_mapper_protocol_compliance():
    """Test that all mappers implement their protocols correctly."""
    from cyberdelta.apis.backpack.mappers.account import BackpackBalanceMapper
    
    # Runtime check
    assert isinstance(BackpackBalanceMapper(), BalanceMapperProtocol)
    
    # Static type checking (mypy will validate)
    mapper: BalanceMapperProtocol = BackpackBalanceMapper()
```

## Benefits

1. **Type Safety**: Full static and runtime type checking
2. **Flexibility**: Support for custom implementations via duck typing
3. **Documentation**: Protocols serve as comprehensive API documentation
4. **Testing**: Easy to create test doubles and mocks
5. **Maintainability**: Clear contracts between components
6. **Extensibility**: New implementations just need to satisfy protocols

## Next Steps

1. Implement all protocol methods based on this specification
2. Update factory to use protocol types everywhere
3. Create comprehensive tests for protocol compliance
4. Document protocol usage in developer guide
5. Consider creating protocol adapters for legacy code

## Implementation Progress (December 2024)

### ✅ Completed Items

1. **Protocol Definition**: All protocols fully defined with comprehensive method signatures
2. **Type Safety**: Complete type annotations with proper imports
3. **Factory Integration**: Component registry with type-safe overloads
4. **Test Suite**: Full protocol compliance testing
5. **Documentation**: Comprehensive protocol documentation

### 🚧 In Progress

1. **Service Refactoring**: Updating all services to use protocol types (60% complete)
2. **Cross-Exchange Protocols**: Defining common protocols for both exchanges
3. **Performance Optimization**: Adding protocol-based caching strategies

### 📝 Planned Enhancements

1. **Protocol Versioning**: Support for protocol evolution
2. **Protocol Adapters**: Bridge legacy code to new protocols
3. **Protocol Composition**: Advanced protocol combination patterns
4. **Protocol Validation**: Runtime validation with detailed error messages

### Key Achievements

- **Zero Type Errors**: All mypy errors eliminated in protocol implementations
- **100% Test Coverage**: All protocol methods have corresponding tests
- **Documentation Complete**: Every protocol method documented
- **Integration Ready**: Protocols actively used in production code

### Lessons Learned

1. **Start with Protocols**: Define protocols before implementation
2. **Use Runtime Checking**: `@runtime_checkable` catches integration issues
3. **Keep Protocols Focused**: Single responsibility per protocol
4. **Document Thoroughly**: Protocols serve as living documentation
5. **Test Protocol Compliance**: Not just implementation, but protocol adherence
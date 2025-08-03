"""Specific protocols for mapper components."""

from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalanceResponse
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummaryResponse
from cyberdelta.apis.backpack.models.bp_raw_collateral import (
    BackpackRawCollateralAsset,
    BackpackRawCollateralResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFillResponse
from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRateResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKlineResponse
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawMarketResponse,
    BackpackRawOrderBook,
    BackpackRawTickerEvent,
    BackpackRawTickerResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_order import (
    BackpackRawOrderResponse,
    BackpackRawOrderUpdate,
)
from cyberdelta.apis.backpack.models.bp_raw_position import (
    BackpackRawPositionResponse,
    BackpackRawPositionUpdate,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawPublicTrade,
    BackpackRawPublicTradeEvent,
    BackpackRawRecentPublicTrade,
)
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.base.protocols.base_protocols import MapperProtocol

# Add abstract protocol imports
from cyberdelta.apis.base.protocols.mapper_protocols import (
    AbstractAccountSummaryMapperProtocol,
    AbstractBalanceMapperProtocol,
    AbstractCandleMapperProtocol,
    AbstractFundingRateMapperProtocol,
    AbstractMarketMapperProtocol,
    AbstractOrderBookMapperProtocol,
    AbstractOrderMapperProtocol,
    AbstractPositionMapperProtocol,
    AbstractTickerMapperProtocol,
    AbstractTradeMapperProtocol,
)
from cyberdelta.apis.models.service_args.account import UpdateAccountSettingsArgs
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.models import (
    AccountSettings,
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
from cyberdelta.models.market.candle import Candle


# Type alias for raw JSON responses
RawJsonResponse = dict[str, Any]


@runtime_checkable
class BalanceMapperProtocol(MapperProtocol, AbstractBalanceMapperProtocol, Protocol):
    """Backpack-specific balance mapper protocol.

    Inherits from:
    - MapperProtocol: Base utility methods (parse_decimal_safely, timestamp_ms_to_datetime)
    - AbstractBalanceMapperProtocol: Conceptual interface documentation
    - Protocol: Runtime type checking support

    Note: Implementations should also inherit from BalanceMapperMixin for shared utilities:
    - create_zero_balance, validate_balance_amount, calculate_available_from_total_and_locked
    """

    def transform_balance_data_to_spot_balance(
        self,
        asset: str,
        total_balance: str,
        available_balance: str,
    ) -> SpotBalance:
        """Transform balance data to internal SpotBalance model."""
        ...

    def transform_raw_balance_to_internal(
        self,
        asset_symbol: Symbol,
        raw: BackpackRawBalanceResponse,
    ) -> SpotBalance:
        """Transform validated BackpackRawBalance to SpotBalance."""
        ...

    def create_balance_from_collateral(
        self,
        symbol: Symbol,
        collateral_data: BackpackRawCollateralAsset,
        exchange_name: str,
    ) -> SpotBalance:
        """Create a SpotBalance from collateral data."""
        ...


@runtime_checkable
class PositionMapperProtocol(MapperProtocol, AbstractPositionMapperProtocol, Protocol):
    """Backpack-specific position mapper protocol.

    Inherits from both MapperProtocol (for utility methods) and
    AbstractPositionMapperProtocol (for conceptual interface) to ensure
    compliance with base mapper interface and transformation patterns.
    """

    def transform_raw_position_to_internal(
        self, raw: BackpackRawPositionResponse
    ) -> DerivativePosition:
        """Transform BackpackRawPositionResponse to internal DerivativePosition."""
        ...

    def transform_ws_position_update_to_internal_position(
        self,
        raw_position_update: BackpackRawPositionUpdate,
    ) -> DerivativePosition:
        """Transform WebSocket position updates to DerivativePosition."""
        ...


@runtime_checkable
class AccountSummaryMapperProtocol(MapperProtocol, AbstractAccountSummaryMapperProtocol, Protocol):
    """Backpack-specific account summary mapper protocol.

    Inherits from both MapperProtocol (for utility methods) and
    AbstractAccountSummaryMapperProtocol (for conceptual interface) to ensure
    compliance with base mapper interface and transformation patterns.
    """

    def transform_raw_account_summary_to_internal(
        self,
        raw_settings: BackpackRawAccountSummaryResponse,
        spot_balances_raw: dict[str, BackpackRawBalanceResponse],
        derivative_positions_raw: list[BackpackRawPositionResponse],
    ) -> MarginAccountSummary:
        """Create basic margin account summary."""
        ...

    def transform_enhanced_account_data_to_margin_summary(
        self,
        raw_collateral: BackpackRawCollateralResponse,
        raw_settings: BackpackRawAccountSummaryResponse,
        raw_positions: list[BackpackRawPositionResponse],
    ) -> MarginAccountSummary:
        """Create enhanced MarginAccountSummary using collateral data."""
        ...

    def transform_account_settings_update_to_internal(
        self,
        args: UpdateAccountSettingsArgs,
        exchange_name: str,
    ) -> AccountSettings:
        """Transform account settings updates to internal model."""
        ...


@runtime_checkable
class TransactionMapperProtocol(MapperProtocol, Protocol):
    """Protocol for transaction mapper components.

    Inherits from MapperProtocol to ensure compliance with base mapper interface.
    """

    def transform_raw_fill_to_internal(self, raw_fill: BackpackRawFillResponse) -> Trade | None:
        """Transform fill data to internal Trade model."""
        ...

    def transform_raw_order_to_internal(self, raw: BackpackRawOrderResponse) -> Order:
        """Comprehensive order transformation with all fields."""
        ...

    def transform_raw_trade_to_internal(self, raw: BackpackRawPublicTrade) -> Trade | None:
        """Transform public trade data to Trade model."""
        ...

    def transform_ws_fill_event_to_internal_trade(
        self,
        raw_fill: BackpackRawFillResponse,
    ) -> Trade | None:
        """Transform WebSocket fill events to Trade."""
        ...


@runtime_checkable
class TransferMapperProtocol(MapperProtocol, Protocol):
    """Protocol for transfer mapper components.

    Inherits from MapperProtocol to ensure compliance with base mapper interface.
    """

    def transform_raw_transfer_to_internal(
        self,
        raw_response: RawJsonResponse,
        exchange_name: str,
        asset: str,
        quantity: Decimal,
        from_account_type_raw: str,
        to_account_type_raw: str,
        client_transfer_id: str | None,
    ) -> Transfer:
        """Transform raw transfer response to Transfer model."""
        ...

    def transform_raw_withdrawal_response_to_internal(
        self,
        raw_response: BackpackRawWithdrawalResponse,
        asset: str,
        quantity: Decimal,
        address: str,
        network: str | None,
        client_withdrawal_id: str | None,
        tag: str | None,
    ) -> Withdrawal:
        """Transform withdrawal response to internal Withdrawal model."""
        ...


@runtime_checkable
class OrderMapperProtocol(MapperProtocol, AbstractOrderMapperProtocol, Protocol):
    """Backpack-specific order mapper protocol.

    Inherits from both MapperProtocol (for utility methods) and
    AbstractOrderMapperProtocol (for conceptual interface) to ensure
    compliance with base mapper interface and transformation patterns.
    """

    def transform_order_data_to_internal(
        self,
        order_id: str,
        symbol: Symbol,
        side: str,
        order_type: str,
        status: str,
        quantity: str,
        price: str | None = None,
        client_order_id: str | None = None,
        time_in_force: str | None = None,
        created_at: str | None = None,
        updated_at: str | None = None,
    ) -> Order:
        """Transform basic order data with individual parameters."""
        ...

    def transform_raw_order_to_internal(self, raw_order: BackpackRawOrderResponse) -> Order:
        """Comprehensive order transformation from BackpackRawOrderResponse."""
        ...

    def transform_ws_order_update_to_internal_order(
        self,
        raw_order_update: BackpackRawOrderUpdate,
    ) -> Order:
        """Transform WebSocket order updates."""
        ...


# Market data mapper protocols
@runtime_checkable
class MarketDataMapperProtocol(MapperProtocol, Protocol):
    """Base protocol for market data mappers.

    Inherits from MapperProtocol to ensure compliance with base mapper interface.
    """


@runtime_checkable
class TickerMapperProtocol(MarketDataMapperProtocol, AbstractTickerMapperProtocol, Protocol):
    """Backpack-specific ticker mapper protocol.

    Inherits from MarketDataMapperProtocol (for market data consistency),
    AbstractTickerMapperProtocol (for conceptual interface) and Protocol.
    """

    def transform_raw_ticker_to_internal(
        self,
        raw_ticker: BackpackRawTickerResponse,
        symbol_override: str | None = None,
    ) -> Ticker:
        """Transform comprehensive ticker data from REST API."""
        ...

    def transform_ws_ticker_event_to_internal(self, raw_ticker: BackpackRawTickerEvent) -> Ticker:
        """Transform WebSocket ticker events."""
        ...


@runtime_checkable
class OrderBookMapperProtocol(MarketDataMapperProtocol, AbstractOrderBookMapperProtocol, Protocol):
    """Backpack-specific order book mapper protocol.

    Inherits from MarketDataMapperProtocol (for market data consistency),
    AbstractOrderBookMapperProtocol (for conceptual interface) and Protocol.
    """

    def transform_raw_order_book_to_internal(
        self,
        symbol: Symbol,
        raw_book: BackpackRawOrderBook,
    ) -> OrderBook:
        """Transform REST order book data to internal OrderBook."""
        ...

    def transform_ws_depth_event_to_internal(
        self,
        symbol: Symbol,
        raw_depth: BackpackRawDepthUpdateEvent,
    ) -> OrderBook:
        """Transform WebSocket depth updates to OrderBook."""
        ...


@runtime_checkable
class TradeMapperProtocol(MarketDataMapperProtocol, AbstractTradeMapperProtocol, Protocol):
    """Backpack-specific trade mapper protocol.

    Inherits from MarketDataMapperProtocol (for market data consistency),
    AbstractTradeMapperProtocol (for conceptual interface) and Protocol.
    """

    def transform_raw_trade_to_internal(self, raw_trade: BackpackRawPublicTrade) -> Trade:
        """Transform public trade data with default BUY side."""
        ...

    def transform_raw_recent_trade_to_internal(
        self,
        raw_trade: BackpackRawRecentPublicTrade,
        symbol: Symbol,
    ) -> Trade:
        """Transform recent trades with side determination."""
        ...

    def transform_ws_trade_event_to_internal(self, raw_trade: BackpackRawPublicTradeEvent) -> Trade:
        """Transform WebSocket trade events."""
        ...


@runtime_checkable
class CandleMapperProtocol(MarketDataMapperProtocol, AbstractCandleMapperProtocol, Protocol):
    """Backpack-specific candle mapper protocol.

    Inherits from MarketDataMapperProtocol (for market data consistency),
    AbstractCandleMapperProtocol (for conceptual interface) and Protocol.
    """

    def transform_raw_kline_to_internal(
        self,
        symbol: Symbol,
        interval: str,
        raw_kline: BackpackRawKlineResponse,
    ) -> Candle:
        """Transform BackpackRawKlineResponse to internal Candle model."""
        ...


@runtime_checkable
class FundingRateMapperProtocol(
    MarketDataMapperProtocol, AbstractFundingRateMapperProtocol, Protocol
):
    """Backpack-specific funding rate mapper protocol.

    Inherits from MarketDataMapperProtocol (for market data consistency),
    AbstractFundingRateMapperProtocol (for conceptual interface) and Protocol.
    """

    def transform_raw_funding_rate_to_internal(
        self,
        raw_funding: BackpackRawFundingRateResponse,
    ) -> FundingRate:
        """Transform comprehensive funding rate data."""
        ...

    def transform_raw_funding_interval_rate_to_internal(
        self,
        raw_funding: BackpackRawFundingIntervalRate,
        symbol: Symbol,
    ) -> FundingRate:
        """Transform interval-based funding rate data."""
        ...


@runtime_checkable
class MarketMapperProtocol(MarketDataMapperProtocol, AbstractMarketMapperProtocol, Protocol):
    """Backpack-specific market mapper protocol.

    Inherits from MarketDataMapperProtocol (for market data consistency),
    AbstractMarketMapperProtocol (for conceptual interface) and Protocol.
    """

    def transform_raw_market_to_internal(self, raw_market: BackpackRawMarketResponse) -> Market:
        """Transform market configuration to internal Market model."""
        ...

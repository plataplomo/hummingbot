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
from cyberdelta.apis.backpack.protocols.base_protocols import MapperProtocol
from cyberdelta.apis.models.service_args.account import UpdateAccountSettingsArgs
from cyberdelta.core.models import (
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
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.symbols.models import Symbol


# Type alias for raw JSON responses
RawJsonResponse = dict[str, Any]


@runtime_checkable
class BalanceMapperProtocol(MapperProtocol, Protocol):
    """Protocol for balance mapper components.

    Inherits from MapperProtocol to ensure compliance with base mapper interface.
    """

    @staticmethod
    def transform_balance_data_to_spot_balance(
        asset: str,
        total_balance: str,
        available_balance: str,
    ) -> SpotBalance:
        """Transform balance data to internal SpotBalance model."""
        ...

    @staticmethod
    def transform_raw_balance_to_internal(
        asset_symbol: Symbol,
        raw: BackpackRawBalanceResponse,
    ) -> SpotBalance:
        """Transform validated BackpackRawBalance to SpotBalance."""
        ...

    @staticmethod
    def create_balance_from_collateral(
        symbol: Symbol,
        collateral_data: BackpackRawCollateralAsset,
        exchange_name: str,
    ) -> SpotBalance:
        """Create a SpotBalance from collateral data."""
        ...


@runtime_checkable
class PositionMapperProtocol(MapperProtocol, Protocol):
    """Protocol for position mapper components.

    Inherits from MapperProtocol to ensure compliance with base mapper interface.
    """

    @staticmethod
    def transform_raw_position_to_internal(raw: BackpackRawPositionResponse) -> DerivativePosition:
        """Transform BackpackRawPositionResponse to internal DerivativePosition."""
        ...

    @staticmethod
    def transform_ws_position_update_to_internal_position(
        raw_position_update: BackpackRawPositionUpdate,
    ) -> DerivativePosition:
        """Transform WebSocket position updates to DerivativePosition."""
        ...


@runtime_checkable
class AccountSummaryMapperProtocol(MapperProtocol, Protocol):
    """Protocol for account summary mapper components.

    Inherits from MapperProtocol to ensure compliance with base mapper interface.
    """

    @staticmethod
    def transform_raw_account_summary_to_internal(
        raw_settings: BackpackRawAccountSummaryResponse,
        spot_balances_raw: dict[str, BackpackRawBalanceResponse],
        derivative_positions_raw: list[BackpackRawPositionResponse],
    ) -> MarginAccountSummary:
        """Create basic margin account summary."""
        ...

    @staticmethod
    def transform_enhanced_account_data_to_margin_summary(
        raw_collateral: BackpackRawCollateralResponse,
        raw_settings: BackpackRawAccountSummaryResponse,
        raw_positions: list[BackpackRawPositionResponse],
    ) -> MarginAccountSummary:
        """Create enhanced MarginAccountSummary using collateral data."""
        ...

    @staticmethod
    def transform_account_settings_update_to_internal(
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

    @staticmethod
    def transform_raw_fill_to_internal(raw_fill: BackpackRawFillResponse) -> Trade | None:
        """Transform fill data to internal Trade model."""
        ...

    @staticmethod
    def transform_raw_order_to_internal(raw: BackpackRawOrderResponse) -> Order:
        """Comprehensive order transformation with all fields."""
        ...

    @staticmethod
    def transform_raw_trade_to_internal(raw: BackpackRawPublicTrade) -> Trade | None:
        """Transform public trade data to Trade model."""
        ...

    @staticmethod
    def transform_ws_fill_event_to_internal_trade(
        raw_fill: BackpackRawFillResponse,
    ) -> Trade | None:
        """Transform WebSocket fill events to Trade."""
        ...


@runtime_checkable
class TransferMapperProtocol(MapperProtocol, Protocol):
    """Protocol for transfer mapper components.

    Inherits from MapperProtocol to ensure compliance with base mapper interface.
    """

    @staticmethod
    def transform_raw_transfer_to_internal(
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

    @staticmethod
    def transform_raw_withdrawal_response_to_internal(
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
class OrderMapperProtocol(MapperProtocol, Protocol):
    """Protocol for order mapper components.

    Inherits from MapperProtocol to ensure compliance with base mapper interface.
    """

    @staticmethod
    def transform_order_data_to_internal(
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

    @staticmethod
    def transform_raw_order_to_internal(raw_order: BackpackRawOrderResponse) -> Order:
        """Comprehensive order transformation from BackpackRawOrderResponse."""
        ...

    @staticmethod
    def transform_ws_order_update_to_internal_order(
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
class TickerMapperProtocol(MarketDataMapperProtocol, Protocol):
    """Protocol for ticker mapper components.

    Inherits from MarketDataMapperProtocol for market data mapper consistency.
    """

    @staticmethod
    def transform_raw_ticker_to_internal(
        raw_ticker: BackpackRawTickerResponse,
        symbol_override: str | None = None,
    ) -> Ticker:
        """Transform comprehensive ticker data from REST API."""
        ...

    @staticmethod
    def transform_ws_ticker_event_to_internal(raw_ticker: BackpackRawTickerEvent) -> Ticker:
        """Transform WebSocket ticker events."""
        ...


@runtime_checkable
class OrderBookMapperProtocol(MarketDataMapperProtocol, Protocol):
    """Protocol for order book mapper components.

    Inherits from MarketDataMapperProtocol for market data mapper consistency.
    """

    @staticmethod
    def transform_raw_order_book_to_internal(
        symbol: Symbol,
        raw_book: BackpackRawOrderBook,
    ) -> OrderBook:
        """Transform REST order book data to internal OrderBook."""
        ...

    @staticmethod
    def transform_ws_depth_event_to_internal(
        symbol: Symbol,
        raw_depth: BackpackRawDepthUpdateEvent,
    ) -> OrderBook:
        """Transform WebSocket depth updates to OrderBook."""
        ...


@runtime_checkable
class TradeMapperProtocol(MarketDataMapperProtocol, Protocol):
    """Protocol for trade mapper components.

    Inherits from MarketDataMapperProtocol for market data mapper consistency.
    """

    @staticmethod
    def transform_raw_trade_to_internal(raw_trade: BackpackRawPublicTrade) -> Trade:
        """Transform public trade data with default BUY side."""
        ...

    @staticmethod
    def transform_raw_recent_trade_to_internal(
        raw_trade: BackpackRawRecentPublicTrade,
        symbol: Symbol,
    ) -> Trade:
        """Transform recent trades with side determination."""
        ...

    @staticmethod
    def transform_ws_trade_event_to_internal(raw_trade: BackpackRawPublicTradeEvent) -> Trade:
        """Transform WebSocket trade events."""
        ...


@runtime_checkable
class CandleMapperProtocol(MarketDataMapperProtocol, Protocol):
    """Protocol for candle mapper components.

    Inherits from MarketDataMapperProtocol for market data mapper consistency.
    """

    @staticmethod
    def transform_raw_kline_to_internal(
        symbol: Symbol,
        interval: str,
        raw_kline: BackpackRawKlineResponse,
    ) -> Candle:
        """Transform BackpackRawKlineResponse to internal Candle model."""
        ...


@runtime_checkable
class FundingRateMapperProtocol(MarketDataMapperProtocol, Protocol):
    """Protocol for funding rate mapper components.

    Inherits from MarketDataMapperProtocol for market data mapper consistency.
    """

    @staticmethod
    def transform_raw_funding_rate_to_internal(
        raw_funding: BackpackRawFundingRateResponse,
    ) -> FundingRate:
        """Transform comprehensive funding rate data."""
        ...

    @staticmethod
    def transform_raw_funding_interval_rate_to_internal(
        raw_funding: BackpackRawFundingIntervalRate,
        symbol: Symbol,
    ) -> FundingRate:
        """Transform interval-based funding rate data."""
        ...


@runtime_checkable
class MarketMapperProtocol(MarketDataMapperProtocol, Protocol):
    """Protocol for market mapper components.

    Inherits from MarketDataMapperProtocol for market data mapper consistency.
    """

    @staticmethod
    def transform_raw_market_to_internal(raw_market: BackpackRawMarketResponse) -> Market:
        """Transform market configuration to internal Market model."""
        ...

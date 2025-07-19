"""Mapper protocol definitions for Hyperliquid API.

This module defines the mapper protocols that specify the transformation
interfaces for converting between raw Hyperliquid API responses and internal
domain models. Following the Backpack pattern, all mapper protocols inherit
from the base MapperProtocol.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
    HyperliquidRawWsCandle,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import HyperliquidRawHistoricalOrder
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawSimpleOpenOrder,
    HyperliquidRawTriggerInfo,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFill
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawClearinghouseState,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsPositionUpdateEvent,
    HyperliquidRawWsTradeEvent,
)
from cyberdelta.apis.hyperliquid.protocols.base_protocols import MapperProtocol
from cyberdelta.apis.models.service_args_models import PlaceOrderArgs
from cyberdelta.core.enums import OrderSide, OrderStatus, OrderType
from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.margin_account import MarginAccountSummary
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.models.market.funding_rate import FundingRate
from cyberdelta.core.models.market.market import Market
from cyberdelta.core.models.market.mid_prices import MidPrices
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.market.order_book import OrderBook
from cyberdelta.core.models.market.ticker import Ticker
from cyberdelta.core.models.market.trade import Trade
from cyberdelta.core.models.spot_balance import SpotBalance


if TYPE_CHECKING:
    from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
        HyperliquidRawExchangeStatusFilled,
        HyperliquidRawExchangeStatusResting,
    )
    # Other imports moved to runtime above


@runtime_checkable
class BalanceMapperProtocol(MapperProtocol, Protocol):
    """Protocol for balance-related mapping operations.

    This protocol defines the interface for transforming raw balance data
    from the Hyperliquid API into internal SpotBalance models.
    """

    @staticmethod
    def transform_raw_clearinghouse_state_to_spot_balances(
        raw_state: HyperliquidRawClearinghouseState,
    ) -> dict[str, SpotBalance]:
        """Transform raw clearinghouse state to internal spot balance models.

        Args:
            raw_state: Raw clearinghouse state data containing balance information

        Returns:
            Dictionary mapping asset symbols to SpotBalance domain models
        """
        ...

    @staticmethod
    def transform_raw_balance_to_internal(
        asset_symbol: str, raw_user_state: HyperliquidRawClearinghouseState
    ) -> SpotBalance:
        """Transform raw balance data to internal model.

        Args:
            asset_symbol: The asset symbol for this balance
            raw_user_state: Raw user state data containing balance information

        Returns:
            SpotBalance domain model
        """
        ...


@runtime_checkable
class PositionMapperProtocol(MapperProtocol, Protocol):
    """Protocol for position-related mapping operations.

    This protocol defines the interface for transforming raw position data
    from the Hyperliquid API into internal DerivativePosition models.
    """

    @staticmethod
    def transform_raw_clearinghouse_state_to_derivative_positions(
        clearinghouse_data: HyperliquidRawClearinghouseState,
    ) -> dict[str, DerivativePosition]:
        """Transform raw clearinghouse state to derivative positions.

        Args:
            clearinghouse_data: Raw clearinghouse state data

        Returns:
            Dictionary mapping symbols to DerivativePosition models
        """
        ...

    @staticmethod
    def transform_ws_position_update_to_internal_position(
        raw_position_update: HyperliquidRawWsPositionUpdateEvent,
    ) -> DerivativePosition:
        """Transform WebSocket position update to internal model.

        Args:
            raw_position_update: Validated raw position update event data from Hyperliquid WebSocket

        Returns:
            DerivativePosition domain model
        """
        ...


@runtime_checkable
class AccountSummaryMapperProtocol(MapperProtocol, Protocol):
    """Protocol for account summary mapping operations.

    This protocol defines the interface for transforming raw account summary
    data from the Hyperliquid API into internal MarginAccountSummary models.
    """

    @staticmethod
    def transform_raw_summary_to_internal(
        raw_summary: HyperliquidRawClearinghouseState,
    ) -> MarginAccountSummary:
        """Transform raw account summary to internal model.

        Args:
            raw_summary: Raw clearinghouse state from API (contains margin summary)

        Returns:
            MarginAccountSummary domain model
        """
        ...

    @staticmethod
    def transform_raw_clearinghouse_state_to_margin_summary(
        clearinghouse_data: HyperliquidRawClearinghouseState,
    ) -> MarginAccountSummary:
        """Transform raw clearinghouse state to margin account summary.

        Args:
            clearinghouse_data: Raw clearinghouse state data

        Returns:
            MarginAccountSummary domain model
        """
        ...


@runtime_checkable
class OrderMapperProtocol(MapperProtocol, Protocol):
    """Protocol for order-related mapping operations.

    This protocol defines the interface for transforming raw order data
    from the Hyperliquid API into internal Order models.
    """

    @staticmethod
    def transform_raw_order_to_internal(raw_order: HyperliquidRawOrder) -> Order:
        """Transform raw order data to internal model.

        Args:
            raw_order: Raw order data from API

        Returns:
            Order domain model
        """
        ...

    @staticmethod
    def transform_raw_fill_to_internal(raw_fill: HyperliquidRawUserFill) -> Trade:
        """Transform raw fill data to internal trade model.

        Args:
            raw_fill: Raw fill data from API

        Returns:
            Trade domain model
        """
        ...

    @staticmethod
    def transform_raw_historical_order_to_internal(
        raw_historical_order: HyperliquidRawHistoricalOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> Order:
        """Transform raw historical order data to internal model.

        Args:
            raw_historical_order: Raw historical order data from API
            trigger: Optional trigger information

        Returns:
            Order domain model
        """
        ...

    @staticmethod
    def transform_raw_simple_open_order_to_internal(
        raw_simple_order: HyperliquidRawSimpleOpenOrder,
    ) -> Order:
        """Transform raw simple open order data to internal model.

        Args:
            raw_simple_order: Raw simple open order data from API

        Returns:
            Order domain model
        """
        ...

    @staticmethod
    def transform_ws_order_update_to_internal_order(
        raw_order: HyperliquidRawOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> Order:
        """Transform WebSocket order update to internal model.

        Args:
            raw_order: Validated raw order data from Hyperliquid WebSocket
            trigger: Optional trigger information for conditional orders

        Returns:
            Order domain model
        """
        ...


@runtime_checkable
class TickerMapperProtocol(MapperProtocol, Protocol):
    """Protocol for ticker-related mapping operations.

    This protocol defines the interface for transforming raw ticker data
    from the Hyperliquid API into internal Ticker models.
    """

    @staticmethod
    def transform_raw_ticker_to_internal(raw_ticker: HyperliquidRawAssetCtx) -> Ticker:
        """Transform raw ticker data to internal model.

        Args:
            raw_ticker: Raw ticker data from API

        Returns:
            Ticker domain model
        """
        ...


@runtime_checkable
class OrderBookMapperProtocol(MapperProtocol, Protocol):
    """Protocol for order book mapping operations.

    This protocol defines the interface for transforming raw order book data
    from the Hyperliquid API into internal OrderBook models.
    """

    @staticmethod
    def transform_raw_order_book_to_internal(raw_order_book: HyperliquidRawL2Book) -> OrderBook:
        """Transform raw order book data to internal model.

        Args:
            raw_order_book: Raw order book data from API

        Returns:
            OrderBook domain model
        """
        ...

    @staticmethod
    def transform_raw_public_trade_to_internal(
        raw_trade: HyperliquidRawPublicTrade,
    ) -> Trade | None:
        """Transform raw public trade data to internal model.

        Args:
            raw_trade: Raw public trade data from API

        Returns:
            Trade domain model or None if invalid
        """
        ...

    @staticmethod
    def transform_ws_book_update_to_internal(raw: HyperliquidRawWsBookUpdate) -> OrderBook:
        """Transform WebSocket book update to internal model.

        Args:
            raw: Validated raw WebSocket book update from Hyperliquid

        Returns:
            OrderBook domain model
        """
        ...

    @staticmethod
    def transform_ws_trade_event_to_internal(raw: HyperliquidRawWsTradeEvent) -> Trade:
        """Transform WebSocket trade event to internal model.

        Args:
            raw: Validated raw WebSocket trade event from Hyperliquid

        Returns:
            Trade domain model
        """
        ...


@runtime_checkable
class TradeMapperProtocol(MapperProtocol, Protocol):
    """Protocol for trade-related mapping operations.

    This protocol defines the interface for transforming raw trade data
    from the Hyperliquid API into internal Trade models.
    """

    @staticmethod
    def transform_raw_trade_to_internal(raw_trade: HyperliquidRawPublicTrade) -> Trade:
        """Transform raw trade data to internal model.

        Args:
            raw_trade: Raw trade data from API

        Returns:
            Trade domain model
        """
        ...


@runtime_checkable
class CandleMapperProtocol(MapperProtocol, Protocol):
    """Protocol for candle-related mapping operations.

    This protocol defines the interface for transforming raw candle data
    from the Hyperliquid API into internal Candle models.
    """

    @staticmethod
    def transform_raw_candle_to_internal(raw_candle: HyperliquidRawCandleSnapshot) -> Candle:
        """Transform raw candle data to internal model.

        Args:
            raw_candle: Raw candle data from API

        Returns:
            Candle domain model
        """
        ...

    @staticmethod
    def transform_ws_candle_to_internal(raw_ws_candle: HyperliquidRawWsCandle) -> Candle:
        """Transform WebSocket candle to internal Candle model.

        Args:
            raw_ws_candle: Validated WebSocket candle data from Hyperliquid

        Returns:
            Candle domain model
        """
        ...


@runtime_checkable
class FundingRateMapperProtocol(MapperProtocol, Protocol):
    """Protocol for funding rate mapping operations.

    This protocol defines the interface for transforming raw funding rate data
    from the Hyperliquid API into internal FundingRate models.
    """

    @staticmethod
    def transform_raw_funding_rate_to_internal(
        raw_funding_rate: HyperliquidRawFundingHistoryItem,
    ) -> FundingRate:
        """Transform raw funding rate data to internal model.

        Args:
            raw_funding_rate: Raw funding rate data from API

        Returns:
            FundingRate domain model
        """
        ...


@runtime_checkable
class MarketMapperProtocol(MapperProtocol, Protocol):
    """Protocol for market-related mapping operations.

    This protocol defines the interface for transforming raw market data
    from the Hyperliquid API into internal Market models.
    """

    @staticmethod
    def transform_single_asset_to_market(
        asset_def: "HyperliquidRawAssetDefinition", asset_ctx: HyperliquidRawAssetCtx | None = None
    ) -> Market:
        """Transform asset definition and context to market model.

        Args:
            asset_def: Asset definition with trading rules from meta response
            asset_ctx: Optional asset context with current pricing data

        Returns:
            Market domain model
        """
        ...


@runtime_checkable
class MarketDataMapperProtocol(MapperProtocol, Protocol):
    """Base protocol for market data mapping operations.

    This protocol defines common interface for all market data mappers.
    """

    @staticmethod
    def transform_raw_market_data_to_internal(raw_data: object) -> object:
        """Transform raw market data to internal model.

        Args:
            raw_data: Raw market data from API

        Returns:
            Appropriate market data domain model
        """
        ...


@runtime_checkable
class OrderResponseMapperProtocol(MapperProtocol, Protocol):
    """Protocol for order response mapping operations.

    This protocol defines the interface for transforming raw order response data
    from the Hyperliquid API into internal models.
    """

    @staticmethod
    async def map_place_order_response_to_order(
        processed_status: dict[str, Any],
        order_args: PlaceOrderArgs,
        timestamp: datetime,
    ) -> Order:
        """Map place order response to internal Order model.

        Args:
            processed_status: Processed order status data
            order_args: Original order placement arguments
            timestamp: Timestamp for the order

        Returns:
            Order domain model
        """
        ...

    @staticmethod
    def transform_resting_order_to_internal(
        resting_data: "HyperliquidRawExchangeStatusResting",
        order_args: PlaceOrderArgs,
    ) -> Order:
        """Transform raw resting order status to internal Order model.

        Args:
            resting_data: Raw resting order status from API
            order_args: Original order placement arguments

        Returns:
            Order domain model
        """
        ...

    @staticmethod
    def transform_filled_order_to_internal(
        filled_data: "HyperliquidRawExchangeStatusFilled",
        order_args: PlaceOrderArgs,
    ) -> Order:
        """Transform raw filled order status to internal Order model.

        Args:
            filled_data: Raw filled order status from API
            order_args: Original order placement arguments

        Returns:
            Order domain model
        """
        ...


@runtime_checkable
class TransactionMapperProtocol(MapperProtocol, Protocol):
    """Protocol for transaction/trade mapping operations.

    This protocol defines the interface for transforming raw transaction data
    from the Hyperliquid API into internal Trade models.
    """

    @staticmethod
    def transform_raw_user_fill_to_internal(raw_fill: HyperliquidRawUserFill) -> Trade:
        """Transform raw user fill data to internal model.

        Args:
            raw_fill: Raw user fill data from API

        Returns:
            Trade domain model
        """
        ...

    @staticmethod
    def transform_ws_fill_event_to_internal(raw_fill: HyperliquidRawWsFillEvent) -> Trade:
        """Transform WebSocket fill event to internal model.

        Args:
            raw_fill: Validated raw WebSocket fill event from Hyperliquid

        Returns:
            Trade domain model
        """
        ...


@runtime_checkable
class TradingEnumMapperProtocol(MapperProtocol, Protocol):
    """Protocol for trading enum mapping operations.

    This protocol defines the interface for mapping between exchange-specific
    and internal trading enumerations.
    """

    @staticmethod
    def map_order_side(raw_side: str) -> OrderSide:
        """Map raw order side to internal enum.

        Args:
            raw_side: Raw order side string from API

        Returns:
            Internal order side enum
        """
        ...

    @staticmethod
    def map_order_type(raw_type: str) -> OrderType:
        """Map raw order type to internal enum.

        Args:
            raw_type: Raw order type string from API

        Returns:
            Internal order type enum
        """
        ...

    @staticmethod
    def map_order_status(raw_status: str) -> OrderStatus:
        """Map raw order status to internal enum.

        Args:
            raw_status: Raw order status string from API

        Returns:
            Internal order status enum
        """
        ...


@runtime_checkable
class PriceTickerMapperProtocol(MapperProtocol, Protocol):
    """Protocol for price ticker mapping operations.

    This protocol defines the interface for transforming raw price ticker data
    from the Hyperliquid API into internal Ticker models.
    """

    @staticmethod
    def transform_raw_price_ticker_to_internal(raw_ticker: HyperliquidRawAssetCtx) -> Ticker:
        """Transform raw price ticker data to internal model.

        Args:
            raw_ticker: Raw price ticker data from API

        Returns:
            Ticker domain model
        """
        ...

    @staticmethod
    def transform_raw_asset_ctx_to_ticker(raw_asset_ctx: HyperliquidRawAssetCtx) -> Ticker:
        """Transform raw asset context data to internal ticker model.

        Args:
            raw_asset_ctx: Raw asset context data from API

        Returns:
            Ticker domain model
        """
        ...

    @staticmethod
    def transform_raw_all_mids_to_internal(raw_all_mids: HyperliquidRawAllMids) -> MidPrices:
        """Transform raw all mids data to internal model.

        Args:
            raw_all_mids: Raw all mids data from API

        Returns:
            MidPrices domain model
        """
        ...


@runtime_checkable
class HistoricalDataMapperProtocol(MapperProtocol, Protocol):
    """Protocol for historical data mapping operations.

    This protocol defines the interface for transforming raw historical data
    from the Hyperliquid API into internal models.
    """

    @staticmethod
    def transform_raw_candle_to_internal(raw_candle: HyperliquidRawCandleSnapshot) -> Candle:
        """Transform raw candle data to internal model.

        Args:
            raw_candle: Raw candle data from API

        Returns:
            Candle domain model
        """
        ...

    @staticmethod
    def transform_ws_candle_to_internal(raw_ws_candle: HyperliquidRawWsCandle) -> Candle:
        """Transform WebSocket candle to internal Candle model.

        Args:
            raw_ws_candle: Validated WebSocket candle data from Hyperliquid

        Returns:
            Candle domain model
        """
        ...

    @staticmethod
    def transform_raw_funding_history_to_internal(
        raw_funding: HyperliquidRawFundingHistoryItem,
    ) -> FundingRate:
        """Transform raw funding history data to internal model.

        Args:
            raw_funding: Raw funding history data from API

        Returns:
            FundingRate domain model
        """
        ...

    @staticmethod
    def transform_raw_funding_history_item_to_internal(
        raw_funding_item: HyperliquidRawFundingHistoryItem,
    ) -> FundingRate:
        """Transform raw funding history item data to internal model.

        Args:
            raw_funding_item: Raw funding history item data from API

        Returns:
            FundingRate domain model
        """
        ...

    @staticmethod
    def transform_raw_asset_ctx_to_funding_rate(
        raw_asset_ctx: HyperliquidRawAssetCtx,
    ) -> FundingRate | None:
        """Transform raw asset context data to internal funding rate model.

        Args:
            raw_asset_ctx: Raw asset context data from API

        Returns:
            FundingRate domain model or None if no funding data
        """
        ...

    @staticmethod
    def transform_raw_candle_snapshot_to_candles(
        raw_snapshot: HyperliquidRawCandleSnapshot,
        symbol: str,
        interval: str,
    ) -> list[Candle]:
        """Transform raw candle snapshot data to internal candle models.

        Args:
            raw_snapshot: Raw candle snapshot data from API
            symbol: Symbol for the candles
            interval: Time interval for the candles

        Returns:
            List of Candle domain models
        """
        ...


@runtime_checkable
class MarketMetadataMapperProtocol(MapperProtocol, Protocol):
    """Protocol for market metadata mapping operations.

    This protocol defines the interface for transforming raw market metadata
    from the Hyperliquid API into internal Market models.
    """

    @staticmethod
    def transform_raw_meta_and_asset_ctxs_to_markets(
        raw_meta_and_asset_ctxs: HyperliquidRawMetaAndAssetCtxsResponse,
    ) -> list[Market]:
        """Transform raw meta and asset contexts to internal Market models.

        Args:
            raw_meta_and_asset_ctxs: Raw response containing asset definitions and contexts

        Returns:
            List of Market objects with metadata for all assets
        """
        ...

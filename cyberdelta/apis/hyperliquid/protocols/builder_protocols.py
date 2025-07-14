"""Request builder protocol definitions for Hyperliquid API.

This module defines the request builder protocols that specify the interfaces
for building various types of API requests. Uses proper Pydantic models for
type safety and runtime validation instead of loose dict[str, object] types.
"""

from decimal import Decimal
from typing import Protocol, runtime_checkable

from eth_typing import ChecksumAddress

from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import (
    HyperliquidRawAllMidsRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiCancelOrderRequest,
    HyperliquidApiPlaceOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshotRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
)

# Import proper Pydantic models for type-safe returns
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawHistoricalOrdersRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order_status import (
    HyperliquidRawOrderStatusRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawL2BookRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawRecentTradesRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFillsRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawUserStateRequestPayload,
)
from cyberdelta.apis.hyperliquid.protocols.base_protocols import RequestBuilderProtocol
from cyberdelta.apis.models.service_args_models import (
    GetCandleSnapshotArgs,
    GetHistoricalFundingRatesArgs,
    GetL2BookArgs,
    GetOpenOrdersArgs,
    GetRecentTradesArgs,
    GetUserFillsArgs,
    GetUserStateArgs,
    HyperliquidGetOrderStatusArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.models.enums import OrderSide, OrderType


@runtime_checkable
class AccountRequestBuilderProtocol(RequestBuilderProtocol, Protocol):
    """Protocol for account-related request building.

    This protocol defines the interface for building account-related API requests
    such as balance queries, position queries, and clearinghouse state requests.
    """

    @staticmethod
    def build_get_user_state_params(user: ChecksumAddress) -> HyperliquidRawUserStateRequestPayload:
        """Build parameters for user state retrieval.

        Args:
            user: The user address to query

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_user_state_payload(
        args: GetUserStateArgs,
    ) -> HyperliquidRawUserStateRequestPayload:
        """Build parameters for user state retrieval using service args.

        Args:
            args: Validated user state arguments containing wallet address

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_get_clearinghouse_state_params(
        user: ChecksumAddress,
    ) -> HyperliquidRawUserStateRequestPayload:
        """Build parameters for clearinghouse state retrieval.

        Args:
            user: The user address to query

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_get_open_orders_params(
        user: ChecksumAddress,
    ) -> HyperliquidRawOpenOrdersRequestPayload:
        """Build parameters for open orders retrieval.

        Args:
            user: The user address to query

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_get_user_fills_params(user: ChecksumAddress) -> HyperliquidRawUserFillsRequestPayload:
        """Build parameters for user fills retrieval.

        Args:
            user: The user address to query

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_historical_orders_payload(
        wallet_address: str,
    ) -> HyperliquidRawHistoricalOrdersRequestPayload:
        """Build the payload for fetching historical orders.

        Args:
            wallet_address: The wallet address to query

        Returns:
            Validated Pydantic model containing historical orders query payload
        """
        ...


@runtime_checkable
class TradingRequestBuilderProtocol(RequestBuilderProtocol, Protocol):
    """Protocol for trading-related request building.

    This protocol defines the interface for building trading-related API requests
    such as order placement, cancellation, and modification.
    """

    @staticmethod
    def build_place_order_payload(
        symbol: str,
        order_type: OrderType,
        order_side: OrderSide,
        quantity: Decimal,
        price: Decimal | None = None,
        reduce_only: bool = False,
        vault_address: ChecksumAddress | None = None,
    ) -> HyperliquidApiPlaceOrderRequest:
        """Build order placement payload.

        Args:
            symbol: The trading symbol
            order_type: Type of order (limit, market, etc.)
            order_side: Side of order (buy/sell)
            quantity: Order quantity
            price: Order price (None for market orders)
            reduce_only: Whether this is a reduce-only order
            vault_address: Optional vault address for vault trading

        Returns:
            Validated Pydantic model containing order placement payload
        """
        ...

    @staticmethod
    def build_cancel_order_payload(order_id: str, symbol: str) -> HyperliquidApiCancelOrderRequest:
        """Build order cancellation payload.

        Args:
            order_id: The order ID to cancel
            symbol: The trading symbol

        Returns:
            Validated Pydantic model containing order cancellation payload
        """
        ...

    @staticmethod
    def build_cancel_all_orders_payload(
        symbol: str | None = None,
    ) -> HyperliquidApiCancelOrderRequest:
        """Build cancel all orders payload.

        Args:
            symbol: Optional symbol to cancel orders for (None for all symbols)

        Returns:
            Validated Pydantic model containing cancel all orders payload
        """
        ...

    @staticmethod
    def build_modify_order_payload(
        order_id: str, symbol: str, quantity: Decimal | None = None, price: Decimal | None = None
    ) -> HyperliquidApiPlaceOrderRequest:
        """Build order modification payload.

        Note: Hyperliquid doesn't have direct order modification - this is implemented
        as cancel + place operation.

        Args:
            order_id: The order ID to modify
            symbol: The trading symbol
            quantity: New quantity (None to keep current)
            price: New price (None to keep current)

        Returns:
            Validated Pydantic model containing order modification payload
        """
        ...

    @staticmethod
    def build_order_status_payload(
        args: HyperliquidGetOrderStatusArgs,
    ) -> HyperliquidRawOrderStatusRequestPayload:
        """Build the payload for querying the status of a specific order.

        Args:
            args: Validated order status arguments containing wallet address and order ID

        Returns:
            Validated Pydantic model containing order status query payload
        """
        ...

    @staticmethod
    def build_open_orders_payload(
        args: GetOpenOrdersArgs,
    ) -> HyperliquidRawOpenOrdersRequestPayload:
        """Build the payload for fetching open orders.

        Args:
            args: Validated open orders arguments containing wallet address

        Returns:
            Validated Pydantic model containing open orders query payload
        """
        ...

    def build_place_order_request(
        self,
        _orders: list[PlaceOrderArgs],
        orders_with_indices: list[tuple[PlaceOrderArgs, int]],
        tif_mapping: dict[str, str | None] | None,
    ) -> HyperliquidApiPlaceOrderRequest:
        """Build place order request payload.

        Wrapper around build_batch_place_order_payload for compatibility with
        the decomposed trading services.

        Args:
            _orders: List of order placement arguments (unused, for compatibility)
            orders_with_indices: List of (PlaceOrderArgs, asset_index) tuples
            tif_mapping: Time-in-force mapping

        Returns:
            HyperliquidApiPlaceOrderRequest: Validated request payload model
        """
        ...

    def build_batch_place_order_payload(
        self,
        orders: list[PlaceOrderArgs],
        orders_with_indices: list[tuple[PlaceOrderArgs, int]],
        tif_mapping: dict[str, str | None] | None,
    ) -> HyperliquidApiPlaceOrderRequest:
        """Build batch place order request payload.

        Args:
            orders: List of order placement arguments
            orders_with_indices: List of (PlaceOrderArgs, asset_index) tuples
            tif_mapping: Time-in-force mapping

        Returns:
            HyperliquidApiPlaceOrderRequest: Validated request payload model
        """
        ...

    def build_batch_cancel_order_payload(
        self,
        cancel_requests: list[tuple[str, int, str]],
    ) -> HyperliquidApiCancelOrderRequest:
        """Build batch cancel order request payload.

        Args:
            cancel_requests: List of (order_id, asset_index, symbol) tuples

        Returns:
            HyperliquidApiCancelOrderRequest: Validated request payload model
        """
        ...

    @staticmethod
    def build_user_fills_request_payload(
        args: GetUserFillsArgs,
    ) -> HyperliquidRawUserFillsRequestPayload:
        """Build the payload for fetching user fills/trades.

        Args:
            args: Validated user fills arguments containing wallet address

        Returns:
            Validated Pydantic model containing user fills query payload
        """
        ...


@runtime_checkable
class MarketDataRequestBuilderProtocol(RequestBuilderProtocol, Protocol):
    """Protocol for market data request building.

    This protocol defines the interface for building market data requests
    such as ticker queries, order book queries, and historical data requests.
    """

    @staticmethod
    def build_info_request_payload() -> HyperliquidRawMetaAndAssetCtxsRequestPayload:
        """Build the request payload for meta and asset contexts.

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_all_mids_request_payload() -> HyperliquidRawAllMidsRequestPayload:
        """Build the request payload for all mid prices.

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_l2_book_request_payload(args: GetL2BookArgs) -> HyperliquidRawL2BookRequestPayload:
        """Build the request payload for L2 order book data.

        Args:
            args: Validated GetL2BookArgs containing symbol

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_recent_trades_request_payload(
        args: GetRecentTradesArgs,
    ) -> HyperliquidRawRecentTradesRequestPayload:
        """Build the request payload for recent public trades.

        Args:
            args: Validated GetRecentTradesArgs containing symbol

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_candle_snapshot_payload(
        args: GetCandleSnapshotArgs,
    ) -> HyperliquidRawCandleSnapshotRequestPayload:
        """Build the request payload for candle/OHLCV data.

        Args:
            args: Validated GetCandleSnapshotArgs containing candle parameters

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_historical_funding_rates_payload(
        args: GetHistoricalFundingRatesArgs,
    ) -> HyperliquidRawFundingHistoryRequestPayload:
        """Build the request payload for historical funding rates.

        Args:
            args: Validated GetHistoricalFundingRatesArgs containing funding rate parameters

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    # Keep old method names for backward compatibility
    @staticmethod
    def build_get_all_mids_params() -> HyperliquidRawAllMidsRequestPayload:
        """Build parameters for all mids (mid prices) retrieval.

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_get_l2_book_params(symbol: str) -> HyperliquidRawL2BookRequestPayload:
        """Build parameters for L2 order book retrieval.

        Args:
            symbol: The trading symbol

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_get_recent_trades_params(symbol: str) -> HyperliquidRawRecentTradesRequestPayload:
        """Build parameters for recent trades retrieval.

        Args:
            symbol: The trading symbol

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_get_candles_params(
        symbol: str, interval: str, start_time: int | None = None, end_time: int | None = None
    ) -> HyperliquidRawCandleSnapshotRequestPayload:
        """Build parameters for candle data retrieval.

        Args:
            symbol: The trading symbol
            interval: Candle interval (e.g., "1m", "5m", "1h")
            start_time: Optional start time (timestamp)
            end_time: Optional end time (timestamp)

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_get_funding_history_params(
        symbol: str, start_time: int | None = None, end_time: int | None = None
    ) -> HyperliquidRawFundingHistoryRequestPayload:
        """Build parameters for funding history retrieval.

        Args:
            symbol: The trading symbol
            start_time: Optional start time (timestamp)
            end_time: Optional end time (timestamp)

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

    @staticmethod
    def build_get_meta_params() -> HyperliquidRawMetaAndAssetCtxsRequestPayload:
        """Build parameters for meta information retrieval.

        Returns:
            Validated Pydantic model containing request parameters
        """
        ...

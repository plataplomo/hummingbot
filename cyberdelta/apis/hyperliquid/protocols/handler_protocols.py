"""Response handler protocol definitions for Hyperliquid API.

This module defines the response handler protocols that specify the interfaces
for handling various types of API responses. Following the Backpack pattern,
all handler protocols inherit from the base ResponseHandlerProtocol.
"""

from collections.abc import Mapping
from typing import Protocol, runtime_checkable

from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersResponse,
    HyperliquidRawOrderStatusResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawL2Book as HyperliquidRawOrderBookResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawPublicTrade,
    HyperliquidRawRecentTradesResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFillsResponse
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState
from cyberdelta.apis.hyperliquid.protocols.base_protocols import ResponseHandlerProtocol
from cyberdelta.utils.typing import ParsedJsonResponse


@runtime_checkable
class AccountResponseHandlerProtocol(ResponseHandlerProtocol, Protocol):
    """Protocol for account response handling.

    This protocol defines the interface for handling account-related API responses
    such as user state, clearinghouse state, and position data.
    """

    @staticmethod
    def handle_get_user_state_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawClearinghouseState:
        """Handle user state response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing user state data
        """
        ...

    @staticmethod
    def handle_get_clearinghouse_state_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawClearinghouseState:
        """Handle clearinghouse state response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing clearinghouse state data
        """
        ...

    @staticmethod
    def handle_get_open_orders_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawOpenOrdersResponse:
        """Handle open orders response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing order data
        """
        ...

    @staticmethod
    def handle_get_user_fills_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawUserFillsResponse:
        """Handle user fills response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing fill data
        """
        ...

    def handle_info_user_fills_response(
        self,
        raw_data: dict[str, object],
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawUserFillsResponse:
        """Handle user fills info response.

        Args:
            raw_data: Raw response data from API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated Pydantic model containing fill data
        """
        ...

    def handle_historical_orders_response(
        self,
        raw_data: ParsedJsonResponse,
        wallet_address: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[HyperliquidRawHistoricalOrderResponse]:
        """Handle historical orders response.

        Args:
            raw_data: Raw response data from API
            wallet_address: Wallet address for the query
            status_code: HTTP status code
            headers: Response headers

        Returns:
            List of validated historical order responses
        """
        ...


@runtime_checkable
class TradingResponseHandlerProtocol(ResponseHandlerProtocol, Protocol):
    """Protocol for trading response handling.

    This protocol defines the interface for handling trading-related API responses
    such as order placement, cancellation, and modification confirmations.
    """

    @staticmethod
    def handle_place_order_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawExchangeResponse:
        """Handle order placement response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing order placement result
        """
        ...

    @staticmethod
    def handle_cancel_order_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawExchangeResponse:
        """Handle order cancellation response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing order cancellation result
        """
        ...

    @staticmethod
    def handle_cancel_all_orders_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawExchangeResponse:
        """Handle cancel all orders response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing cancel all orders result
        """
        ...

    @staticmethod
    def handle_modify_order_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawExchangeResponse:
        """Handle order modification response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing order modification result
        """
        ...

    def handle_info_order_status_response(
        self,
        raw_data: dict[str, object],
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawOrderStatusResponse:
        """Handle order status query response.

        Args:
            raw_data: Raw response data from API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated Pydantic model containing order status data
        """
        ...

    def handle_info_open_orders_response(
        self,
        raw_data: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawOpenOrdersResponse:
        """Handle open orders query response.

        Args:
            raw_data: Raw response data from API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated Pydantic model containing open orders data
        """
        ...

    def handle_exchange_response(
        self,
        raw_response_content: dict[str, object],
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawExchangeResponse:
        """Handle generic exchange response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated Pydantic model containing exchange response data
        """
        ...


@runtime_checkable
class MarketDataResponseHandlerProtocol(ResponseHandlerProtocol, Protocol):
    """Protocol for market data response handling.

    This protocol defines the interface for handling market data API responses
    such as ticker data, order book data, and historical data.
    """

    @staticmethod
    def handle_get_all_mids_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawAllMids:
        """Handle all mids (mid prices) response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing mid prices data
        """
        ...

    @staticmethod
    def handle_get_l2_book_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawOrderBookResponse:
        """Handle L2 order book response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing order book data
        """
        ...

    @staticmethod
    def handle_get_recent_trades_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawRecentTradesResponse:
        """Handle recent trades response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing trade data
        """
        ...

    @staticmethod
    def handle_get_candles_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawCandleSnapshot:
        """Handle candle data response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing candle data
        """
        ...

    @staticmethod
    def handle_get_funding_history_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawFundingHistoryResponse:
        """Handle funding history response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing funding history data
        """
        ...

    @staticmethod
    def handle_get_meta_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Handle meta information response.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing meta information
        """
        ...

    def handle_info_meta_and_asset_ctxs_response(
        self,
        raw_data: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Handle info meta and asset contexts response.

        Args:
            raw_data: Raw response data from API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated Pydantic model containing meta and asset contexts
        """
        ...

    def handle_all_mids_response(
        self,
        raw_data: dict[str, object],
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawAllMids:
        """Handle all mids response.

        Args:
            raw_data: Raw response data from API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated Pydantic model containing all mid prices
        """
        ...

    def handle_info_l2_book_response(
        self,
        raw_data: dict[str, object],
        symbol: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawOrderBookResponse:
        """Handle info L2 book response.

        Args:
            raw_data: Raw response data from API
            symbol: The trading symbol
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated Pydantic model containing L2 book data
        """
        ...

    def handle_info_recent_trades_response(
        self,
        raw_data: ParsedJsonResponse,
        coin: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[HyperliquidRawPublicTrade]:
        """Handle info recent trades response.

        Args:
            raw_data: Raw response data from API
            coin: The coin/symbol
            status_code: HTTP status code
            headers: Response headers

        Returns:
            List of validated public trade data
        """
        ...

    def handle_info_candle_snapshot_response(
        self,
        raw_data: ParsedJsonResponse,
        symbol: str,
        interval: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawCandleSnapshot:
        """Handle info candle snapshot response.

        Args:
            raw_data: Raw response data from API
            symbol: The trading symbol
            interval: The candle interval
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated Pydantic model containing candle snapshot data
        """
        ...

    def handle_historical_funding_rates_response(
        self,
        raw_data: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawFundingHistoryResponse:
        """Handle historical funding rates response.

        Args:
            raw_data: Raw response data from API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            Validated Pydantic model containing historical funding rates
        """
        ...

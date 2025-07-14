"""Protocol definitions for response handler components."""

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralResponse
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFill
from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRate,
)
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_limits import (
    BackpackRawMaxBorrowQuantity,
    BackpackRawMaxOrderQuantity,
    BackpackRawMaxWithdrawalQuantity,
)
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawMarket,
    BackpackRawOrderBook,
    BackpackRawTicker,
)
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawPublicTrade,
    BackpackRawRecentPublicTrade,
)
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.backpack.protocols.base_protocols import ResponseHandlerProtocol
from cyberdelta.core.models import CancelOrderResult


# Type alias for raw JSON responses
RawJsonResponse = dict[str, Any]


__all__ = [
    "AccountResponseHandlerProtocol",
    "MarketDataResponseHandlerProtocol",
    "TradingResponseHandlerProtocol",
]


@runtime_checkable
class AccountResponseHandlerProtocol(ResponseHandlerProtocol, Protocol):
    """Protocol for account response handler components.

    Defines the interface for handling account-related API responses.
    Inherits from ResponseHandlerProtocol for base handler compliance.
    """

    @staticmethod
    def handle_get_balances_response(
        raw_response_content: RawJsonResponse, status_code: int
    ) -> dict[str, BackpackRawBalance]:
        """Validate GET /capital endpoint response."""
        ...

    @staticmethod
    def handle_get_positions_response(
        raw_response_content: dict[str, Any] | list[Any], symbol: str | None, status_code: int
    ) -> list[BackpackRawPosition]:
        """Validate positions response."""
        ...

    @staticmethod
    def handle_get_account_info_response(
        raw_response_content: RawJsonResponse, status_code: int
    ) -> BackpackRawAccountSummary:
        """Validate account summary information."""
        ...

    @staticmethod
    def handle_withdraw_response(
        raw_response_content: RawJsonResponse, status_code: int
    ) -> BackpackRawWithdrawalResponse:
        """Validate withdrawal operation response."""
        ...

    @staticmethod
    def handle_transfer_response(
        raw_response_content: dict[str, Any] | list[Any] | str, status_code: int
    ) -> dict[str, Any] | list[Any] | str:
        """Validate internal capital transfer response."""
        ...

    @staticmethod
    def handle_get_collateral_response(
        raw_response_content: dict[str, Any] | list[Any] | str,
        subaccount_id: int | None,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawCollateralResponse:
        """Validate GET /api/v1/capital/collateral response."""
        ...

    @staticmethod
    def handle_max_borrow_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMaxBorrowQuantity:
        """INTERNAL USE ONLY: Validate max borrow quantity limits."""
        ...

    @staticmethod
    def handle_max_order_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        side: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMaxOrderQuantity:
        """INTERNAL USE ONLY: Validate max order quantity limits."""
        ...

    @staticmethod
    def handle_max_withdrawal_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMaxWithdrawalQuantity:
        """INTERNAL USE ONLY: Validate max withdrawal quantity limits."""
        ...


@runtime_checkable
class MarketDataResponseHandlerProtocol(ResponseHandlerProtocol, Protocol):
    """Protocol for market data response handler components.

    Defines the interface for handling market data API responses.
    Inherits from ResponseHandlerProtocol for base handler compliance.
    """

    @staticmethod
    def handle_get_ticker_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawTicker:
        """Validate ticker data for a specific symbol."""
        ...

    @staticmethod
    def handle_get_order_book_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawOrderBook:
        """Validate order book data with bids and asks."""
        ...

    @staticmethod
    def handle_get_recent_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawRecentPublicTrade]:
        """Validate list of recent public trades."""
        ...

    @staticmethod
    def handle_get_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawFundingRate:
        """Validate funding rate data."""
        ...

    @staticmethod
    def handle_get_markets_response(
        raw_response_content: RawJsonResponse, status_code: int
    ) -> list[BackpackRawMarket]:
        """Validate list of all available markets."""
        ...

    @staticmethod
    def handle_get_market_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMarket:
        """Validate single market information."""
        ...

    @staticmethod
    def handle_get_market_data_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        timeframe: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawKline]:
        """Validate historical kline/candlestick data."""
        ...

    @staticmethod
    def handle_get_historical_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawPublicTrade]:
        """Validate historical public trade data."""
        ...

    @staticmethod
    def handle_get_current_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawFundingRate:
        """Validate current funding rate for a symbol."""
        ...

    @staticmethod
    def handle_get_historical_funding_rates_response(
        raw_response_content: RawJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawFundingIntervalRate]:
        """Validate historical funding rates."""
        ...


@runtime_checkable
class TradingResponseHandlerProtocol(ResponseHandlerProtocol, Protocol):
    """Protocol for trading response handler components.

    Defines the interface for handling trading-related API responses.
    Inherits from ResponseHandlerProtocol for base handler compliance.
    """

    @staticmethod
    def handle_place_order_response(
        raw_response_content: RawJsonResponse, status_code: int
    ) -> BackpackRawOrder:
        """Validate order placement response."""
        ...

    @staticmethod
    def handle_cancel_order_response(
        raw_response_content: RawJsonResponse, order_id: str, symbol: str
    ) -> CancelOrderResult:
        """Validate order cancellation response."""
        ...

    @staticmethod
    def handle_get_open_orders_response(
        raw_response_content: list[Any], symbol: str | None, status_code: int
    ) -> list[BackpackRawOrder]:
        """Validate list of open orders."""
        ...

    @staticmethod
    def handle_get_order_history_response(
        raw_response_content: RawJsonResponse, symbol: str | None, status_code: int
    ) -> list[BackpackRawOrder]:
        """Validate historical order data."""
        ...

    @staticmethod
    def handle_get_trade_history_response(
        raw_response_content: RawJsonResponse, symbol: str | None, status_code: int
    ) -> list[BackpackRawPublicTrade]:
        """Validate user's trade history."""
        ...

    @staticmethod
    def handle_get_fills_response(
        raw_response_content: RawJsonResponse, symbol: str | None, status_code: int
    ) -> list[BackpackRawFill]:
        """Validate fills history."""
        ...

    @staticmethod
    def handle_get_order_status_response(
        raw_response_content: RawJsonResponse, identifier: str, status_code: int
    ) -> BackpackRawOrder:
        """Validate single order status query."""
        ...

    @staticmethod
    def handle_cancel_all_orders_response(
        raw_response_content: list[Any], symbol: str | None, status_code: int
    ) -> list[BackpackRawOrder]:
        """Validate response from DELETE /api/v1/orders/cancelAll."""
        ...

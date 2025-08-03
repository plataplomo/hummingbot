"""Protocol definitions for response handler components."""

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalanceResponse
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummaryResponse
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralResponse
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFillResponse
from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRateResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKlineResponse
from cyberdelta.apis.backpack.models.bp_raw_limits import (
    BackpackRawMaxBorrowQuantity,
    BackpackRawMaxOrderQuantity,
    BackpackRawMaxWithdrawalQuantity,
)
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawMarketResponse,
    BackpackRawOrderBook,
    BackpackRawTickerResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPositionResponse
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawPublicTrade,
    BackpackRawRecentPublicTrade,
)
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.base.protocols.base_protocols import ResponseHandlerProtocol
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.models import CancelOrderResult


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
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> dict[str, BackpackRawBalanceResponse]:
        """Validate GET /capital endpoint response."""
        ...

    @staticmethod
    def handle_get_positions_response(
        raw_response_content: dict[str, Any] | list[Any],
        symbol: Symbol | None,
        status_code: int,
    ) -> list[BackpackRawPositionResponse]:
        """Validate positions response."""
        ...

    @staticmethod
    def handle_get_account_info_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> BackpackRawAccountSummaryResponse:
        """Validate account summary information."""
        ...

    @staticmethod
    def handle_withdraw_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> BackpackRawWithdrawalResponse:
        """Validate withdrawal operation response."""
        ...

    @staticmethod
    def handle_transfer_response(
        raw_response_content: dict[str, Any] | list[Any] | str,
        status_code: int,
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
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMaxBorrowQuantity:
        """INTERNAL USE ONLY: Validate max borrow quantity limits."""
        ...

    @staticmethod
    def handle_max_order_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        side: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMaxOrderQuantity:
        """INTERNAL USE ONLY: Validate max order quantity limits."""
        ...

    @staticmethod
    def handle_max_withdrawal_quantity_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
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
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawTickerResponse:
        """Validate ticker data for a specific symbol."""
        ...

    @staticmethod
    def handle_get_order_book_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawOrderBook:
        """Validate order book data with bids and asks."""
        ...

    @staticmethod
    def handle_get_recent_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawRecentPublicTrade]:
        """Validate list of recent public trades."""
        ...

    @staticmethod
    def handle_get_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawFundingRateResponse:
        """Validate funding rate data."""
        ...

    @staticmethod
    def handle_get_markets_response(
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> list[BackpackRawMarketResponse]:
        """Validate list of all available markets."""
        ...

    @staticmethod
    def handle_get_market_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawMarketResponse:
        """Validate single market information."""
        ...

    @staticmethod
    def handle_get_market_data_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        timeframe: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawKlineResponse]:
        """Validate historical kline/candlestick data."""
        ...

    @staticmethod
    def handle_get_historical_trades_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[BackpackRawPublicTrade]:
        """Validate historical public trade data."""
        ...

    @staticmethod
    def handle_get_current_funding_rate_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
        status_code: int,
        headers: Mapping[str, str],
    ) -> BackpackRawFundingRateResponse:
        """Validate current funding rate for a symbol."""
        ...

    @staticmethod
    def handle_get_historical_funding_rates_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol,
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
        raw_response_content: RawJsonResponse,
        status_code: int,
    ) -> BackpackRawOrderResponse:
        """Validate order placement response."""
        ...

    @staticmethod
    def handle_cancel_order_response(
        raw_response_content: RawJsonResponse,
        order_id: str,
        symbol: Symbol,
    ) -> CancelOrderResult:
        """Validate order cancellation response."""
        ...

    @staticmethod
    def handle_get_open_orders_response(
        raw_response_content: list[Any],
        symbol: Symbol | None,
        status_code: int,
    ) -> list[BackpackRawOrderResponse]:
        """Validate list of open orders."""
        ...

    @staticmethod
    def handle_get_order_history_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol | None,
        status_code: int,
    ) -> list[BackpackRawOrderResponse]:
        """Validate historical order data."""
        ...

    @staticmethod
    def handle_get_trade_history_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol | None,
        status_code: int,
    ) -> list[BackpackRawPublicTrade]:
        """Validate user's trade history."""
        ...

    @staticmethod
    def handle_get_fills_response(
        raw_response_content: RawJsonResponse,
        symbol: Symbol | None,
        status_code: int,
    ) -> list[BackpackRawFillResponse]:
        """Validate fills history."""
        ...

    @staticmethod
    def handle_get_order_status_response(
        raw_response_content: RawJsonResponse,
        identifier: str,
        status_code: int,
    ) -> BackpackRawOrderResponse:
        """Validate single order status query."""
        ...

    @staticmethod
    def handle_cancel_all_orders_response(
        raw_response_content: list[Any],
        symbol: Symbol | None,
        status_code: int,
    ) -> list[BackpackRawOrderResponse]:
        """Validate response from DELETE /api/v1/orders/cancelAll."""
        ...

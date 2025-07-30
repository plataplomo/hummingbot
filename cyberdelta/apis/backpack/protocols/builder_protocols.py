"""Protocol definitions for request builder components."""

from decimal import Decimal
from typing import Literal, Protocol, runtime_checkable

from cyberdelta.apis.backpack.models.bp_raw_api_request_payloads import (
    BackpackRawAccountConvertDustRequest,
    BackpackRawAccountWithdrawalRequest,
    BackpackRawBorrowLendExecuteRequest,
    BackpackRawInternalTransferRequest,
    BackpackRawOrderCancelAllRequest,
    BackpackRawOrderCancelRequest,
    BackpackRawOrderExecuteRequest,
    BackpackRawUpdateAccountSettingsRequest,
)
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralQueryParams
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetAccountInfoParams,
    BackpackRawGetBalancesParams,
    BackpackRawGetFundingRateParams,
    BackpackRawGetHistoricalFundingRatesParams,
    BackpackRawGetHistoricalTradesParams,
    BackpackRawGetMarketDataParams,
    BackpackRawGetMarketParams,
    BackpackRawGetMarketsParams,
    BackpackRawGetOpenOrdersParams,
    BackpackRawGetOrderBookParams,
    BackpackRawGetOrderHistoryParams,
    BackpackRawGetOrderParams,
    BackpackRawGetPositionsParams,
    BackpackRawGetRecentTradesParams,
    BackpackRawGetTickerParams,
    BackpackRawGetTradeHistoryParams,
    BackpackRawMaxBorrowQuantityParams,
    BackpackRawMaxOrderQuantityParams,
    BackpackRawMaxWithdrawalQuantityParams,
)
from cyberdelta.apis.backpack.protocols.base_protocols import RequestBuilderProtocol
from cyberdelta.apis.base.trading_execution_domain import AccountSettings, OrderExecution
from cyberdelta.apis.models.service_args.internal import (
    GetMaxBorrowQuantityArgs,
    GetMaxOrderQuantityArgs,
    GetMaxWithdrawalQuantityArgs,
)
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import OrderSide, OrderType, TimeInForce


__all__ = [
    "AccountRequestBuilderProtocol",
    "MarketDataRequestBuilderProtocol",
    "TradingRequestBuilderProtocol",
]


@runtime_checkable
class AccountRequestBuilderProtocol(RequestBuilderProtocol, Protocol):
    """Protocol for account request builder components.

    Defines the interface for building account-related API requests.
    Inherits from RequestBuilderProtocol for base builder compliance.
    """

    @staticmethod
    def build_get_balances_params() -> BackpackRawGetBalancesParams:
        """Build query parameters for fetching account balances."""
        ...

    @staticmethod
    def build_get_positions_params(symbol: Symbol | None) -> BackpackRawGetPositionsParams:
        """Build query parameters for fetching account positions."""
        ...

    @staticmethod
    def build_get_account_info_params() -> BackpackRawGetAccountInfoParams:
        """Build query parameters for fetching account information."""
        ...

    @staticmethod
    def build_withdraw_payload(
        asset_symbol: Symbol,
        network: str,
        address: str,
        amount: Decimal,
        transaction_priority: str | None = None,
        tag: str | None = None,
        client_withdraw_id: str | None = None,
    ) -> BackpackRawAccountWithdrawalRequest:
        """Build request payload for withdrawing assets."""
        ...

    @staticmethod
    def build_internal_transfer_payload(
        asset_symbol: Symbol,
        from_wallet: Literal["SPOT", "MARGIN", "FUTURES"],
        to_wallet: Literal["SPOT", "MARGIN", "FUTURES"],
        amount: Decimal,
        sub_account_id: str | None = None,
    ) -> BackpackRawInternalTransferRequest:
        """Build request payload for internal transfers."""
        ...

    @staticmethod
    def build_convert_dust_payload(asset_symbol: Symbol) -> BackpackRawAccountConvertDustRequest:
        """Build request payload for converting dust to USDC."""
        ...

    @staticmethod
    def build_borrow_lend_payload(
        operation: Literal["BORROW", "REPAY", "LEND", "REDEEM"],
        asset_symbol: Symbol,
        amount: Decimal,
    ) -> BackpackRawBorrowLendExecuteRequest:
        """Build request payload for borrowing/lending operations."""
        ...

    @staticmethod
    def build_update_account_settings_payload(
        leverage: int | None = None,
        account_settings: AccountSettings | None = None,
        margin_account_type: Literal["STANDARD", "PORTFOLIO"] | None = None,
    ) -> BackpackRawUpdateAccountSettingsRequest:
        """Build request payload for updating account settings."""
        ...

    @staticmethod
    def build_collateral_query_params(
        sub_account_id: str | None = None,
    ) -> BackpackRawCollateralQueryParams:
        """Build query parameters for fetching collateral information."""
        ...

    @staticmethod
    def build_max_borrow_quantity_params(
        args: GetMaxBorrowQuantityArgs,
    ) -> BackpackRawMaxBorrowQuantityParams:
        """Build query parameters for fetching maximum borrow quantity."""
        ...

    @staticmethod
    def build_max_order_quantity_params(
        args: GetMaxOrderQuantityArgs,
    ) -> BackpackRawMaxOrderQuantityParams:
        """Build query parameters for fetching maximum order quantity."""
        ...

    @staticmethod
    def build_max_withdrawal_quantity_params(
        args: GetMaxWithdrawalQuantityArgs,
    ) -> BackpackRawMaxWithdrawalQuantityParams:
        """Build query parameters for fetching maximum withdrawal quantity."""
        ...


@runtime_checkable
class MarketDataRequestBuilderProtocol(RequestBuilderProtocol, Protocol):
    """Protocol for market data request builder components.

    Defines the interface for building market data API requests.
    Inherits from RequestBuilderProtocol for base builder compliance.
    """

    @staticmethod
    def build_get_ticker_params(symbol: Symbol) -> BackpackRawGetTickerParams:
        """Build query parameters for fetching ticker data."""
        ...

    @staticmethod
    def build_get_order_book_params(
        symbol: Symbol,
        depth: int | None = None,
    ) -> BackpackRawGetOrderBookParams:
        """Build query parameters for fetching order book data."""
        ...

    @staticmethod
    def build_get_recent_trades_params(
        symbol: Symbol,
        limit: int | None = None,
    ) -> BackpackRawGetRecentTradesParams:
        """Build query parameters for fetching recent trades."""
        ...

    @staticmethod
    def build_get_markets_params() -> BackpackRawGetMarketsParams:
        """Build query parameters for fetching all markets."""
        ...

    @staticmethod
    def build_get_market_params(symbol: Symbol) -> BackpackRawGetMarketParams:
        """Build query parameters for fetching a specific market."""
        ...

    @staticmethod
    def build_get_funding_rate_params(symbol: Symbol) -> BackpackRawGetFundingRateParams:
        """Build query parameters for fetching current funding rate."""
        ...

    @staticmethod
    def build_get_historical_funding_rates_params(
        symbol: Symbol,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 100,
    ) -> BackpackRawGetHistoricalFundingRatesParams:
        """Build query parameters for fetching historical funding rates."""
        ...

    @staticmethod
    def build_get_market_data_params(
        symbol: Symbol,
        interval: str,
        start_time: int,
        end_time: int | None = None,
        limit: int = 500,
    ) -> BackpackRawGetMarketDataParams:
        """Build query parameters for fetching historical market data (klines)."""
        ...

    @staticmethod
    def build_get_historical_trades_params(
        symbol: Symbol,
        limit: int = 100,
        from_id: str | None = None,
    ) -> BackpackRawGetHistoricalTradesParams:
        """Build query parameters for fetching historical trades."""
        ...


@runtime_checkable
class TradingRequestBuilderProtocol(RequestBuilderProtocol, Protocol):
    """Protocol for trading request builder components.

    Defines the interface for building trading-related API requests.
    Inherits from RequestBuilderProtocol for base builder compliance.
    """

    @staticmethod
    def map_order_enums_to_api_strings(
        order_type: OrderType,
        order_side: OrderSide,
        time_in_force: TimeInForce | None,
    ) -> tuple[str, str, str | None]:
        """Map internal enum values to Backpack API string values."""
        ...

    @staticmethod
    def build_place_order_payload(
        symbol: Symbol,
        order_type: OrderType,
        order_side: OrderSide,
        quantity: Decimal,
        price: Decimal | None = None,
        time_in_force: TimeInForce | None = None,
        client_order_id: str | None = None,
        execution: OrderExecution | None = None,
        stop_price: Decimal | None = None,
        take_profit_price: Decimal | None = None,
        self_trade_prevention: str | None = None,
    ) -> BackpackRawOrderExecuteRequest:
        """Build request payload for placing an order."""
        ...

    @staticmethod
    def build_cancel_order_payload(
        symbol: Symbol,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> BackpackRawOrderCancelRequest:
        """Build request payload for cancelling an order."""
        ...

    @staticmethod
    def build_cancel_all_orders_payload(
        symbol: Symbol | None = None,
    ) -> BackpackRawOrderCancelAllRequest:
        """Build request payload for cancelling all orders."""
        ...

    @staticmethod
    def build_get_open_orders_params(symbol: Symbol | None) -> BackpackRawGetOpenOrdersParams:
        """Build query parameters for fetching open orders."""
        ...

    @staticmethod
    def build_get_order_params(symbol: Symbol) -> BackpackRawGetOrderParams:
        """Build query parameters for fetching a specific order."""
        ...

    @staticmethod
    def build_get_order_history_params(
        symbol: Symbol | None = None,
        order_id: str | None = None,
        client_id: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 100,
    ) -> BackpackRawGetOrderHistoryParams:
        """Build query parameters for fetching order history."""
        ...

    @staticmethod
    def build_get_trade_history_params(
        symbol: Symbol | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 100,
        from_id: str | None = None,
    ) -> BackpackRawGetTradeHistoryParams:
        """Build query parameters for fetching trade history."""
        ...

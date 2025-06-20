"""CyberDeltaEngine: Backpack Raw Models Package.

--------------------------------------------

This package provides strict, well-documented Pydantic models for validating and parsing
all major Backpack Exchange API (REST and WebSocket) responses. Each model mirrors the
Backpack OpenAPI spec or WS event schema as closely as possible, with:

- Accurate field names and types (using Field aliasing for exact JSON keys)
- Comprehensive docstrings for every model
- Strict validation (extra fields forbidden)
- Logical grouping by resource/domain (account, order, trade, etc.)

Usage:
    from cyberdelta.apis.backpack.models import BackpackRawOrder, BackpackRawTrade

Do not use these models for internal business logic—use core models for that. These are
for boundary validation and transformation only.
"""

from .bp_raw_account import BackpackRawAccount, BackpackRawBalance
from .bp_raw_api_request_payloads import (
    BackpackRawAccountConvertDustRequest,
    BackpackRawAccountWithdrawalRequest,
    BackpackRawBorrowLendExecuteRequest,
    BackpackRawInternalTransferRequest,
    BackpackRawOrderCancelAllRequest,
    BackpackRawOrderCancelRequest,
    BackpackRawOrderExecuteRequest,
    BackpackRawQuoteAcceptRequest,
    BackpackRawQuoteSubmitRequest,
    BackpackRawRequestForQuoteCancelRequest,
    BackpackRawRequestForQuoteRefreshRequest,
    BackpackRawRequestForQuoteRequest,
    BackpackRawUpdateAccountSettingsRequest,
)
from .bp_raw_error import BackpackRawApiError
from .bp_raw_funding import BackpackRawFundingRate, BackpackRawMarkPrice
from .bp_raw_margin_functions import BackpackRawImfFunction, BackpackRawMmfFunction
from .bp_raw_market import (
    BackpackRawMarket,
    BackpackRawOpenInterest,
    BackpackRawOrderBookFilters,
    BackpackRawPriceFilter,
    BackpackRawQuantityFilter,
    BackpackRawTicker,
)
from .bp_raw_order import BackpackRawOrder, BackpackRawOrderBook, BackpackRawOrderUpdate
from .bp_raw_position import BackpackRawPosition, BackpackRawPositionUpdate
from .bp_raw_query_params import (
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
)
from .bp_raw_trade import (
    BackpackRawPublicTrade,
    BackpackRawPublicTradeEvent,
    BackpackRawRecentPublicTrade,
)
from .bp_raw_transfer import BackpackRawDeposit, BackpackRawLiquidation, BackpackRawWithdrawal
from .bp_ws_payloads import BackpackRawWsSubscriptionRequest

__all__ = [
    # Error
    "BackpackRawApiError",
    # Account
    "BackpackRawAccount",
    "BackpackRawBalance",
    # Position
    "BackpackRawPosition",
    "BackpackRawPositionUpdate",
    # Order
    "BackpackRawOrder",
    "BackpackRawOrderBook",
    "BackpackRawOrderUpdate",
    # Trade
    "BackpackRawPublicTrade",
    "BackpackRawRecentPublicTrade",
    "BackpackRawPublicTradeEvent",
    # Funding/Market
    "BackpackRawFundingRate",
    "BackpackRawMarkPrice",
    "BackpackRawMarket",
    "BackpackRawTicker",
    "BackpackRawOpenInterest",
    "BackpackRawPriceFilter",
    "BackpackRawQuantityFilter",
    "BackpackRawOrderBookFilters",
    # Transfer
    "BackpackRawWithdrawal",
    "BackpackRawDeposit",
    "BackpackRawLiquidation",
    # Margin
    "BackpackRawImfFunction",
    "BackpackRawMmfFunction",
    # Request Payloads
    "BackpackRawOrderExecuteRequest",
    "BackpackRawOrderCancelRequest",
    "BackpackRawOrderCancelAllRequest",
    "BackpackRawAccountWithdrawalRequest",
    "BackpackRawUpdateAccountSettingsRequest",
    "BackpackRawAccountConvertDustRequest",
    "BackpackRawBorrowLendExecuteRequest",
    "BackpackRawRequestForQuoteRequest",
    "BackpackRawQuoteSubmitRequest",
    "BackpackRawQuoteAcceptRequest",
    "BackpackRawRequestForQuoteCancelRequest",
    "BackpackRawRequestForQuoteRefreshRequest",
    "BackpackRawInternalTransferRequest",
    # Query Parameters
    "BackpackRawGetTickerParams",
    "BackpackRawGetOrderBookParams",
    "BackpackRawGetRecentTradesParams",
    "BackpackRawGetBalancesParams",
    "BackpackRawGetPositionsParams",
    "BackpackRawGetOpenOrdersParams",
    "BackpackRawGetFundingRateParams",
    "BackpackRawGetHistoricalFundingRatesParams",
    "BackpackRawGetAccountInfoParams",
    "BackpackRawGetOrderHistoryParams",
    "BackpackRawGetTradeHistoryParams",
    "BackpackRawGetMarketDataParams",
    "BackpackRawGetMarketParams",
    "BackpackRawGetMarketsParams",
    "BackpackRawGetHistoricalTradesParams",
    "BackpackRawGetOrderParams",
    # WebSocket Payloads
    "BackpackRawWsSubscriptionRequest",
]

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
    from cyberdelta.apis.backpack.models import BackpackRawOrderResponse, BackpackRawTrade

Do not use these models for internal business logic—use core models for that. These are
for boundary validation and transformation only.
"""

from .bp_raw_account import BackpackRawAccount, BackpackRawBalanceResponse
from .bp_raw_account_summary import BackpackRawAccountSummaryResponse
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
from .bp_raw_fills import BackpackRawFillResponse
from .bp_raw_funding import BackpackRawFundingRateResponse, BackpackRawMarkPrice
from .bp_raw_kline import BackpackRawKlineResponse
from .bp_raw_margin_functions import BackpackRawImfFunction, BackpackRawMmfFunction
from .bp_raw_market import (
    BackpackRawMarketResponse,
    BackpackRawOpenInterest,
    BackpackRawOrderBookFilters,
    BackpackRawPriceFilter,
    BackpackRawQuantityFilter,
    BackpackRawTickerResponse,
)
from .bp_raw_order import BackpackRawOrderBook, BackpackRawOrderResponse, BackpackRawOrderUpdate
from .bp_raw_position import BackpackRawPositionResponse, BackpackRawPositionUpdate
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
from .bp_raw_withdrawal import BackpackRawWithdrawalResponse
from .bp_ws_payloads import BackpackRawWsSignatureComponents, BackpackRawWsSubscriptionRequest


__all__ = [
    # Account
    "BackpackRawAccount",
    "BackpackRawAccountConvertDustRequest",
    "BackpackRawAccountSummaryResponse",
    "BackpackRawAccountWithdrawalRequest",
    # Error
    "BackpackRawApiError",
    "BackpackRawBalanceResponse",
    "BackpackRawBorrowLendExecuteRequest",
    "BackpackRawDeposit",
    # Fills
    "BackpackRawFillResponse",
    # Funding/Market
    "BackpackRawFundingRateResponse",
    "BackpackRawGetAccountInfoParams",
    "BackpackRawGetBalancesParams",
    "BackpackRawGetFundingRateParams",
    "BackpackRawGetHistoricalFundingRatesParams",
    "BackpackRawGetHistoricalTradesParams",
    "BackpackRawGetMarketDataParams",
    "BackpackRawGetMarketParams",
    "BackpackRawGetMarketsParams",
    "BackpackRawGetOpenOrdersParams",
    "BackpackRawGetOrderBookParams",
    "BackpackRawGetOrderHistoryParams",
    "BackpackRawGetOrderParams",
    "BackpackRawGetPositionsParams",
    "BackpackRawGetRecentTradesParams",
    # Query Parameters
    "BackpackRawGetTickerParams",
    "BackpackRawGetTradeHistoryParams",
    # Margin
    "BackpackRawImfFunction",
    "BackpackRawInternalTransferRequest",
    # Kline
    "BackpackRawKlineResponse",
    "BackpackRawLiquidation",
    "BackpackRawMarkPrice",
    "BackpackRawMarketResponse",
    "BackpackRawMmfFunction",
    "BackpackRawOpenInterest",
    "BackpackRawOrderBook",
    "BackpackRawOrderBookFilters",
    "BackpackRawOrderCancelAllRequest",
    "BackpackRawOrderCancelRequest",
    # Request Payloads
    "BackpackRawOrderExecuteRequest",
    # Order
    "BackpackRawOrderResponse",
    "BackpackRawOrderUpdate",
    # Position
    "BackpackRawPositionResponse",
    "BackpackRawPositionUpdate",
    "BackpackRawPriceFilter",
    # Trade
    "BackpackRawPublicTrade",
    "BackpackRawPublicTradeEvent",
    "BackpackRawQuantityFilter",
    "BackpackRawQuoteAcceptRequest",
    "BackpackRawQuoteSubmitRequest",
    "BackpackRawRecentPublicTrade",
    "BackpackRawRequestForQuoteCancelRequest",
    "BackpackRawRequestForQuoteRefreshRequest",
    "BackpackRawRequestForQuoteRequest",
    "BackpackRawTickerResponse",
    "BackpackRawUpdateAccountSettingsRequest",
    # Transfer
    "BackpackRawWithdrawal",
    "BackpackRawWithdrawalResponse",
    # WebSocket Payloads
    "BackpackRawWsSignatureComponents",
    "BackpackRawWsSubscriptionRequest",
]

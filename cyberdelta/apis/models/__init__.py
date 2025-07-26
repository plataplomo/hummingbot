"""Models for API-related functionality in CyberDeltaEngine.

This package contains shared models used across different exchange APIs,
including error handling and configuration models.
"""

from cyberdelta.apis.common.api_error import APIError, TransformationError
from cyberdelta.apis.common.api_error_codes import APIErrorCode
from cyberdelta.apis.common.api_error_response import APIErrorResponse

from .exchange_api_config import ExchangeAPIConfig
from .service_args import (
    CancelAllOrdersArgs,
    CancelOrderArgs,
    GetAllMidsArgs,
    GetAllOpenOrdersArgs,
    GetFundingRatesArgs,
    GetMarketDataArgs,
    GetOrderBookArgs,
    GetOrderHistoryArgs,
    GetTickerArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
    TransferArgs,
    WithdrawArgs,
)


__all__ = [
    # Error handling
    "APIError",
    "APIErrorCode",
    "APIErrorResponse",
    "CancelAllOrdersArgs",
    "CancelOrderArgs",
    # Configuration
    "ExchangeAPIConfig",
    "GetAllMidsArgs",
    "GetAllOpenOrdersArgs",
    "GetFundingRatesArgs",
    "GetMarketDataArgs",
    "GetOrderBookArgs",
    "GetOrderHistoryArgs",
    "GetTickerArgs",
    "GetTradeHistoryArgs",
    # Service arguments
    "PlaceOrderArgs",
    "TransferArgs",
    "TransformationError",
    "WithdrawArgs",
]

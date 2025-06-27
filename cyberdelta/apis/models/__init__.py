"""Models for API-related functionality in CyberDeltaEngine.

This package contains shared models used across different exchange APIs,
including error handling and configuration models.
"""

from ..common.api_error import APIError, TransformationError
from ..common.api_error_codes import APIErrorCode
from ..common.api_error_response import APIErrorResponse
from .exchange_api_config import ExchangeAPIConfig
from .service_args_models import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetFundingRatesArgs,
    GetMarketDataArgs,
    GetOrderHistoryArgs,
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
    "CancelOrderArgs",
    # Configuration
    "ExchangeAPIConfig",
    "GetAllOpenOrdersArgs",
    "GetFundingRatesArgs",
    "GetMarketDataArgs",
    "GetOrderHistoryArgs",
    "GetTradeHistoryArgs",
    # Service arguments
    "PlaceOrderArgs",
    "TransferArgs",
    "TransformationError",
    "WithdrawArgs",
]

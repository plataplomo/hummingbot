"""
CyberDeltaEngine: apis package initializer
This file marks the directory as a Python package and enables submodule imports.
"""

from __future__ import annotations

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.connectivity.http_client import HttpClient, HttpRequestFailedError
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.models.api_error import APIError

# from cyberdelta.apis.models.api import (
#     OrderUpdateEvent, PlaceOrderRequest, TradeFillEvent # Commented out - Models not found
# )
# from cyberdelta.apis.rate_limiter import (
#     TokenBucketRateLimiterRuntime # Removed unused import
# )

__all__ = [
    "BackpackAPI",
    "HyperliquidAPI",
    # "OrderUpdateEvent", # Removed - Corresponds to commented import
    # "PlaceOrderRequest", # Removed - Corresponds to commented import
    # "TradeFillEvent", # Removed - Corresponds to commented import
    # "ExchangeAPI", # Removed - Corresponds to commented import
]

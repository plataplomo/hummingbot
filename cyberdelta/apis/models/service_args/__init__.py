"""Service arguments models module.

This module provides structured access to service argument models organized by domain.
No re-exports - explicit imports required as per clean break refactor plan.
"""

# No re-exports as per the reorganization plan
# Users must explicitly import from the appropriate submodule:
# - from cyberdelta.apis.models.service_args.trading import PlaceOrderArgs
# - from cyberdelta.apis.models.service_args.account import TransferArgs
# - from cyberdelta.apis.models.service_args.market_data import GetMarketDataArgs
# - from cyberdelta.apis.models.service_args.internal import GetMaxOrderQuantityArgs
# - from cyberdelta.apis.models.service_args.hyperliquid import HyperliquidGetOrderStatusArgs

__all__: list[str] = []

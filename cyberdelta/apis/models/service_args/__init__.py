"""Service arguments models module.

This module provides structured access to service argument models organized by exchange and
functionality.
"""

# Re-export all common (generic) models
from .common import (
    CancelAllOrdersArgs,
    CancelOrderArgs,
    GetAllMidsArgs,
    GetAllOpenOrdersArgs,
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetL2BookArgs,
    GetMarketArgs,
    GetMarketDataArgs,
    GetMarketsArgs,
    GetMaxBorrowQuantityArgs,
    GetMaxOrderQuantityArgs,
    GetMaxWithdrawalQuantityArgs,
    GetOrderArgs,
    GetOrderBookArgs,
    GetOrderHistoryArgs,
    GetOrderStatusArgs,  # This is an alias for GetOrderArgs
    GetRecentTradesArgs,
    GetTickerArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
    TransferArgs,
    UpdateAccountSettingsArgs,
    WithdrawArgs,
)

# Re-export all Hyperliquid-specific models
from .hyperliquid import (
    HyperliquidGetCandleSnapshotArgs,
    HyperliquidGetOpenOrdersArgs,
    HyperliquidGetOrderHistoryArgs,
    HyperliquidGetOrderStatusArgs,
    HyperliquidGetUserFillsArgs,
    HyperliquidGetUserStateArgs,
    HyperliquidTransferL2UsdArgs,
    HyperliquidUpdateLeverageArgs,
    HyperliquidWithdrawL1Args,
)


# Re-export from backpack when models are added
# Future: from .backpack import BackpackSpecificModels

__all__ = [
    "CancelAllOrdersArgs",
    "CancelOrderArgs",
    "GetAllMidsArgs",
    "GetAllOpenOrdersArgs",
    "GetFundingRatesArgs",
    "GetHistoricalFundingRatesArgs",
    "GetL2BookArgs",
    "GetMarketArgs",
    "GetMarketDataArgs",
    "GetMarketsArgs",
    "GetMaxBorrowQuantityArgs",
    "GetMaxOrderQuantityArgs",
    "GetMaxWithdrawalQuantityArgs",
    "GetOrderArgs",
    "GetOrderBookArgs",
    "GetOrderHistoryArgs",
    "GetOrderStatusArgs",
    "GetRecentTradesArgs",
    "GetTickerArgs",
    "GetTradeHistoryArgs",
    "HyperliquidGetCandleSnapshotArgs",
    "HyperliquidGetOpenOrdersArgs",
    "HyperliquidGetOrderHistoryArgs",
    "HyperliquidGetOrderStatusArgs",
    "HyperliquidGetUserFillsArgs",
    "HyperliquidGetUserStateArgs",
    "HyperliquidTransferL2UsdArgs",
    "HyperliquidUpdateLeverageArgs",
    "HyperliquidWithdrawL1Args",
    "PlaceOrderArgs",
    "TransferArgs",
    "UpdateAccountSettingsArgs",
    "WithdrawArgs",
]

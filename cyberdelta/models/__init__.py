"""CyberDeltaEngine models package: aggregates all core trading, API, and enum models.

- Enums (OrderSide, OrderType, etc.) are only re-exported from .enums to avoid type conflicts.
- All models are imported from .trading and .api as needed.
- Star imports are avoided for clarity and type safety.
"""

# Import Account models
from cyberdelta.core.enums import (
    # ExchangeType, # Removed - Not defined in enums.py
    # Interval, # Removed - Not defined in enums.py
    MarketDataInterval,
    OrderExpiryReason,
    OrderStatus,
    OrderUpdateOrigin,
    SelfTradePrevention,
    SignalType,
    TriggerType,
)
from cyberdelta.enums import (
    OrderSide,
    OrderType,
    TimeInForce,
)

from .account_settings import (
    AccountSettings,
    BackpackAccountSettingsDetails,
    HyperliquidAccountSettingsDetails,
)
from .derivative_position import (
    BackpackPositionDetails,
    DerivativePosition,
    HyperliquidPositionDetails,
)
from .margin_account import (
    BackpackMarginDetails,
    HyperliquidMarginDetails,
    MarginAccountSummary,
)
from .market import CancelOrderResult, Fill, FundingRate, Market, MidPrices, OrderBook, Ticker
from .market.order import (
    BackpackOrderDetails,
    HyperliquidOrderDetails,
    Order,
)
from .operations import (
    BackpackTransferDetails,
    BackpackWithdrawalDetails,
    HyperliquidTransferDetails,
    HyperliquidWithdrawalDetails,
    Transfer,
    Withdrawal,
)
from .spot_balance import BackpackSpotBalanceDetails, HyperliquidSpotBalanceDetails, SpotBalance
from .trade_signal import (
    TradeSignal,
)
from .trading.execution_request import ExecutionRequest


__all__ = [
    # Account Settings Models
    "AccountSettings",
    "BackpackAccountSettingsDetails",  # Account Settings Detail
    "BackpackMarginDetails",  # Margin Account Detail
    "BackpackOrderDetails",
    "BackpackPositionDetails",  # Derivative Position Detail
    "BackpackSpotBalanceDetails",
    "BackpackTransferDetails",
    "BackpackWithdrawalDetails",
    "CancelOrderResult",
    "DerivativePosition",
    "ExecutionRequest",
    "Fill",
    "FundingRate",
    "HyperliquidAccountSettingsDetails",  # Account Settings Detail
    "HyperliquidMarginDetails",  # Margin Account Detail
    "HyperliquidOrderDetails",
    "HyperliquidPositionDetails",  # Derivative Position Detail
    "HyperliquidSpotBalanceDetails",
    "HyperliquidTransferDetails",
    "HyperliquidWithdrawalDetails",
    "MarginAccountSummary",
    "Market",
    "MarketDataInterval",
    "MidPrices",
    # Market Data Models
    "Order",
    "OrderBook",
    "OrderExpiryReason",
    # Core Enums
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "OrderUpdateOrigin",
    "SelfTradePrevention",
    "SignalType",
    # Portfolio State Models
    "SpotBalance",
    "Ticker",
    "TimeInForce",
    # Strategy Models
    "TradeSignal",
    # New Operation Models
    "Transfer",
    "TriggerType",
    "Withdrawal",
    # --- TODO: Resolve Missing Modules/Imports ---
    # "APIKeys",
    # "ExchangeType",
    # "Interval",
    # "PositionInfo",
    # "PositionSide",
    # "Quote",
]

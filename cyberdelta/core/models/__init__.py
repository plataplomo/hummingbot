"""
CyberDeltaEngine models package: aggregates all core trading, API, and enum models for
convenient import.

- Enums (OrderSide, OrderType, etc.) are only re-exported from .enums to avoid type conflicts.
- All models are imported from .trading and .api as needed.
- Star imports are avoided for clarity and type safety.
"""

# from .credentials import APIKeys # TODO: Resolve ModuleNotFoundError
from .derivative_position import (
    BackpackPositionDetails,
    DerivativePosition,
    HyperliquidPositionDetails,
)
from .enums import (
    # ExchangeType, # Removed - Not defined in enums.py
    # Interval, # Removed - Not defined in enums.py
    OrderExpiryReason,
    OrderSide,
    OrderStatus,
    OrderType,
    OrderUpdateOrigin,
    SelfTradePrevention,
    SignalType,
    TimeInForce,
    TriggerType,
)

# Import new Margin Account models
from .margin_account import (
    BackpackMarginDetails,
    HyperliquidMarginDetails,
    MarginAccountSummary,
)
from .market import FundingRate, OrderBook, Ticker, Trade
from .market.order import (
    BackpackOrderDetails,
    HyperliquidOrderDetails,
    Order,
)
from .spot_balance import BackpackSpotBalanceDetails, HyperliquidSpotBalanceDetails, SpotBalance

# from .positions import PositionInfo, PositionSide # TODO: Resolve ModuleNotFoundError
# from .quotes import Quote # TODO: Resolve ModuleNotFoundError
from .trade_signal import (
    TradeSignal,
)

__all__ = [
    # Core Enums
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "SignalType",
    "TimeInForce",
    "OrderExpiryReason",
    "OrderUpdateOrigin",
    "SelfTradePrevention",
    "TriggerType",
    # Market Data Models
    "Order",
    "HyperliquidOrderDetails",
    "BackpackOrderDetails",
    "Trade",
    "Ticker",
    "OrderBook",
    "FundingRate",
    # Portfolio State Models
    "SpotBalance",
    "HyperliquidSpotBalanceDetails",
    "BackpackSpotBalanceDetails",
    "DerivativePosition",
    "HyperliquidPositionDetails",  # Derivative Position Detail
    "BackpackPositionDetails",  # Derivative Position Detail
    "MarginAccountSummary",
    "HyperliquidMarginDetails",  # Margin Account Detail
    "BackpackMarginDetails",  # Margin Account Detail
    # Strategy Models
    "TradeSignal",
    # --- TODO: Resolve Missing Modules/Imports ---
    # "APIKeys",
    # "ExchangeType",
    # "Interval",
    # "PositionInfo",
    # "PositionSide",
    # "Quote",
]

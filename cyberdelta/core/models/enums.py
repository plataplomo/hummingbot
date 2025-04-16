from enum import Enum


class OrderSide(Enum):
    """Enum representing the side of an order (BUY or SELL)."""

    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Enum representing the type of an order."""

    LIMIT = "limit"
    MARKET = "market"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TAKE_PROFIT = "take_profit"
    TAKE_PROFIT_LIMIT = "take_profit_limit"


class OrderStatus(Enum):
    """Enum representing the status of an order."""

    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    OPEN = "OPEN"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class SignalType(Enum):
    """Enum representing the type of a trading signal."""

    ENTER_LONG = "ENTER_LONG"
    EXIT_LONG = "EXIT_LONG"
    ENTER_SHORT = "ENTER_SHORT"
    EXIT_SHORT = "EXIT_SHORT"
    HOLD = "HOLD"
    REBALANCE = "REBALANCE"


class TimeInForce(Enum):
    """Enum representing the time in force for an order."""

    GTC = "GTC"  # Good 'Til Canceled
    IOC = "IOC"  # Immediate Or Cancel
    FOK = "FOK"  # Fill Or Kill

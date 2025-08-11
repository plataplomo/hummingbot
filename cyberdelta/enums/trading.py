"""Trading enumeration types for CyberDeltaEngine.

This module defines trading-related enums in a neutral location
to avoid circular imports between core, APIs, and other modules.
These enums are shared across the entire system.
"""

from enum import Enum


class OrderSide(Enum):
    """Enum representing the side of an order.

    Used throughout CyberDeltaEngine for both REST and WebSocket APIs.
    - BUY: Represents intent to buy (often the Bid side of the book).
    - SELL: Represents intent to sell (often the Ask side of the book).
    """

    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    """Enum representing the type of an order, merged from common types and exchange specifics.

    Used for both REST and WebSocket order placement and status.
    - MARKET: Market order
    - LIMIT: Limit order
    - STOP_MARKET: Stop loss executed as market order
    - STOP_LIMIT: Stop loss executed as limit order
    - TAKE_PROFIT_MARKET: Take profit executed as market order
    - TAKE_PROFIT_LIMIT: Take profit executed as limit order
    """

    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"  # Stop Loss executed as Market
    STOP_LIMIT = "STOP_LIMIT"  # Stop Loss executed as Limit
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"  # Take Profit executed as Market
    TAKE_PROFIT_LIMIT = "TAKE_PROFIT_LIMIT"  # Take Profit executed as Limit
    # Note: Hyperliquid uses nested structure for trigger orders.
    # Backpack spec mentions separate SL/TP fields for order execution.
    # These enums represent the *intended execution type*.


class TimeInForce(Enum):
    """Enum representing the time in force for an order.

    Used for both REST and WebSocket order placement.
    - GTC: Good 'Til Canceled
    - IOC: Immediate Or Cancel
    - FOK: Fill Or Kill
    - ALO: Add Liquidity Only / Post-Only (Hyperliquid-specific)
    """

    GTC = "GTC"  # Good 'Til Canceled (Backpack, HL via default Limit)
    IOC = "IOC"  # Immediate Or Cancel (Backpack, HL via Limit)
    FOK = "FOK"  # Fill Or Kill (Backpack, HL via Limit)
    ALO = "ALO"  # Add Liquidity Only / Post-Only (Hyperliquid specific TIF value)


class MakerTaker(Enum):
    """Enum representing whether a fill was executed as maker or taker.

    Used throughout CyberDeltaEngine for fee calculation and execution reporting.
    - MAKER: Order was filled as a maker (provided liquidity to the book)
    - TAKER: Order was filled as a taker (removed liquidity from the book)
    """

    MAKER = "MAKER"
    TAKER = "TAKER"


class TradingAction(Enum):
    """Enum representing trading signal actions.

    Used in SignalEvent and strategy implementations to represent
    the intended trading action based on signal analysis.
    - BUY: Signal indicates to open or increase long position
    - SELL: Signal indicates to open or increase short position
    - HOLD: Signal indicates to maintain current position
    - CLOSE: Signal indicates to close current position
    """

    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    CLOSE = "CLOSE"


class OrderEventType(Enum):
    """Enum representing order lifecycle event types.

    Used in OrderEvent structures for type-safe event handling.
    - PLACED: Order successfully placed on exchange
    - FILLED: Order completely filled
    - PARTIALLY_FILLED: Order partially filled, remaining quantity active
    - CANCELLED: Order cancelled by user or system
    - REJECTED: Order rejected by exchange
    - EXPIRED: Order expired due to time constraints
    - AMENDED: Order price/quantity amended
    """

    PLACED = "PLACED"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    AMENDED = "AMENDED"


class PositionEventType(Enum):
    """Enum representing position lifecycle event types.

    Used in PositionEvent structures for type-safe event handling.
    - OPENED: New position opened
    - UPDATED: Existing position updated (size/price changed)
    - CLOSED: Position closed normally
    - LIQUIDATED: Position force-closed due to margin requirements
    """

    OPENED = "OPENED"
    UPDATED = "UPDATED"
    CLOSED = "CLOSED"
    LIQUIDATED = "LIQUIDATED"

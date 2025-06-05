"""Core enumeration types for the CyberDeltaEngine trading system.

This module defines all enumeration types used throughout the trading engine,
including order-related enums, market data types, signal types, and status
indicators. These enums provide type safety and standardization across
different exchange APIs and internal components.

The enums are organized into logical groups:
- Order-related enums (OrderSide, OrderType, OrderStatus, etc.)
- Market data enums (MarketType, Blockchain)
- Trading signal enums (SignalType)
- Operation status enums (transfer and withdrawal statuses)
"""

from enum import Enum

# --------------------
# Order Related Enums
# --------------------


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


class OrderStatus(Enum):
    """Enum representing the status of an order, merged from common states and exchange specifics.

    Used for tracking order lifecycle and state transitions.
    - NEW: Order received by system, not yet acknowledged by exchange.
    - OPEN: Order acknowledged and resting on the book (or waiting trigger).
    - PARTIALLY_FILLED: Order partially executed.
    - FILLED: Order fully executed.
    - CANCELED: Order explicitly canceled by user or system.
    - REJECTED: Order rejected by exchange upon submission.
    - EXPIRED: Order expired due to TimeInForce or other reason.
    - TRIGGER_PENDING: Conditional order waiting for trigger price.
    - FAILED: Order submission or lifecycle failed unexpectedly.
    - UNKNOWN: Status cannot be determined.
    """

    # Common Lifecycle States
    NEW = "NEW"  # Order received by system, not yet acknowledged by exchange.
    OPEN = "OPEN"  # Order acknowledged and resting on the book (or waiting trigger).
    # Includes NEW and PARTIALLY_FILLED logically.
    PARTIALLY_FILLED = "PARTIALLY_FILLED"  # Order partially executed.
    FILLED = "FILLED"  # Order fully executed.
    # Terminal States (Cancellation/Rejection)
    CANCELED = "CANCELED"  # Order explicitly canceled by user or system. (Backpack: Cancelled)
    REJECTED = "REJECTED"  # Order rejected by exchange upon submission.
    EXPIRED = (
        "EXPIRED"  # Order expired due to TimeInForce policy or other reason. (Backpack: Expired)
    )
    # Trigger Order Specific States (Map from exchange data where possible)
    TRIGGER_PENDING = (
        "TRIGGER_PENDING"  # Conditional order waiting for trigger price. (Backpack: TriggerPending)
    )
    # Failure/Unknown States
    FAILED = "FAILED"  # Order submission or lifecycle failed unexpectedly.
    UNKNOWN = "UNKNOWN"  # Status cannot be determined.

    def is_open(self) -> bool:
        """Check if the order status represents an open order."""
        return self in {
            OrderStatus.NEW,
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.TRIGGER_PENDING,
            # Add other non-terminal, active statuses if they exist
            # e.g., PENDING_NEW, REPLACED, PENDING_CANCEL, UNTRIGGERED
            # Based on the current enum definition, these are the key open ones.
        }

    def is_closed(self) -> bool:
        """Check if the order status represents a closed/terminal order."""
        return self in {
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
            OrderStatus.FAILED,
        }

    def is_terminal(self) -> bool:  # Alias for is_closed for clarity
        """Alias for is_closed, checks for terminal states."""
        return self.is_closed()


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


class SelfTradePrevention(Enum):
    """Enum for self-trade prevention actions (from Backpack spec).

    - REJECT_TAKER: Reject the taker side of a self-trade.
    - REJECT_MAKER: Reject the maker side of a self-trade.
    - REJECT_BOTH: Reject both sides of a self-trade.
    - NONE: No self-trade prevention (default if not specified).
    """

    REJECT_TAKER = "RejectTaker"
    REJECT_MAKER = "RejectMaker"
    REJECT_BOTH = "RejectBoth"
    NONE = "None"  # Default if not specified/supported


class TriggerType(Enum):
    """Enum for reference price used for triggering conditional orders.

    - LAST_PRICE: Trigger based on the last traded price.
    - MARK_PRICE: Trigger based on the mark price.
    - INDEX_PRICE: Trigger based on the index price.
    """

    LAST_PRICE = "LastPrice"  # Trigger based on the last traded price.
    MARK_PRICE = "MarkPrice"  # Trigger based on the mark price.
    INDEX_PRICE = "IndexPrice"  # Trigger based on the index price.


class OrderUpdateOrigin(Enum):
    """Enum for the origin of an order update event (from Backpack WS spec).

    - USER: User-initiated update.
    - LIQUIDATION_AUTOCLOSE: Liquidation event.
    - ADL_AUTOCLOSE: Auto-deleveraging event.
    - COLLATERAL_CONVERSION: Collateral conversion event.
    - SETTLEMENT_AUTOCLOSE: Settlement event.
    - BACKSTOP_LIQUIDITY_PROVIDER: Backstop liquidity provider event.
    - UNKNOWN: Unknown origin.
    """

    USER = "USER"
    LIQUIDATION_AUTOCLOSE = "LIQUIDATION_AUTOCLOSE"
    ADL_AUTOCLOSE = "ADL_AUTOCLOSE"
    COLLATERAL_CONVERSION = "COLLATERAL_CONVERSION"
    SETTLEMENT_AUTOCLOSE = "SETTLEMENT_AUTOCLOSE"
    BACKSTOP_LIQUIDITY_PROVIDER = "BACKSTOP_LIQUIDITY_PROVIDER"
    UNKNOWN = "UNKNOWN"


class OrderExpiryReason(Enum):
    """Enum for reason for order expiry or cancellation (merged from Backpack spec).

    - ACCOUNT_TRADING_SUSPENDED: Trading suspended on account.
    - FILL_OR_KILL: Order expired due to FOK policy.
    - INSUFFICIENT_BORROWABLE_QUANTITY: Not enough borrowable quantity.
    - INSUFFICIENT_FUNDS: Not enough funds.
    - INSUFFICIENT_LIQUIDITY: Not enough liquidity.
    - INVALID_PRICE: Price was invalid.
    - INVALID_QUANTITY: Quantity was invalid.
    - IMMEDIATE_OR_CANCEL: Order expired due to IOC policy.
    - INSUFFICIENT_MARGIN: Not enough margin.
    - LIQUIDATION: Order expired due to liquidation.
    - POST_ONLY_TAKER: Post-only order would have crossed.
    - REDUCE_ONLY_NOT_REDUCED: Reduce-only order wouldn't reduce position.
    - SELF_TRADE_PREVENTION: Order canceled due to self-trade prevention.
    - STOP_WITHOUT_POSITION: Stop order placed without relevant position.
    - PRICE_IMPACT: Market order exceeded price impact limits.
    - USER_CANCELLED: Explicit user cancellation.
    - EXCHANGE_MAINTENANCE: Exchange maintenance event.
    - UNKNOWN: Unknown reason.
    """

    ACCOUNT_TRADING_SUSPENDED = "AccountTradingSuspended"
    FILL_OR_KILL = "FillOrKill"
    INSUFFICIENT_BORROWABLE_QUANTITY = "InsufficientBorrowableQuantity"
    INSUFFICIENT_FUNDS = "InsufficientFunds"
    INSUFFICIENT_LIQUIDITY = "InsufficientLiquidity"
    INVALID_PRICE = "InvalidPrice"
    INVALID_QUANTITY = "InvalidQuantity"
    IMMEDIATE_OR_CANCEL = "ImmediateOrCancel"
    INSUFFICIENT_MARGIN = "InsufficientMargin"
    LIQUIDATION = "Liquidation"
    POST_ONLY_TAKER = "PostOnlyTaker"  # Post-only order would have crossed
    REDUCE_ONLY_NOT_REDUCED = "ReduceOnlyNotReduced"  # Reduce-only order wouldn't reduce position
    SELF_TRADE_PREVENTION = "SelfTradePrevention"
    STOP_WITHOUT_POSITION = "StopWithoutPosition"  # Stop order placed without relevant position
    PRICE_IMPACT = "PriceImpact"  # Market order exceeded price impact limits
    USER_CANCELLED = "UserCancelled"  # Explicitly add this common reason
    EXCHANGE_MAINTENANCE = "ExchangeMaintenance"
    UNKNOWN = "Unknown"
    # Add other reasons if discovered


# --------------------
# Strategy Related Enums
# --------------------


class SignalType(Enum):
    """Enum representing the type of a trading signal.

    Used for strategy logic and event handling.
    - ENTER_LONG: Signal to enter a long position.
    - EXIT_LONG: Signal to exit a long position.
    - ENTER_SHORT: Signal to enter a short position.
    - EXIT_SHORT: Signal to exit a short position.
    - HOLD: Signal to maintain current state.
    - REBALANCE: Signal to adjust position to target.
    """

    ENTER_LONG = "ENTER_LONG"
    EXIT_LONG = "EXIT_LONG"
    ENTER_SHORT = "ENTER_SHORT"
    EXIT_SHORT = "EXIT_SHORT"
    HOLD = "HOLD"  # Signal to maintain current state
    REBALANCE = "REBALANCE"  # Signal to adjust position to target


# --------------------
# Market Data / Exchange Enums
# --------------------


class MarketType(Enum):
    """Enum for type of market (from Backpack spec).

    - SPOT: Spot market.
    - PERP: Perpetual futures market.
    - IPERP: Inverse perpetual market.
    - DATED: Dated future market.
    - PREDICTION: Prediction market.
    - RFQ: Request For Quote market.
    """

    SPOT = "SPOT"
    PERP = "PERP"
    IPERP = "IPERP"  # Inverse Perpetual?
    DATED = "DATED"  # Dated Future
    PREDICTION = "PREDICTION"
    RFQ = "RFQ"  # Request For Quote Market


class Blockchain(Enum):
    """Enum for supported blockchains (expand as needed).

    - SOLANA: Solana blockchain.
    - ETHEREUM: Ethereum blockchain.
    - ARBITRUM: Arbitrum blockchain.
    """

    SOLANA = "Solana"
    ETHEREUM = "Ethereum"
    ARBITRUM = "Arbitrum"
    # Add others from spec...


class CancelOrderResultStatus(Enum):
    """Enum representing the status of a cancel order operation for a single order or a batch."""

    SUCCESS = "SUCCESS"  # All specified orders were successfully canceled.
    PARTIAL = "PARTIAL"  # Some orders were canceled, some failed or were not found.
    FAILED = "FAILED"  # The cancel operation failed for all specified orders.
    NOT_FOUND = "NOT_FOUND"  # No orders matching the criteria were found.
    ALREADY_CANCELLED_OR_CLOSED = (
        "ALREADY_CANCELLED_OR_CLOSED"  # Orders were already in a terminal state.
    )
    PENDING = "PENDING"  # Cancellation request submitted, awaiting confirmation.
    UNKNOWN = "UNKNOWN"  # The outcome of the cancellation is unknown.


# --------------------
# Operation Status Enums (New)
# --------------------


class InternalTransferStatus(Enum):
    """Enum representing the internal, standardized status of a funds transfer operation."""

    PENDING = "PENDING"  # Transfer initiated but not yet confirmed/failed.
    COMPLETED = "COMPLETED"  # Transfer successfully processed.
    FAILED = "FAILED"  # Transfer attempt failed (e.g., insufficient funds, network issue).
    REJECTED = "REJECTED"  # Transfer explicitly rejected by the exchange or system.
    UNKNOWN = "UNKNOWN"  # Status cannot be determined.


class InternalWithdrawalStatus(Enum):
    """Enum representing the internal, standardized status of a withdrawal operation."""

    PENDING = "PENDING"  # Withdrawal request received, awaiting processing.
    PROCESSING = "PROCESSING"  # Withdrawal is being processed by the exchange.
    AWAITING_CONFIRMATION = "AWAITING_CONFIRMATION"  # Tx broadcast, awaiting network confirmations.
    COMPLETED = "COMPLETED"  # Withdrawal successfully processed and confirmed.
    FAILED = "FAILED"  # Withdrawal attempt failed (e.g., invalid address, network issue).
    CANCELED = "CANCELED"  # Withdrawal was canceled before processing or confirmation.
    REJECTED = "REJECTED"  # Withdrawal request rejected by the exchange.
    UNKNOWN = "UNKNOWN"  # Status cannot be determined.


# Define __all__ for explicit public export
__all__ = [
    "OrderSide",
    "OrderType",
    "OrderStatus",
    "SignalType",
    "TimeInForce",
    "SelfTradePrevention",
    "TriggerType",
    "OrderUpdateOrigin",
    "OrderExpiryReason",
    "MarketType",
    "Blockchain",
    "CancelOrderResultStatus",
    # New Operation Status Enums
    "InternalTransferStatus",
    "InternalWithdrawalStatus",
]

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
        """Check if the order status represents an open order.

        Returns:
            True if the status represents an open/active order, False otherwise
        """
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
        """Check if the order status represents a closed/terminal order.

        Returns:
            True if the status represents a closed/terminal order, False otherwise
        """
        return self in {
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
            OrderStatus.FAILED,
        }

    def is_terminal(self) -> bool:  # Alias for is_closed for clarity
        """Alias for is_closed, checks for terminal states.

        Returns:
            True if the status represents a terminal state, False otherwise
        """
        return self.is_closed()


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


class MarketDataInterval(Enum):
    """Enum representing time intervals for market data (candlesticks, OHLCV data)."""

    ONE_MINUTE = "1m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    ONE_HOUR = "1h"
    FOUR_HOURS = "4h"
    ONE_DAY = "1d"
    ONE_WEEK = "1w"
    ONE_MONTH = "1M"


__all__ = [
    "Blockchain",
    "CancelOrderResultStatus",
    # New Operation Status Enums
    "InternalTransferStatus",
    "InternalWithdrawalStatus",
    "MarketDataInterval",
    "MarketType",
    "OrderExpiryReason",
    "OrderStatus",
    "OrderUpdateOrigin",
    "SelfTradePrevention",
    "TriggerType",
]

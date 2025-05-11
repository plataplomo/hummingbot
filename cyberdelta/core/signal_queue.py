from __future__ import annotations  # Enable postponed evaluation

import asyncio
import heapq
import logging
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.core.models import OrderSide, TradeSignal
from cyberdelta.core.models.enums import SignalType
from cyberdelta.utils.config import Config
from cyberdelta.utils.logging_config import get_logger
from cyberdelta.validation.circuit_breaker import BreakerState, CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity

"""
Priority Signal Queue for trade signals.

This module implements a priority queue for trade signals with expiration
handling, supporting the efficient management of trading opportunities based
on their utility scores and other attributes.
"""

if TYPE_CHECKING:
    from cyberdelta.core.models import SignalType, TradeSignal

# Setup logging
logger = get_logger(__name__)


class PrioritySignalQueue:
    """
    Priority queue for trade signals with expiration handling.

    This class manages trade signals ordered by their utility scores,
    automatically handling signal expiration and integration with safety systems.
    """

    def __init__(
        self, config: Config, circuit_breaker_system: CircuitBreakerSystem | None = None
    ) -> None:
        """
        Initialize the priority signal queue.

        Args:
            config: Configuration parameters
            circuit_breaker_system: Optional circuit breaker system for safety checks
        """
        self.config = config
        self.circuit_breaker_system = circuit_breaker_system
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Priority queue: [(negative_utility_score, unique_id, signal)]
        # Using negative utility score for max-heap behavior
        self.signal_queue: list[tuple[float, int, TradeSignal]] = []

        # Heap for async wait/pop operations
        self.signal_heap: list[tuple[float, datetime, int, TradeSignal]] = []

        # Lock Management:
        # The class supports both synchronous and asynchronous operations
        # _sync_lock: For synchronous methods (thread-safety in non-async contexts)
        # lock: For asynchronous methods (when used with 'async with')
        self._sync_lock = threading.Lock()
        self.lock = asyncio.Lock()  # For async methods
        self.new_signal_event = asyncio.Event()  # For async signaling

        # Counter for generating unique IDs
        self.counter = 0

        # Queue parameters - Cast config values to expected types
        # Ensure config.get returns something convertible to float/int or handle error
        def get_config_value(key: str, default: Any, target_type: type) -> Any:
            value = config.get(key, default)
            try:
                # Ensure value is convertible before calling int()
                return int(str(value))
            except (ValueError, TypeError) as e:
                logger.error(
                    f"Invalid config value '{value}' for '{key}'. "
                    f"Using default: {default}. Error: {e}"
                )
                return target_type(default)

        self.default_expiration_seconds: float = get_config_value(
            "default_signal_expiration_seconds", 300.0, float
        )
        self.max_queue_size: int = get_config_value("max_signal_queue_size", 100, int)
        self.cleanup_interval: float = get_config_value("queue_cleanup_interval", 10.0, float)

        self.last_cleanup = datetime.now(UTC)

        logger.info("Initialized priority signal queue")

    def add_signal(self, signal: TradeSignal) -> bool:
        """
        Add a signal to the priority queue.

        Args:
            signal: Trade signal to add

        Returns:
            True if signal was added, False if rejected
        """
        # Check if signal already has metadata, create if not
        if signal.metadata is None:
            signal.metadata = {}

        # Check if utility score is present
        if "utility_score" not in signal.metadata:
            logger.warning(f"Signal for {signal.symbol} has no utility score, using default 0.0")
            signal.metadata["utility_score"] = 0.0

        # Ensure utility_score is a float
        try:
            signal.metadata["utility_score"] = float(signal.metadata["utility_score"])
        except (ValueError, TypeError):
            # Shorten f-string for line length
            score_val = signal.metadata.get("utility_score", "N/A")  # Use get for safety
            logger.warning(
                f"Invalid utility_score '{score_val}' for signal {signal.symbol}. "
                f"Using default 0.0"  # Ruff E501 fix: Split long f-string
            )
            signal.metadata["utility_score"] = 0.0

        # Set expiration time if not already set
        if signal.expiration is None:
            signal = self._calculate_expiration(signal)  # Assign the modified signal back

        # Check circuit breakers before adding
        if self.circuit_breaker_system and not self._check_circuit_breakers_pre_add(signal):
            logger.warning(f"Circuit breaker active for {signal.symbol}, rejecting signal")
            return False

        # Clean expired signals periodically
        now: datetime = datetime.now(UTC)
        if (now - self.last_cleanup).total_seconds() > self.cleanup_interval:
            self._clean_expired_signals()
            self.last_cleanup = now

        # First, check if we need to trim the queue
        if len(self.signal_queue) >= self.max_queue_size:
            # Check if new signal has higher utility than the lowest in queue
            if len(self.signal_queue) > 0:
                # Find minimum utility score (maximum negative score since we use negative values)
                min_score = max(self.signal_queue, key=lambda x: x[0])[0]
                # Mypy fix: Check metadata is not None before indexing
                if signal.metadata is None:
                    logger.error(
                        f"Signal metadata is None for {signal.symbol} during trim check. "
                        f"Cannot proceed."
                    )
                    return False  # Or handle appropriately
                # Use .get() for safety, although metadata should exist here
                new_score = -float(signal.metadata.get("utility_score", 0.0))

                # If new signal has lower utility than the lowest, reject it
                if new_score >= min_score:
                    logger.debug(
                        f"Rejected signal for {signal.symbol} with score "
                        # Mypy fix: Check metadata is not None before indexing
                        # Use .get() for safety
                        f"{signal.metadata.get('utility_score', 0.0)} (lower than min {-min_score})"
                    )
                    return False

                # Otherwise, remove the lowest utility signal
                self._trim_queue()

        # Add to priority queue
        self.counter += 1
        # Mypy fix: Check metadata is not None before indexing
        if signal.metadata is None:
            logger.error(
                f"Signal metadata is None for {signal.symbol} before push. Cannot proceed."
            )
            return False  # Or handle appropriately
        # Use .get() for safety
        utility_score = float(signal.metadata.get("utility_score", 0.0))
        heapq.heappush(self.signal_queue, (-utility_score, self.counter, signal))

        logger.debug(
            f"Added signal for {signal.symbol} to queue with score "
            # Mypy fix: Check metadata is not None before indexing
            # Use .get() for safety
            f"{signal.metadata.get('utility_score', 0.0)}"
        )
        return True

    def add_from_opportunity(
        self, opportunity: ArbitrageOpportunity, strategy_name: str
    ) -> TradeSignal | None:
        """
        Create and add a trade signal from an arbitrage opportunity.

        Args:
            opportunity: Arbitrage opportunity
            strategy_name: Name of the strategy creating the signal

        Returns:
            Created signal if added successfully, None otherwise
        """
        # Determine SignalType based on opportunity details (simplified example)
        # TODO: Refine logic to determine signal type based on opportunity context
        signal_type = SignalType.ENTER_LONG  # Default, needs better logic
        side = OrderSide.BUY if signal_type == SignalType.ENTER_LONG else OrderSide.SELL

        # Extract required fields
        symbol = opportunity.symbol
        timestamp = datetime.now(UTC)

        # Safely build metadata from optional opportunity fields
        metadata = {
            "utility_score": getattr(opportunity, "utility_score", 0.0),
            "confidence_score": getattr(opportunity, "confidence_score", None),
            "expected_profit": str(getattr(opportunity, "expected_profit", Decimal("0"))),
            "basis_volatility": getattr(opportunity, "basis_volatility", None),
            "long_exchange": opportunity.long_exchange,
            "short_exchange": opportunity.short_exchange,
            "long_funding_rate": str(getattr(opportunity, "long_funding_rate", None)),
            "short_funding_rate": str(getattr(opportunity, "short_funding_rate", None)),
            "net_funding_differential": str(getattr(opportunity, "net_funding_differential", None)),
        }

        # Ensure price and quantity are always Decimal, never None
        # ArbitrageOpportunity.long_price is Decimal = Field(gt=0)
        price = opportunity.long_price  # Use long_price for ENTER_LONG

        quantity: Decimal
        if (
            opportunity.optimal_size is not None
            and opportunity.optimal_size > Decimal("0")
            and price > Decimal("0")
        ):
            quantity = opportunity.optimal_size / price
            if quantity <= Decimal("0"):  # If calculated quantity is not positive
                quantity = Decimal("0.000001")  # Placeholder for gt=0 constraint
        else:
            # Fallback if optimal_size is None or price is not suitable for division
            # This case should ideally be refined based on strategy requirements.
            # For now, use a placeholder to satisfy TradeSignal's gt=0 constraint.
            logger.warning(
                f"Could not determine quantity for signal {opportunity.symbol} from optimal_size. "
                f"Using placeholder."
            )
            quantity = Decimal("0.000001")  # Placeholder

        # Create signal
        signal = TradeSignal(
            source_strategy=strategy_name,
            symbol=symbol,
            signal_type=signal_type,
            side=side,
            timestamp=timestamp,
            price=price,  # Always Decimal
            quantity=quantity,  # Always Decimal
            # Pass both exchanges involved in the arbitrage
            exchange=[opportunity.long_exchange, opportunity.short_exchange],
            expiration=getattr(opportunity, "expiration", None),
            metadata=metadata,
        )

        # Add to queue
        if self.add_signal(signal):
            return signal

        return None

    def get_next_signal(self) -> TradeSignal | None:
        """
        Get highest priority unexpired signal.

        Returns:
            Highest priority trade signal or None if queue is empty
        """
        # Clean expired signals
        self._clean_expired_signals()

        # Return None if queue is empty
        if not self.signal_queue:
            return None

        try:
            # Process signals from the heap until we find a valid one
            while self.signal_queue:
                # Get highest priority signal item without removing
                _neg_score, uid, potential_signal = self.signal_queue[0]

                # Check validity (expiration)
                if not potential_signal.is_valid():
                    # Remove expired signal and log
                    heapq.heappop(self.signal_queue)
                    logger.debug(
                        f"Removed expired signal {uid} for {potential_signal.symbol} "
                        f"from queue during get_next."
                    )
                    continue  # Try the next item in the heap

                # Check circuit breakers before returning
                if self.circuit_breaker_system and not self._check_circuit_breakers_post_get(
                    potential_signal
                ):
                    # Remove signal blocked by CB and log
                    heapq.heappop(self.signal_queue)
                    # Shorten f-string for line length
                    # Shorten f-string for line length
                    logger.warning(
                        f"CB active for {potential_signal.symbol}, skipping signal {uid}"
                    )
                    continue  # Try the next item

                # If valid and passes CB, pop and return
                heapq.heappop(self.signal_queue)
                logger.debug(f"Returning signal {uid} for {potential_signal.symbol}")
                return potential_signal

            # If loop finishes, queue is empty
            return None

        except Exception as e:
            # Log error and return None to prevent system crash
            logger.error(f"Error in get_next_signal: {e}")
            return None

    def peek_next_signal(self) -> TradeSignal | None:
        """
        Peek at highest priority unexpired signal without removing it.

        Returns:
            Highest priority trade signal or None if queue is empty
        """
        # Clean expired signals
        self._clean_expired_signals()

        # Check if queue is empty
        if not self.signal_queue:
            return None

        # Get copy of highest priority signal without removing
        _, _, signal = self.signal_queue[0]

        # Verify signal is still valid (expiration)
        if signal.is_valid():
            # Also check circuit breaker status without removing
            if self.circuit_breaker_system and not self._check_circuit_breakers_post_get(signal):
                logger.warning(
                    f"Peek: Signal for {signal.symbol} would be blocked by circuit breaker."
                )
                # Technically, the signal is still in the queue, but won't be returned by get_next
                # Returning None might be confusing, let's return the signal but log the CB status
                return signal  # Return signal but warn
            return signal

        # Invalid signal (expired), clean and try again
        # Note: This doesn't guarantee the *next* peek will be valid
        self._clean_expired_signals()
        return self.peek_next_signal() if self.signal_queue else None

    def get_signals(self, max_count: int = 10, symbol: str | None = None) -> list[TradeSignal]:
        """
        Get multiple signals in priority order.

        Args:
            max_count: Maximum number of signals to return.
            symbol: Optional symbol to filter by.

        Returns:
            List of signals in priority order.
        """
        # TODO: This method currently returns potentially expired signals.
        # It should likely behave similarly to get_next_signal and filter expired.
        # Consider using heapq.nsmallest after cleaning expired signals.

        result: list[TradeSignal] = []
        temp_queue: list[tuple[float, int, TradeSignal]] = self.signal_queue.copy()

        # Clean expired signals
        self._clean_expired_signals()

        # Get signals
        while temp_queue and len(result) < max_count:
            _, _, signal = heapq.heappop(temp_queue)  # Pop highest priority
            # Filter by symbol if provided
            if symbol is None or signal.symbol == symbol:
                result.append(signal)

        return result

    def count(self) -> int:
        """
        Get count of unexpired signals in queue.

        Returns:
            Number of valid signals
        """
        # Clean expired signals
        self._clean_expired_signals()
        return len(self.signal_queue)

    def clear(self) -> None:
        """Clear all signals from the queue."""
        self.signal_queue = []
        logger.info("Signal queue cleared")

    def _clean_expired_signals(self) -> int:
        """
        Remove expired signals from the queue.

        Returns:
            Number of signals removed
        """
        original_count = len(self.signal_queue)
        valid_signals: list[tuple[float, int, TradeSignal]] = []
        for score, count, signal in self.signal_queue:
            try:
                if signal.is_valid():
                    valid_signals.append((score, count, signal))
                else:
                    self.logger.debug(f"Removed expired signal for {signal.symbol} during cleanup")
            except Exception as e:
                self.logger.warning(
                    f"Error checking validity of signal for {signal.symbol}: {e}. "
                    f"Keeping signal in queue."
                )
                valid_signals.append((score, count, signal))
        if len(valid_signals) < original_count:
            self.signal_queue = valid_signals
            heapq.heapify(self.signal_queue)
            removed = original_count - len(valid_signals)
            logger.debug(f"Removed {removed} expired signals")
            return removed
        return 0

    def _trim_queue(self) -> bool:
        """
        Trim queue to max size by removing lowest priority signals.

        Returns:
            True if a signal was removed, False otherwise
        """
        # Ensure max_queue_size is usable (cast during init)
        current_size = len(self.signal_queue)
        if current_size <= self.max_queue_size:
            return False

        # Determine how many items to remove
        num_to_remove = current_size - self.max_queue_size

        if num_to_remove > 0:
            # Get the `num_to_remove` lowest priority items using nlargest on the negative score
            # This is more efficient than popping one by one
            lowest_items = heapq.nlargest(num_to_remove, self.signal_queue, key=lambda x: x[0])

            # Log removals before modifying the heap
            for _score, _uid, low_signal in lowest_items:
                utility = -_score  # Convert back to positive
                logger.debug(
                    f"Trimming signal for {low_signal.symbol} with score {utility} "
                    f"from queue (size {current_size} > max {self.max_queue_size})"
                )

            # Efficiently remove the lowest priority items
            # Convert heap to list, filter out lowest items, then heapify again
            lowest_items_set = set(lowest_items)  # For efficient lookup
            self.signal_queue = [item for item in self.signal_queue if item not in lowest_items_set]
            heapq.heapify(self.signal_queue)

            logger.debug(
                f"Trimmed signal queue to {len(self.signal_queue)} items "
                f"(max {self.max_queue_size})"
            )
            return True

        return True

    def _calculate_expiration(self, signal: TradeSignal) -> TradeSignal:
        """
        Calculate and set the signal expiration time if not already set.

        Modifies the signal object in place.

        Args:
            signal: Trade signal to potentially modify.

        Returns:
            The modified (or original) TradeSignal object.
        """
        # Base expiration time
        base_expiration_seconds = self.default_expiration_seconds

        expiration_seconds: float = base_expiration_seconds  # Initialize with base

        # Adjust based on signal confidence if available
        if signal.metadata and "confidence_score" in signal.metadata:
            # Get confidence score and ensure it's a float
            confidence_raw = signal.metadata["confidence_score"]
            try:
                # Handle if confidence is a string or another type
                confidence = float(confidence_raw) if confidence_raw is not None else 0.5
                # Ensure confidence is between 0 and 1
                confidence = max(0.0, min(1.0, confidence))
                # Lower confidence = shorter expiration
                confidence_factor = 0.5 + confidence * 0.5  # Range: 0.5 - 1.0
                expiration_seconds = base_expiration_seconds * confidence_factor
            except (ValueError, TypeError):
                # If conversion fails, use the base expiration time
                self.logger.warning(
                    f"Invalid confidence_score '{confidence_raw}' for signal {signal.symbol}. "
                    f"Using default expiration time."
                )
                expiration_seconds = base_expiration_seconds
        else:
            expiration_seconds = base_expiration_seconds

        # Only set expiration if it's not already set
        if signal.expiration is None:
            # Create expiration time using timezone-aware datetime
            # Ensure expiration_seconds is float before passing to timedelta
            expiration_time = datetime.now(UTC) + timedelta(seconds=float(expiration_seconds))
            signal.expiration = expiration_time
            self.logger.debug(
                f"Calculated expiration for signal {signal.signal_id}: {expiration_time}"
            )
        else:
            self.logger.debug(
                f"Expiration already set for signal {signal.signal_id}: {signal.expiration}"
            )

        return signal  # Return the modified (or original) signal

    def _check_circuit_breakers_pre_add(self, signal: TradeSignal) -> bool:
        """Check circuit breakers before adding a signal to the queue."""
        if not self.circuit_breaker_system:
            return True  # No CB system, always allow

        exchanges_to_check: set[str] = set()
        metadata = signal.metadata or {}
        long_exchange = metadata.get("long_exchange")
        short_exchange = metadata.get("short_exchange")

        if long_exchange:
            exchanges_to_check.add(long_exchange)
        if short_exchange:
            exchanges_to_check.add(short_exchange)

        # Fallback: Infer exchange from symbol if metadata is missing
        if not exchanges_to_check:
            inferred_exchange = self._infer_exchange_from_symbol(signal.symbol)
            if inferred_exchange:
                exchanges_to_check.add(str(inferred_exchange))
                self.logger.debug(
                    f"Inferred exchange '{inferred_exchange}' from symbol {signal.symbol} "
                    f"for pre-add CB check."
                )
            else:
                self.logger.warning(
                    f"Cannot determine exchange for pre-add circuit breaker check on signal "
                    f"{signal.symbol}. Allowing signal."
                )
                return True  # Allow signal if exchange unknown, can't check CB

        # Check breakers for relevant exchanges
        for ex in exchanges_to_check:
            exchange_id: str = ex  # Use a new variable name
            # Only check if breaker is OPEN. We don't care about HALF_OPEN here.
            # Get the exchange API error breaker first
            api_breaker = self.circuit_breaker_system.get_exchange_breaker(
                exchange_id, "api_errors"
            )
            if api_breaker and api_breaker.state == BreakerState.OPEN:
                self.logger.warning(
                    f"Pre-add check: Signal for {signal.symbol} rejected. Exchange {exchange_id} "
                    f"API circuit breaker is OPEN: {api_breaker.trip_reason}"
                )
                return False  # Reject if API breaker is OPEN

            # Check other potential breakers like volatility
            volatility_breaker = self.circuit_breaker_system.get_exchange_breaker(
                exchange_id, f"{signal.symbol}_volatility"
            )
            if volatility_breaker and volatility_breaker.state == BreakerState.OPEN:
                self.logger.warning(
                    f"Pre-add check: Signal for {signal.symbol} rejected. Exchange {exchange_id} "
                    f"volatility circuit breaker is OPEN: {volatility_breaker.trip_reason}"
                )
                return False  # Reject if volatility breaker is OPEN

        return True  # Allow if all relevant breakers are CLOSED or HALF_OPEN

    def _check_circuit_breakers_post_get(self, signal: TradeSignal) -> bool:
        """Check circuit breakers just before returning a signal from get_next_signal."""
        if not self.circuit_breaker_system:
            return True  # No CB system, always allow

        exchanges_to_check: set[str] = set()
        metadata = signal.metadata or {}
        long_exchange = metadata.get("long_exchange")
        short_exchange = metadata.get("short_exchange")

        if long_exchange:
            exchanges_to_check.add(long_exchange)
        if short_exchange:
            exchanges_to_check.add(short_exchange)

        # Fallback: Infer exchange from symbol if metadata is missing
        if not exchanges_to_check:
            inferred_exchange = self._infer_exchange_from_symbol(signal.symbol)
            if inferred_exchange:
                exchanges_to_check.add(str(inferred_exchange))
                self.logger.debug(
                    f"Inferred exchange '{inferred_exchange}' from symbol {signal.symbol} "
                    f"for post-get CB check."
                )
            else:
                self.logger.warning(
                    f"Cannot determine exchange for post-get circuit breaker check on signal "
                    f"{signal.symbol}. Allowing signal."
                )
                return True  # Allow signal if exchange unknown

        # Check breakers using can_execute, which handles HALF_OPEN state
        for ex in exchanges_to_check:
            exchange_id: str = ex  # Use new variable name
            can_exec, reason = self.circuit_breaker_system.can_execute(exchange_id, signal.symbol)
            if not can_exec:
                self.logger.warning(
                    f"Post-get check: Signal for {signal.symbol} blocked by exchange {exchange_id} "
                    f"circuit breaker: {reason}"
                )
                return False  # Block if execution not allowed

        return True  # Allow if all relevant breakers allow execution

    def _infer_exchange_from_symbol(self, symbol: str) -> str | None:
        """
        Attempt to infer the exchange based on the symbol format.

        This method tries to extract exchange information from the symbol formatting.
        Different exchanges use different symbol formats.

        Args:
            symbol: The trading symbol to analyze

        Returns:
            Inferred exchange name or None if inference fails
        """
        if not symbol:
            return None

        # Try different exchange-specific symbol formats
        symbol = symbol.strip().upper()

        # Format: EXCHANGE-SYMBOL-PERP (e.g., HYPERLIQUID-BTC-PERP)
        if "-PERP" in symbol:
            parts = symbol.split("-")
            if len(parts) == 3 and parts[0]:
                return parts[0].lower()
            return "hyperliquid"  # Default assumption for perp contracts

        # Format: EXCHANGE-SYMBOL (e.g., BINANCE-BTCUSDT)
        if "-" in symbol:
            parts = symbol.split("-", 1)
            if len(parts) == 2 and parts[0]:
                return parts[0].lower()

        # Format: SYMBOL_EXCHANGE (e.g., BTC_BACKPACK)
        if "_" in symbol:
            parts = symbol.split("_")
            if len(parts) == 2 and parts[1]:
                return parts[1].lower()
            return "backpack"  # Example assumption

        # Format: SYMBOL:EXCHANGE (e.g., BTC:DYDX)
        if ":" in symbol:
            parts = symbol.split(":")
            if len(parts) == 2 and parts[1]:
                return parts[1].lower()

        # Try to extract common exchange names from the symbol
        common_exchanges = [
            "binance",
            "coinbase",
            "bybit",
            "okx",
            "kucoin",
            "dydx",
            "hyperliquid",
            "backpack",
            "kraken",
            "huobi",
        ]

        for exchange in common_exchanges:
            if exchange.lower() in symbol.lower():
                return exchange.lower()

        # If no exchange could be inferred
        self.logger.debug(f"Could not infer exchange from symbol: {symbol}")
        return None

    def get_pending_signals(self) -> list[TradeSignal]:
        """Get a list of all signals currently pending in the queue."""
        # Use the threading Lock for synchronous code
        with self._sync_lock:
            # Create a sorted list for a snapshot view
            # Type result explicitly
            pending_signals: list[TradeSignal] = [
                signal
                for _score, _count, signal in sorted(
                    list(self.signal_queue), key=lambda x: (x[0], x[1])
                )
                if signal.is_valid()
            ]
        return pending_signals

    def get_signal_count(self) -> int:
        """Get the number of signals currently in the queue."""
        # Remove expired signals
        now = datetime.now(UTC)
        valid_signals: list[tuple[float, int, TradeSignal]] = []
        while self.signal_queue:
            score, count, signal = heapq.heappop(self.signal_queue)
            if True and (signal.expiration is None or signal.expiration > now):
                valid_signals.append((score, count, signal))
        self.signal_queue = valid_signals
        return len(self.signal_queue)

    def _process_priority_levels(self) -> None:
        """Internal method to process signals based on priority levels."""
        processed_signals: list[TradeSignal] = []
        while self.signal_queue:
            _score, _count, signal = heapq.heappop(self.signal_queue)
            # Assuming this method is meant to process ALL signals regardless of the `True` check
            processed_signals.append(signal)
            self.logger.debug(f"Processing signal: {signal.signal_type.name} for {signal.symbol}")
            # TODO: Add actual processing/validation logic here if this method is used.
            # The `if True:` block was likely placeholder/dead code.

    def _add_signal(self, signal: TradeSignal, priority_score: float) -> None:
        """Adds a signal to the internal queue with appropriate priority."""
        # Use a counter to maintain FIFO for signals with the same priority/timestamp
        current_count = self.counter
        self.counter += 1
        timestamp = signal.timestamp or datetime.now(UTC)

        # Create a new TradeSignal instance with direct parameter passing
        # instead of using dictionary unpacking which causes type issues
        # Remove redundant None checks as price/quantity are Decimals
        price_decimal = signal.price
        quantity_decimal = signal.quantity
        source_strategy = getattr(signal, "source_strategy", None)
        stop_loss = getattr(signal, "stop_loss", None)
        take_profit = getattr(signal, "take_profit", None)
        expiration = getattr(signal, "expiration", None)
        metadata_dict = getattr(signal, "metadata", None)
        # Extract exchange from the original signal, ensuring it's a string
        exchange_name_raw = getattr(signal, "exchange", None)
        if not isinstance(exchange_name_raw, str):
            # Handle missing or invalid exchange - perhaps log and skip?
            # For now, defaulting to an empty string or raising might be options.
            # Let's log a warning and potentially skip or use a default.
            logger.warning(
                f"Signal missing or invalid exchange: {signal}. Defaulting to 'unknown'."
            )
            exchange_name = "unknown"  # Default or raise error
        else:
            exchange_name = exchange_name_raw

        new_signal = TradeSignal(
            symbol=signal.symbol,
            signal_type=signal.signal_type,
            side=signal.side,
            price=price_decimal,
            quantity=quantity_decimal,
            timestamp=timestamp,
            exchange=exchange_name,  # Add missing exchange
            confidence=signal.confidence,
            source_strategy=source_strategy,
            stop_loss=stop_loss,
            take_profit=take_profit,
            expiration=expiration,
            metadata=metadata_dict,
        )

        # Priority: Lower score means higher priority
        # Timestamp: Earlier timestamp means higher priority (for same score)
        # Count: Ensures FIFO for exact same score/timestamp
        heap_item = (-priority_score, timestamp, current_count, new_signal)

        # Ensure the signal_heap attribute exists before using it
        if not hasattr(self, "signal_heap"):
            self.signal_heap = []

        heapq.heappush(self.signal_heap, heap_item)
        self.logger.debug(
            f"Added signal to queue: symbol={new_signal.symbol}, "
            f"type={new_signal.signal_type.name}, priority={priority_score}"
        )

    def wait_for_signals(
        self, timeout: float | None = None, max_signals: int = 1
    ) -> list[TradeSignal]:
        """
        Waits for signals to become available and returns up to max_signals highest
        priority signals.

        Args:
            timeout: Maximum time to wait in seconds (None = wait indefinitely)
            max_signals: Maximum number of signals to return

        Returns:
            List of TradeSignal objects ordered by priority
        """
        # First see if we already have signals available
        result = self.pop_signals(max_signals)
        if result:
            return result

        # Wait for the signal event
        # This is used in an async context, so we can't use .wait() directly
        # This would need to be awaited in an async function
        if self.new_signal_event.is_set():
            # Event is already set, signals should be available
            return self.pop_signals(max_signals)
        else:
            # Can't wait in a synchronous context
            self.logger.debug("Cannot wait for signals in synchronous context")
            return []

    def pop_signals(self, max_signals: int = 1) -> list[TradeSignal]:
        """
        Removes and returns up to max_signals highest priority signals from the queue.

        Args:
            max_signals: Maximum number of signals to remove and return

        Returns:
            List of TradeSignal objects ordered by priority
        """
        self._clean_expired_signals()

        result: list[TradeSignal] = []

        # Use the threading Lock for synchronous code
        with self._sync_lock:
            # Return empty list if no signals
            if not self.signal_heap:
                self.logger.debug("No signals available in queue to pop")
                return []

            # Pop up to max_signals from the heap
            remaining = min(max_signals, len(self.signal_heap))

            for _ in range(remaining):
                if not self.signal_heap:
                    break

                _priority, _timestamp, _count, popped_signal = heapq.heappop(self.signal_heap)
                # The check is redundant as self.signal_heap contains TradeSignals
                result.append(popped_signal)

            # If we emptied the heap, clear the event
            if not self.signal_heap:
                self.new_signal_event.clear()

        if result:
            self.logger.info(f"Popped {len(result)} signals from queue")

        return result

    def get_next_signals(self, max_count: int = 1) -> list[TradeSignal]:
        """
        Get multiple highest priority unexpired signals and remove them from the queue.

        Args:
            max_count: Maximum number of signals to return

        Returns:
            List of valid trade signals in priority order
        """
        result: list[TradeSignal] = []

        # Clean expired signals before processing
        self._clean_expired_signals()

        while self.signal_queue and len(result) < max_count:
            signal = self.get_next_signal()
            if signal is not None:
                result.append(signal)

        return result

    def _check_circuit_breakers(self, signal: TradeSignal) -> bool:
        """Check if a signal is blocked by circuit breakers."""
        try:
            # If no circuit breaker system is configured, allow all signals
            if not self.circuit_breaker_system:
                return True

            # Initialize an empty dict if metadata is None
            metadata = signal.metadata or {}

            # Determine relevant exchange(s) from metadata
            exchanges_to_check: set[str] = set()
            exchange = metadata.get("exchange")
            long_exchange = metadata.get("long_exchange")
            short_exchange = metadata.get("short_exchange")

            if exchange:
                exchanges_to_check.add(str(exchange))
            if long_exchange:
                exchanges_to_check.add(str(long_exchange))
            if short_exchange:
                exchanges_to_check.add(str(short_exchange))

            # If no exchanges were found in metadata, try to infer from symbol
            if not exchanges_to_check:
                inferred_exchange = self._infer_exchange_from_symbol(signal.symbol)
                if inferred_exchange:
                    exchanges_to_check.add(str(inferred_exchange))
                    self.logger.debug(
                        f"Inferred exchange '{inferred_exchange}' from symbol {signal.symbol} "
                        f"for circuit breaker check."
                    )
                else:
                    self.logger.warning(
                        f"Cannot determine exchange for circuit breaker check on signal "
                        f"{signal.symbol}. Skipping check."
                    )
                    return True  # Allow signal if exchange unknown

            # Check each relevant exchange and the specific symbol
            for ex in exchanges_to_check:
                exchange_id: str = ex  # Use new variable name
                can_exec, reason = self.circuit_breaker_system.can_execute(
                    exchange_id, signal.symbol
                )
                if not can_exec:
                    self.logger.warning(
                        f"Signal for {signal.symbol} on exchange {exchange_id} blocked by "
                        f"circuit breaker: {reason}"
                    )
                    return False

            # All checks passed
            return True

        except Exception as e:
            self.logger.error(f"Error checking circuit breakers: {e}")
            # Fail safe on error
            return False

    def is_empty(self) -> bool:
        """
        Check if the queue is empty.

        Returns:
            True if the queue is empty, False otherwise
        """
        # Clean expired signals first
        self._clean_expired_signals()
        return len(self.signal_queue) == 0

    async def enqueue_signal(self, signal: TradeSignal) -> None:
        """
        Asynchronously enqueue a trade signal into the priority queue and notify listeners.

        Args:
            signal: TradeSignal to enqueue.
        """
        async with self.lock:
            added = self.add_signal(signal)
            if added:
                self.new_signal_event.set()
                self.logger.info(f"Enqueued signal for {signal.symbol} (async)")
            else:
                self.logger.warning(f"Failed to enqueue signal for {signal.symbol} (async)")

    async def run(self, cancellation_token: asyncio.Event) -> None:
        """
        Asynchronous run loop for the signal queue. Waits for new signals and processes them.

        Args:
            cancellation_token: An asyncio.Event used to signal shutdown.
        """
        self.logger.info("PrioritySignalQueue run loop started.")
        try:
            while not cancellation_token.is_set():
                await self.new_signal_event.wait()
                # Placeholder: In a real system, process signals here
                self.logger.debug("Signal event triggered. (Processing logic TBD)")
                self.new_signal_event.clear()
        except asyncio.CancelledError:
            self.logger.info("PrioritySignalQueue run loop cancelled.")
        finally:
            self.logger.info("PrioritySignalQueue run loop stopped.")

    async def process_signal(self) -> TradeSignal | None:
        # Implementation of process_signal method
        pass

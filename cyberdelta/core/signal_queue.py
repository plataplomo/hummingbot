from __future__ import annotations  # Enable postponed evaluation

"""
Priority Signal Queue for trade signals.

This module implements a priority queue for trade signals with expiration
handling, supporting the efficient management of trading opportunities based
on their utility scores and other attributes.
"""

import asyncio
import heapq
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING  # Added TYPE_CHECKING

# from cyberdelta.validation.funding_data import ArbitrageOpportunity # Moved below
from cyberdelta.core.models import ArbitrageOpportunity, OrderSide, SignalType, TradeSignal
from cyberdelta.utils.config import Config

# from cyberdelta.core.models import SignalType, TradeSignal # Moved below
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem

if TYPE_CHECKING:
    from cyberdelta.core.models import SignalType, TradeSignal
    from cyberdelta.validation.funding_data import ArbitrageOpportunity

# Setup logging
logger = logging.getLogger(__name__)


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
        self.logger = logging.getLogger(
            f"{__name__}.{self.__class__.__name__}"
        )  # Initialize logger
        self.circuit_breaker = circuit_breaker_system

        # Priority queue: [(negative_utility_score, unique_id, signal)]
        # Using negative utility score for max-heap behavior
        self.signal_queue: list[tuple[float, int, TradeSignal]] = []

        # Counter for generating unique IDs
        self.counter = 0

        # Queue parameters
        self.default_expiration_seconds = config.get("default_signal_expiration_seconds", 300)
        self.max_queue_size = config.get("max_signal_queue_size", 100)
        self.cleanup_interval = config.get("queue_cleanup_interval", 10)
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

        # Set expiration time if not already set
        if signal.expiration is None:
            signal.expiration = self._calculate_expiration(signal)

        # Check circuit breakers before adding
        if self.circuit_breaker_system and not self._check_circuit_breakers(signal):
            logger.warning(f"Circuit breaker active for {signal.symbol}, rejecting signal")
            return False

        # Clean expired signals periodically
        now = datetime.now(UTC)
        if (now - self.last_cleanup).total_seconds() > self.cleanup_interval:
            self._clean_expired_signals()
            self.last_cleanup = now

        # First, check if we need to trim the queue
        if len(self.signal_queue) >= self.max_queue_size:
            # Check if new signal has higher utility than the lowest in queue
            if len(self.signal_queue) > 0:
                # Find minimum utility score (maximum negative score since we use negative values)
                min_score = min(self.signal_queue, key=lambda x: x[0])[0]

                # If new signal has lower utility than the lowest, reject it
                if -signal.metadata["utility_score"] >= min_score:
                    logger.debug(
                        f"Rejected signal for {signal.symbol} with score "
                        f"{signal.metadata['utility_score']} (lower than min {-min_score})"
                    )
                    return False

                # Otherwise, remove the lowest utility signal
                self._trim_queue()

        # Add to priority queue
        self.counter += 1
        heapq.heappush(self.signal_queue, (-signal.metadata["utility_score"], self.counter, signal))

        logger.debug(
            f"Added signal for {signal.symbol} to queue with score "
            f"{signal.metadata['utility_score']}"
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
            "expected_profit": str(
                getattr(opportunity, "expected_profit", Decimal("0"))
            ),  # Store as str
            "basis_volatility": getattr(opportunity, "basis_volatility", None),
            "long_exchange": opportunity.long_exchange,
            "short_exchange": opportunity.short_exchange,
            "long_funding_rate": str(
                getattr(opportunity, "long_funding_rate", None)
            ),  # Store as str
            "short_funding_rate": str(
                getattr(opportunity, "short_funding_rate", None)
            ),  # Store as str
            "net_funding_differential": str(
                getattr(opportunity, "net_funding_differential", None)
            ),  # Store as str
            # Add other relevant opportunity details if needed
        }

        # Create signal
        signal = TradeSignal(
            source_strategy=strategy_name,
            symbol=symbol,
            signal_type=signal_type,
            side=side,
            timestamp=timestamp,
            price=None,  # Price might be determined later or based on execution
            quantity=None,  # Quantity determined by RiskManager
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

        # Check if queue is empty
        if not self.signal_queue:
            return None

        # Get highest priority signal
        _, _, signal = heapq.heappop(self.signal_queue)

        # Verify signal is still valid
        if signal.is_valid():
            # Double-check circuit breakers before returning
            if self.circuit_breaker_system and not self._check_circuit_breakers(signal):
                logger.warning(f"Circuit breaker active for {signal.symbol}, skipping signal")
                return self.get_next_signal()  # Recursively get next signal

            return signal

        # Invalid signal, try next one
        logger.warning(f"Retrieved invalid signal for {signal.symbol}, trying next")
        return self.get_next_signal()

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

        # Verify signal is still valid
        if signal.is_valid():
            return signal

        # Invalid signal, clean and try again
        self._clean_expired_signals()
        return self.peek_next_signal() if self.signal_queue else None

    def get_signals(self, max_count: int = 10) -> list[TradeSignal]:
        """
        Get multiple signals in priority order.

        Args:
            max_count: Maximum number of signals to return

        Returns:
            List of signals in priority order
        """
        result = []
        temp_queue = self.signal_queue.copy()

        # Clean expired signals
        self._clean_expired_signals()

        # Get signals
        while temp_queue and len(result) < max_count:
            _, _, signal = heapq.heappop(temp_queue)
            if signal.is_valid():
                if not self.circuit_breaker_system or self._check_circuit_breakers(signal):
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

        # Filter out expired signals
        valid_signals = []
        for score, count, signal in self.signal_queue:
            if signal.is_valid():
                valid_signals.append((score, count, signal))

        # Rebuild queue if any signals were removed
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
        if len(self.signal_queue) <= self.max_queue_size:
            return False

        # Remove lowest priority signals until we're at max size
        while len(self.signal_queue) > self.max_queue_size:
            # Find the item with lowest priority (highest negative score)
            lowest_priority_idx = max(
                range(len(self.signal_queue)), key=lambda i: self.signal_queue[i][0]
            )

            # Remove the item
            self.signal_queue.pop(lowest_priority_idx)

        # Re-heapify the queue
        heapq.heapify(self.signal_queue)

        logger.debug(f"Trimmed signal queue to {self.max_queue_size} items")
        return True

    def _calculate_expiration(self, signal: TradeSignal) -> datetime:
        """
        Calculate signal expiration time.

        Args:
            signal: Trade signal

        Returns:
            Expiration datetime
        """
        # Base expiration time
        base_expiration_seconds = self.default_expiration_seconds

        # Adjust based on signal confidence if available
        if signal.metadata and "confidence_score" in signal.metadata:
            confidence = signal.metadata["confidence_score"]
            # Lower confidence = shorter expiration
            confidence_factor = 0.5 + confidence * 0.5  # Range: 0.5 - 1.0
            expiration_seconds = base_expiration_seconds * confidence_factor
        else:
            expiration_seconds = base_expiration_seconds

        # Create expiration time using timezone-aware datetime
        return datetime.now(UTC) + timedelta(seconds=expiration_seconds)

    def _check_circuit_breakers(self, signal: TradeSignal) -> bool:
        """
        Check if any circuit breakers are active for this signal.

        Args:
            signal: Trade signal to check

        Returns:
            True if no circuit breakers are active, False otherwise
        """
        if not self.circuit_breaker_system:
            return True

        try:
            # Check circuit breakers
            if self.circuit_breaker:
                exchange = signal.metadata.get("exchange")  # Attempt to get exchange from metadata
                if not exchange:
                    # Try inferring from symbol if possible (e.g., "EXCHANGE-SYMBOL")
                    parts = signal.symbol.split("-", 1)
                    if len(parts) == 2:
                        exchange = parts[0].lower()  # Assume first part is exchange
                    else:
                        logger.warning(
                            f"Cannot determine exchange for circuit breaker check on signal {signal.symbol}. Skipping check."
                        )
                        return True  # Allow signal if exchange unknown

                # Use check_symbol which implicitly checks exchange and global
                can_exec, reason = self.circuit_breaker.can_execute(exchange, signal.symbol)
                if not can_exec:
                    self.logger.warning(
                        f"Signal for {signal.symbol} blocked by circuit breaker: {reason}"
                    )
                    return False

            # All checks passed
            return True

        except Exception as e:
            logger.error(f"Error checking circuit breakers: {e}")
            # Fail safe on error
            return False

    def get_pending_signals(self) -> list[TradeSignal]:
        """Get a list of all signals currently pending in the queue."""
        result: list[TradeSignal] = []
        with self.lock:
            # Create a sorted list for a snapshot view
            sorted_heap = sorted(list(self.signal_queue), key=lambda x: (x[0], x[1]))
            result = [signal for score, count, signal in sorted_heap if signal.is_valid()]
        return result

    def get_signal_count(self) -> int:
        """Get the number of signals currently in the queue."""
        # Remove expired signals
        now = datetime.now(UTC)
        valid_signals = []
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
            score, count, signal = heapq.heappop(self.signal_queue)
            if True:
                processed_signals.append(signal)
                self.logger.debug(
                    f"Processing signal: {signal.signal_type.name} for {signal.symbol}"
                )
                # TODO: Validate signal against risk limits, portfolio state, etc.
                # Example: if not self.risk_manager.is_signal_safe(signal):
                #              continue

                # Temporary: Create TradeSignal with potentially incorrect args (will fix)
                # Removed strategy_name kwarg
                new_signal = TradeSignal(
                    symbol=signal.symbol,
                    signal_type=signal.signal_type,
                    side=signal.side,
                    # Ensure price is Decimal
                    price=Decimal(str(signal.price)) if signal.price is not None else None,
                    quantity=signal.quantity,
                    timestamp=datetime.now(UTC),
                    confidence=signal.confidence,
                    source_strategy=signal.source_strategy,  # Use source_strategy if available
                    stop_loss=signal.stop_loss,
                    take_profit=signal.take_profit,
                    expiration=signal.expiration,
                    metadata=signal.metadata,
                )

                # Check if the signal is valid (example - replace with actual validation)
                # Removed: if signal.is_valid():
                if True:  # Placeholder for actual validation
                    self.logger.info(
                        "Adding valid signal to processing queue",
                        signal_type=signal.signal_type,
                        symbol=signal.symbol,
                        price=signal.price,
                        quantity=signal.quantity,
                        timestamp=signal.timestamp,
                        confidence=signal.confidence,
                        source_strategy=signal.source_strategy,
                        stop_loss=signal.stop_loss,
                        take_profit=signal.take_profit,
                        expiration=signal.expiration,
                        metadata=signal.metadata,
                    )
                    # ... existing code ...

    def _add_signal(self, signal: TradeSignal, priority_score: float) -> None:
        """Adds a signal to the internal queue with appropriate priority."""
        with self.lock:
            # Use a counter to maintain FIFO for signals with the same priority/timestamp
            count = next(self.counter)
            timestamp = signal.timestamp or datetime.now(UTC)

            # Create a new TradeSignal instance, mapping fields correctly
            # Ensure required fields are present in the input 'signal' object
            # Note: Removed 'strategy_name' kwarg, assuming source_strategy is used
            new_signal_data = {
                "symbol": signal.symbol,
                "signal_type": signal.signal_type,
                "side": signal.side,
                "price": Decimal(str(signal.price)) if signal.price is not None else None,
                "quantity": signal.quantity,
                "timestamp": timestamp,  # Use the determined timestamp
                "confidence": signal.confidence,
                "source_strategy": getattr(
                    signal, "source_strategy", None
                ),  # Safely get source_strategy
                "stop_loss": getattr(signal, "stop_loss", None),
                "take_profit": getattr(signal, "take_profit", None),
                "expiration": getattr(signal, "expiration", None),
                "metadata": getattr(signal, "metadata", None),
            }
            # Filter out None values if the dataclass expects non-optional or specific defaults
            # For now, assume TradeSignal handles Nones correctly
            new_signal = TradeSignal(**new_signal_data)

            # Priority: Lower score means higher priority
            # Timestamp: Earlier timestamp means higher priority (for same score)
            # Count: Ensures FIFO for exact same score/timestamp
            heap_item = (-priority_score, timestamp, count, new_signal)
            heapq.heappush(self.signal_heap, heap_item)
            self.logger.debug(
                "Added signal to queue",
                symbol=new_signal.symbol,
                type=new_signal.signal_type,
                priority=priority_score,
            )

    async def wait_for_signal(self, timeout: float | None = None) -> TradeSignal | None:
        """Waits for a signal to become available in the queue."""
        # ... (existing implementation)
        processed_signal = None
        try:
            # Wait for the event or timeout
            await asyncio.wait_for(self.new_signal_event.wait(), timeout=timeout)

            # Retrieve the highest priority signal
            with self.lock:
                if self.signal_heap:
                    priority, timestamp, count, signal = heapq.heappop(self.signal_heap)
                    # Check validity (placeholder)
                    # Removed: if signal.is_valid():
                    if True:  # Placeholder for validation
                        processed_signal = signal
                        self.logger.debug(
                            "Retrieved signal from queue",
                            symbol=signal.symbol,
                            type=signal.signal_type,
                        )
                    else:
                        self.logger.warning("Skipping invalid signal", signal=signal)
                        # Potentially re-push valid signals if needed, or handle invalid signals
                # Reset the event if the queue is now empty
                if not self.signal_heap:
                    self.new_signal_event.clear()

        except TimeoutError:
            self.logger.debug("Timeout waiting for signal")
            return None

        return processed_signal

    def get_prioritized_signals(self, max_signals: int = 1) -> list[TradeSignal]:
        """Retrieves a list of the highest priority signals without waiting."""
        signals = []
        with self.lock:
            # Pop up to max_signals items
            count = 0
            while self.signal_heap and count < max_signals:
                priority, timestamp, heap_count, signal = heapq.heappop(self.signal_heap)
                # Check validity (placeholder)
                # Removed: if signal.is_valid():
                if True:  # Placeholder for validation
                    signals.append(signal)
                    count += 1
                else:
                    self.logger.warning("Skipping invalid signal during get", signal=signal)

            # If we emptied the heap, clear the event
            if not self.signal_heap:
                self.new_signal_event.clear()

        return signals

    async def clear_expired_signals(self) -> None:
        """Periodically remove expired signals from the queue."""
        while True:
            # Sleep for the configured interval
            await asyncio.sleep(self.cleanup_interval)
            removed_count = 0
            with self.lock:
                now = datetime.now(UTC)
                valid_signals_heap = []
                while self.signal_queue:
                    score, count, signal = heapq.heappop(self.signal_queue)
                    is_expired = signal.expiration is not None and signal.expiration <= now
                    # Placeholder validity check
                    is_valid_check = signal.is_valid()

                    if is_valid_check and not is_expired:
                        heapq.heappush(valid_signals_heap, (score, count, signal))
                    else:
                        removed_count += 1
                        self.logger.debug(
                            "Removing signal",
                            reason="Expired" if is_expired else "Invalid",
                            signal=signal,
                        )
                self.signal_queue = valid_signals_heap
            if removed_count > 0:
                self.logger.info(f"Cleared {removed_count} expired/invalid signals.")

    def _check_circuit_breakers(self, signal: TradeSignal) -> bool:
        """Check if a signal is blocked by circuit breakers."""
        try:
            # Check circuit breakers using the assigned instance attribute
            if self.circuit_breaker:
                exchange = signal.metadata.get("exchange")
                long_exchange = signal.metadata.get("long_exchange")
                short_exchange = signal.metadata.get("short_exchange")

                # Determine relevant exchange(s)
                exchanges_to_check = set()
                if exchange:
                    exchanges_to_check.add(exchange)
                if long_exchange:
                    exchanges_to_check.add(long_exchange)
                if short_exchange:
                    exchanges_to_check.add(short_exchange)

                if not exchanges_to_check:
                    # Try to infer from symbol if possible
                    parts = signal.symbol.split("-", 1)
                    if len(parts) == 2:
                        inferred_exchange = parts[0].lower()
                        exchanges_to_check.add(inferred_exchange)
                        self.logger.debug(
                            f"Inferred exchange '{inferred_exchange}' from symbol {signal.symbol} for CB check."
                        )
                    else:
                        self.logger.warning(
                            f"Cannot determine exchange for circuit breaker check on signal {signal.symbol}. Skipping check."
                        )
                        return True  # Allow signal if exchange unknown

                # Check each relevant exchange and the specific symbol
                for ex in exchanges_to_check:
                    can_exec, reason = self.circuit_breaker.can_execute(ex, signal.symbol)
                    if not can_exec:
                        self.logger.warning(
                            f"Signal for {signal.symbol} on exchange {ex} blocked by circuit breaker: {reason}"
                        )
                        return False

            # All checks passed
            return True

        except Exception as e:
            self.logger.error(f"Error checking circuit breakers: {e}")
            # Fail safe on error
            return False

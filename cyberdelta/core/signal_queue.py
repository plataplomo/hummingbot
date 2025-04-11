"""
Priority Signal Queue for trade signals.

This module implements a priority queue for trade signals with expiration
handling, supporting the efficient management of trading opportunities based
on their utility scores and other attributes.
"""

import heapq
import logging
from datetime import datetime, timedelta
from typing import Any

from cyberdelta.core.types import SignalType, TradeSignal
from cyberdelta.validation.funding_data import ArbitrageOpportunity

logger = logging.getLogger(__name__)


class PrioritySignalQueue:
    """
    Priority queue for trade signals with expiration handling.

    This class manages trade signals ordered by their utility scores,
    automatically handling signal expiration and integration with safety systems.
    """

    def __init__(self, config: dict[str, Any], circuit_breaker_system: Any | None = None) -> None:
        """
        Initialize the priority signal queue.

        Args:
            config: Configuration parameters
            circuit_breaker_system: Optional circuit breaker system for safety checks
        """
        self.config = config
        self.circuit_breaker_system = circuit_breaker_system

        # Priority queue: [(negative_utility_score, unique_id, signal)]
        # Using negative utility score for max-heap behavior
        self.signal_queue: list[tuple[float, int, TradeSignal]] = []

        # Counter for generating unique IDs
        self.counter = 0

        # Queue parameters
        self.default_expiration_seconds = config.get("default_signal_expiration_seconds", 300)
        self.max_queue_size = config.get("max_signal_queue_size", 100)
        self.cleanup_interval = config.get("queue_cleanup_interval", 10)
        self.last_cleanup = datetime.now()

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
        now = datetime.now()
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
        # Create signal from opportunity
        signal = TradeSignal(
            strategy_name=strategy_name,
            symbol=opportunity.symbol,
            signal_type=SignalType.ENTER_LONG,  # TODO: Adjust based on opportunity
            timestamp=datetime.now(),
            price=0.0,  # Should be set by the caller
            expiration=opportunity.expiration if hasattr(opportunity, 'expiration') else None,
            metadata={
                "utility_score": opportunity.utility_score,
                "confidence_score": opportunity.confidence_score if hasattr(opportunity, 'confidence_score') else None,
                "expected_profit": opportunity.expected_profit,
                "basis_volatility": opportunity.basis_volatility,
                "long_exchange": opportunity.long_exchange,
                "short_exchange": opportunity.short_exchange,
                "long_funding_rate": opportunity.long_funding_rate,
                "short_funding_rate": opportunity.short_funding_rate,
                "net_funding_differential": opportunity.net_funding_differential,
                # "adjusted_thresholds": opportunity.adjusted_thresholds, # Attribute might not exist
            },
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

        # Create expiration time
        return datetime.now() + timedelta(seconds=expiration_seconds)

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
            # Check exchange circuit breakers
            if signal.metadata and "long_exchange" in signal.metadata:
                long_exchange = signal.metadata["long_exchange"]
                if not self.circuit_breaker_system.check_exchange(long_exchange):
                    return False

            if signal.metadata and "short_exchange" in signal.metadata:
                short_exchange = signal.metadata["short_exchange"]
                if not self.circuit_breaker_system.check_exchange(short_exchange):
                    return False

            # Check symbol circuit breakers
            if not self.circuit_breaker_system.check_symbol(signal.symbol):
                return False

            # All checks passed
            return True

        except Exception as e:
            logger.error(f"Error checking circuit breakers: {e}")
            # Fail safe on error
            return False

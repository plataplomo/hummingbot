"""Priority Signal Queue for trade signals.

This module implements a priority queue for trade signals with expiration
handling, supporting the efficient management of trading opportunities based
on their utility scores and other attributes.
"""

from __future__ import annotations  # Enable postponed evaluation

import asyncio
import heapq
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.config_models import AppSettings
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models import OrderSide, TradeSignal
from cyberdelta.core.models.enums import SignalType
from cyberdelta.validation.circuit_breaker import BreakerState, CircuitBreaker, CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity


if TYPE_CHECKING:
    from cyberdelta.core.models import SignalType, TradeSignal

# Setup logging
logger = get_logger(__name__)


class PrioritySignalQueue:
    """Priority queue for trade signals with expiration handling.

    This class manages trade signals ordered by their utility scores,
    automatically handling signal expiration and integration with safety systems.
    """

    def __init__(
        self,
        app_settings: AppSettings,
        circuit_breaker_system: CircuitBreakerSystem | None = None,
    ) -> None:
        """Initialize the priority signal queue.

        Args:
            app_settings: AppSettingsuration parameters
            circuit_breaker_system: Optional circuit breaker system for safety checks

        """
        self.app_settings = app_settings
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
        self.lock = asyncio.Lock()  # For async methods
        self.new_signal_event = asyncio.Event()  # For async signaling

        # Counter for generating unique IDs
        self.counter = 0

        # Configuration values from AppSettings
        # TODO: Add signal queue specific configuration to AppSettings when needed
        # For now, use reasonable defaults since signal queue config is not in current AppSettings
        self.default_expiration_seconds: float = 300.0  # 5 minutes default
        self.max_queue_size: int = 100  # Default max queue size
        self.cleanup_interval: float = 10.0  # Default cleanup interval

        self.last_cleanup = datetime.now(UTC)

        self.logger.info("Initialized priority signal queue")

    async def add_signal(self, signal: TradeSignal) -> bool:
        """Add a signal to the priority queue asynchronously.

        Args:
            signal: Trade signal to add

        Returns:
            True if signal was added, False if rejected

        """
        # Prepare signal metadata and validation
        if not self._prepare_signal_metadata(signal):
            return False

        # Set expiration time if not already set
        if signal.expiration is None:
            self._calculate_expiration(signal)  # Modifies signal in place

        # Check circuit breakers before adding
        if self.circuit_breaker_system and not self._check_circuit_breakers_pre_add(signal):
            return False

        # Clean expired signals periodically
        await self._cleanup_expired_signals_if_needed()

        # Add signal to queue
        return await self._add_signal_to_queue(signal)

    def _prepare_signal_metadata(self, signal: TradeSignal) -> bool:
        """Prepare and validate signal metadata."""
        # Ensure signal.metadata exists for score checking/setting
        if signal.metadata is None:
            signal.metadata = {}

        # Default utility score if missing
        if "utility_score" not in signal.metadata:
            self.logger.warning(
                f"Signal for {signal.symbol} has no utility score, using default 0.0",
            )
            signal.metadata["utility_score"] = 0.0

        # Ensure utility_score is float
        try:
            utility_score = float(signal.metadata["utility_score"])
            signal.metadata["utility_score"] = utility_score  # Store validated float back
        except (ValueError, TypeError, KeyError):  # Added KeyError
            score_val = signal.metadata.get("utility_score", "N/A")  # Use get for safety
            self.logger.warning(
                f"Invalid utility_score '{score_val}' for signal {signal.symbol}. "
                f"Using default 0.0",
            )
            utility_score = 0.0
            signal.metadata["utility_score"] = utility_score  # Store default back

        return True

    async def _cleanup_expired_signals_if_needed(self) -> None:
        """Clean expired signals periodically if needed."""
        now = datetime.now(UTC)
        if (now - self.last_cleanup).total_seconds() > self.cleanup_interval:
            async with self.lock:  # Acquire lock specifically for cleanup
                # Double check condition inside lock
                if (now - self.last_cleanup).total_seconds() > self.cleanup_interval:
                    self._clean_expired_signals()  # This modifies self.signal_queue
                    self.last_cleanup = now

    async def _add_signal_to_queue(self, signal: TradeSignal) -> bool:
        """Add signal to the priority queue with duplicate checking and trimming."""
        # Use asyncio.Lock for adding the signal
        async with self.lock:  # Changed to async with self.lock
            # Check for duplicates before adding
            if self._is_duplicate_signal(signal):
                return False

            # Add to priority queue FIRST
            self._push_signal_to_heap(signal)

            # THEN check if trimming is needed
            trimmed = self._trim_queue_if_needed()
            if trimmed is False:  # Explicit False check for trim failure
                return False

            # Log addition and signal event
            self._log_signal_addition(signal, trimmed)
            self.new_signal_event.set()  # Directly set event in async context

            return True  # Indicate successful addition

    def _is_duplicate_signal(self, signal: TradeSignal) -> bool:
        """Check if signal is a duplicate."""
        existing_ids = {s.signal_id for _, _, s in self.signal_queue if hasattr(s, "signal_id")}
        if hasattr(signal, "signal_id") and signal.signal_id in existing_ids:
            self.logger.debug(f"Rejected duplicate signal ID: {signal.signal_id}")
            return True
        return False

    def _push_signal_to_heap(self, signal: TradeSignal) -> None:
        """Push signal to the heap with proper priority calculation."""
        self.counter += 1
        # Ensure utility_score is float before negation
        try:
            metadata = signal.metadata or {}
            priority = -float(metadata.get("utility_score", 0.0))
        except (ValueError, TypeError):
            priority = 0.0  # Default priority if conversion fails
            self.logger.warning(
                f"Invalid utility score for {signal.symbol}, using 0.0 priority.",
            )

        heapq.heappush(self.signal_queue, (priority, self.counter, signal))

    def _trim_queue_if_needed(self) -> bool | None:
        """Trim queue if needed. Returns True if trimmed, False if failed, None if not needed."""
        if len(self.signal_queue) > self.max_queue_size:
            trimmed = self._trim_queue()  # Trim if queue is over size limit
            if not trimmed:
                self.logger.error(
                    "Failed to trim queue after adding signal. State might be inconsistent.",
                )
                return False  # Indicate potential issue post-add
            return True
        return None  # No trimming needed

    def _log_signal_addition(self, signal: TradeSignal, trimmed: bool | None) -> None:
        """Log the signal addition with appropriate level."""
        log_level = logging.DEBUG if not trimmed else logging.INFO  # Log INFO if trimming occurred
        metadata = signal.metadata or {}
        self.logger.log(
            log_level,
            f"Added signal for {signal.symbol} to queue with score "
            f"{metadata.get('utility_score', 'N/A')}"
            f"{' (and trimmed queue)' if trimmed else ''}",
        )

    async def add_from_opportunity(
        self,
        opportunity: ArbitrageOpportunity,
        strategy_name: str,
    ) -> TradeSignal | None:
        """Create and add a trade signal from an arbitrage opportunity.

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
            self.logger.warning(
                f"Could not determine quantity for signal {opportunity.symbol} from optimal_size. "
                f"Using placeholder.",
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

        # Use the async add_signal method
        if await self.add_signal(signal):
            return signal
        return None

    async def get_next_signal(self) -> TradeSignal | None:
        """Get the highest priority unexpired signal and remove it from the queue.

        Returns:
            Highest priority valid trade signal or None if queue is empty/all expired

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
                    self.logger.debug(
                        f"Removed expired signal {uid} for {potential_signal.symbol} "
                        f"from queue during get_next.",
                    )
                    continue  # Try the next item in the heap

                # Check circuit breakers before returning
                if self.circuit_breaker_system and not self._check_circuit_breakers_post_get(
                    potential_signal,
                ):
                    # Remove signal blocked by CB and log
                    heapq.heappop(self.signal_queue)
                    # Shorten f-string for line length
                    self.logger.warning(
                        f"CB active for {potential_signal.symbol}, skipping signal {uid}",
                    )
                    continue  # Try the next item

                # If valid and passes CB, pop and return
                heapq.heappop(self.signal_queue)
                self.logger.debug(f"Returning signal {uid} for {potential_signal.symbol}")
                return potential_signal

            # If loop finishes, queue is empty
            return None

        except Exception as e:
            # Log error and return None to prevent system crash
            self.logger.error(f"Error in get_next_signal: {e}")
            return None

    async def peek_next_signal(self) -> TradeSignal | None:
        """Peek at the highest priority unexpired signal without removing it from the queue.

        Returns:
            Highest priority valid trade signal or None if queue is empty/all expired

        """
        async with self.lock:  # Use async lock
            # Clean expired signals first
            now = datetime.now(UTC)
            if (now - self.last_cleanup).total_seconds() > self.cleanup_interval:
                self._clean_expired_signals()  # Clean within lock
                self.last_cleanup = now

            if not self.signal_queue:
                self.logger.debug("Peek: Queue is empty.")
                return None

            # Peek at the highest priority signal (lowest neg score)
            _priority, _counter, signal = self.signal_queue[0]  # Smallest item is root of min-heap

            # Post-get circuit breaker check for peeking
            if self._check_circuit_breakers_post_get(signal):
                self.logger.debug(f"Peeked signal: {signal.symbol}")
                return signal
            else:
                # If breaker check fails for peeked signal, it might be good to log
                # but we don't remove it on peek.
                self.logger.warning(
                    f"Peek: Circuit breaker check failed for signal {signal.signal_id}. "
                    f"Signal remains in queue but would be rejected on get.",
                )
                # Depending on desired behavior, we could return None here too.
                # For now, return the signal but note it would be blocked.
                return signal  # Or None, if strict rejection on peek is desired

    async def get_signals(
        self,
        max_count: int = 10,
        symbol: str | None = None,
    ) -> list[TradeSignal]:
        """Get a list of signals, optionally filtered by symbol. Asynchronous version."""
        signals_with_priority: list[tuple[float, int, TradeSignal]] = []
        # Acquire lock to safely access queue state
        async with self.lock:  # Acquire lock before accessing signal_queue
            # Perform cleanup first if needed
            now = datetime.now(UTC)
            if (now - self.last_cleanup).total_seconds() > self.cleanup_interval:
                self._clean_expired_signals()  # Clean within lock
                self.last_cleanup = now

            # Copy the current queue state within the lock
            signals_with_priority = self.signal_queue.copy()

        # Extract just the TradeSignal objects after releasing the lock
        all_signals: list[TradeSignal] = [item[2] for item in signals_with_priority]

        # Filter signals after releasing the lock
        if symbol:
            signals_to_sort = [s for s in all_signals if s.symbol == symbol]
        else:
            signals_to_sort = all_signals

        # Sort by priority (descending utility score) before returning
        # Ensure metadata and utility_score exist before sorting
        def get_score(s: TradeSignal) -> float:
            if s.metadata and "utility_score" in s.metadata:
                try:
                    return float(s.metadata["utility_score"])
                except (ValueError, TypeError):
                    return 0.0
            return 0.0

        signals_to_sort.sort(key=get_score, reverse=True)

        return signals_to_sort[:max_count]

    async def count(self) -> int:
        """Return the number of signals currently in the queue. Asynchronous version."""
        # Acquire lock for safe access
        async with self.lock:  # Acquire lock
            return len(self.signal_queue)

    async def clear(self) -> None:
        """Remove all signals from the queue. Asynchronous version."""
        # Acquire lock for modification
        async with self.lock:  # Acquire lock
            self.signal_queue.clear()
        self.logger.info("Cleared all signals from the queue.")

    def _clean_expired_signals(self) -> int:
        """Remove expired signals from the queue.

        This method MUST be called within a lock context (sync or async)
        that protects self.signal_queue.

        Returns:
            The number of signals removed.

        """
        now = datetime.now(UTC)
        original_count = len(self.signal_queue)
        self.logger.debug(
            f"[_clean_expired] Running at time {now}. Current queue size: {original_count}",
        )

        # Rebuild the heap excluding expired signals
        valid_signals: list[tuple[float, int, TradeSignal]] = []  # Add type hint
        removed_count_local = 0
        for item in self.signal_queue:
            priority, _, signal = item  # Replaced counter with _
            is_valid = signal.is_valid()  # Removed 'now' argument
            self.logger.debug(
                f"[_clean_expired] Checking signal {signal.signal_id} (score={-priority:.4f}, "
                f"expiry={signal.expiration}). Valid={is_valid}.",
            )
            if is_valid:
                valid_signals.append(item)
            else:
                removed_count_local += 1
                self.logger.debug(f"[_clean_expired] Removing expired signal {signal.signal_id}")

        # self.signal_queue = valid_signals  # Assign the filtered list back
        # heapq.heapify(self.signal_queue)  # Re-heapify is crucial after filtering
        # Update: Optimized approach - Build new heap directly if many removals expected,
        # or selectively remove if few. For simplicity and clarity, rebuilding is robust.
        self.signal_queue.clear()  # Clear the existing list
        self.signal_queue.extend(valid_signals)  # Add back valid ones
        heapq.heapify(self.signal_queue)  # Re-heapify

        removed_count = original_count - len(self.signal_queue)
        if removed_count > 0:
            self.logger.info(f"Cleaned {removed_count} expired signals.")
        elif removed_count != removed_count_local:
            self.logger.warning(
                f"[_clean_expired] Mismatch in removed count! Logic="
                f"{removed_count_local}, Diff={removed_count}",
            )

        self.logger.debug(f"[_clean_expired] Finished. New queue size: {len(self.signal_queue)}")
        return removed_count

    def _trim_queue(self) -> bool:
        """Remove lowest priority signals until queue is at max size."""
        if len(self.signal_queue) <= self.max_queue_size:
            return True  # No trimming needed

        try:
            # Keep the N signals with the smallest negative_priority values
            # (highest actual priority)
            num_to_keep = self.max_queue_size
            highest_priority_signals = heapq.nsmallest(
                num_to_keep,
                self.signal_queue,
                key=lambda x: x[0],
            )
            num_removed = len(self.signal_queue) - len(highest_priority_signals)

            # Rebuild the heap efficiently (though direct assignment might be okay
            # if heap property isn't strictly needed elsewhere)
            self.signal_queue = highest_priority_signals  # Direct assignment is simpler

            if num_removed > 0:
                self.logger.info(f"Trimmed {num_removed} lowest priority signals from queue.")
            return True
        except Exception as e:
            self.logger.error(f"Error during queue trimming: {e}", exc_info=True)
            return False

    def _calculate_expiration(self, signal: TradeSignal) -> TradeSignal:
        """Calculate and set default expiration if needed. Modifies signal in place."""
        if signal.expiration is None:
            try:
                # Ensure timestamp is timezone-aware (UTC)
                if signal.timestamp.tzinfo is None:
                    signal.timestamp = signal.timestamp.replace(tzinfo=UTC)
                # Ensure default_expiration_seconds is float or compatible
                expiration_delta = timedelta(seconds=float(self.default_expiration_seconds))
                signal.expiration = signal.timestamp + expiration_delta
                self.logger.debug(
                    f"Set default expiration for {signal.symbol} to {signal.expiration}",
                )
            except (TypeError, ValueError) as e:
                self.logger.error(
                    f"Failed to calculate default expiration for signal {signal.symbol}. "
                    f"Expiration remains None. Error: {e}",
                )
                signal.expiration = None  # Ensure it's None if calculation fails
        return signal

    def _check_circuit_breakers_pre_add(self, signal: TradeSignal) -> bool:
        """Check circuit breakers before adding a signal to the queue."""
        if not self.circuit_breaker_system:
            return True  # No CB system, always allow

        # Determine exchanges to check
        exchanges_to_check = self._get_exchanges_for_cb_check(signal)

        # Check symbol-level circuit breaker
        if not self._check_symbol_level_breaker(signal):
            return False

        # If no exchanges to check, symbol CB passed
        if not exchanges_to_check:
            self.logger.debug(
                f"No exchanges derived for signal {signal.signal_id}, symbol CB passed. Allowing.",
            )
            return True

        # Check exchange-level circuit breakers
        if not self._check_exchange_level_breakers(signal, exchanges_to_check):
            return False

        self.logger.debug(
            f"All circuit breakers passed for signal {signal.signal_id} ({signal.symbol})",
        )
        return True

    def _get_exchanges_for_cb_check(self, signal: TradeSignal) -> list[str]:
        """Get list of exchanges to check for circuit breakers."""
        exchanges_to_check: list[str] = []

        # Prioritize metadata if available for specific long/short exchanges
        if self._has_arbitrage_metadata(signal):
            exchanges_to_check = self._extract_arbitrage_exchanges(signal)
        # Fallback to signal.exchange field
        elif isinstance(signal.exchange, str):
            exchanges_to_check.append(signal.exchange)
        else:  # Assumed to be list[str] if not str, per type hint str | list[str]
            exchanges_to_check.extend([str(ex) for ex in signal.exchange])

        if not exchanges_to_check:
            self.logger.debug(
                f"No specific exchanges found for signal {signal.signal_id} "
                f"({signal.symbol}) to check CB. Checking symbol-level.",
            )

        return exchanges_to_check

    def _has_arbitrage_metadata(self, signal: TradeSignal) -> bool:
        """Check if signal has arbitrage metadata."""
        return (
            signal.metadata is not None
            and "long_exchange" in signal.metadata
            and signal.metadata["long_exchange"] is not None
        )

    def _extract_arbitrage_exchanges(self, signal: TradeSignal) -> list[str]:
        """Extract exchanges from arbitrage metadata."""
        if signal.metadata is None:
            return []

        exchanges_to_check = [str(signal.metadata["long_exchange"])]

        if "short_exchange" in signal.metadata and signal.metadata["short_exchange"] is not None:
            # Ensure not to add the same exchange twice if long_exchange == short_exchange
            short_exchange = str(signal.metadata["short_exchange"])
            if short_exchange != str(signal.metadata["long_exchange"]):
                exchanges_to_check.append(short_exchange)

        return exchanges_to_check

    def _check_symbol_level_breaker(self, signal: TradeSignal) -> bool:
        """Check symbol-level circuit breaker."""
        if not self.circuit_breaker_system:
            return True

        # Assume a naming convention like "symbol_SYMBOL_main" for general symbol breakers
        symbol_breaker_name = f"symbol_{signal.symbol}_main"
        symbol_breaker = self.circuit_breaker_system.get_breaker(symbol_breaker_name)

        if symbol_breaker and symbol_breaker.state == BreakerState.OPEN:
            self.logger.warning(
                f"Symbol circuit breaker for {signal.symbol} ({symbol_breaker_name}) is OPEN. "
                f"Reason: {symbol_breaker.trip_reason}. Signal {signal.signal_id} rejected.",
            )
            return False

        return True

    def _check_exchange_level_breakers(
        self, signal: TradeSignal, exchanges_to_check: list[str]
    ) -> bool:
        """Check exchange-level circuit breakers."""
        for exchange_name in set(exchanges_to_check):  # Use set to avoid redundant checks
            if not self._check_single_exchange_cb(signal, exchange_name):
                return False
        return True

    def _check_single_exchange_cb(self, signal: TradeSignal, exchange_name: str) -> bool:
        """Check circuit breakers for a single exchange."""
        # Check for API error breakers specifically
        if not self._check_api_error_breaker(signal, exchange_name):
            return False

        # Check exchange-symbol pair specific breaker
        if not self._check_pair_breaker(signal, exchange_name):
            return False

        return True

    def _check_api_error_breaker(self, signal: TradeSignal, exchange_name: str) -> bool:
        """Check API error breaker for an exchange."""
        if not self.circuit_breaker_system:
            return True

        exchange_breaker_item = self.circuit_breaker_system.get_exchange_breaker(
            exchange_name,
            "api_errors",
        )

        # Handle the case where get_exchange_breaker returns a CircuitBreaker directly
        if isinstance(exchange_breaker_item, CircuitBreaker):
            if exchange_breaker_item.state == BreakerState.OPEN:
                self.logger.warning(
                    f"Exchange circuit breaker for {exchange_name} (api_errors) is OPEN. "
                    f"Reason: {exchange_breaker_item.trip_reason}. Signal {signal.signal_id} "
                    f"rejected.",
                )
                return False
        elif isinstance(exchange_breaker_item, dict):
            # If it's a dict, it contains symbol-specific breakers
            # For now, check if any symbol-specific breaker is open
            for symbol_breaker in exchange_breaker_item.values():
                if symbol_breaker.state == BreakerState.OPEN:
                    self.logger.warning(
                        f"Symbol-specific circuit breaker for {exchange_name} is OPEN. "
                        f"Reason: {symbol_breaker.trip_reason}. Signal {signal.signal_id} "
                        f"rejected.",
                    )
                    return False

        return True

    def _check_pair_breaker(self, signal: TradeSignal, exchange_name: str) -> bool:
        """Check exchange-symbol pair specific breaker."""
        if not self.circuit_breaker_system:
            return True

        pair_breaker_name = f"pair_{exchange_name}_{signal.symbol}_main"
        pair_breaker = self.circuit_breaker_system.get_breaker(pair_breaker_name)

        if pair_breaker and pair_breaker.state == BreakerState.OPEN:
            self.logger.warning(
                f"Pair circuit breaker for {exchange_name}-{signal.symbol} "
                f"({pair_breaker_name}) is OPEN. "
                f"Reason: {pair_breaker.trip_reason}. Signal {signal.signal_id} "
                f"rejected.",
            )
            return False

        return True

    def _check_circuit_breakers_post_get(self, signal: TradeSignal) -> bool:
        """Check circuit breakers just before returning a signal from get_next_signal."""
        # This method simply reuses the pre-add logic.
        # If the pre-add check passes, the signal is considered okay from CB perspective
        # *at this moment*.
        # The pre-add logic already handles logging for rejections.
        if not self.circuit_breaker_system:
            return True  # No CB system, always allow

        allow_signal = self._check_circuit_breakers_pre_add(signal)
        if not allow_signal:
            # Log message already handled within _check_circuit_breakers_pre_add,
            # but add a debug message here indicating it was caught post-get.
            self.logger.debug(
                f"Post-get circuit breaker check failed for signal {signal.symbol}. Discarding.",
            )
        return allow_signal

    def _try_perp_format(self, symbol: str) -> str | None:
        """Try to extract exchange from EXCHANGE-SYMBOL-PERP format."""
        if "-PERP" not in symbol:
            return None
        parts = symbol.split("-")
        if len(parts) == 3 and parts[0]:
            return parts[0].lower()
        return "hyperliquid"  # Default assumption for perp contracts

    def _try_dash_format(self, symbol: str) -> str | None:
        """Try to extract exchange from EXCHANGE-SYMBOL format."""
        if "-" not in symbol:
            return None
        parts = symbol.split("-", 1)
        if len(parts) == 2 and parts[0]:
            return parts[0].lower()
        return None

    def _try_underscore_format(self, symbol: str) -> str | None:
        """Try to extract exchange from SYMBOL_EXCHANGE format."""
        if "_" not in symbol:
            return None
        parts = symbol.split("_")
        if len(parts) == 2 and parts[1]:
            return parts[1].lower()
        return "backpack"  # Example assumption

    def _try_colon_format(self, symbol: str) -> str | None:
        """Try to extract exchange from SYMBOL:EXCHANGE format."""
        if ":" not in symbol:
            return None
        parts = symbol.split(":")
        if len(parts) == 2 and parts[1]:
            return parts[1].lower()
        return None

    def _try_common_exchange_names(self, symbol: str) -> str | None:
        """Try to find common exchange names within the symbol."""
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
        return None

    def _infer_exchange_from_symbol(self, symbol: str) -> str | None:
        """Attempt to infer the exchange based on the symbol format.

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

        # Try each format in order of specificity
        for format_checker in [
            self._try_perp_format,
            self._try_dash_format,
            self._try_underscore_format,
            self._try_colon_format,
            self._try_common_exchange_names,
        ]:
            result = format_checker(symbol)
            if result:
                return result

        # If no exchange could be inferred
        self.logger.debug(f"Could not infer exchange from symbol: {symbol}")
        return None

    async def get_pending_signals(self) -> list[TradeSignal]:
        """Get a list of all signals currently pending in the queue."""
        # Use the asyncio Lock for asynchronous code
        async with self.lock:
            # Create a sorted list for a snapshot view
            # Type result explicitly
            pending_signals: list[TradeSignal] = [
                signal
                for _score, _count, signal in sorted(
                    self.signal_queue,
                    key=lambda x: (x[0], x[1]),
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
        """Process signals based on priority levels.

        Internal method that handles signal processing according to their
        calculated priority scores and queue ordering.
        """
        processed_signals: list[TradeSignal] = []
        while self.signal_queue:
            _score, _count, signal = heapq.heappop(self.signal_queue)
            # Assuming this method is meant to process ALL signals regardless of the `True` check
            processed_signals.append(signal)
            self.logger.debug(f"Processing signal: {signal.signal_type.name} for {signal.symbol}")
            # TODO: Add actual processing/validation logic here if this method is used.
            # The `if True:` block was likely placeholder/dead code.

    def _add_signal(self, signal: TradeSignal, priority_score: float) -> None:
        """Add a signal with a calculated priority score.

        Internal method that adds a signal to the queue using the provided
        priority score for ordering.

        Args:
            signal: Trade signal to add to the queue.
            priority_score: Calculated priority score for queue ordering.

        """
        # Assume lock is already held if called internally from an async method
        # If called synchronously, it would need its own lock acquisition
        # (using _sync_lock if that was kept, or potentially blocking async lock)

        # Check circuit breakers before adding
        if self.circuit_breaker_system and not self._check_circuit_breakers_pre_add(signal):
            # Log rejection details within _check_circuit_breakers_pre_add
            return  # Do not add if breaker tripped

        # Check queue size and trim if necessary (assuming lock is held)
        if len(self.signal_queue) >= self.max_queue_size:
            # Find minimum utility score (maximum negative score) in the heap
            if not self.signal_queue:
                self.logger.error("Queue became empty unexpectedly before trimming check.")
                return  # Should not happen if len check passed

            min_score_in_queue = self.signal_queue[0][0]  # Smallest item is root of min-heap
            new_signal_score_neg = -priority_score

            if new_signal_score_neg >= min_score_in_queue:
                # New signal has lower or equal priority than the worst in queue
                self.logger.debug(
                    f"Rejected signal for {signal.symbol} with score "
                    f"{priority_score} (lower than min {-min_score_in_queue}) - Queue full.",
                )
                return  # Do not add

            # New signal is better than the worst; trim the queue
            if not self._trim_queue():  # Assumes lock is held
                self.logger.error("Failed to trim queue, cannot add new signal.")
                return  # Do not add

        # Add to priority queue
        self.counter += 1
        # Use negative score for max-heap behavior with heapq (min-heap)
        heapq.heappush(self.signal_queue, (-priority_score, self.counter, signal))

        self.logger.debug(f"Added signal for {signal.symbol} to queue with score {priority_score}")

        # Signal the async wait event
        self.new_signal_event.set()

    async def wait_for_signals(
        self,
        timeout: float | None = None,
        max_signals: int = 1,
    ) -> list[TradeSignal]:
        """Wait for signals to become available.

        Returns up to max_signals highest priority signals.

        Args:
            timeout: Maximum time to wait in seconds (None = wait indefinitely)
            max_signals: Maximum number of signals to return

        Returns:
            List of TradeSignal objects ordered by priority

        """
        # First see if we already have signals available
        result = await self.pop_signals(max_signals)
        if result:
            return result

        # Wait for the signal event
        # This is used in an async context, so we can't use .wait() directly
        # This would need to be awaited in an async function
        if self.new_signal_event.is_set():
            # Event is already set, signals should be available
            return await self.pop_signals(max_signals)
        else:
            # Can't wait in a synchronous context
            self.logger.debug("Cannot wait for signals in synchronous context")
            return []

    async def pop_signals(self, max_signals: int = 1) -> list[TradeSignal]:
        """Atomically pop the highest priority signals from the queue."""
        signals: list[TradeSignal] = []  # Explicitly type the list
        async with self.lock:
            # Perform cleanup first if needed
            now = datetime.now(UTC)
            if (now - self.last_cleanup).total_seconds() > self.cleanup_interval:
                self._clean_expired_signals()  # Clean within lock
                self.last_cleanup = now

            count = 0
            while count < max_signals and self.signal_queue:
                try:
                    # Pop highest priority signal (lowest neg score)
                    _priority, _counter, signal = heapq.heappop(self.signal_queue)

                    # Post-get circuit breaker check
                    if self._check_circuit_breakers_post_get(signal):
                        signals.append(signal)  # Append to correctly typed list
                        count += 1
                    else:
                        # Log rejection and continue to next signal
                        continue

                except IndexError:
                    # Should not happen if self.signal_queue check passes, but defensive
                    break
        return signals  # Return the correctly typed list

    async def get_next_signals(self, max_count: int = 1) -> list[TradeSignal]:
        """Get the next highest priority signals without removing them."""
        signals: list[TradeSignal] = []  # Add type hint
        async with self.lock:
            # Perform cleanup first if needed
            now = datetime.now(UTC)
            if (now - self.last_cleanup).total_seconds() > self.cleanup_interval:
                self._clean_expired_signals()  # Clean within lock
                self.last_cleanup = now

            # Get up to max_count highest priority items using nsmallest on negative score
            # Equivalent to nlargest on positive utility score
            # heapq.nsmallest returns a list sorted from smallest to largest
            potential_signals = heapq.nsmallest(max_count, self.signal_queue)

            # Post-get circuit breaker check for peeking signals
            for _priority, _counter, raw_signal in potential_signals:
                signal = raw_signal  # Removed unnecessary cast
                if self._check_circuit_breakers_post_get(signal):
                    signals.append(signal)
                else:
                    # Log rejection
                    self.logger.debug(
                        f"Circuit breaker tripped for {signal.exchange} / {signal.symbol}, "
                        f"skipping peeked signal {signal.signal_id}.",
                    )
        return signals  # Already sorted by priority due to nsmallest

    def _check_circuit_breakers(self, signal: TradeSignal) -> bool:
        """Check relevant circuit breakers for a given signal.

        Args:
            signal: The trade signal to check.

        Returns:
            True if all relevant circuit breakers are closed or allow the trade,
            False otherwise.

        """
        if not self.circuit_breaker_system:
            return True  # No CB system, always pass

        # Determine exchanges to check
        exchanges_to_check = self._get_exchanges_for_cb_check(signal)

        # Check symbol-level circuit breaker
        if not self._check_symbol_level_breaker(signal):
            return False

        # If no exchanges to check, symbol CB passed
        if not exchanges_to_check:
            self.logger.debug(
                f"No exchanges derived for signal {signal.signal_id}, symbol CB passed. Allowing.",
            )
            return True

        # Check exchange-level circuit breakers
        if not self._check_exchange_level_breakers(signal, exchanges_to_check):
            return False

        self.logger.debug(
            f"All circuit breakers passed for signal {signal.signal_id} ({signal.symbol})",
        )
        return True

    async def is_empty(self) -> bool:
        """Check if the queue is empty asynchronously."""
        async with self.lock:
            # Optionally clean expired signals before checking
            # This depends on whether "is_empty" should reflect only valid signals
            # If cleanup is desired here, uncomment:
            # now = datetime.now(UTC)
            # if (now - self.last_cleanup).total_seconds() > self.cleanup_interval:
            #     self._clean_expired_signals()
            #     self.last_cleanup = now
            is_currently_empty = not self.signal_queue
        return is_currently_empty  # Return the boolean value

    async def enqueue_signal(self, signal: TradeSignal) -> None:
        """Asynchronously enqueue a trade signal into the priority queue and notify listeners.

        Args:
            signal: TradeSignal to enqueue.

        """
        async with self.lock:
            added = await self.add_signal(signal)
            if added:
                self.new_signal_event.set()
                self.logger.info(f"Enqueued signal for {signal.symbol} (async)")
            else:
                self.logger.warning(f"Failed to enqueue signal for {signal.symbol} (async)")

    async def run(self, cancellation_token: asyncio.Event) -> None:
        """Asynchronous run loop for the signal queue. Waits for new signals and processes them.

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
        """Process the next available signal from the queue.

        Retrieves and processes the highest priority signal from the queue,
        performing any necessary validation and circuit breaker checks.

        Returns:
            The processed TradeSignal if available, None otherwise.

        """
        # Implementation of process_signal method
        pass

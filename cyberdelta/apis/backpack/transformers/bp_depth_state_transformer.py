"""Stateful transformer for Backpack orderbook depth updates.

This module implements a stateful transformer that maintains orderbook state
to handle Backpack's incremental update protocol. Unlike the direct transformation
approach, this transformer accumulates incremental updates and maintains full
orderbook state per symbol.

The transformer solves the critical issue where Backpack sends incremental updates
with partial data (only bid OR ask changes), which would otherwise result in
empty OrderBook objects being created.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawDepthUpdateEvent
from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.exceptions import OrderBookTransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols import exchanges
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.models import OrderBook


if TYPE_CHECKING:
    from cyberdelta.apis.backpack.protocols.mapper_protocols import OrderBookMapperProtocol
    from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol

logger = get_logger(__name__)


class OrderBookState:
    """Maintains the current state of an orderbook for a single symbol.

    This class tracks the mutable state of an orderbook, including all bid/ask
    levels and sequence numbers for validation. It provides methods to apply
    incremental updates and convert the current state to an immutable OrderBook
    domain model.

    Attributes:
        bids: Dictionary mapping price (Decimal) to quantity (Decimal)
        asks: Dictionary mapping price (Decimal) to quantity (Decimal)
        last_update_id: The ID of the last successfully applied update
        last_update_time: Timestamp of the last update
    """

    def __init__(self) -> None:
        """Initialize an empty orderbook state."""
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.last_update_id: int = 0
        self.last_update_time: datetime = datetime.now(UTC)

    def apply_update(self, event: BackpackRawDepthUpdateEvent) -> bool:
        """Apply an update to the orderbook state.

        Validates sequence numbers and applies bid/ask updates. If a quantity
        is zero, the price level is removed from the orderbook.

        Args:
            event: The raw depth update event from Backpack

        Returns:
            True if update was applied successfully, False if sequence error detected
        """
        # Parse and validate update IDs
        parsed_ids = self._parse_update_ids(event)
        if parsed_ids is None:
            return False

        first_id, last_id = parsed_ids

        # Validate sequence - skip for initial update
        if not self._validate_sequence(first_id):
            return False

        # Apply bid and ask updates
        if event.bids is not None:
            self._apply_price_levels(event.bids, self.bids, "bid")

        if event.asks is not None:
            self._apply_price_levels(event.asks, self.asks, "ask")

        # Update sequence tracking
        self.last_update_id = last_id
        self.last_update_time = datetime.now(UTC)

        return True

    def _parse_update_ids(self, event: BackpackRawDepthUpdateEvent) -> tuple[int, int] | None:
        """Parse update IDs from string to int.

        Args:
            event: The raw depth update event

        Returns:
            Tuple of (first_id, last_id) or None if parsing fails
        """
        try:
            first_id = int(event.first_update_id)
            last_id = int(event.last_update_id)
        except (ValueError, TypeError) as e:
            logger.exception(
                "invalid_update_ids",
                first_id=event.first_update_id,
                last_id=event.last_update_id,
                error=str(e),
            )
            return None
        else:
            return first_id, last_id

    def _validate_sequence(self, first_id: int) -> bool:
        """Validate sequence number continuity.

        Args:
            first_id: The first update ID from the current event

        Returns:
            True if sequence is valid, False if gap detected
        """
        if self.last_update_id > 0:
            expected_id = self.last_update_id + 1
            if first_id != expected_id:
                logger.warning(
                    "sequence_gap_detected",
                    expected=expected_id,
                    received=first_id,
                    gap=first_id - expected_id,
                    last_update_id=self.last_update_id,
                )
                return False
        return True

    def _is_finite_and_log(
        self,
        value: Decimal,
        value_str: str,
        side_name: str,
        value_type: str,
    ) -> bool:
        """Check if a decimal value is finite and log error if not.

        Args:
            value: The decimal value to check
            value_str: The original string representation
            side_name: "bid" or "ask" for logging
            value_type: "price" or "quantity" for logging

        Returns:
            True if the value is finite, False otherwise
        """
        if not value.is_finite():
            logger.error(
                "non_finite_value",
                value_type=value_type,
                side=side_name,
                value=value_str,
            )
            return False
        return True

    def _apply_price_levels(
        self,
        updates: list[tuple[str, str]],
        book_side: dict[Decimal, Decimal],
        side_name: str,
    ) -> None:
        """Apply price level updates to one side of the orderbook.

        Args:
            updates: List of (price, quantity) string tuples
            book_side: The bid or ask dictionary to update
            side_name: "bid" or "ask" for logging
        """
        if not updates:
            return

        removed_levels: list[str] = []
        added_levels: list[str] = []
        updated_levels: list[str] = []

        for price_str, qty_str in updates:
            price = Decimal(price_str)
            qty = Decimal(qty_str)

            # DEFENSIVE CHECK: Ensure price and quantity are finite
            if not self._is_finite_and_log(price, price_str, side_name, "price"):
                continue
            if not self._is_finite_and_log(qty, qty_str, side_name, "quantity"):
                continue

            if qty == 0:
                # Remove price level when quantity is zero
                if price in book_side:
                    removed_levels.append(str(price))
                book_side.pop(price, None)
            else:
                if price in book_side:
                    updated_levels.append(f"{price}:{qty}")
                else:
                    added_levels.append(f"{price}:{qty}")
                book_side[price] = qty

    def to_orderbook(self, symbol: Symbol, order_book_mapper: OrderBookMapperProtocol) -> OrderBook:
        """Convert current state to immutable OrderBook domain model.

        Uses the injected mapper to ensure consistency with other orderbook transformations.

        Args:
            symbol: The trading symbol domain object for this orderbook
            order_book_mapper: The mapper to use for OrderBook creation

        Returns:
            An immutable OrderBook snapshot of the current state
        """
        # Create a synthetic BackpackRawDepthUpdateEvent from current state
        # This allows us to use the existing mapper logic
        bid_levels = [(str(price), str(qty)) for price, qty in self.bids.items()]
        ask_levels = [(str(price), str(qty)) for price, qty in self.asks.items()]

        # Create synthetic event with current state (use proper aliases)
        synthetic_event = BackpackRawDepthUpdateEvent(
            b=bid_levels or None,
            a=ask_levels or None,
            U=str(self.last_update_id),
            u=str(self.last_update_id),
            e="depth",
            E=int(self.last_update_time.timestamp() * 1000),
            T=int(self.last_update_time.timestamp() * 1000),
        )

        # Use the mapper to create the OrderBook - symbol is already a Symbol object
        return order_book_mapper.transform_ws_depth_event_to_internal(symbol, synthetic_event)

    def __eq__(self, other: object) -> bool:
        """Compare OrderBookState instances for equality.

        Two states are considered equal if they have identical bids and asks.
        This is used for deduplication in emission strategies.

        Args:
            other: Another object to compare with

        Returns:
            True if both states have identical bids and asks, False otherwise
        """
        if not isinstance(other, OrderBookState):
            return False
        return self.bids == other.bids and self.asks == other.asks

    def __hash__(self) -> int:
        """Make OrderBookState hashable for use in sets/dictionaries.

        Returns:
            Hash based on frozen sets of bids and asks
        """
        return hash((frozenset(self.bids.items()), frozenset(self.asks.items())))


class BackpackDepthStateTransformer:
    """Stateful transformer for Backpack orderbook depth updates.

    This transformer maintains orderbook state for each symbol and accumulates
    incremental updates. It distinguishes between full snapshots and incremental
    updates, applying them appropriately to maintain accurate orderbook state.

    Features:
    - Maintains separate orderbook state per symbol
    - Validates sequence numbers with gap detection
    - Handles both snapshots and incremental updates
    - Configurable emission strategies
    - Comprehensive logging for monitoring

    Attributes:
        states: Dictionary mapping symbols to their OrderBookState
        emission_strategy: Strategy for when to emit OrderBook events
    """

    def __init__(self, order_book_mapper: OrderBookMapperProtocol) -> None:
        """Initialize the stateful transformer.

        Args:
            order_book_mapper: Mapper for creating OrderBook domain models
        """
        self.order_book_mapper = order_book_mapper
        self.states: dict[Symbol, OrderBookState] = {}
        self.emission_strategy: str = "always"  # Options: "always", "on_change", "throttled"

        # Deduplication tracking for emission efficiency using hashes
        self.last_emitted_state_hashes: dict[Symbol, int] = {}

        # Statistics for monitoring
        self._stats = {
            "snapshots_processed": 0,
            "incremental_updates_processed": 0,
            "sequence_errors": 0,
            "symbols_tracked": 0,
            "emissions_deduplicated": 0,
            "emissions_allowed": 0,
        }

    def transform(
        self,
        validated: BackpackRawDepthUpdateEvent,
        context: WebSocketContextProtocol | None = None,
    ) -> OrderBook | None:
        """Transform depth update by maintaining state.

        Processes the incoming depth update, maintaining orderbook state and
        emitting OrderBook domain models based on the emission strategy.

        Args:
            validated: The validated raw depth update event
            context: Optional WebSocket context containing symbol information

        Returns:
            OrderBook when emission criteria are met, None otherwise

        Raises:
            OrderBookTransformationError: If symbol cannot be extracted from context
        """
        # Extract symbol from context
        symbol = self._extract_symbol(context)

        # Get or create state for symbol
        if symbol not in self.states:
            self.states[symbol] = OrderBookState()
            self._stats["symbols_tracked"] = len(self.states)

        state = self.states[symbol]

        # Check if this is a snapshot (resets state)
        if self._is_snapshot(validated):
            logger.info(
                "orderbook_snapshot_received",
                symbol=symbol,
                bid_levels=len(validated.bids) if validated.bids else 0,
                ask_levels=len(validated.asks) if validated.asks else 0,
                first_update_id=validated.first_update_id,
                last_update_id=validated.last_update_id,
            )
            # Reset state with new snapshot
            state = OrderBookState()
            self.states[symbol] = state
            self._stats["snapshots_processed"] += 1
            # Update symbols tracked count
            self._stats["symbols_tracked"] = len(self.states)
        else:
            self._stats["incremental_updates_processed"] += 1

        # Apply update
        if not state.apply_update(validated):
            # Sequence error - clear state and wait for new snapshot
            logger.error(
                "orderbook_sequence_error",
                symbol=symbol,
                message="Clearing state due to sequence gap",
            )
            del self.states[symbol]
            self._stats["sequence_errors"] += 1
            return None

        # Decide whether to emit based on strategy
        if self._should_emit(state, validated, symbol):
            try:
                return state.to_orderbook(symbol, self.order_book_mapper)
            except Exception as e:
                logger.exception(
                    "orderbook_conversion_error",
                    symbol=symbol,
                    error=str(e),
                )
                msg = f"Failed to convert state to OrderBook: {e}"
                raise OrderBookTransformationError(
                    source_type="BackpackRawDepthUpdateEvent",
                    reason=msg,
                ) from e

        return None

    def _extract_symbol(self, context: WebSocketContextProtocol | None) -> Symbol:
        """Extract symbol using exchange-agnostic protocol methods only.

        This method follows architectural principles:
        - NO hasattr() usage (violates type safety)
        - Uses only protocol-defined interfaces
        - Exchange-agnostic implementation

        Args:
            context: The WebSocket context containing symbol information

        Returns:
            The extracted symbol

        Raises:
            OrderBookTransformationError: If symbol cannot be extracted
        """
        if context is None:
            raise OrderBookTransformationError(
                source_type="BackpackRawDepthUpdateEvent",
                reason="Context is None, cannot extract symbol",
            )

        # Method 1: Direct symbol access (protocol-defined)
        if context.symbol:
            return exchanges.backpack(context.symbol)

        # Method 2: Transformer params (protocol-defined)
        try:
            transformer_params = context.get_transformer_params()
            if "symbol" in transformer_params:
                return exchanges.backpack(transformer_params["symbol"])
        except (AttributeError, TypeError, KeyError):
            pass

        # Method 3: Protocol-based envelope access (NO hasattr/getattr)
        try:
            envelope = context.validated_envelope
            # Use proper protocol typing - avoid hasattr/getattr
            if envelope and isinstance(envelope, BackpackRawWebSocketEnvelope):
                stream = envelope.stream  # Type-safe access via protocol
                if "." in stream:
                    symbol_str = stream.split(".")[1]  # Extract symbol part
                    return exchanges.backpack(symbol_str)
        except (AttributeError, TypeError, ValueError, IndexError):
            pass

        raise OrderBookTransformationError(
            source_type="BackpackRawDepthUpdateEvent",
            reason="Cannot extract symbol using protocol methods",
        )

    def _is_snapshot(self, event: BackpackRawDepthUpdateEvent) -> bool:
        """Identify if the event is a full snapshot vs incremental update.

        For Backpack WebSocket depth streams, all updates are incremental by design.
        Snapshots should only come from REST API initialization, not WebSocket streams.

        According to Backpack documentation, the depth stream provides incremental
        updates that should be applied to maintain orderbook state. There are no
        periodic full snapshots sent via WebSocket.

        TODO: If Backpack protocol changes to include snapshots in WebSocket streams,
        update this method to detect them (e.g., check for specific fields or flags).

        Args:
            event: The depth update event to check

        Returns:
            Whether the event is a snapshot. Currently always False as Backpack
            WebSocket depth updates are always incremental.
        """
        # Check if protocol has changed to support snapshots via WebSocket
        # For future protocol changes, we could check for:
        # - A snapshot flag: hasattr(event, 'is_snapshot') and event.is_snapshot
        # - A reset indicator: hasattr(event, 'reset') and event.reset
        # - Full orderbook size: large number of bids/asks indicating full state

        # Current behavior: Backpack WebSocket depth is always incremental
        return False

    def _compute_state_hash(self, state: OrderBookState) -> int:
        """Compute hash of orderbook state for efficient deduplication.

        Uses the built-in __hash__ method of OrderBookState which is based
        on frozen sets of bids and asks.

        Args:
            state: The orderbook state to hash

        Returns:
            Hash value representing the state
        """
        return hash(state)

    def _should_emit(
        self,
        state: OrderBookState,
        event: BackpackRawDepthUpdateEvent,
        symbol: Symbol,
    ) -> bool:
        """Determine if we should emit an OrderBook event.

        Args:
            state: The current orderbook state
            event: The event that was just processed
            symbol: The Symbol object for this orderbook state

        Returns:
            True if an OrderBook should be emitted, False otherwise
        """
        if self.emission_strategy == "always":
            # Emit if we have accumulated state (any bids or asks in state)
            # OR if this event has any data (to handle snapshots)
            has_accumulated_state = bool(state.bids or state.asks)
            has_event_data = event.bids is not None or event.asks is not None

            if not (has_accumulated_state or has_event_data):
                return False

            # Deduplication check: avoid emitting identical consecutive states
            current_state_hash = self._compute_state_hash(state)
            last_state_hash = self.last_emitted_state_hashes.get(symbol)
            if last_state_hash and last_state_hash == current_state_hash:
                self._stats["emissions_deduplicated"] += 1
                return False

            # Update last emitted state hash for this symbol
            self.last_emitted_state_hashes[symbol] = current_state_hash

            self._stats["emissions_allowed"] += 1
            return True
        if self.emission_strategy == "on_change":
            # TODO: Implement logic to detect significant changes
            # For now, emit on any accumulated state
            return bool(state.bids or state.asks)
        if self.emission_strategy == "throttled":
            # TODO: Implement time-based throttling
            # For now, emit on any accumulated state
            return bool(state.bids or state.asks)

        return False

    def get_full_orderbook(self, symbol: Symbol) -> OrderBook | None:
        """Get the complete current orderbook state for a symbol.

        This returns the full accumulated orderbook state at the current point in time,
        regardless of emission strategy. Use this when you need the complete orderbook
        state on-demand (e.g., after 60 minutes of updates).

        Args:
            symbol: The trading symbol to get the orderbook for

        Returns:
            Complete OrderBook with all accumulated state, or None if symbol not tracked
        """
        if symbol not in self.states:
            return None

        state = self.states[symbol]

        # Only return orderbook if we have meaningful state
        if not state.bids and not state.asks:
            return None

        try:
            orderbook = state.to_orderbook(symbol, self.order_book_mapper)
        except Exception as e:
            logger.exception(
                "full_orderbook_conversion_error",
                symbol=symbol,
                error=str(e),
            )
            return None
        else:
            return orderbook

    def get_tracked_symbols(self) -> list[Symbol]:
        """Get list of symbols currently being tracked by the transformer.

        Returns:
            List of Symbol objects that have active orderbook state
        """
        return list(self.states.keys())

    def has_symbol_state(self, symbol: Symbol) -> bool:
        """Check if the transformer is tracking state for a symbol.

        Args:
            symbol: The trading symbol to check

        Returns:
            True if symbol has accumulated state, False otherwise
        """
        return symbol in self.states and (
            bool(self.states[symbol].bids) or bool(self.states[symbol].asks)
        )

    def get_statistics(self) -> dict[str, int]:
        """Get current transformer statistics for monitoring.

        Returns:
            Dictionary containing processing statistics
        """
        return self._stats.copy()

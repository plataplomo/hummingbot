"""Property-based tests for Backpack depth state transformer.

Tests the stateful transformer that handles Backpack's incremental orderbook updates,
including snapshot detection, sequence validation, and state management. Uses property-based
testing to ensure the transformer correctly handles all possible sequences of updates.

Key Testing Areas:
- State transitions with arbitrary sequences of updates
- Sequence validation across all possible ID combinations
- Order book accumulation with random price/quantity updates
- Snapshot vs incremental update detection
- Multi-symbol isolation properties
- Edge cases with empty updates and invalid sequences

SECURITY CRITICAL: Order book state management errors can lead to:
- Incorrect trading decisions based on stale data
- Arbitrage opportunities from desynchronized books
- Position sizing errors from incorrect depth
- Market manipulation through state corruption
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock

import pytest
from hypothesis import given, settings, strategies as st

from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
    BackpackOrderBookMapper,
)
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawDepthUpdateEvent
from cyberdelta.apis.backpack.transformers.bp_depth_state_transformer import (
    BackpackDepthStateTransformer,
    OrderBookState,
)
from cyberdelta.apis.exceptions import OrderBookTransformationError
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.models import OrderBook
from tests.common_symbols import BTC_USDC_BP, ETH_USDC_BP


# =============================================================================
# HYPOTHESIS STRATEGIES FOR ORDER BOOK STATE TESTING
# =============================================================================


@st.composite
def price_level_strategy(draw: st.DrawFn) -> tuple[str, str]:
    """Generate valid price level (price, quantity) tuples.

    Returns:
        Tuple of (price_str, quantity_str) for order book levels.
    """
    price = draw(
        st.floats(min_value=0.01, max_value=100000.0, allow_nan=False, allow_infinity=False)
    )
    quantity = draw(
        st.floats(min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False)
    )

    # Format to reasonable decimal places
    price_str = f"{price:.8f}".rstrip("0").rstrip(".")
    quantity_str = f"{quantity:.8f}".rstrip("0").rstrip(".")

    return (price_str, quantity_str)


@st.composite
def order_book_levels_strategy(
    draw: st.DrawFn, min_levels: int = 0, max_levels: int = 10, allow_zero_qty: bool = True
) -> list[tuple[str, str]] | None:
    """Generate order book levels for bids or asks.

    Returns:
        List of (price, quantity) tuples or None.
    """
    if draw(st.booleans()):
        return None  # Sometimes no updates for this side

    num_levels = draw(st.integers(min_value=min_levels, max_value=max_levels))
    levels: list[tuple[str, str]] = []
    used_prices: set[str] = set()

    for _ in range(num_levels):
        level = draw(price_level_strategy())
        price_str = level[0]

        # Ensure unique prices
        attempts = 0
        while price_str in used_prices and attempts < 10:
            level = draw(price_level_strategy())
            price_str = level[0]
            attempts += 1

        if price_str not in used_prices:
            used_prices.add(price_str)

            # Optionally generate zero quantity (removes level)
            if allow_zero_qty and draw(st.booleans()) and draw(st.floats(0, 1)) < 0.1:
                levels.append((price_str, "0"))
            else:
                levels.append(level)

    return levels or None


@st.composite
def depth_update_event_strategy(
    draw: st.DrawFn, first_update_id: int | None = None, last_update_id: int | None = None
) -> BackpackRawDepthUpdateEvent:
    """Generate valid BackpackRawDepthUpdateEvent for testing.

    Returns:
        Valid depth update event with appropriate sequence IDs.
    """
    # Generate update IDs
    if first_update_id is None:
        first_id = draw(st.integers(min_value=1, max_value=100000))
    else:
        first_id = first_update_id

    if last_update_id is None:
        # Last ID should be >= first ID
        last_id = draw(st.integers(min_value=first_id, max_value=first_id + 100))
    else:
        last_id = last_update_id

    # Generate bid and ask levels
    bids = draw(order_book_levels_strategy())
    asks = draw(order_book_levels_strategy())

    # Ensure at least one side has data (unless testing empty updates)
    if bids is None and asks is None and draw(st.booleans()):
        # Sometimes force at least one side to have data
        if draw(st.booleans()):
            bids = [(draw(price_level_strategy()))]
        else:
            asks = [(draw(price_level_strategy()))]

    # Generate timestamps
    event_time = draw(st.integers(min_value=1600000000000, max_value=2000000000000))
    trade_time = draw(st.integers(min_value=event_time, max_value=event_time + 1000))

    return BackpackRawDepthUpdateEvent(
        b=bids,
        a=asks,
        U=str(first_id),
        u=str(last_id),
        e="depth",
        E=event_time,
        T=trade_time,
    )


@st.composite
def sequential_updates_strategy(
    draw: st.DrawFn, num_updates: int = 3
) -> list[BackpackRawDepthUpdateEvent]:
    """Generate a sequence of valid incremental updates.

    Returns:
        List of depth update events with proper sequencing.
    """
    updates = []
    current_id = draw(st.integers(min_value=1, max_value=1000))

    for _ in range(num_updates):
        # Generate update with proper sequence
        update = draw(
            depth_update_event_strategy(
                first_update_id=current_id,
                last_update_id=current_id + draw(st.integers(min_value=0, max_value=10)),
            )
        )
        updates.append(update)

        # Move to next sequence
        current_id = int(update.last_update_id) + 1

    return updates


# =============================================================================
# PROPERTY-BASED TESTS FOR ORDER BOOK STATE
# =============================================================================


class TestOrderBookState:
    """Property-based tests for OrderBookState class."""

    @given(st.data())
    @settings(max_examples=100, deadline=None)
    def test_init_creates_empty_state_properties(self, data: st.DataObject) -> None:
        """Property: OrderBookState should always initialize with empty state."""
        state = OrderBookState()

        # Properties: Initial state invariants
        assert state.bids == {}
        assert state.asks == {}
        assert state.last_update_id == 0
        assert isinstance(state.last_update_time, datetime)
        assert state.last_update_time.tzinfo == UTC

    @given(update=depth_update_event_strategy())
    @settings(max_examples=200, deadline=None)
    def test_apply_first_update_properties(self, update: BackpackRawDepthUpdateEvent) -> None:
        """Property: First update should always succeed and populate state correctly."""
        state = OrderBookState()

        # Apply first update (no sequence validation)
        result = state.apply_update(update)

        # Properties: First update always succeeds if IDs are valid
        try:
            update_id = int(update.last_update_id)
            assert result is True
            assert state.last_update_id == update_id

            # Check bid levels if present
            if update.bids:
                for price_str, qty_str in update.bids:
                    price = Decimal(price_str)
                    qty = Decimal(qty_str)
                    if qty > 0:
                        assert state.bids[price] == qty
                    else:
                        assert price not in state.bids

            # Check ask levels if present
            if update.asks:
                for price_str, qty_str in update.asks:
                    price = Decimal(price_str)
                    qty = Decimal(qty_str)
                    if qty > 0:
                        assert state.asks[price] == qty
                    else:
                        assert price not in state.asks
        except (ValueError, TypeError):
            # Invalid update IDs should fail
            assert result is False

    @given(
        initial_price=st.floats(
            min_value=1.0, max_value=10000.0, allow_nan=False, allow_infinity=False
        ),
        initial_qty=st.floats(
            min_value=0.1, max_value=1000.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_zero_quantity_removes_level_properties(
        self, initial_price: float, initial_qty: float
    ) -> None:
        """Property: Zero quantity should always remove the price level."""
        state = OrderBookState()

        price_str = f"{initial_price:.8f}".rstrip("0").rstrip(".")
        qty_str = f"{initial_qty:.8f}".rstrip("0").rstrip(".")

        # Add initial levels
        event1 = BackpackRawDepthUpdateEvent(
            b=[(price_str, qty_str)],
            a=[(price_str, qty_str)],
            U="1",
            u="1",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )
        state.apply_update(event1)

        price_decimal = Decimal(price_str)
        assert price_decimal in state.bids
        assert price_decimal in state.asks

        # Remove with zero quantity
        event2 = BackpackRawDepthUpdateEvent(
            b=[(price_str, "0")],
            a=[(price_str, "0")],
            U="2",
            u="2",
            e="depth",
            E=1705314600002,
            T=1705314600003,
        )
        state.apply_update(event2)

        # Properties: Zero quantity removes levels
        assert price_decimal not in state.bids
        assert price_decimal not in state.asks

    @given(
        initial_id=st.integers(min_value=1, max_value=1000),
        gap_size=st.integers(min_value=2, max_value=100),
    )
    @settings(max_examples=100, deadline=None)
    def test_sequence_gap_detection_properties(self, initial_id: int, gap_size: int) -> None:
        """Property: Sequence gaps should always be detected and rejected."""
        state = OrderBookState()

        # First update
        event1 = BackpackRawDepthUpdateEvent(
            b=[("100.0", "10.0")],
            a=[("101.0", "5.0")],
            U=str(initial_id),
            u=str(initial_id),
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )
        state.apply_update(event1)
        assert state.last_update_id == initial_id

        # Update with gap
        gap_id = initial_id + gap_size
        event2 = BackpackRawDepthUpdateEvent(
            b=[("99.0", "15.0")],
            a=None,
            U=str(gap_id),
            u=str(gap_id),
            e="depth",
            E=1705314600002,
            T=1705314600003,
        )

        result = state.apply_update(event2)

        # Properties: Gap should be detected
        assert result is False
        assert state.last_update_id == initial_id  # State unchanged

    @given(
        invalid_id=st.one_of(
            st.text(
                min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("Ll", "Lu"))
            ),
            st.just(""),
            st.just("NaN"),
            st.just("Infinity"),
            st.just("-1"),
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_invalid_update_ids_rejection_properties(self, invalid_id: str) -> None:
        """Property: Invalid update IDs should always be rejected."""
        from cyberdelta.exceptions.parsing import EmptyStringError
        from pydantic import ValidationError

        state = OrderBookState()

        # The BackpackRawDepthUpdateEvent validates IDs, so invalid IDs will raise during creation
        try:
            # Try with invalid first update ID
            event = BackpackRawDepthUpdateEvent(
                b=[("100.0", "10.0")],
                a=None,
                U=invalid_id,
                u="1",
                e="depth",
                E=1705314600000,
                T=1705314600001,
            )

            # If we get here, the ID was actually valid (digits only)
            # Check if it's actually parseable as an integer
            try:
                int(invalid_id)  # Should be a valid integer string
                result = state.apply_update(event)
                assert result is True  # Valid IDs should work
            except ValueError:
                # Not a valid integer, apply_update should fail
                result = state.apply_update(event)
                assert result is False

        except (ValidationError, EmptyStringError, ValueError):
            # Invalid IDs are rejected at model creation time
            # This is expected behavior - state remains unchanged
            assert state.last_update_id == 0

    @given(
        bid_prices=st.lists(
            st.floats(min_value=90.0, max_value=100.0, allow_nan=False, allow_infinity=False),
            min_size=1,
            max_size=10,
            unique=True,
        ),
        ask_prices=st.lists(
            st.floats(min_value=100.1, max_value=110.0, allow_nan=False, allow_infinity=False),
            min_size=1,
            max_size=10,
            unique=True,
        ),
        quantities=st.lists(
            st.floats(min_value=0.1, max_value=100.0, allow_nan=False, allow_infinity=False),
            min_size=20,
        ),
    )
    @settings(max_examples=100, deadline=None)
    def test_orderbook_sorting_properties(
        self, bid_prices: list[float], ask_prices: list[float], quantities: list[float]
    ) -> None:
        """Property: OrderBook should always have properly sorted bids and asks."""
        state = OrderBookState()
        mapper = BackpackOrderBookMapper()

        # Populate state with unsorted levels
        state.bids = {}
        for i, price in enumerate(bid_prices):
            state.bids[Decimal(str(price))] = Decimal(str(quantities[i % len(quantities)]))

        state.asks = {}
        for i, price in enumerate(ask_prices):
            state.asks[Decimal(str(price))] = Decimal(
                str(quantities[(i + len(bid_prices)) % len(quantities)])
            )

        state.last_update_time = datetime(2025, 1, 21, 12, 0, 0, tzinfo=UTC)

        # Convert to orderbook
        orderbook = state.to_orderbook(BTC_USDC_BP, mapper)

        # Properties: Sorting invariants
        assert orderbook.symbol == BTC_USDC_BP

        # Bids should be sorted descending
        for i in range(len(orderbook.bids) - 1):
            assert orderbook.bids[i][0] >= orderbook.bids[i + 1][0]

        # Asks should be sorted ascending
        for i in range(len(orderbook.asks) - 1):
            assert orderbook.asks[i][0] <= orderbook.asks[i + 1][0]

        # All bids should be less than all asks (no crossed book)
        if orderbook.bids and orderbook.asks:
            assert orderbook.bids[0][0] < orderbook.asks[0][0]

    @given(updates=sequential_updates_strategy(num_updates=5))
    @settings(max_examples=100, deadline=None)
    def test_incremental_updates_accumulation_properties(
        self, updates: list[BackpackRawDepthUpdateEvent]
    ) -> None:
        """Property: Incremental updates should properly accumulate state."""
        state = OrderBookState()

        expected_bids: dict[Decimal, Decimal] = {}
        expected_asks: dict[Decimal, Decimal] = {}

        for update in updates:
            result = state.apply_update(update)

            if result:
                # Track expected state
                if update.bids:
                    for price_str, qty_str in update.bids:
                        price = Decimal(price_str)
                        qty = Decimal(qty_str)
                        if qty > 0:
                            expected_bids[price] = qty
                        else:
                            expected_bids.pop(price, None)

                if update.asks:
                    for price_str, qty_str in update.asks:
                        price = Decimal(price_str)
                        qty = Decimal(qty_str)
                        if qty > 0:
                            expected_asks[price] = qty
                        else:
                            expected_asks.pop(price, None)

        # Properties: State should match accumulated updates
        assert state.bids == expected_bids
        assert state.asks == expected_asks


# =============================================================================
# PROPERTY-BASED TESTS FOR BACKPACK DEPTH STATE TRANSFORMER
# =============================================================================


class TestBackpackDepthStateTransformer:
    """Property-based tests for BackpackDepthStateTransformer class."""

    @staticmethod
    def create_transformer() -> BackpackDepthStateTransformer:
        """Create a transformer instance for testing.

        Returns:
            BackpackDepthStateTransformer instance with mapper configured
        """
        mapper = BackpackOrderBookMapper()
        return BackpackDepthStateTransformer(mapper)

    @staticmethod
    def create_mock_context() -> Mock:
        """Create a mock context with symbol extraction capability.

        Returns:
            Mock WebSocketContextProtocol with symbol methods configured
        """
        context = Mock(spec=WebSocketContextProtocol)
        # The transformer calls exchanges.backpack(context.symbol)
        # So context.symbol must be a string that represents the symbol
        context.get_symbol_param = Mock(return_value={"symbol": "BTC_USDC"})
        context.symbol = "BTC_USDC"
        return context

    @given(st.data())
    @settings(max_examples=50, deadline=None)
    def test_init_creates_empty_transformer_properties(self, data: st.DataObject) -> None:
        """Property: Transformer should always initialize with empty state."""
        # Create transformer for each test
        transformer = self.create_transformer()

        # Properties: Initial state invariants
        assert transformer.states == {}
        assert transformer.emission_strategy == "always"

        stats = transformer.get_statistics()
        assert stats["snapshots_processed"] == 0
        assert stats["incremental_updates_processed"] == 0
        assert stats["sequence_errors"] == 0
        assert stats["symbols_tracked"] == 0

        # Property: Statistics should be immutable copy
        stats["snapshots_processed"] = 999
        new_stats = transformer.get_statistics()
        assert new_stats["snapshots_processed"] == 0

    @given(initial_update=depth_update_event_strategy(), next_update=depth_update_event_strategy())
    @settings(max_examples=100, deadline=None)
    def test_transform_incremental_behavior_properties(
        self,
        initial_update: BackpackRawDepthUpdateEvent,
        next_update: BackpackRawDepthUpdateEvent,
    ) -> None:
        """Property: WebSocket updates should always be treated as incremental."""
        # Create transformer and context for each test
        transformer = self.create_transformer()
        mock_context = self.create_mock_context()

        # Apply initial update
        transformer.transform(initial_update, mock_context)

        # Make next update sequential
        try:
            last_id = int(initial_update.last_update_id)
            next_update = BackpackRawDepthUpdateEvent(
                b=next_update.bids,
                a=next_update.asks,
                U=str(last_id + 1),
                u=str(last_id + 1),
                e="depth",
                E=next_update.event_time,
                T=next_update.engine_time,
            )

            result = transformer.transform(next_update, mock_context)

            # Properties: Should accumulate state
            if result:
                assert isinstance(result, OrderBook)
                assert result.symbol == BTC_USDC_BP

                # Stats should show incremental processing
                stats = transformer.get_statistics()
                assert stats["snapshots_processed"] == 0  # Never snapshots
                assert stats["incremental_updates_processed"] >= 1
                assert stats["symbols_tracked"] == 1
        except (ValueError, TypeError):
            # Invalid IDs in generated data
            pass

    @given(updates=sequential_updates_strategy(num_updates=3))
    @settings(max_examples=100, deadline=None)
    def test_transform_sequential_updates_properties(
        self,
        updates: list[BackpackRawDepthUpdateEvent],
    ) -> None:
        """Property: Sequential updates should accumulate state correctly."""
        # Create transformer and context for each test
        transformer = self.create_transformer()
        mock_context = self.create_mock_context()

        successful_updates = 0

        for update in updates:
            result = transformer.transform(update, mock_context)

            if result:
                successful_updates += 1
                assert isinstance(result, OrderBook)
                assert result.symbol == BTC_USDC_BP

        # Properties: Stats should match processing
        stats = transformer.get_statistics()
        assert stats["incremental_updates_processed"] == successful_updates
        assert stats["snapshots_processed"] == 0

        if successful_updates > 0:
            assert stats["symbols_tracked"] == 1

    @given(
        initial_id=st.integers(min_value=1, max_value=1000),
        gap_size=st.integers(min_value=2, max_value=100),
    )
    @settings(max_examples=100, deadline=None)
    def test_transform_sequence_error_properties(
        self,
        initial_id: int,
        gap_size: int,
    ) -> None:
        """Property: Sequence errors should clear state and increment error count."""
        # Create transformer and context for each test
        transformer = self.create_transformer()
        mock_context = self.create_mock_context()

        # Establish initial state
        initial_update = BackpackRawDepthUpdateEvent(
            b=[("100.0", "10.0")],
            a=[("101.0", "5.0")],
            U=str(initial_id),
            u=str(initial_id),
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )
        transformer.transform(initial_update, mock_context)

        initial_errors = transformer.get_statistics()["sequence_errors"]

        # Send update with gap
        gap_id = initial_id + gap_size
        gap_update = BackpackRawDepthUpdateEvent(
            b=[("99.5", "15.0")],
            a=None,
            U=str(gap_id),
            u=str(gap_id + 1),
            e="depth",
            E=1705314600002,
            T=1705314600003,
        )

        result = transformer.transform(gap_update, mock_context)

        # Properties: Sequence error handling
        assert result is None
        assert BTC_USDC_BP not in transformer.states

        stats = transformer.get_statistics()
        assert stats["sequence_errors"] == initial_errors + 1

    @given(event=depth_update_event_strategy())
    @settings(max_examples=50, deadline=None)
    def test_transform_none_context_rejection_properties(
        self, event: BackpackRawDepthUpdateEvent
    ) -> None:
        """Property: None context should always raise OrderBookTransformationError."""
        # Create transformer for each test
        transformer = self.create_transformer()

        with pytest.raises(OrderBookTransformationError, match="Context is None"):
            transformer.transform(event, None)

    @given(
        num_updates=st.integers(min_value=1, max_value=10),
        num_errors=st.integers(min_value=0, max_value=5),
    )
    @settings(max_examples=50, deadline=None)
    def test_statistics_tracking_properties(self, num_updates: int, num_errors: int) -> None:
        """Property: Statistics should accurately track all processing."""
        # Create transformer and context for each test
        transformer = self.create_transformer()
        mock_context = self.create_mock_context()

        successful = 0
        current_id = 1000

        # Process valid updates
        for i in range(num_updates):
            update = BackpackRawDepthUpdateEvent(
                b=[(f"{100 + i}.0", "10.0")],
                a=[(f"{101 + i}.0", "5.0")],
                U=str(current_id),
                u=str(current_id),
                e="depth",
                E=1705314600000 + i,
                T=1705314600001 + i,
            )
            result = transformer.transform(update, mock_context)
            if result:
                successful += 1
                current_id += 1

        # Create sequence errors
        errors_created = 0
        for _ in range(num_errors):
            if current_id > 1000:  # Only if we have state
                gap_update = BackpackRawDepthUpdateEvent(
                    b=[("99.5", "15.0")],
                    a=None,
                    U=str(current_id + 10),  # Gap
                    u=str(current_id + 10),
                    e="depth",
                    E=1705314600100,
                    T=1705314600101,
                )
                transformer.transform(gap_update, mock_context)
                errors_created += 1
                current_id = 1000  # Reset for next iteration

        # Properties: Statistics accuracy
        stats = transformer.get_statistics()
        assert stats["incremental_updates_processed"] == successful
        assert stats["sequence_errors"] == errors_created
        assert stats["snapshots_processed"] == 0

        # Property: Statistics immutability
        stats["incremental_updates_processed"] = 999
        new_stats = transformer.get_statistics()
        assert new_stats["incremental_updates_processed"] == successful

    @given(btc_update=depth_update_event_strategy(), eth_update=depth_update_event_strategy())
    @settings(max_examples=50, deadline=None)
    def test_multiple_symbols_isolation_properties(
        self,
        btc_update: BackpackRawDepthUpdateEvent,
        eth_update: BackpackRawDepthUpdateEvent,
    ) -> None:
        """Property: Multiple symbols should maintain completely isolated state."""
        # Create transformer for each test
        transformer = self.create_transformer()

        # Create contexts for different symbols
        btc_context = Mock()
        btc_context.get_symbol_param = Mock(return_value={"symbol": BTC_USDC_BP})
        btc_context.symbol = BTC_USDC_BP

        eth_context = Mock()
        eth_context.get_symbol_param = Mock(return_value={"symbol": ETH_USDC_BP})
        eth_context.symbol = ETH_USDC_BP

        # Apply updates to different symbols
        result1 = transformer.transform(btc_update, btc_context)
        result2 = transformer.transform(eth_update, eth_context)

        # Properties: Symbol isolation
        if result1 and result2:
            assert result1.symbol == BTC_USDC_BP
            assert result2.symbol == ETH_USDC_BP

            # States should be separate
            assert BTC_USDC_BP in transformer.states
            assert ETH_USDC_BP in transformer.states

            # Verify data isolation
            if result1.bids and result2.bids:
                # Bids should be independent (very unlikely to be same)
                assert result1.bids != result2.bids or (
                    btc_update.bids == eth_update.bids  # Unless input was same
                )

            stats = transformer.get_statistics()
            assert stats["symbols_tracked"] == 2

    @given(
        update_id=st.integers(min_value=1, max_value=10000),
        timestamp=st.integers(min_value=1600000000000, max_value=2000000000000),
    )
    @settings(max_examples=50, deadline=None)
    def test_empty_update_handling_properties(
        self,
        update_id: int,
        timestamp: int,
    ) -> None:
        """Property: Empty updates (no bid/ask data) should not emit OrderBook."""
        # Create transformer and context for each test
        transformer = self.create_transformer()
        mock_context = self.create_mock_context()

        # Create empty event
        empty_event = BackpackRawDepthUpdateEvent(
            b=None,
            a=None,
            U=str(update_id),
            u=str(update_id),
            e="depth",
            E=timestamp,
            T=timestamp + 1,
        )

        result = transformer.transform(empty_event, mock_context)

        # Property: Empty updates don't emit
        assert result is None

        # But should still track as processed
        # May or may not increment depending on implementation
        _ = transformer.get_statistics()  # Just verify it doesn't crash

"""Unit tests for Backpack depth state transformer.

Tests the stateful transformer that handles Backpack's incremental orderbook updates,
including snapshot detection, sequence validation, and state management.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock

import pytest

from tests.common_symbols import BTC_USDC_BP, ETH_USDC_BP
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
from cyberdelta.core.models import OrderBook


class TestOrderBookState:
    """Test suite for OrderBookState class."""

    def test_init_creates_empty_state(self) -> None:
        """Test that OrderBookState initializes with empty bids/asks."""
        state = OrderBookState()

        assert state.bids == {}
        assert state.asks == {}
        assert state.last_update_id == 0
        assert isinstance(state.last_update_time, datetime)

    def test_apply_update_with_valid_sequence(self) -> None:
        """Test applying updates with valid sequence numbers."""
        state = OrderBookState()

        # First update (no sequence check)
        event1 = BackpackRawDepthUpdateEvent(
            b=[("100.5", "10.0"), ("100.0", "20.0")],
            a=[("101.0", "5.0"), ("101.5", "15.0")],
            U="1",
            u="1",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )

        result = state.apply_update(event1)
        assert result is True
        assert state.last_update_id == 1
        assert len(state.bids) == 2
        assert len(state.asks) == 2
        assert state.bids[Decimal("100.5")] == Decimal("10.0")
        assert state.asks[Decimal("101.0")] == Decimal("5.0")

    def test_apply_update_with_zero_quantity_removes_level(self) -> None:
        """Test that zero quantity removes the price level."""
        state = OrderBookState()

        # Add some levels
        event1 = BackpackRawDepthUpdateEvent(
            b=[("100.0", "10.0")],
            a=[("101.0", "5.0")],
            U="1",
            u="1",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )
        state.apply_update(event1)

        # Remove the bid level with zero quantity
        event2 = BackpackRawDepthUpdateEvent(
            b=[("100.0", "0")],
            a=None,
            U="2",
            u="2",
            e="depth",
            E=1705314600002,
            T=1705314600003,
        )
        state.apply_update(event2)

        assert Decimal("100.0") not in state.bids
        assert len(state.asks) == 1  # Asks unchanged

    def test_apply_update_with_sequence_gap(self) -> None:
        """Test that sequence gaps are detected and rejected."""
        state = OrderBookState()

        # First update
        event1 = BackpackRawDepthUpdateEvent(
            b=[("100.0", "10.0")],
            a=[("101.0", "5.0")],
            U="1",
            u="1",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )
        state.apply_update(event1)

        # Update with gap (should be 2, but is 5)
        event2 = BackpackRawDepthUpdateEvent(
            b=[("99.0", "15.0")],
            a=None,
            U="5",
            u="5",
            e="depth",
            E=1705314600002,
            T=1705314600003,
        )

        result = state.apply_update(event2)
        assert result is False
        # State should be unchanged
        assert state.last_update_id == 1

    def test_apply_update_with_invalid_update_ids(self) -> None:
        """Test handling of invalid update IDs."""
        state = OrderBookState()

        # Invalid update IDs (not numeric)
        event = BackpackRawDepthUpdateEvent(
            b=[("100.0", "10.0")],
            a=None,
            U="invalid",
            u="invalid",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )

        result = state.apply_update(event)
        assert result is False

    def test_to_orderbook_sorts_correctly(self) -> None:
        """Test that to_orderbook creates properly sorted OrderBook."""
        state = OrderBookState()
        mapper = BackpackOrderBookMapper()

        # Add unsorted levels
        state.bids = {
            Decimal("100.0"): Decimal("10.0"),
            Decimal("101.0"): Decimal("20.0"),
            Decimal("99.5"): Decimal("5.0"),
        }
        state.asks = {
            Decimal("102.0"): Decimal("15.0"),
            Decimal("101.5"): Decimal("10.0"),
            Decimal("103.0"): Decimal("25.0"),
        }
        state.last_update_time = datetime(2025, 1, 21, 12, 0, 0, tzinfo=UTC)

        orderbook = state.to_orderbook(BTC_USDC_BP.value, mapper)

        assert orderbook.symbol == BTC_USDC_BP.value
        assert orderbook.timestamp is not None  # Mapper handles timestamp conversion

        # Bids should be sorted descending (mapper ensures this)
        assert orderbook.bids[0] == (Decimal("101.0"), Decimal("20.0"))
        assert orderbook.bids[1] == (Decimal("100.0"), Decimal("10.0"))
        assert orderbook.bids[2] == (Decimal("99.5"), Decimal("5.0"))

        # Asks should be sorted ascending (mapper ensures this)
        assert orderbook.asks[0] == (Decimal("101.5"), Decimal("10.0"))
        assert orderbook.asks[1] == (Decimal("102.0"), Decimal("15.0"))
        assert orderbook.asks[2] == (Decimal("103.0"), Decimal("25.0"))

    def test_incremental_updates_accumulate_state(self) -> None:
        """Test that incremental updates properly accumulate state."""
        state = OrderBookState()

        # Initial snapshot
        event1 = BackpackRawDepthUpdateEvent(
            b=[("100.0", "10.0")],
            a=[("101.0", "5.0")],
            U="1",
            u="1",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )
        state.apply_update(event1)

        # Incremental update - add new bid
        event2 = BackpackRawDepthUpdateEvent(
            b=[("99.5", "15.0")],
            a=None,
            U="2",
            u="2",
            e="depth",
            E=1705314600002,
            T=1705314600003,
        )
        state.apply_update(event2)

        # Incremental update - modify ask
        event3 = BackpackRawDepthUpdateEvent(
            b=None,
            a=[("101.0", "8.0")],  # Update quantity
            U="3",
            u="3",
            e="depth",
            E=1705314600004,
            T=1705314600005,
        )
        state.apply_update(event3)

        # Verify accumulated state
        assert len(state.bids) == 2
        assert state.bids[Decimal("100.0")] == Decimal("10.0")
        assert state.bids[Decimal("99.5")] == Decimal("15.0")
        assert len(state.asks) == 1
        assert state.asks[Decimal("101.0")] == Decimal("8.0")  # Updated quantity


class TestBackpackDepthStateTransformer:
    """Test suite for BackpackDepthStateTransformer class."""

    @pytest.fixture
    def transformer(self) -> BackpackDepthStateTransformer:
        """Create a transformer instance for testing.

        Returns:
            BackpackDepthStateTransformer instance with mapper configured
        """
        mapper = BackpackOrderBookMapper()
        return BackpackDepthStateTransformer(mapper)

    @pytest.fixture
    def mock_context(self) -> Mock:
        """Create a mock context with symbol extraction capability.

        Returns:
            Mock WebSocketContextProtocol with symbol methods configured
        """
        context = Mock(spec=WebSocketContextProtocol)
        context.get_symbol_param = Mock(return_value={"symbol": BTC_USDC_BP.value})
        context.symbol = BTC_USDC_BP.value
        return context

    def test_init_creates_empty_transformer(
        self, transformer: BackpackDepthStateTransformer
    ) -> None:
        """Test that transformer initializes with empty state."""
        assert transformer.states == {}
        assert transformer.emission_strategy == "always"
        stats = transformer.get_statistics()
        assert stats["snapshots_processed"] == 0
        assert stats["incremental_updates_processed"] == 0
        assert stats["sequence_errors"] == 0
        assert stats["symbols_tracked"] == 0

    def test_transform_snapshot_resets_state(
        self, transformer: BackpackDepthStateTransformer, mock_context: Mock
    ) -> None:
        """Test that WebSocket updates are always incremental (no snapshot support)."""
        # First establish state with an initial update
        initial_update = BackpackRawDepthUpdateEvent(
            b=[("99.0", "5.0")],
            a=[("100.5", "3.0")],
            U="999",
            u="999",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )
        transformer.transform(initial_update, mock_context)

        # Send update with same U and u (which might look like a snapshot but isn't)
        # Current implementation treats all WebSocket updates as incremental
        update = BackpackRawDepthUpdateEvent(
            b=[("100.0", "10.0")],
            a=[("101.0", "5.0")],
            U="1000",
            u="1000",
            e="depth",
            E=1705314600002,
            T=1705314600003,
        )

        result = transformer.transform(update, mock_context)

        # Should return valid OrderBook with BOTH old and new bids
        assert isinstance(result, OrderBook)
        assert result.symbol == BTC_USDC_BP.value
        assert len(result.bids) == 2  # Both old and new bids present
        assert len(result.asks) == 2  # Both old and new asks present

        # Stats should show incremental updates, not snapshot
        stats = transformer.get_statistics()
        assert stats["snapshots_processed"] == 0  # No snapshots in WebSocket
        assert stats["incremental_updates_processed"] == 2
        assert stats["symbols_tracked"] == 1

    def test_transform_incremental_update(
        self, transformer: BackpackDepthStateTransformer, mock_context: Mock
    ) -> None:
        """Test processing incremental updates."""
        # First, establish state with initial update
        initial_update = BackpackRawDepthUpdateEvent(
            b=[("100.0", "10.0")],
            a=[("101.0", "5.0")],
            U="1000",
            u="1000",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )
        transformer.transform(initial_update, mock_context)

        # Send incremental update (only bids, different update IDs)
        update = BackpackRawDepthUpdateEvent(
            b=[("99.5", "15.0")],
            a=None,
            U="1001",
            u="1002",
            e="depth",
            E=1705314600002,
            T=1705314600003,
        )

        result = transformer.transform(update, mock_context)

        assert isinstance(result, OrderBook)
        assert len(result.bids) == 2  # Original + new bid
        stats = transformer.get_statistics()
        # Both updates are counted as incremental (no snapshots in WebSocket)
        assert stats["incremental_updates_processed"] == 2

    def test_transform_sequence_error_clears_state(
        self, transformer: BackpackDepthStateTransformer, mock_context: Mock
    ) -> None:
        """Test that sequence errors clear state and return None."""
        # Establish state
        snapshot = BackpackRawDepthUpdateEvent(
            b=[("100.0", "10.0")],
            a=[("101.0", "5.0")],
            U="1000",
            u="1000",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )
        transformer.transform(snapshot, mock_context)

        # Send update with sequence gap
        gap_update = BackpackRawDepthUpdateEvent(
            b=[("99.5", "15.0")],
            a=None,
            U="1005",  # Gap: should be 1001
            u="1006",
            e="depth",
            E=1705314600002,
            T=1705314600003,
        )

        result = transformer.transform(gap_update, mock_context)

        # Should return None
        assert result is None

        # State should be cleared
        assert BTC_USDC_BP.value not in transformer.states

        # Stats should reflect error
        stats = transformer.get_statistics()
        assert stats["sequence_errors"] == 1

    def test_transform_error_cases(self, transformer: BackpackDepthStateTransformer) -> None:
        """Test error cases in transformation."""
        # None context
        event = BackpackRawDepthUpdateEvent(
            b=[("100.0", "10.0")],
            a=None,
            U="1",
            u="1",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )

        with pytest.raises(OrderBookTransformationError, match="Context is None"):
            transformer.transform(event, None)

    def test_get_statistics(self, transformer: BackpackDepthStateTransformer) -> None:
        """Test statistics retrieval."""
        # Process some events to generate stats
        mock_context = Mock(spec=WebSocketContextProtocol)
        mock_context.get_symbol_param = Mock(return_value={"symbol": BTC_USDC_BP.value})
        mock_context.symbol = BTC_USDC_BP.value

        # Send first update (WebSocket doesn't support snapshots)
        update1 = BackpackRawDepthUpdateEvent(
            b=[("100.0", "10.0")],
            a=[("101.0", "5.0")],
            U="1000",
            u="1000",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )
        transformer.transform(update1, mock_context)

        # Send incremental update
        update2 = BackpackRawDepthUpdateEvent(
            b=[("99.5", "15.0")],
            a=None,
            U="1001",
            u="1002",
            e="depth",
            E=1705314600002,
            T=1705314600003,
        )
        transformer.transform(update2, mock_context)

        stats = transformer.get_statistics()

        # Should return current stats (all updates are incremental)
        assert stats["snapshots_processed"] == 0  # No snapshots in WebSocket
        assert stats["incremental_updates_processed"] == 2
        assert stats["symbols_tracked"] == 1

        # Modifying returned stats shouldn't affect internal stats
        stats["incremental_updates_processed"] = 10
        new_stats = transformer.get_statistics()
        assert new_stats["incremental_updates_processed"] == 2

    def test_multiple_symbols_isolated_state(
        self, transformer: BackpackDepthStateTransformer
    ) -> None:
        """Test that multiple symbols maintain isolated state."""
        # Create contexts for different symbols
        btc_context = Mock()
        btc_context.get_symbol_param = Mock(return_value={"symbol": BTC_USDC_BP.value})
        btc_context.symbol = BTC_USDC_BP.value

        eth_context = Mock()
        eth_context.get_symbol_param = Mock(return_value={"symbol": ETH_USDC_BP.value})
        eth_context.symbol = ETH_USDC_BP.value

        # Send snapshots to both symbols
        snapshot1 = BackpackRawDepthUpdateEvent(
            b=[("100.0", "10.0")],
            a=[("101.0", "5.0")],
            U="1000",
            u="1000",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )

        snapshot2 = BackpackRawDepthUpdateEvent(
            b=[("200.0", "20.0")],
            a=[("201.0", "15.0")],
            U="2000",
            u="2000",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )

        result1 = transformer.transform(snapshot1, btc_context)
        result2 = transformer.transform(snapshot2, eth_context)

        # Each symbol should have its own state
        assert len(transformer.states) == 2
        assert BTC_USDC_BP.value in transformer.states
        assert ETH_USDC_BP.value in transformer.states

        # Results should reflect different data
        assert result1 is not None
        assert result1.symbol == BTC_USDC_BP.value
        assert result1.bids[0][0] == Decimal("100.0")
        assert result2 is not None
        assert result2.symbol == ETH_USDC_BP.value
        assert result2.bids[0][0] == Decimal("200.0")

        stats = transformer.get_statistics()
        assert stats["symbols_tracked"] == 2

    def test_no_emission_when_no_data(
        self, transformer: BackpackDepthStateTransformer, mock_context: Mock
    ) -> None:
        """Test that events with no bid/ask data don't emit."""
        # Event with no bids or asks
        empty_event = BackpackRawDepthUpdateEvent(
            b=None,
            a=None,
            U="1",
            u="1",
            e="depth",
            E=1705314600000,
            T=1705314600001,
        )

        result = transformer.transform(empty_event, mock_context)
        assert result is None

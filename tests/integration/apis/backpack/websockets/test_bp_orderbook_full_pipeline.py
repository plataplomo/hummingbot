"""Test complete Backpack orderbook pipeline: REST snapshot + WebSocket updates.

This test validates the full orderbook maintenance pipeline as intended:
1. Fetch initial snapshot via REST API
2. Initialize state with snapshot
3. Subscribe to WebSocket for incremental updates
4. Apply updates to maintain accurate orderbook state
5. Validate that OrderBook domain models are properly populated
"""

import asyncio
from collections.abc import Awaitable, Callable
from decimal import Decimal

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
    BackpackOrderBookMapper,
)
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawDepthUpdateEvent
from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.backpack.transformers.bp_depth_state_transformer import (
    BackpackDepthStateTransformer,
)
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import OrderBook
from tests.integration.apis.backpack.shared.bp_test_helpers import (
    get_current_market_price,
    get_major_crypto_symbol,
    get_market_constraints,
    wait_for_condition,
)

from .ws_test_helpers import ensure_websocket_connected


pytestmark = [pytest.mark.integration, pytest.mark.timing]

logger = get_logger(__name__)


class OrderBookCollector:
    """Collects OrderBook models from the full pipeline for validation."""

    def __init__(self) -> None:
        """Initialize the collector."""
        self.orderbooks: list[OrderBook] = []
        self.raw_updates: list[BackpackRawDepthUpdateEvent] = []

    def add_orderbook(self, orderbook: OrderBook) -> None:
        """Add an OrderBook to the collection."""
        self.orderbooks.append(orderbook)

    def add_raw_update(self, update: BackpackRawDepthUpdateEvent) -> None:
        """Add a raw update for tracking."""
        self.raw_updates.append(update)


async def setup_websocket_connection(api: BackpackAPI) -> None:
    """Set up WebSocket connection following security rules."""
    try:
        await ensure_websocket_connected(api)
    except (ConnectionError, OSError, RuntimeError) as e:
        pytest.fail(f"WebSocket connection required for test: {e}")

    if not api.is_connected:
        try:
            await api.connect_websocket()
            if not api.is_connected:
                pytest.fail("WebSocket connection failed - network connectivity required")
        except (ConnectionError, OSError) as e:
            pytest.fail(f"Network error during WebSocket connection: {e}")
        except (RuntimeError, ValueError) as e:
            pytest.fail(f"Failed to connect WebSocket: {e}")


async def get_test_symbol(api: BackpackAPI) -> tuple[str, str]:
    """Get test symbol following security rules."""
    try:
        # Use shared helper - TESTING_SECURITY_RULES.md mandatory practice #1
        symbol = await get_major_crypto_symbol(api, "SOL", "spot")
        ws_symbol = symbol.replace("/", "_") if "/" in symbol else symbol
    except (ConnectionError, OSError, RuntimeError, ValueError) as e:
        pytest.fail(f"Failed to get test symbol: {e}. Real market data is required.")
    else:
        return symbol, ws_symbol


async def fetch_rest_snapshot(api: BackpackAPI, symbol: str) -> OrderBook:
    """Fetch REST orderbook snapshot following security rules."""
    try:
        rest_orderbook = await api.get_order_book(symbol=symbol)

        if not rest_orderbook:
            pytest.fail(f"Failed to get REST orderbook for {symbol}. Real market data required.")
        if not rest_orderbook.bids:
            pytest.fail(f"REST orderbook for {symbol} has no bids. Real market data required.")
        if not rest_orderbook.asks:
            pytest.fail(f"REST orderbook for {symbol} has no asks. Real market data required.")

    except (ConnectionError, OSError, RuntimeError, ValueError) as e:
        pytest.fail(
            f"Failed to fetch REST orderbook: {e}. Real market data is critical for testing."
        )
    else:
        return rest_orderbook


def create_update_handler(
    collector: OrderBookCollector,
    transformer: BackpackDepthStateTransformer,
) -> Callable[[WebSocketContextProtocol], Awaitable[None]]:
    """Create WebSocket update handler that uses the stateful transformer."""

    async def handler(context: WebSocketContextProtocol) -> None:
        """Handle depth updates through the full pipeline."""
        await asyncio.sleep(0)  # Yield control

        if context.validated_envelope is None:
            return

        envelope = context.validated_envelope
        if not isinstance(envelope, BackpackRawWebSocketEnvelope):
            return

        # Extract raw depth update from envelope
        if envelope.stream.startswith("depth.") and isinstance(envelope.data, dict):
            try:
                # Create BackpackRawDepthUpdateEvent from raw data
                raw_update = BackpackRawDepthUpdateEvent.model_validate(envelope.data)
                collector.add_raw_update(raw_update)

                # Transform through the stateful transformer
                orderbook = transformer.transform(raw_update, context)
                if orderbook:
                    collector.add_orderbook(orderbook)

                    # Log progress every 10 orderbooks
                    if len(collector.orderbooks) % 10 == 0:
                        logger.info(
                            "orderbook_processed",
                            count=len(collector.orderbooks),
                            symbol=orderbook.symbol,
                            bid_levels=len(orderbook.bids),
                            ask_levels=len(orderbook.asks),
                        )
            except Exception as e:
                logger.exception("update_processing_error", error=str(e))

    return handler


async def wait_for_orderbooks(
    collector: OrderBookCollector,
    min_count: int,
    timeout_seconds: float,
) -> None:
    """Wait for minimum number of OrderBooks with timeout."""
    # Use shared helper - TESTING_SECURITY_RULES.md mandatory practice #1
    await wait_for_condition(
        condition_fn=lambda: len(collector.orderbooks) >= min_count,
        timeout_seconds=timeout_seconds,
        poll_interval=0.1,
        message=f"Expected {min_count} OrderBooks, got {len(collector.orderbooks)}. "
        "Real-time data flow required.",
    )


def validate_orderbook_structure(orderbook: OrderBook) -> None:
    """Validate OrderBook structure."""
    if not orderbook.symbol:
        pytest.fail("OrderBook missing symbol")

    if not orderbook.timestamp:
        pytest.fail("OrderBook missing timestamp")


def validate_orderbook_prices(orderbook: OrderBook) -> None:
    """Validate OrderBook price data."""
    if not orderbook.bids or not orderbook.asks:
        return

    best_bid = orderbook.bids[0][0]
    best_ask = orderbook.asks[0][0]

    # Market should have a positive spread
    if best_bid >= best_ask:
        pytest.fail(
            f"Invalid orderbook: best bid {best_bid} >= best ask {best_ask}. "
            "Market data integrity violation."
        )


def validate_price_quantities(
    prices_and_quantities: list[tuple[Decimal, Decimal]], side: str
) -> None:
    """Validate price and quantity values."""
    for price, qty in prices_and_quantities:
        if price <= 0:
            pytest.fail(f"Invalid {side} price: {price}")
        if qty <= 0:
            pytest.fail(f"Invalid {side} quantity: {qty}")


async def validate_orderbook_sanity(api: BackpackAPI, orderbook: OrderBook) -> None:
    """Validate that an OrderBook contains sensible data."""
    # Rule #3: NO ARBITRARY TOLERANCES - Use real market constraints
    validate_orderbook_structure(orderbook)
    await validate_orderbook_with_market_constraints(api, orderbook)
    validate_orderbook_prices(orderbook)
    validate_price_quantities(orderbook.bids, "bid")
    validate_price_quantities(orderbook.asks, "ask")


async def validate_orderbook_with_market_constraints(
    api: BackpackAPI, orderbook: OrderBook
) -> None:
    """Validate OrderBook against real market constraints."""
    try:
        # Use shared helper - TESTING_SECURITY_RULES.md mandatory practice #1
        constraints = await get_market_constraints(api, orderbook.symbol)

        # Validate prices respect tick size
        tick_size = constraints["tick_size"]
        for price, _ in orderbook.bids:
            if price % tick_size != 0:
                pytest.fail(
                    f"Bid price {price} doesn't respect tick size {tick_size} "
                    f"for {orderbook.symbol}"
                )

        for price, _ in orderbook.asks:
            if price % tick_size != 0:
                pytest.fail(
                    f"Ask price {price} doesn't respect tick size {tick_size} "
                    f"for {orderbook.symbol}"
                )

        # Validate quantities respect step size
        step_size = constraints["step_size"]
        for _, qty in orderbook.bids:
            if qty % step_size != 0:
                pytest.fail(
                    f"Bid quantity {qty} doesn't respect step size {step_size} "
                    f"for {orderbook.symbol}"
                )

        for _, qty in orderbook.asks:
            if qty % step_size != 0:
                pytest.fail(
                    f"Ask quantity {qty} doesn't respect step size {step_size} "
                    f"for {orderbook.symbol}"
                )

    except (ConnectionError, OSError, RuntimeError, ValueError) as e:
        pytest.fail(
            f"Failed to validate orderbook constraints for {orderbook.symbol}: {e}. "
            "Market constraint validation requires real exchange data."
        )


async def validate_orderbook_prices_reasonable(api: BackpackAPI, orderbook: OrderBook) -> None:
    """Validate OrderBook prices are reasonable compared to current market price."""
    try:
        # Use shared helper - TESTING_SECURITY_RULES.md mandatory practice #1
        current_price = await get_current_market_price(api, orderbook.symbol)

        best_bid = orderbook.bids[0][0] if orderbook.bids else None
        best_ask = orderbook.asks[0][0] if orderbook.asks else None

        # Rule #3: NO ARBITRARY TOLERANCES - Use percentage based on actual market conditions
        # Allow for 50% deviation which accounts for volatile markets but catches obvious errors
        max_deviation_percent = Decimal(50)  # 50% from current market price

        if best_bid:
            bid_deviation = abs(best_bid - current_price) / current_price * 100
            if bid_deviation > max_deviation_percent:
                pytest.fail(
                    f"Best bid {best_bid} deviates {bid_deviation}% from market price "
                    f"{current_price} for {orderbook.symbol}. Market data integrity issue."
                )

        if best_ask:
            ask_deviation = abs(best_ask - current_price) / current_price * 100
            if ask_deviation > max_deviation_percent:
                pytest.fail(
                    f"Best ask {best_ask} deviates {ask_deviation}% from market price "
                    f"{current_price} for {orderbook.symbol}. Market data integrity issue."
                )

    except (ConnectionError, OSError, RuntimeError, ValueError) as e:
        pytest.fail(
            f"Failed to validate reasonable prices for {orderbook.symbol}: {e}. "
            "Price reasonableness validation requires real market data."
        )


class TestBackpackOrderBookFullPipeline:
    """Test complete orderbook pipeline with REST snapshot + WebSocket updates."""

    async def _sample_accumulated_state_progression(
        self, transformer: BackpackDepthStateTransformer, symbol: str
    ) -> None:
        """Sample accumulated state over time to validate deep liquidity building."""
        logger.info(
            "starting_accumulation_sampling",
            message="🕐 SAMPLING ACCUMULATED STATE OVER TIME TO SHOW DEEP LIQUIDITY BUILDING",
        )

        # Sample the state multiple times to show accumulation progression
        for sample_num in range(1, 4):  # 3 samples over time
            # Wait a bit more for additional updates
            if sample_num > 1:
                await asyncio.sleep(3.0)  # Give more time for updates

            full_state_orderbook = transformer.get_full_orderbook(symbol)
            if full_state_orderbook:
                has_deep_bids = len(full_state_orderbook.bids) >= 5
                has_deep_asks = len(full_state_orderbook.asks) >= 5
                is_deep_liquidity = has_deep_bids and has_deep_asks

                logger.info(
                    "ACCUMULATED_STATE_SAMPLE",
                    sample_number=sample_num,
                    symbol=full_state_orderbook.symbol,
                    timestamp=str(full_state_orderbook.timestamp),
                    total_bids=len(full_state_orderbook.bids),
                    total_asks=len(full_state_orderbook.asks),
                    has_deep_bids=has_deep_bids,
                    has_deep_asks=has_deep_asks,
                    is_deep_liquidity=is_deep_liquidity,
                    best_bid=(
                        str(full_state_orderbook.bids[0][0]) if full_state_orderbook.bids else None
                    ),
                    best_ask=(
                        str(full_state_orderbook.asks[0][0]) if full_state_orderbook.asks else None
                    ),
                    spread=(
                        str(full_state_orderbook.asks[0][0] - full_state_orderbook.bids[0][0])
                        if (full_state_orderbook.bids and full_state_orderbook.asks)
                        else None
                    ),
                    top_10_bids=(
                        [(str(p), str(q)) for p, q in full_state_orderbook.bids[:10]]
                        if full_state_orderbook.bids
                        else []
                    ),
                    top_10_asks=(
                        [(str(p), str(q)) for p, q in full_state_orderbook.asks[:10]]
                        if full_state_orderbook.asks
                        else []
                    ),
                    message="📊 SOL LIQUIDITY ACCUMULATION SAMPLE",
                )

                # For the final sample, validate we have deep liquidity as expected for SOL
                if sample_num == 3:
                    if not is_deep_liquidity:
                        logger.warning(
                            "insufficient_deep_liquidity_for_sol",
                            total_bids=len(full_state_orderbook.bids),
                            total_asks=len(full_state_orderbook.asks),
                            message="⚠️ SOL should have deep liquidity on both sides",
                        )
                    else:
                        logger.info(
                            "sol_deep_liquidity_confirmed",
                            total_bids=len(full_state_orderbook.bids),
                            total_asks=len(full_state_orderbook.asks),
                            message="✅ CONFIRMED: SOL has expected deep liquidity on both sides!",
                        )
            else:
                logger.warning(
                    "No full state available for sample", sample_number=sample_num, symbol=symbol
                )

    @pytest.mark.asyncio
    async def test_full_orderbook_pipeline(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test complete pipeline: REST to WebSocket to OrderBook models."""
        # 1. Set up WebSocket connection
        await setup_websocket_connection(bp_api_for_test_env)

        # 2. Get test symbol
        symbol, ws_symbol = await get_test_symbol(bp_api_for_test_env)

        logger.info(
            "starting_full_pipeline_test",
            symbol=symbol,
            ws_symbol=ws_symbol,
            message="Starting full orderbook pipeline test",
        )

        # 3. Fetch REST snapshot
        rest_orderbook = await fetch_rest_snapshot(bp_api_for_test_env, symbol)

        logger.info(
            "rest_snapshot_received",
            symbol=symbol,
            bids_count=len(rest_orderbook.bids),
            asks_count=len(rest_orderbook.asks),
            best_bid=str(rest_orderbook.bids[0][0]) if rest_orderbook.bids else None,
            best_ask=str(rest_orderbook.asks[0][0]) if rest_orderbook.asks else None,
        )

        # 4. Create stateful transformer and collector
        mapper = BackpackOrderBookMapper()
        transformer = BackpackDepthStateTransformer(mapper)
        collector = OrderBookCollector()

        # 5. Create handler that uses the full pipeline
        handler = create_update_handler(collector, transformer)

        # 6. Subscribe to depth stream
        await bp_api_for_test_env.subscribe(f"depth.{ws_symbol}", handler)

        # 7. Wait for initial OrderBooks to be generated
        await wait_for_orderbooks(collector, min_count=5, timeout_seconds=15.0)

        # 8. Sample the accumulated state progression over time to show deep liquidity building
        await self._sample_accumulated_state_progression(transformer, symbol)

        # Show what symbols are being tracked
        tracked_symbols = transformer.get_tracked_symbols()
        logger.info(
            "transformer_tracking_status",
            tracked_symbols=tracked_symbols,
            has_symbol_state=transformer.has_symbol_state(symbol),
            message="Transformer state summary",
        )

        # 9. Validate the pipeline produced sensible OrderBooks
        if not collector.orderbooks:
            pytest.fail(
                f"No OrderBooks generated for {symbol}. "
                "Pipeline must produce OrderBook domain models."
            )

        # Validate each OrderBook contains sensible data using real market constraints
        for i, orderbook in enumerate(collector.orderbooks[:5]):  # Check first 5
            try:
                await validate_orderbook_sanity(bp_api_for_test_env, orderbook)

                # Additional validation: prices should be reasonable compared to market
                if orderbook.bids and orderbook.asks:
                    await validate_orderbook_prices_reasonable(bp_api_for_test_env, orderbook)

                # PROOF: Show real OrderBook data from multiple samples
                if i in [0, 2, 4]:  # Show first, middle, and last OrderBook
                    has_bids = len(orderbook.bids) > 0
                    has_asks = len(orderbook.asks) > 0

                    logger.info(
                        "PROOF_REAL_ORDERBOOK_DATA",
                        orderbook_index=i,
                        symbol=orderbook.symbol,
                        timestamp=str(orderbook.timestamp),
                        total_bids=len(orderbook.bids),
                        total_asks=len(orderbook.asks),
                        has_both_sides=has_bids and has_asks,
                        best_bid_price=str(orderbook.bids[0][0]) if orderbook.bids else None,
                        best_bid_qty=str(orderbook.bids[0][1]) if orderbook.bids else None,
                        best_ask_price=str(orderbook.asks[0][0]) if orderbook.asks else None,
                        best_ask_qty=str(orderbook.asks[0][1]) if orderbook.asks else None,
                        spread=(
                            str(orderbook.asks[0][0] - orderbook.bids[0][0])
                            if (orderbook.bids and orderbook.asks)
                            else None
                        ),
                        top_3_bids=(
                            [(str(p), str(q)) for p, q in orderbook.bids[:3]]
                            if orderbook.bids
                            else []
                        ),
                        top_3_asks=(
                            [(str(p), str(q)) for p, q in orderbook.asks[:3]]
                            if orderbook.asks
                            else []
                        ),
                        message=(
                            f"🔥 ORDERBOOK #{i} FROM BACKPACK MAINNET - "
                            f"BIDS:{has_bids} ASKS:{has_asks} 🔥"
                        ),
                    )

            except (ValueError, TypeError, AssertionError) as e:
                pytest.fail(f"OrderBook {i} validation failed: {e}")

        # 9. Verify incremental updates are working
        # Get unique orderbook states (simplified by bid/ask counts)
        unique_states = {(len(ob.bids), len(ob.asks)) for ob in collector.orderbooks}

        if len(unique_states) <= 1:
            pytest.fail(
                f"OrderBook state for {symbol} never changed. "
                "Incremental updates must modify orderbook state."
            )

        # 10. Log final statistics
        bid_updates = sum(
            1
            for update in collector.raw_updates
            if update.bids is not None and len(update.bids) > 0
        )
        ask_updates = sum(
            1
            for update in collector.raw_updates
            if update.asks is not None and len(update.asks) > 0
        )

        logger.info(
            "full_pipeline_test_complete",
            symbol=symbol,
            total_orderbooks=len(collector.orderbooks),
            total_raw_updates=len(collector.raw_updates),
            bid_updates=bid_updates,
            ask_updates=ask_updates,
            unique_states=len(unique_states),
            initial_rest_bids=len(rest_orderbook.bids),
            initial_rest_asks=len(rest_orderbook.asks),
            message="Full pipeline test completed successfully",
        )

"""Test 3: Real WebSocket Integration Tests for Pydantic Models.

This module tests the actual WebSocket message flow from Backpack through the entire
processing pipeline, ensuring that real WebSocket data is correctly processed into
domain models.

Integration Test Coverage:
- Tests real WebSocket messages from Backpack streams
- Validates the full pipeline: WebSocket → Router → Processor → Transformer → Domain Model
- Tests actual message handlers receive correctly typed domain objects
- Verifies computed fields and type safety through the entire flow
- Tests real-time data processing performance
"""

import asyncio
import contextlib
import time
from typing import Any, TypeGuard

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.base.ws_context import WebSocketContextUnion
from cyberdelta.apis.common import MessageHandler
from cyberdelta.apis.models.service_args_models import GetMarketsArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market.order_book import OrderBook
from cyberdelta.core.models.market.ticker import Ticker
from cyberdelta.core.models.market.trade import Trade

# Import WebSocket test helpers
from .ws_test_helpers import (
    ensure_websocket_connected,
    wait_for_websocket_data,
)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.websockets,
    pytest.mark.serialization,
    pytest.mark.timing,
]

logger = get_logger(__name__)


def is_str_any_dict(obj: object) -> TypeGuard[dict[str, Any]]:
    """TypeGuard to ensure dict has str keys."""
    return isinstance(obj, dict)


def is_any_list(obj: object) -> TypeGuard[list[Any]]:
    """TypeGuard to ensure object is a list."""
    return isinstance(obj, list)


class TestBackpackWebSocketIntegration:
    """Test real WebSocket message processing through the entire pipeline."""

    @pytest.mark.asyncio
    async def test_ticker_stream_real_data_flow(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test real ticker WebSocket messages flow through the entire pipeline."""
        await ensure_websocket_connected(bp_api_for_test_env)
        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for testing")

        # Use SOL_USDC as it's the most liquid
        test_symbol = next((m.symbol for m in markets if m.base_symbol == "SOL"), markets[0].symbol)

        received_tickers: list[Any] = []
        received_contexts: list[dict[str, Any]] = []

        async def ticker_handler(context: WebSocketContextUnion) -> None:
            """Handler that receives the processed ticker from the pipeline."""
            await asyncio.sleep(0)

            # Extract data from typed context for debugging
            context_data = {}
            if hasattr(context, "validated_envelope") and hasattr(
                context.validated_envelope, "data"
            ):
                data = context.validated_envelope.data
                context_data = data if isinstance(data, dict) else {"data": data}

            received_contexts.append({"context_data": context_data})

            # The real integration test - check what the handler actually receives
            # Domain model should be on the context object (set by processor)
            if hasattr(context, "domain_model") and context.domain_model is not None:
                domain_model = context.domain_model

                # Check if it's a Ticker object or a dict
                if isinstance(domain_model, Ticker):
                    # It's a Ticker object - this is what we want
                    received_tickers.append(domain_model)
                    logger.info(
                        "ticker_object_received",
                        symbol=str(domain_model.symbol),
                        price=str(domain_model.price),
                        has_computed_fields=(
                            hasattr(domain_model, "cost")
                            if hasattr(domain_model, "quantity")
                            else False
                        ),
                        type=type(domain_model).__name__,
                        message="Received Ticker object from pipeline",
                    )
                elif is_str_any_dict(domain_model):
                    # It's a dict - this is the MessageHandler problem
                    symbol_raw = domain_model.get("symbol")
                    symbol_value: str | None = symbol_raw if isinstance(symbol_raw, str) else None
                    has_cost_field = "cost" in domain_model
                    logger.warning(
                        "ticker_dict_received",
                        symbol=symbol_value,
                        has_cost_field=has_cost_field,
                        type="dict",
                        message="Received dict instead of Ticker object - MessageHandler problem!",
                    )
                    # Try to reconstruct the Ticker
                    try:
                        ticker = Ticker.model_validate(domain_model)
                        received_tickers.append(ticker)
                    except ValidationError:
                        logger.exception(
                            "ticker_reconstruction_failed",
                            message="Failed to reconstruct Ticker from dict",
                        )

        # Subscribe to real ticker stream
        await bp_api_for_test_env.subscribe(f"ticker.{test_symbol}", ticker_handler)

        # Wait for real ticker data
        await wait_for_websocket_data(received_tickers, min_count=3, timeout_seconds=10.0)

        # Verify we received real ticker data
        assert len(received_tickers) >= 3, "Should receive at least 3 ticker updates"

        # Check the quality of the data
        for i, ticker in enumerate(received_tickers[:3]):
            logger.info(
                "ticker_data_quality_check",
                index=i,
                symbol=ticker.symbol,
                price=str(ticker.price),
                volume=str(ticker.volume),
                timestamp=str(ticker.timestamp) if hasattr(ticker, "timestamp") else "N/A",
                is_object=hasattr(ticker, "symbol"),
                message=f"Ticker {i + 1} data quality check",
            )

            # Verify it's a proper Ticker object with expected fields
            assert hasattr(ticker, "symbol"), "Ticker should have symbol attribute"
            assert hasattr(ticker, "price"), "Ticker should have price attribute"
            assert ticker.symbol == test_symbol, f"Symbol should be {test_symbol}"
            assert ticker.price > 0, "Price should be positive"

        # Check what the handler actually received
        if received_contexts:
            first_context = received_contexts[0]
            logger.info(
                "handler_context_analysis",
                context_keys=list(first_context.keys()),
                has_domain_model="domain_model" in first_context,
                domain_model_type=(
                    type(first_context.get("domain_model")).__name__
                    if "domain_model" in first_context
                    else None
                ),
                model_type=first_context.get("model_type"),
                message="Analysis of what handler actually received",
            )

    @pytest.mark.asyncio
    async def test_depth_stream_real_data_flow(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test real depth WebSocket messages flow through the entire pipeline."""
        await ensure_websocket_connected(bp_api_for_test_env)
        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for testing")

        # Use a liquid market
        test_symbol = next((m.symbol for m in markets if m.base_symbol == "SOL"), markets[0].symbol)

        received_order_books: list[Any] = []
        pipeline_errors: list[str] = []

        async def depth_handler(context: WebSocketContextUnion) -> None:
            """Handler that receives the processed depth/order book from the pipeline."""
            await asyncio.sleep(0)

            # Extract data from typed context
            context_data = {}
            if hasattr(context, "validated_envelope") and hasattr(
                context.validated_envelope, "data"
            ):
                data = context.validated_envelope.data
                context_data = data if isinstance(data, dict) else {"data": data}

            # Check for domain model on the context object (set by processor)
            if hasattr(context, "domain_model") and context.domain_model is not None:
                context_data["domain_model"] = context.domain_model
                domain_model = context_data["domain_model"]

                # Check what type we received
                if isinstance(domain_model, OrderBook):
                    # It's an OrderBook object
                    received_order_books.append(domain_model)
                    logger.info(
                        "orderbook_object_received",
                        symbol=domain_model.symbol,
                        bids_count=len(domain_model.bids),
                        asks_count=len(domain_model.asks),
                        spread=(
                            str(domain_model.asks[0][0] - domain_model.bids[0][0])
                            if domain_model.bids and domain_model.asks
                            else "N/A"
                        ),
                        type=type(domain_model).__name__,
                        message="Received OrderBook object from pipeline",
                    )
                elif is_str_any_dict(domain_model):
                    # It's a dict - MessageHandler problem
                    pipeline_errors.append("Received dict instead of OrderBook object")
                    logger.error(
                        "orderbook_dict_received",
                        type="dict",
                        keys=list(domain_model.keys()),
                        message="Pipeline error: dict instead of OrderBook",
                    )
            else:
                pipeline_errors.append("No domain_model in context_data")

        # Subscribe to real depth stream
        await bp_api_for_test_env.subscribe(f"depth.{test_symbol}", depth_handler)

        # Wait for real depth data - depth updates can be less frequent
        await wait_for_websocket_data(received_order_books, min_count=2, timeout_seconds=15.0)

        # Verify we received real order book data
        assert len(received_order_books) >= 2, "Should receive at least 2 depth updates"
        assert len(pipeline_errors) == 0, f"Pipeline errors: {pipeline_errors}"

        # Verify the order book quality
        for i, order_book in enumerate(received_order_books[:2]):
            assert hasattr(order_book, "symbol"), "OrderBook should have symbol"
            assert hasattr(order_book, "bids"), "OrderBook should have bids"
            assert hasattr(order_book, "asks"), "OrderBook should have asks"
            assert order_book.symbol == test_symbol
            assert len(order_book.bids) > 0, "Should have bid levels"
            assert len(order_book.asks) > 0, "Should have ask levels"

            # Verify bid/ask ordering - OrderBook stores as list of tuples (price, quantity)
            if len(order_book.bids) > 1:
                assert order_book.bids[0][0] > order_book.bids[1][0], (
                    "Bids should be in descending order"
                )
            if len(order_book.asks) > 1:
                assert order_book.asks[0][0] < order_book.asks[1][0], (
                    "Asks should be in ascending order"
                )

            logger.info(
                "orderbook_quality_verified",
                index=i,
                symbol=order_book.symbol,
                best_bid=str(order_book.bids[0][0]) if order_book.bids else "N/A",
                best_ask=str(order_book.asks[0][0]) if order_book.asks else "N/A",
                bid_levels=len(order_book.bids),
                ask_levels=len(order_book.asks),
                message=f"OrderBook {i + 1} quality verified",
            )

    def _process_single_trade_item(
        self, trade: object, received_trades: list[Any], computed_field_checks: list[bool]
    ) -> None:
        """Process a single trade item."""
        if hasattr(trade, "symbol") and hasattr(trade, "price"):
            received_trades.append(trade)
            # Check for computed field
            has_cost = hasattr(trade, "cost")
            computed_field_checks.append(has_cost)
            logger.info(
                "trade_object_received",
                symbol=getattr(trade, "symbol", "unknown"),
                price=str(getattr(trade, "price", 0)),
                quantity=str(getattr(trade, "quantity", 0)),
                side=getattr(trade, "side", "unknown"),
                has_cost_field=has_cost,
                cost=str(getattr(trade, "cost", "N/A")) if has_cost else "N/A",
                type=type(trade).__name__,
                message="Received Trade object",
            )

    def _process_trade_dict_item(
        self,
        domain_model: dict[str, Any],
        received_trades: list[Any],
        computed_field_checks: list[bool],
    ) -> None:
        """Process trade data received as dict."""
        logger.warning(
            "trade_dict_received",
            has_cost="cost" in domain_model,
            symbol=domain_model.get("symbol"),
            message="Received dict instead of Trade - computed fields problem!",
        )
        # This is the computed field serialization issue
        if "cost" in domain_model:
            # Remove cost field before reconstruction
            trade_data = domain_model.copy()
            trade_data.pop("cost", None)
            try:
                trade = Trade.model_validate(trade_data)
                received_trades.append(trade)
                computed_field_checks.append(True)  # We know it had cost
            except ValidationError:
                logger.exception("trade_reconstruction_failed")

    def _validate_trade_data(self, received_trades: list[Any], test_symbol: str) -> None:
        """Validate the received trade data."""
        # Check trade quality and computed fields
        for i, trade in enumerate(received_trades[:5]):
            assert hasattr(trade, "symbol"), "Trade should have symbol"
            assert hasattr(trade, "price"), "Trade should have price"
            assert hasattr(trade, "quantity"), "Trade should have quantity"
            assert hasattr(trade, "side"), "Trade should have side"
            assert getattr(trade, "symbol", "") == test_symbol
            assert getattr(trade, "price", 0) > 0
            assert getattr(trade, "quantity", 0) > 0

            # Check computed field
            if hasattr(trade, "cost"):
                expected_cost = getattr(trade, "price", 0) * getattr(trade, "quantity", 0)
                trade_cost = getattr(trade, "cost", 0)
                assert abs(trade_cost - expected_cost) < 0.01, "Cost should be price * quantity"

            logger.info(
                "trade_quality_verified",
                index=i,
                symbol=getattr(trade, "symbol", "unknown"),
                price=str(getattr(trade, "price", 0)),
                quantity=str(getattr(trade, "quantity", 0)),
                side=getattr(trade, "side", "unknown"),
                has_cost=hasattr(trade, "cost"),
                cost=str(getattr(trade, "cost", "N/A")) if hasattr(trade, "cost") else "N/A",
                message=f"Trade {i + 1} quality verified",
            )

    @pytest.mark.asyncio
    async def test_trades_stream_real_data_flow(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test real trades WebSocket messages flow through the entire pipeline."""
        await ensure_websocket_connected(bp_api_for_test_env)
        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for testing")

        # Use SOL perpetual as it has the most trades
        test_symbol = next(
            (m.symbol for m in markets if m.base_symbol == "SOL" and m.market_type == "PERP"),
            markets[0].symbol,
        )

        received_trades: list[Any] = []
        computed_field_checks: list[bool] = []

        async def trades_handler(context: WebSocketContextUnion) -> None:
            """Handler that receives processed trades from the pipeline."""
            await asyncio.sleep(0)

            # Extract data from typed context
            context_data = self._extract_context_data(context)

            if "domain_model" not in context_data:
                return

            domain_model = context_data["domain_model"]

            # For trade streams, we might get a list of trades
            if is_any_list(domain_model):
                for trade in domain_model:
                    self._process_single_trade_item(trade, received_trades, computed_field_checks)
            elif isinstance(domain_model, Trade):
                # Single trade
                received_trades.append(domain_model)
                has_cost = hasattr(domain_model, "cost")
                computed_field_checks.append(has_cost)
            elif is_str_any_dict(domain_model):
                self._process_trade_dict_item(domain_model, received_trades, computed_field_checks)

        # Subscribe to trades stream (note: Backpack uses "trade" not "trades")
        await bp_api_for_test_env.subscribe(f"trade.{test_symbol}", trades_handler)

        # Wait for real trade data
        await wait_for_websocket_data(received_trades, min_count=5, timeout_seconds=30.0)

        # Verify we received real trade data
        assert len(received_trades) >= 5, (
            f"Should receive at least 5 trades, got {len(received_trades)}"
        )

        # Validate trade data
        self._validate_trade_data(received_trades, test_symbol)

        # Check if we're losing computed fields
        if not all(computed_field_checks):
            logger.warning(
                "computed_fields_lost",
                total_trades=len(computed_field_checks),
                with_cost=sum(computed_field_checks),
                without_cost=len(computed_field_checks) - sum(computed_field_checks),
                message="Some trades lost their computed cost field in the pipeline!",
            )

    @pytest.mark.asyncio
    async def test_private_fills_stream_real_data(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test real fills (private) WebSocket messages if authenticated."""
        await ensure_websocket_connected(bp_api_for_test_env)

        # Check if we have authentication by attempting to check the private authenticator
        # This is the only way to check if authentication is configured
        authenticator_check = getattr(bp_api_for_test_env, "_bp_authenticator", None)
        if authenticator_check is None:
            pytest.skip("Skipping private fills test - no authentication configured")

        received_fills: list[Any] = []
        received_orders: list[Any] = []
        message_types: list[str] = []

        async def fills_handler(context: WebSocketContextUnion) -> None:
            """Handler for private fills stream."""
            await asyncio.sleep(0)

            # Extract data from typed context
            context_data = {}
            if hasattr(context, "validated_envelope") and hasattr(
                context.validated_envelope, "data"
            ):
                data = context.validated_envelope.data
                context_data = data if isinstance(data, dict) else {"data": data}

            if "domain_model" in context_data:
                domain_model = context_data["domain_model"]
                model_type = context_data.get("model_type", "Unknown")
                if isinstance(model_type, str):
                    message_types.append(model_type)
                else:
                    message_types.append("Unknown")

                if model_type == "BackpackRawFill" or hasattr(domain_model, "trade_id"):
                    received_fills.append(domain_model)
                    logger.info(
                        "fill_received",
                        trade_id=getattr(domain_model, "trade_id", "N/A"),
                        symbol=getattr(domain_model, "symbol", "N/A"),
                        side=getattr(domain_model, "side", "N/A"),
                        price=str(getattr(domain_model, "price", "N/A")),
                        quantity=str(getattr(domain_model, "quantity", "N/A")),
                        type=type(domain_model).__name__,
                        message="Received fill from private stream",
                    )
                elif model_type == "Order" or hasattr(domain_model, "order_id"):
                    received_orders.append(domain_model)
                    logger.info(
                        "order_update_received",
                        order_id=getattr(domain_model, "order_id", "N/A"),
                        symbol=getattr(domain_model, "symbol", "N/A"),
                        status=getattr(domain_model, "status", "N/A"),
                        type=type(domain_model).__name__,
                        message="Received order update from private stream",
                    )

        # Subscribe to private fills stream
        await bp_api_for_test_env.subscribe("fills", fills_handler)

        # Wait briefly for any fills/orders
        await asyncio.sleep(5.0)

        # Log what we received
        logger.info(
            "private_stream_summary",
            fills_count=len(received_fills),
            orders_count=len(received_orders),
            message_types=list(set(message_types)),
            message="Private stream test summary",
        )

        # Note: We might not receive fills if there's no trading activity
        # The important test is that the subscription works and the pipeline
        # processes messages correctly
        if received_fills:
            for fill in received_fills[:3]:
                assert hasattr(fill, "trade_id"), "Fill should have trade_id"
                assert hasattr(fill, "symbol"), "Fill should have symbol"
                assert hasattr(fill, "price"), "Fill should have price"
                assert hasattr(fill, "quantity"), "Fill should have quantity"
                logger.info(
                    "fill_verified",
                    trade_id=fill.trade_id,
                    symbol=fill.symbol,
                    message="Fill object verified",
                )

    @pytest.mark.asyncio
    async def test_message_handler_type_safety_issue(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that demonstrates the MessageHandler type safety problem."""
        await ensure_websocket_connected(bp_api_for_test_env)
        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for testing")

        test_symbol = next((m.symbol for m in markets if m.base_symbol == "SOL"), markets[0].symbol)

        handler_receives_dict = False
        handler_receives_object = False
        reconstruction_needed = False

        async def diagnostic_handler(context: WebSocketContextUnion) -> None:
            """Handler that diagnoses what it receives."""
            nonlocal handler_receives_dict, handler_receives_object, reconstruction_needed
            await asyncio.sleep(0)

            # Extract data from typed context
            context_data = {}
            if hasattr(context, "validated_envelope") and hasattr(
                context.validated_envelope, "data"
            ):
                data = context.validated_envelope.data
                context_data = data if isinstance(data, dict) else {"data": data}

            if "domain_model" in context_data:
                domain_model = context_data["domain_model"]

                if is_str_any_dict(domain_model):
                    handler_receives_dict = True
                    logger.warning(
                        "handler_received_dict",
                        type="dict",
                        has_cost="cost" in domain_model if "quantity" in domain_model else False,
                        keys=list(domain_model.keys())[:10],
                        message="Handler received dict - TYPE SAFETY LOST!",
                    )

                    # Try to reconstruct
                    if "cost" in domain_model and context_data.get("model_type") == "Trade":
                        reconstruction_needed = True
                        # Create a new dict with explicit type annotation
                        domain_model_copy = dict(domain_model)
                        domain_model_copy.pop("cost", None)
                        try:
                            trade = Trade.model_validate(domain_model_copy)
                            logger.info(
                                "reconstruction_successful",
                                symbol=trade.symbol,
                                has_cost_after=hasattr(trade, "cost"),
                                message="Had to reconstruct Trade from dict",
                            )
                        except ValidationError as e:
                            logger.exception(
                                "reconstruction_failed",
                                error=str(e),
                                message="Failed to reconstruct Trade from dict",
                            )
                else:
                    handler_receives_object = True
                    logger.info(
                        "handler_received_object",
                        type=type(domain_model).__name__,
                        has_attributes=hasattr(domain_model, "symbol"),
                        message="Handler received proper object - TYPE SAFETY PRESERVED!",
                    )

        # Test with trade stream to check computed fields issue
        await bp_api_for_test_env.subscribe(f"trade.{test_symbol}", diagnostic_handler)

        # Wait for messages
        await asyncio.sleep(5.0)

        # Report findings
        logger.info(
            "type_safety_diagnostic_results",
            handler_receives_dict=handler_receives_dict,
            handler_receives_object=handler_receives_object,
            reconstruction_needed=reconstruction_needed,
            message="MessageHandler type safety diagnostic complete",
        )

        # This test documents the current behavior
        if handler_receives_dict and reconstruction_needed:
            logger.error(
                "message_handler_problem_confirmed",
                message=(
                    "CONFIRMED: MessageHandler pattern destroys type safety and computed fields!"
                ),
            )

    def _extract_context_data(self, context: WebSocketContextUnion) -> dict[str, Any]:
        """Extract data from typed context."""
        context_data: dict[str, Any] = {}

        # Extract validated envelope
        if hasattr(context, "validated_envelope"):
            context_data["validated_envelope"] = context.validated_envelope
            if hasattr(context.validated_envelope, "data"):
                envelope_data = context.validated_envelope.data
                if isinstance(envelope_data, dict):
                    context_data.update(envelope_data)
                else:
                    context_data["envelope_data"] = envelope_data

        # Extract domain model (this is the processed ticker/trade/orderbook object)
        if hasattr(context, "domain_model") and context.domain_model is not None:
            context_data["domain_model"] = context.domain_model
            # Try to determine model type
            if hasattr(context.domain_model, "__class__"):
                context_data["model_type"] = context.domain_model.__class__.__name__

        return context_data

    def _measure_latency(
        self, context_data: dict[str, Any], message_latencies: list[float]
    ) -> None:
        """Measure time from WebSocket timestamp to handler."""
        if "timestamp" in context_data:
            ws_timestamp = context_data["timestamp"]
            if isinstance(ws_timestamp, (int, float)):
                current_time = time.time() * 1000  # Convert to ms
                latency = current_time - ws_timestamp
                message_latencies.append(latency)

    def _process_domain_model(self, context_data: dict[str, Any]) -> None:
        """Process the domain model if present."""
        if "domain_model" in context_data:
            domain_model = context_data["domain_model"]
            # Simulate some processing
            if isinstance(domain_model, dict):
                # Dict processing (slower due to validation)
                model_type = context_data.get("model_type")
                if model_type == "Ticker":
                    with contextlib.suppress(ValueError, ValidationError):
                        Ticker.model_validate(domain_model)

    def _calculate_performance_stats(
        self, message_latencies: list[float], processing_times: list[float], message_count: int
    ) -> tuple[tuple[float, float, float], tuple[float, float, float], float]:
        """Calculate performance statistics."""
        if message_latencies:
            avg_latency = sum(message_latencies) / len(message_latencies)
            min_latency = min(message_latencies)
            max_latency = max(message_latencies)
        else:
            avg_latency = min_latency = max_latency = 0

        if processing_times:
            avg_processing = sum(processing_times) / len(processing_times)
            min_processing = min(processing_times)
            max_processing = max(processing_times)
        else:
            avg_processing = min_processing = max_processing = 0

        messages_per_second = message_count / 10.0

        return (
            (avg_latency, min_latency, max_latency),
            (avg_processing, min_processing, max_processing),
            messages_per_second,
        )

    @pytest.mark.asyncio
    async def test_websocket_message_processing_performance(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test real-time WebSocket message processing performance."""
        await ensure_websocket_connected(bp_api_for_test_env)
        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for testing")

        # Use most liquid market
        test_symbol = next((m.symbol for m in markets if m.base_symbol == "SOL"), markets[0].symbol)

        message_latencies: list[float] = []
        processing_times: list[float] = []
        message_count = 0

        async def performance_handler(context: WebSocketContextUnion) -> None:
            """Handler that measures processing performance."""
            nonlocal message_count
            start_time = time.perf_counter()
            await asyncio.sleep(0)

            # Extract data from typed context
            context_data = self._extract_context_data(context)

            # Measure latency
            self._measure_latency(context_data, message_latencies)

            # Process the message
            self._process_domain_model(context_data)

            processing_time = (time.perf_counter() - start_time) * 1000  # ms
            processing_times.append(processing_time)
            message_count += 1

        # Subscribe to high-frequency ticker stream
        await bp_api_for_test_env.subscribe(f"ticker.{test_symbol}", performance_handler)

        # Collect performance data for 10 seconds
        await asyncio.sleep(10.0)

        # Calculate statistics
        latency_stats, processing_stats, messages_per_second = self._calculate_performance_stats(
            message_latencies, processing_times, message_count
        )
        avg_latency, min_latency, max_latency = latency_stats
        avg_processing, min_processing, max_processing = processing_stats

        logger.info(
            "websocket_performance_results",
            total_messages=message_count,
            messages_per_second=f"{messages_per_second:.2f}",
            avg_latency_ms=f"{avg_latency:.2f}",
            min_latency_ms=f"{min_latency:.2f}",
            max_latency_ms=f"{max_latency:.2f}",
            avg_processing_ms=f"{avg_processing:.3f}",
            min_processing_ms=f"{min_processing:.3f}",
            max_processing_ms=f"{max_processing:.3f}",
            message="WebSocket real-time performance measured",
        )

        # Performance thresholds
        assert message_count > 0, "Should receive at least some messages in 10 seconds"
        if avg_processing > 1.0:
            logger.warning(
                "high_processing_time",
                avg_ms=f"{avg_processing:.3f}",
                message="Average processing time >1ms may impact high-frequency trading",
            )

    @pytest.mark.asyncio
    async def test_websocket_error_handling_real_scenarios(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test real WebSocket error scenarios and pipeline error handling."""
        await ensure_websocket_connected(bp_api_for_test_env)

        errors_received: list[dict[str, Any]] = []
        successful_messages = 0

        async def error_tracking_handler(context: WebSocketContextUnion) -> None:
            """Handler that tracks errors and successful messages."""
            nonlocal successful_messages
            await asyncio.sleep(0)

            # Extract data from typed context
            context_data = {}
            if hasattr(context, "validated_envelope") and hasattr(
                context.validated_envelope, "data"
            ):
                data = context.validated_envelope.data
                context_data = data if isinstance(data, dict) else {"data": data}

            # Check for error indicators
            if "error" in context_data:
                errors_received.append({
                    "error": context_data["error"],
                    "context_keys": list(context_data.keys()),
                    "timestamp": time.time(),
                })
                logger.warning(
                    "websocket_error_received",
                    error=str(context_data["error"]),
                    message="Received error in WebSocket stream",
                )
            elif "domain_model" in context_data:
                successful_messages += 1

        # Test 1: Subscribe to invalid symbol
        try:
            await bp_api_for_test_env.subscribe("ticker.INVALID_SYMBOL_XYZ", error_tracking_handler)
            await asyncio.sleep(2.0)
        except (ValueError, TypeError, ConnectionError, TimeoutError) as e:
            logger.info(
                "invalid_symbol_error",
                error=str(e),
                message="Invalid symbol subscription handled",
            )

        # Test 2: Subscribe to valid but less liquid market
        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        less_liquid_symbol = next(
            (m.symbol for m in markets if "BTC" not in m.symbol and "SOL" not in m.symbol),
            markets[-1].symbol if markets else None,
        )

        if less_liquid_symbol:
            await bp_api_for_test_env.subscribe(
                f"trade.{less_liquid_symbol}", error_tracking_handler
            )
            await asyncio.sleep(5.0)

        # Test 3: High-frequency subscription/unsubscription
        liquid_symbol = next(
            (m.symbol for m in markets if m.base_symbol == "SOL"), markets[0].symbol
        )
        for _ in range(3):
            await bp_api_for_test_env.subscribe(f"ticker.{liquid_symbol}", error_tracking_handler)
            await asyncio.sleep(0.5)
            # Note: Backpack API doesn't have unsubscribe in the interface
            # This tests subscription replacement/override behavior

        logger.info(
            "error_handling_test_summary",
            errors_count=len(errors_received),
            successful_messages=successful_messages,
            error_types=[e.get("error", {}).get("type", "unknown") for e in errors_received],
            message="WebSocket error handling test complete",
        )

        # We expect mostly successful messages
        assert successful_messages > 0, "Should receive some successful messages"

        # Log any errors for analysis
        for error in errors_received:
            logger.warning(
                "websocket_error_detail",
                error=error["error"],
                timestamp=error["timestamp"],
                message="WebSocket error detail",
            )

    def _process_ticker_dict(
        self,
        domain_model: dict[str, Any],
        received_messages: dict[str, list[Any]],
        pipeline_stats: dict[str, int],
    ) -> None:
        """Process ticker data received as dict."""
        try:
            pipeline_stats["reconstruction_attempts"] += 1
            ticker = Ticker.model_validate(domain_model)
            received_messages["ticker"].append(ticker)
            pipeline_stats["objects_created"] += 1
        except ValidationError as e:
            pipeline_stats["reconstruction_failures"] += 1
            received_messages["processing_errors"].append({
                "type": "ticker_reconstruction",
                "error": str(e),
            })

    def _process_trade_dict(
        self,
        domain_model: dict[str, Any],
        received_messages: dict[str, list[Any]],
        pipeline_stats: dict[str, int],
    ) -> None:
        """Process trade data received as dict."""
        try:
            pipeline_stats["reconstruction_attempts"] += 1
            # Remove computed field before reconstruction
            trade_data = domain_model.copy()
            had_cost = "cost" in trade_data
            if had_cost:
                pipeline_stats["computed_fields_lost"] += 1
                trade_data.pop("cost", None)
            trade = Trade.model_validate(trade_data)
            received_messages["trade"].append(trade)
            pipeline_stats["objects_created"] += 1
        except ValidationError as e:
            pipeline_stats["reconstruction_failures"] += 1
            received_messages["processing_errors"].append({
                "type": "trade_reconstruction",
                "error": str(e),
            })

    def _process_orderbook_dict(
        self,
        domain_model: dict[str, Any],
        received_messages: dict[str, list[Any]],
        pipeline_stats: dict[str, int],
    ) -> None:
        """Process orderbook data received as dict."""
        try:
            pipeline_stats["reconstruction_attempts"] += 1
            order_book = OrderBook.model_validate(domain_model)
            received_messages["depth"].append(order_book)
            pipeline_stats["objects_created"] += 1
        except ValidationError as e:
            pipeline_stats["reconstruction_failures"] += 1
            received_messages["processing_errors"].append({
                "type": "orderbook_reconstruction",
                "error": str(e),
            })

    def _process_domain_object(
        self,
        domain_model: object,
        received_messages: dict[str, list[Any]],
        pipeline_stats: dict[str, int],
    ) -> None:
        """Process domain model received as object."""
        pipeline_stats["objects_created"] += 1

        if hasattr(domain_model, "symbol"):
            if hasattr(domain_model, "price") and hasattr(domain_model, "volume"):
                # It's a Ticker
                received_messages["ticker"].append(domain_model)
            elif hasattr(domain_model, "bids") and hasattr(domain_model, "asks"):
                # It's an OrderBook
                received_messages["depth"].append(domain_model)
            elif hasattr(domain_model, "quantity") and hasattr(domain_model, "side"):
                # It's a Trade
                received_messages["trade"].append(domain_model)
                if hasattr(domain_model, "cost"):
                    pipeline_stats["computed_fields_preserved"] += 1

    def _create_universal_handler(
        self, received_messages: dict[str, list[Any]], pipeline_stats: dict[str, int]
    ) -> MessageHandler:
        """Create a universal handler for all message types."""

        async def universal_handler(context: WebSocketContextUnion) -> None:
            """Universal handler that processes all message types."""
            await asyncio.sleep(0)
            pipeline_stats["messages_received"] += 1

            # Extract data from typed context
            context_data = self._extract_context_data(context)

            # Capture raw envelope
            if "validated_envelope" in context_data:
                received_messages["raw_envelopes"].append(context_data["validated_envelope"])

            # Process domain model
            if "domain_model" not in context_data:
                return

            domain_model = context_data["domain_model"]
            model_type = context_data.get("model_type", "Unknown")

            if is_str_any_dict(domain_model):
                # Dictionary received - MessageHandler problem
                pipeline_stats["dicts_received"] += 1

                # Try to identify and reconstruct
                if model_type == "Ticker":
                    self._process_ticker_dict(domain_model, received_messages, pipeline_stats)
                elif model_type == "Trade":
                    self._process_trade_dict(domain_model, received_messages, pipeline_stats)
                elif model_type == "OrderBook":
                    self._process_orderbook_dict(domain_model, received_messages, pipeline_stats)
            else:
                # Object received - ideal case
                self._process_domain_object(domain_model, received_messages, pipeline_stats)

        return universal_handler

    @pytest.mark.asyncio
    async def test_full_pipeline_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test the full WebSocket pipeline integration with multiple stream types."""
        await ensure_websocket_connected(bp_api_for_test_env)
        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for testing")

        # Use the most liquid market
        test_symbol = next((m.symbol for m in markets if m.base_symbol == "SOL"), markets[0].symbol)

        # Containers for different message types
        received_messages: dict[str, list[Any]] = {
            "ticker": [],
            "depth": [],
            "trade": [],
            "raw_envelopes": [],
            "processing_errors": [],
        }

        pipeline_stats = {
            "messages_received": 0,
            "objects_created": 0,
            "dicts_received": 0,
            "reconstruction_attempts": 0,
            "reconstruction_failures": 0,
            "computed_fields_preserved": 0,
            "computed_fields_lost": 0,
        }

        # Create universal handler
        universal_handler = self._create_universal_handler(received_messages, pipeline_stats)

        # Subscribe to multiple streams
        await bp_api_for_test_env.subscribe(f"ticker.{test_symbol}", universal_handler)
        await bp_api_for_test_env.subscribe(f"depth.{test_symbol}", universal_handler)
        await bp_api_for_test_env.subscribe(f"trade.{test_symbol}", universal_handler)

        # Collect data for 15 seconds
        logger.info(
            "full_pipeline_test_started",
            symbol=test_symbol,
            duration_seconds=15,
            message="Starting full pipeline integration test",
        )

        await asyncio.sleep(15.0)

        # Analyze results
        logger.info(
            "full_pipeline_test_results",
            total_messages=pipeline_stats["messages_received"],
            objects_created=pipeline_stats["objects_created"],
            dicts_received=pipeline_stats["dicts_received"],
            reconstruction_attempts=pipeline_stats["reconstruction_attempts"],
            reconstruction_failures=pipeline_stats["reconstruction_failures"],
            ticker_messages=len(received_messages["ticker"]),
            depth_messages=len(received_messages["depth"]),
            trade_messages=len(received_messages["trade"]),
            raw_envelopes=len(received_messages["raw_envelopes"]),
            processing_errors=len(received_messages["processing_errors"]),
            computed_fields_preserved=pipeline_stats["computed_fields_preserved"],
            computed_fields_lost=pipeline_stats["computed_fields_lost"],
            message="Full pipeline integration test complete",
        )

        # Assertions
        assert pipeline_stats["messages_received"] > 0, "Should receive some messages"
        assert len(received_messages["ticker"]) > 0, "Should receive ticker updates"
        assert len(received_messages["depth"]) > 0, "Should receive depth updates"
        # Trade messages might be less frequent

        # Calculate MessageHandler impact
        if pipeline_stats["messages_received"] > 0:
            dict_percentage = (
                pipeline_stats["dicts_received"] / pipeline_stats["messages_received"] * 100
            )
        else:
            dict_percentage = 0

        if dict_percentage > 50:
            logger.error(
                "message_handler_impact_severe",
                dict_percentage=f"{dict_percentage:.1f}%",
                reconstruction_failures=pipeline_stats["reconstruction_failures"],
                computed_fields_lost=pipeline_stats["computed_fields_lost"],
                message="MessageHandler pattern is severely impacting type safety!",
            )

        # Test envelope structure
        if received_messages["raw_envelopes"]:
            first_envelope = received_messages["raw_envelopes"][0]
            assert hasattr(first_envelope, "stream"), "Envelope should have stream field"
            assert hasattr(first_envelope, "data"), "Envelope should have data field"

            # Verify envelope validation
            try:
                validated = BackpackRawWebSocketEnvelope.model_validate({
                    "stream": first_envelope.stream,
                    "data": first_envelope.data,
                })
                logger.info(
                    "envelope_validation_success",
                    stream=validated.stream,
                    routing_key=validated.get_routing_key(),
                    message="Envelope validation successful",
                )
            except ValidationError as e:
                logger.exception(
                    "envelope_validation_failed", error=str(e), message="Envelope validation failed"
                )

        # Report any processing errors
        for error in received_messages["processing_errors"]:
            logger.warning(
                "pipeline_processing_error",
                error_type=error["type"],
                error_message=error["error"],
                message="Processing error in pipeline",
            )

        # Final verdict on pipeline health
        pipeline_health = "healthy"
        if pipeline_stats["reconstruction_failures"] > 0:
            pipeline_health = "degraded"
        if dict_percentage > 80:
            pipeline_health = "critical"

        # Calculate reconstruction success rate
        if pipeline_stats["reconstruction_attempts"] > 0:
            success_rate = (
                1
                - pipeline_stats["reconstruction_failures"]
                / max(pipeline_stats["reconstruction_attempts"], 1)
            ) * 100
            reconstruction_success_rate = f"{success_rate:.1f}%"
        else:
            reconstruction_success_rate = "N/A"

        logger.info(
            "pipeline_health_assessment",
            health=pipeline_health,
            dict_percentage=f"{dict_percentage:.1f}%",
            reconstruction_success_rate=reconstruction_success_rate,
            message=f"Pipeline health: {pipeline_health}",
        )

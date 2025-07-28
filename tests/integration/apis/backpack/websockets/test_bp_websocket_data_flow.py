"""Test WebSocket Data Flow from Raw Streams to Domain Models.

This test validates the complete data transformation pipeline:
1. WebSocket receives raw messages from Backpack
2. Messages are validated with Pydantic envelope models
3. Raw data is extracted and validated with stream-specific models
4. Raw models are transformed to internal domain models
5. Domain models are passed to handlers

This ensures the entire pipeline works correctly with real WebSocket data.
"""

import asyncio
from collections.abc import Callable, Coroutine
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.service_args.market_data import GetMarketsArgs
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market.order_book import OrderBook
from cyberdelta.core.models.market.ticker import Ticker
from cyberdelta.core.models.market.trade import Trade
from cyberdelta.enums import OrderSide


pytestmark = [pytest.mark.integration, pytest.mark.timing]

logger = get_logger(__name__)


class TestBackpackWebSocketDataFlow:
    """Test the complete WebSocket data flow from raw streams to domain models."""

    @pytest.mark.asyncio
    async def test_ticker_stream_complete_data_flow(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test ticker stream data flow: raw message -> envelope -> raw model -> domain model."""
        # Track what we receive at each stage
        received_stages: dict[str, list[Any]] = {
            "raw_messages": [],
            "domain_models": [],
        }

        async def ticker_handler(context: WebSocketContextProtocol) -> None:
            """Handler that receives the final domain model."""
            await asyncio.sleep(0)  # Satisfy RUF029

            # The domain model is stored directly on the context by the processor
            if hasattr(context, "domain_model") and context.domain_model is not None:
                domain_model: Any = context.domain_model

                # Add to raw messages to track that handler was called
                received_stages["raw_messages"].append({"model": domain_model})

                # Check if it's a Ticker instance
                if isinstance(domain_model, Ticker):
                    received_stages["domain_models"].append(domain_model)

                    logger.info(
                        "ticker_domain_model_received",
                        symbol=domain_model.symbol,
                        price=str(domain_model.price),
                        timestamp=(
                            domain_model.timestamp.isoformat() if domain_model.timestamp else None
                        ),
                        list_length=len(received_stages["domain_models"]),
                        message="✓ Ticker domain model successfully received in handler",
                    )
                else:
                    logger.warning(
                        "ticker_handler_wrong_model_type",
                        model_type=type(domain_model).__name__,
                        routing_key=getattr(context, "routing_key", "unknown"),
                        message=f"Expected Ticker but got {type(domain_model).__name__}",
                    )
            else:
                logger.warning(
                    "ticker_handler_no_domain_model",
                    has_domain_model=hasattr(context, "domain_model"),
                    domain_model_is_none=getattr(context, "domain_model", None) is None,
                    routing_key=getattr(context, "routing_key", "unknown"),
                    message="Handler called but no domain model found on context",
                )

        try:
            # Connect WebSocket
            await bp_api_for_test_env.connect_websocket()
            assert bp_api_for_test_env.is_connected, "WebSocket connection failed"

            # Get a test symbol
            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            assert markets, "No markets available for testing"
            test_symbol = markets[0].symbol

            # Subscribe to ticker stream
            await bp_api_for_test_env.subscribe(f"ticker.{test_symbol}", ticker_handler)

            # Wait for data with timeout
            timeout = 15.0
            start_time = asyncio.get_event_loop().time()

            while (asyncio.get_event_loop().time() - start_time) < timeout:
                if received_stages["domain_models"]:
                    break
                await asyncio.sleep(0.1)

            # Validate we received domain models
            assert received_stages["domain_models"], (
                f"No Ticker domain models received for {test_symbol} after {timeout}s. "
                "Data flow pipeline may not be working correctly."
            )

            # Validate the domain models
            for ticker in received_stages["domain_models"][:3]:  # Check first 3
                assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"
                assert ticker.symbol == test_symbol, (
                    f"Expected symbol {test_symbol}, got {ticker.symbol}"
                )
                assert isinstance(ticker.price, Decimal), "Price should be Decimal"
                assert ticker.price > 0, "Price should be positive"
                assert ticker.timestamp is not None, "Timestamp should be present"
                assert ticker.timestamp.tzinfo is not None, "Timestamp should be timezone-aware"

            logger.info(
                "ticker_data_flow_test_success",
                symbol=test_symbol,
                models_received=len(received_stages["domain_models"]),
                message=f"✓ Successfully validated ticker data flow for {test_symbol}",
            )

        except (ValueError, TypeError, ConnectionError, TimeoutError) as e:
            pytest.fail(f"Ticker data flow test failed: {e}")

    def _create_depth_handler(
        self, received_orderbooks: list[OrderBook]
    ) -> Callable[[WebSocketContextProtocol], Coroutine[Any, Any, None]]:
        """Create handler for orderbook depth stream.

        Returns:
            Callable: Async handler function for processing orderbook updates.
        """

        async def depth_handler(context: WebSocketContextProtocol) -> None:
            """Handler that receives the final domain model."""
            await asyncio.sleep(0)  # Satisfy RUF029

            # The domain model is stored directly on the context by the processor
            if hasattr(context, "domain_model") and context.domain_model is not None:
                domain_model: Any = context.domain_model

                # Check if it's an OrderBook instance
                if isinstance(domain_model, OrderBook):
                    received_orderbooks.append(domain_model)

                    logger.info(
                        "orderbook_domain_model_received",
                        symbol=domain_model.symbol,
                        bids_count=len(domain_model.bids),
                        asks_count=len(domain_model.asks),
                        message="✓ OrderBook domain model successfully received in handler",
                    )

        return depth_handler

    def _validate_orderbook_bid_ask_structure(self, orderbook: OrderBook) -> None:
        """Validate bid/ask structure for orderbook."""
        # Validate bid/ask structure
        if orderbook.bids:
            for price, size in orderbook.bids[:5]:  # Check first 5 levels
                assert isinstance(price, Decimal), "Bid price should be Decimal"
                assert isinstance(size, Decimal), "Bid size should be Decimal"
                assert price > 0, "Bid price should be positive"
                assert size > 0, "Bid size should be positive"

        if orderbook.asks:
            for price, size in orderbook.asks[:5]:  # Check first 5 levels
                assert isinstance(price, Decimal), "Ask price should be Decimal"
                assert isinstance(size, Decimal), "Ask size should be Decimal"
                assert price > 0, "Ask price should be positive"
                assert size > 0, "Ask size should be positive"

    def _validate_orderbook_ordering(self, orderbook: OrderBook) -> None:
        """Validate bid/ask ordering for orderbook."""
        # Validate bid/ask ordering
        if len(orderbook.bids) > 1:
            # Bids should be in descending order
            assert orderbook.bids[0][0] > orderbook.bids[1][0], "Bids should be in descending order"

        if len(orderbook.asks) > 1:
            # Asks should be in ascending order
            assert orderbook.asks[0][0] < orderbook.asks[1][0], "Asks should be in ascending order"

    async def _wait_for_orderbook_data(
        self, received_orderbooks: list[OrderBook], max_wait: float = 15.0
    ) -> None:
        """Wait for orderbook data with timeout."""
        start_time = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start_time) < max_wait:
            if received_orderbooks:
                break
            await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_orderbook_stream_complete_data_flow(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test orderbook (depth) stream data flow.

        Tests: raw message -> envelope -> raw model -> domain model -> handler.
        """
        received_orderbooks: list[OrderBook] = []
        depth_handler = self._create_depth_handler(received_orderbooks)

        try:
            # Connect and get test symbol
            await bp_api_for_test_env.connect_websocket()
            assert bp_api_for_test_env.is_connected, "WebSocket connection failed"

            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            assert markets, "No markets available for testing"
            test_symbol = markets[0].symbol

            # Subscribe to depth stream
            await bp_api_for_test_env.subscribe(f"depth.{test_symbol}", depth_handler)

            # Wait for data
            await self._wait_for_orderbook_data(received_orderbooks)

            # Validate results
            assert received_orderbooks, (
                f"No OrderBook domain models received for {test_symbol} after 15s. "
                "Data flow pipeline may not be working correctly."
            )

            # Validate the domain models
            for orderbook in received_orderbooks[:3]:
                assert isinstance(orderbook, OrderBook), (
                    f"Expected OrderBook, got {type(orderbook)}"
                )
                assert orderbook.symbol == test_symbol, (
                    f"Expected symbol {test_symbol}, got {orderbook.symbol}"
                )
                assert isinstance(orderbook.bids, list), "Bids should be a list"
                assert isinstance(orderbook.asks, list), "Asks should be a list"

                self._validate_orderbook_bid_ask_structure(orderbook)
                self._validate_orderbook_ordering(orderbook)

            logger.info(
                "orderbook_data_flow_test_success",
                symbol=test_symbol,
                models_received=len(received_orderbooks),
                message=f"✓ Successfully validated orderbook data flow for {test_symbol}",
            )

        except (ValueError, TypeError, ConnectionError, TimeoutError) as e:
            pytest.fail(f"OrderBook data flow test failed: {e}")

    def _create_trades_handler(
        self, received_trades: list[Trade]
    ) -> Callable[[WebSocketContextProtocol], Coroutine[Any, Any, None]]:
        """Create trades handler to reduce complexity.

        Returns:
            Callable: Async handler function for processing trade updates.
        """

        async def trades_handler(context: WebSocketContextProtocol) -> None:
            """Handler that receives the final domain model."""
            await asyncio.sleep(0)  # Satisfy RUF029

            # The domain model is stored directly on the context by the processor
            if hasattr(context, "domain_model") and context.domain_model is not None:
                # Use getattr to avoid direct Any access for better type inference
                domain_model = getattr(context, "domain_model", None)

                # Handle single Trade instance
                if isinstance(domain_model, Trade):
                    received_trades.append(domain_model)

                    logger.info(
                        "trade_domain_model_received",
                        symbol=domain_model.symbol,
                        price=str(domain_model.price),
                        quantity=str(domain_model.quantity),
                        side=domain_model.side,
                        message="✓ Trade domain model successfully received in handler",
                    )
                # Handle list of trades (batch processing)
                elif (
                    domain_model is not None
                    and hasattr(domain_model, "__iter__")
                    and not isinstance(domain_model, str)
                ):
                    # Safely iterate without direct list access
                    trade_count = 0
                    try:
                        for item in domain_model:
                            if isinstance(item, Trade):
                                received_trades.append(item)
                                trade_count += 1

                        if trade_count > 0:
                            logger.info(
                                "trade_batch_received",
                                count=trade_count,
                                message=f"✓ Received batch of {trade_count} trades",
                            )
                    except TypeError:
                        # Not iterable, ignore
                        pass

        return trades_handler

    def _validate_trade_data(self, received_trades: list[Trade], test_symbol: str) -> None:
        """Validate trade data to reduce complexity."""
        for trade in received_trades[:5]:  # Check first 5 trades
            assert isinstance(trade, Trade), f"Expected Trade, got {type(trade)}"
            assert trade.symbol == test_symbol, f"Expected symbol {test_symbol}, got {trade.symbol}"
            assert isinstance(trade.price, Decimal), "Price should be Decimal"
            assert isinstance(trade.quantity, Decimal), "Quantity should be Decimal"
            assert trade.price > 0, "Price should be positive"
            assert trade.quantity > 0, "Quantity should be positive"
            assert trade.side in [OrderSide.BUY, OrderSide.SELL], f"Invalid side: {trade.side}"
            assert trade.executed_at is not None, "Executed timestamp should be present"
            assert trade.executed_at.tzinfo is not None, (
                "Executed timestamp should be timezone-aware"
            )

    @pytest.mark.asyncio
    async def test_trades_stream_complete_data_flow(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test trades stream data flow.

        Tests: raw message -> envelope -> raw model -> domain model -> handler.
        """
        received_trades: list[Trade] = []
        trades_handler = self._create_trades_handler(received_trades)

        try:
            # Connect and get test symbol
            await bp_api_for_test_env.connect_websocket()
            assert bp_api_for_test_env.is_connected, "WebSocket connection failed"

            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            assert markets, "No markets available for testing"
            test_symbol = markets[0].symbol

            # Subscribe to trades stream
            # NOTE: Backpack uses "trade" (singular) not "trades" for the stream name
            await bp_api_for_test_env.subscribe(f"trade.{test_symbol}", trades_handler)

            # Wait for data
            timeout = 15.0
            start_time = asyncio.get_event_loop().time()

            while (asyncio.get_event_loop().time() - start_time) < timeout:
                if received_trades:
                    break
                await asyncio.sleep(0.1)

            # Validate results
            assert received_trades, (
                f"No Trade domain models received for {test_symbol} after {timeout}s. "
                "Data flow pipeline may not be working correctly."
            )

            # Validate the domain models
            self._validate_trade_data(received_trades, test_symbol)

            logger.info(
                "trades_data_flow_test_success",
                symbol=test_symbol,
                models_received=len(received_trades),
                message=f"✓ Successfully validated trades data flow for {test_symbol}",
            )

        except (ValueError, TypeError, ConnectionError, TimeoutError) as e:
            pytest.fail(f"Trades data flow test failed: {e}")

    def _create_universal_handler(
        self, stream_type: str, received_models: dict[str, list[Any]]
    ) -> Callable[[WebSocketContextProtocol], Coroutine[Any, Any, None]]:
        """Create a handler for a specific stream type.

        Returns:
            Callable: Async handler function for the specified stream type.
        """

        async def handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Fix RUF029

            # The domain model is stored directly on the context by the processor
            if hasattr(context, "domain_model") and context.domain_model is not None:
                # Use getattr to avoid direct Any access for better type inference
                domain_model = getattr(context, "domain_model", None)

                if isinstance(domain_model, Ticker) and stream_type == "ticker":
                    received_models["ticker"].append(domain_model)
                elif isinstance(domain_model, OrderBook) and stream_type == "orderbook":
                    received_models["orderbook"].append(domain_model)
                elif isinstance(domain_model, Trade) and stream_type == "trades":
                    received_models["trades"].append(domain_model)
                elif (
                    domain_model is not None
                    and hasattr(domain_model, "__iter__")
                    and not isinstance(domain_model, str)
                    and stream_type == "trades"
                ):
                    # Handle batch trades - safely iterate without direct list access
                    try:
                        for item in domain_model:
                            if isinstance(item, Trade):
                                received_models["trades"].append(item)
                    except TypeError:
                        # Not iterable, ignore
                        pass

        return handler

    async def _subscribe_to_concurrent_streams(
        self, bp_api: BackpackAPI, test_symbol: str, received_models: dict[str, list[Any]]
    ) -> None:
        """Subscribe to multiple streams concurrently."""
        await bp_api.subscribe(
            f"ticker.{test_symbol}", self._create_universal_handler("ticker", received_models)
        )
        await bp_api.subscribe(
            f"depth.{test_symbol}", self._create_universal_handler("orderbook", received_models)
        )
        await bp_api.subscribe(
            f"trade.{test_symbol}", self._create_universal_handler("trades", received_models)
        )

    async def _wait_for_concurrent_stream_data(
        self, received_models: dict[str, list[Any]], max_wait: float = 15.0
    ) -> int:
        """Wait for data from concurrent streams and return active stream count.

        Returns:
            int: Number of active streams that received data.
        """
        start_time = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start_time) < max_wait:
            # Check if we have data from at least 2 streams
            active_streams = sum(1 for models in received_models.values() if models)
            if active_streams >= 2:
                # Give a bit more time to collect data from all streams
                await asyncio.sleep(1.0)
                break
            await asyncio.sleep(0.1)
        return sum(1 for models in received_models.values() if models)

    def _log_concurrent_stream_results(self, received_models: dict[str, list[Any]]) -> None:
        """Log results from concurrent streams."""
        for stream_type, models in received_models.items():
            if models:
                logger.info(
                    "concurrent_stream_data_received",
                    stream_type=stream_type,
                    count=len(models),
                    message=f"✓ Received {len(models)} {stream_type} models",
                )
            else:
                logger.info(
                    "concurrent_stream_no_data",
                    stream_type=stream_type,
                    message=f"No data received from {stream_type} stream",
                )

    def _validate_stream_isolation(self, received_models: dict[str, list[Any]]) -> None:
        """Validate data isolation (each stream should only receive its own model type)."""
        if received_models["ticker"]:
            assert all(isinstance(m, Ticker) for m in received_models["ticker"]), (
                "Ticker stream contaminated"
            )
        if received_models["orderbook"]:
            assert all(isinstance(m, OrderBook) for m in received_models["orderbook"]), (
                "OrderBook stream contaminated"
            )
        if received_models["trades"]:
            assert all(isinstance(m, Trade) for m in received_models["trades"]), (
                "Trades stream contaminated"
            )

    @pytest.mark.asyncio
    async def test_multiple_streams_concurrent_data_flow(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test multiple streams running concurrently to ensure data flow isolation."""
        received_models: dict[str, list[Any]] = {
            "ticker": [],
            "orderbook": [],
            "trades": [],
        }

        try:
            # Connect and get test symbol
            await bp_api_for_test_env.connect_websocket()
            assert bp_api_for_test_env.is_connected, "WebSocket connection failed"

            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            assert markets, "No markets available for testing"
            test_symbol = markets[0].symbol

            # Subscribe to multiple streams concurrently
            await self._subscribe_to_concurrent_streams(
                bp_api_for_test_env, test_symbol, received_models
            )

            # Wait for data from all streams
            active_streams = await self._wait_for_concurrent_stream_data(received_models)

            # Validate results
            assert active_streams >= 2, (
                f"Expected data from at least 2 streams, got {active_streams}. "
                "Concurrent stream handling may not be working correctly."
            )

            # Log results
            self._log_concurrent_stream_results(received_models)

            # Validate data isolation
            self._validate_stream_isolation(received_models)

            logger.info(
                "concurrent_data_flow_test_success",
                active_streams=active_streams,
                ticker_count=len(received_models["ticker"]),
                orderbook_count=len(received_models["orderbook"]),
                trades_count=len(received_models["trades"]),
                message="✓ Successfully validated concurrent stream data flow",
            )

        except (ValueError, TypeError, ConnectionError, TimeoutError) as e:
            pytest.fail(f"Concurrent data flow test failed: {e}")

    @pytest.mark.asyncio
    async def test_data_flow_error_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that data flow pipeline handles errors gracefully.

        Ensures errors don't affect other messages.
        """
        received_models: list[Any] = []
        error_count = 0

        async def error_prone_handler(context: WebSocketContextProtocol) -> None:
            """Handler that sometimes raises errors to test error isolation.

            Raises:
                ValueError: Simulated error for testing error handling.
            """
            nonlocal error_count
            await asyncio.sleep(0)  # Fix RUF029

            # The domain model is stored directly on the context by the processor
            if hasattr(context, "domain_model") and context.domain_model is not None:
                domain_model: Any = context.domain_model

                # Simulate intermittent errors
                if len(received_models) % 3 == 1:  # Every 3rd message after the first
                    error_count += 1
                    raise ValueError("Simulated handler error")

                # Otherwise, process normally
                if isinstance(domain_model, Ticker):
                    received_models.append(domain_model)

        try:
            # Connect and get test symbol
            await bp_api_for_test_env.connect_websocket()
            assert bp_api_for_test_env.is_connected, "WebSocket connection failed"

            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            assert markets, "No markets available for testing"
            test_symbol = markets[0].symbol

            # Subscribe with error-prone handler
            await bp_api_for_test_env.subscribe(f"ticker.{test_symbol}", error_prone_handler)

            # Wait for data
            timeout = 15.0
            start_time = asyncio.get_event_loop().time()

            while (asyncio.get_event_loop().time() - start_time) < timeout:
                if len(received_models) >= 5:  # Wait for at least 5 successful messages
                    break
                await asyncio.sleep(0.1)

            # Validate results
            assert received_models, "No models received despite handler errors"
            assert error_count > 0, "No errors were triggered in the test"

            # All received models should be valid
            for model in received_models:
                assert isinstance(model, Ticker), f"Expected Ticker, got {type(model)}"
                assert model.symbol == test_symbol, (
                    f"Expected symbol {test_symbol}, got {model.symbol}"
                )

            logger.info(
                "error_handling_data_flow_test_success",
                models_received=len(received_models),
                errors_triggered=error_count,
                message=f"✓ Data flow continued despite {error_count} handler errors",
            )

        except (ValueError, TypeError, ConnectionError, TimeoutError, ValidationError) as e:
            pytest.fail(f"Error handling data flow test failed: {e}")

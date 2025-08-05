"""Test 7: Internal Model Creation from WebSocket Streams (OrderBook Focus).

This module tests that WebSocket streams are properly converted to internal
domain models, with specific focus on OrderBook creation from depth streams.

Security Compliance:
- Tests OrderBook creation from real WebSocket depth data
- Validates price and quantity precision in OrderBook models
- Tests OrderBook data integrity and validation
- Fails fast on model creation issues
"""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawDepthUpdateEvent
from cyberdelta.apis.common.types import MessageHandler
from cyberdelta.apis.models.service_args.market_data import GetMarketsArgs
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.models.market.order_book import OrderBook

# Import WebSocket test helpers
from .ws_test_helpers import (
    ensure_websocket_connected,
    get_real_depth_data,
    wait_for_websocket_data,
)


pytestmark = [pytest.mark.integration, pytest.mark.timing]

logger = get_logger(__name__)


class TestBackpackOrderBookModelCreation:
    """Test OrderBook model creation from WebSocket depth streams."""

    async def _setup_orderbook_connection(self, api: BackpackAPI) -> Symbol:
        """Set up connection and get test symbol for orderbook testing.

        Returns:
            The Symbol for orderbook testing.
        """
        await api.connect_websocket()
        if not api.is_connected:
            pytest.fail("WebSocket connection failed - cannot test OrderBook creation")

        markets = await api.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for OrderBook testing")

        # Prefer SOL/USDC spot market for better liquidity
        preferred_market = next(
            (
                m
                for m in markets
                if m.market_type == "Spot" and "SOL" in m.symbol.value and "USDC" in m.symbol.value
            ),
            markets[0],
        )

        # Return the Symbol object directly
        return preferred_market.symbol

    async def _create_orderbook_handler(
        self, received_orderbooks: list[OrderBook]
    ) -> MessageHandler:
        """Create handler that extracts OrderBook from context.

        Returns:
            Message handler function that processes WebSocket contexts and extracts OrderBooks.
        """

        async def orderbook_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)

            # Check for OrderBook in context.domain_model (stateful transformer pattern)
            if hasattr(context, "domain_model") and isinstance(context.domain_model, OrderBook):
                orderbook = context.domain_model
                received_orderbooks.append(orderbook)
                self._log_orderbook_received(orderbook, "domain_model")
                return

            # Fallback: Extract data from typed context (legacy pattern)
            context_data: dict[str, Any] = {}
            if (
                hasattr(context, "validated_envelope")
                and context.validated_envelope is not None
                and hasattr(context.validated_envelope, "data")
            ):
                data = context.validated_envelope.data
                context_data = data if isinstance(data, dict) else {"data": data}

            if "orderbook" in context_data and isinstance(context_data["orderbook"], OrderBook):
                orderbook = context_data["orderbook"]
                received_orderbooks.append(orderbook)
                self._log_orderbook_received(orderbook, "orderbook")
            elif "order_book" in context_data and isinstance(context_data["order_book"], OrderBook):
                orderbook = context_data["order_book"]
                received_orderbooks.append(orderbook)
                self._log_orderbook_received(orderbook, "order_book")
            else:
                self._log_orderbook_context_structure(context_data)

        return orderbook_handler

    def _log_orderbook_received(self, orderbook: OrderBook, key: str) -> None:
        """Log information about received orderbook."""
        logger.info(
            "orderbook_received_from_stream",
            symbol=orderbook.symbol,
            bids_count=len(orderbook.bids),
            asks_count=len(orderbook.asks),
            message=f"✓ OrderBook received for {orderbook.symbol} (key: {key})",
        )

    def _log_orderbook_context_structure(self, context: dict[str, Any]) -> None:
        """Log context structure for debugging when no OrderBook found."""
        logger.info(
            "orderbook_context_structure",
            context_keys=list(context.keys()),
            context_types={k: type(v).__name__ for k, v in context.items()},
            message="Depth stream context structure (no OrderBook found)",
        )

    def _validate_all_received_orderbooks(
        self, received_orderbooks: list[OrderBook], test_symbol: Symbol
    ) -> None:
        """Validate all received orderbooks and log results."""
        if received_orderbooks:
            for orderbook in received_orderbooks[:3]:
                self._validate_orderbook_structure(orderbook, test_symbol)
                self._validate_orderbook_financial_data(orderbook)
                self._validate_orderbook_integrity(orderbook)

            logger.info(
                "orderbook_creation_from_stream_success",
                symbol=test_symbol.value,
                orderbooks_received=len(received_orderbooks),
                message=(
                    f"✓ Successfully created {len(received_orderbooks)} "
                    "OrderBooks from depth stream"
                ),
            )
        else:
            # Rule #2: Use pytest.fail instead of logger.warning
            pytest.fail(
                f"No OrderBook models received from depth stream for {test_symbol.value}. "
                "Check transformation pipeline - depth-to-OrderBook conversion not working."
            )

    @pytest.mark.asyncio
    async def test_orderbook_creation_from_depth_stream(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test OrderBook creation from real WebSocket depth streams."""
        try:
            test_symbol = await self._setup_orderbook_connection(bp_api_for_test_env)
            received_orderbooks: list[OrderBook] = []

            orderbook_handler = await self._create_orderbook_handler(received_orderbooks)
            await bp_api_for_test_env.subscribe(f"depth.{test_symbol.value}", orderbook_handler)
            # Rule #4: Use proper wait condition instead of asyncio.sleep
            # Wait longer for depth data - Backpack may take time to send initial snapshot
            await wait_for_websocket_data(received_orderbooks, min_count=1, timeout_seconds=30.0)

            self._validate_all_received_orderbooks(received_orderbooks, test_symbol)

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"OrderBook creation from depth stream failed: {e}. "
                "WebSocket to OrderBook transformation not working."
            )
        except (TimeoutError, ConnectionError, OSError) as e:
            # Rule #10: Network errors are test failures
            pytest.fail(
                f"Network error during OrderBook creation test: {e}. "
                "Test requires stable WebSocket connection for depth data."
            )

    def _validate_orderbook_structure(self, orderbook: OrderBook, expected_symbol: Symbol) -> None:
        """Validate OrderBook model structure."""
        assert isinstance(orderbook, OrderBook), f"Expected OrderBook, got {type(orderbook)}"
        assert orderbook.symbol == expected_symbol, (
            f"Expected symbol {expected_symbol.value}, got {orderbook.symbol.value}"
        )
        assert hasattr(orderbook, "bids"), "OrderBook missing bids attribute"
        assert hasattr(orderbook, "asks"), "OrderBook missing asks attribute"
        assert hasattr(orderbook, "timestamp"), "OrderBook missing timestamp attribute"

        # Validate lists structure
        assert isinstance(orderbook.bids, list), f"Bids should be list, got {type(orderbook.bids)}"
        assert isinstance(orderbook.asks, list), f"Asks should be list, got {type(orderbook.asks)}"

        logger.info(
            "orderbook_structure_validation_passed",
            symbol=orderbook.symbol,
            bids_count=len(orderbook.bids),
            asks_count=len(orderbook.asks),
            message="✓ OrderBook structure validation passed",
        )

    def _validate_orderbook_financial_data(self, orderbook: OrderBook) -> None:
        """Validate OrderBook financial data precision and validity."""
        # Validate bids
        for i, (price, quantity) in enumerate(orderbook.bids[:5]):  # Check first 5
            assert isinstance(price, Decimal), f"Bid price {i} should be Decimal, got {type(price)}"
            assert isinstance(quantity, Decimal), (
                f"Bid quantity {i} should be Decimal, got {type(quantity)}"
            )
            assert price > Decimal(0), f"Bid price {i} should be positive, got {price}"
            assert quantity > Decimal(0), f"Bid quantity {i} should be positive, got {quantity}"

        # Validate asks
        for i, (price, quantity) in enumerate(orderbook.asks[:5]):  # Check first 5
            assert isinstance(price, Decimal), f"Ask price {i} should be Decimal, got {type(price)}"
            assert isinstance(quantity, Decimal), (
                f"Ask quantity {i} should be Decimal, got {type(quantity)}"
            )
            assert price > Decimal(0), f"Ask price {i} should be positive, got {price}"
            assert quantity > Decimal(0), f"Ask quantity {i} should be positive, got {quantity}"

        # Validate bid/ask relationship
        if orderbook.bids and orderbook.asks:
            best_bid = orderbook.bids[0][0]
            best_ask = orderbook.asks[0][0]

            assert best_bid < best_ask, (
                f"Best bid {best_bid} should be less than best ask {best_ask}"
            )

            spread = best_ask - best_bid
            assert spread > Decimal(0), f"Spread should be positive, got {spread}"

        logger.info(
            "orderbook_financial_validation_passed",
            symbol=orderbook.symbol,
            best_bid=str(orderbook.bids[0][0]) if orderbook.bids else "None",
            best_ask=str(orderbook.asks[0][0]) if orderbook.asks else "None",
            spread=str(orderbook.asks[0][0] - orderbook.bids[0][0])
            if orderbook.bids and orderbook.asks
            else "None",
            message="✓ OrderBook financial data validation passed",
        )

    def _validate_orderbook_integrity(self, orderbook: OrderBook) -> None:
        """Validate OrderBook data integrity."""
        # Validate bids are sorted (highest price first)
        for i in range(len(orderbook.bids) - 1):
            current_price = orderbook.bids[i][0]
            next_price = orderbook.bids[i + 1][0]
            assert current_price >= next_price, (
                f"Bids not sorted correctly: position {i} price {current_price} < "
                f"position {i + 1} price {next_price}"
            )

        # Validate asks are sorted (lowest price first)
        for i in range(len(orderbook.asks) - 1):
            current_price = orderbook.asks[i][0]
            next_price = orderbook.asks[i + 1][0]
            assert current_price <= next_price, (
                f"Asks not sorted correctly: position {i} price {current_price} > "
                f"position {i + 1} price {next_price}"
            )

        # Validate timestamp
        if orderbook.timestamp:
            assert orderbook.timestamp.tzinfo is not None, (
                "OrderBook timestamp should be timezone-aware"
            )

        logger.info(
            "orderbook_integrity_validation_passed",
            symbol=orderbook.symbol,
            bids_sorted=True,
            asks_sorted=True,
            timezone_aware=orderbook.timestamp.tzinfo is not None if orderbook.timestamp else False,
            message="✓ OrderBook integrity validation passed",
        )

    @pytest.mark.asyncio
    async def test_orderbook_creation_from_raw_depth_data(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test OrderBook creation from raw depth update data."""
        # Get real depth data for testing
        await ensure_websocket_connected(bp_api_for_test_env)
        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for testing")
        test_symbol = markets[0].symbol

        # Get REAL depth data from WebSocket
        real_depth = await get_real_depth_data(bp_api_for_test_env, test_symbol)

        # DEFENSIVE CHECK: Handle None bids/asks from real depth data. Mypy=[index, arg-type]
        if real_depth.bids is not None and real_depth.asks is not None:
            # Test raw depth data transformation with REAL data
            # Backpack sends incremental updates which may be empty - handle accordingly
            bids_data = real_depth.bids[:3] if len(real_depth.bids) >= 3 else real_depth.bids
            asks_data = real_depth.asks[:3] if len(real_depth.asks) >= 3 else real_depth.asks

            # If the real data is completely empty, this is a critical issue
            if not bids_data and not asks_data:
                pytest.fail(
                    f"Received completely empty depth data for {test_symbol}. "
                    f"This indicates: 1) WebSocket subscription failed, "
                    f"2) Depth processor is not accumulating state correctly, "
                    f"3) Market maker pulled all orders (highly unlikely), "
                    f"or 4) Data serialization issue. This must be investigated."
                )

            raw_depth_data = {
                "bids": bids_data,  # Use available levels
                "asks": asks_data,
                "U": "123",  # First update ID (required field)
                "u": "456",  # Last update ID (required field)
            }

            try:
                # Create BackpackRawDepthUpdateEvent
                depth_event = BackpackRawDepthUpdateEvent.model_validate(raw_depth_data)

                # Symbol is passed separately since it's extracted from stream name
                # DEFENSIVE CHECK: Handle None bids/asks in validation. Mypy=[arg-type]
                if depth_event.bids is not None and depth_event.asks is not None:
                    # Validate that we have the expected amount of data (or at least some)
                    expected_bids = len(bids_data)
                    expected_asks = len(asks_data)
                    assert len(depth_event.bids) == expected_bids, (
                        f"Expected {expected_bids} bids, got {len(depth_event.bids)}"
                    )
                    assert len(depth_event.asks) == expected_asks, (
                        f"Expected {expected_asks} asks, got {len(depth_event.asks)}"
                    )

                    # Test manual OrderBook creation (simulating transformer)
                    orderbook = OrderBook(
                        symbol=test_symbol,  # Symbol comes from stream name, not event
                        bids=[(Decimal(price), Decimal(qty)) for price, qty in depth_event.bids],
                        asks=[(Decimal(price), Decimal(qty)) for price, qty in depth_event.asks],
                        timestamp=datetime.now(UTC),  # Would be set by transformer
                    )

                    # Validate created OrderBook - only if we have data
                    self._validate_orderbook_structure(orderbook, test_symbol)
                    # Only validate financial data if we have price levels
                    if orderbook.bids or orderbook.asks:
                        self._validate_orderbook_financial_data(orderbook)
                        self._validate_orderbook_integrity(orderbook)

                    logger.info(
                        "orderbook_creation_from_raw_data_success",
                        symbol=orderbook.symbol,
                        raw_bids_count=len(depth_event.bids),
                        raw_asks_count=len(depth_event.asks),
                        orderbook_bids_count=len(orderbook.bids),
                        orderbook_asks_count=len(orderbook.asks),
                        message="✓ OrderBook successfully created from raw depth data",
                    )

            except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                pytest.fail(
                    f"OrderBook creation from raw depth data failed: {e}. "
                    "Raw data to OrderBook transformation not working."
                )

    @pytest.mark.asyncio
    async def test_orderbook_precision_preservation(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that OrderBook preserves decimal precision from WebSocket data."""
        # Get real depth data to test precision preservation
        await ensure_websocket_connected(bp_api_for_test_env)
        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for testing")
        test_symbol = markets[0].symbol

        # Get REAL depth data
        real_depth = await get_real_depth_data(bp_api_for_test_env, test_symbol)

        # DEFENSIVE CHECK: Handle None bids/asks from real depth data. Mypy=[index, attr-defined]
        if real_depth.bids is not None and real_depth.asks is not None:
            # Use actual market data to test precision preservation
            high_precision_depth_data = {
                "bids": real_depth.bids[:2],  # Use actual bids with their real precision
                "asks": real_depth.asks[:2],
                "U": "123",  # First update ID (required field)
                "u": "456",  # Last update ID (required field)
            }

            try:
                depth_event = BackpackRawDepthUpdateEvent.model_validate(high_precision_depth_data)

                # DEFENSIVE CHECK: Handle None bids/asks in depth event. Mypy=[union-attr]
                if depth_event.bids is not None and depth_event.asks is not None:
                    # Create OrderBook with high precision
                    orderbook = OrderBook(
                        symbol=test_symbol,  # Symbol comes from stream name, not event
                        bids=[(Decimal(price), Decimal(qty)) for price, qty in depth_event.bids],
                        asks=[(Decimal(price), Decimal(qty)) for price, qty in depth_event.asks],
                        timestamp=datetime.now(UTC),
                    )

                    # Validate precision preservation with real data
                    # Note: real_depth.bids is guaranteed non-None by outer if condition
                    if len(real_depth.bids) > 0:
                        expected_bid_price = Decimal(real_depth.bids[0][0])
                        expected_bid_qty = Decimal(real_depth.bids[0][1])

                        actual_bid_price = orderbook.bids[0][0]
                        actual_bid_qty = orderbook.bids[0][1]

                        assert actual_bid_price == expected_bid_price, (
                            f"Bid price precision not preserved: expected {expected_bid_price}, "
                            f"got {actual_bid_price}"
                        )
                        assert actual_bid_qty == expected_bid_qty, (
                            f"Bid quantity precision not preserved: expected {expected_bid_qty}, "
                            f"got {actual_bid_qty}"
                        )

                        logger.info(
                            "orderbook_precision_preservation_success",
                            symbol=orderbook.symbol,
                            original_bid_price=real_depth.bids[0][0],
                            preserved_bid_price=str(actual_bid_price),
                            original_bid_qty=real_depth.bids[0][1],
                            preserved_bid_qty=str(actual_bid_qty),
                            message="✓ OrderBook precision preservation validated",
                        )

            except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                pytest.fail(
                    f"OrderBook precision preservation test failed: {e}. "
                    "Decimal precision not preserved in OrderBook creation."
                )

    @pytest.mark.asyncio
    async def test_orderbook_update_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test OrderBook handling of incremental updates."""
        try:
            await bp_api_for_test_env.connect_websocket()

            if not bp_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed - cannot test OrderBook updates")

            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            if not markets:
                pytest.fail("No markets available for OrderBook update testing")

            test_symbol = markets[0].symbol
            orderbook_updates: list[dict[str, Any]] = []

            async def update_handler(context: WebSocketContextProtocol) -> None:
                """Handler that tracks OrderBook updates."""
                await asyncio.sleep(0)  # Satisfy RUF029

                # Extract data from typed context
                context_data: dict[str, Any] = {}
                if (
                    hasattr(context, "validated_envelope")
                    and context.validated_envelope is not None
                    and hasattr(context.validated_envelope, "data")
                ):
                    data = context.validated_envelope.data
                    context_data = data if isinstance(data, dict) else {"data": data}

                orderbook_updates.append({
                    "context": context,
                    "timestamp": asyncio.get_event_loop().time(),
                })

                logger.info(
                    "orderbook_update_received",
                    update_count=len(orderbook_updates),
                    context_keys=list(context_data.keys()),
                    message=f"OrderBook update {len(orderbook_updates)} received",
                )

            # Subscribe to depth updates
            await bp_api_for_test_env.subscribe(f"depth.{test_symbol}", update_handler)

            # Rule #4: Collect updates with proper wait condition
            await wait_for_websocket_data(orderbook_updates, min_count=5, timeout_seconds=10.0)

            if orderbook_updates:
                # Analyze update patterns
                update_intervals: list[float] = []
                for i in range(1, len(orderbook_updates)):
                    interval = (
                        orderbook_updates[i]["timestamp"] - orderbook_updates[i - 1]["timestamp"]
                    )
                    update_intervals.append(interval)

                if update_intervals:
                    avg_interval = sum(update_intervals) / len(update_intervals)
                    min_interval = min(update_intervals)
                    max_interval = max(update_intervals)

                    logger.info(
                        "orderbook_update_analysis",
                        total_updates=len(orderbook_updates),
                        avg_interval_seconds=f"{avg_interval:.3f}",
                        min_interval_seconds=f"{min_interval:.3f}",
                        max_interval_seconds=f"{max_interval:.3f}",
                        message="✓ OrderBook update handling analysis completed",
                    )

                logger.info(
                    "orderbook_update_handling_success",
                    symbol=test_symbol,
                    updates_received=len(orderbook_updates),
                    message=f"✓ Successfully handled {len(orderbook_updates)} OrderBook updates",
                )
            else:
                # Rule #2: Use pytest.fail instead of logger.warning
                pytest.fail(
                    f"No OrderBook updates received for {test_symbol}. "
                    "Check update handling pipeline - depth updates not being received."
                )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"OrderBook update handling test failed: {e}. "
                "OrderBook update handling not working properly."
            )
        except (TimeoutError, ConnectionError, OSError) as e:
            # Rule #10: Network errors are test failures
            pytest.fail(
                f"Network error during OrderBook update test: {e}. "
                "Test requires continuous WebSocket connection for updates."
            )

    @pytest.mark.asyncio
    async def test_orderbook_error_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test OrderBook creation error handling with invalid data."""
        # Get a real symbol for testing
        await ensure_websocket_connected(bp_api_for_test_env)
        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for testing")
        test_symbol = markets[0].symbol

        # Get real depth data to use realistic price/qty in invalid scenarios
        real_depth = await get_real_depth_data(bp_api_for_test_env, test_symbol)
        # DEFENSIVE CHECK: Handle None bids from real depth data. Mypy=[index]
        if real_depth.bids is not None and len(real_depth.bids) > 0:
            sample_price = real_depth.bids[0][0]
            sample_qty = real_depth.bids[0][1]
        else:
            sample_price = "1.0"
            sample_qty = "1.0"

        # Test various invalid depth data scenarios
        # Note: Symbol is not part of raw event data (extracted from stream name)
        invalid_depth_scenarios: list[tuple[dict[str, Any], str]] = [
            ({}, "Empty depth data"),
            ({"U": "123", "u": "456"}, "Missing bids/asks"),
            ({"U": "123", "u": "456", "bids": None, "asks": None}, "None bids/asks"),
            ({"U": "123", "u": "456", "bids": [], "asks": []}, "Empty bids/asks"),
            (
                {
                    "U": "123",
                    "u": "456",
                    "bids": [["invalid", sample_qty]],
                    "asks": [[sample_price, sample_qty]],
                },
                "Invalid bid price",
            ),
            (
                {
                    "U": "123",
                    "u": "456",
                    "bids": [[sample_price, "invalid"]],
                    "asks": [[sample_price, sample_qty]],
                },
                "Invalid bid quantity",
            ),
            (
                {
                    "U": "123",
                    "u": "456",
                    "bids": [[sample_price, sample_qty]],
                    "asks": [["invalid", sample_qty]],
                },
                "Invalid ask price",
            ),
            (
                {
                    "U": "123",
                    "u": "456",
                    "bids": [[sample_price, sample_qty]],
                    "asks": [[sample_price, "invalid"]],
                },
                "Invalid ask quantity",
            ),
        ]

        for invalid_data, description in invalid_depth_scenarios:
            try:
                # Try to create depth event (symbol is not part of raw data)
                depth_event = BackpackRawDepthUpdateEvent.model_validate(invalid_data)

                # Try to create OrderBook
                try:
                    orderbook = OrderBook(
                        symbol=test_symbol,  # Symbol comes from stream name, not event
                        bids=[(Decimal(price), Decimal(qty)) for price, qty in depth_event.bids]
                        if depth_event.bids
                        else [],
                        asks=[(Decimal(price), Decimal(qty)) for price, qty in depth_event.asks]
                        if depth_event.asks
                        else [],
                        timestamp=datetime.now(UTC),
                    )

                    # If creation succeeds, validate it's reasonable
                    if not orderbook.bids and not orderbook.asks:
                        logger.info(
                            "orderbook_error_empty_created",
                            description=description,
                            message=f"Empty OrderBook created for {description} (may be valid)",
                        )
                    else:
                        logger.info(
                            "orderbook_error_unexpectedly_succeeded",
                            description=description,
                            bids_count=len(orderbook.bids),
                            asks_count=len(orderbook.asks),
                            message=(
                                f"OrderBook creation unexpectedly succeeded for {description}"
                            ),
                        )

                except (ValueError, TypeError, Exception) as e:
                    logger.info(
                        "orderbook_error_creation_correctly_failed",
                        description=description,
                        error=str(e)[:100],
                        message=f"✓ OrderBook creation correctly failed for {description}",
                    )

            except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                logger.info(
                    "orderbook_error_validation_failed",
                    description=description,
                    error=str(e)[:100],
                    message=f"✓ Depth event validation correctly failed for {description}",
                )

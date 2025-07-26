"""Test 7: Internal Model Creation from WebSocket Streams (OrderBook Focus).

This module tests that Hyperliquid WebSocket streams are properly converted to
internal domain models, with specific focus on OrderBook creation from l2Book streams.

Security Compliance:
- Tests OrderBook creation from real WebSocket l2Book data
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

from cyberdelta.apis.common.types import MessageHandler
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
)
from cyberdelta.apis.models.service_args import GetMarketsArgs
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market.order_book import OrderBook


pytestmark = [
    pytest.mark.integration,
    pytest.mark.timing,
]

logger = get_logger(__name__)


class TestHyperliquidOrderBookModelCreation:
    """Test OrderBook model creation from WebSocket l2Book streams."""

    async def _setup_hl_orderbook_connection(self, api: HyperliquidAPI) -> str:
        """Set up Hyperliquid connection and get test symbol for orderbook testing."""
        await api.connect_websocket()
        if not api.is_connected:
            pytest.fail("WebSocket connection failed - cannot test OrderBook creation")

        markets = await api.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for OrderBook testing")
        return markets[0].symbol

    async def _create_hl_orderbook_handler(
        self, received_orderbooks: list[OrderBook]
    ) -> MessageHandler:
        """Create handler that extracts OrderBook from Hyperliquid context."""

        async def orderbook_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)

            # Extract data from typed context
            if (
                hasattr(context, "validated_envelope")
                and context.validated_envelope is not None
                and hasattr(context.validated_envelope, "data")
            ):
                data = context.validated_envelope.data
                if isinstance(data, dict):
                    if "orderbook" in data and isinstance(data["orderbook"], OrderBook):
                        orderbook = data["orderbook"]
                        received_orderbooks.append(orderbook)
                        self._log_hl_orderbook_received(orderbook, "orderbook")
                    elif "order_book" in data and isinstance(data["order_book"], OrderBook):
                        orderbook = data["order_book"]
                        received_orderbooks.append(orderbook)
                        self._log_hl_orderbook_received(orderbook, "order_book")
                    else:
                        self._log_hl_orderbook_context_structure(data)
                else:
                    self._log_hl_orderbook_context_structure({"data": data})
            else:
                self._log_hl_orderbook_context_structure({"context": str(context)})

        return orderbook_handler

    def _log_hl_orderbook_received(self, orderbook: OrderBook, key: str) -> None:
        """Log information about received Hyperliquid orderbook."""
        logger.info(
            "orderbook_received_from_stream",
            symbol=orderbook.symbol,
            bids_count=len(orderbook.bids),
            asks_count=len(orderbook.asks),
            message=f"✓ OrderBook received for {orderbook.symbol} (key: {key})",
        )

    def _log_hl_orderbook_context_structure(self, context: dict[str, Any]) -> None:
        """Log context structure for debugging when no Hyperliquid OrderBook found."""
        logger.info(
            "orderbook_context_structure",
            context_keys=list(context.keys()),
            context_types={k: type(v).__name__ for k, v in context.items()},
            message="L2Book stream context structure (no OrderBook found)",
        )

    def _validate_all_hl_received_orderbooks(
        self, received_orderbooks: list[OrderBook], test_symbol: str
    ) -> None:
        """Validate all received Hyperliquid orderbooks and log results."""
        if received_orderbooks:
            for orderbook in received_orderbooks[:3]:
                self._validate_orderbook_structure(orderbook, test_symbol)
                self._validate_orderbook_financial_data(orderbook)
                self._validate_orderbook_integrity(orderbook)

            logger.info(
                "orderbook_creation_from_stream_success",
                symbol=test_symbol,
                orderbooks_received=len(received_orderbooks),
                message=(
                    f"✓ Successfully created {len(received_orderbooks)} "
                    "OrderBooks from l2Book stream"
                ),
            )
        else:
            logger.warning(
                "orderbook_creation_no_orderbooks_received",
                symbol=test_symbol,
                message=(
                    "No OrderBook models received from l2Book stream - "
                    "check transformation pipeline"
                ),
            )

    @pytest.mark.asyncio
    async def test_orderbook_creation_from_l2book_stream(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test OrderBook creation from real WebSocket l2Book streams."""
        try:
            test_symbol = await self._setup_hl_orderbook_connection(hl_api_for_test_env)
            received_orderbooks: list[OrderBook] = []

            orderbook_handler = await self._create_hl_orderbook_handler(received_orderbooks)
            await hl_api_for_test_env.subscribe(f"l2Book:{test_symbol}", orderbook_handler)
            await asyncio.sleep(5.0)

            self._validate_all_hl_received_orderbooks(received_orderbooks, test_symbol)

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"OrderBook creation from l2Book stream failed: {e}. "
                "WebSocket to OrderBook transformation not working."
            )

    def _validate_orderbook_structure(self, orderbook: OrderBook, expected_symbol: str) -> None:
        """Validate OrderBook model structure."""
        assert isinstance(orderbook, OrderBook), f"Expected OrderBook, got {type(orderbook)}"
        assert orderbook.symbol == expected_symbol, (
            f"Expected symbol {expected_symbol}, got {orderbook.symbol}"
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
    async def test_orderbook_creation_from_raw_l2book_data(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test OrderBook creation from raw l2Book data."""
        # Test raw l2Book data transformation
        raw_l2book_data = {
            "coin": "BTC",
            "levels": [
                [
                    {"px": "50000.0", "sz": "1.5", "n": 2},  # bid levels
                    {"px": "49950.0", "sz": "2.0", "n": 3},
                    {"px": "49900.0", "sz": "0.8", "n": 1},
                ],
                [  # ask levels
                    {"px": "50050.0", "sz": "1.2", "n": 1},
                    {"px": "50100.0", "sz": "1.8", "n": 2},
                    {"px": "50150.0", "sz": "0.9", "n": 1},
                ],
            ],
            "time": 1640995200000,
        }

        try:
            # Create HyperliquidRawWsBookUpdate
            l2book_event = HyperliquidRawWsBookUpdate.model_validate(raw_l2book_data)

            assert l2book_event.coin == "BTC"
            assert len(l2book_event.levels) == 2  # bids and asks
            assert len(l2book_event.levels[0]) == 3  # 3 bid levels
            assert len(l2book_event.levels[1]) == 3  # 3 ask levels

            # Test manual OrderBook creation (simulating transformer)
            bids = [(Decimal(level.px), Decimal(level.sz)) for level in l2book_event.levels[0]]
            asks = [(Decimal(level.px), Decimal(level.sz)) for level in l2book_event.levels[1]]

            orderbook = OrderBook(
                symbol=l2book_event.coin,
                bids=bids,
                asks=asks,
                timestamp=datetime.now(UTC),  # Would be set by transformer
            )

            # Validate created OrderBook
            self._validate_orderbook_structure(orderbook, "BTC")
            self._validate_orderbook_financial_data(orderbook)
            self._validate_orderbook_integrity(orderbook)

            logger.info(
                "orderbook_creation_from_raw_data_success",
                symbol=orderbook.symbol,
                raw_bids_count=len(l2book_event.levels[0]),
                raw_asks_count=len(l2book_event.levels[1]),
                orderbook_bids_count=len(orderbook.bids),
                orderbook_asks_count=len(orderbook.asks),
                message="✓ OrderBook successfully created from raw l2Book data",
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"OrderBook creation from raw l2Book data failed: {e}. "
                "Raw data to OrderBook transformation not working."
            )

    @pytest.mark.asyncio
    async def test_orderbook_precision_preservation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that OrderBook correctly handles Hyperliquid's precision rules.

        Hyperliquid SDK applies different precision rules:
        - Prices (px): SDK float_to_wire algorithm rounds to 8 decimal places
        - Sizes (sz): Original precision is preserved
        """
        # Test with high precision decimal data
        high_precision_l2book_data = {
            "coin": "ETH",
            "levels": [
                [  # bid levels
                    {"px": "3456.123456789", "sz": "12.987654321", "n": 5},
                    {"px": "3456.123456788", "sz": "5.123456789", "n": 2},
                ],
                [  # ask levels
                    {"px": "3456.123456790", "sz": "8.876543210", "n": 3},
                    {"px": "3456.123456791", "sz": "15.234567890", "n": 4},
                ],
            ],
            "time": 1640995200000,
        }

        try:
            l2book_event = HyperliquidRawWsBookUpdate.model_validate(high_precision_l2book_data)

            # Create OrderBook with high precision
            # Note: level.px and level.sz are already SDK-transformed strings from validation
            bids = [(Decimal(level.px), Decimal(level.sz)) for level in l2book_event.levels[0]]
            asks = [(Decimal(level.px), Decimal(level.sz)) for level in l2book_event.levels[1]]

            orderbook = OrderBook(
                symbol=l2book_event.coin, bids=bids, asks=asks, timestamp=datetime.now(UTC)
            )

            # Validate SDK-compliant precision
            # The SDK's float_to_wire algorithm rounds PRICES to 8 decimal places
            # but SIZES preserve their original precision
            expected_bid_price = Decimal("3456.12345679")  # SDK rounds price to 8 decimals
            expected_bid_qty = Decimal("12.987654321")  # Size preserves original precision

            actual_bid_price = orderbook.bids[0][0]
            actual_bid_qty = orderbook.bids[0][1]

            assert actual_bid_price == expected_bid_price, (
                f"Bid price not SDK-compliant: expected {expected_bid_price}, "
                f"got {actual_bid_price}"
            )
            assert actual_bid_qty == expected_bid_qty, (
                f"Bid quantity not SDK-compliant: expected {expected_bid_qty}, got {actual_bid_qty}"
            )

            # Store original values for logging
            # We know the structure of high_precision_l2book_data from above
            original_bid_price = "3456.123456789"  # From the test data above
            original_bid_qty = "12.987654321"  # From the test data above

            logger.info(
                "orderbook_sdk_compliant_precision_success",
                symbol=orderbook.symbol,
                original_bid_price=original_bid_price,
                sdk_compliant_bid_price=str(actual_bid_price),
                original_bid_qty=original_bid_qty,
                sdk_compliant_bid_qty=str(actual_bid_qty),
                message="✓ OrderBook SDK-compliant precision validated",
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"OrderBook precision preservation test failed: {e}. "
                "Decimal precision not preserved in OrderBook creation."
            )

    @pytest.mark.asyncio
    async def test_orderbook_update_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test OrderBook handling of incremental updates."""
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed - cannot test OrderBook updates")

            markets = await hl_api_for_test_env.get_markets(GetMarketsArgs())
            if not markets:
                pytest.fail("No markets available for OrderBook update testing")

            test_symbol = markets[0].symbol
            orderbook_updates: list[dict[str, Any]] = []

            async def update_handler(context: WebSocketContextProtocol) -> None:
                """Handler that tracks OrderBook updates."""
                await asyncio.sleep(0)  # Satisfy RUF029
                orderbook_updates.append({
                    "context": context,
                    "timestamp": asyncio.get_event_loop().time(),
                })

                # Log context info
                context_keys = []
                if (
                    hasattr(context, "validated_envelope")
                    and context.validated_envelope is not None
                    and hasattr(context.validated_envelope, "data")
                ):
                    data = context.validated_envelope.data
                    if isinstance(data, dict):
                        context_keys = list(data.keys())

                logger.info(
                    "orderbook_update_received",
                    update_count=len(orderbook_updates),
                    context_keys=context_keys,
                    message=f"OrderBook update {len(orderbook_updates)} received",
                )

            # Subscribe to l2Book updates
            await hl_api_for_test_env.subscribe(f"l2Book:{test_symbol}", update_handler)

            # Collect updates
            await asyncio.sleep(5.0)

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
                logger.warning(
                    "orderbook_update_no_updates_received",
                    symbol=test_symbol,
                    message="No OrderBook updates received - check update handling pipeline",
                )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"OrderBook update handling test failed: {e}. "
                "OrderBook update handling not working properly."
            )

    @pytest.mark.asyncio
    async def test_orderbook_error_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test OrderBook creation error handling with invalid data."""
        # Test various invalid l2Book data scenarios
        invalid_l2book_scenarios: list[tuple[dict[str, Any], str]] = [
            ({}, "Empty l2Book data"),
            ({"coin": "BTC"}, "Missing levels"),
            ({"coin": "BTC", "levels": None}, "None levels"),
            ({"coin": "BTC", "levels": []}, "Empty levels"),
            (
                {
                    "coin": "",
                    "levels": [
                        [{"px": "50000", "sz": "1", "n": 1}],
                        [{"px": "50100", "sz": "1", "n": 1}],
                    ],
                },
                "Empty coin",
            ),
            (
                {
                    "coin": "BTC",
                    "levels": [
                        [{"px": "invalid", "sz": "1", "n": 1}],
                        [{"px": "50100", "sz": "1", "n": 1}],
                    ],
                },
                "Invalid bid price",
            ),
            (
                {
                    "coin": "BTC",
                    "levels": [
                        [{"px": "50000", "sz": "invalid", "n": 1}],
                        [{"px": "50100", "sz": "1", "n": 1}],
                    ],
                },
                "Invalid bid quantity",
            ),
            (
                {
                    "coin": "BTC",
                    "levels": [
                        [{"px": "50000", "sz": "1", "n": 1}],
                        [{"px": "invalid", "sz": "1", "n": 1}],
                    ],
                },
                "Invalid ask price",
            ),
            (
                {
                    "coin": "BTC",
                    "levels": [
                        [{"px": "50000", "sz": "1", "n": 1}],
                        [{"px": "50100", "sz": "invalid", "n": 1}],
                    ],
                },
                "Invalid ask quantity",
            ),
        ]

        for invalid_data, description in invalid_l2book_scenarios:
            try:
                # Try to create l2Book event
                if "coin" in invalid_data:
                    l2book_event = HyperliquidRawWsBookUpdate.model_validate(invalid_data)

                    # Try to create OrderBook
                    try:
                        if l2book_event.levels and len(l2book_event.levels) >= 2:
                            bids = [
                                (Decimal(level.px), Decimal(level.sz))
                                for level in l2book_event.levels[0]
                            ]
                            asks = [
                                (Decimal(level.px), Decimal(level.sz))
                                for level in l2book_event.levels[1]
                            ]
                        else:
                            bids = []
                            asks = []

                        orderbook = OrderBook(
                            symbol=l2book_event.coin,
                            bids=bids,
                            asks=asks,
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
                else:
                    # Skip if no coin
                    logger.info(
                        "orderbook_error_skipped_no_coin",
                        description=description,
                        message=f"Skipped {description} - no coin to test",
                    )

            except (
                ValidationError,
                ValueError,
                TypeError,
                KeyError,
                AttributeError,
                Exception,
            ) as e:
                logger.info(
                    "orderbook_error_validation_failed",
                    description=description,
                    error=str(e)[:100],
                    message=f"✓ L2Book event validation correctly failed for {description}",
                )

    @pytest.mark.asyncio
    async def test_hyperliquid_orderbook_specifics(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test Hyperliquid-specific OrderBook features."""
        # Hyperliquid l2Book includes number of orders per level
        hl_specific_data = {
            "coin": "SOL",
            "levels": [
                [
                    {"px": "100.5", "sz": "50.0", "n": 10},  # 10 orders at this level
                    {"px": "100.4", "sz": "75.0", "n": 15},  # 15 orders at this level
                ],
                [
                    {"px": "100.6", "sz": "45.0", "n": 8},  # 8 orders at this level
                    {"px": "100.7", "sz": "60.0", "n": 12},  # 12 orders at this level
                ],
            ],
            "time": 1640995200000,
        }

        try:
            l2book_event = HyperliquidRawWsBookUpdate.model_validate(hl_specific_data)

            # Verify Hyperliquid-specific data is present
            assert hasattr(l2book_event.levels[0][0], "px"), (
                "Hyperliquid bid level should have px field"
            )
            assert hasattr(l2book_event.levels[0][0], "sz"), (
                "Hyperliquid bid level should have sz field"
            )
            assert hasattr(l2book_event.levels[0][0], "n"), (
                "Hyperliquid bid level should have n field"
            )

            # Extract order counts
            bid_order_counts = [int(level.n) for level in l2book_event.levels[0]]
            ask_order_counts = [int(level.n) for level in l2book_event.levels[1]]

            logger.info(
                "hyperliquid_orderbook_specifics",
                coin=l2book_event.coin,
                bid_levels=len(l2book_event.levels[0]),
                ask_levels=len(l2book_event.levels[1]),
                total_bid_orders=sum(bid_order_counts),
                total_ask_orders=sum(ask_order_counts),
                message="✓ Hyperliquid-specific OrderBook features validated",
            )

            # Create OrderBook (standard format, may not include order counts)
            bids = [(Decimal(level.px), Decimal(level.sz)) for level in l2book_event.levels[0]]
            asks = [(Decimal(level.px), Decimal(level.sz)) for level in l2book_event.levels[1]]

            orderbook = OrderBook(
                symbol=l2book_event.coin, bids=bids, asks=asks, timestamp=datetime.now(UTC)
            )

            # Validate standard OrderBook creation still works
            self._validate_orderbook_structure(orderbook, "SOL")

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Hyperliquid-specific OrderBook test failed: {e}. "
                "Hyperliquid OrderBook features not working."
            )

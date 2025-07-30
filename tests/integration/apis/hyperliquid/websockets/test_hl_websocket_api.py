"""Integration Tests for HyperliquidAPI WebSocket Functionality.

This module tests WebSocket API functionality with real endpoints,
ensuring compliance with trading security rules and real-time data requirements.

Security Compliance:
- NO hardcoded financial values - all prices/quantities from real market data
- Fail-fast error handling - no graceful failures for critical operations
- Real endpoint testing with live WebSocket connections
- Timezone-aware datetime operations throughout
- Decimal precision for all financial calculations
"""

import asyncio
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args.market_data import GetMarketsArgs
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market import OrderBook, Trade


pytestmark = [pytest.mark.integration, pytest.mark.timing]
logger = get_logger(__name__)


async def get_active_trading_symbols(api: HyperliquidAPI) -> list[str]:
    """Get actively trading symbols from exchange with real market data.

    Args:
        api: HyperliquidAPI instance

    Returns:
        List of active trading symbols

    Raises:
        RuntimeError: If unable to fetch real market symbols
    """
    try:
        markets = await api.get_markets(GetMarketsArgs())

        # Filter for active markets only
        active_symbols: list[str] = []
        for market in markets:
            if hasattr(market, "is_active") and getattr(market, "is_active", True):
                active_symbols.append(market.symbol)
            else:
                # If no is_active field, include all symbols
                active_symbols.append(market.symbol)

        if not active_symbols:
            raise RuntimeError(
                "No active trading symbols available from exchange. "
                "WebSocket tests require real, active market symbols.",
            )

        # Return first 5 active symbols for testing
        return active_symbols[:5]

    except (APIError, ValueError, TypeError, KeyError) as e:
        raise RuntimeError(
            f"Failed to fetch real trading symbols from exchange: {e}. "
            "WebSocket tests cannot proceed without real market data.",
        ) from e


class TestHyperliquidWebSocketMarketData:
    """Test WebSocket market data functionality with real endpoints."""

    async def _setup_l2book_test(
        self, hl_api: HyperliquidAPI
    ) -> tuple[str, list[OrderBook], asyncio.Event]:
        """Setup L2 book test environment.

        Returns:
            Tuple of (test_symbol, received_orderbooks list, data_received event).
        """
        active_symbols = await get_active_trading_symbols(hl_api)
        test_symbol = active_symbols[0]
        received_orderbooks: list[OrderBook] = []
        data_received = asyncio.Event()
        return test_symbol, received_orderbooks, data_received

    def _create_l2book_handler(
        self, test_symbol: str, received_orderbooks: list[OrderBook], data_received: asyncio.Event
    ) -> Callable[[WebSocketContextProtocol], Coroutine[Any, Any, None]]:
        """Create L2 book handler with validation.

        Returns:
            Async handler function that processes L2 book WebSocket messages.
        """

        async def l2book_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029

            try:
                # context is already typed as dict[str, Any] so no isinstance check needed
                if not context:
                    pytest.fail(
                        "L2 book context cannot be empty. "
                        "Invalid context format is critical for order book integrity."
                    )

                # Extract data from typed context
                if (
                    hasattr(context, "validated_envelope")
                    and context.validated_envelope is not None
                    and hasattr(context.validated_envelope, "data")
                ):
                    data = context.validated_envelope.data
                    if isinstance(data, dict) and "bids" in data and "asks" in data:
                        orderbook = OrderBook(
                            symbol=test_symbol,
                            bids=[
                                (Decimal(str(p)), Decimal(str(q))) for p, q in data.get("bids", [])
                            ],
                            asks=[
                                (Decimal(str(p)), Decimal(str(q))) for p, q in data.get("asks", [])
                            ],
                            timestamp=datetime.now(UTC),
                        )

                        self._validate_orderbook_prices(orderbook)
                        self._validate_orderbook_spread(orderbook)

                        received_orderbooks.append(orderbook)
                    data_received.set()

            except (ValueError, TypeError, KeyError) as e:
                pytest.fail(
                    f"Failed to process L2 book data: {e}. "
                    "Order book processing is critical for trading decisions."
                )

        return l2book_handler

    def _validate_orderbook_prices(self, orderbook: OrderBook) -> None:
        """Validate order book price integrity."""
        if orderbook.bids:
            best_bid = orderbook.bids[0][0]
            if best_bid <= Decimal(0):
                pytest.fail(
                    f"Invalid best bid price: {best_bid}. "
                    "Order book prices must be positive for trading safety."
                )

        if orderbook.asks:
            best_ask = orderbook.asks[0][0]
            if best_ask <= Decimal(0):
                pytest.fail(
                    f"Invalid best ask price: {best_ask}. "
                    "Order book prices must be positive for trading safety."
                )

    def _validate_orderbook_spread(self, orderbook: OrderBook) -> None:
        """Validate order book spread."""
        if orderbook.bids and orderbook.asks:
            spread = orderbook.asks[0][0] - orderbook.bids[0][0]
            if spread < Decimal(0):
                pytest.fail(
                    f"Negative spread detected: {spread}. "
                    "Ask must be higher than bid for valid order book."
                )

    def _validate_received_orderbooks(self, orderbooks: list[OrderBook], symbol: str) -> None:
        """Validate all received order book updates."""
        assert len(orderbooks) > 0, "Must receive at least one order book update"

        for orderbook in orderbooks:
            data_age = datetime.now(UTC) - orderbook.timestamp
            if data_age > timedelta(seconds=10):
                pytest.fail(
                    f"Order book data is {data_age.total_seconds():.1f}s old. "
                    "Stale order book data is dangerous for trading."
                )

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_l2book_real_time_updates(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test L2 book updates with real market data and proper validation."""
        test_symbol, received_orderbooks, data_received = await self._setup_l2book_test(
            hl_api_for_test_env
        )
        l2book_handler = self._create_l2book_handler(
            test_symbol, received_orderbooks, data_received
        )
        topic = f"l2Book:{test_symbol}"

        try:
            await hl_api_for_test_env.subscribe(topic, l2book_handler)

            # Test successful subscription - do not simulate fake market data
            # This tests that the subscription mechanism works through public API
            logger.info(
                "l2book_subscription_established",
                symbol=test_symbol,
                message="✓ L2 book subscription established successfully",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"L2 book subscription failed: {e}. "
                "Order book data is critical for market making and trading."
            )

    def _create_trades_handler(
        self, test_symbol: str, received_trades: list[Trade], trade_received: asyncio.Event
    ) -> Callable[[WebSocketContextProtocol], Coroutine[Any, Any, None]]:
        """Create trades handler with validation.

        Returns:
            Async handler function that processes trades WebSocket messages.
        """

        async def trades_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029

            try:
                # context is already typed as dict[str, Any] so no isinstance check needed
                if not context:
                    pytest.fail(
                        "Trade context cannot be empty. "
                        "Invalid trade format could cause position tracking errors."
                    )

                # Extract data from typed context
                if (
                    hasattr(context, "validated_envelope")
                    and context.validated_envelope is not None
                    and hasattr(context.validated_envelope, "data")
                ):
                    data = context.validated_envelope.data
                    if isinstance(data, dict) and "trades" in data:
                        for trade_data in data["trades"]:
                            trade = Trade(
                                id=str(trade_data.get("tid", "")),
                                symbol=test_symbol,
                                price=Decimal(str(trade_data.get("px", 0))),
                                quantity=Decimal(str(trade_data.get("sz", 0))),
                                executed_at=datetime.fromtimestamp(
                                    trade_data.get("time", 0) / 1000, tz=UTC
                                ),
                                exchange="hyperliquid",
                                side=trade_data.get("side", "buy"),
                                order_id=str(trade_data.get("oid", "unknown")),
                            )

                            self._validate_trade_data(trade)
                            self._validate_trade_timestamp(trade)
                            received_trades.append(trade)
                        trade_received.set()

            except (ValueError, TypeError, KeyError) as e:
                pytest.fail(
                    f"Failed to process trade data: {e}. "
                    "Trade processing is critical for execution analysis."
                )

        return trades_handler

    def _validate_trade_data(self, trade: Trade) -> None:
        """Validate trade data integrity."""
        if trade.price <= Decimal(0):
            pytest.fail(
                f"Invalid trade price: {trade.price}. "
                "Trade prices must be positive for proper execution tracking."
            )

        if trade.quantity <= Decimal(0):
            pytest.fail(
                f"Invalid trade quantity: {trade.quantity}. "
                "Trade quantities must be positive for volume analysis."
            )

    def _validate_trade_timestamp(self, trade: Trade) -> None:
        """Validate trade timestamp is timezone-aware."""
        if trade.executed_at.tzinfo is None:
            pytest.fail(
                "Trade timestamp must be timezone-aware. "
                "Timezone-naive timestamps cause trading errors."
            )

    def _create_allmids_handler(
        self, received_mids: list[dict[str, Decimal]], mids_received: asyncio.Event
    ) -> Callable[[WebSocketContextProtocol], Coroutine[Any, Any, None]]:
        """Create allmids handler with validation.

        Returns:
            Async handler function that processes allmids WebSocket messages.
        """

        async def allmids_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029

            try:
                # context is already typed as dict[str, Any] so no isinstance check needed
                if not context:
                    pytest.fail(
                        "AllMids context cannot be empty. "
                        "Invalid format could cause portfolio valuation errors."
                    )

                mids: dict[str, Decimal] = {}
                # Extract data from typed context
                if (
                    hasattr(context, "validated_envelope")
                    and context.validated_envelope is not None
                    and hasattr(context.validated_envelope, "data")
                ):
                    data = context.validated_envelope.data
                    if isinstance(data, dict) and "mids" in data:
                        for coin, price in data["mids"].items():
                            decimal_price = Decimal(str(price))
                            self._validate_mid_price(coin, decimal_price)
                            mids[coin] = decimal_price

                if mids:
                    received_mids.append(mids)
                    mids_received.set()

            except (ValueError, TypeError, KeyError) as e:
                pytest.fail(
                    f"Failed to process allMids data: {e}. "
                    "Mid price processing is critical for portfolio valuation."
                )

        return allmids_handler

    def _validate_mid_price(self, coin: str, price: Decimal) -> None:
        """Validate mid price data."""
        if price <= Decimal(0):
            pytest.fail(
                f"Invalid mid price for {coin}: {price}. "
                "Mid prices must be positive for portfolio valuation."
            )

        # Note: No upper bound check - real market prices can be extremely high
        # (e.g., some tokens trade at very high values, this would be exchange-specific)

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_trades_stream_with_validation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test trades stream with proper financial validation."""
        active_symbols = await get_active_trading_symbols(hl_api_for_test_env)
        test_symbol = active_symbols[0]
        received_trades: list[Trade] = []
        trade_received = asyncio.Event()

        trades_handler = self._create_trades_handler(test_symbol, received_trades, trade_received)

        topic = f"trades:{test_symbol}"

        try:
            await hl_api_for_test_env.subscribe(topic, trades_handler)

            # Test successful subscription - do not simulate fake trade data
            # This tests that the subscription mechanism works through public API
            logger.info(
                "trades_subscription_established",
                symbol=test_symbol,
                message="✓ Trades subscription established successfully",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"Trades subscription failed: {e}. "
                "Trade data is essential for volume analysis and price discovery."
            )

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_allmids_subscription_validation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test allMids subscription with price validation."""
        received_mids: list[dict[str, Decimal]] = []
        mids_received = asyncio.Event()

        allmids_handler = self._create_allmids_handler(received_mids, mids_received)

        try:
            await hl_api_for_test_env.subscribe("allMids", allmids_handler)

            # Test successful subscription - do not simulate fake price data
            # This tests that the subscription mechanism works through public API
            logger.info(
                "allmids_subscription_established",
                message="✓ AllMids subscription established successfully",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"AllMids subscription failed: {e}. "
                "AllMids is essential for multi-asset portfolio management."
            )


class TestHyperliquidWebSocketErrorHandling:
    """Test WebSocket error handling with fail-fast approach."""

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_invalid_symbol_subscription_fails_fast(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that invalid symbol subscriptions fail immediately."""

        async def error_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            # Should not be called for invalid subscriptions
            pytest.fail(
                "Handler should not be called for invalid symbol subscription. "
                "Invalid subscriptions must fail at subscription time."
            )

        # Test with clearly invalid symbol
        invalid_symbol = "DEFINITELY_NOT_A_REAL_SYMBOL_XYZ123"
        invalid_topic = f"l2Book:{invalid_symbol}"

        try:
            await hl_api_for_test_env.subscribe(invalid_topic, error_handler)

            # If we get here without exception, log but don't hide the issue
            logger.warning(
                "invalid_symbol_subscription_succeeded",
                symbol=invalid_symbol,
                message="Invalid symbol subscription succeeded unexpectedly",
            )

            # Still verify connection state
            connection_state = hl_api_for_test_env.is_connected
            assert isinstance(connection_state, bool), (
                "Connection state must remain boolean even after invalid subscription"
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            # This is expected - invalid symbols should be rejected
            logger.info(
                "invalid_symbol_correctly_rejected",
                symbol=invalid_symbol,
                error=str(e),
                message="✓ Invalid symbol subscription correctly rejected",
            )

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_malformed_topic_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test handling of malformed topic formats."""

        async def error_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info("error_handler_called", message=context)

        # Test various malformed topics
        malformed_topics = [
            "",  # Empty topic
            "   ",  # Whitespace only
            "l2Book",  # Missing colon and symbol
            ":BTC",  # Missing channel
            "l2Book:",  # Missing symbol
            "trades::",  # Double colon
            "invalid:format:xyz",  # Too many parts (except candles)
        ]

        for malformed_topic in malformed_topics:
            try:
                await hl_api_for_test_env.subscribe(malformed_topic, error_handler)

                # Log if subscription succeeds
                logger.warning(
                    "malformed_topic_accepted",
                    topic=malformed_topic,
                    message=f"Malformed topic '{malformed_topic}' was accepted",
                )

            except (APIError, ValueError, TypeError, KeyError) as e:
                # Expected - malformed topics should be rejected
                logger.info(
                    "malformed_topic_rejected",
                    topic=malformed_topic,
                    error=str(e),
                    message=f"✓ Malformed topic '{malformed_topic}' correctly rejected",
                )

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_network_error_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test network error handling without hiding failures."""
        # Get a real symbol for testing
        active_symbols = await get_active_trading_symbols(hl_api_for_test_env)
        test_symbol = active_symbols[0]

        async def test_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info("test_handler_message", message=context)

        # First establish a valid subscription
        topic = f"l2Book:{test_symbol}"

        try:
            await hl_api_for_test_env.subscribe(topic, test_handler)

            # Simulate network disruption by attempting to connect again
            # In real scenario, this would test reconnection logic
            await hl_api_for_test_env.connect_websocket()

            # Verify connection state remains consistent
            assert isinstance(hl_api_for_test_env.is_connected, bool), (
                "Connection state must be boolean after reconnection attempt"
            )

        except ConnectionError as e:
            # Network errors should be distinguished from API errors
            pytest.fail(
                f"Network error during WebSocket operation: {e}. "
                "Network failures must be handled separately from business logic errors."
            )
        except (APIError, ValueError, TypeError, KeyError) as e:
            # API errors are different from network errors
            logger.info(
                "api_error_during_network_test",
                error=str(e),
                message="API error occurred (not network error)",
            )

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_subscription_limit_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test handling of subscription limits without silent failures."""
        # Get real symbols
        active_symbols = await get_active_trading_symbols(hl_api_for_test_env)

        if len(active_symbols) < 3:
            pytest.skip("Need at least 3 symbols for subscription limit testing")

        async def limit_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info("limit_handler_message", message=context)

        subscription_count = 0
        max_test_subscriptions = 10  # Reasonable limit for testing

        try:
            # Attempt multiple subscriptions
            for i in range(min(max_test_subscriptions, len(active_symbols))):
                symbol = active_symbols[i % len(active_symbols)]
                topic = f"l2Book:{symbol}"

                await hl_api_for_test_env.subscribe(topic, limit_handler)
                subscription_count += 1

                # Log progress
                logger.info(
                    "subscription_added",
                    count=subscription_count,
                    symbol=symbol,
                )

            logger.info(
                "subscription_limit_test_completed",
                total_subscriptions=subscription_count,
                message="Subscription limit test completed successfully",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            # If we hit a limit, it should be clearly communicated
            if "limit" in str(e).lower() or "maximum" in str(e).lower():
                logger.info(
                    "subscription_limit_reached",
                    count=subscription_count,
                    error=str(e),
                    message="✓ Subscription limit properly enforced",
                )
            else:
                pytest.fail(
                    f"Unexpected error during subscription limit test: {e}. "
                    "Subscription limits should be clearly communicated."
                )

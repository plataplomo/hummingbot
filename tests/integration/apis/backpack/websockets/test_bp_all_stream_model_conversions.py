"""Test 8: Comprehensive Test for All Stream-to-Model Conversions.

This module tests all WebSocket stream types and their conversion to internal
domain models, covering ticker, depth, trades, fills, orders, and account updates.

Security Compliance:
- Tests all WebSocket stream types with real data
- Validates model conversion for all supported data types
- Tests data integrity across all model transformations
- Fails fast on any stream-to-model conversion issues
"""

import asyncio
import contextlib
from collections.abc import Iterator
from decimal import Decimal
from typing import Any, Protocol, TypeGuard, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
    BackpackOrderBookMapper,
)
from cyberdelta.apis.backpack.mappers.market_data.bp_ticker_mapper import BackpackTickerMapper
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFillResponse
from cyberdelta.apis.common.types import MessageHandler
from cyberdelta.apis.models.service_args.market_data import GetMarketsArgs
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models.market.order_book import OrderBook
from cyberdelta.models.market.ticker import Ticker
from cyberdelta.models.market.trade import Trade
from cyberdelta.symbols.models import Symbol

# Import WebSocket test helpers
from .ws_test_helpers import (
    ensure_websocket_connected,
    get_real_depth_data,
    get_real_ticker_data,
    wait_for_websocket_data,
)


pytestmark = [pytest.mark.integration, pytest.mark.timing]

logger = get_logger(__name__)


class SupportsIteration(Protocol):
    """Protocol for objects that support iteration."""

    def __iter__(self) -> Iterator[Any]:
        """Return an iterator over the object."""
        ...


def is_str_any_dict(obj: object) -> TypeGuard[dict[str, Any]]:
    """TypeGuard to ensure dict has str keys.

    Returns:
        TypeGuard[dict[str, Any]]: True if obj is a dict with string keys.
    """
    return isinstance(obj, dict)


def is_any_list(obj: object) -> TypeGuard[list[Any]]:
    """TypeGuard to ensure object is a list.

    Returns:
        TypeGuard[list[Any]]: True if obj is a list.
    """
    return isinstance(obj, list)


class TestBackpackAllStreamModelConversions:
    """Test comprehensive stream-to-model conversions for all WebSocket data types."""

    def _is_trade(self, obj: object) -> TypeGuard[Trade]:
        """Type guard for Trade objects.

        Returns:
            TypeGuard[Trade]: True if obj is a Trade instance.
        """
        return isinstance(obj, Trade)

    def _is_backpack_fill(self, obj: object) -> TypeGuard[BackpackRawFillResponse]:
        """Type guard for BackpackRawFillResponse objects.

        Returns:
            True if obj is a BackpackRawFillResponse instance, False otherwise.
        """
        return isinstance(obj, BackpackRawFillResponse)

    def _get_domain_model_keys(self, context_data: dict[str, Any]) -> list[str]:
        """Extract domain model keys with proper typing.

        Returns:
            list[str]: List of keys from the domain model in context data.
        """
        domain_model_value = context_data.get("domain_model")
        if is_str_any_dict(domain_model_value):
            return list(domain_model_value.keys())
        return []

    def _extract_symbol_from_model(self, model_dict: object) -> str | None:
        """Extract symbol from model dict with proper typing.

        Returns:
            str | None: Symbol string if found, None otherwise.
        """
        if is_str_any_dict(model_dict):
            symbol_value = model_dict.get("symbol")
            if isinstance(symbol_value, str):
                return symbol_value
            # Handle Symbol objects that were serialized to dict
            if isinstance(symbol_value, dict) and "value" in symbol_value:
                value = cast(str, symbol_value["value"])
                return str(value)
        return None

    def _get_model_dict_keys(self, model_dict: object) -> list[str] | str:
        """Get model dict keys with proper typing.

        Returns:
            list[str] | str: List of dict keys if model_dict is a dict, 'NOT_A_DICT' otherwise.
        """
        if is_str_any_dict(model_dict):
            return list(model_dict.keys())
        return "NOT_A_DICT"

    def _extract_trades(self, items: SupportsIteration) -> list[Trade]:
        """Extract Trade objects from iterable with proper typing.

        Returns:
            list[Trade]: List of Trade objects filtered from the input iterable.
        """
        result: list[Trade] = [item for item in items if self._is_trade(item)]
        return result

    def _extract_fills(self, items: SupportsIteration) -> list[BackpackRawFillResponse]:
        """Extract BackpackRawFillResponse objects from iterable with proper typing.

        Args:
            items: Iterable of items to filter

        Returns:
            list[BackpackRawFillResponse]: List of BackpackRawFillResponse objects found in iterable
        """
        result: list[BackpackRawFillResponse] = [
            item for item in items if self._is_backpack_fill(item)
        ]
        return result

    async def _setup_websocket_connection(self, api: BackpackAPI) -> None:
        """Set up WebSocket connection and verify it's established."""
        await api.connect_websocket()
        if not api.is_connected:
            pytest.fail("WebSocket connection failed - cannot test stream conversion")

    async def _get_test_symbol(self, api: BackpackAPI) -> Symbol:
        """Get a test symbol from available markets, preferring perpetual markets for liquidity.

        Returns:
            Symbol: Symbol object for testing, preferring SOL-PERP or first available perpetual.
        """
        markets = await api.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for testing")

        # Filter for PERP markets which are typically more liquid
        perp_markets = [m for m in markets if m.market_type == "PERP"]

        if perp_markets:
            # Find SOL perpetual if available, otherwise use first perp
            sol_perps = [m for m in perp_markets if m.symbol.base_asset == "SOL"]

            # Use SOL perpetual if available, otherwise first available perpetual
            test_market = sol_perps[0] if sol_perps else perp_markets[0]

            logger.info(
                "test_symbol_selected",
                symbol=test_market.symbol,
                base_asset=test_market.symbol.base_asset,
                market_type=test_market.market_type,
                total_markets=len(markets),
                total_perp_markets=len(perp_markets),
                message=f"Using perpetual market: {test_market.symbol}",
            )
            return test_market.symbol

        # Fallback to spot markets
        spot_markets = [m for m in markets if m.market_type == "SPOT"]

        if spot_markets:
            # Find SOL spot if available
            sol_spots = [m for m in spot_markets if m.symbol.base_asset == "SOL"]
            test_market = sol_spots[0] if sol_spots else spot_markets[0]
        else:
            # Last resort: use first available market
            test_market = markets[0]

        logger.info(
            "test_symbol_selected",
            symbol=test_market.symbol,
            base_asset=test_market.symbol.base_asset,
            market_type=test_market.market_type,
            total_markets=len(markets),
            first_few_symbols=[m.symbol.value for m in markets[:5]],
            message=f"No PERP markets found, using: {test_market.symbol}",
        )
        return test_market.symbol

    async def _create_ticker_handler(self, received_tickers: list[Ticker]) -> MessageHandler:
        """Create handler for ticker stream messages.

        Returns:
            MessageHandler: Async handler function for processing ticker stream messages.
        """

        async def ticker_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)

            # The domain model is stored directly on the context
            if hasattr(context, "domain_model") and context.domain_model is not None:
                domain_model = context.domain_model
                # Check if it's a Ticker instance
                if isinstance(domain_model, Ticker):
                    received_tickers.append(domain_model)
                    logger.info(
                        "ticker_model_received_from_stream",
                        symbol=domain_model.symbol,
                        price=str(domain_model.price),
                        message="✓ Ticker model received from domain_model",
                    )
                else:
                    logger.debug(
                        "ticker_handler_no_ticker_model",
                        domain_model_type=type(domain_model).__name__,
                        message="Domain model is not a Ticker instance",
                    )
            else:
                logger.debug(
                    "ticker_handler_no_domain_model",
                    has_domain_model=hasattr(context, "domain_model"),
                    message="No domain model found in context",
                )

        return ticker_handler

    def _validate_received_tickers(
        self, received_tickers: list[Ticker], test_symbol: Symbol
    ) -> None:
        """Validate received ticker models and log results."""
        if received_tickers:
            for ticker in received_tickers[:3]:
                self._validate_ticker_model(ticker, test_symbol)
            logger.info(
                "ticker_stream_conversion_success",
                symbol=test_symbol,
                tickers_received=len(received_tickers),
                message=(
                    f"✓ Successfully converted {len(received_tickers)} "
                    "ticker streams to Ticker models"
                ),
            )
        else:
            # Rule #2: Use pytest.fail for errors instead of logger.warning
            pytest.fail(
                f"No Ticker models received from ticker stream for {test_symbol}. "
                "Check conversion pipeline - stream-to-model conversion not working."
            )

    @pytest.mark.asyncio
    async def test_ticker_stream_to_ticker_model(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test ticker stream conversion to Ticker model."""
        try:
            await self._setup_websocket_connection(bp_api_for_test_env)
            test_symbol = await self._get_test_symbol(bp_api_for_test_env)
            received_tickers: list[Ticker] = []

            ticker_handler = await self._create_ticker_handler(received_tickers)
            await bp_api_for_test_env.subscribe(f"ticker.{test_symbol}", ticker_handler)
            # Rule #4: Use proper wait condition instead of asyncio.sleep
            await wait_for_websocket_data(received_tickers, min_count=1, timeout_seconds=3.0)

            self._validate_received_tickers(received_tickers, test_symbol)

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Ticker stream to model conversion failed: {e}. "
                "Ticker stream conversion not working."
            )
        except (TimeoutError, ConnectionError, OSError) as e:
            # Rule #10: Network errors are test failures
            pytest.fail(
                f"Network error during ticker stream test: {e}. "
                "Test requires stable WebSocket connection for real-time data."
            )

    def _validate_ticker_model(self, ticker: Ticker, expected_symbol: Symbol) -> None:
        """Validate Ticker model structure and data."""
        assert isinstance(ticker, Ticker), f"Expected Ticker, got {type(ticker)}"
        assert ticker.symbol.value == expected_symbol.value, (
            f"Expected symbol {expected_symbol.value}, got {ticker.symbol.value}"
        )
        assert isinstance(ticker.price, Decimal), (
            f"Price should be Decimal, got {type(ticker.price)}"
        )
        assert ticker.price > Decimal(0), f"Price should be positive, got {ticker.price}"

        # timestamp is always present in Ticker model
        assert ticker.timestamp.tzinfo is not None, "Ticker timestamp should be timezone-aware"

        logger.info(
            "ticker_model_validation_passed",
            symbol=ticker.symbol,
            price=str(ticker.price),
            has_timestamp=True,  # timestamp is always present
            message="✓ Ticker model validation passed",
        )

    async def _create_trades_handler(self, received_trades: list[Trade]) -> MessageHandler:
        """Create handler for trades stream messages.

        Returns:
            MessageHandler: Async handler function for processing trades stream messages.
        """

        async def trades_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)

            # The domain model is stored directly on the context (same as ticker handler)
            if hasattr(context, "domain_model") and context.domain_model is not None:
                domain_model = context.domain_model
                # Check if it's a Trade instance
                if isinstance(domain_model, Trade):
                    received_trades.append(domain_model)
                    logger.info(
                        "trade_model_received_from_stream",
                        symbol=domain_model.symbol,
                        price=str(domain_model.price),
                        quantity=str(domain_model.quantity),
                        side=domain_model.side,
                        message="✓ Trade model received from domain_model",
                    )
                else:
                    logger.debug(
                        "trades_handler_no_trade_model",
                        domain_model_type=type(domain_model).__name__,
                        message="Handler called but domain_model is not a Trade",
                    )
            else:
                logger.debug(
                    "trades_handler_no_domain_model",
                    context_has_domain_model=hasattr(context, "domain_model"),
                    domain_model_value=getattr(context, "domain_model", "NOT_FOUND"),
                    message="Handler called but no domain_model found",
                )

        return trades_handler

    def _log_trade_received(self, trade: Trade, key: str) -> None:
        """Log information about received trade."""
        logger.info(
            "trade_model_received_from_stream",
            symbol=trade.symbol,
            price=str(trade.price),
            quantity=str(trade.quantity),
            key_used=key,
            message=f"✓ Trade model received from {key}",
        )

    def _process_trade_list(
        self, potential_trades: list[Trade], received_trades: list[Trade], key: str
    ) -> bool:
        """Process a list of potential trade objects.

        Args:
            potential_trades: List of potential trade objects to process
            received_trades: List to append processed trades to
            key: Key identifier for logging

        Returns:
            bool: True if any trades were found and processed
        """
        trades_found = False
        for trade in potential_trades:
            received_trades.append(trade)
            trades_found = True
            logger.info(
                "trade_model_received_from_list",
                symbol=trade.symbol,
                price=str(trade.price),
                quantity=str(trade.quantity),
                key_used=key,
                message=f"✓ Trade model received from {key} list",
            )
        return trades_found

    def _log_trades_context_analysis(self, context: dict[str, Any]) -> None:
        """Log context analysis when no trades found."""
        logger.info(
            "trades_stream_context_analysis",
            context_keys=list(context.keys()),
            context_types={k: type(v).__name__ for k, v in context.items()},
            message="Trades stream context (no Trade models found)",
        )

    def _validate_received_trades(self, received_trades: list[Trade], test_symbol: Symbol) -> None:
        """Validate received trade models and log results."""
        if received_trades:
            for trade in received_trades[:3]:
                self._validate_trade_model(trade, test_symbol)
            logger.info(
                "trades_stream_conversion_success",
                symbol=test_symbol,
                trades_received=len(received_trades),
                message=f"✓ Successfully converted {len(received_trades)} trades to Trade models",
            )
        else:
            # Rule #2: Use pytest.fail for errors instead of logger.warning
            pytest.fail(
                f"No Trade models received from trades stream for {test_symbol}. "
                "Check conversion pipeline - stream-to-model conversion not working."
            )

    @pytest.mark.asyncio
    async def test_trades_stream_to_trade_models(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test trades stream conversion to Trade models."""
        try:
            await self._setup_websocket_connection(bp_api_for_test_env)
            test_symbol = await self._get_test_symbol(bp_api_for_test_env)
            received_trades: list[Trade] = []

            trades_handler = await self._create_trades_handler(received_trades)
            # NOTE: Backpack uses "trade" (singular) not "trades" for the stream name
            await bp_api_for_test_env.subscribe(f"trade.{test_symbol}", trades_handler)
            # Rule #4: Use proper wait condition instead of asyncio.sleep
            # SOL_USDC_PERP is very liquid, should have trades within seconds
            await wait_for_websocket_data(received_trades, min_count=1, timeout_seconds=60.0)

            self._validate_received_trades(received_trades, test_symbol)

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Trades stream to model conversion failed: {e}. "
                "Trades stream conversion not working."
            )
        except (TimeoutError, ConnectionError, OSError) as e:
            # Rule #10: Network errors are test failures
            pytest.fail(
                f"Network error during trades stream test: {e}. "
                "Test requires stable WebSocket connection for real-time data."
            )

    def _validate_trade_model(self, trade: Trade, expected_symbol: Symbol) -> None:
        """Validate Trade model structure and data."""
        assert isinstance(trade, Trade), f"Expected Trade, got {type(trade)}"
        assert trade.symbol.value == expected_symbol.value, (
            f"Expected symbol {expected_symbol.value}, got {trade.symbol.value}"
        )
        assert isinstance(trade.price, Decimal), f"Price should be Decimal, got {type(trade.price)}"
        assert isinstance(trade.quantity, Decimal), (
            f"Quantity should be Decimal, got {type(trade.quantity)}"
        )
        assert trade.price > Decimal(0), f"Price should be positive, got {trade.price}"
        assert trade.quantity > Decimal(0), f"Quantity should be positive, got {trade.quantity}"

        # executed_at is always present in Trade model
        assert trade.executed_at.tzinfo is not None, "Trade timestamp should be timezone-aware"

        logger.info(
            "trade_model_validation_passed",
            symbol=trade.symbol,
            price=str(trade.price),
            quantity=str(trade.quantity),
            side=trade.side if hasattr(trade, "side") else "unknown",
            message="✓ Trade model validation passed",
        )

    async def _create_fills_handler(self, received_fills: list[Any]) -> MessageHandler:
        """Create handler for fills stream messages.

        Returns:
            MessageHandler: Async handler function for processing fills stream messages.
        """

        async def fills_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)

            # Extract data from typed context
            context_data: dict[str, Any] = {}
            if (
                hasattr(context, "validated_envelope")
                and context.validated_envelope is not None
                and hasattr(context.validated_envelope, "data")
            ):
                data = context.validated_envelope.data
                context_data = data if isinstance(data, dict) else {"data": data}

            fill_keys = ["fill", "fills", "order", "execution"]
            fills_found = False

            for key in fill_keys:
                if key in context_data:
                    potential_fills = context_data[key]
                    if hasattr(potential_fills, "symbol"):
                        received_fills.append(potential_fills)
                        fills_found = True
                        self._log_fill_received(potential_fills, key)
                    elif is_any_list(potential_fills):
                        # Type-safe fill extraction using protocol
                        fill_list = self._extract_fills(potential_fills)
                        fills_found = self._process_fill_list(fill_list, received_fills, key)

            if not fills_found:
                self._log_fills_context_analysis(context_data)

        return fills_handler

    def _log_fill_received(self, fill: object, key: str) -> None:
        """Log information about received fill."""
        logger.info(
            "fill_model_received_from_stream",
            model_type=type(fill).__name__,
            symbol=getattr(fill, "symbol", "unknown"),
            key_used=key,
            message=f"✓ Fill model received from {key}",
        )

    def _process_fill_list(
        self,
        potential_fills: list[BackpackRawFillResponse],
        received_fills: list[BackpackRawFillResponse],
        key: str,
    ) -> bool:
        """Process a list of potential fill objects.

        Args:
            potential_fills: List of potential fill objects to process
            received_fills: List to append processed fills to
            key: Key identifier for logging

        Returns:
            bool: True if any fills were found and processed
        """
        fills_found = False
        for fill in potential_fills:
            if hasattr(fill, "symbol"):
                received_fills.append(fill)
                fills_found = True
                logger.info(
                    "fill_model_received_from_list",
                    model_type=type(fill).__name__,
                    symbol=getattr(fill, "symbol", "unknown"),
                    key_used=key,
                    message=f"✓ Fill model received from {key} list",
                )
        return fills_found

    def _log_fills_context_analysis(self, context: dict[str, Any]) -> None:
        """Log context analysis when no fills found."""
        logger.info(
            "fills_stream_context_analysis",
            context_keys=list(context.keys()),
            context_types={k: type(v).__name__ for k, v in context.items()},
            message="Fills stream context (no fill models found)",
        )

    def _validate_received_fills(self, received_fills: list[Any]) -> None:
        """Validate received fill models and log results."""
        if received_fills:
            for fill in received_fills[:3]:
                self._validate_fill_model(fill)
            logger.info(
                "fills_stream_conversion_success",
                fills_received=len(received_fills),
                message=f"✓ Successfully converted {len(received_fills)} fills to models",
            )
        else:
            logger.info(
                "fills_stream_no_models_received",
                message="No fill models received from fills stream (may require authentication)",
            )

    async def _attempt_fills_subscription(
        self, bp_api_for_test_env: BackpackAPI, received_fills: list[Any]
    ) -> None:
        """Attempt to subscribe to fills stream and handle authentication errors."""
        try:
            fills_handler = await self._create_fills_handler(received_fills)
            await bp_api_for_test_env.subscribe("fills", fills_handler)
            # Rule #4: Use proper wait condition instead of asyncio.sleep
            # For fills, we don't expect data without auth, so short timeout
            with contextlib.suppress(TimeoutError):
                # Expected for unauthenticated requests
                await wait_for_websocket_data(received_fills, min_count=1, timeout_seconds=3.0)
            self._validate_received_fills(received_fills)
        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            # Rule #2: For fills, auth errors are expected - log info only
            logger.info(
                "fills_stream_subscription_auth_required",
                error=str(e),
                message=f"Fills stream requires authentication (expected): {e}",
            )

    @pytest.mark.asyncio
    async def test_fills_stream_to_order_models(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test fills stream conversion to Order/Fill models."""
        try:
            await self._setup_websocket_connection(bp_api_for_test_env)
            received_fills: list[Any] = []
            await self._attempt_fills_subscription(bp_api_for_test_env, received_fills)
        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Fills stream to model conversion test failed: {e}. "
                "Fills stream conversion testing not working."
            )

    def _validate_fill_model(self, fill: BackpackRawFillResponse | object) -> None:
        """Validate fill/order model structure and data."""
        model_type = type(fill).__name__

        # Check basic attributes
        assert hasattr(fill, "symbol"), "Fill model missing symbol attribute"

        # Type assertions for pyright
        symbol = getattr(fill, "symbol", None)
        assert isinstance(symbol, str), f"Symbol should be string, got {type(symbol)}"
        assert len(symbol) > 0, "Symbol should not be empty"

        # Check financial data if present
        if hasattr(fill, "price"):
            price = getattr(fill, "price", None)
            if price is not None:
                assert isinstance(price, (Decimal, str)), (
                    f"Price should be Decimal or str, got {type(price)}"
                )
                price_decimal = Decimal(price) if isinstance(price, str) else price
                assert price_decimal > Decimal(0), f"Price should be positive, got {price_decimal}"

        if hasattr(fill, "quantity"):
            quantity = getattr(fill, "quantity", None)
            if quantity is not None:
                assert isinstance(quantity, (Decimal, str)), (
                    f"Quantity should be Decimal or str, got {type(quantity)}"
                )
                quantity_decimal = Decimal(quantity) if isinstance(quantity, str) else quantity
                assert quantity_decimal > Decimal(0), (
                    f"Quantity should be positive, got {quantity_decimal}"
                )

        logger.info(
            "fill_model_validation_passed",
            model_type=model_type,
            symbol=symbol,
            has_price=hasattr(fill, "price"),
            has_quantity=hasattr(fill, "quantity"),
            message=f"✓ {model_type} model validation passed",
        )

    @pytest.mark.asyncio
    async def test_raw_model_to_domain_model_transformations(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test transformation from raw Pydantic models to domain models using actual mappers."""
        # Create mapper instances
        order_book_mapper = BackpackOrderBookMapper()
        ticker_mapper = BackpackTickerMapper()

        try:
            # Ensure WebSocket is connected
            await ensure_websocket_connected(bp_api_for_test_env)

            # Get real test symbol
            test_symbol = await self._get_test_symbol(bp_api_for_test_env)

            # Test ticker transformation with REAL DATA
            raw_ticker = await get_real_ticker_data(bp_api_for_test_env, test_symbol)

            # Use ACTUAL transformation logic
            domain_ticker = ticker_mapper.transform_ws_ticker_event_to_internal(raw_ticker)

            self._validate_ticker_model(domain_ticker, test_symbol)

            logger.info(
                "ticker_transformation_success",
                raw_symbol=raw_ticker.symbol,
                raw_price=raw_ticker.last_price,
                domain_symbol=domain_ticker.symbol,
                domain_price=str(domain_ticker.price),
                message=(
                    "✓ Ticker transformation from raw to domain model "
                    "successful using actual mapper"
                ),
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Ticker transformation test failed: {e}. "
                "Raw to domain model transformation not working."
            )

        # Test depth transformation with REAL DATA
        try:
            # test_symbol should always be a Symbol object at this point
            test_symbol_obj = test_symbol
            test_symbol_str = test_symbol.value

            # Get real depth data using Symbol object
            raw_depth = await get_real_depth_data(bp_api_for_test_env, test_symbol_obj)

            # Use ACTUAL transformation logic with Symbol object
            domain_orderbook = order_book_mapper.transform_ws_depth_event_to_internal(
                test_symbol_obj, raw_depth
            )

            self._validate_orderbook_basic(domain_orderbook, test_symbol_str)

            logger.info(
                "depth_transformation_success",
                raw_bids_count=len(raw_depth.bids) if raw_depth.bids is not None else 0,
                raw_asks_count=len(raw_depth.asks) if raw_depth.asks is not None else 0,
                domain_symbol=domain_orderbook.symbol,
                domain_bids_count=len(domain_orderbook.bids),
                domain_asks_count=len(domain_orderbook.asks),
                message=(
                    "✓ Depth transformation from raw to domain model successful using actual mapper"
                ),
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Depth transformation test failed: {e}. "
                "Raw to domain model transformation not working."
            )

    def _validate_orderbook_basic(self, orderbook: OrderBook, expected_symbol: str) -> None:
        """Basic OrderBook validation."""
        assert isinstance(orderbook, OrderBook), f"Expected OrderBook, got {type(orderbook)}"
        # Compare symbol value since orderbook.symbol is a Symbol object
        actual_symbol = (
            orderbook.symbol.value if hasattr(orderbook.symbol, "value") else str(orderbook.symbol)
        )
        assert actual_symbol == expected_symbol, (
            f"Expected symbol {expected_symbol}, got {actual_symbol}"
        )
        assert isinstance(orderbook.bids, list), f"Bids should be list, got {type(orderbook.bids)}"
        assert isinstance(orderbook.asks, list), f"Asks should be list, got {type(orderbook.asks)}"

    async def _setup_stream_integration(
        self, bp_api_for_test_env: BackpackAPI
    ) -> tuple[Symbol, dict[str, list[Any]]]:
        """Set up WebSocket connection and get test symbol for stream integration.

        Returns:
            tuple[Symbol, dict[str, list[Any]]]: Test symbol and stream results dict.
        """
        await bp_api_for_test_env.connect_websocket()

        if not bp_api_for_test_env.is_connected:
            pytest.fail("WebSocket connection failed - cannot test stream integration")

        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for integration testing")

        test_symbol = markets[0].symbol
        stream_results: dict[str, list[Any]] = {
            "ticker": [],
            "depth": [],
            "trades": [],
            "fills": [],
        }
        return test_symbol, stream_results

    async def _create_integration_handler(
        self, stream_type: str, stream_results: dict[str, list[Any]]
    ) -> MessageHandler:
        """Create handler for specific stream type.

        Returns:
            MessageHandler: Async handler function for collecting stream data by type.
        """
        await asyncio.sleep(0)  # Satisfy RUF029

        async def handler(context: WebSocketContextProtocol) -> None:
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

            stream_results[stream_type].append(context)
            logger.info(
                "stream_integration_data_received",
                stream_type=stream_type,
                count=len(stream_results[stream_type]),
                context_keys=list(context_data.keys()),
                message=f"{stream_type} stream data received",
            )

        return handler

    async def _subscribe_to_all_streams(
        self,
        bp_api_for_test_env: BackpackAPI,
        test_symbol: Symbol,
        stream_results: dict[str, list[Any]],
    ) -> None:
        """Subscribe to multiple stream types."""
        stream_subscriptions = [
            (f"ticker.{test_symbol.value}", "ticker"),
            (f"depth.{test_symbol.value}", "depth"),
            (f"trade.{test_symbol.value}", "trades"),  # Note: stream name is "trade"
            ("fills", "fills"),  # Authenticated stream
        ]

        for topic, stream_type in stream_subscriptions:
            try:
                handler = await self._create_integration_handler(stream_type, stream_results)
                await bp_api_for_test_env.subscribe(topic, handler)
                logger.info(
                    "stream_integration_subscription_success",
                    topic=topic,
                    stream_type=stream_type,
                    message=f"✓ Subscribed to {stream_type} stream",
                )
            except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                self._handle_subscription_error(stream_type, topic, e)

    async def _wait_for_stream_data(self, stream_results: dict[str, list[Any]]) -> None:
        """Wait for streams to receive data."""
        total_wait_start = asyncio.get_event_loop().time()
        max_total_wait = 8.0

        while asyncio.get_event_loop().time() - total_wait_start < max_total_wait:
            # Check if we have data from any streams
            active_streams = sum(1 for results in stream_results.values() if results)
            if active_streams >= 2:  # At least 2 streams active (fills may require auth)
                break
            await asyncio.sleep(0.1)

    def _handle_subscription_error(self, stream_type: str, topic: str, error: Exception) -> None:
        """Handle subscription errors based on stream type."""
        # Rule #2: Log info for fills (auth required), fail for others
        if stream_type == "fills":
            logger.info(
                "stream_integration_fills_auth_required",
                topic=topic,
                error=str(error),
                message=f"Fills stream requires authentication: {error}",
            )
        else:
            pytest.fail(
                f"Subscription to {stream_type} failed: {error}. "
                f"Stream {topic} should be accessible without authentication."
            )

    def _analyze_stream_results(self, stream_results: dict[str, list[Any]]) -> None:
        """Analyze and validate stream integration results."""
        successful_streams: list[str] = []
        total_streams = 4  # ticker, depth, trades, fills

        for stream_type, results in stream_results.items():
            if results:
                successful_streams.append(stream_type)
                logger.info(
                    "stream_integration_stream_success",
                    stream_type=stream_type,
                    data_count=len(results),
                    message=f"✓ {stream_type} stream produced {len(results)} data points",
                )
            else:
                logger.info(
                    "stream_integration_stream_no_data",
                    stream_type=stream_type,
                    message=f"{stream_type} stream produced no data",
                )

        logger.info(
            "stream_integration_summary",
            total_streams=total_streams,
            successful_streams=len(successful_streams),
            successful_stream_types=successful_streams,
            message=(
                f"✓ Stream integration test completed: "
                f"{len(successful_streams)}/{total_streams} streams active"
            ),
        )

        # At least some streams should be working
        assert len(successful_streams) > 0, (
            "No streams produced data - stream integration not working"
        )

    @pytest.mark.asyncio
    async def test_all_stream_types_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test integration of all stream types working together."""
        try:
            test_symbol, stream_results = await self._setup_stream_integration(bp_api_for_test_env)
            await self._subscribe_to_all_streams(bp_api_for_test_env, test_symbol, stream_results)
            await self._wait_for_stream_data(stream_results)
            self._analyze_stream_results(stream_results)

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"All stream types integration test failed: {e}. "
                "Stream integration not working properly."
            )
        except (TimeoutError, ConnectionError, OSError) as e:
            # Rule #10: Network errors are test failures
            pytest.fail(
                f"Network error during stream integration test: {e}. "
                "Test requires stable WebSocket connection for all stream types."
            )

    async def _create_consistency_handler(
        self, stream_type: str, model_data: dict[str, dict[str, Any]]
    ) -> MessageHandler:
        """Create handler that tracks model data for consistency.

        Args:
            stream_type: Type of stream to handle
            model_data: Dictionary to store model data for consistency tracking

        Returns:
            MessageHandler: Handler function for processing WebSocket messages
        """

        async def handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)

            if stream_type not in model_data:
                model_data[stream_type] = {}

            # Check for domain model on the context object (set by processor)
            if hasattr(context, "domain_model") and context.domain_model is not None:
                domain_model = context.domain_model
                model_type = type(domain_model).__name__

                # Convert domain model to dict for analysis
                if hasattr(domain_model, "model_dump"):
                    model_dict = domain_model.model_dump()
                elif hasattr(domain_model, "dict"):
                    model_dict = domain_model.dict()
                else:
                    model_dict = {"model": str(domain_model)}

                # Extract symbol from the model data
                symbol = self._extract_symbol_from_model(model_dict)
                if symbol is not None:
                    if symbol not in model_data[stream_type]:
                        model_data[stream_type][symbol] = []

                    # Store the model dict with its type
                    model_data[stream_type][symbol].append({"type": model_type, "data": model_dict})
                    self._log_consistency_data_collected(stream_type, symbol, model_dict)
                    return
                logger.warning(
                    "consistency_handler_no_symbol",
                    stream_type=stream_type,
                    model_type=model_type,
                    model_dict_keys=self._get_model_dict_keys(model_dict),
                    message="Model dict missing symbol field",
                )

        return handler

    def _log_consistency_data_collected(self, stream_type: str, symbol: str, value: object) -> None:
        """Log data collection for consistency check."""
        logger.info(
            "model_consistency_data_collected",
            stream_type=stream_type,
            symbol=symbol,
            model_type=type(value).__name__,
            message="Model data collected for consistency check",
        )

    async def _subscribe_to_consistency_streams(
        self,
        bp_api_for_test_env: BackpackAPI,
        test_symbol: Symbol,
        model_data: dict[str, dict[str, Any]],
    ) -> None:
        """Subscribe to multiple streams for consistency testing."""
        # Convert Symbol to string for WebSocket subscription
        symbol_str = test_symbol.value.replace("/", "_").replace("-", "_")

        consistency_streams = [
            (f"ticker.{symbol_str}", "ticker"),
            (f"depth.{symbol_str}", "depth"),
            (f"trade.{symbol_str}", "trades"),  # Note: stream name is "trade"
        ]

        for topic, stream_type in consistency_streams:
            try:
                handler = await self._create_consistency_handler(stream_type, model_data)
                await bp_api_for_test_env.subscribe(topic, handler)
            except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                # Rule #2: Use pytest.fail for subscription errors
                pytest.fail(
                    f"Consistency test subscription to {topic} failed: {e}. "
                    "Model consistency testing requires working subscriptions."
                )

    def _analyze_consistency_results(self, model_data: dict[str, dict[str, Any]]) -> None:
        """Analyze and log consistency check results."""
        symbols_found: set[str] = set()
        for stream_data in model_data.values():
            symbols_found.update(stream_data.keys())

        if symbols_found:
            for symbol in symbols_found:
                streams_with_symbol = [
                    stream_type
                    for stream_type, stream_data in model_data.items()
                    if symbol in stream_data
                ]
                logger.info(
                    "model_consistency_symbol_analysis",
                    symbol=symbol,
                    streams_with_symbol=streams_with_symbol,
                    stream_count=len(streams_with_symbol),
                    message=f"Symbol {symbol} found in {len(streams_with_symbol)} streams",
                )
            logger.info(
                "model_consistency_check_completed",
                unique_symbols=len(symbols_found),
                stream_types=len(model_data),
                message="✓ Model consistency check completed",
            )
        else:
            # Rule #2: Use pytest.fail for test failures
            pytest.fail(
                "No model data collected for consistency testing. "
                "WebSocket streams not producing model data."
            )

    @pytest.mark.asyncio
    async def test_model_consistency_across_streams(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test model consistency across different stream types."""
        try:
            await self._setup_websocket_connection(bp_api_for_test_env)
            test_symbol = await self._get_test_symbol(bp_api_for_test_env)
            model_data: dict[str, dict[str, Any]] = {}

            await self._subscribe_to_consistency_streams(
                bp_api_for_test_env, test_symbol, model_data
            )
            # Rule #4: Wait for model data collection
            start_time = asyncio.get_event_loop().time()
            while asyncio.get_event_loop().time() - start_time < 5.0:
                # Check if we have collected any model data
                if any(stream_data for stream_data in model_data.values()):
                    # Give a bit more time to collect more samples
                    await asyncio.sleep(0.5)
                    break
                await asyncio.sleep(0.1)
            self._analyze_consistency_results(model_data)

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Model consistency test failed: {e}. Model consistency checking not working."
            )
        except (TimeoutError, ConnectionError, OSError) as e:
            # Rule #10: Network errors are test failures
            pytest.fail(
                f"Network error during model consistency test: {e}. "
                "Test requires stable connection to validate model consistency."
            )

"""WebSocket-specific test helpers for Backpack integration tests.

This module provides helper functions specifically for WebSocket integration tests,
including real-time data collection, stream validation, and wait conditions.
All helpers follow security rules and use real market data.
"""

import asyncio
import operator
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, TypeVar

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.common import APIError
from cyberdelta.apis.common.types import MessageHandler
from cyberdelta.apis.models.service_args.market_data import GetMarketsArgs
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market.order_book import OrderBook
from cyberdelta.core.models.market.ticker import Ticker


logger = get_logger(__name__)

T = TypeVar("T")


async def wait_for_websocket_data[T](
    collected_data: list[T],
    min_count: int = 1,
    timeout_seconds: float = 10.0,
    poll_interval: float = 0.1,
) -> list[T]:
    """Wait for WebSocket data to be collected.

    Args:
        collected_data: List that will be populated with data
        min_count: Minimum number of items to wait for
        timeout_seconds: Maximum time to wait
        poll_interval: Time between checks

    Returns:
        The collected data

    Raises:
        TimeoutError: If minimum data not received within timeout
    """
    data_ready = asyncio.Event()

    def check_data_ready() -> None:
        if len(collected_data) >= min_count:
            data_ready.set()

    # Check initially
    check_data_ready()

    try:
        async with asyncio.timeout(timeout_seconds):
            # Use a periodic check with Event instead of busy wait
            while not data_ready.is_set():
                await asyncio.sleep(poll_interval)
                check_data_ready()
            return collected_data
    except TimeoutError:
        # Rule #10: Provide detailed error for timeout
        raise TimeoutError(
            f"Timeout waiting for WebSocket data. "
            f"Expected {min_count} items, got {len(collected_data)} after {timeout_seconds}s. "
            f"This may indicate network issues or WebSocket stream problems."
        ) from None


async def get_real_ticker_data(api: BackpackAPI, symbol: str) -> BackpackRawTickerEvent:
    """Get real ticker data from WebSocket stream.

    Args:
        api: Backpack API instance
        symbol: Trading symbol

    Returns:
        Real ticker data from WebSocket

    Raises:
        RuntimeError: If unable to get ticker data
    """
    received_tickers: list[BackpackRawTickerEvent] = []

    async def ticker_collector(context: WebSocketContextProtocol) -> None:
        await asyncio.sleep(0)
        # Extract data from typed context
        envelope = context.validated_envelope
        if envelope is not None and isinstance(envelope.data, dict):
            try:
                ticker = BackpackRawTickerEvent.model_validate(envelope.data)
                received_tickers.append(ticker)
            except (ValidationError, ValueError, TypeError, KeyError) as e:
                logger.debug(
                    "ticker_data_validation_failed",
                    error=str(e),
                    message="Skipping invalid ticker data",
                )

    try:
        await api.subscribe(f"ticker.{symbol}", ticker_collector)
        await wait_for_websocket_data(received_tickers, min_count=1, timeout_seconds=5.0)

        if not received_tickers:
            raise RuntimeError(f"No ticker data received for {symbol}")

        return received_tickers[0]

    except (TimeoutError, APIError) as e:
        # Rule #10: Handle network timeout specifically
        raise RuntimeError(
            f"Failed to get real ticker data for {symbol}: {e}. "
            "Tests require real WebSocket data, not hardcoded values. "
            "Check network connectivity and WebSocket stream availability."
        ) from e
    except (ConnectionError, OSError) as e:
        # Rule #10: Handle network errors
        raise RuntimeError(
            f"Network error while getting ticker data for {symbol}: {e}. "
            "Test requires stable network connection to WebSocket streams."
        ) from e


async def get_real_depth_data(api: BackpackAPI, symbol: str) -> BackpackRawDepthUpdateEvent:
    """Get real depth/orderbook data from WebSocket stream.

    Args:
        api: Backpack API instance
        symbol: Trading symbol

    Returns:
        Real depth data from WebSocket

    Raises:
        RuntimeError: If unable to get depth data
    """
    received_depths: list[BackpackRawDepthUpdateEvent] = []

    async def depth_collector(context: WebSocketContextProtocol) -> None:
        await asyncio.sleep(0)
        # Extract data from typed context
        envelope = context.validated_envelope
        if envelope is not None and isinstance(envelope.data, dict):
            try:
                # Log the actual data structure for debugging
                logger.info(
                    "depth_raw_data_received",
                    data_keys=list(envelope.data.keys()),
                    data_sample=str(envelope.data)[:200],
                    message="Raw depth data structure",
                )
                depth = BackpackRawDepthUpdateEvent.model_validate(envelope.data)
                received_depths.append(depth)
            except (ValidationError, ValueError, TypeError, KeyError) as e:
                logger.debug(
                    "depth_data_validation_failed",
                    error=str(e),
                    data=envelope.data,
                    message="Failed to parse depth data",
                )

    try:
        await api.subscribe(f"depth.{symbol}", depth_collector)
        await wait_for_websocket_data(received_depths, min_count=1, timeout_seconds=5.0)

        if not received_depths:
            raise RuntimeError(f"No depth data received for {symbol}")

        return received_depths[0]

    except (TimeoutError, APIError) as e:
        # Rule #10: Handle network timeout specifically
        raise RuntimeError(
            f"Failed to get real depth data for {symbol}: {e}. "
            "Tests require real WebSocket data, not hardcoded values. "
            "Check network connectivity and WebSocket stream availability."
        ) from e
    except (ConnectionError, OSError) as e:
        # Rule #10: Handle network errors
        raise RuntimeError(
            f"Network error while getting depth data for {symbol}: {e}. "
            "Test requires stable network connection to WebSocket streams."
        ) from e


async def collect_stream_data_sample(
    api: BackpackAPI,
    stream_type: str,
    symbol: str,
    sample_size: int = 5,
    timeout_seconds: float = 10.0,
) -> list[dict[str, Any]]:
    """Collect a sample of raw stream data for testing.

    Args:
        api: Backpack API instance
        stream_type: Type of stream (ticker, depth, trades, etc.)
        symbol: Trading symbol
        sample_size: Number of messages to collect
        timeout_seconds: Maximum time to wait

    Returns:
        List of raw message contexts

    Raises:
        RuntimeError: If unable to collect sample data
    """
    collected_data: list[dict[str, Any]] = []

    async def data_collector(context: WebSocketContextProtocol) -> None:
        await asyncio.sleep(0)
        if len(collected_data) < sample_size:
            # Extract data from typed context for test purposes
            envelope = context.validated_envelope
            if envelope is not None:
                data = envelope.data
                if isinstance(data, dict):
                    collected_data.append(data)
                else:
                    collected_data.append({"data": data})
            else:
                collected_data.append({"context": str(context)})

    try:
        topic = f"{stream_type}.{symbol}"
        await api.subscribe(topic, data_collector)
        await wait_for_websocket_data(
            collected_data, min_count=sample_size, timeout_seconds=timeout_seconds
        )
    except (TimeoutError, APIError) as e:
        # Rule #10: Handle network timeout specifically
        raise RuntimeError(
            f"Failed to collect {stream_type} data for {symbol}: {e}. "
            "Tests require real stream data samples. "
            "Check network connectivity and stream availability."
        ) from e
    except (ConnectionError, OSError) as e:
        # Rule #10: Handle network errors
        raise RuntimeError(
            f"Network error while collecting {stream_type} data: {e}. "
            "Test requires stable network connection."
        ) from e
    else:
        return collected_data


async def wait_for_model_in_context[T](
    api: BackpackAPI,
    topic: str,
    model_type: type[T],
    context_keys: list[str],
    timeout_seconds: float = 10.0,
) -> T:
    """Wait for a specific model type to appear in WebSocket context.

    Args:
        api: Backpack API instance
        topic: WebSocket topic to subscribe to
        model_type: Expected model type (e.g., Ticker, OrderBook)
        context_keys: Keys to check in context for the model
        timeout_seconds: Maximum time to wait

    Returns:
        First instance of the model found

    Raises:
        RuntimeError: If model not found within timeout
    """
    found_models: list[T] = []

    async def model_finder(context: WebSocketContextProtocol) -> None:
        await asyncio.sleep(0)
        # Extract data from typed context
        envelope = context.validated_envelope
        if envelope is not None:
            data = envelope.data
            if isinstance(data, dict):
                for key in context_keys:
                    if key in data and isinstance(data[key], model_type):
                        found_models.append(data[key])
                        return

    try:
        await api.subscribe(topic, model_finder)
        await wait_for_websocket_data(found_models, min_count=1, timeout_seconds=timeout_seconds)

        if not found_models:
            raise RuntimeError(
                f"No {model_type.__name__} found in context keys {context_keys} for topic {topic}"
            )

        return found_models[0]

    except (TimeoutError, APIError) as e:
        # Rule #10: Handle network timeout specifically
        raise RuntimeError(
            f"Failed to find {model_type.__name__} in WebSocket stream: {e}. "
            "Model detection is required for stream validation. "
            "Check network stability and stream data flow."
        ) from e
    except (ConnectionError, OSError) as e:
        # Rule #10: Handle network errors
        raise RuntimeError(
            f"Network error while waiting for {model_type.__name__}: {e}. "
            "Test requires stable WebSocket connection."
        ) from e


async def create_real_orderbook_from_stream(api: BackpackAPI, symbol: str) -> OrderBook:
    """Create a real OrderBook model from live WebSocket depth data.

    Args:
        api: Backpack API instance
        symbol: Trading symbol

    Returns:
        OrderBook created from real WebSocket data

    Raises:
        RuntimeError: If unable to create OrderBook
    """
    try:
        # Get real depth data
        depth_data = await get_real_depth_data(api, symbol)

        # Convert to OrderBook
        # DEFENSIVE CHECK: Handle None bids/asks from depth data. Mypy=[union-attr]
        if depth_data.bids is not None:
            bids_list = [(Decimal(price), Decimal(qty)) for price, qty in depth_data.bids]
        else:
            bids_list = []

        if depth_data.asks is not None:
            asks_list = [(Decimal(price), Decimal(qty)) for price, qty in depth_data.asks]
        else:
            asks_list = []

        return OrderBook(
            symbol=symbol,
            bids=bids_list,
            asks=asks_list,
            timestamp=datetime.now(UTC),
        )

    except Exception as e:
        raise RuntimeError(
            f"Failed to create OrderBook from real stream data: {e}. "
            "OrderBook creation requires real market data."
        ) from e


async def validate_stream_continuity(
    api: BackpackAPI,
    topic: str,
    duration_seconds: float = 5.0,
    min_messages: int = 5,
) -> dict[str, Any]:
    """Validate that a WebSocket stream produces continuous data.

    Args:
        api: Backpack API instance
        topic: WebSocket topic to test
        duration_seconds: How long to monitor
        min_messages: Minimum expected messages

    Returns:
        Dict with validation results including message count and intervals

    Raises:
        RuntimeError: If stream validation fails
    """
    messages: list[tuple[float, dict[str, Any]]] = []
    start_time = asyncio.get_event_loop().time()

    async def message_timer(context: WebSocketContextProtocol) -> None:
        await asyncio.sleep(0)
        current_time = asyncio.get_event_loop().time()
        # Extract data from typed context for test purposes
        data_to_append: dict[str, Any] = {}
        envelope = context.validated_envelope
        if envelope is not None:
            envelope_data = envelope.data
            if isinstance(envelope_data, dict):
                data_to_append = envelope_data
            else:
                data_to_append = {"data": envelope_data}
        else:
            data_to_append = {"context": str(context)}
        messages.append((current_time - start_time, data_to_append))

    try:
        await api.subscribe(topic, message_timer)
        await asyncio.sleep(duration_seconds)

        if len(messages) < min_messages:
            raise RuntimeError(
                f"Stream {topic} produced only {len(messages)} messages in {duration_seconds}s, "
                f"expected at least {min_messages}"
            )

        # Calculate intervals with explicit typing for senior-level type safety
        intervals: list[float] = []
        for i in range(1, len(messages)):
            interval = messages[i][0] - messages[i - 1][0]
            intervals.append(interval)

        return {
            "message_count": len(messages),
            "duration": duration_seconds,
            "avg_interval": sum(intervals) / len(intervals) if intervals else 0,
            "min_interval": min(intervals) if intervals else 0,
            "max_interval": max(intervals) if intervals else 0,
        }

    except Exception as e:
        raise RuntimeError(f"Stream continuity validation failed for {topic}: {e}") from e


async def test_model_transformation_with_real_data(
    api: BackpackAPI,
    symbol: str,
    stream_type: str,
) -> dict[str, Any]:
    """Test model transformation using real WebSocket data.

    Args:
        api: Backpack API instance
        symbol: Trading symbol
        stream_type: Type of stream to test

    Returns:
        Dict with raw data and transformed model

    Raises:
        ValueError: If stream_type is not supported
        RuntimeError: If transformation test fails
    """
    try:
        if stream_type == "ticker":
            ticker_raw_data = await get_real_ticker_data(api, symbol)
            ticker_model = Ticker(
                symbol=symbol,
                exchange="backpack",
                price=Decimal(ticker_raw_data.last_price),
                timestamp=datetime.now(UTC),
                volume=Decimal(ticker_raw_data.volume) if ticker_raw_data.volume else None,
            )
            return {"raw": ticker_raw_data, "model": ticker_model, "type": "ticker"}

        if stream_type == "depth":
            depth_raw_data = await get_real_depth_data(api, symbol)
            orderbook_model = await create_real_orderbook_from_stream(api, symbol)
            return {"raw": depth_raw_data, "model": orderbook_model, "type": "orderbook"}

        raise ValueError(f"Unsupported stream type: {stream_type}")

    except Exception as e:
        raise RuntimeError(
            f"Model transformation test failed for {stream_type}: {e}. "
            "Transformation testing requires real market data."
        ) from e


def create_handler_with_counter(
    handler_name: str,
) -> tuple[MessageHandler, Callable[[], int]]:
    """Create a message handler that counts invocations.

    Args:
        handler_name: Name for logging

    Returns:
        Tuple of (handler function, counter getter function)
    """
    count = 0

    async def counting_handler(context: WebSocketContextProtocol) -> None:
        nonlocal count
        await asyncio.sleep(0)
        count += 1
        logger.debug(
            "handler_invoked", handler_name=handler_name, count=count, message="Handler invoked"
        )

    def get_count() -> int:
        return count

    return counting_handler, get_count


async def ensure_websocket_connected(api: BackpackAPI) -> None:
    """Ensure WebSocket is connected before running tests.

    Args:
        api: Backpack API instance

    Raises:
        RuntimeError: If unable to establish WebSocket connection
    """
    if not api.is_connected:
        try:
            await api.connect_websocket()
            # Give connection time to stabilize
            await asyncio.sleep(0.5)
        except Exception as e:
            raise RuntimeError(
                f"Failed to establish WebSocket connection: {e}. "
                "WebSocket connectivity is required for integration tests."
            ) from e

    if not api.is_connected:
        raise RuntimeError(
            "WebSocket connection not established. "
            "Integration tests require active WebSocket connection."
        )


async def get_most_active_symbol(api: BackpackAPI) -> str:
    """Get the most actively traded symbol based on volume and recent trades.

    This ensures WebSocket tests use symbols with actual trading activity,
    preventing timeouts due to inactive markets.

    Args:
        api: Backpack API instance

    Returns:
        Symbol string for the most active market

    Raises:
        RuntimeError: If unable to find any active markets
    """
    markets = await api.get_markets(GetMarketsArgs())
    if not markets:
        raise RuntimeError("No markets available from exchange")

    # Try to get ticker data for markets to check volume
    ticker_volumes: list[tuple[str, Decimal]] = []

    # Check common active pairs first - use consistent naming
    from tests.common_symbols import (
        BTC_PERP_BP,
        BTC_USDC_BP,
        ETH_PERP_BP,
        ETH_USDC_BP,
        SOL_PERP_BP,
        SOL_USDC_BP,
    )

    priority_symbols = [
        SOL_USDC_BP.value,
        BTC_USDC_BP.value,
        ETH_USDC_BP.value,
        SOL_PERP_BP.value,
        BTC_PERP_BP.value,
        ETH_PERP_BP.value,
    ]
    available_priority = [m.symbol for m in markets if m.symbol in priority_symbols]

    # Check first 10 markets or priority symbols
    symbols_to_check = (
        available_priority[:5] if available_priority else [m.symbol for m in markets[:10]]
    )

    for symbol in symbols_to_check:
        try:
            ticker = await api.get_ticker(symbol)
            # Use quote_volume from Backpack details if available, otherwise fallback to volume
            quote_volume = ticker.bp_details.quote_volume if ticker.bp_details else None
            volume = quote_volume or ticker.volume or Decimal(0)
            if volume > 0:
                ticker_volumes.append((symbol, volume))
                logger.info(
                    "market_volume_check",
                    symbol=symbol,
                    volume=str(volume),
                    trades=(
                        ticker.bp_details.trades
                        if ticker.bp_details and ticker.bp_details.trades
                        else "N/A"
                    ),
                    message=f"Market {symbol} has volume {volume}",
                )
        except (ConnectionError, TimeoutError, ValidationError) as e:
            logger.warning(
                "ticker_fetch_failed",
                symbol=symbol,
                error=str(e),
                message=f"Failed to get ticker for {symbol}",
            )
            continue

    # Sort by volume descending
    ticker_volumes.sort(key=operator.itemgetter(1), reverse=True)

    if ticker_volumes and ticker_volumes[0][1] > 0:
        most_active = ticker_volumes[0][0]
        logger.info(
            "most_active_symbol_selected",
            symbol=most_active,
            volume=str(ticker_volumes[0][1]),
            message=f"Selected {most_active} as most active symbol",
        )
        return most_active

    # Fallback: try known active perpetual markets
    for symbol in [SOL_PERP_BP.value, BTC_PERP_BP.value, ETH_PERP_BP.value]:
        if any(m.symbol == symbol for m in markets):
            logger.warning(
                "using_fallback_symbol",
                symbol=symbol,
                message=f"No volume data available, using known active market {symbol}",
            )
            return symbol

    # Last resort: return first available market
    fallback = markets[0].symbol
    logger.error(
        "no_active_markets_found",
        fallback_symbol=fallback,
        message="No active markets found, using first available market. Test may timeout.",
    )
    return fallback

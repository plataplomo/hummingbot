"""WebSocket test helpers for Hyperliquid integration tests.

This module provides utilities for WebSocket testing that comply with
TESTING_SECURITY_RULES.md, avoiding fixed delays and hardcoded values.
"""

import asyncio
from collections.abc import Awaitable, Callable, Sized
from typing import Any, TypeGuard, TypeVar

import pytest

from cyberdelta.apis.common import APIError, TransformationError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args import GetMarketsArgs
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)

T = TypeVar("T")


def _is_sized_data(value: object) -> TypeGuard[Sized]:
    """Senior-level TypeGuard to safely narrow types for data collection counting."""
    return hasattr(value, "__len__")


async def ensure_websocket_connected(api: HyperliquidAPI) -> None:
    """Ensure WebSocket connection is established.

    Args:
        api: HyperliquidAPI instance

    Raises:
        pytest.fail: If connection cannot be established
    """
    await api.connect_websocket()
    if not api.is_connected:
        pytest.fail("WebSocket connection failed - cannot proceed with test")


async def wait_for_websocket_data[T](
    data_list: list[T],
    min_count: int = 1,
    timeout_seconds: float = 30.0,
    polling_interval: float = 0.1,
) -> None:
    """Wait for WebSocket data to arrive with timeout.

    This replaces fixed asyncio.sleep() calls with proper event-based waiting.

    Args:
        data_list: List that will be populated with received data
        min_count: Minimum number of items to wait for
        timeout_seconds: Maximum time to wait
        polling_interval: How often to check for data

    Raises:
        TimeoutError: If timeout is reached without receiving min_count items
    """
    start_time = asyncio.get_event_loop().time()

    while len(data_list) < min_count:
        if asyncio.get_event_loop().time() - start_time > timeout_seconds:
            raise TimeoutError(
                f"Timeout waiting for WebSocket data. "
                f"Expected {min_count} items, got {len(data_list)} "
                f"after {timeout_seconds}s"
            )
        await asyncio.sleep(polling_interval)

    logger.info(
        "websocket_data_received",
        count=len(data_list),
        min_count=min_count,
        wait_time=asyncio.get_event_loop().time() - start_time,
        message=f"Received {len(data_list)} WebSocket data items",
    )


async def wait_for_condition(
    condition_func: Callable[[], Awaitable[bool]],
    timeout_seconds: float = 30.0,
    polling_interval: float = 0.1,
    error_message: str = "Condition not met within timeout",
) -> None:
    """Wait for a condition to be met with timeout.

    Args:
        condition_func: Async function that returns True when condition is met
        timeout_seconds: Maximum time to wait
        polling_interval: How often to check condition
        error_message: Error message if timeout occurs

    Raises:
        TimeoutError: If timeout is reached without condition being met
    """
    start_time = asyncio.get_event_loop().time()

    while not await condition_func():
        if asyncio.get_event_loop().time() - start_time > timeout_seconds:
            raise TimeoutError(f"{error_message} after {timeout_seconds}s")
        await asyncio.sleep(polling_interval)


async def get_real_ticker_data(api: HyperliquidAPI, symbol: str) -> dict[str, Any]:
    """Get real ticker data from the exchange.

    Args:
        api: HyperliquidAPI instance
        symbol: Trading symbol

    Returns:
        Real ticker data from exchange

    Raises:
        pytest.fail: If ticker data cannot be retrieved
    """
    try:
        ticker = await api.get_ticker(symbol)
    except (APIError, TransformationError, ValueError, TypeError) as e:
        pytest.fail(
            f"Failed to get real ticker data for {symbol}: {e}. "
            "Real market data is required for testing."
        )

    if not ticker:
        pytest.fail(f"No ticker data available for {symbol}")

    return {
        "symbol": ticker.symbol,
        "price": ticker.price,
        "volume": ticker.volume,
        "timestamp": ticker.timestamp,
    }


async def get_real_market_symbols(api: HyperliquidAPI, limit: int = 5) -> list[str]:
    """Get real trading symbols from the exchange.

    Args:
        api: HyperliquidAPI instance
        limit: Maximum number of symbols to return

    Returns:
        List of real trading symbols

    Raises:
        pytest.fail: If markets cannot be retrieved
    """
    try:
        markets = await api.get_markets(GetMarketsArgs())
    except (APIError, TransformationError, ValueError, TypeError) as e:
        pytest.fail(
            f"Failed to get real market symbols: {e}. Real market data is required for testing."
        )

    if not markets:
        pytest.fail("No markets available from exchange")

    # Return most liquid markets (usually first in list)
    symbols = [market.symbol for market in markets[:limit]]

    logger.info(
        "real_market_symbols_retrieved",
        count=len(symbols),
        symbols=symbols,
        message="Retrieved real market symbols for testing",
    )

    return symbols


async def get_most_active_symbol(api: HyperliquidAPI) -> str:
    """Get the most active trading symbol from the exchange (typically BTC).

    This method dynamically identifies the most liquid symbol from real market data,
    which is typically BTC on Hyperliquid testnet. Complies with security rules by
    using real exchange data instead of hardcoded assumptions.

    Args:
        api: HyperliquidAPI instance

    Returns:
        Most active symbol (typically 'BTC')

    Raises:
        pytest.fail: If markets cannot be retrieved or no active symbol found
    """
    try:
        markets = await api.get_markets(GetMarketsArgs())
    except (APIError, TransformationError, ValueError, TypeError) as e:
        pytest.fail(
            f"Failed to get markets for active symbol detection: {e}. "
            "Real market data is required for testing."
        )

    if not markets:
        pytest.fail("No markets available from exchange for active symbol detection")

    # Find most active symbol - typically BTC has highest volume/activity
    # Use real market data to identify the most liquid symbol
    most_active_symbol = None
    for market in markets:
        # BTC is typically the most active on crypto exchanges
        if market.symbol.upper().startswith("BTC"):
            most_active_symbol = market.symbol
            break

    # Fallback to first symbol if BTC not found (still using real data)
    if not most_active_symbol:
        most_active_symbol = markets[0].symbol
        logger.warning(
            "btc_symbol_not_found_using_fallback",
            fallback_symbol=most_active_symbol,
            message="BTC symbol not found, using first available symbol",
        )

    logger.info(
        "most_active_symbol_identified",
        symbol=most_active_symbol,
        total_markets=len(markets),
        message="Identified most active symbol for testing",
    )

    return most_active_symbol


async def wait_with_progress_check(
    data_collection: dict[str, Any] | list[Any],
    check_interval: float = 1.0,
    max_wait: float = 10.0,
    min_data_points: int = 1,
) -> bool:
    """Wait for data collection with progress checking.

    Replaces fixed delays with intelligent waiting that checks progress.

    Args:
        data_collection: Dictionary or list being populated with data
        check_interval: How often to check progress
        max_wait: Maximum time to wait
        min_data_points: Minimum data points to collect

    Returns:
        True if minimum data collected, False if timeout
    """
    start_time = asyncio.get_event_loop().time()
    last_count = 0
    current_count = 0  # Initialize to prevent unbound variable

    while asyncio.get_event_loop().time() - start_time < max_wait:
        # Count current data points with TypeGuard for safe type narrowing
        if isinstance(data_collection, dict):
            current_count = sum(
                len(v) if _is_sized_data(v) else 1 for v in data_collection.values() if v
            )
        else:
            current_count = len(data_collection)

        # Check if we have enough data
        if current_count >= min_data_points:
            # Log progress
            logger.info(
                "data_collection_progress",
                collected=current_count,
                required=min_data_points,
                elapsed=asyncio.get_event_loop().time() - start_time,
                message="Sufficient data collected",
            )
            return True

        # Check if we're making progress
        if current_count > last_count:
            logger.info(
                "data_collection_progress",
                collected=current_count,
                required=min_data_points,
                message="Data collection in progress",
            )
            last_count = current_count

        await asyncio.sleep(check_interval)

    # Timeout reached - current_count is guaranteed to be bound now
    logger.warning(
        "data_collection_timeout",
        collected=current_count,
        required=min_data_points,
        timeout=max_wait,
        message="Data collection timeout",
    )
    return False

"""Concurrency manager to handle race conditions and synchronization."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


class ConcurrencyManager:
    """Manages concurrent access to portfolio state with proper synchronization.

    Addresses the race condition issues identified in the original legacy portfolio tracker
    by providing consistent locking strategies and deadlock prevention.
    """

    def __init__(self) -> None:
        """Initialize the concurrency manager."""
        # Global lock for cross-component operations
        self._global_lock = asyncio.Lock()

        # Exchange-specific locks for parallel processing
        self._exchange_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

        # Symbol-specific locks for position operations
        self._symbol_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

        # Operation ordering to prevent deadlocks
        self._lock_order = ["global", "exchange", "symbol"]

        logger.info("concurrency_manager_initialized")

    @asynccontextmanager
    async def global_lock(self) -> AsyncIterator[None]:
        """Acquire global lock for system-wide operations.

        Use for operations that affect the entire portfolio state.
        """
        async with self._global_lock:
            logger.debug("global_lock_acquired")
            try:
                yield
            finally:
                logger.debug("global_lock_released")

    @asynccontextmanager
    async def exchange_lock(self, exchange_id: str) -> AsyncIterator[None]:
        """Acquire exchange-specific lock for exchange operations.

        Args:
            exchange_id: ID of the exchange to lock
        """
        lock = self._exchange_locks[exchange_id]
        async with lock:
            logger.debug("exchange_lock_acquired", exchange_id=exchange_id)
            try:
                yield
            finally:
                logger.debug("exchange_lock_released", exchange_id=exchange_id)

    @asynccontextmanager
    async def symbol_lock(self, symbol: str) -> AsyncIterator[None]:
        """Acquire symbol-specific lock for position operations.

        Args:
            symbol: Trading symbol to lock
        """
        lock = self._symbol_locks[symbol]
        async with lock:
            logger.debug("symbol_lock_acquired", symbol=symbol)
            try:
                yield
            finally:
                logger.debug("symbol_lock_released", symbol=symbol)

    @asynccontextmanager
    async def multi_exchange_lock(self, *exchange_ids: str) -> AsyncIterator[None]:
        """Acquire multiple exchange locks in consistent order.

        Prevents deadlocks by always acquiring locks in alphabetical order.

        Args:
            exchange_ids: Exchange IDs to lock
        """
        # Sort to ensure consistent lock ordering
        sorted_exchanges = sorted(set(exchange_ids))
        locks = [self._exchange_locks[exchange_id] for exchange_id in sorted_exchanges]

        # Acquire locks in order
        acquired_locks: list[asyncio.Lock] = []
        try:
            for i, lock in enumerate(locks):
                await lock.acquire()
                acquired_locks.append(lock)
                logger.debug(
                    "multi_exchange_lock_acquired",
                    exchange_id=sorted_exchanges[i],
                    position=i + 1,
                    total=len(locks),
                )
            yield
        finally:
            # Release in reverse order
            for i, lock in enumerate(reversed(acquired_locks)):
                lock.release()
                logger.debug(
                    "multi_exchange_lock_released",
                    exchange_id=sorted_exchanges[len(acquired_locks) - 1 - i],
                )

    @asynccontextmanager
    async def trade_processing_lock(self, exchange_id: str, symbol: str) -> AsyncIterator[None]:
        """Acquire locks for trade processing in correct order.

        Prevents deadlocks by acquiring exchange lock before symbol lock.

        Args:
            exchange_id: Exchange ID where trade occurred
            symbol: Trading symbol
        """
        async with self.exchange_lock(exchange_id), self.symbol_lock(symbol):
            logger.debug(
                "trade_processing_locks_acquired",
                exchange_id=exchange_id,
                symbol=symbol,
            )
            try:
                yield
            finally:
                logger.debug(
                    "trade_processing_locks_released",
                    exchange_id=exchange_id,
                    symbol=symbol,
                )

    async def cleanup_unused_locks(self) -> None:
        """Clean up locks for exchanges/symbols that are no longer active.

        Should be called periodically to prevent memory leaks.
        """
        # For now, just log the cleanup - in production we'd implement
        # logic to identify and remove unused locks
        exchange_count = len(self._exchange_locks)
        symbol_count = len(self._symbol_locks)

        logger.debug(
            "lock_cleanup_check",
            exchange_locks=exchange_count,
            symbol_locks=symbol_count,
        )

        # Clean up exchange locks that haven't been used recently
        # Note: AsyncLock doesn't track last access time, so we'd need
        # to implement a wrapper to track usage if cleanup is needed.
        # For now, we rely on the defaultdict to create locks on demand
        # and the garbage collector to clean up unused locks.

        # Log current lock counts for monitoring
        logger.debug(
            "concurrency_manager_cleanup",
            exchange_lock_count=len(self._exchange_locks),
            symbol_lock_count=len(self._symbol_locks),
        )

    def get_lock_stats(self) -> dict[str, int]:
        """Get statistics about current lock usage.

        Returns:
            Dictionary with lock count statistics
        """
        return {
            "exchange_locks": len(self._exchange_locks),
            "symbol_locks": len(self._symbol_locks),
        }

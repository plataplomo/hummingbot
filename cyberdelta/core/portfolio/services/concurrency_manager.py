"""Type-safe concurrency manager with explicit asyncio.Lock types.

COMPLETE REPLACEMENT - Eliminates all type inference issues with locks.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from pydantic import BaseModel, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.models.base import BaseStateModel, ValidationResult


logger = get_logger(__name__)


class LockStats(BaseModel):
    """Statistics about lock usage."""

    exchange_locks: int = Field(description="Number of exchange locks")
    symbol_locks: int = Field(description="Number of symbol locks")
    total_acquisitions: int = Field(description="Total lock acquisitions")
    total_releases: int = Field(description="Total lock releases")


class ConcurrencyManager(BaseStateModel):
    """Type-safe concurrency manager with explicit asyncio.Lock types.

    COMPLETE REPLACEMENT of the old implementation that caused 6 pyright errors
    due to type inference issues with defaultdict and lock access patterns.

    Features:
    - Explicit asyncio.Lock type declarations - no type inference needed
    - Pydantic validation for configuration
    - Protocol compliance for ServiceLifecycle, StateStorable, Validatable
    - No dict[str, Any] or untyped collections
    """

    # Configuration
    max_locks_per_type: int = Field(
        default=1000, description="Maximum number of locks per type to prevent memory leaks"
    )

    # Lock statistics
    total_acquisitions: int = Field(default=0, description="Total number of lock acquisitions")
    total_releases: int = Field(default=0, description="Total number of lock releases")

    # Service state
    is_initialized: bool = Field(default=False)
    is_running: bool = Field(default=False)

    # Metadata
    metadata: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Manager metadata with strict typing"
    )

    def __init__(self, state_id: str | None = None, **data: str | float | bool) -> None:
        """Initialize concurrency manager with explicit typed locks."""
        if state_id is None:
            state_id = f"concurrency_manager_{int(time.time())}"
        super().__init__(state_id=state_id)

        # Explicit asyncio.Lock declarations - NO type inference issues
        self._global_lock: asyncio.Lock = asyncio.Lock()
        self._exchange_locks: dict[str, asyncio.Lock] = {}
        self._symbol_locks: dict[str, asyncio.Lock] = {}

        # Lock order for deadlock prevention
        self._lock_order: list[str] = ["global", "exchange", "symbol"]

        logger.info(
            "concurrency_manager_created",
            state_id=self.state_id,
            max_locks_per_type=self.max_locks_per_type,
        )

    async def initialize(self) -> None:
        """Initialize the concurrency manager."""
        if self.is_initialized:
            logger.warning("concurrency_manager_already_initialized", state_id=self.state_id)
            return

        self.is_initialized = True
        logger.info("concurrency_manager_initialized", state_id=self.state_id)

    async def start(self) -> None:
        """Start the concurrency manager."""
        if not self.is_initialized:
            await self.initialize()

        self.is_running = True
        logger.info("concurrency_manager_started", state_id=self.state_id)

    async def stop(self) -> None:
        """Stop the concurrency manager."""
        self.is_running = False
        logger.info(
            "concurrency_manager_stopped", state_id=self.state_id, final_stats=self.get_lock_stats()
        )

    async def health_check(self) -> bool:
        """Check health of concurrency manager.
        
        Returns:
            True if the manager is healthy, False otherwise
        """
        return (
            self.is_initialized
            and self.is_running
            and len(self._exchange_locks) < self.max_locks_per_type
            and len(self._symbol_locks) < self.max_locks_per_type
        )

    def _get_exchange_lock(self, exchange_id: str) -> asyncio.Lock:
        """Get exchange lock with explicit type safety.

        Args:
            exchange_id: Exchange identifier

        Returns:
            asyncio.Lock instance - explicitly typed
            
        Raises:
            RuntimeError: If maximum number of exchange locks is reached
        """
        if exchange_id not in self._exchange_locks:
            if len(self._exchange_locks) >= self.max_locks_per_type:
                msg = f"Maximum exchange locks ({self.max_locks_per_type}) reached"
                raise RuntimeError(msg)

            # Explicit asyncio.Lock creation - no type inference
            self._exchange_locks[exchange_id] = asyncio.Lock()

            logger.debug(
                "exchange_lock_created",
                exchange_id=exchange_id,
                total_exchange_locks=len(self._exchange_locks),
            )

        # Return type is explicitly asyncio.Lock
        return self._exchange_locks[exchange_id]

    def _get_symbol_lock(self, symbol: str) -> asyncio.Lock:
        """Get symbol lock with explicit type safety.

        Args:
            symbol: Trading symbol

        Returns:
            asyncio.Lock instance - explicitly typed
            
        Raises:
            RuntimeError: If maximum number of symbol locks is reached
        """
        if symbol not in self._symbol_locks:
            if len(self._symbol_locks) >= self.max_locks_per_type:
                msg = f"Maximum symbol locks ({self.max_locks_per_type}) reached"
                raise RuntimeError(msg)

            # Explicit asyncio.Lock creation - no type inference
            self._symbol_locks[symbol] = asyncio.Lock()

            logger.debug(
                "symbol_lock_created", symbol=symbol, total_symbol_locks=len(self._symbol_locks)
            )

        # Return type is explicitly asyncio.Lock
        return self._symbol_locks[symbol]

    @asynccontextmanager
    async def global_lock(self) -> AsyncIterator[None]:
        """Acquire global lock for system-wide operations.

        Yields:
            None while global lock is held
        """
        # self._global_lock is explicitly typed as asyncio.Lock
        async with self._global_lock:
            self.total_acquisitions += 1
            logger.debug(
                "global_lock_acquired", state_id=self.state_id, acquisitions=self.total_acquisitions
            )
            try:
                yield
            finally:
                self.total_releases += 1
                logger.debug(
                    "global_lock_released", state_id=self.state_id, releases=self.total_releases
                )

    @asynccontextmanager
    async def exchange_lock(self, exchange_id: str) -> AsyncIterator[None]:
        """Acquire exchange-specific lock.

        Args:
            exchange_id: Exchange identifier

        Yields:
            None while exchange lock is held
        """
        lock = self._get_exchange_lock(exchange_id)  # lock: asyncio.Lock

        async with lock:  # No type errors - lock is explicitly asyncio.Lock
            self.total_acquisitions += 1
            logger.debug(
                "exchange_lock_acquired",
                exchange_id=exchange_id,
                acquisitions=self.total_acquisitions,
            )
            try:
                yield
            finally:
                self.total_releases += 1
                logger.debug(
                    "exchange_lock_released", exchange_id=exchange_id, releases=self.total_releases
                )

    @asynccontextmanager
    async def symbol_lock(self, symbol: str) -> AsyncIterator[None]:
        """Acquire symbol-specific lock.

        Args:
            symbol: Trading symbol

        Yields:
            None while symbol lock is held
        """
        lock = self._get_symbol_lock(symbol)  # lock: asyncio.Lock

        async with lock:  # No type errors - lock is explicitly asyncio.Lock
            self.total_acquisitions += 1
            logger.debug(
                "symbol_lock_acquired", symbol=symbol, acquisitions=self.total_acquisitions
            )
            try:
                yield
            finally:
                self.total_releases += 1
                logger.debug("symbol_lock_released", symbol=symbol, releases=self.total_releases)

    @asynccontextmanager
    async def multi_exchange_lock(self, *exchange_ids: str) -> AsyncIterator[None]:
        """Acquire multiple exchange locks in consistent order.

        Args:
            exchange_ids: Exchange identifiers to lock

        Yields:
            None while all exchange locks are held
        """
        # Sort to ensure consistent lock ordering
        sorted_exchanges = sorted(set(exchange_ids))
        locks: list[asyncio.Lock] = [
            self._get_exchange_lock(exchange_id) for exchange_id in sorted_exchanges
        ]

        # Acquire locks in order - all locks are explicitly asyncio.Lock
        acquired_locks: list[asyncio.Lock] = []
        try:
            for i, lock in enumerate(locks):
                await lock.acquire()  # lock: asyncio.Lock - no type errors
                acquired_locks.append(lock)
                self.total_acquisitions += 1

                logger.debug(
                    "multi_exchange_lock_acquired",
                    exchange_id=sorted_exchanges[i],
                    position=i + 1,
                    total=len(locks),
                    acquisitions=self.total_acquisitions,
                )
            yield
        finally:
            # Release in reverse order
            for i, lock in enumerate(reversed(acquired_locks)):
                lock.release()  # lock: asyncio.Lock - no type errors
                self.total_releases += 1

                logger.debug(
                    "multi_exchange_lock_released",
                    exchange_id=sorted_exchanges[len(acquired_locks) - 1 - i],
                    releases=self.total_releases,
                )

    @asynccontextmanager
    async def trade_processing_lock(self, exchange_id: str, symbol: str) -> AsyncIterator[None]:
        """Acquire locks for trade processing in correct order.

        Args:
            exchange_id: Exchange where trade occurred
            symbol: Trading symbol

        Yields:
            None while both locks are held
        """
        async with self.exchange_lock(exchange_id), self.symbol_lock(symbol):
            logger.debug("trade_processing_locks_acquired", exchange_id=exchange_id, symbol=symbol)
            try:
                yield
            finally:
                logger.debug(
                    "trade_processing_locks_released", exchange_id=exchange_id, symbol=symbol
                )

    async def cleanup_unused_locks(self) -> None:
        """Clean up unused locks to prevent memory leaks."""
        initial_exchange_count = len(self._exchange_locks)
        initial_symbol_count = len(self._symbol_locks)

        # For now, we'll just log the cleanup check
        # In production, we'd implement usage tracking to remove unused locks
        logger.debug(
            "lock_cleanup_check",
            state_id=self.state_id,
            exchange_locks=initial_exchange_count,
            symbol_locks=initial_symbol_count,
            total_acquisitions=self.total_acquisitions,
            total_releases=self.total_releases,
        )

    def get_lock_stats(self) -> LockStats:
        """Get lock usage statistics with strong typing.

        Returns:
            LockStats model with current statistics
        """
        return LockStats(
            exchange_locks=len(self._exchange_locks),
            symbol_locks=len(self._symbol_locks),
            total_acquisitions=self.total_acquisitions,
            total_releases=self.total_releases,
        )

    async def validate_state(self) -> ValidationResult:
        """Validate concurrency manager state.

        Returns:
            ValidationResult with validation status
        """
        result = ValidationResult(valid=True)

        # Validate service state
        if not self.state_id:
            result.add_error("State ID cannot be empty")

        if self.max_locks_per_type <= 0:
            result.add_error("Max locks per type must be positive")

        if self.total_acquisitions < self.total_releases:
            result.add_error("Releases cannot exceed acquisitions")

        # Validate lock counts
        if len(self._exchange_locks) > self.max_locks_per_type:
            result.add_error(
                f"Exchange locks ({len(self._exchange_locks)}) "
                f"exceed maximum ({self.max_locks_per_type})"
            )

        if len(self._symbol_locks) > self.max_locks_per_type:
            result.add_error(
                f"Symbol locks ({len(self._symbol_locks)}) "
                f"exceed maximum ({self.max_locks_per_type})"
            )

        # Validate health
        if not await self.health_check():
            result.add_warning("Concurrency manager health check failed")

        return result

    @property
    def state_key(self) -> str:
        """State key for StateStorable protocol compliance."""
        return f"concurrency_manager_{self.state_id}"

    def to_state_dict(
        self,
    ) -> dict[str, str | int | float | bool | dict[str, str | int | float | bool]]:
        """Convert to state dictionary for persistence.

        Returns:
            Dictionary representation of state
        """
        return {
            "state_id": self.state_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "max_locks_per_type": self.max_locks_per_type,
            "total_acquisitions": self.total_acquisitions,
            "total_releases": self.total_releases,
            "is_initialized": self.is_initialized,
            "is_running": self.is_running,
            "lock_stats": self.get_lock_stats().model_dump(),
            "metadata": self.metadata.copy(),
        }

    @classmethod
    def from_state_dict(
        cls,
        data: dict[str, str | int | float | bool | dict[str, str | int | float | bool]],
    ) -> ConcurrencyManager:
        """Create manager from state dictionary.

        Args:
            data: Dictionary representation of state

        Returns:
            New ConcurrencyManager instance
        """
        # Extract and type-narrow each field
        state_id = str(data["state_id"])

        # Extract values with proper type checking
        max_locks_value = data.get("max_locks_per_type", 1000)
        max_locks_per_type = (
            int(max_locks_value) if isinstance(max_locks_value, (int, float)) else 1000
        )

        acquisitions_value = data.get("total_acquisitions", 0)
        total_acquisitions = (
            int(acquisitions_value) if isinstance(acquisitions_value, (int, float)) else 0
        )

        releases_value = data.get("total_releases", 0)
        total_releases = int(releases_value) if isinstance(releases_value, (int, float)) else 0

        is_initialized = bool(data.get("is_initialized"))
        is_running = bool(data.get("is_running"))

        metadata_raw = data.get("metadata", {})
        metadata = metadata_raw if isinstance(metadata_raw, dict) else {}

        # Create instance with state_id only, then set fields directly
        instance = cls(state_id=state_id)
        instance.max_locks_per_type = max_locks_per_type
        instance.total_acquisitions = total_acquisitions
        instance.total_releases = total_releases
        instance.is_initialized = is_initialized
        instance.is_running = is_running
        instance.metadata = metadata
        return instance


# Legacy compatibility function
async def create_concurrency_manager() -> ConcurrencyManager:
    """Create a new concurrency manager with type safety.

    Returns:
        Configured ConcurrencyManager instance
    """
    manager = ConcurrencyManager()
    await manager.initialize()
    return manager

"""Unit tests for HyperliquidClearinghouseCacheService."""

import threading
import time
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from eth_typing import ChecksumAddress, HexAddress, HexStr

from cyberdelta.apis.base.infrastructure_config_domain import CachingPolicy
from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState
from cyberdelta.apis.hyperliquid.services.account.hl_clearinghouse_cache_service import (
    HyperliquidClearinghouseCacheService,
)


if TYPE_CHECKING:
    pass


# Test data and constants
CACHE_INITIALIZATION_PARAMS = [
    (5.0, True, 1000),  # Default configuration
    (10.0, True, 500),  # Custom duration and size
    (1.0, False, 100),  # Cache disabled
    (60.0, True, 5000),  # Large cache with long TTL
]

CACHE_EXPIRATION_TEST_CASES = [
    (0.1, 0.2),  # 100ms TTL, wait 200ms
    (0.5, 1.0),  # 500ms TTL, wait 1s
    (1.0, 2.0),  # 1s TTL, wait 2s
]

THREAD_SAFETY_SCENARIOS = [
    (3, 30),  # 3 threads, 30 operations each
    (5, 50),  # 5 threads, 50 operations each
    (10, 20),  # 10 threads, 20 operations each
]


# Fixtures
@pytest.fixture
def cache_service() -> HyperliquidClearinghouseCacheService:
    """Provide a default cache service instance for tests.
    
    Returns:
        HyperliquidClearinghouseCacheService: Cache service with default configuration.
    """
    return HyperliquidClearinghouseCacheService()


@pytest.fixture
def mock_clearinghouse_state() -> MagicMock:
    """Provide a mock clearinghouse state for tests.
    
    Returns:
        MagicMock: Mock clearinghouse state object for testing.
    """
    return MagicMock(spec=HyperliquidRawClearinghouseState)


@pytest.fixture
def test_user_address() -> ChecksumAddress:
    """Provide a test user address.
    
    Returns:
        ChecksumAddress: A valid Ethereum address for testing.
    """
    return ChecksumAddress(HexAddress(HexStr("0x1234567890123456789012345678901234567890")))


@pytest.fixture
def test_user_address_2() -> ChecksumAddress:
    """Provide a second test user address.
    
    Returns:
        ChecksumAddress: A second valid Ethereum address for testing.
    """
    return ChecksumAddress(HexAddress(HexStr("0x0987654321098765432109876543210987654321")))


@pytest.fixture
def multiple_user_addresses() -> list[ChecksumAddress]:
    """Provide multiple test user addresses.
    
    Returns:
        list[ChecksumAddress]: List of valid Ethereum addresses for testing.
    """
    return [
        ChecksumAddress(HexAddress(HexStr("0x1234567890123456789012345678901234567890"))),
        ChecksumAddress(HexAddress(HexStr("0x0987654321098765432109876543210987654321"))),
        ChecksumAddress(HexAddress(HexStr("0x1111111111111111111111111111111111111111"))),
        ChecksumAddress(HexAddress(HexStr("0x2222222222222222222222222222222222222222"))),
        ChecksumAddress(HexAddress(HexStr("0x3333333333333333333333333333333333333333"))),
    ]


# Test Classes
@pytest.mark.cache
@pytest.mark.initialization
class TestCacheServiceInitialization:
    """Test cache service initialization and configuration."""

    def test_default_initialization(self) -> None:
        """Test cache service initializes with correct defaults."""
        cache_service = HyperliquidClearinghouseCacheService()

        # Use public properties and methods
        assert cache_service.cache_duration == 5.0
        assert cache_service.enable_cache is True

        # Get stats through public method
        stats = cache_service.get_cache_stats()
        assert stats["cache_duration"] == 5.0
        assert stats["cache_enabled"] is True
        assert stats["max_cache_size"] == 1000
        assert stats["total_entries"] == 0
        assert stats["hits"] == 0
        assert stats["misses"] == 0

    @pytest.mark.parametrize(
        ("cache_duration", "enable_cache", "max_cache_size"), CACHE_INITIALIZATION_PARAMS
    )
    def test_custom_initialization_scenarios(
        self,
        cache_duration: float,
        enable_cache: bool,
        max_cache_size: int,
    ) -> None:
        """Test cache service initializes with various custom parameters."""
        cache_service = HyperliquidClearinghouseCacheService(
            cache_duration=cache_duration,
            caching_policy=CachingPolicy.ENABLED if enable_cache else CachingPolicy.DISABLED,
            max_cache_size=max_cache_size,
        )

        # Use public properties and methods
        assert cache_service.cache_duration == cache_duration
        expected_policy = CachingPolicy.ENABLED if enable_cache else CachingPolicy.DISABLED
        assert cache_service.caching_policy == expected_policy

        # Verify max_cache_size through stats
        stats = cache_service.get_cache_stats()
        assert stats["cache_duration"] == cache_duration
        assert stats["cache_enabled"] == enable_cache
        assert stats["max_cache_size"] == max_cache_size


@pytest.mark.cache
@pytest.mark.basic_operations
class TestBasicCacheOperations:
    """Test basic cache operations: store, retrieve, miss."""

    def test_cache_miss_when_empty(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        test_user_address: ChecksumAddress,
    ) -> None:
        """Test cache miss when cache is empty."""
        result = cache_service.get_cached_state(test_user_address)

        assert result is None

        # Check stats through public method
        stats = cache_service.get_cache_stats()
        assert stats["misses"] == 1
        assert stats["hits"] == 0

    def test_cache_store_and_retrieve(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        test_user_address: ChecksumAddress,
        mock_clearinghouse_state: MagicMock,
    ) -> None:
        """Test storing and retrieving cached state."""
        # Cache the state
        cache_service.cache_state(test_user_address, mock_clearinghouse_state)

        # Retrieve the cached state
        result = cache_service.get_cached_state(test_user_address)

        assert result is mock_clearinghouse_state

        # Check stats through public method
        stats = cache_service.get_cache_stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 0

    def test_cache_key_generation_uniqueness(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        multiple_user_addresses: list[ChecksumAddress],
    ) -> None:
        """Test cache key generation for different users."""
        # Test uniqueness by caching different states for each user
        mock_states: list[MagicMock] = []
        for address in multiple_user_addresses:
            mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)
            mock_states.append(mock_state)
            cache_service.cache_state(address, mock_state)

        # Verify each user has their own cached state
        for i, address in enumerate(multiple_user_addresses):
            cached = cache_service.get_cached_state(address)
            assert cached is mock_states[i]


@pytest.mark.cache
@pytest.mark.expiration
class TestCacheExpiration:
    """Test cache expiration and TTL behavior."""

    @pytest.mark.parametrize(("ttl_duration", "wait_time"), CACHE_EXPIRATION_TEST_CASES)
    @pytest.mark.timing
    def test_cache_expiration_scenarios(
        self,
        ttl_duration: float,
        wait_time: float,
        test_user_address: ChecksumAddress,
    ) -> None:
        """Test cache expiration based on various TTL settings."""
        cache_service = HyperliquidClearinghouseCacheService(cache_duration=ttl_duration)

        # Create mock clearinghouse state
        mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)

        # Cache the state
        cache_service.cache_state(test_user_address, mock_state)

        # Immediately retrieve - should be cached
        result = cache_service.get_cached_state(test_user_address)
        assert result is mock_state

        # Check hit was recorded through public stats
        stats = cache_service.get_cache_stats()
        assert stats["hits"] == 1

        # Wait for expiration
        time.sleep(wait_time)

        # Retrieve again - should be expired
        result = cache_service.get_cached_state(test_user_address)
        assert result is None

        # Check miss was recorded through public stats
        stats = cache_service.get_cache_stats()
        assert stats["misses"] == 1

    @pytest.mark.timing
    def test_cache_cleanup_manual(
        self,
        test_user_address: ChecksumAddress,
    ) -> None:
        """Test manual cache cleanup."""
        cache_service = HyperliquidClearinghouseCacheService(cache_duration=0.1)

        # Create mock state
        mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)

        # Cache the state
        cache_service.cache_state(test_user_address, mock_state)

        # Wait for expiration
        time.sleep(0.2)

        # Manual cleanup
        cache_service.cleanup_cache()

        # Verify cleanup stats
        stats = cache_service.get_cache_stats()
        assert stats["cleanups"] == 1


@pytest.mark.cache
@pytest.mark.invalidation
class TestCacheInvalidation:
    """Test cache invalidation patterns."""

    def test_specific_user_invalidation(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        multiple_user_addresses: list[ChecksumAddress],
    ) -> None:
        """Test cache invalidation for specific user."""
        user_address1, user_address2 = multiple_user_addresses[:2]

        # Create mock states
        mock_state1 = MagicMock(spec=HyperliquidRawClearinghouseState)
        mock_state2 = MagicMock(spec=HyperliquidRawClearinghouseState)

        # Cache both states
        cache_service.cache_state(user_address1, mock_state1)
        cache_service.cache_state(user_address2, mock_state2)

        # Verify both are cached
        assert cache_service.get_cached_state(user_address1) is mock_state1
        assert cache_service.get_cached_state(user_address2) is mock_state2

        # Invalidate one user
        cache_service.invalidate_cache(user_address1)

        # Verify only one is invalidated
        assert cache_service.get_cached_state(user_address1) is None
        assert cache_service.get_cached_state(user_address2) is mock_state2

    def test_full_cache_invalidation(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        multiple_user_addresses: list[ChecksumAddress],
    ) -> None:
        """Test cache invalidation for all users."""
        user_address1, user_address2 = multiple_user_addresses[:2]

        # Create mock states
        mock_state1 = MagicMock(spec=HyperliquidRawClearinghouseState)
        mock_state2 = MagicMock(spec=HyperliquidRawClearinghouseState)

        # Cache both states
        cache_service.cache_state(user_address1, mock_state1)
        cache_service.cache_state(user_address2, mock_state2)

        # Verify both are cached
        assert cache_service.get_cached_state(user_address1) is mock_state1
        assert cache_service.get_cached_state(user_address2) is mock_state2

        # Invalidate all users
        cache_service.invalidate_cache()

        # Verify both are invalidated
        assert cache_service.get_cached_state(user_address1) is None
        assert cache_service.get_cached_state(user_address2) is None

    def test_invalidation_statistics_tracking(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        multiple_user_addresses: list[ChecksumAddress],
    ) -> None:
        """Test that invalidation operations are properly tracked in statistics."""
        users = multiple_user_addresses[:3]
        mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)

        # Cache states for multiple users
        for user in users:
            cache_service.cache_state(user, mock_state)

        # Perform individual invalidations
        cache_service.invalidate_cache(users[0])
        cache_service.invalidate_cache(users[1])

        # Perform full invalidation
        cache_service.invalidate_cache()

        stats = cache_service.get_cache_stats()
        assert stats["invalidations"] >= 3  # At least 2 individual + 1 full


@pytest.mark.cache
@pytest.mark.statistics
class TestCacheStatistics:
    """Test cache statistics tracking and reporting."""

    def test_comprehensive_statistics_tracking(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        test_user_address: ChecksumAddress,
    ) -> None:
        """Test comprehensive cache statistics tracking."""
        # Initial stats
        stats = cache_service.get_cache_stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["hit_rate"] == 0.0
        assert stats["total_entries"] == 0

        # Cache miss
        cache_service.get_cached_state(test_user_address)
        stats = cache_service.get_cache_stats()
        assert stats["misses"] == 1
        assert stats["hit_rate"] == 0.0

        # Cache store
        mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)
        cache_service.cache_state(test_user_address, mock_state)
        stats = cache_service.get_cache_stats()
        assert stats["total_entries"] == 1

        # Cache hit
        cache_service.get_cached_state(test_user_address)
        stats = cache_service.get_cache_stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["hit_rate"] == 0.5

    def test_hit_rate_calculation_accuracy(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        test_user_address: ChecksumAddress,
    ) -> None:
        """Test that hit rate calculations are accurate."""
        mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)
        cache_service.cache_state(test_user_address, mock_state)

        # Generate specific hit/miss pattern
        hits = 0
        total_accesses = 10

        for _ in range(total_accesses):
            result = cache_service.get_cached_state(test_user_address)
            if result is not None:
                hits += 1

        stats = cache_service.get_cache_stats()
        expected_hit_rate = hits / total_accesses
        # Allow small floating point variance
        assert abs(stats["hit_rate"] - expected_hit_rate) < 0.01


@pytest.mark.cache
@pytest.mark.disabled
class TestCacheDisabledBehavior:
    """Test cache behavior when disabled."""

    def test_cache_disabled_operations(
        self,
        test_user_address: ChecksumAddress,
    ) -> None:
        """Test cache behavior when disabled."""
        cache_service = HyperliquidClearinghouseCacheService(caching_policy=CachingPolicy.DISABLED)

        # Create mock state
        mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)

        # Try to cache state
        cache_service.cache_state(test_user_address, mock_state)

        # Try to retrieve - should always return None
        result = cache_service.get_cached_state(test_user_address)
        assert result is None

        # Stats should not be affected
        stats = cache_service.get_cache_stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 0

    def test_disabled_cache_statistics(
        self,
        test_user_address: ChecksumAddress,
    ) -> None:
        """Test that disabled cache doesn't track statistics."""
        cache_service = HyperliquidClearinghouseCacheService(caching_policy=CachingPolicy.DISABLED)
        mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)

        # Perform multiple operations
        for _ in range(10):
            cache_service.cache_state(test_user_address, mock_state)
            cache_service.get_cached_state(test_user_address)

        # Verify statistics remain at zero
        stats = cache_service.get_cache_stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["total_entries"] == 0


@pytest.mark.cache
@pytest.mark.thread_safety
class TestThreadSafety:
    """Test thread safety of cache operations."""

    @pytest.mark.parametrize(("thread_count", "operations_per_thread"), THREAD_SAFETY_SCENARIOS)
    def test_thread_safety_scenarios(
        self,
        test_user_address: ChecksumAddress,
        test_user_address_2: ChecksumAddress,
        thread_count: int,
        operations_per_thread: int,
    ) -> None:
        """Test thread safety under various concurrent scenarios."""
        cache_service = HyperliquidClearinghouseCacheService()
        results: dict[str, Any] = {"errors": [], "operations": 0}
        results_lock = threading.Lock()

        def cache_operations(
            user_address: ChecksumAddress, iterations: int = operations_per_thread
        ) -> None:
            """Perform multiple cache operations in a thread."""
            local_operations = 0
            try:
                for i in range(iterations):
                    # Create a unique state for each iteration
                    mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)
                    mock_state.iteration = i

                    # Occasionally invalidate BEFORE caching
                    if i % 10 == 0 and i > 0:
                        cache_service.invalidate_cache(user_address)

                    # Cache the state
                    cache_service.cache_state(user_address, mock_state)

                    # Retrieve - may be None if just invalidated
                    # In concurrent scenarios, cache might be invalidated
                    # between cache and retrieval - this is valid behavior
                    cache_service.get_cached_state(user_address)

                    # Get stats
                    stats = cache_service.get_cache_stats()
                    assert isinstance(stats, dict)

                    local_operations += 1

            except (AssertionError, AttributeError, TypeError, ValueError) as e:
                with results_lock:
                    results["errors"].append(str(e))
            finally:
                with results_lock:
                    results["operations"] += local_operations

        # Create multiple threads
        threads: list[threading.Thread] = []
        for i in range(thread_count):
            user_addr = test_user_address if i % 2 == 0 else test_user_address_2
            thread = threading.Thread(target=cache_operations, args=(user_addr,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify no errors occurred
        assert len(results["errors"]) == 0
        assert results["operations"] == thread_count * operations_per_thread

    def test_concurrent_invalidation_safety(
        self,
        multiple_user_addresses: list[ChecksumAddress],
    ) -> None:
        """Test thread safety during concurrent invalidation operations."""
        cache_service = HyperliquidClearinghouseCacheService()
        mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)

        # Pre-populate cache
        for user in multiple_user_addresses:
            cache_service.cache_state(user, mock_state)

        errors: list[str] = []
        error_lock = threading.Lock()

        def invalidate_operations() -> None:
            """Perform invalidation operations."""
            try:
                # Mix individual and full invalidations
                for i in range(10):
                    if i % 3 == 0:
                        cache_service.invalidate_cache()  # Full invalidation
                    else:
                        user = multiple_user_addresses[i % len(multiple_user_addresses)]
                        cache_service.invalidate_cache(user)  # Individual invalidation
            except (APIError, ValueError, KeyError, RuntimeError) as e:
                with error_lock:
                    errors.append(str(e))

        # Launch multiple invalidation threads
        threads: list[threading.Thread] = []
        for _ in range(5):
            thread = threading.Thread(target=invalidate_operations)
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify no errors
        assert len(errors) == 0

        # Verify cache is properly managed
        stats = cache_service.get_cache_stats()
        assert isinstance(stats, dict)
        assert "invalidations" in stats


@pytest.mark.cache
@pytest.mark.edge_cases
class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_none_user_address_handling(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
    ) -> None:
        """Test handling of None user address."""
        mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)

        # Current implementation allows None as a cache key - align test with business logic
        cache_service.cache_state(None, mock_state)  # type: ignore
        result = cache_service.get_cached_state(None)  # type: ignore
        assert result is mock_state  # Cache accepts None as valid key

    def test_empty_string_user_address(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
    ) -> None:
        """Test handling of zero address."""
        mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)
        zero_address = ChecksumAddress(
            HexAddress(HexStr("0x0000000000000000000000000000000000000000"))
        )

        cache_service.cache_state(zero_address, mock_state)
        result = cache_service.get_cached_state(zero_address)

        # Should work normally with zero address as key
        assert result is mock_state

    def test_none_state_caching(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        test_user_address: ChecksumAddress,
    ) -> None:
        """Test caching None state."""
        cache_service.cache_state(test_user_address, None)  # type: ignore
        result = cache_service.get_cached_state(test_user_address)

        # Should be able to cache None
        assert result is None

    def test_large_number_of_users(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
    ) -> None:
        """Test cache behavior with large number of users."""
        mock_state = MagicMock(spec=HyperliquidRawClearinghouseState)

        # Add many users (more than default cache size)
        large_user_count = 1500  # Exceeds default max_cache_size of 1000
        users = [
            ChecksumAddress(HexAddress(HexStr(f"0x{i:040x}"))) for i in range(large_user_count)
        ]

        for user in users:
            cache_service.cache_state(user, mock_state)

        # Verify cache respects size limits through public stats
        stats = cache_service.get_cache_stats()
        assert stats["total_entries"] <= stats["max_cache_size"]
        assert stats["evictions"] > 0

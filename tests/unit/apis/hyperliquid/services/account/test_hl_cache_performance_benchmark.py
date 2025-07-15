"""Performance benchmark tests for Hyperliquid caching implementation.

This module tests the performance of the clearinghouse cache service to validate:
- 60-70% API call reduction target
- >80% cache hit rate target
- TTL behavior and cache invalidation
- Performance under various load patterns
"""

import asyncio
import secrets
import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest
from eth_typing import ChecksumAddress, HexAddress, HexStr

from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
    HyperliquidRawLeverage,
    HyperliquidRawMarginSummary,
    HyperliquidRawPositionInfo,
)
from cyberdelta.apis.hyperliquid.services.account.hl_clearinghouse_cache_service import (
    HyperliquidClearinghouseCacheService,
)
from cyberdelta.apis.hyperliquid.services.account.hl_clearinghouse_state_service import (
    HyperliquidClearinghouseStateService,
)


# Test data and constants
CACHE_CONFIGURATIONS = [
    (5.0, True, 1000),  # Default configuration
    (10.0, True, 500),  # Custom duration and size
    (1.0, False, 100),  # Cache disabled
    (60.0, True, 5000),  # Large cache with long TTL
]

HIT_RATE_TEST_CASES = [
    (10, 1000, 0.95),  # 10 users, 1000 accesses, 95% hit rate
    (5, 500, 0.98),  # 5 users, 500 accesses, 98% hit rate
    (20, 2000, 0.90),  # 20 users, 2000 accesses, 90% hit rate
]

CONCURRENT_ACCESS_SCENARIOS = [
    (10, 50, 0.80),  # 10 users, 50 accesses each, 80% hit rate
    (20, 30, 0.75),  # 20 users, 30 accesses each, 75% hit rate
    (5, 100, 0.85),  # 5 users, 100 accesses each, 85% hit rate
]

EVICTION_TEST_SCENARIOS = [
    (10, 15, 5),  # max_size=10, entries=15, min_evictions=5
    (5, 10, 5),  # max_size=5, entries=10, min_evictions=5
    (20, 25, 5),  # max_size=20, entries=25, min_evictions=5
]


# Fixtures
@pytest.fixture
def mock_clearinghouse_state() -> HyperliquidRawClearinghouseState:
    """Create a mock clearinghouse state for testing."""
    return HyperliquidRawClearinghouseState(
        assetPositions=[
            HyperliquidRawAssetPosition(
                asset="BTC",
                position=HyperliquidRawPositionInfo(
                    coin="BTC",
                    entryPx="50000.0",
                    leverage=HyperliquidRawLeverage(type="isolated", value=10),
                    liquidationPx="45000.0",
                    marginUsed="5000.0",
                    maxLeverage=10,
                    positionValue="50000.0",
                    returnOnEquity="0.10",
                    szi="1.0",
                    unrealizedPnl="500.0",
                    cumFunding={"total": "0.0"},
                ),
                type="oneWay",
            )
        ],
        crossMaintenanceMarginUsed="1000.0",
        crossMarginSummary=HyperliquidRawMarginSummary(
            accountValue="100000.0",
            totalMarginUsed="10000.0",
            totalNtlPos="50000.0",
            totalRawUsd="50000.0",
        ),
        marginSummary=HyperliquidRawMarginSummary(
            accountValue="100000.0",
            totalMarginUsed="10000.0",
            totalNtlPos="50000.0",
            totalRawUsd="50000.0",
        ),
        time=int(datetime.now(UTC).timestamp() * 1000),
        withdrawable="40000.0",
        isolatedMaintenanceMarginUsed=None,
        isolatedMarginSummary=None,
    )


@pytest.fixture
def cache_service() -> HyperliquidClearinghouseCacheService:
    """Create a cache service with 5-second TTL."""
    return HyperliquidClearinghouseCacheService(
        cache_duration=5.0,
        enable_cache=True,
        max_cache_size=1000,
    )


@pytest.fixture
def test_user_address() -> ChecksumAddress:
    """Provide a test user address."""
    return ChecksumAddress(HexAddress(HexStr("0x1234567890123456789012345678901234567890")))


@pytest.fixture
def multiple_users() -> list[ChecksumAddress]:
    """Provide multiple test user addresses."""
    return [ChecksumAddress(HexAddress(HexStr(f"0x{i:040x}"))) for i in range(10)]


@pytest.fixture
def mock_http_requester() -> AsyncMock:
    """Create a mock HTTP requester for realistic trading scenarios."""
    mock_requester = AsyncMock()
    mock_requester.return_value = (
        {
            "assetPositions": [],
            "crossMaintenanceMarginUsed": "0",
            "crossMarginSummary": {
                "accountValue": "100000",
                "totalMarginUsed": "0",
                "totalNtlPos": "0",
                "totalRawUsd": "100000",
                "withdrawable": "100000",
            },
            "marginSummary": {
                "accountValue": "100000",
                "totalMarginUsed": "0",
                "totalNtlPos": "0",
                "totalRawUsd": "100000",
                "withdrawable": "100000",
            },
            "time": int(time.time() * 1000),
            "withdrawable": "100000",
        },
        200,
        {},
    )
    return mock_requester


# Test Classes
@pytest.mark.performance
@pytest.mark.cache
class TestCacheHitRatePerformance:
    """Test cache hit rate performance under various scenarios."""

    def test_sequential_access_hit_rate(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        test_user_address: ChecksumAddress,
    ) -> None:
        """Test cache hit rate with sequential access pattern."""
        # First access - cache miss
        assert cache_service.get_cached_state(test_user_address) is None
        cache_service.cache_state(test_user_address, mock_clearinghouse_state)

        # Subsequent accesses - cache hits
        hit_count = 0
        total_accesses = 100

        for _ in range(total_accesses):
            cached_state = cache_service.get_cached_state(test_user_address)
            if cached_state is not None:
                hit_count += 1

        # Calculate hit rate
        hit_rate = hit_count / total_accesses
        stats = cache_service.get_cache_stats()

        assert hit_rate >= 0.99  # Should be near 100% for sequential access
        assert stats["hit_rate"] >= 0.99
        assert stats["hits"] == hit_count
        assert stats["misses"] == 1  # Only the first access

    @pytest.mark.parametrize(("user_count", "total_accesses", "min_hit_rate"), HIT_RATE_TEST_CASES)
    def test_multiple_users_hit_rate(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        user_count: int,
        total_accesses: int,
        min_hit_rate: float,
    ) -> None:
        """Test cache hit rate with multiple users."""
        users = [ChecksumAddress(HexAddress(HexStr(f"0x{i:040x}"))) for i in range(user_count)]

        # Populate cache for all users
        for user in users:
            cache_service.cache_state(user, mock_clearinghouse_state)

        # Simulate realistic access pattern
        hit_count = 0

        for _ in range(total_accesses):
            user = secrets.choice(users)
            cached_state = cache_service.get_cached_state(user)
            if cached_state is not None:
                hit_count += 1

        # Calculate hit rate
        hit_rate = hit_count / total_accesses
        stats = cache_service.get_cache_stats()

        assert hit_rate >= min_hit_rate
        assert stats["hit_rate"] >= min_hit_rate


@pytest.mark.performance
@pytest.mark.cache
class TestAPICallReduction:
    """Test API call reduction efficiency."""

    def test_api_call_reduction_basic(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        test_user_address: ChecksumAddress,
    ) -> None:
        """Test API call reduction with cache enabled vs disabled."""
        # Simulate API calls without cache
        api_calls_without_cache = 100

        # Simulate with cache
        api_calls_with_cache = 0
        cache_service.cache_state(test_user_address, mock_clearinghouse_state)

        for _ in range(100):
            cached_state = cache_service.get_cached_state(test_user_address)
            if cached_state is None:
                api_calls_with_cache += 1
                # Re-cache after miss
                cache_service.cache_state(test_user_address, mock_clearinghouse_state)

        # Calculate reduction
        reduction = 1 - (api_calls_with_cache / api_calls_without_cache)

        assert reduction >= 0.60  # Should achieve at least 60% reduction
        assert reduction <= 0.99  # But not 100% (some misses expected)

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_realistic_trading_scenario_performance(
        self,
        mock_http_requester: AsyncMock,
    ) -> None:
        """Test cache performance in a realistic trading scenario."""
        # Create service with caching
        cache_service = HyperliquidClearinghouseCacheService(
            cache_duration=5.0,
            enable_cache=True,
        )

        # Create clearinghouse state service with cache
        with patch.object(HyperliquidClearinghouseStateService, "_cache_service", cache_service):
            service = HyperliquidClearinghouseStateService(
                http_client_requester=mock_http_requester,
                request_builder=Mock(),
                response_handler=Mock(),
                authenticator=None,
                exchange_name="hyperliquid",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

            # Simulate trading session
            start_time = time.time()
            api_calls = 0

            # Run for 30 seconds of simulated time
            while time.time() - start_time < 30:
                # Position check (most frequent)
                await service.get_clearinghouse_state()

                # Occasionally invalidate cache (simulating trades)
                if secrets.randbelow(100) < 5:  # 5% chance
                    wallet_addr = ChecksumAddress(
                        HexAddress(HexStr("0x1234567890123456789012345678901234567890"))
                    )
                    cache_service.invalidate_cache(wallet_addr)
                    api_calls += 1  # Will cause a cache miss

                await asyncio.sleep(0.1)  # 100ms between checks

            # Calculate actual API calls
            total_checks = int((time.time() - start_time) / 0.1)
            api_calls = mock_http_requester.call_count

            # Calculate reduction
            reduction = 1 - (api_calls / total_checks)

            # Get final cache stats
            stats = cache_service.get_cache_stats()

            assert reduction >= 0.60  # Should achieve 60-70% reduction
            assert stats["hit_rate"] >= 0.80  # Should maintain >80% hit rate


@pytest.mark.performance
@pytest.mark.cache
class TestCacheTTLAndEviction:
    """Test cache TTL behavior and eviction policies."""

    @pytest.mark.timing
    def test_ttl_expiration_behavior(
        self,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        test_user_address: ChecksumAddress,
    ) -> None:
        """Test TTL expiration behavior."""
        cache_service = HyperliquidClearinghouseCacheService(
            cache_duration=5.0,
            enable_cache=True,
            max_cache_size=1000,
        )

        # Cache state
        cache_service.cache_state(test_user_address, mock_clearinghouse_state)

        # Should hit immediately
        assert cache_service.get_cached_state(test_user_address) is not None

        # Sleep for less than TTL - should still hit
        time.sleep(2)
        assert cache_service.get_cached_state(test_user_address) is not None

        # Sleep past TTL - should miss
        time.sleep(4)  # Total 6 seconds > 5 second TTL
        assert cache_service.get_cached_state(test_user_address) is None

        stats = cache_service.get_cache_stats()
        assert stats["expired_entries"] == 0  # Entry was removed on access

    @pytest.mark.parametrize(("max_size", "entry_count", "min_evictions"), EVICTION_TEST_SCENARIOS)
    def test_cache_eviction_scenarios(
        self,
        max_size: int,
        entry_count: int,
        min_evictions: int,
    ) -> None:
        """Test cache eviction when size limit is reached."""
        # Create cache with specified size limit
        cache_service = HyperliquidClearinghouseCacheService(
            cache_duration=60.0,  # Long TTL to avoid expiration
            enable_cache=True,
            max_cache_size=max_size,
        )

        # Add more entries than max size
        for i in range(entry_count):
            user = ChecksumAddress(HexAddress(HexStr(f"0x{i:040x}")))
            state = HyperliquidRawClearinghouseState(
                assetPositions=[],
                crossMaintenanceMarginUsed="0",
                crossMarginSummary=HyperliquidRawMarginSummary(
                    accountValue=str(i),
                    totalMarginUsed="0",
                    totalNtlPos="0",
                    totalRawUsd="0",
                ),
                marginSummary=HyperliquidRawMarginSummary(
                    accountValue=str(i),
                    totalMarginUsed="0",
                    totalNtlPos="0",
                    totalRawUsd="0",
                ),
                time=i,
                withdrawable="0",
                isolatedMaintenanceMarginUsed=None,
                isolatedMarginSummary=None,
            )
            cache_service.cache_state(user, state)

        stats = cache_service.get_cache_stats()
        assert stats["total_entries"] <= max_size  # Should not exceed max size
        assert stats["evictions"] >= min_evictions  # Should have evicted entries


@pytest.mark.performance
@pytest.mark.cache
@pytest.mark.asyncio
class TestConcurrentPerformance:
    """Test cache performance under concurrent access."""

    @pytest.mark.parametrize(
        ("user_count", "accesses_per_user", "min_hit_rate"), CONCURRENT_ACCESS_SCENARIOS
    )
    @pytest.mark.timing
    async def test_concurrent_access_scenarios(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        user_count: int,
        accesses_per_user: int,
        min_hit_rate: float,
    ) -> None:
        """Test cache performance under various concurrent access scenarios."""
        users = [ChecksumAddress(HexAddress(HexStr(f"0x{i:040x}"))) for i in range(user_count)]

        # Pre-populate cache
        for user in users:
            cache_service.cache_state(user, mock_clearinghouse_state)

        # Simulate concurrent access
        async def access_cache(user_address: ChecksumAddress, count: int) -> int:
            hits = 0
            for _ in range(count):
                if cache_service.get_cached_state(user_address) is not None:
                    hits += 1
                await asyncio.sleep(0.001)  # Small delay to simulate processing
            return hits

        # Launch concurrent tasks
        tasks: list[asyncio.Task[int]] = []
        for user in users:
            task = asyncio.create_task(access_cache(user, accesses_per_user))
            tasks.append(task)

        # Wait for all tasks
        results = await asyncio.gather(*tasks)

        # Calculate overall hit rate
        total_hits = sum(results)
        total_accesses = len(users) * accesses_per_user
        hit_rate = total_hits / total_accesses

        assert hit_rate >= min_hit_rate


@pytest.mark.cache
@pytest.mark.invalidation
class TestCacheInvalidation:
    """Test cache invalidation patterns and behavior."""

    def test_single_user_invalidation(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        multiple_users: list[ChecksumAddress],
    ) -> None:
        """Test single user cache invalidation."""
        # Populate cache
        for user in multiple_users[:5]:  # Use first 5 users
            cache_service.cache_state(user, mock_clearinghouse_state)

        # Test single user invalidation
        cache_service.invalidate_cache(multiple_users[0])
        assert cache_service.get_cached_state(multiple_users[0]) is None
        assert cache_service.get_cached_state(multiple_users[1]) is not None

    def test_full_cache_invalidation(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        multiple_users: list[ChecksumAddress],
    ) -> None:
        """Test full cache invalidation."""
        # Populate cache
        for user in multiple_users[:5]:  # Use first 5 users
            cache_service.cache_state(user, mock_clearinghouse_state)

        # Test full cache invalidation
        cache_service.invalidate_cache()
        for user in multiple_users[:5]:
            assert cache_service.get_cached_state(user) is None

        stats = cache_service.get_cache_stats()
        assert stats["invalidations"] >= 6  # 5 users + 1 full invalidation

    def test_invalidation_patterns(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        multiple_users: list[ChecksumAddress],
    ) -> None:
        """Test various cache invalidation patterns."""
        users = multiple_users[:5]

        # Populate cache
        for user in users:
            cache_service.cache_state(user, mock_clearinghouse_state)

        # Test single user invalidation
        cache_service.invalidate_cache(users[0])
        assert cache_service.get_cached_state(users[0]) is None
        assert cache_service.get_cached_state(users[1]) is not None

        # Test full cache invalidation
        cache_service.invalidate_cache()
        for user in users:
            assert cache_service.get_cached_state(user) is None

        stats = cache_service.get_cache_stats()
        assert stats["invalidations"] >= len(users) + 1


@pytest.mark.performance
@pytest.mark.cache
@pytest.mark.parametrize(("cache_duration", "enable_cache", "max_cache_size"), CACHE_CONFIGURATIONS)
class TestCacheConfigurations:
    """Test various cache configurations and their performance."""

    def test_cache_configuration_behavior(
        self,
        cache_duration: float,
        enable_cache: bool,
        max_cache_size: int,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        test_user_address: ChecksumAddress,
    ) -> None:
        """Test cache behavior with different configurations."""
        cache_service = HyperliquidClearinghouseCacheService(
            cache_duration=cache_duration,
            enable_cache=enable_cache,
            max_cache_size=max_cache_size,
        )

        # Test configuration through behavior instead of private attributes
        # The cache behavior will demonstrate if the configuration is correct

        # Test basic cache operations
        cache_service.cache_state(test_user_address, mock_clearinghouse_state)
        result = cache_service.get_cached_state(test_user_address)

        if enable_cache:
            assert result is not None
            # Verify the cached state matches what we stored
            assert result == mock_clearinghouse_state
        else:
            # When cache is disabled, get_cached_state should return None
            assert result is None

        # Test max_cache_size behavior by filling the cache
        if enable_cache and max_cache_size > 0:
            # Create unique addresses to fill the cache
            test_addresses = [
                ChecksumAddress(HexAddress(HexStr(f"0x{i:040x}")))
                for i in range(max_cache_size + 1)
            ]

            # Fill cache to max capacity
            for addr in test_addresses[:max_cache_size]:
                cache_service.cache_state(addr, mock_clearinghouse_state)

            # Verify all items within limit are cached
            for addr in test_addresses[:max_cache_size]:
                assert cache_service.get_cached_state(addr) is not None

            # Add one more item (should trigger eviction in LRU cache)
            cache_service.cache_state(test_addresses[max_cache_size], mock_clearinghouse_state)

            # The exact eviction behavior depends on the cache implementation
            # We just verify that the cache respects some size limit
            cached_count = sum(
                1 for addr in test_addresses if cache_service.get_cached_state(addr) is not None
            )
            assert cached_count <= max_cache_size

    def test_configuration_performance_impact(
        self,
        cache_duration: float,
        enable_cache: bool,
        max_cache_size: int,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        multiple_users: list[ChecksumAddress],
    ) -> None:
        """Test performance impact of different configurations."""
        cache_service = HyperliquidClearinghouseCacheService(
            cache_duration=cache_duration,
            enable_cache=enable_cache,
            max_cache_size=max_cache_size,
        )

        # Populate cache with up to max_cache_size entries
        users_to_test = multiple_users[: min(len(multiple_users), max_cache_size)]
        for user in users_to_test:
            cache_service.cache_state(user, mock_clearinghouse_state)

        # Test access patterns
        hit_count = 0
        total_accesses = 100

        for _ in range(total_accesses):
            user = secrets.choice(users_to_test) if users_to_test else multiple_users[0]
            if cache_service.get_cached_state(user) is not None:
                hit_count += 1

        hit_rate = hit_count / total_accesses if total_accesses > 0 else 0

        if enable_cache and users_to_test:
            assert hit_rate > 0  # Should have some hits when cache is enabled
        else:
            assert hit_rate == 0  # Should have no hits when cache is disabled


@pytest.mark.integration
@pytest.mark.cache
class TestComprehensivePerformance:
    """Comprehensive performance tests and summaries."""

    def test_performance_summary(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        multiple_users: list[ChecksumAddress],
    ) -> None:
        """Generate a comprehensive performance summary."""
        # Test 1: Sequential access pattern
        user = multiple_users[0]
        cache_service.cache_state(user, mock_clearinghouse_state)

        sequential_hits = 0
        for _ in range(100):
            if cache_service.get_cached_state(user) is not None:
                sequential_hits += 1

        # Test 2: Random access pattern
        cache_service.invalidate_cache()  # Clear cache
        users = multiple_users[:10]
        for u in users:
            cache_service.cache_state(u, mock_clearinghouse_state)

        random_hits = 0
        for _ in range(100):
            u = secrets.choice(users)
            if cache_service.get_cached_state(u) is not None:
                random_hits += 1

        # Test 3: API call reduction
        baseline_calls = 100
        cached_calls = 100 - random_hits  # Misses require API calls
        reduction = (baseline_calls - cached_calls) / baseline_calls

        # Final stats
        final_stats = cache_service.get_cache_stats()

        # Validate targets
        assert reduction >= 0.60, "Should achieve 60-70% API call reduction"
        assert final_stats["hit_rate"] >= 0.80, "Should achieve >80% cache hit rate"

    @pytest.mark.benchmark
    @pytest.mark.timing
    def test_cache_performance_benchmarks(
        self,
        cache_service: HyperliquidClearinghouseCacheService,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        multiple_users: list[ChecksumAddress],
    ) -> None:
        """Run comprehensive cache performance benchmarks."""
        # Benchmark 1: Cache population speed
        start_time = time.time()
        for user in multiple_users:
            cache_service.cache_state(user, mock_clearinghouse_state)
        population_time = time.time() - start_time

        # Benchmark 2: Cache access speed
        start_time = time.time()
        for _ in range(1000):
            user = secrets.choice(multiple_users)
            cache_service.get_cached_state(user)
        access_time = time.time() - start_time

        # Benchmark 3: Cache invalidation speed
        start_time = time.time()
        cache_service.invalidate_cache()
        invalidation_time = time.time() - start_time

        # Verify reasonable performance
        assert population_time < 1.0  # Should populate quickly
        assert access_time < 1.0  # Should access quickly
        assert invalidation_time < 0.1  # Should invalidate quickly

        # Get final performance stats
        stats = cache_service.get_cache_stats()
        assert stats["hit_rate"] >= 0.80  # Maintain high hit rate

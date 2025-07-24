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
from unittest.mock import AsyncMock, Mock

import pytest
from eth_typing import ChecksumAddress, HexAddress, HexStr

from cyberdelta.apis.base.infrastructure_config_domain import CachingPolicy
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
from tests.fixtures.time_fixtures import FreezerProtocol


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
        time=int(datetime.now(UTC).timestamp() * 1000),  # Uses frozen time
        withdrawable="40000.0",
        isolatedMaintenanceMarginUsed=None,
        isolatedMarginSummary=None,
    )


@pytest.fixture
def cache_service() -> HyperliquidClearinghouseCacheService:
    """Create a cache service with 5-second TTL."""
    return HyperliquidClearinghouseCacheService(
        cache_duration=5.0,
        caching_policy=CachingPolicy.ENABLED,
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
def mock_http_requester(frozen_time: FreezerProtocol) -> AsyncMock:
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
            "time": int(datetime.now(UTC).timestamp() * 1000),  # Uses frozen time
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

        # Simulate with cache - introduce some cache misses to ensure reduction is not 100%
        api_calls_with_cache = 0
        cache_service.cache_state(test_user_address, mock_clearinghouse_state)

        for i in range(100):
            # Simulate cache expiration every 20 iterations to create realistic scenario
            if i > 0 and i % 20 == 0:
                cache_service.invalidate_cache(test_user_address)

            cached_state = cache_service.get_cached_state(test_user_address)
            if cached_state is None:
                api_calls_with_cache += 1
                # Re-cache after miss
                cache_service.cache_state(test_user_address, mock_clearinghouse_state)

        # Calculate reduction
        reduction = 1 - (api_calls_with_cache / api_calls_without_cache)

        assert reduction >= 0.60  # Should achieve at least 60% reduction
        assert reduction <= 1.0  # Perfect cache can achieve up to 100% reduction

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_realistic_trading_scenario_performance(
        self,
        mock_http_requester: AsyncMock,
        frozen_time: FreezerProtocol,
    ) -> None:
        """Test cache performance in a realistic trading scenario."""
        # Create cache service directly
        cache_service = HyperliquidClearinghouseCacheService(
            cache_duration=5.0,
            caching_policy=CachingPolicy.ENABLED,
        )

        # Create mock authenticator and response handler
        mock_authenticator = Mock()
        mock_authenticator.__bool__ = Mock(return_value=True)  # Make it truthy

        mock_request_builder = Mock()
        mock_request_builder.build_user_state_payload.return_value = Mock()

        mock_response_handler = Mock()
        # Create a mock clearinghouse state to return
        mock_state = HyperliquidRawClearinghouseState(
            assetPositions=[],
            crossMaintenanceMarginUsed="0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="100000",
                totalMarginUsed="0",
                totalNtlPos="0",
                totalRawUsd="100000",
            ),
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="100000",
                totalMarginUsed="0",
                totalNtlPos="0",
                totalRawUsd="100000",
            ),
            time=int(time.time() * 1000),
            withdrawable="100000",
            isolatedMaintenanceMarginUsed=None,
            isolatedMarginSummary=None,
        )
        mock_response_handler.handle_get_user_state_response.return_value = mock_state

        # Create clearinghouse state service with cache injected
        service = HyperliquidClearinghouseStateService(
            http_client_requester=mock_http_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="hyperliquid",
            wallet_address="0x1234567890123456789012345678901234567890",
            cache_service=cache_service,  # Inject our cache service
        )

        # Simulate a realistic trading session with limited duration
        start_time = time.time()
        max_duration = 1.0  # Limit to 1 second for test performance
        check_interval = 0.05  # 50ms between checks

        while time.time() - start_time < max_duration:
            # Position check (most frequent operation)
            await service.get_clearinghouse_state()

            # Occasionally invalidate cache (simulating trades/position changes)
            if secrets.randbelow(100) < 10:  # 10% chance
                wallet_addr = ChecksumAddress(
                    HexAddress(HexStr("0x1234567890123456789012345678901234567890"))
                )
                service.invalidate_cache(wallet_addr)

            await asyncio.sleep(check_interval)

        # Calculate metrics
        total_checks = int((time.time() - start_time) / check_interval)
        api_calls = mock_http_requester.call_count

        # Get cache performance stats
        stats = service.get_cache_stats()

        # Calculate reduction based on actual API calls vs potential calls
        reduction = 1 - api_calls / total_checks if total_checks > 0 else 0

        # Validate performance targets
        assert reduction >= 0.60, f"Should achieve 60% API call reduction, got {reduction:.2%}"
        assert stats["hit_rate"] >= 0.80, (
            f"Should maintain >80% cache hit rate, got {stats['hit_rate']:.2%}"
        )


@pytest.mark.performance
@pytest.mark.cache
class TestCacheTTLAndEviction:
    """Test cache TTL behavior and eviction policies."""

    @pytest.mark.timing
    def test_ttl_expiration_behavior(
        self,
        mock_clearinghouse_state: HyperliquidRawClearinghouseState,
        test_user_address: ChecksumAddress,
        frozen_time: FreezerProtocol,
    ) -> None:
        """Test TTL expiration behavior."""
        cache_service = HyperliquidClearinghouseCacheService(
            cache_duration=5.0,
            caching_policy=CachingPolicy.ENABLED,
            max_cache_size=1000,
        )

        # Cache state
        cache_service.cache_state(test_user_address, mock_clearinghouse_state)

        # Should hit immediately
        assert cache_service.get_cached_state(test_user_address) is not None

        # Advance time for less than TTL - should still hit
        current_time = datetime.now(UTC)
        frozen_time.move_to(current_time.replace(second=current_time.second + 2))
        assert cache_service.get_cached_state(test_user_address) is not None

        # Advance time past TTL - should miss
        frozen_time.move_to(
            current_time.replace(second=current_time.second + 6)
        )  # Total 6 seconds > 5 second TTL
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
            caching_policy=CachingPolicy.ENABLED,
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
        # 5 users invalidated (cache counts entries, not operations)
        assert stats["invalidations"] >= 5

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

        # Test full cache invalidation (this will invalidate remaining 4 users)
        cache_service.invalidate_cache()
        for user in users:
            assert cache_service.get_cached_state(user) is None

        stats = cache_service.get_cache_stats()
        assert stats["invalidations"] >= len(users)  # 5 total users invalidated (1 single + 4 full)


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
            caching_policy=CachingPolicy.ENABLED if enable_cache else CachingPolicy.DISABLED,
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
            caching_policy=CachingPolicy.ENABLED if enable_cache else CachingPolicy.DISABLED,
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
        frozen_time: FreezerProtocol,
    ) -> None:
        """Run comprehensive cache performance benchmarks."""
        # Benchmark 1: Cache population - verify it succeeds without errors
        for user in multiple_users:
            cache_service.cache_state(user, mock_clearinghouse_state)

        # Verify all users were cached successfully
        for user in multiple_users:
            assert cache_service.get_cached_state(user) is not None

        # Benchmark 2: Cache access - verify high hit rate
        hits = 0
        total_accesses = 1000
        for _ in range(total_accesses):
            user = secrets.choice(multiple_users)
            if cache_service.get_cached_state(user) is not None:
                hits += 1

        hit_rate = hits / total_accesses
        assert hit_rate >= 0.95  # Should have very high hit rate

        # Benchmark 3: Cache invalidation - verify it clears cache
        cache_service.invalidate_cache()

        # Verify all users were invalidated
        for user in multiple_users:
            assert cache_service.get_cached_state(user) is None

        # Get final performance stats
        stats = cache_service.get_cache_stats()
        assert stats["hit_rate"] >= 0.80  # Maintain high hit rate

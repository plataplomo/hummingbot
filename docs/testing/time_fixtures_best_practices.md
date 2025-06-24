# Time Fixtures Best Practices Guide

**Last Updated**: June 2025
**Applies To**: CyberDeltaEngine Test Suite

## Overview

This guide provides best practices for using time control fixtures in the CyberDeltaEngine test suite. Proper time handling in tests is critical for a trading engine to ensure deterministic, reliable test execution.

## Available Time Fixtures

All time fixtures are centralized in `tests/fixtures/time_fixtures.py` and automatically available through `conftest.py`.

### 1. `freezer` (pytest-freezer)
The base fixture provided by the pytest-freezer plugin.

```python
def test_with_freezer(freezer):
    freezer.move_to("2024-01-01 12:00:00")
    assert datetime.now().hour == 12
```

### 2. `frozen_time`
Enhanced wrapper around freezer with better defaults.

```python
def test_with_frozen_time(frozen_time):
    # Starts at 2024-01-01 00:00:00+00:00
    frozen_time.move_to("2024-06-15 14:30:00+00:00")
    # All datetime.now() calls return this time
```

### 3. `mock_time_factory`
Factory for creating module-specific time mocks.

```python
def test_with_time_factory(mock_time_factory):
    mock_dt = mock_time_factory(
        module_path="cyberdelta.core.models.trade_signal.datetime",
        fixed_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    )
    with mock_dt:
        # Module uses mocked time
```

### 4. `mock_time_patch`
Standard fixture for mocking `time.time()`.

```python
def test_auth_signature(mock_time_patch):
    mock_time_patch.return_value = 1678886400.0
    # Test auth signature generation
```

### 5. `market_time_simulation`
Simulate different market conditions.

```python
def test_market_hours(market_time_simulation):
    market_time_simulation(market="NYSE", hour=9, minute=30)
    # Test market open behavior
```

### 6. `rate_limit_timer`
Advance time for rate limiting tests.

```python
def test_rate_limiting(rate_limit_timer):
    # Make first request
    rate_limit_timer(advance_seconds=0.1)
    # Make second request after 100ms
```

## When to Use Each Fixture

### Use `frozen_time` when:
- Testing code that uses `datetime.now()` or `datetime.utcnow()`
- Need deterministic timestamps in integration tests
- Testing time-based business logic
- Working with VCR cassettes that need consistent timestamps

### Use `mock_time_factory` when:
- Need to mock specific module imports
- Have complex time progression requirements
- Need different time behaviors for different modules

### Use `mock_time_patch` when:
- Testing authentication signatures
- Working with `time.time()` based code
- Testing UNIX timestamp handling

### Use `market_time_simulation` when:
- Testing market hours logic
- Simulating different trading sessions
- Testing timezone-aware operations

### Use `rate_limit_timer` when:
- Testing rate limiting logic
- Need precise time advancement
- Testing timeout behavior

## Best Practices

### 1. Always Use UTC

```python
# ✅ GOOD
frozen_time.move_to("2024-01-01 12:00:00+00:00")
fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

# ❌ BAD - Timezone naive
frozen_time.move_to("2024-01-01 12:00:00")
fixed_time = datetime(2024, 1, 1, 12, 0, 0)
```

### 2. Use Type Hints

```python
# ✅ GOOD
def test_something(frozen_time: FreezerProtocol) -> None:
    frozen_time.move_to("2024-01-01")

# ❌ BAD - No type hints
def test_something(frozen_time):
    frozen_time.move_to("2024-01-01")
```

### 3. Be Explicit About Time Requirements

```python
# ✅ GOOD - Clear about what time is needed
def test_market_close(frozen_time: FreezerProtocol) -> None:
    # Set to 4 PM EST for NYSE close
    frozen_time.move_to("2024-01-01 21:00:00+00:00")  # 4 PM EST in UTC

# ❌ BAD - Magic time values
def test_something(frozen_time: FreezerProtocol) -> None:
    frozen_time.move_to("2024-01-01 21:00:00+00:00")  # Why this time?
```

### 4. Test Time Progression

```python
# ✅ GOOD - Test behavior over time
def test_order_timeout(frozen_time: FreezerProtocol) -> None:
    order = place_order()

    # Advance 29 seconds - should still be pending
    frozen_time.move_to(datetime.now(UTC) + timedelta(seconds=29))
    assert order.status == "pending"

    # Advance past 30 second timeout
    frozen_time.move_to(datetime.now(UTC) + timedelta(seconds=31))
    assert order.status == "timeout"
```

### 5. Use Markers for Timing Tests

```python
# ✅ GOOD
@pytest.mark.timing
def test_rate_limiter(frozen_time: FreezerProtocol) -> None:
    # Test implementation

# Run only timing tests
# pytest -m timing

# Skip timing tests
# pytest -m "not timing"
```

### 6. Document Time Dependencies

```python
def test_funding_rate_calculation(frozen_time: FreezerProtocol) -> None:
    """Test funding rate calculation at market snapshot time.

    Requires:
    - Time set to funding snapshot (00:00, 08:00, or 16:00 UTC)
    - Consistent time across all market data calls
    """
    frozen_time.move_to("2024-01-01 08:00:00+00:00")
    # Test implementation
```

## Common Patterns

### Pattern 1: Testing None Timestamp Fallback

```python
def test_none_timestamp_handling(frozen_time: FreezerProtocol) -> None:
    frozen_time.move_to("2024-01-01 12:00:00+00:00")

    # When timestamp is None, should use current time
    result = process_order(timestamp=None)
    assert result.timestamp == datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
```

### Pattern 2: Testing Time-Based Expiry

```python
def test_order_expiry(frozen_time: FreezerProtocol) -> None:
    # Create order with 5 minute expiry
    frozen_time.move_to("2024-01-01 12:00:00+00:00")
    order = create_order(expiry_minutes=5)

    # Check not expired after 4 minutes
    frozen_time.move_to("2024-01-01 12:04:00+00:00")
    assert not order.is_expired()

    # Check expired after 6 minutes
    frozen_time.move_to("2024-01-01 12:06:00+00:00")
    assert order.is_expired()
```

### Pattern 3: Testing Rate Limiting

```python
def test_rate_limit_enforcement(rate_limit_timer) -> None:
    limiter = RateLimiter(rate=10, per_second=1)  # 10 requests per second

    # Make 10 requests - should succeed
    for _ in range(10):
        assert limiter.allow_request()

    # 11th request should fail
    assert not limiter.allow_request()

    # Advance 1 second
    rate_limit_timer(advance_seconds=1)

    # Should allow requests again
    assert limiter.allow_request()
```

### Pattern 4: VCR Cassette Compatibility

```python
@pytest.mark.vcr
def test_market_data_with_vcr(
    frozen_time: FreezerProtocol,
    custom_vcr_config: dict[str, Any],
) -> None:
    # Freeze time for deterministic cassette matching
    frozen_time.move_to("2024-01-01 12:00:00+00:00")

    # API calls will have consistent timestamps
    data = await get_market_data()
    assert data.timestamp == datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
```

## Migration Guide

### From unittest.mock to pytest-freezer

```python
# ❌ OLD - unittest.mock
@patch("module.datetime")
def test_old_way(mock_datetime):
    mock_datetime.now.return_value = datetime(2024, 1, 1, tzinfo=UTC)
    # test code

# ✅ NEW - pytest-freezer
def test_new_way(frozen_time: FreezerProtocol) -> None:
    frozen_time.move_to("2024-01-01 00:00:00+00:00")
    # test code
```

### From Multiple Patches to Single Fixture

```python
# ❌ OLD - Multiple patches
with (
    patch("module1.datetime") as mock_dt1,
    patch("module2.datetime") as mock_dt2,
):
    now = datetime(2024, 1, 1, tzinfo=UTC)
    mock_dt1.now.return_value = now
    mock_dt2.now.return_value = now

# ✅ NEW - Single fixture patches globally
def test_new_way(frozen_time: FreezerProtocol) -> None:
    frozen_time.move_to("2024-01-01 00:00:00+00:00")
    # Both modules automatically use frozen time
```

## Troubleshooting

### Issue: Time not frozen in async code
**Solution**: Ensure pytest-asyncio mode is set correctly
```python
# pyproject.toml
[tool.pytest.ini_options]
asyncio_mode = "strict"
```

### Issue: Fixture not found
**Solution**: Ensure conftest.py imports time_fixtures
```python
# tests/conftest.py
from tests.fixtures.time_fixtures import *
```

### Issue: Type hints not working
**Solution**: Import FreezerProtocol explicitly
```python
from tests.fixtures.time_fixtures import FreezerProtocol
```

### Issue: VCR cassettes not matching
**Solution**: Ensure consistent time freezing
```python
def test_vcr(frozen_time: FreezerProtocol) -> None:
    # Freeze BEFORE making API calls
    frozen_time.move_to("2024-01-01 00:00:00+00:00")
    # Now make API calls
```

## Security Considerations

Per TESTING_SECURITY_RULES.md, remember:

1. **Never use hardcoded timestamps as fallbacks in production code**
2. **Always validate timestamp data from external sources**
3. **Use proper timezone handling (UTC) for all financial operations**
4. **Test time-based security features (replay protection) thoroughly**

## Continuous Improvement

This guide is a living document. When you:
- Discover new patterns
- Find better approaches
- Encounter issues

Please update this guide to help future developers.

## Related Documentation

- [TESTING_SECURITY_RULES.md](../integration/TESTING_SECURITY_RULES.md)
- [Time Fixtures API](../../tests/fixtures/time_fixtures.py)
- [Migration Plan](../../workflow/time_fixtures_migration_plan.md)
- [pytest-freezer Documentation](https://pypi.org/project/pytest-freezer/)

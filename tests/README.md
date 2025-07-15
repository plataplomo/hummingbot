# Test Organization

This document describes the test structure and organization for the CyberDeltaEngine project.

## Directory Structure

```
tests/
├── unit/                    # Fast, isolated unit tests
│   ├── apis/               # API client unit tests
│   ├── core/               # Core business logic unit tests
│   └── ...
├── integration/            # Integration tests requiring external resources
│   ├── apis/               # API integration tests
│   ├── config/             # Configuration file tests
│   ├── core/               # Multi-component workflow tests
│   ├── visualization/      # File I/O visualization tests
│   └── ...
└── fixtures/               # Test data and fixtures

```

## Test Categories

### Unit Tests (`tests/unit/`)
- **Fast execution** (< 100ms per test)
- **Isolated** - test single components in isolation
- **Mocked dependencies** - external services are mocked
- **No file I/O** - no real file system operations
- **No network calls** - no real API calls
- **Deterministic** - no timing dependencies

### Integration Tests (`tests/integration/`)
- **Multi-component** - test interaction between components
- **File I/O operations** - create/read/write real files
- **Network dependencies** - may require real API calls
- **Timing sensitive** - may include sleep/timeout operations
- **Environment dependent** - may require specific setup

## Test Markers

Use pytest markers to categorize and run specific test types:

```bash
# Run only integration tests
pytest -m integration

# Run spot trading tests
pytest -m spot

# Run perpetual trading tests
pytest -m perp

# Skip timing-dependent tests
pytest -m "not timing"

# Run safe tests only (zero balance)
pytest -m zero_balance

# Run cross-exchange tests
pytest -m cross_exchange
```

Available markers:
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.timing` - Tests with timing dependencies or slow execution
- `@pytest.mark.spot` - Spot trading specific tests
- `@pytest.mark.perp` - Perpetual/derivatives trading tests
- `@pytest.mark.zero_balance` - Safe tests with zero balance (safe to run)
- `@pytest.mark.requires_balance` - Tests requiring real money/balance (use with caution)
- `@pytest.mark.shared` - Shared/common functionality tests
- `@pytest.mark.cross_exchange` - Cross-exchange validation tests
- `@pytest.mark.vcr` - VCR cassette recording for network isolation
- `@pytest.mark.slow` - Alias for timing-dependent tests

## Running Tests

### Development Workflow
```bash
# Run fast unit tests during development
pytest tests/unit/ -x --ff

# Run integration tests before commits
pytest tests/integration/

# Run all tests with coverage
pytest --cov=cyberdelta --cov-report=term-missing
```

### CI/CD Pipeline
```bash
# Stage 1: Fast feedback (unit tests)
pytest tests/unit/ --maxfail=5

# Stage 2: Integration validation
pytest tests/integration/ --maxfail=3

# Stage 3: Full test suite with coverage
pytest --cov=cyberdelta --cov-fail-under=90
```

## Best Practices

### Unit Tests
- Mock external dependencies using `unittest.mock` or `pytest-mock`
- Test public interfaces, not implementation details
- Avoid accessing protected members (`._attribute`)
- Use fixtures for common test data
- Keep tests focused on single behaviors

### Integration Tests
- Use temporary directories for file operations
- Clean up resources after tests
- Use realistic test data
- Test complete workflows end-to-end
- Mark with appropriate pytest markers

### Refactoring Protected Member Access
Instead of testing internal implementation:
```python
# ❌ Bad - testing implementation details
def test_internal_method():
    obj._internal_method()
    assert obj._internal_state == expected

# ✅ Good - testing public behavior
def test_public_behavior():
    result = obj.public_method()
    assert result == expected
```

## Recent Changes

The test suite has been reorganized to improve maintainability:

### Moved to Integration Tests
- `test_config_security.py` - File I/O operations
- `test_simplified_visualizer.py` - File creation and plotting
- `test_execution_handler.py` - Complex multi-component workflows

### Refactored Protected Member Access
- `test_symbol_mapper.py` - Now tests through public `get_exchange_symbol()` API
- `test_portfolio_tracker.py` - Uses mocked dependencies instead of internal methods
- `test_hl_auth_sign_l1_action.py` - Renamed helper methods to public

### Configuration
- Added pytest markers in `pyproject.toml`
- Updated test discovery patterns
- Added separate coverage configurations for unit vs integration tests

This organization enables faster development cycles with quick unit test feedback while maintaining comprehensive integration test coverage.

# Pytest-Recording Configuration and Cassette Organization

This document explains the VCR cassette recording setup using pytest-recording and documents issues and solutions related to cassette directory organization.

## Overview

The CyberDeltaEngine project uses [pytest-recording](https://github.com/kiwicom/pytest-recording) to record and replay HTTP interactions during integration tests. This allows tests to make real API calls initially and then replay them from cassette files in subsequent runs, providing both accuracy and speed.

## Directory Structure

Cassettes are organized in a hierarchical structure under `tests/cassettes/`:

```
tests/cassettes/
├── apis/                          # Integration API tests (current structure)
│   ├── backpack/
│   │   ├── public/               # Public endpoint cassettes
│   │   └── private/              # Private endpoint cassettes (future)
│   ├── hyperliquid/
│   │   ├── public/               # Public endpoint cassettes
│   │   └── private/              # Private endpoint cassettes (future)
│   └── demo/
│       └── filtering/            # Demo tests for VCR filtering
└── api/                          # Legacy structure (deprecated)
    ├── backpack/
    └── hyperliquid/
```

## Configuration

### Key Fixtures

1. **`vcr_config`** (`tests/integration/conftest.py`): Provides base VCR configuration including request/response filtering, security filtering, and recording settings.

2. **`vcr_cassette_dir`** (`tests/integration/conftest.py`): Determines where cassette files are saved. This fixture:
   - Checks for `custom_vcr_cassette_dir` parametrization
   - Creates organized directory paths from test parameters
   - Falls back to module-based directory structure

3. **`custom_vcr_cassette_dir`** (`tests/integration/conftest.py`): Allows tests to specify custom cassette directories via parametrization.

### Usage in Tests

Tests use parametrization to specify their cassette directory:

```python
@pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/public"], indirect=True)
@pytest.mark.vcr
async def test_backpack_public_endpoint(
    active_bp_config: ExchangeSpecificConfig,
    custom_vcr_cassette_dir: str,  # Must accept the parametrized fixture
) -> None:
    # Test implementation
```

## CRITICAL ISSUE AND SOLUTION (Documented 2025-01-06)

### Problem
VCR cassettes were being saved in the root `tests/cassettes/` directory instead of organized subdirectories like `tests/cassettes/apis/backpack/public/`.

### Root Cause
The `vcr_config` fixture was hardcoding `"cassette_library_dir": "tests/cassettes"`, which overrode the `vcr_cassette_dir` fixture that pytest-recording uses for directory organization.

**Key Insight**: When pytest-recording merges VCR configuration, settings from the `vcr_config` fixture take precedence over the directory path determined by the `vcr_cassette_dir` fixture.

### Solution Applied
1. **Removed hardcoded `cassette_library_dir`**: Removed the line `"cassette_library_dir": "tests/cassettes"` from the `vcr_config` fixture.

2. **Let pytest-recording manage directories**: Allowed pytest-recording to use the `vcr_cassette_dir` fixture properly for directory organization.

3. **Fixed test parametrization**: Ensured test functions accept the `custom_vcr_cassette_dir: str` parameter when using `indirect=True` parametrization.

### Before (BROKEN):
```python
# In vcr_config fixture
config = {
    # ... other settings ...
    "cassette_library_dir": "tests/cassettes",  # ❌ This overrides vcr_cassette_dir!
    # ... other settings ...
}
```

Result: All cassettes saved in `tests/cassettes/test_name.yaml`

### After (FIXED):
```python
# In vcr_config fixture
config = {
    # ... other settings ...
    # NOTE: cassette_library_dir is handled by the vcr_cassette_dir fixture
    # ... other settings ...
}
```

Result: Cassettes saved in organized subdirectories like `tests/cassettes/apis/backpack/public/test_name.yaml`

## Security Features

The VCR configuration includes comprehensive filtering to prevent sensitive data from being recorded in cassettes:

### Filtered Headers
- Authentication headers (Authorization, Bearer, X-API-Key, etc.)
- Signature headers (X-Signature, X-BP-Signature, etc.)
- Timestamp headers (X-Timestamp, X-BP-Timestamp, etc.)
- Exchange-specific headers (X-BP-*, X-HL-*, etc.)

### Filtered Query Parameters
- API keys, signatures, timestamps, nonces
- User identification (user_id, client_id, wallet, address)
- Session and tracking parameters

### Filtered POST Data
- Private keys, secrets, passwords, mnemonics
- API keys and signatures in request bodies

### Custom Request/Response Filtering
- Request bodies are scanned for sensitive patterns and replaced with filtered placeholders
- Timestamps are normalized for test determinism

## Recording Modes

Recording behavior can be controlled via environment variable:

```bash
# Record new cassettes (default)
VCR_RECORD_MODE=once pytest tests/integration/apis/

# Always record (overwrite existing cassettes)
VCR_RECORD_MODE=new_episodes pytest tests/integration/apis/

# Never record (use existing cassettes only)
VCR_RECORD_MODE=none pytest tests/integration/apis/
```

## Best Practices

1. **Organize by exchange and access level**: Use directory structure like `apis/{exchange}/{public|private}/`

2. **Use parametrization for custom directories**: Always use `@pytest.mark.parametrize("custom_vcr_cassette_dir", ["path"], indirect=True)`

3. **Accept the fixture parameter**: Test functions must accept `custom_vcr_cassette_dir: str` when using indirect parametrization

4. **Verify cassette location**: After recording, verify cassettes are saved in the expected subdirectory

5. **Never commit secrets**: The filtering should prevent this, but always verify cassettes don't contain sensitive data

## Troubleshooting

### Cassettes saved in wrong directory
- Check that `vcr_config` fixture doesn't hardcode `cassette_library_dir`
- Verify `vcr_cassette_dir` fixture logic is correct
- Ensure test accepts `custom_vcr_cassette_dir` parameter when using indirect parametrization

### Sensitive data in cassettes
- Review filtering configuration in `vcr_config` fixture
- Add new patterns to request/response filters as needed
- Check that test headers don't bypass filtering

### Test collection errors
- Verify fixture names match between parametrization and test function parameters
- Check that `indirect=True` is used for custom directory parametrization

## References

- [pytest-recording documentation](https://github.com/kiwicom/pytest-recording)
- [VCR.py documentation](https://vcrpy.readthedocs.io/)
- [Project VCR configuration](../integration/conftest.py)

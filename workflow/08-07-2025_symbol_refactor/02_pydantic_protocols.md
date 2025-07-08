# Enhanced Type Safety with Pydantic and Protocols for SymbolMapper

## Executive Summary

This document outlines how to enhance the SymbolMapper's type safety using Pydantic models and Python Protocols. Based on the issues identified in `01_start.md` and analysis of the existing configuration system, we'll create improvements that integrate seamlessly with the current codebase while addressing type safety concerns.

**Important Note**: The codebase already has:
- Pydantic models for configuration (`ExchangeSpecificConfig` with `symbols: dict[str, str]`)
- An `ISymbolMapper` Protocol in `cyberdelta/core/services/interfaces.py`
- A working configuration system using `AppSettings` and `ConfigManager`

This document focuses on enhancing the existing system rather than replacing it.

## 1. Current Type Safety Issues

### Problems Identified:
- Excessive use of `dict[str, Any]` for configuration
- Multiple `cast()` operations bypassing type checking
- No runtime validation of inputs
- Inconsistent error handling between validation failures
- No Protocol definition despite having `ISymbolMapper` interface

## 2. Enhancing Existing Pydantic Models

### 2.1 Enhanced Symbol Validation

Since `ExchangeSpecificConfig` already exists with `symbols: dict[str, str]`, we'll enhance it with better validation:

```python
# In cyberdelta/config/models/config_models.py - enhance the existing validator

from pydantic import field_validator
import re

class ExchangeSpecificConfig(BaseModel):
    """Enhanced existing exchange configuration with better symbol validation."""

    # ... existing fields ...

    symbols: dict[str, str]

    @field_validator('symbols')
    @classmethod
    def validate_symbols_enhanced(cls, v: dict[str, str]) -> dict[str, str]:
        """Enhanced validation for symbol mappings."""
        if not v:
            raise ValueError("At least one symbol mapping is required")

        # Pattern for internal symbols (uppercase alphanumeric)
        internal_pattern = re.compile(r'^[A-Z0-9]+$')

        validated = {}
        for internal, exchange in v.items():
            # Validate internal symbol
            internal = internal.strip()
            if not internal:
                raise ValueError("Internal symbol cannot be empty")
            if not internal_pattern.match(internal):
                raise ValueError(
                    f"Internal symbol '{internal}' must be uppercase alphanumeric"
                )

            # Validate exchange symbol
            exchange = exchange.strip()
            if not exchange:
                raise ValueError(f"Exchange symbol for '{internal}' cannot be empty")

            # Check for duplicates in validated dict
            if exchange in validated.values():
                existing_internal = next(k for k, v in validated.items() if v == exchange)
                raise ValueError(
                    f"Exchange symbol '{exchange}' maps to both '{existing_internal}' "
                    f"and '{internal}'"
                )

            validated[internal] = exchange

        return validated


# Additional value object for type-safe symbol handling
class SymbolMapping(BaseModel):
    """Value object representing a validated symbol mapping."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    internal_symbol: str = Field(
        ...,
        min_length=1,
        max_length=20,
        pattern=r'^[A-Z0-9]+$',
        description="Internal symbol representation (e.g., 'BTC', 'ETH')"
    )
    exchange_symbol: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Exchange-specific symbol (e.g., 'BTC_PERP', 'ETH-USD')"
    )
    exchange_id: str = Field(
        ...,
        min_length=1,
        description="Exchange identifier"
    )
```

### 2.2 Symbol-Specific Configuration Models

Instead of duplicating `ExchangeSpecificConfig`, we'll create focused models for symbol mapping:

```python
# New models to complement existing configuration

class SymbolMappingConfig(BaseModel):
    """Configuration specifically for symbol mappings across exchanges."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    mappings: dict[str, dict[str, str]]  # {exchange_id: {internal: exchange_symbol}}

    @field_validator('mappings')
    @classmethod
    def validate_mappings(cls, v: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
        """Validate symbol mappings across all exchanges."""
        if not v:
            raise ValueError("At least one exchange mapping is required")

        # Track all internal symbols and their exchange mappings
        symbol_coverage: dict[str, set[str]] = {}

        for exchange_id, symbols in v.items():
            if not symbols:
                raise ValueError(f"Exchange '{exchange_id}' has no symbol mappings")

            for internal_symbol in symbols.keys():
                if internal_symbol not in symbol_coverage:
                    symbol_coverage[internal_symbol] = set()
                symbol_coverage[internal_symbol].add(exchange_id)

        # Warn about symbols not available on all exchanges
        for symbol, exchanges in symbol_coverage.items():
            if len(exchanges) < len(v):
                missing = set(v.keys()) - exchanges
                logger.warning(
                    f"Symbol '{symbol}' not available on exchanges: {missing}"
                )

        return v

    @classmethod
    def from_app_settings(cls, settings: AppSettings) -> 'SymbolMappingConfig':
        """Create from existing AppSettings."""
        mappings = {}
        for exchange_id, config in settings.exchanges.items():
            if config.enabled and config.symbols:
                mappings[exchange_id] = config.symbols
        return cls(mappings=mappings)


class SymbolValidationResult(BaseModel):
    """Result of symbol validation operations."""

    model_config = ConfigDict(frozen=True)

    valid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    validated_mappings: dict[str, dict[str, str]] | None = None

    @property
    def has_errors(self) -> bool:
        """Check if validation has errors."""
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        """Check if validation has warnings."""
        return len(self.warnings) > 0
```

## 3. Enhancing the Existing Protocol

### 3.1 Extended ISymbolMapper Protocol

The existing `ISymbolMapper` in `interfaces.py` only has basic methods. We'll extend it:

```python
# In cyberdelta/core/services/interfaces.py - extend the existing protocol

from typing import Protocol, runtime_checkable


@runtime_checkable
class ISymbolMapper(Protocol):
    """Extended protocol for symbol mapping operations."""

    # Existing methods
    def get_exchange_symbol(self, internal_symbol: str, exchange_id: str) -> str | None:
        """Get exchange-specific symbol from internal symbol."""
        ...

    def get_internal_symbol(self, exchange_symbol: str, exchange_id: str) -> str | None:
        """Get internal symbol from exchange-specific symbol."""
        ...

    # New methods to add
    def get_all_internal_symbols(self) -> list[str]:
        """Get all configured internal symbols."""
        ...

    def get_exchange_symbols_for_internal(self, internal_symbol: str) -> dict[str, str]:
        """Get all exchange symbols for an internal symbol."""
        ...

    def get_internal_symbols_for_exchange(self, exchange_id: str) -> dict[str, str]:
        """Get all internal symbols for an exchange."""
        ...

    def is_symbol_supported(self, internal_symbol: str, exchange_id: str) -> bool:
        """Check if symbol is supported on exchange."""
        ...

    def validate_symbol_pair(
        self,
        internal_symbol: str,
        long_exchange: str,
        short_exchange: str
    ) -> tuple[bool, str | None]:
        """Validate symbol is available on both exchanges."""
        ...

    def get_symbol_validation_result(
        self, internal_symbol: str, exchange_ids: list[str]
    ) -> SymbolValidationResult:
        """Get detailed validation result for symbol across exchanges."""
        ...


class ISymbolMapperCache(Protocol):
    """Protocol for caching symbol lookups."""

    def get(self, key: tuple[str, str]) -> str | None:
        """Get cached value."""
        ...

    def set(self, key: tuple[str, str], value: str) -> None:
        """Set cached value."""
        ...

    def clear(self) -> None:
        """Clear cache."""
        ...
```

### 3.2 Value Objects for Type Safety

```python
from typing import NewType, final
from dataclasses import dataclass


# Type aliases for clarity
InternalSymbol = NewType('InternalSymbol', str)
ExchangeSymbol = NewType('ExchangeSymbol', str)
ExchangeId = NewType('ExchangeId', str)


@final
@dataclass(frozen=True, slots=True)
class SymbolPair:
    """Immutable representation of a symbol mapping."""

    internal: InternalSymbol
    exchange: ExchangeSymbol
    exchange_id: ExchangeId

    def __post_init__(self) -> None:
        """Validate fields are not empty."""
        if not self.internal or not self.exchange or not self.exchange_id:
            raise ValueError("All fields must be non-empty")


@final
@dataclass(frozen=True, slots=True)
class SymbolLookupResult:
    """Result of a symbol lookup operation."""

    found: bool
    symbol: str | None = None
    error: str | None = None

    @classmethod
    def success(cls, symbol: str) -> 'SymbolLookupResult':
        """Create successful result."""
        return cls(found=True, symbol=symbol)

    @classmethod
    def not_found(cls, error: str) -> 'SymbolLookupResult':
        """Create not found result."""
        return cls(found=False, error=error)
```

## 4. Enhanced SymbolMapper Implementation

### 4.1 Thread-Safe Implementation with Validation

```python
from threading import RLock
from functools import lru_cache
from collections import defaultdict
from typing import Final
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.config.models import AppSettings


logger = get_logger(__name__)


class EnhancedSymbolMapper:
    """Enhanced thread-safe symbol mapper that works with existing config."""

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize with validated AppSettings."""
        self._lock: Final[RLock] = RLock()
        self._settings: Final[AppSettings] = app_settings

        # Initialize mappings
        self._internal_to_exchange: Final[dict[str, dict[str, str]]] = {}
        self._exchange_to_internal: Final[dict[str, dict[str, str]]] = {}
        self._all_internal_symbols: Final[set[str]] = set()

        # Process configuration
        self._process_configuration()

        # Freeze the instance
        self._initialized = True

    def _process_configuration(self) -> None:
        """Process validated configuration into mappings."""
        with self._lock:
            for exchange_id, exchange_config in self._config.exchanges.items():
                if not exchange_config.enabled:
                    continue

                self._exchange_to_internal[exchange_id] = {}

                for internal, exchange in exchange_config.symbols.items():
                    # Add to internal -> exchange mapping
                    if internal not in self._internal_to_exchange:
                        self._internal_to_exchange[internal] = {}

                    # Check for duplicates
                    if exchange_id in self._internal_to_exchange[internal]:
                        raise ValueError(
                            f"Duplicate internal symbol '{internal}' "
                            f"for exchange '{exchange_id}'"
                        )

                    self._internal_to_exchange[internal][exchange_id] = exchange

                    # Add to exchange -> internal mapping
                    if exchange in self._exchange_to_internal[exchange_id]:
                        existing = self._exchange_to_internal[exchange_id][exchange]
                        raise ValueError(
                            f"Exchange symbol '{exchange}' on '{exchange_id}' "
                            f"maps to both '{existing}' and '{internal}'"
                        )

                    self._exchange_to_internal[exchange_id][exchange] = internal
                    self._all_internal_symbols.add(internal)

    @lru_cache(maxsize=1024)
    def get_exchange_symbol(self, internal_symbol: str, exchange_id: str) -> str | None:
        """Get exchange symbol with caching."""
        if not internal_symbol or not exchange_id:
            return None

        with self._lock:
            return self._internal_to_exchange.get(internal_symbol, {}).get(exchange_id)

    @lru_cache(maxsize=1024)
    def get_internal_symbol(self, exchange_symbol: str, exchange_id: str) -> str | None:
        """Get internal symbol with caching."""
        if not exchange_symbol or not exchange_id:
            return None

        with self._lock:
            return self._exchange_to_internal.get(exchange_id, {}).get(exchange_symbol)

    def get_all_internal_symbols(self) -> list[str]:
        """Get all internal symbols."""
        with self._lock:
            return sorted(self._all_internal_symbols)

    def get_exchange_symbols_for_internal(self, internal_symbol: str) -> dict[str, str]:
        """Get all exchange mappings for internal symbol."""
        with self._lock:
            return self._internal_to_exchange.get(internal_symbol, {}).copy()

    def get_internal_symbols_for_exchange(self, exchange_id: str) -> dict[str, str]:
        """Get all internal mappings for exchange."""
        with self._lock:
            return self._exchange_to_internal.get(exchange_id, {}).copy()

    def is_symbol_supported(self, internal_symbol: str, exchange_id: str) -> bool:
        """Check if symbol is supported."""
        return self.get_exchange_symbol(internal_symbol, exchange_id) is not None

    def validate_symbol_pair(
        self,
        internal_symbol: str,
        long_exchange: str,
        short_exchange: str
    ) -> tuple[bool, str | None]:
        """Validate symbol availability on both exchanges."""
        long_symbol = self.get_exchange_symbol(internal_symbol, long_exchange)
        if not long_symbol:
            return False, f"Symbol '{internal_symbol}' not available on {long_exchange}"

        short_symbol = self.get_exchange_symbol(internal_symbol, short_exchange)
        if not short_symbol:
            return False, f"Symbol '{internal_symbol}' not available on {short_exchange}"

        return True, None

    def __setattr__(self, name: str, value: object) -> None:
        """Prevent modification after initialization."""
        if hasattr(self, '_initialized') and self._initialized:
            raise AttributeError(f"SymbolMapper is immutable after initialization")
        super().__setattr__(name, value)
```

## 5. Factory Pattern Integration with Existing System

```python
from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.config.models import AppSettings


class SymbolMapperFactory:
    """Factory that integrates with existing ConfigManager."""

    @staticmethod
    def create_from_app_settings(settings: AppSettings) -> ISymbolMapper:
        """Create SymbolMapper from validated AppSettings."""
        # Extract exchanges config
        exchanges_config = {
            exchange_id: config.model_dump()
            for exchange_id, config in settings.exchanges.items()
            if config.enabled
        }

        # Create the current SymbolMapper with enhanced validation
        mapper = SymbolMapper(exchanges_config)

        # Verify it implements the extended protocol
        if not isinstance(mapper, ISymbolMapper):
            raise TypeError("SymbolMapper must implement ISymbolMapper protocol")

        return mapper

    @staticmethod
    def create_enhanced(settings: AppSettings) -> ISymbolMapper:
        """Create enhanced SymbolMapper with additional features."""
        return EnhancedSymbolMapper(settings)

    @staticmethod
    def create_mock(mappings: dict[str, dict[str, str]]) -> ISymbolMapper:
        """Create mock SymbolMapper for testing."""
        # Build config from simple mappings
        exchanges = {}
        for exchange_id, symbols in mappings.items():
            exchanges[exchange_id] = ExchangeConfig(
                exchange_name=exchange_id,
                enabled=True,
                api_base_url_mainnet="https://mock.api",
                ws_url_mainnet="wss://mock.ws",
                symbols=symbols
            )

        config = SymbolMapperConfig(exchanges=exchanges)
        return SymbolMapper(config)
```

## 6. Integration with Existing System

```python
# In main.py - minimal changes to existing code
from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.core.symbol_mapper import SymbolMapper, SymbolMapperFactory

async def main() -> None:
    """Enhanced main with better symbol mapper."""
    # Use existing ConfigManager
    config_manager = ConfigManager()
    settings = config_manager.load_config()

    # Option 1: Use existing SymbolMapper (no code change)
    symbol_mapper = SymbolMapper(settings.model_dump()["exchanges"])

    # Option 2: Use factory for enhanced version
    symbol_mapper = SymbolMapperFactory.create_enhanced(settings)

    # Rest of initialization remains the same
    # ...

# Enhanced usage with validation
from cyberdelta.core.symbol_mapper import InternalSymbol, ExchangeId

# Type-safe symbol handling
internal_symbol = InternalSymbol("BTC")
exchange_id = ExchangeId("hyperliquid")

# Get validation result with details
validation = symbol_mapper.get_symbol_validation_result(
    "BTC",
    ["hyperliquid", "backpack"]
)
if not validation.valid:
    logger.error("Symbol validation failed", errors=validation.errors)
    raise ValueError(f"Symbol validation failed: {validation.errors}")

# Proceed with validated symbols
exchange_symbol = symbol_mapper.get_exchange_symbol(
    internal_symbol,
    exchange_id
)
```

## 7. Testing with Type Safety

```python
import pytest
from pydantic import ValidationError


def test_symbol_mapping_validation():
    """Test Pydantic validation catches errors."""
    # Invalid symbol format
    with pytest.raises(ValidationError) as exc:
        SymbolMapping(
            internal_symbol="",  # Empty not allowed
            exchange_symbol="BTC-PERP"
        )
    assert "Symbol cannot be empty" in str(exc.value)

    # Invalid config structure
    with pytest.raises(ValidationError) as exc:
        SymbolMapperConfig(exchanges={})  # No exchanges
    assert "At least one exchange" in str(exc.value)


def test_type_safe_usage():
    """Test Protocol compliance."""
    mapper = SymbolMapperFactory.create_mock({
        "hyperliquid": {"BTC": "BTC", "ETH": "ETH"},
        "backpack": {"BTC": "BTC_PERP", "ETH": "ETH_PERP"}
    })

    # Verify it implements the protocol
    assert isinstance(mapper, ISymbolMapper)

    # Type-safe operations
    result = mapper.get_exchange_symbol("BTC", "hyperliquid")
    assert result == "BTC"
```

## 8. Benefits of This Approach

### 8.1 Compile-Time Safety
- Pydantic models validate at object construction
- Protocol ensures interface compliance
- Type hints catch errors before runtime

### 8.2 Runtime Validation
- All inputs validated through Pydantic
- Immutable after initialization
- Thread-safe operations

### 8.3 Better Developer Experience
- Clear error messages from Pydantic
- Auto-completion in IDEs
- Self-documenting code

### 8.4 Testability
- Easy to mock with Protocol
- Validation logic separated
- Factory pattern for different scenarios

## 9. Migration Strategy

### Phase 1: Add Pydantic Models (Day 1-2)
1. Create Pydantic models alongside existing code
2. Add validation in factory method
3. Keep existing API unchanged

### Phase 2: Introduce Protocol (Day 3-4)
1. Define ISymbolMapper protocol
2. Update type hints to use Protocol
3. Verify existing code complies

### Phase 3: Refactor Implementation (Day 5-7)
1. Implement thread-safe version
2. Add caching and validation methods
3. Update all consumers to use new features

### Phase 4: Remove Legacy Code (Week 2)
1. Remove old implementation
2. Update all tests
3. Document new patterns

## 10. Key Differences from Initial Proposal

After analyzing the existing codebase, here are the key adjustments:

### What Already Exists:
1. **Pydantic Configuration**: `ExchangeSpecificConfig` already has `symbols: dict[str, str]`
2. **ISymbolMapper Protocol**: Already defined in `interfaces.py` (minimal version)
3. **ConfigManager**: Handles all configuration loading and validation
4. **Structured Logging**: Uses `structlog` via `get_logger()`

### What We're Adding:
1. **Enhanced Validation**: Better symbol format validation in existing models
2. **Extended Protocol**: Additional methods for the `ISymbolMapper` interface
3. **Value Objects**: Type-safe wrappers like `InternalSymbol`, `ExchangeId`
4. **Thread Safety**: Proper locking in SymbolMapper implementation
5. **Validation Results**: Detailed validation feedback with `SymbolValidationResult`

### Integration Strategy:
1. **Minimal Breaking Changes**: Enhance existing classes rather than replace
2. **Backward Compatible**: Existing code continues to work
3. **Gradual Migration**: Can adopt new features incrementally
4. **Leverage Existing**: Build on `AppSettings` and `ConfigManager`

## 11. Conclusion

By working with the existing configuration system rather than replacing it, we can achieve:

1. **Better Type Safety** without breaking existing code
2. **Enhanced Validation** using the current Pydantic models
3. **Extended Functionality** through protocol additions
4. **Improved Thread Safety** in the implementation
5. **Seamless Integration** with current `ConfigManager` workflow

This approach respects the existing architecture while addressing all type safety concerns identified in the original analysis.

# Pydantic Configuration Migration Guide

## Overview

This guide helps you migrate from stdlib dataclasses to Pydantic dataclasses in the portfolio configuration system.

## Key Changes

### 1. Import Changes

**Before:**
```python
from dataclasses import dataclass, field
from typing import Optional
```

**After:**
```python
from pydantic import Field, field_validator
from pydantic.dataclasses import dataclass
```

### 2. Field Definitions

**Before:**
```python
@dataclass
class CacheConfiguration:
    max_size: int = 10000
    default_ttl: float = 3600.0
    enabled: bool = True
```

**After:**
```python
@dataclass
class CacheConfiguration:
    max_size: int = Field(default=10000, gt=0, le=1000000, description="Maximum cache entries")
    default_ttl: float = Field(default=3600.0, gt=0, le=86400, description="Default TTL in seconds")
    enabled: bool = Field(default=True, description="Enable caching")
```

### 3. Validation

**Before (manual validation):**
```python
def __post_init__(self):
    if self.max_size <= 0:
        raise ValueError("max_size must be positive")
    if self.max_size > 1000000:
        raise ValueError("max_size too large")
```

**After (automatic validation):**
```python
@field_validator("max_size", mode="before")
@classmethod
def validate_max_size(cls, v: int | str) -> int:
    # Automatic string conversion
    if isinstance(v, str):
        v = int(v)
    # Field(gt=0, le=1000000) handles bounds checking
    return v
```

### 4. Optional Fields

**Before:**
```python
liquidation_price: Optional[Decimal] = None
```

**After:**
```python
liquidation_price: Decimal | None = Field(default=None, gt=0, description="Liquidation price if applicable")
```

### 5. Complex Validations

**Before:**
```python
def validate(self) -> list[str]:
    errors = []
    if self.min_price >= self.max_price:
        errors.append("min_price must be less than max_price")
    return errors
```

**After:**
```python
@field_validator("max_price", mode="after")
@classmethod  
def validate_price_range(cls, v: Decimal, info: Any) -> Decimal:
    if "min_price" in info.data:
        min_price = info.data["min_price"]
        if min_price >= v:
            raise ValueError("max_price must be greater than min_price")
    return v
```

## Common Patterns

### 1. Decimal Validation

```python
@field_validator("price", "amount", mode="before")
@classmethod
def validate_decimals(cls, v: Decimal) -> Decimal:
    if not isinstance(v, Decimal):
        v = Decimal(str(v))
    if not v.is_finite():
        raise ValueError("Value must be finite")
    return v
```

### 2. String Normalization

```python
@field_validator("symbol", mode="before")
@classmethod
def normalize_symbol(cls, v: str) -> str:
    if not v or not isinstance(v, str):
        raise ValueError("Symbol must be a non-empty string")
    return v.upper().strip()
```

### 3. Enum Validation

```python
@field_validator("side", mode="before")
@classmethod
def validate_side(cls, v: str) -> str:
    if v not in ["LONG", "SHORT"]:
        raise ValueError(f"Invalid side: {v}")
    return v
```

### 4. Cross-Field Validation

```python
@field_validator("end_time", mode="after")
@classmethod
def validate_time_range(cls, v: int, info: Any) -> int:
    if "start_time" in info.data:
        if v <= info.data["start_time"]:
            raise ValueError("end_time must be after start_time")
    return v
```

## Factory Method Pattern

For classes with custom __init__ methods:

```python
@dataclass
class Event:
    metadata: EventMetadata
    data: EventData
    
    @classmethod
    def create(cls, data: EventData, **metadata_kwargs) -> Event:
        metadata = EventMetadata(**metadata_kwargs)
        return cls(metadata=metadata, data=data)
```

## Testing Validation

```python
import pytest
from pydantic import ValidationError

def test_invalid_configuration():
    with pytest.raises(ValueError) as exc_info:
        CacheConfiguration(max_size=-1)
    assert "greater than 0" in str(exc_info.value)

def test_valid_configuration():
    config = CacheConfiguration(max_size=5000)
    assert config.max_size == 5000
```

## Dictionary Integration

When passing configuration as dictionaries, rely on Pydantic's validation:

```python
def update_cache_config(config: dict[str, Any]) -> CacheConfiguration:
    # Pydantic will validate the dictionary contents
    return CacheConfiguration(**config)
```

## Serialization

```python
# To dictionary
config_dict = config.to_dict()

# From dictionary  
config = PortfolioConfiguration(**config_dict)

# JSON serialization
import json
json_str = json.dumps(config.to_dict())
```

## Common Gotchas

1. **String Coercion**: Always handle string inputs in validators
2. **Field Order**: Validators run in field definition order
3. **Mode Selection**: Use `mode="before"` for type conversion, `mode="after"` for cross-field validation
4. **Type Annotations**: Must include type hints for all fields
5. **Default Values**: Use Field(default=...) not Python defaults

## Benefits

- **Automatic Validation**: No manual validation code needed
- **Type Conversion**: Automatic string to number conversion
- **Better Errors**: Descriptive validation error messages
- **IDE Support**: Full autocomplete and type checking
- **Serialization**: Built-in JSON/dict conversion
- **Documentation**: Field descriptions in Field() definitions
# Service Base Class Inheritance Patterns

## Overview
This document establishes consistent inheritance patterns for portfolio services to ensure architectural consistency and proper lifecycle management.

## Base Classes

### 1. BasePortfolioService (Primary Service Base)
**Location**: `services/base/base_service.py`
**Purpose**: Abstract base class for all portfolio infrastructure services
**Features**:
- Basic service lifecycle (start/stop)
- Configuration management via `ServiceConfiguration`
- Thread-safe operations with locks
- Proper logging and error handling

**Usage Pattern**:
```python
class MyService(BasePortfolioService):
    def __init__(self, name: str = "MyService", config: dict[str, object] | None = None):
        super().__init__(name, config)
```

### 2. ServiceLifecycle (Advanced Service Base)
**Location**: `services/base/service_lifecycle.py`
**Purpose**: Enhanced service base with advanced lifecycle management
**Features**:
- All BasePortfolioService features
- Health check management
- Service metrics collection
- Startup/shutdown handlers
- Service status tracking

**Current Usage**: Not currently used by any services
**Recommendation**: Consider for services requiring health monitoring

### 3. ServiceConfiguration (Shared Configuration)
**Location**: `services/base/base_service.py`
**Purpose**: Validated configuration model for all services
**Features**:
- Pydantic validation
- Health check settings
- Timeout configurations
- Service naming

## Current Service Inheritance Patterns

### ✅ Consistent Services (Using BasePortfolioService)
- `PortfolioAnalyticsService`
- `PortfolioAuditTrailService`
- `PortfolioBackupService`
- `PortfolioConfigManager`
- `PortfolioHealthMonitor`
- `PortfolioMetricsAggregationService`
- `PortfolioReconciliationService`
- `PortfolioResilienceService`
- `SymbolNormalizationService`

### ❌ Inconsistent Services
- `StatePersistenceService` - inherits from `BaseStateModel` (Pydantic model)

## Inheritance Rules

### Rule 1: Service vs Model Separation
- **Services** should inherit from service base classes (`BasePortfolioService` or `ServiceLifecycle`)
- **Models** should inherit from model base classes (`BaseModel`, `BaseStateModel`)
- A class should not be both a service and a Pydantic model

### Rule 2: Configuration Pattern
- Services should use `ServiceConfiguration` or extend it
- Configuration should be passed to `super().__init__(name, config)`
- Service-specific config should be separate models

### Rule 3: Lifecycle Methods
- Override `_start_internal()` and `_stop_internal()` for custom lifecycle
- Use async methods for I/O operations
- Implement proper error handling and logging

### Rule 4: Naming Convention
- Service classes: `[Domain][Purpose]Service` (e.g., `PortfolioAnalyticsService`)
- Default service name should match class name
- Configuration classes: `[Service]Configuration`

## Recommendations

### Immediate Fixes
1. **StatePersistenceService**: Consider refactoring to inherit from `BasePortfolioService` while preserving Pydantic functionality through composition

### Long-term Improvements
1. **ServiceLifecycle Adoption**: Evaluate services that would benefit from health checks and metrics
2. **Base Class Consolidation**: Consider merging `BasePortfolioService` and `ServiceLifecycle` if the advanced features are generally needed
3. **Configuration Standardization**: Ensure all service configurations follow the same validation patterns

## Migration Guide

### Converting to BasePortfolioService
```python
# Before
class MyService(SomeOtherBase):
    def __init__(self, config_dict):
        self.config = config_dict

# After  
class MyService(BasePortfolioService):
    def __init__(self, name: str = "MyService", config: dict[str, object] | None = None):
        super().__init__(name, config)
        # Service-specific initialization here
```

### Adding Service Lifecycle
```python
class MyService(BasePortfolioService):
    async def _start_internal(self) -> None:
        """Custom startup logic."""
        await self._initialize_resources()
        
    async def _stop_internal(self) -> None:
        """Custom shutdown logic."""
        await self._cleanup_resources()
```

This document should be updated as inheritance patterns evolve.
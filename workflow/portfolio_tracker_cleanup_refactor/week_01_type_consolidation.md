# Week 1: Type System Consolidation - Clean Break Approach

**Duration:** Week 1 (2025-07-28 to 2025-08-03)
**Approach:** Clean Break - No Backward Compatibility
**Priority:** Critical
**Objective:** Consolidate 17 type files into 4 clean modules, remove legacy imports

## Overview

The modular system has 17 separate type definition files creating unnecessary complexity for a 0.0.1 system. This week consolidates them into 4 focused modules and removes all legacy type dependencies.

**Clean Break Strategy:**
- ❌ No adapters or compatibility layers
- ❌ No gradual migration
- ✅ Direct replacement of old with new
- ✅ Breaking changes are acceptable for 0.0.1

## Target Type Architecture

### BEFORE: 17 Fragmented Files
```
portfolio_types/
├── annotated_types.py
├── calculation_types.py
├── data_transfer_objects.py
├── discriminated_unions.py
├── domain_models.py
├── exception_models.py
├── manager_protocols.py
├── portfolio_data_models.py
├── portfolio_models.py
├── resilience_types.py
├── result_types.py
├── serializable_protocol.py
├── service_protocols.py
├── state_types.py
├── type_guards.py
├── update_models.py
└── validation_types.py
```

### AFTER: 4 Clean Modules
```
portfolio_types/
├── __init__.py
├── models.py          # All data models and domain objects
├── protocols.py       # All protocol definitions
├── calculations.py    # Calculation inputs/outputs and financial types
└── infrastructure.py  # Events, exceptions, validation, state management
```

## Week 1 Deliverables

### Day 1-2: Analysis & Consolidation Plan
- [ ] **Analyze Current Type Usage**
  ```bash
  # Find all imports of portfolio_types
  rg "from cyberdelta.core.portfolio.portfolio_types" --type py
  rg "import.*portfolio_types" --type py
  ```

- [ ] **Create Consolidation Mapping**
  ```python
  consolidation_map = {
      # TARGET: models.py
      "domain_models.py": "models.py",
      "portfolio_data_models.py": "models.py",
      "portfolio_models.py": "models.py",
      "data_transfer_objects.py": "models.py",
      "annotated_types.py": "models.py",

      # TARGET: protocols.py
      "service_protocols.py": "protocols.py",
      "manager_protocols.py": "protocols.py",
      "serializable_protocol.py": "protocols.py",

      # TARGET: calculations.py
      "calculation_types.py": "calculations.py",
      "result_types.py": "calculations.py",

      # TARGET: infrastructure.py
      "state_types.py": "infrastructure.py",
      "exception_models.py": "infrastructure.py",
      "validation_types.py": "infrastructure.py",
      "resilience_types.py": "infrastructure.py",
      "update_models.py": "infrastructure.py",
      "discriminated_unions.py": "infrastructure.py",
      "type_guards.py": "infrastructure.py",
  }
  ```

### Day 3-4: Create New Consolidated Modules

- [ ] **models.py - All Data Models**
  ```python
  """Portfolio data models and domain objects."""
  from __future__ import annotations

  from decimal import Decimal
  from datetime import datetime
  from typing import Any
  from pydantic import BaseModel, Field

  # Portfolio State Models
  class PortfolioState(BaseModel):
      """Complete portfolio state snapshot."""
      total_capital: Decimal
      positions: dict[str, list[Position]]
      balances: dict[str, dict[str, SpotBalance]]
      exposure_metrics: ExposureMetrics
      timestamp: datetime

  # Position Models
  class Position(BaseModel):
      """Position data model."""
      exchange: str
      symbol: str
      size: Decimal
      entry_price: Decimal | None = None
      mark_price: Decimal | None = None
      unrealized_pnl: Decimal | None = None

  # Balance Models
  class SpotBalance(BaseModel):
      """Spot balance data model."""
      exchange: str
      asset: str
      total_quantity: Decimal
      available_quantity: Decimal
      timestamp: datetime

  # Exposure Models
  class ExposureMetrics(BaseModel):
      """Portfolio exposure metrics."""
      total_exposure: Decimal
      currency_exposures: dict[str, Decimal]
      position_count: int
      leverage_ratio: Decimal | None = None

  # Configuration Models
  class PortfolioConfig(BaseModel):
      """Portfolio configuration."""
      base_currency: str = "USDC"
      enable_pnl_tracking: bool = True
      enable_exposure_monitoring: bool = True
      # ... other config fields
  ```

- [ ] **protocols.py - All Protocol Definitions**
  ```python
  """Portfolio protocols and interfaces."""
  from __future__ import annotations

  from abc import abstractmethod
  from decimal import Decimal
  from typing import Protocol, runtime_checkable

  from .models import PortfolioState, Position, SpotBalance, ExposureMetrics

  @runtime_checkable
  class PortfolioManagerProtocol(Protocol):
      """Core portfolio management interface."""

      @abstractmethod
      async def get_total_capital(self, base_currency: str = "USDC") -> Decimal: ...

      @abstractmethod
      async def get_positions(self, exchange_id: str | None = None) -> list[Position]: ...

      @abstractmethod
      async def get_balances(self, exchange_id: str | None = None) -> dict[str, SpotBalance]: ...

      @abstractmethod
      async def get_exposure_metrics(self, valuation_asset: str = "USDC") -> ExposureMetrics: ...

  @runtime_checkable
  class CalculatorProtocol(Protocol):
      """Financial calculator interface."""

      @abstractmethod
      async def calculate(self, input_data: Any) -> Any: ...

  @runtime_checkable
  class ServiceProtocol(Protocol):
      """Base service interface."""

      @abstractmethod
      async def initialize(self) -> None: ...

      @abstractmethod
      async def shutdown(self) -> None: ...
  ```

- [ ] **calculations.py - Financial Calculations**
  ```python
  """Financial calculation types and results."""
  from __future__ import annotations

  from decimal import Decimal
  from datetime import datetime
  from typing import Any
  from pydantic import BaseModel

  # Calculation Inputs
  class PerformanceInput(BaseModel):
      """Input for performance calculations."""
      positions: list[Position]
      balances: dict[str, SpotBalance]
      base_currency: str = "USDC"

  class ExposureInput(BaseModel):
      """Input for exposure calculations."""
      positions: list[Position]
      valuation_currency: str = "USDC"

  # Calculation Results
  class PerformanceResult(BaseModel):
      """Performance calculation result."""
      total_capital: Decimal
      realized_pnl: Decimal
      unrealized_pnl: Decimal
      high_watermark: Decimal | None = None
      drawdown: Decimal | None = None

  class ExposureResult(BaseModel):
      """Exposure calculation result."""
      total_exposure: Decimal
      currency_breakdown: dict[str, Decimal]
      risk_metrics: dict[str, Decimal]

  # Financial Types
  class PnLBreakdown(BaseModel):
      """Profit and loss breakdown."""
      realized: Decimal
      unrealized: Decimal
      total: Decimal
      by_exchange: dict[str, Decimal]
      by_symbol: dict[str, Decimal]
  ```

- [ ] **infrastructure.py - Infrastructure Types**
  ```python
  """Infrastructure types for events, validation, state management."""
  from __future__ import annotations

  from enum import Enum
  from datetime import datetime
  from typing import Any, Literal
  from pydantic import BaseModel

  # State Management
  class StateSnapshot(BaseModel):
      """Portfolio state snapshot."""
      data: dict[str, Any]
      timestamp: datetime
      version: str
      checksum: str

  # Events
  class EventType(str, Enum):
      """Portfolio event types."""
      BALANCE_UPDATED = "balance_updated"
      POSITION_UPDATED = "position_updated"
      ORDER_FILLED = "order_filled"
      TRADE_EXECUTED = "trade_executed"

  class PortfolioEvent(BaseModel):
      """Portfolio event base class."""
      event_type: EventType
      exchange_id: str
      timestamp: datetime
      data: dict[str, Any]

  # Validation
  class ValidationResult(BaseModel):
      """Validation result."""
      is_valid: bool
      errors: list[str] = []
      warnings: list[str] = []

  # Exceptions
  class PortfolioError(Exception):
      """Base portfolio exception."""
      pass

  class CalculationError(PortfolioError):
      """Calculation error."""
      pass

  class StateError(PortfolioError):
      """State management error."""
      pass

  # Type Guards
  def is_valid_exchange_id(value: str) -> bool:
      """Check if exchange ID is valid."""
      return isinstance(value, str) and len(value) > 0

  def is_valid_symbol(value: str) -> bool:
      """Check if symbol is valid."""
      return isinstance(value, str) and "-" in value or "/" in value
  ```

### Day 5-7: Mass Import Replacement

- [ ] **Update All Import Statements**
  ```python
  # Create automated replacement script
  import_replacements = {
      # Old fragmented imports
      "from cyberdelta.core.portfolio.portfolio_types.domain_models import":
          "from cyberdelta.core.portfolio.portfolio_types.models import",

      "from cyberdelta.core.portfolio.portfolio_types.service_protocols import":
          "from cyberdelta.core.portfolio.portfolio_types.protocols import",

      "from cyberdelta.core.portfolio.portfolio_types.calculation_types import":
          "from cyberdelta.core.portfolio.portfolio_types.calculations import",

      # ... complete mapping for all 17 files
  }
  ```

- [ ] **Remove Old Type Files**
  ```bash
  # After all imports updated, delete old files
  rm cyberdelta/core/portfolio/portfolio_types/annotated_types.py
  rm cyberdelta/core/portfolio/portfolio_types/calculation_types.py
  # ... remove all 17 files except the 4 new ones
  ```

- [ ] **Update __init__.py**
  ```python
  """Consolidated portfolio types."""

  # Models
  from .models import (
      PortfolioState,
      Position,
      SpotBalance,
      ExposureMetrics,
      PortfolioConfig,
  )

  # Protocols
  from .protocols import (
      PortfolioManagerProtocol,
      CalculatorProtocol,
      ServiceProtocol,
  )

  # Calculations
  from .calculations import (
      PerformanceInput,
      PerformanceResult,
      ExposureInput,
      ExposureResult,
      PnLBreakdown,
  )

  # Infrastructure
  from .infrastructure import (
      StateSnapshot,
      EventType,
      PortfolioEvent,
      ValidationResult,
      PortfolioError,
      CalculationError,
      StateError,
      is_valid_exchange_id,
      is_valid_symbol,
  )

  __all__ = [
      # Models
      "PortfolioState", "Position", "SpotBalance", "ExposureMetrics", "PortfolioConfig",
      # Protocols
      "PortfolioManagerProtocol", "CalculatorProtocol", "ServiceProtocol",
      # Calculations
      "PerformanceInput", "PerformanceResult", "ExposureInput", "ExposureResult", "PnLBreakdown",
      # Infrastructure
      "StateSnapshot", "EventType", "PortfolioEvent", "ValidationResult",
      "PortfolioError", "CalculationError", "StateError",
      "is_valid_exchange_id", "is_valid_symbol",
  ]
  ```

## Implementation Strategy

### Automated Mass Replacement
```python
#!/usr/bin/env python3
"""Script to replace all portfolio type imports."""

import os
import re
from pathlib import Path

def replace_imports_in_file(file_path: Path, replacements: dict[str, str]) -> bool:
    """Replace imports in a single file."""
    try:
        content = file_path.read_text()
        original_content = content

        for old_import, new_import in replacements.items():
            content = content.replace(old_import, new_import)

        if content != original_content:
            file_path.write_text(content)
            print(f"Updated imports in {file_path}")
            return True
        return False

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Replace all portfolio type imports in the codebase."""

    # Define all import replacements
    replacements = {
        # From domain_models.py -> models.py
        "from cyberdelta.core.portfolio.portfolio_types.domain_models":
            "from cyberdelta.core.portfolio.portfolio_types.models",
        "from .domain_models": "from .models",

        # From service_protocols.py -> protocols.py
        "from cyberdelta.core.portfolio.portfolio_types.service_protocols":
            "from cyberdelta.core.portfolio.portfolio_types.protocols",
        "from .service_protocols": "from .protocols",

        # ... add all 17 file mappings
    }

    # Find all Python files in the project
    project_root = Path("cyberdelta")
    python_files = list(project_root.rglob("*.py"))

    updated_files = 0
    for file_path in python_files:
        if replace_imports_in_file(file_path, replacements):
            updated_files += 1

    print(f"Updated imports in {updated_files} files")

if __name__ == "__main__":
    main()
```

## Testing Strategy

### Immediate Validation
- [ ] **Import Validation**
  ```bash
  # Test that all imports resolve correctly
  python -c "from cyberdelta.core.portfolio.portfolio_types import *"
  ```

- [ ] **Type Checking**
  ```bash
  # Run mypy on entire portfolio module
  mypy cyberdelta/core/portfolio/ --strict
  ```

- [ ] **Unit Test Execution**
  ```bash
  # Run all portfolio tests to ensure nothing broken
  pytest tests/unit/portfolio/ -v
  ```

### Breaking Change Management
Since this is 0.0.1, breaking changes are acceptable:
- Document all breaking changes
- Update all affected components in the same week
- No backward compatibility required

## Success Metrics

### Technical Metrics
- [ ] **File Reduction**: 17 files → 4 files (77% reduction)
- [ ] **Import Simplification**: All imports use 4 modules instead of 17
- [ ] **Type Safety**: 100% mypy strict compliance maintained
- [ ] **Test Coverage**: No test failures from type consolidation

### Quality Metrics
- [ ] **Reduced Complexity**: Easier to understand type relationships
- [ ] **Import Performance**: Faster import times due to fewer files
- [ ] **Developer Experience**: Simpler imports, clearer organization
- [ ] **Maintainability**: Single source of truth for each type category

## Risk Management

### Technical Risks
1. **Circular Import Issues**
   - **Risk**: Consolidation may create circular dependencies
   - **Mitigation**: Careful dependency analysis and forward references
   - **Detection**: Import testing and mypy validation

2. **Missing Type Exports**
   - **Risk**: Some types may not be exported from new modules
   - **Mitigation**: Comprehensive __all__ definitions and import testing
   - **Detection**: Automated import verification script

### Breaking Change Risks
Since this is 0.0.1, breaking changes are expected and acceptable:
- All components must be updated in the same week
- No gradual rollout needed
- Focus on clean, simple final state

## Expected Outcomes

### Week 1 Deliverables
- [ ] **4 Consolidated Type Modules** - Clean, focused type organization
- [ ] **Updated Import Statements** - All imports use new module structure
- [ ] **Removed Legacy Files** - 17 old type files completely removed
- [ ] **Import Replacement Script** - Automated tooling for future consolidations
- [ ] **Validation Testing** - Comprehensive testing of new type structure

### System Benefits
- [ ] **Simplified Architecture** - Easier to understand and navigate
- [ ] **Improved Performance** - Fewer files to import and process
- [ ] **Better Maintainability** - Clear separation of concerns
- [ ] **Enhanced Developer Experience** - Intuitive import structure

### Foundation for Week 2
- [ ] **Clean Type Foundation** - Solid base for service consolidation
- [ ] **Proven Tooling** - Scripts and processes for mass refactoring
- [ ] **Team Experience** - Knowledge of clean-break refactoring approach
- [ ] **Validation Framework** - Testing approach for architectural changes

This clean-break approach eliminates technical debt immediately rather than carrying it forward with compatibility layers. For a 0.0.1 system, this aggressive consolidation is the right approach.

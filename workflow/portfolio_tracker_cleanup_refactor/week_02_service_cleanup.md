# Week 2: Service Architecture Cleanup - Clean Break Approach

**Duration:** Week 2 (2025-08-04 to 2025-08-10)
**Approach:** Clean Break - No Backward Compatibility
**Priority:** Critical
**Objective:** Break down oversized services, remove redundancy, create clean service architecture

## Overview

Current service layer has multiple violations:
- `portfolio_analytics_service.py`: 1,562 lines
- `portfolio_config_manager.py`: 1,170 lines
- `portfolio_metrics_aggregation_service.py`: 1,211 lines

**Clean Break Strategy:**
- ❌ No gradual refactoring of existing services
- ❌ No compatibility wrappers
- ✅ Complete replacement with focused services
- ✅ Single responsibility principle enforced

## Target Service Architecture

### BEFORE: Monolithic Services
```
services/
├── analytics/
│   └── portfolio_analytics_service.py    # 1,562 lines - VIOLATION
├── config/
│   └── portfolio_config_manager.py       # 1,170 lines - VIOLATION
├── metrics/
│   └── portfolio_metrics_aggregation_service.py  # 1,211 lines - VIOLATION
└── ... (other services)
```

### AFTER: Focused Services
```
services/
├── analytics/
│   ├── performance_analytics.py          # <300 lines
│   ├── risk_analytics.py                 # <300 lines
│   └── reporting_service.py              # <300 lines
├── config/
│   ├── config_loader.py                  # <200 lines
│   ├── config_validator.py               # <200 lines
│   └── config_persistence.py             # <200 lines
├── metrics/
│   ├── pnl_metrics.py                    # <300 lines
│   ├── exposure_metrics.py               # <300 lines
│   └── performance_metrics.py            # <300 lines
└── core/
    ├── base_service.py                   # <150 lines
    └── service_factory.py               # <200 lines
```

## Week 2 Deliverables

### Critical: Portfolio/Risk Module Boundary Cleanup
Before service breakdown, address major architectural boundary violations between portfolio and risk modules:

```python
# CRITICAL BOUNDARY VIOLATIONS FOUND
BOUNDARY_VIOLATIONS = {
    "portfolio_module_overreach": {
        "exposure_calculators": "4 exposure calculators belong in risk module",
        "risk_analytics": "Portfolio analytics contains VaR, stress testing",
        "risk_limits": "Portfolio enforces limits it shouldn't calculate",
        "position_sizing": "Duplicated position sizing logic"
    },
    "architecture_inconsistency": {
        "validation_frameworks": "Two separate validation systems",
        "service_patterns": "Different architectural approaches",
        "data_models": "Inconsistent exposure model definitions"
    }
}
```

**REQUIRED: Module Boundary Tasks for Week 2:**
- [ ] **Move Exposure Calculators to Risk Module** (Portfolio → Risk)
- [ ] **Extract Risk Logic from Portfolio Analytics** (Clean separation)
- [ ] **Consolidate Validation Frameworks** (Single approach)
- [ ] **Establish Clean Integration Patterns** (Portfolio ↔ Risk)

### Security Integration (Progressive Security Phase 1)
After boundary cleanup, implement foundational security practices:

```python
# Security requirements for service refactoring
SECURITY_STANDARDS = {
    "input_validation": "All service inputs validated with Pydantic models",
    "error_handling": "No sensitive data in error messages or logs",
    "api_key_handling": "Services must not log or expose API credentials",
    "data_sanitization": "Portfolio data sanitized before logging",
    "access_control": "Service interfaces validate caller permissions"
}
```

**Security Tasks for Week 2:**
- [ ] Add input validation to all new service interfaces
- [ ] Implement secure error handling patterns
- [ ] Audit existing services for hardcoded secrets
- [ ] Add data sanitization to portfolio logging functions
- [ ] Create service-level access control framework

### Day 1-2: Portfolio/Risk Boundary Analysis & Cleanup Plan

- [ ] **Critical Boundary Violation Analysis**
  ```python
  # Files that must move from portfolio to risk module
  MISPLACED_FILES = {
      "exposure_calculators": [
          "portfolio/calculators/exposure_calculator.py",
          "portfolio/calculators/portfolio_exposure_calculator.py",
          "portfolio/calculators/currency_exposure_calculator.py",
          "portfolio/calculators/position_exposure_calculator.py"
      ],
      "risk_logic_in_analytics": [
          "portfolio/services/analytics/portfolio_analytics_service.py:1216-1237",  # VaR calculations
          "portfolio/services/analytics/portfolio_analytics_service.py:890-950",   # Risk scoring
          "portfolio/services/analytics/portfolio_analytics_service.py:1100-1180" # Stress testing
      ]
  }
  ```

- [ ] **Define Clean Module Boundaries**
  ```python
  # CORRECT module responsibilities after cleanup
  MODULE_BOUNDARIES = {
      "portfolio_module": {
          "responsibilities": [
              "Position/balance state management",
              "Trade execution tracking",
              "P&L calculation (realized/unrealized)",
              "Performance metrics (returns, attribution)",
              "Data persistence and caching"
          ],
          "forbidden": ["Risk calculations", "Exposure metrics", "Position sizing"]
      },
      "risk_module": {
          "responsibilities": [
              "All exposure calculations and risk metrics",
              "Position sizing and allocation decisions",
              "Pre-trade risk validation",
              "Risk limit enforcement",
              "Stress testing and scenario analysis"
          ],
          "forbidden": ["Portfolio state management", "Trade execution", "P&L tracking"]
      }
  }
  ```

- [ ] **Create Integration Architecture**
  ```python
  # Clean integration pattern: Portfolio → Risk → Portfolio
  INTEGRATION_PATTERN = {
      "data_flow": [
          "1. Portfolio provides position/balance data to Risk",
          "2. Risk calculates exposures, limits, sizing",
          "3. Portfolio uses risk assessments for decisions",
          "4. Portfolio executes trades based on risk approval"
      ],
      "interface_design": {
          "portfolio_to_risk": "PortfolioState → RiskAssessment",
          "risk_to_portfolio": "RiskMetrics → TradingDecisions",
          "shared_models": "Position, Balance, ExposureMetrics"
      }
  }
  ```

### Day 1-2: Service Analysis & Decomposition Plan

- [ ] **Analyze Oversized Services**
  ```python
  # Analyze portfolio_analytics_service.py (1,562 lines)
  service_analysis = {
      "portfolio_analytics_service.py": {
          "line_count": 1562,
          "responsibilities": [
              "performance_analytics",    # ~400 lines
              "risk_analytics",          # ~350 lines
              "portfolio_reporting",     # ~300 lines
              "data_aggregation",        # ~250 lines
              "visualization_helpers",   # ~200 lines
              "configuration_management" # ~62 lines
          ]
      },
      "portfolio_config_manager.py": {
          "line_count": 1170,
          "responsibilities": [
              "config_loading",          # ~300 lines
              "config_validation",       # ~250 lines
              "config_persistence",      # ~200 lines
              "config_transformation",   # ~180 lines
              "config_migration",        # ~150 lines
              "environment_management"   # ~90 lines
          ]
      },
      "portfolio_metrics_aggregation_service.py": {
          "line_count": 1211,
          "responsibilities": [
              "pnl_aggregation",         # ~350 lines
              "exposure_aggregation",    # ~300 lines
              "performance_aggregation", # ~250 lines
              "real_time_metrics",       # ~200 lines
              "historical_metrics"       # ~111 lines
          ]
      }
  }
  ```

- [ ] **Define Clean Service Boundaries**
  ```python
  # New focused services with clear responsibilities
  new_service_architecture = {
      "analytics": {
          "performance_analytics.py": {
              "responsibility": "Calculate portfolio performance metrics",
              "max_lines": 300,
              "dependencies": ["portfolio_types.calculations", "portfolio_types.models"]
          },
          "risk_analytics.py": {
              "responsibility": "Calculate risk metrics and exposures",
              "max_lines": 300,
              "dependencies": ["portfolio_types.calculations", "portfolio_types.models"]
          },
          "reporting_service.py": {
              "responsibility": "Generate portfolio reports",
              "max_lines": 300,
              "dependencies": ["performance_analytics", "risk_analytics"]
          }
      },
      "config": {
          "config_loader.py": {
              "responsibility": "Load configuration from various sources",
              "max_lines": 200,
              "dependencies": ["portfolio_types.models"]
          },
          "config_validator.py": {
              "responsibility": "Validate configuration data",
              "max_lines": 200,
              "dependencies": ["portfolio_types.infrastructure"]
          },
          "config_persistence.py": {
              "responsibility": "Save and manage configuration persistence",
              "max_lines": 200,
              "dependencies": ["config_loader"]
          }
      }
  }
  ```

### Day 3-4: Create New Focused Services

- [ ] **Performance Analytics Service**
  ```python
  """Focused performance analytics service."""
  from __future__ import annotations

  from decimal import Decimal
  from typing import Any

  from pydantic import BaseModel, ConfigDict, Field

  from cyberdelta.core.portfolio.portfolio_types.models import PortfolioState
  from cyberdelta.core.portfolio.portfolio_types.calculations import (
      PerformanceInput,
      PerformanceResult
  )
  from cyberdelta.core.portfolio.portfolio_types.protocols import CalculatorProtocol

  class PerformanceAnalyticsService(BaseModel):
      """Calculates portfolio performance metrics only."""

      base_currency: str = Field(default="USDC", description="Base currency for calculations")

      model_config = ConfigDict(extra="forbid", validate_assignment=True)

      async def calculate_performance(
          self,
          portfolio_state: PortfolioState
      ) -> PerformanceResult:
          """Calculate performance metrics from portfolio state."""

          # Calculate total capital
          total_capital = await self._calculate_total_capital(portfolio_state)

          # Calculate P&L
          realized_pnl, unrealized_pnl = await self._calculate_pnl(portfolio_state)

          # Calculate drawdown if high watermark exists
          drawdown = await self._calculate_drawdown(total_capital)

          return PerformanceResult(
              total_capital=total_capital,
              realized_pnl=realized_pnl,
              unrealized_pnl=unrealized_pnl,
              drawdown=drawdown,
          )

      async def _calculate_total_capital(self, state: PortfolioState) -> Decimal:
          """Calculate total portfolio capital."""
          # Implementation focused only on capital calculation
          # No other concerns mixed in
          pass

      async def _calculate_pnl(self, state: PortfolioState) -> tuple[Decimal, Decimal]:
          """Calculate realized and unrealized P&L."""
          # Implementation focused only on P&L calculation
          pass

      async def _calculate_drawdown(self, current_capital: Decimal) -> Decimal | None:
          """Calculate current drawdown from high watermark."""
          # Implementation focused only on drawdown calculation
          pass
  ```

- [ ] **Config Loader Service**
  ```python
  """Clean configuration loading service."""
  from __future__ import annotations

  from pathlib import Path
  from typing import Any

  from pydantic import BaseModel, ConfigDict, Field

  from cyberdelta.core.portfolio.portfolio_types.models import PortfolioConfig
  from cyberdelta.core.portfolio.portfolio_types.infrastructure import ValidationResult

  class ConfigLoaderService(BaseModel):
      """Loads configuration from various sources only."""

      supported_formats: list[str] = Field(default_factory=lambda: [".yaml", ".json", ".toml"])

      model_config = ConfigDict(extra="forbid", validate_assignment=True)

      async def load_from_file(self, config_path: Path) -> PortfolioConfig:
          """Load configuration from file."""

          if not config_path.exists():
              raise FileNotFoundError(f"Config file not found: {config_path}")

          # Determine format and load
          suffix = config_path.suffix.lower()
          if suffix == ".yaml":
              return await self._load_yaml(config_path)
          elif suffix == ".json":
              return await self._load_json(config_path)
          elif suffix == ".toml":
              return await self._load_toml(config_path)
          else:
              raise ValueError(f"Unsupported config format: {suffix}")

      async def load_from_env(self) -> PortfolioConfig:
          """Load configuration from environment variables."""
          # Implementation focused only on environment loading
          pass

      async def _load_yaml(self, path: Path) -> PortfolioConfig:
          """Load YAML configuration."""
          # Single responsibility: YAML loading only
          pass
  ```

- [ ] **Base Service Pattern**
  ```python
  """Base service pattern for all portfolio services."""
  from __future__ import annotations

  from abc import ABC, abstractmethod
  from typing import Any

  from pydantic import BaseModel, ConfigDict, Field

  from cyberdelta.core.portfolio.portfolio_types.protocols import ServiceProtocol

  class BasePortfolioService(BaseModel, ServiceProtocol):
      """Base class for all portfolio services."""

      service_name: str = Field(..., description="Name of the service")
      _initialized: bool = Field(default=False, description="Service initialization state")

      model_config = ConfigDict(extra="forbid", validate_assignment=True)

      async def initialize(self) -> None:
          """Initialize the service."""
          if self._initialized:
              return

          await self._initialize_service()
          self._initialized = True

      async def shutdown(self) -> None:
          """Shutdown the service."""
          if not self._initialized:
              return

          await self._shutdown_service()
          self._initialized = False

      @abstractmethod
      async def _initialize_service(self) -> None:
          """Service-specific initialization."""
          pass

      @abstractmethod
      async def _shutdown_service(self) -> None:
          """Service-specific shutdown."""
          pass

      def is_initialized(self) -> bool:
          """Check if service is initialized."""
          return self._initialized
  ```

### Day 5-7: Service Integration & Legacy Removal

- [ ] **Service Factory for Clean Dependency Injection**
  ```python
  """Service factory for creating focused services."""
  from __future__ import annotations

  from typing import Any

  from .analytics.performance_analytics import PerformanceAnalyticsService
  from .analytics.risk_analytics import RiskAnalyticsService
  from .config.config_loader import ConfigLoaderService
  from .config.config_validator import ConfigValidatorService
  from .metrics.pnl_metrics import PnLMetricsService

  class PortfolioServiceFactory:
      """Creates and manages portfolio services."""

      def __init__(self):
          self._services: dict[str, Any] = {}

      def create_performance_analytics(self) -> PerformanceAnalyticsService:
          """Create performance analytics service."""
          if "performance_analytics" not in self._services:
              self._services["performance_analytics"] = PerformanceAnalyticsService()
          return self._services["performance_analytics"]

      def create_config_loader(self) -> ConfigLoaderService:
          """Create config loader service."""
          if "config_loader" not in self._services:
              self._services["config_loader"] = ConfigLoaderService()
          return self._services["config_loader"]

      async def initialize_all(self) -> None:
          """Initialize all created services."""
          for service in self._services.values():
              await service.initialize()

      async def shutdown_all(self) -> None:
          """Shutdown all services."""
          for service in self._services.values():
              await service.shutdown()
  ```

- [ ] **Move Misplaced Code Between Modules**
  ```bash
  # CRITICAL: Move exposure calculators to risk module
  mkdir -p cyberdelta/core/risk/exposure/
  mv cyberdelta/core/portfolio/calculators/exposure_calculator.py cyberdelta/core/risk/exposure/position_exposure.py
  mv cyberdelta/core/portfolio/calculators/portfolio_exposure_calculator.py cyberdelta/core/risk/exposure/portfolio_exposure.py
  mv cyberdelta/core/portfolio/calculators/currency_exposure_calculator.py cyberdelta/core/risk/exposure/currency_exposure.py
  mv cyberdelta/core/portfolio/calculators/position_exposure_calculator.py cyberdelta/core/risk/exposure/individual_position.py

  # Create risk service factory for clean integration
  cat > cyberdelta/core/risk/services/risk_service_factory.py << 'EOF'
  """Risk service factory for portfolio integration."""
  from cyberdelta.core.risk.exposure.portfolio_exposure import PortfolioExposureCalculator
  from cyberdelta.core.risk.sizing.orchestrator.position_sizer import PositionSizer
  from cyberdelta.core.risk.utils.risk_metrics_calculator import RiskMetricsCalculator

  class RiskServiceFactory:
      def create_exposure_calculator(self) -> PortfolioExposureCalculator:
          return PortfolioExposureCalculator()

      def create_position_sizer(self) -> PositionSizer:
          return PositionSizer()

      def create_risk_metrics_calculator(self) -> RiskMetricsCalculator:
          return RiskMetricsCalculator()
  EOF
  ```

- [ ] **Complete Legacy Service Removal**
  ```bash
  # Remove oversized services completely - no gradual migration
  rm cyberdelta/core/portfolio/services/analytics/portfolio_analytics_service.py
  rm cyberdelta/core/portfolio/services/config/portfolio_config_manager.py
  rm cyberdelta/core/portfolio/services/metrics/portfolio_metrics_aggregation_service.py

  # Update all imports immediately - breaking change acceptable for 0.0.1
  find cyberdelta -name "*.py" -exec sed -i 's/from.*portfolio_analytics_service import/from ..analytics.performance_analytics import/g' {} \;
  find cyberdelta -name "*.py" -exec sed -i 's/from.*portfolio_config_manager import/from ..config.config_loader import/g' {} \;
  ```

- [ ] **Update Service Imports Script**
  ```python
  #!/usr/bin/env python3
  """Replace all service imports with new focused services."""

  import re
  from pathlib import Path

  def update_service_imports():
      """Update all service imports to use new focused services."""

      replacements = {
          # Analytics service replacements
          "from cyberdelta.core.portfolio.services.analytics.portfolio_analytics_service import PortfolioAnalyticsService":
              "from cyberdelta.core.portfolio.services.analytics.performance_analytics import PerformanceAnalyticsService",

          # Config service replacements
          "from cyberdelta.core.portfolio.services.config.portfolio_config_manager import PortfolioConfigManager":
              "from cyberdelta.core.portfolio.services.config.config_loader import ConfigLoaderService",

          # Metrics service replacements
          "from cyberdelta.core.portfolio.services.metrics.portfolio_metrics_aggregation_service import MetricsAggregationService":
              "from cyberdelta.core.portfolio.services.metrics.pnl_metrics import PnLMetricsService",
      }

      # Find all Python files and replace imports
      for py_file in Path("cyberdelta").rglob("*.py"):
          content = py_file.read_text()
          original_content = content

          for old_import, new_import in replacements.items():
              content = content.replace(old_import, new_import)

          if content != original_content:
              py_file.write_text(content)
              print(f"Updated service imports in {py_file}")

  if __name__ == "__main__":
      update_service_imports()
  ```

## Service Size Enforcement

### Automated Size Checking
```python
#!/usr/bin/env python3
"""Enforce service size limits."""

from pathlib import Path

MAX_SERVICE_SIZE = 300  # lines
MAX_BASE_SERVICE_SIZE = 200  # lines

def check_service_sizes():
    """Check that all services respect size limits."""

    violations = []

    for service_file in Path("cyberdelta/core/portfolio/services").rglob("*.py"):
        if service_file.name == "__init__.py":
            continue

        line_count = len(service_file.read_text().splitlines())

        # Base services have stricter limits
        limit = MAX_BASE_SERVICE_SIZE if "base" in service_file.name else MAX_SERVICE_SIZE

        if line_count > limit:
            violations.append({
                "file": service_file,
                "lines": line_count,
                "limit": limit,
                "violation": line_count - limit
            })

    if violations:
        print("SERVICE SIZE VIOLATIONS:")
        for v in violations:
            print(f"  {v['file']}: {v['lines']} lines (limit: {v['limit']}, over by {v['violation']})")
        return False

    print("All services within size limits ✓")
    return True

if __name__ == "__main__":
    import sys
    if not check_service_sizes():
        sys.exit(1)
```

## Testing Strategy

### Service Integration Testing
```python
"""Test new focused services work together."""
import pytest
from cyberdelta.core.portfolio.services import PortfolioServiceFactory

@pytest.fixture
async def service_factory():
    """Create and initialize service factory."""
    factory = PortfolioServiceFactory()
    await factory.initialize_all()
    yield factory
    await factory.shutdown_all()

async def test_performance_analytics_service(service_factory):
    """Test performance analytics service."""
    service = service_factory.create_performance_analytics()

    # Test service is properly initialized
    assert service.is_initialized()

    # Test performance calculation
    portfolio_state = create_test_portfolio_state()
    result = await service.calculate_performance(portfolio_state)

    assert result.total_capital > 0
    assert isinstance(result.realized_pnl, Decimal)

async def test_config_loader_service(service_factory):
    """Test config loader service."""
    service = service_factory.create_config_loader()

    # Test config loading
    config = await service.load_from_env()
    assert config.base_currency == "USDC"
```

## Success Metrics

### Technical Metrics
- [ ] **Service Count**: 3 oversized services → 9 focused services
- [ ] **Average Service Size**: <300 lines per service (vs 1,300+ before)
- [ ] **Single Responsibility**: Each service has one clear purpose
- [ ] **Dependency Clarity**: Clean dependency graph with no cycles

### Quality Metrics
- [ ] **Maintainability**: Easier to understand and modify services
- [ ] **Testability**: Smaller services easier to test in isolation
- [ ] **Reusability**: Focused services can be reused in different contexts
- [ ] **Performance**: Smaller services load faster and use less memory

### Code Quality Metrics
- [ ] **Cyclomatic Complexity**: Reduced complexity per service
- [ ] **Test Coverage**: >90% coverage for each focused service
- [ ] **Type Safety**: 100% mypy compliance maintained
- [ ] **Import Speed**: Faster imports due to smaller service files

## Risk Management

### Technical Risks
1. **Service Coordination Complexity**
   - **Risk**: Multiple focused services may be harder to coordinate
   - **Mitigation**: Clear service factory pattern and dependency injection
   - **Detection**: Integration testing and service interaction validation

2. **Breaking Changes**
   - **Risk**: Replacing services will break existing code
   - **Mitigation**: Acceptable for 0.0.1, all updates done in same week
   - **Detection**: Comprehensive testing and import validation

### Operational Risks
1. **Service Discovery**
   - **Risk**: Developers may not know which service to use
   - **Mitigation**: Clear service factory and documentation
   - **Documentation**: Service responsibility matrix and usage examples

## Expected Outcomes

### Week 2 Deliverables
- [ ] **9 Focused Services** - Replace 3 oversized services with 9 focused ones
- [ ] **Service Factory** - Clean dependency injection and service management
- [ ] **Updated Imports** - All imports updated to use new focused services
- [ ] **Size Enforcement** - Automated checking to prevent service bloat
- [ ] **Integration Testing** - Validation that focused services work together

### System Benefits
- [ ] **Maintainability** - Smaller, focused services easier to maintain
- [ ] **Testability** - Each service can be tested in isolation
- [ ] **Performance** - Smaller services load faster and use less memory
- [ ] **Developer Experience** - Clear service responsibilities and boundaries

### Foundation for Week 3
- [ ] **Clean Service Architecture** - Foundation for legacy code removal
- [ ] **Service Patterns** - Established patterns for creating focused services
- [ ] **Dependency Management** - Clear approach to service dependencies
- [ ] **Quality Gates** - Automated enforcement of service quality standards

This clean-break approach eliminates service bloat immediately and establishes a sustainable architecture where services can't grow beyond reasonable limits.

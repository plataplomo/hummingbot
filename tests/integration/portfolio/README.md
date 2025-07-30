# Portfolio Integration Tests

This directory contains integration tests for the portfolio module, specifically focusing on the interactions between different portfolio components and external systems.

## Test Files

### `test_portfolio_risk_integration.py`
**Purpose**: Tests the Portfolio-Risk Coordination Layer implementation

**What it tests**:
- `PortfolioRiskCoordinator` functionality
- Integration between portfolio state management and risk assessment
- `UnifiedServiceFactory` service coordination
- `CyberDeltaApplication` lifecycle management
- Trade request validation and processing
- Service factory dependency injection patterns

**Key Test Classes**:
- `TestPortfolioRiskCoordinator`: Core coordinator functionality
- `TestUnifiedServiceFactory`: Service factory integration 
- `TestCyberDeltaApplication`: Application lifecycle testing

**Usage**:
```bash
# Run all portfolio integration tests
python -m pytest tests/integration/portfolio/ -v

# Run specific test file
python -m pytest tests/integration/portfolio/test_portfolio_risk_integration.py -v

# Run basic validation standalone
python tests/integration/portfolio/test_portfolio_risk_integration.py
```

## Implementation Notes

These tests validate the successful implementation of the portfolio tracker cleanup refactor, specifically:

1. **Type System Consolidation** (Week 1): Consolidated types are properly imported and used
2. **Service Architecture Cleanup** (Week 2): Focused services work together through factories
3. **Legacy Code Removal** (Week 3): No legacy components are referenced
4. **Modular Integration** (Week 4): Clean coordination between portfolio and risk modules

## Dependencies

The tests require:
- Portfolio service factory and associated services
- Risk service factory and risk calculation services  
- Portfolio-risk coordinator implementation
- Unified service factory for dependency injection
- Integrated application with proper lifecycle management

## Test Strategy

The integration tests use mocking to isolate the coordination logic while still validating:
- Proper service instantiation and dependency injection
- Correct data flow between portfolio and risk modules
- Error handling and validation patterns
- Type safety and Pydantic model validation
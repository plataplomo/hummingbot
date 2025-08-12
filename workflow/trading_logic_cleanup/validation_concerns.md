# Validation Concerns - Separation of Responsibilities

## Overview

This document defines the proper separation of validation concerns across the CyberDeltaEngine architecture, clarifying what should be handled at each layer to avoid architectural violations and maintain clean boundaries.

## Validation Layer Responsibilities

### ❌ What ValidationService Should NOT Handle

**Exchange/API-Specific Validation** - These belong at the API boundary:
- Order type compatibility (LIMIT, MARKET, STOP_* per exchange)
- TimeInForce support variations (GTC, IOC, ALO for Hyperliquid vs FOK for others) 
- Exchange-specific constraints and batch limits (50 for Hyperliquid, 20 for Backpack)
- API request format validation
- Authentication and rate limiting validation
- Protocol-specific validation (WebSocket message formats, API payloads)

**Why this separation matters:**
- Exchange APIs are the natural boundary for exchange-specific concerns
- Prevents ValidationService from becoming a monolithic validator
- Maintains proper layered architecture
- Avoids coupling infrastructure layer to specific exchange implementations

### ✅ What ValidationService SHOULD Handle

**Cross-Domain Business Logic Validation:**

1. **PRECISION Category** (fail-fast validation):
   - Price precision validation against market tick sizes
   - Quantity precision validation against market step sizes
   - Decimal precision and bounds checking

2. **LIMITS Category** (basic constraints):
   - Order value limits from configuration
   - Position sizing constraints
   - Trading session limits

3. **BALANCE Category** (fund availability):
   - Portfolio balance validation for sufficient funds
   - Multi-currency balance coordination
   - Balance tolerance validation

4. **RISK Category** (position and exposure management):
   - Maximum position size limits per symbol
   - Total exposure limits across portfolio
   - Drawdown limit enforcement
   - Correlation limits between positions

5. **MARKET Category** (market conditions):
   - Market status validation (open/closed/suspended)
   - Trading hours enforcement per symbol
   - Liquidity condition checks (bid-ask spreads, volumes)
   - Market volatility circuit breakers

6. **STATE Category** (trading state coordination):
   - Trading state validation (ACTIVE, HALTED, REDUCING)
   - Risk-off mode coordination
   - Emergency stop validation

## Architectural Layers

### 1. Exchange/API Layer (`cyberdelta/apis/`)
**Responsibility**: Exchange-specific validation at the boundary
- **Location**: `cyberdelta/apis/{exchange}/services/utils/order_validation.py`
- **Scope**: Exchange compatibility, protocol validation, API constraints
- **Examples**:
  - Hyperliquid doesn't support FOK TimeInForce
  - Backpack has different batch size limits
  - Exchange-specific order type support

### 2. Infrastructure ValidationService (`cyberdelta/infrastructure/validation/`)
**Responsibility**: Cross-domain validation orchestration
- **Location**: `cyberdelta/infrastructure/validation/validation_service.py`
- **Scope**: Business logic validation coordination
- **Examples**:
  - Orchestrating balance checks across portfolio service
  - Coordinating risk limits across risk service
  - Market condition validation across market data

### 3. Domain Services (`cyberdelta/domain/`)
**Responsibility**: Domain-specific validation logic
- **Locations**: 
  - `cyberdelta/domain/portfolio/balance_manager.py` (balance validation)
  - `cyberdelta/domain/risk/risk_checker.py` (risk validation)
- **Scope**: Domain expertise validation
- **Examples**:
  - Balance manager validates sufficient funds for specific currencies
  - Risk checker validates position sizes against configured limits

## Benefits of This Separation

### 1. **Clean Architecture**
- Each layer handles its appropriate concerns
- No circular dependencies between layers
- Clear responsibility boundaries

### 2. **Maintainability**
- Exchange-specific changes isolated to API layer
- Business logic changes isolated to domain/infrastructure
- Easier to add new exchanges without affecting core validation

### 3. **Testability**
- Exchange validation can be tested with exchange-specific scenarios
- Business validation can be tested with domain scenarios
- Clear mocking boundaries for unit tests

### 4. **Scalability**
- New exchanges only require API layer changes
- New business rules only require ValidationService changes
- No cross-contamination of concerns

## Implementation Guidelines

### For Exchange API Development:
```python
# ✅ Correct - Exchange-specific validation in API layer
def validate_hyperliquid_order_type(order_type: OrderType) -> bool:
    supported = [OrderType.LIMIT, OrderType.MARKET, OrderType.STOP_MARKET, OrderType.STOP_LIMIT]
    return order_type in supported

# ❌ Wrong - Business logic validation in API layer  
def validate_position_risk(position_size: Decimal, max_risk: Decimal) -> bool:
    return position_size <= max_risk  # This belongs in ValidationService
```

### For ValidationService Development:
```python
# ✅ Correct - Domain validation coordination
async def validate_order(self, order: Order) -> ValidationResult:
    # Coordinate validation across domains
    balance_result = await self._balance_validator.validate(order)
    risk_result = await self._risk_validator.validate(order)
    return self._combine_results([balance_result, risk_result])

# ❌ Wrong - Exchange-specific validation in ValidationService
def validate_exchange_compatibility(self, order: Order) -> ValidationResult:
    if order.exchange == "hyperliquid" and order.time_in_force == TimeInForce.FOK:
        return ValidationResult(valid=False, reason="Hyperliquid doesn't support FOK")
```

## Decision Record

**Decision**: ValidationService focuses on cross-domain business validation only. Exchange-specific validation remains at the API boundary.

**Rationale**: 
- Maintains proper layered architecture
- Prevents ValidationService from becoming a monolithic validator
- Keeps exchange concerns isolated to exchange APIs
- Enables independent evolution of exchange APIs and business validation

**Status**: Implemented - Exchange validation rules removed from ValidationService

**Date**: 2025-01-12

## Future Considerations

1. **New Exchange Integration**: Exchange-specific validation should be implemented in the new exchange's API layer, following existing patterns in `hyperliquid/services/utils/order_validation.py`

2. **Cross-Exchange Validation**: If validation logic needs to coordinate across multiple exchanges, it should be implemented in ValidationService with proper abstraction

3. **Regulatory Compliance**: Regulatory validation should be implemented as business rules in ValidationService, not as exchange-specific rules

4. **Circuit Breakers**: Market-wide circuit breakers belong in ValidationService MARKET category, exchange-specific circuit breakers belong in exchange APIs
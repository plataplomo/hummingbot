# PnL Calculation Unification - Implementation Guide

**Start Date:** 2025-01-13  
**Task:** Unify PnL calculations using existing infrastructure  
**Timeline:** 4-6 hours (not 80 hours)  

---

## 🎯 **ACTUAL PROBLEM**

We have **inconsistent PnL calculations** across three locations:

1. **`DerivativePosition.calculate_unrealized_pnl()`** - BUG: inconsistent abs() usage
2. **`cyberdelta/domain/portfolio/pnl_calculator.py`** - CORRECT: consistent abs() usage  
3. **`cyberdelta/apis/base/protocols/mapper_protocols.py`** - CORRECT: consistent abs() usage

**Goal:** Make all three use the **existing domain PnL calculator** as single source of truth.

---

## 🏗️ **EXISTING INFRASTRUCTURE TO USE**

### **✅ ALREADY EXISTS - USE THESE:**
- **PnL Models:** `/cyberdelta/models/portfolio/pnl_report.py` 
  - `PnLReport` (comprehensive portfolio PnL)
  - `PositionPnLDetail` (individual position PnL)
- **PnL Calculator:** `/cyberdelta/domain/portfolio/pnl_calculator.py`
  - `PnLCalculator` class with proper configuration injection
- **PnL Protocol:** `/cyberdelta/protocols/domain/portfolio/pnl_calculation.py`
  - `PnLCalculatorProtocol` interface
- **State Management:** `/cyberdelta/protocols/domain/portfolio/state_management.py`
  - `PortfolioStateManagerProtocol`

### **❌ DO NOT CREATE:**
- No new `cyberdelta/services/` directory
- No new `UnifiedPnLCalculator` class  
- No new `PnLResult` model
- No duplicate protocols

---

## 🚀 **IMPLEMENTATION STEPS**

### **Step 1: Fix DerivativePosition Bug (30 minutes)**

**File:** `/cyberdelta/models/derivative_position.py:264-267`

**Current (INCONSISTENT):**
```python
def calculate_unrealized_pnl(self, mark_price: Decimal) -> Decimal | None:
    if self.size == Decimal(0) or not self.entry_price:
        return None

    if self.side == OrderSide.BUY:
        return self.size * (mark_price - self.entry_price)  # Uses signed size
    # SELL
    return abs(self.size) * (self.entry_price - mark_price)  # Uses abs() - BUG!
```

**Fixed (CONSISTENT):**
```python
def calculate_unrealized_pnl(self, mark_price: Decimal) -> Decimal | None:
    if self.size == Decimal(0) or not self.entry_price:
        return None

    # Always use absolute size for consistent calculation
    size_abs = abs(self.size)
    
    if self.side == OrderSide.BUY:
        return size_abs * (mark_price - self.entry_price)
    # SELL
    return size_abs * (self.entry_price - mark_price)
```

### **Step 2: Update DerivativePosition to Use Domain Calculator (1 hour)**

**Enhance:** `/cyberdelta/models/derivative_position.py`

**Add dependency injection option:**
```python
async def calculate_unrealized_pnl_via_service(
    self, 
    pnl_calculator: PnLCalculatorProtocol,
    mark_price: Decimal | None = None
) -> Decimal | None:
    """Calculate unrealized PnL using domain PnL calculator service.
    
    This method delegates to the domain PnL calculator for consistency
    across all PnL calculations in the system.
    """
    if self.size == Decimal(0):
        return None
        
    position_pnl = await pnl_calculator.calculate_position_pnl(
        symbol=self.symbol,
        exchange=self.exchange
    )
    
    return position_pnl.unrealized_pnl_usd if position_pnl else None
```

### **Step 3: Update API Mappers to Use Domain Calculator (1 hour)**

**File:** `/cyberdelta/apis/base/protocols/mapper_protocols.py:109-125`

**Current:**
```python
def calculate_unrealized_pnl(
    self, entry_price: Decimal, current_price: Decimal, size: Decimal, is_long: bool
) -> Decimal:
    if is_long:
        return (current_price - entry_price) * size
    return (entry_price - current_price) * size
```

**Enhanced:**
```python
def calculate_unrealized_pnl(
    self, entry_price: Decimal, current_price: Decimal, size: Decimal, is_long: bool
) -> Decimal:
    """Calculate unrealized PnL using consistent methodology.
    
    Note: This method maintains existing interface but uses consistent
    absolute size calculation. For comprehensive PnL calculations,
    use the domain PnLCalculator service.
    """
    # Always use absolute size for consistency
    size_abs = abs(size)
    
    if is_long:
        return (current_price - entry_price) * size_abs
    return (entry_price - current_price) * size_abs
```

### **Step 4: Add Cross-Validation Tests (1 hour)**

**File:** `/tests/integration/financial/test_pnl_consistency.py`

```python
"""Cross-validation tests to ensure all PnL calculations are consistent."""

import pytest
from decimal import Decimal
from datetime import datetime, UTC

from cyberdelta.enums import OrderSide, ExchangeName
from cyberdelta.models.derivative_position import DerivativePosition
from cyberdelta.domain.portfolio.pnl_calculator import PnLCalculator
from cyberdelta.apis.base.protocols.mapper_protocols import PositionMapperMixin
from cyberdelta.symbols.models import Symbol


class TestPnLCalculationConsistency:
    """Ensure all PnL calculation methods return identical results."""
    
    @pytest.mark.parametrize("side,size,entry_price,mark_price", [
        # Long positions
        (OrderSide.BUY, Decimal("100"), Decimal("50000"), Decimal("55000")),
        (OrderSide.BUY, Decimal("1"), Decimal("100"), Decimal("110")),
        
        # Short positions - both positive and negative size representations
        (OrderSide.SELL, Decimal("-100"), Decimal("50000"), Decimal("45000")),
        (OrderSide.SELL, Decimal("100"), Decimal("50000"), Decimal("45000")),
        
        # Edge cases
        (OrderSide.BUY, Decimal("0.1"), Decimal("1000"), Decimal("1100")),
        (OrderSide.SELL, Decimal("-0.1"), Decimal("1000"), Decimal("900")),
    ])
    async def test_pnl_calculation_consistency_across_all_methods(
        self, 
        side, 
        size, 
        entry_price, 
        mark_price,
        config,
        portfolio_state_manager
    ):
        """CRITICAL: All PnL calculation methods must return identical results."""
        
        # Create test position
        position = DerivativePosition(
            exchange=ExchangeName.BACKPACK,
            symbol=Symbol("BTC-USDC"),
            side=side,
            size=size,
            entry_price=entry_price,
            timestamp=datetime.now(UTC)
        )
        
        # Method 1: DerivativePosition.calculate_unrealized_pnl()
        position_pnl = position.calculate_unrealized_pnl(mark_price)
        
        # Method 2: Domain PnLCalculator
        pnl_calculator = PnLCalculator(config, portfolio_state_manager)
        domain_pnl_detail = await pnl_calculator.calculate_position_pnl(
            symbol=position.symbol,
            exchange=position.exchange
        )
        domain_pnl = domain_pnl_detail.unrealized_pnl_usd if domain_pnl_detail else None
        
        # Method 3: API Mapper calculation
        mapper = PositionMapperMixin()
        is_long = side == OrderSide.BUY
        mapper_pnl = mapper.calculate_unrealized_pnl(
            entry_price=entry_price,
            current_price=mark_price,
            size=size,
            is_long=is_long
        )
        
        # All methods must return identical results
        assert position_pnl == domain_pnl == mapper_pnl, (
            f"PnL calculation inconsistency for {side} position with size {size}:\n"
            f"Position method: {position_pnl}\n"
            f"Domain calculator: {domain_pnl}\n"
            f"API mapper: {mapper_pnl}"
        )
```

### **Step 5: Update Integration Points (1-2 hours)**

**Files to update:**

1. **Portfolio Service:** Already uses domain `PnLCalculator` ✅
2. **API Position Mappers:** Update to use consistent abs() calculation
3. **WebSocket handlers:** Ensure they use domain calculator for PnL updates

**Example for Backpack position mapper:**
```python
# In cyberdelta/apis/backpack/mappers/account/bp_position_mapper.py

async def transform_position_with_pnl(
    self,
    raw_position: dict,
    pnl_calculator: PnLCalculatorProtocol
) -> DerivativePosition:
    """Transform position and calculate PnL using domain calculator."""
    
    # Transform basic position data
    position = self.transform_position(raw_position)
    
    # Use domain calculator for consistent PnL
    if position.size != Decimal(0):
        position_pnl = await pnl_calculator.calculate_position_pnl(
            symbol=position.symbol,
            exchange=position.exchange
        )
        if position_pnl:
            position.unrealized_pnl = position_pnl.unrealized_pnl_usd
    
    return position
```

---

## 🧪 **VALIDATION APPROACH**

### **Immediate Validation (After each step):**
1. **Unit tests pass** for modified components
2. **Integration tests confirm** all methods return identical results
3. **Type checking clean** (mypy, pyright, ruff)

### **Cross-Validation Matrix:**
```
Test Case | DerivativePosition | Domain Calculator | API Mapper | Status
----------|-------------------|-------------------|------------|-------
Long +100 |      $500K       |      $500K       |   $500K   |   ✅
Short -100|      $500K       |      $500K       |   $500K   |   ✅  
Short +100|      $500K       |      $500K       |   $500K   |   ✅
Edge 0.1  |       $10        |       $10        |    $10    |   ✅
```

---

## 📊 **SUCCESS CRITERIA**

### **After 2 hours:**
- [ ] `DerivativePosition.calculate_unrealized_pnl()` uses consistent abs() calculation
- [ ] All unit tests pass
- [ ] No breaking changes to existing interfaces

### **After 4 hours:**
- [ ] Cross-validation tests pass 100%
- [ ] All three calculation methods return identical results
- [ ] API mappers use consistent calculation methodology

### **After 6 hours:**
- [ ] Integration points updated to use domain calculator
- [ ] Performance impact < 5% (async overhead)
- [ ] All type checking passes (mypy, pyright, ruff)

---

## 🚨 **ROLLBACK PLAN**

If validation fails:
1. **Revert DerivativePosition changes**
2. **Keep existing domain PnLCalculator unchanged**
3. **Document specific failure for future investigation**

---

## 💡 **WHY THIS APPROACH**

### **✅ Benefits:**
- **Uses existing infrastructure** (no duplication)
- **Minimal changes** (4-6 hours vs 80 hours)
- **Maintains backward compatibility**
- **Leverages proven domain calculator**
- **Clear validation approach**

### **🎯 Result:**
- **Single source of truth:** Domain `PnLCalculator`
- **Consistent calculations:** All methods use same logic
- **Fixed bug:** Short position calculation corrected
- **Validated system:** Cross-validation tests ensure consistency

---

This approach **unifies PnL calculations** by making the existing domain `PnLCalculator` the authoritative source, rather than creating duplicate infrastructure.
# Integration Tests Refactor Plan: Spot vs Derivatives Separation

**Date:** December 6, 2025  
**Scope:** Comprehensive refactoring of integration tests for clear spot vs derivatives separation  
**Target:** `/tests/integration/apis/` directory structure  

## Executive Summary

This document outlines a comprehensive plan to refactor the CyberDeltaEngine integration tests, creating clear separation between spot and derivatives functionality while addressing testing gaps and improving maintainability.

## Current State Analysis

### 1. Current Test Structure (Strengths to Preserve)

**EXISTING STRUCTURE ANALYSIS:**
```
tests/integration/apis/
├── backpack/
│   ├── conftest.py ✅ (comprehensive fixtures)
│   ├── test_bp_balances_private.py ✅ (positive balance tests)
│   ├── test_bp_balances_zero_balance.py ✅ (zero balance tests)
│   ├── test_bp_positions_private.py ✅ (perp position tests)
│   ├── test_bp_positions_zero_balance.py ✅ (zero position tests)
│   ├── test_bp_orders_private.py ❌ (mixed spot/perp)
│   ├── test_bp_orders_zero_balance.py ❌ (mixed spot/perp)
│   ├── test_bp_account_summary_private.py ✅ (account tests)
│   ├── test_bp_account_summary_zero_balance.py ✅ (zero account)
│   ├── test_bp_positive_balance.py ❌ (mixed functionality)
│   └── [other integration tests] ✅
└── hyperliquid/ ✅ (similar structure)
```

**CURRENT STRENGTHS (MUST PRESERVE):**
- ✅ **VCR Configuration:** Comprehensive filtering, dynamic cassettes
- ✅ **Fixture Architecture:** Session-scoped configs, real vs mocked APIs
- ✅ **Test Quality:** Model validation, business logic, error scenarios
- ✅ **Authentication:** Real Ed25519/EIP-712 testing
- ✅ **Balance Separation:** Already has positive vs zero balance split

**ISSUES TO ADDRESS:**
- ❌ Mixed spot/perp tests in same files (orders, positive_balance)
- ❌ No pytest markers for test categorization
- ❌ Difficult to run only spot or only perp tests
- ❌ No safety markers for balance requirements

### 2. Current File Analysis

#### Backpack Tests (19 files)
| File | Type | Current Classification | Target Classification |
|------|------|----------------------|---------------------|
| `test_bp_balances_private.py` | Spot | ✅ Spot-focused | `spot/balances/positive/` |
| `test_bp_balances_zero_balance.py` | Edge Case | ✅ Spot edge case | `spot/balances/zero/` |
| `test_bp_positions_private.py` | Perp | ✅ Perp-focused | `perp/positions/positive/` |
| `test_bp_positions_zero_balance.py` | Edge Case | ✅ Perp edge case | `perp/positions/zero/` |
| `test_bp_orders_private.py` | Mixed | ❌ Both spot/perp | Split into both |
| `test_bp_orders_zero_balance.py` | Mixed | ❌ Both spot/perp | Split into both |
| `test_bp_account_summary_private.py` | Cross-cutting | ✅ Account-level | `account/positive/` |
| `test_bp_account_summary_zero_balance.py` | Edge Case | ✅ Account edge case | `account/zero/` |
| `test_bp_positive_balance.py` | Mixed | ❌ Positive balance tests | Split into categories |
| `test_bp_funding_rate_integration.py` | Perp | ✅ Perp-focused | `perp/funding/` |
| `test_bp_market_integration.py` | Mixed | ❌ Both market types | Split into both |
| `test_bp_ticker_integration.py` | Mixed | ❌ Both market types | Split into both |
| `test_bp_trade_integration.py` | Mixed | ❌ Both trade types | Split into both |
| `test_bp_order_book_integration.py` | Mixed | ❌ Both market types | Split into both |
| `test_bp_candle_integration.py` | Mixed | ❌ Both market types | Split into both |

#### Hyperliquid Tests (16 files)
| File | Type | Current Classification | Target Classification |
|------|------|----------------------|---------------------|
| `test_hl_balances_private.py` | Spot | ⚠️ Transfer-focused only | `spot/balances/positive/` + enhance |
| `test_hl_balances.py` | Spot | ⚠️ Limited coverage | `spot/balances/zero/` |
| `test_hl_positions_private.py` | Perp | ✅ Perp-focused | `perp/positions/positive/` |
| `test_hl_positions.py` | Perp | ✅ Perp-focused | `perp/positions/zero/` |
| `test_hl_orders_private.py` | Mixed | ❌ Primarily perp | Split into both |
| `test_hl_orders.py` | Mixed | ❌ Primarily perp | Split into both |
| `test_hl_account_summary_private.py` | Cross-cutting | ✅ Account-level | `account/positive/` |
| `test_hl_account_summary.py` | Cross-cutting | ✅ Account-level | `account/zero/` |
| `test_hl_funding_rate_integration.py` | Perp | ✅ Perp-focused | `perp/funding/` |

## Proposed Refactor Plan: **FULL RESTRUCTURE WITH LOGIC PRESERVATION**

### 1. **NEW DIRECTORY STRUCTURE** - Preserve All Logic & Configs

**MIGRATE TO ORGANIZED STRUCTURE WHILE PRESERVING EVERYTHING:**
```
tests/integration/apis/
├── shared/                              # Shared utilities (preserve existing patterns)
│   ├── __init__.py
│   ├── base_test_classes.py            # Extract common patterns from existing tests
│   ├── validation_helpers.py           # Preserve existing validation logic
│   ├── auth_fixtures.py                # Move existing auth patterns here
│   └── vcr_helpers.py                  # Preserve existing VCR configuration
│
├── cross_exchange/                     # Cross-exchange validation tests
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_spot_balance_consistency.py
│   ├── test_derivative_position_consistency.py
│   ├── test_order_lifecycle_compatibility.py
│   ├── test_market_data_consistency.py
│   ├── test_arbitrage_scenarios.py
│   └── test_error_mapping_consistency.py
│
├── backpack/
│   ├── __init__.py
│   ├── conftest.py                     # ✅ PRESERVE all existing fixtures
│   ├── shared/                         # Backpack shared utilities
│   │   ├── __init__.py
│   │   └── bp_test_helpers.py
│   ├── spot/                           # Spot trading tests
│   │   ├── __init__.py
│   │   ├── conftest.py
│   │   ├── balances/
│   │   │   ├── __init__.py
│   │   │   ├── positive/              # ✅ MIGRATE test_bp_balances_private.py here
│   │   │   │   ├── __init__.py
│   │   │   │   ├── test_bp_spot_balances_private.py  # ✅ PRESERVE all logic
│   │   │   │   └── test_bp_spot_transfers.py
│   │   │   └── zero/                  # ✅ MIGRATE test_bp_balances_zero_balance.py here
│   │   │       ├── __init__.py
│   │   │       ├── test_bp_spot_balances_zero.py     # ✅ PRESERVE all logic
│   │   │       └── test_bp_spot_edge_cases.py
│   │   ├── orders/
│   │   │   ├── __init__.py
│   │   │   ├── positive/              # ✅ MIGRATE spot parts of test_bp_orders_private.py
│   │   │   │   ├── __init__.py
│   │   │   │   ├── test_bp_spot_orders_private.py    # ✅ PRESERVE all logic
│   │   │   │   └── test_bp_spot_order_lifecycle.py
│   │   │   └── zero/                  # ✅ MIGRATE spot parts of test_bp_orders_zero_balance.py
│   │   │       ├── __init__.py
│   │   │       ├── test_bp_spot_orders_zero.py       # ✅ PRESERVE all logic
│   │   │       └── test_bp_spot_insufficient_funds.py
│   │   ├── market_data/
│   │   │   ├── __init__.py
│   │   │   ├── test_bp_spot_tickers.py
│   │   │   ├── test_bp_spot_order_books.py
│   │   │   ├── test_bp_spot_trades.py
│   │   │   └── test_bp_spot_candles.py
│   │   └── strategies/
│   │       ├── __init__.py
│   │       └── test_bp_spot_arbitrage.py
│   ├── perp/                          # Perpetual trading tests
│   │   ├── __init__.py
│   │   ├── conftest.py
│   │   ├── positions/
│   │   │   ├── __init__.py
│   │   │   ├── positive/              # ✅ MIGRATE test_bp_positions_private.py here
│   │   │   │   ├── __init__.py
│   │   │   │   ├── test_bp_perp_positions_private.py # ✅ PRESERVE all logic
│   │   │   │   └── test_bp_position_lifecycle.py
│   │   │   └── zero/                  # ✅ MIGRATE test_bp_positions_zero_balance.py here
│   │   │       ├── __init__.py
│   │   │       ├── test_bp_perp_positions_zero.py    # ✅ PRESERVE all logic
│   │   │       └── test_bp_perp_edge_cases.py
│   │   ├── orders/
│   │   │   ├── __init__.py
│   │   │   ├── positive/              # ✅ MIGRATE perp parts of test_bp_orders_private.py
│   │   │   │   ├── __init__.py
│   │   │   │   ├── test_bp_perp_orders_private.py    # ✅ PRESERVE all logic
│   │   │   │   └── test_bp_perp_order_lifecycle.py
│   │   │   └── zero/                  # ✅ MIGRATE perp parts of test_bp_orders_zero_balance.py
│   │   │       ├── __init__.py
│   │   │       ├── test_bp_perp_orders_zero.py       # ✅ PRESERVE all logic
│   │   │       └── test_bp_perp_insufficient_margin.py
│   │   ├── funding/
│   │   │   ├── __init__.py
│   │   │   ├── test_bp_funding_rates.py              # ✅ MIGRATE existing if present
│   │   │   └── test_bp_funding_payments.py
│   │   ├── margin/
│   │   │   ├── __init__.py
│   │   │   ├── test_bp_margin_calculations.py
│   │   │   └── test_bp_liquidation_scenarios.py
│   │   ├── market_data/
│   │   │   ├── __init__.py
│   │   │   ├── test_bp_perp_tickers.py
│   │   │   ├── test_bp_perp_order_books.py
│   │   │   ├── test_bp_perp_trades.py
│   │   │   └── test_bp_perp_candles.py
│   │   └── strategies/
│   │       ├── __init__.py
│   │       ├── test_bp_funding_arbitrage.py
│   │       └── test_bp_delta_neutral.py
│   ├── account/                        # Account management tests
│   │   ├── __init__.py
│   │   ├── conftest.py
│   │   ├── positive/                   # ✅ MIGRATE test_bp_account_summary_private.py
│   │   │   ├── __init__.py
│   │   │   ├── test_bp_account_summary_private.py    # ✅ PRESERVE all logic
│   │   │   └── test_bp_portfolio_management.py
│   │   └── zero/                       # ✅ MIGRATE test_bp_account_summary_zero_balance.py
│   │       ├── __init__.py
│   │       ├── test_bp_account_summary_zero.py       # ✅ PRESERVE all logic
│   │       └── test_bp_account_edge_cases.py
│   └── [websockets, mappers, etc.]     # ✅ MIGRATE all other existing files
│
└── hyperliquid/                        # ✅ PRESERVE and reorganize all existing tests
    └── [Mirror structure with all existing logic preserved]
```

**CRITICAL PRESERVATION REQUIREMENTS:**
- ✅ **PRESERVE ALL TEST LOGIC** - every assertion, every validation, every business rule
- ✅ **PRESERVE ALL VCR CONFIGURATIONS** - maintain existing cassette filtering and paths
- ✅ **PRESERVE ALL FIXTURES** - keep bp_api_for_test_env, bp_api_with_di, etc.
- ✅ **PRESERVE ALL AUTHENTICATION** - maintain Ed25519/EIP-712 testing patterns
- ✅ **PRESERVE ALL ERROR HANDLING** - keep existing error scenario tests

**PYTEST GUARANTEE:**
- 🧪 **100% PYTEST USAGE** - All tests use pytest framework exclusively
- 🧪 **PYTEST FIXTURES** - All shared setup uses pytest fixture patterns
- 🧪 **PYTEST MARKERS** - All categorization uses pytest.mark decorators
- 🧪 **PYTEST PARAMETRIZATION** - All test variations use @pytest.mark.parametrize
- 🧪 **PYTEST ASSERTIONS** - All validations use standard pytest assertions
- 🧪 **PYTEST DISCOVERY** - All tests follow pytest naming and organization conventions

### 2. **MIGRATION STRATEGY** - Full Restructure with Logic Preservation

#### Phase 1: Infrastructure Setup (Week 1)

**APPROACH: Create new structure while preserving ALL existing functionality**

1. **Create shared pytest utilities by extracting common patterns:**
   ```python
   # tests/integration/apis/shared/conftest.py
   # ✅ EXTRACT and PRESERVE common fixtures from existing conftest.py files
   # 🧪 100% PYTEST: All fixtures use pytest.fixture decorator
   import pytest
   from decimal import Decimal
   
   @pytest.fixture
   def spot_test_symbols():
       """Common spot trading symbols for testing."""
       return ["SOL_USDC", "BTC_USDC", "ETH_USDC"]  # ✅ PRESERVE existing symbols
   
   @pytest.fixture  
   def perp_test_symbols():
       """Common perp symbols for testing."""
       return ["SOL-PERP", "BTC-PERP", "ETH-PERP"]  # ✅ PRESERVE existing symbols
   
   @pytest.fixture
   def precision_test_amounts():
       """Test amounts for precision validation."""
       return [
           Decimal("0.00000001"),  # Dust
           Decimal("0.1"),         # Small
           Decimal("100"),         # Normal
           Decimal("999999.99")    # Large
       ]
   ```

2. **Preserve and enhance pytest VCR configuration:**
   ```python
   # tests/integration/apis/shared/vcr_helpers.py
   # ✅ PRESERVE ALL existing VCR filtering and configuration
   # 🧪 100% PYTEST: All VCR integration uses pytest fixtures
   import pytest
   from pathlib import Path
   
   @pytest.fixture
   def vcr_cassette_dir(request, custom_vcr_cassette_dir=None):
       """✅ PRESERVE existing VCR cassette directory logic."""
       if custom_vcr_cassette_dir:
           return custom_vcr_cassette_dir  # ✅ PRESERVE existing parametrized approach
       # ✅ PRESERVE existing dynamic directory logic
       test_file = Path(request.module.__file__)
       return str(test_file.parent.relative_to(Path("tests/integration/apis")))
   
   @pytest.fixture
   def vcr_config():
       """Common VCR configuration for all pytest tests."""
       return {
           "filter_headers": ["authorization", "x-api-key"],
           "match_on": ["method", "scheme", "host", "port", "path", "query"],
           "record_mode": "once",
       }
   ```

3. **Migrate test files to pytest-organized structure while preserving ALL logic:**
   ```python
   # Example migration: 
   # FROM: tests/integration/apis/backpack/test_bp_balances_private.py
   # TO:   tests/integration/apis/backpack/spot/balances/positive/test_bp_spot_balances_private.py
   
   # ✅ PRESERVE: All existing imports, fixtures, VCR config, test methods
   # ✅ PRESERVE: All existing assertions and business logic  
   # ✅ PRESERVE: All existing error handling and edge cases
   # 🧪 100% PYTEST: Add pytest markers, maintain pytest conventions
   
   import pytest  # Add if not present
   # ✅ PRESERVE: all existing imports exactly as they are
   
   @pytest.mark.spot                    # 🧪 PYTEST: Use pytest.mark for categorization
   @pytest.mark.requires_balance        # 🧪 PYTEST: Use pytest.mark for safety
   @pytest.mark.positive_balance        # 🧪 PYTEST: Use pytest.mark for balance type
   @pytest.mark.parametrize("custom_vcr_cassette_dir", ["apis/backpack/private/balances"], indirect=True)
   class TestBpSpotBalancesPrivate:     # 🧪 PYTEST: Follow pytest class naming
       # ✅ PRESERVE: every existing test method EXACTLY as written
       # ✅ PRESERVE: every existing assertion and validation
       # ✅ PRESERVE: every existing fixture usage
       # 🧪 PYTEST: All test methods follow test_* naming convention
       
       def test_existing_method_name(self, existing_fixtures):  # 🧪 PYTEST: test_* naming
           # ✅ PRESERVE: all existing test logic exactly as written
           assert existing_assertion  # 🧪 PYTEST: standard assertions
   ```

#### Phase 2: Exchange-Specific Migration (Weeks 2-3)

**Backpack Migration:**
1. **Migrate existing files with FULL logic preservation:**
   - **✅ MOVE** `test_bp_balances_private.py` → `spot/balances/positive/test_bp_spot_balances_private.py`
   - **✅ MOVE** `test_bp_balances_zero_balance.py` → `spot/balances/zero/test_bp_spot_balances_zero.py`
   - **✅ MOVE** `test_bp_positions_private.py` → `perp/positions/positive/test_bp_perp_positions_private.py`
   - **✅ MOVE** `test_bp_positions_zero_balance.py` → `perp/positions/zero/test_bp_perp_positions_zero.py`
   - **✅ PRESERVE** all existing VCR cassette paths and filtering
   - **✅ PRESERVE** all existing fixture usage patterns

2. **Split mixed test files while preserving ALL logic:**
   - **✅ ANALYZE** `test_bp_orders_private.py` to identify spot vs perp methods
   - **✅ EXTRACT** spot order methods → `spot/orders/positive/test_bp_spot_orders_private.py`
   - **✅ EXTRACT** perp order methods → `perp/orders/positive/test_bp_perp_orders_private.py`
   - **✅ PRESERVE** every assertion, every validation, every error case
   - **✅ PRESERVE** all VCR configurations and cassette organization

3. **Migrate conftest.py files with enhancement:**
   - **✅ PRESERVE** existing `tests/integration/apis/backpack/conftest.py` 
   - **✅ ENHANCE** with spot/perp specific fixtures in subdirectories
   - **✅ MAINTAIN** backward compatibility during migration

**Hyperliquid Migration:**
1. **Mirror Backpack approach with full preservation:**
   - **✅ MIGRATE** all existing Hyperliquid test files to new structure
   - **✅ PRESERVE** all existing authentication and VCR patterns
   - **✅ ENHANCE** spot coverage while maintaining existing perp tests

2. **Address existing gaps while preserving strengths:**
   - **✅ PRESERVE** existing `test_hl_positions_private.py` functionality
   - **✅ ENHANCE** `test_hl_balances_private.py` (currently transfer-focused only)
   - **✅ PRESERVE** all existing business logic and error handling

#### Phase 3: Cross-Exchange Testing (Week 4)

1. **Balance consistency tests:**
   ```python
   # tests/integration/apis/cross_exchange/test_spot_balance_consistency.py
   import pytest
   from tests.integration.apis.shared.validation_helpers import assert_valid_spot_balance
   
   @pytest.mark.cross_exchange
   @pytest.mark.parametrize("exchange", ["backpack", "hyperliquid"])
   class TestSpotBalanceConsistency:
       """Cross-exchange spot balance consistency tests."""
       
       def test_spot_balance_model_consistency(self, exchange, spot_test_symbols):
           """Validate SpotBalance model across exchanges."""
           # Test implementation
           
       @pytest.mark.parametrize("precision", [8, 10, 12])
       def test_spot_balance_precision_handling(self, exchange, precision):
           """Test decimal precision handling across exchanges."""
           # Test implementation
   ```

2. **Order compatibility tests:**
   ```python
   # tests/integration/apis/cross_exchange/test_order_lifecycle_compatibility.py
   import pytest
   from decimal import Decimal
   
   class TestOrderLifecycleCompatibility:
       """Cross-exchange order lifecycle compatibility tests."""
       
       @pytest.fixture(params=["backpack", "hyperliquid"])
       def exchange_client(self, request):
           """Parametrized fixture for exchange clients."""
           # Return appropriate client based on request.param
           
       @pytest.mark.parametrize("order_type", ["LIMIT", "MARKET"])
       def test_spot_order_lifecycle_compatibility(self, exchange_client, order_type):
           """Test spot order lifecycle across both exchanges."""
           # Test implementation
           
       def test_order_precision_compatibility(self, exchange_client, precision_test_amounts):
           """Test order amount precision handling."""
           # Test implementation
   ```

3. **Arbitrage scenario tests:**
   ```python
   # tests/integration/apis/cross_exchange/test_arbitrage_scenarios.py
   import pytest
   from cyberdelta.core.models import SpotBalance, DerivativePosition
   
   @pytest.mark.integration
   class TestArbitrageScenarios:
       """Cross-exchange arbitrage scenario tests."""
       
       @pytest.fixture
       def arbitrage_setup(self):
           """Setup for arbitrage testing."""
           return {
               "spot_exchange": "backpack",
               "perp_exchange": "hyperliquid",
               "test_asset": "SOL",
           }
       
       def test_spot_perp_arbitrage_setup(self, arbitrage_setup):
           """Test setting up delta-neutral arbitrage positions."""
           # Test implementation
           
       @pytest.mark.parametrize("funding_rate", [Decimal("0.01"), Decimal("-0.01")])
       def test_funding_arbitrage_opportunity(self, arbitrage_setup, funding_rate):
           """Test funding rate arbitrage scenarios."""
           # Test implementation
   ```

### 3. Testing Gaps to Address

#### 3.1 Spot Trading Gaps
| Gap Category | Current State | Required Tests |
|--------------|---------------|----------------|
| **Hyperliquid Spot Balances** | Only transfer tests | Balance query, precision validation |
| **Spot Order Lifecycle** | Mixed with perps | Dedicated spot order tests |
| **Spot Market Data** | Mixed validation | Spot-specific ticker/orderbook tests |
| **Spot-Perp Arbitrage** | None | Cross-market arbitrage scenarios |
| **Spot Transfer Edge Cases** | Limited | Dust amounts, precision limits |

#### 3.2 Perpetual Trading Gaps
| Gap Category | Current State | Required Tests |
|--------------|---------------|----------------|
| **Margin Calculations** | Basic validation | Complex margin scenarios |
| **Funding Rate Arbitrage** | None | Funding arbitrage strategies |
| **Position Liquidation** | None | Liquidation trigger scenarios |
| **Cross-Margin Risk** | None | Portfolio-level risk tests |
| **Perp Market Data** | Mixed validation | Perp-specific data validation |

#### 3.3 Cross-Exchange Gaps
| Gap Category | Current State | Required Tests |
|--------------|---------------|----------------|
| **Model Consistency** | None | Cross-exchange model validation |
| **Error Code Mapping** | None | Consistent error handling |
| **Order Compatibility** | None | Same order across exchanges |
| **Market Data Consistency** | None | Price/orderbook comparison |
| **Delta-Neutral Strategies** | None | Cross-exchange arbitrage |

### 4. Implementation Roadmap

#### Phase 1: Foundation (Week 1)
```mermaid
gantt
    title Phase 1: Foundation Setup
    dateFormat  YYYY-MM-DD
    section Infrastructure
    Create shared utilities     :done, 2025-12-06, 1d
    Setup VCR organization     :active, 2025-12-07, 1d
    Create base test classes   :2025-12-08, 1d
    Setup validation helpers   :2025-12-09, 1d
```

**Tasks:**
- [ ] Create `tests/integration/apis/shared/` structure **✅ PRESERVING** existing patterns
- [ ] Extract common validation helpers **✅ FROM EXISTING** test methods
- [ ] Setup standardized VCR cassette organization **✅ MAINTAINING** existing filtering
- [ ] Create fixture utilities for authentication **✅ BASED ON** existing auth patterns

#### Phase 2: Backpack Migration (Week 2)
```mermaid
gantt
    title Phase 2: Backpack Migration with Logic Preservation
    dateFormat  YYYY-MM-DD
    section Backpack Migration
    Create directory structure  :2025-12-09, 1d
    Migrate spot tests         :2025-12-10, 2d
    Migrate perp tests         :2025-12-12, 2d
    Validate all logic preserved :2025-12-14, 1d
```

**Tasks:**
- [ ] Create `tests/integration/apis/backpack/` subdirectory structure with `positive/` and `zero/` folders
- [ ] **✅ MIGRATE** `test_bp_balances_private.py` to `spot/balances/positive/` **PRESERVING ALL LOGIC**
- [ ] **✅ MIGRATE** `test_bp_balances_zero_balance.py` to `spot/balances/zero/` **PRESERVING ALL LOGIC**
- [ ] **✅ MIGRATE** `test_bp_positions_private.py` to `perp/positions/positive/` **PRESERVING ALL LOGIC**
- [ ] **✅ MIGRATE** `test_bp_positions_zero_balance.py` to `perp/positions/zero/` **PRESERVING ALL LOGIC**
- [ ] **✅ SPLIT** `test_bp_orders_private.py` into spot/perp variants **PRESERVING EVERY ASSERTION**
- [ ] **✅ SPLIT** `test_bp_positive_balance.py` into appropriate categories **PRESERVING ALL VALIDATIONS**
- [ ] **✅ VERIFY** all VCR cassettes still work with new structure
- [ ] **✅ VERIFY** all fixtures continue to work exactly as before

#### Phase 3: Hyperliquid Migration (Week 3)
```mermaid
gantt
    title Phase 3: Hyperliquid Migration with Logic Preservation
    dateFormat  YYYY-MM-DD
    section Hyperliquid Migration
    Create directory structure  :2025-12-16, 1d
    Migrate existing tests     :2025-12-17, 2d
    Enhance spot coverage      :2025-12-19, 2d
    Validate all logic preserved :2025-12-21, 1d
```

**Tasks:**
- [ ] Create `tests/integration/apis/hyperliquid/` subdirectory structure with `positive/` and `zero/` folders
- [ ] **✅ MIGRATE** `test_hl_balances_private.py` to `spot/balances/positive/` **PRESERVING ALL LOGIC**
- [ ] **✅ MIGRATE** `test_hl_positions_private.py` to `perp/positions/positive/` **PRESERVING ALL LOGIC**
- [ ] **✅ MIGRATE** all existing Hyperliquid tests **MAINTAINING EXISTING FUNCTIONALITY**
- [ ] **✅ ENHANCE** spot coverage **BUILDING ON** existing patterns
- [ ] **✅ VERIFY** all existing authentication and error handling preserved
- [ ] **✅ VERIFY** all VCR configurations continue to work

#### Phase 4: Cross-Exchange Testing (Week 4)
```mermaid
gantt
    title Phase 4: Cross-Exchange Testing - Building on Preserved Logic
    dateFormat  YYYY-MM-DD
    section Cross-Exchange
    Create consistency tests   :2025-12-23, 2d
    Add arbitrage scenarios   :2025-12-25, 2d
    Validate error mapping    :2025-12-27, 1d
```

**Tasks:**
- [ ] Create `tests/integration/apis/cross_exchange/` structure
- [ ] Implement balance consistency tests **✅ USING** preserved validation patterns
- [ ] Implement order compatibility tests **✅ LEVERAGING** existing order logic
- [ ] Create arbitrage scenario tests **✅ BUILDING ON** existing test infrastructure
- [ ] Add error code mapping validation **✅ PRESERVING** existing error handling
- [ ] Create delta-neutral strategy tests **✅ COMBINING** preserved spot and perp logic

### 5. Test Quality Improvements

#### 5.1 **100% PYTEST** Test Organization
```python
# tests/integration/apis/backpack/spot/conftest.py
# 🧪 100% PYTEST: All fixtures use pytest.fixture decorator
import pytest
from decimal import Decimal
from cyberdelta.apis.backpack import BackpackApiClient

@pytest.fixture(scope="session")  # 🧪 PYTEST: Session-scoped fixture
def bp_spot_client():
    """Backpack spot trading client pytest fixture."""
    return BackpackApiClient()

@pytest.fixture  # 🧪 PYTEST: Function-scoped fixture
def bp_spot_test_config():
    """Backpack spot test configuration pytest fixture."""
    return {
        "symbols": ["SOL_USDC", "BTC_USDC"],
        "min_order_size": Decimal("0.01"),
        "test_quantities": [Decimal("0.01"), Decimal("0.1"), Decimal("1.0")],
    }

# tests/integration/apis/backpack/spot/balances/positive/test_bp_spot_balances_private.py
# 🧪 100% PYTEST: All imports, decorators, and patterns follow pytest conventions
import pytest
from tests.integration.apis.shared.validation_helpers import assert_valid_spot_balance

@pytest.mark.spot                    # 🧪 PYTEST: Use pytest.mark for categorization
@pytest.mark.positive_balance        # 🧪 PYTEST: Use pytest.mark for balance type
@pytest.mark.requires_balance        # 🧪 PYTEST: Use pytest.mark for safety
class TestBackpackSpotBalancesPositive:  # 🧪 PYTEST: Test* class naming
    """Backpack spot balance pytest tests requiring real balance."""
    
    @pytest.mark.vcr()               # 🧪 PYTEST: Use pytest.mark.vcr for VCR integration
    def test_get_spot_balances_with_funds(self, bp_spot_client):  # 🧪 PYTEST: test_* naming
        """Test retrieving spot balances when account has funds."""
        balances = bp_spot_client.get_spot_balances()
        for balance in balances:
            assert_valid_spot_balance(balance)  # 🧪 PYTEST: Standard assert
            # Verify we have actual balances
            assert balance.total_quantity > Decimal("0")  # 🧪 PYTEST: Standard assert
    
    @pytest.mark.parametrize("asset", ["SOL", "USDC", "BTC"])  # 🧪 PYTEST: Parametrization
    def test_withdraw_spot_balance(self, bp_spot_client, asset):  # 🧪 PYTEST: test_* naming
        """Test withdrawing spot balance (requires real funds)."""
        # This test requires actual balance to withdraw
        balance = bp_spot_client.get_spot_balance(asset)
        assert_valid_spot_balance(balance)  # 🧪 PYTEST: Standard assert
        if balance.available_quantity > Decimal("0.01"):
            # Test actual withdrawal
            pass

# tests/integration/apis/backpack/spot/balances/zero/test_bp_spot_balances_zero.py
# 🧪 100% PYTEST: All imports, decorators, and patterns follow pytest conventions
import pytest
from tests.integration.apis.shared.validation_helpers import assert_valid_spot_balance

@pytest.mark.spot                    # 🧪 PYTEST: Use pytest.mark for categorization
@pytest.mark.zero_balance            # 🧪 PYTEST: Use pytest.mark for balance type
class TestBackpackSpotBalancesZero:  # 🧪 PYTEST: Test* class naming
    """Backpack spot balance pytest tests for zero balance scenarios."""
    
    @pytest.mark.vcr()               # 🧪 PYTEST: Use pytest.mark.vcr for VCR integration
    def test_get_spot_balances_zero_state(self, bp_spot_client):  # 🧪 PYTEST: test_* naming
        """Test retrieving balances when account has zero balance."""
        balances = bp_spot_client.get_spot_balances()
        for balance in balances:
            assert_valid_spot_balance(balance)  # 🧪 PYTEST: Standard assert
            # Zero balance tests expect empty or zero balances
            assert balance.total_quantity >= Decimal("0")  # 🧪 PYTEST: Standard assert
    
    def test_insufficient_balance_scenarios(self, bp_spot_client):  # 🧪 PYTEST: test_* naming
        """Test behavior with insufficient balance."""
        # Test edge cases without requiring real funds
        pass
```

#### 5.2 **100% PYTEST** Parameterized Test Patterns
```python
# tests/integration/apis/cross_exchange/test_spot_operations.py
# 🧪 100% PYTEST: All cross-exchange tests use pure pytest patterns
import pytest
from decimal import Decimal

@pytest.mark.integration               # 🧪 PYTEST: Use pytest.mark for integration tests
class TestCrossExchangeSpotOperations: # 🧪 PYTEST: Test* class naming
    """Cross-exchange spot operations pytest test suite."""
    
    @pytest.fixture(params=["backpack", "hyperliquid"])  # 🧪 PYTEST: Parametrized fixture
    def exchange_client(self, request):  # 🧪 PYTEST: request fixture for parametrization
        """Parametrized exchange client pytest fixture."""
        if request.param == "backpack":
            return BackpackApiClient()
        else:
            return HyperliquidApiClient()
    
    @pytest.mark.parametrize("symbol,expected_precision", [  # 🧪 PYTEST: Parametrization
        ("SOL_USDC", 8),
        ("BTC_USDC", 8),
        ("ETH_USDC", 8),
    ])
    def test_spot_balance_precision(self, exchange_client, symbol, expected_precision):  # 🧪 PYTEST: test_* naming
        """Test spot balance precision across exchanges and symbols."""
        balance = exchange_client.get_spot_balance(symbol.split("_")[0])
        assert_valid_spot_balance(balance)  # 🧪 PYTEST: Standard assert
        # Verify precision handling
        assert str(balance.total_quantity).split('.')[-1].rstrip('0') <= expected_precision  # 🧪 PYTEST: Standard assert
    
    @pytest.mark.parametrize("test_amount", [  # 🧪 PYTEST: Parametrization with ids
        pytest.param(Decimal("0.00000001"), id="dust"),
        pytest.param(Decimal("0.1"), id="small"),
        pytest.param(Decimal("100"), id="normal"),
        pytest.param(Decimal("999999.99"), id="large"),
    ])
    def test_order_amount_handling(self, exchange_client, test_amount):  # 🧪 PYTEST: test_* naming
        """Test order amount handling across different scales."""
        # 🧪 PYTEST: All test implementation uses standard pytest assertions
        assert test_amount > Decimal("0")  # 🧪 PYTEST: Standard assert
```

#### 5.3 **100% PYTEST** Marks and Test Discovery
```toml
# pyproject.toml configuration - 🧪 100% PYTEST: All configuration uses pytest.ini_options
[tool.pytest.ini_options]
testpaths = ["tests/integration/apis"]  # 🧪 PYTEST: Standard test discovery paths
python_files = ["test_*.py"]           # 🧪 PYTEST: Standard test file naming
python_classes = ["Test*"]             # 🧪 PYTEST: Standard test class naming
python_functions = ["test_*"]          # 🧪 PYTEST: Standard test function naming
markers = [                            # 🧪 PYTEST: Custom pytest markers
    "integration: Integration tests requiring external services",
    "spot: Spot trading specific tests", 
    "perp: Perpetual/derivatives trading specific tests",
    "cross_exchange: Cross-exchange validation tests",
    "vcr: Tests using VCR cassettes",
    "slow: Slow running tests",
    "requires_balance: Tests requiring real money/balance",
    "zero_balance: Tests with zero balance scenarios",
    "positive_balance: Tests requiring positive balance",
]
addopts = [                            # 🧪 PYTEST: Standard pytest options
    "--strict-markers",                # 🧪 PYTEST: Enforce marker definitions
    "--tb=short",                      # 🧪 PYTEST: Short traceback format
    "--cov=cyberdelta",                # 🧪 PYTEST: Coverage with pytest-cov
    "--cov-report=term-missing",       # 🧪 PYTEST: Coverage reporting
]
```

**🧪 100% PYTEST Usage Examples:**
```python
@pytest.mark.spot                      # 🧪 PYTEST: pytest.mark decorator
@pytest.mark.vcr()                     # 🧪 PYTEST: pytest.mark.vcr for VCR
class TestSpotBalances:                # 🧪 PYTEST: Test* class naming
    """Spot balance pytest test suite."""
    
    @pytest.mark.requires_balance      # 🧪 PYTEST: pytest.mark for safety
    def test_withdraw_spot_balance(self):  # 🧪 PYTEST: test_* function naming
        """Test spot balance withdrawal."""
        assert True  # 🧪 PYTEST: Standard assert

@pytest.mark.perp                      # 🧪 PYTEST: pytest.mark decorator
@pytest.mark.vcr()                     # 🧪 PYTEST: pytest.mark.vcr for VCR
class TestPerpPositions:               # 🧪 PYTEST: Test* class naming
    """Perpetual positions pytest test suite."""
    
    def test_get_perp_positions(self):  # 🧪 PYTEST: test_* function naming
        """Test retrieving perpetual positions."""
        assert True  # 🧪 PYTEST: Standard assert
```

#### 5.4 VCR Cassette Organization
```
cassettes/
├── backpack/
│   ├── spot/
│   │   ├── balances/
│   │   │   ├── positive/         # Cassettes for tests with real balance
│   │   │   └── zero/             # Cassettes for zero balance scenarios
│   │   ├── orders/
│   │   │   ├── positive/         # Order tests requiring balance
│   │   │   └── zero/             # Insufficient funds scenarios
│   │   └── market_data/          # Public data (no balance needed)
│   ├── perp/
│   │   ├── positions/
│   │   │   ├── positive/         # Tests with real margin
│   │   │   └── zero/             # Zero margin scenarios
│   │   ├── orders/
│   │   │   ├── positive/         # Orders requiring margin
│   │   │   └── zero/             # Insufficient margin scenarios
│   │   ├── funding/              # Public funding data
│   │   └── margin/               # Margin calculations
│   └── account/
│       ├── positive/             # Account tests with balance
│       └── zero/                 # Zero balance account tests
├── hyperliquid/
│   └── [same structure]
└── cross_exchange/
    ├── consistency/
    ├── arbitrage/
    └── error_mapping/
```

### 6. Benefits and Expected Outcomes

#### 6.1 Immediate Benefits
1. **Clear Test Organization:** Easy to find tests for specific functionality
2. **Improved Maintainability:** Related tests grouped together  
3. **Better Coverage:** Explicit identification of gaps
4. **Faster Development:** Clear patterns for adding new tests
5. **Risk Management:** Clear separation of real money vs safe tests
6. **CI/CD Safety:** Can run zero-balance tests without financial risk
7. **Developer Experience:** Clear marks for test selection based on account state

#### 6.2 Long-term Benefits
1. **Scalability:** Easy to add new exchanges following same patterns
2. **Quality Assurance:** Comprehensive cross-exchange validation
3. **Strategy Testing:** Foundation for complex arbitrage scenario tests
4. **Documentation:** Tests serve as usage examples

#### 6.3 Balance Testing Strategy Benefits
1. **Financial Safety:** Zero-balance tests prevent accidental fund usage
2. **CI/CD Integration:** Safe automated testing without real money risk
3. **Development Workflow:** Developers can run comprehensive tests locally
4. **Testing Completeness:** Both edge cases (zero) and real scenarios (positive)
5. **Clear Intent:** Test names and structure clearly indicate requirements

#### 6.4 Success Metrics
- **Test Organization:** 100% of tests in appropriate categories
- **Coverage Improvement:** 90%+ coverage for spot and perp separately
- **Cross-Exchange Tests:** Tests for all major functionality across exchanges
- **Maintenance Reduction:** 50% reduction in test maintenance effort
- **Safety Compliance:** 0% accidental real money usage in CI/CD

### 7. Migration Strategy

#### 7.1 Pytest Migration Approach
```python
# Step 1: Create compatibility wrapper during migration
# tests/integration/apis/backpack/test_bp_orders_private.py (original location)
import pytest
from tests.integration.apis.backpack.spot.orders.test_bp_spot_orders_private import *
from tests.integration.apis.backpack.perp.orders.test_bp_perp_orders_private import *

# Add deprecation warning
@pytest.mark.filterwarnings("default::DeprecationWarning")
def test_migration_notice():
    """This test file has been split into spot and derivatives tests."""
    import warnings
    warnings.warn(
        "test_bp_orders_private.py has been split. "
        "Use spot/orders/ or perp/orders/ instead.",
        DeprecationWarning,
        stacklevel=2
    )
```

#### 7.2 Pytest Collection Verification
```bash
# Verify tests are discovered correctly after migration
pytest --collect-only tests/integration/apis/backpack/spot/
pytest --collect-only tests/integration/apis/backpack/perp/
pytest --collect-only tests/integration/apis/cross_exchange/

# Run specific test categories using marks
pytest -m "spot and not slow" tests/integration/apis/
pytest -m "perp and vcr" tests/integration/apis/
pytest -m "cross_exchange" tests/integration/apis/

# Run tests based on balance requirements
pytest -m "zero_balance" tests/integration/apis/          # Safe tests, no real money
pytest -m "positive_balance" tests/integration/apis/     # Tests requiring balance
pytest -m "requires_balance" tests/integration/apis/     # Real money tests
pytest -m "not requires_balance" tests/integration/apis/ # Exclude real money tests
```

#### 7.2 Risk Mitigation
- Phase-by-phase migration to limit scope of changes
- Comprehensive test validation after each phase
- Rollback procedures for each migration step

#### 7.3 Team Coordination
- Clear communication of directory changes
- Updated documentation for test location
- Training on new test organization patterns

### 8. Pytest Fixture Examples for Refactored Structure

#### 8.1 Shared Fixtures
```python
# tests/integration/apis/shared/conftest.py
import pytest
from typing import Dict, Any
from cyberdelta.apis.backpack import BackpackApiClient
from cyberdelta.apis.hyperliquid import HyperliquidApiClient

@pytest.fixture(scope="session")
def exchange_clients() -> Dict[str, Any]:
    """All exchange clients for cross-exchange testing."""
    return {
        "backpack": BackpackApiClient(),
        "hyperliquid": HyperliquidApiClient(),
    }

@pytest.fixture
def mock_order_response():
    """Mock order response for testing."""
    return {
        "client_order_id": "test-123",
        "exchange_order_id": "exchange-456",
        "status": "NEW",
        "quantity_requested": "1.0",
        "quantity_filled": "0.0",
    }
```

#### 8.2 Exchange-Specific Fixtures
```python
# tests/integration/apis/backpack/conftest.py
import pytest
from cyberdelta.apis.backpack import BackpackApiClient

@pytest.fixture(scope="module")
def bp_client():
    """Backpack API client fixture."""
    return BackpackApiClient()

@pytest.fixture
def bp_test_symbol(request):
    """Dynamic symbol based on test type."""
    if "spot" in request.module.__name__:
        return "SOL_USDC"
    elif "perp" in request.module.__name__:
        return "SOL-PERP"
    return "BTC_USDC"  # default
```

#### 8.3 Test Category Fixtures
```python
# tests/integration/apis/backpack/spot/conftest.py
import pytest
from decimal import Decimal

@pytest.fixture
def spot_order_params():
    """Standard spot order parameters."""
    return {
        "symbol": "SOL_USDC",
        "side": "BUY",
        "order_type": "LIMIT",
        "quantity": Decimal("1.0"),
        "price": Decimal("100.0"),
        "time_in_force": "GTC",
    }

# tests/integration/apis/backpack/perp/conftest.py
@pytest.fixture
def perp_position_params():
    """Standard perpetual position parameters."""
    return {
        "symbol": "SOL-PERP",
        "side": "LONG",
        "size": Decimal("10.0"),
        "leverage": 5,
        "margin_type": "cross",
    }
```

## Conclusion

This refactoring plan delivers **comprehensive directory restructuring** while **preserving ALL existing functionality**. The approach ensures:

**PRESERVATION GUARANTEES:**
- ✅ **100% Logic Preservation** - Every assertion, validation, and business rule maintained
- ✅ **Complete VCR Compatibility** - All existing cassette filtering and organization preserved
- ✅ **Full Fixture Compatibility** - All bp_api_for_test_env, bp_api_with_di patterns maintained
- ✅ **Authentication Preservation** - Ed25519/EIP-712 testing patterns unchanged
- ✅ **Error Handling Preservation** - All existing error scenarios and edge cases maintained

**PYTEST GUARANTEES:**
- 🧪 **100% PYTEST FRAMEWORK** - All tests use pytest exclusively, no other testing frameworks
- 🧪 **100% PYTEST FIXTURES** - All shared setup converted to @pytest.fixture decorators
- 🧪 **100% PYTEST MARKERS** - All categorization uses @pytest.mark decorators
- 🧪 **100% PYTEST PARAMETRIZATION** - All test variations use @pytest.mark.parametrize
- 🧪 **100% PYTEST ASSERTIONS** - All validations use standard pytest assert statements
- 🧪 **100% PYTEST DISCOVERY** - All tests follow pytest naming conventions (Test*, test_*)
- 🧪 **100% PYTEST CONFIGURATION** - All settings in pyproject.toml [tool.pytest.ini_options]

**ORGANIZATIONAL BENEFITS:**
- 🎯 **Clear Structure** - Spot vs perp separation with positive/zero balance organization
- 🎯 **Enhanced Discoverability** - Logical directory hierarchy for easy test location
- 🎯 **Pytest Markers** - Comprehensive categorization for flexible test execution
- 🎯 **Cross-Exchange Testing** - Foundation for arbitrage and consistency validation
- 🎯 **Future Scalability** - Clear patterns for adding new exchanges and test types

**IMPLEMENTATION APPROACH:**
- 📁 **File Migration** - Move existing tests to appropriate directories maintaining all logic
- 🏷️ **Marker Addition** - Add pytest markers for categorization without changing functionality  
- 🔧 **Infrastructure Enhancement** - Extract common patterns while preserving existing behavior
- ✅ **Continuous Validation** - Verify all existing functionality works throughout migration

The phased approach ensures **zero functional regression** while delivering immediate organizational benefits and establishing foundation for enhanced cross-exchange testing capabilities.

---

**Next Steps:**
1. **Review and approve this preservation-focused refactoring plan**
2. **Begin Phase 1 implementation - infrastructure setup with logic preservation**
3. **Execute Phase 2 - Backpack migration with comprehensive validation**
4. **Execute Phase 3 - Hyperliquid migration with functionality verification**
5. **Execute Phase 4 - Cross-exchange testing building on preserved logic**
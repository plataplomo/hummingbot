# Collateral Data Implementation Research

## Overview

This document provides comprehensive research and implementation guidance for adding `get_collateral()` functionality to the CyberDeltaEngine API layer, following the established architectural patterns from API_ARCHITECTURE_COMPREHENSIVE.md and API_ARCHITECTURE.md.

## Current State Analysis

### 1. Existing Models

The codebase already contains robust models for handling margin and collateral data:

#### Core Model: MarginAccountSummary
Located in `cyberdelta/core/models/margin_account.py`:

**Core Fields (Exchange-agnostic):**
- `exchange`: str (Required exchange name)
- `timestamp`: datetime (Snapshot timestamp UTC)
- `total_equity`: Decimal (Total account equity >= 0)
- `available_equity`: Decimal (Available equity >= 0)
- `total_initial_margin_required`: Decimal | None (Total initial margin >= 0)
- `total_maintenance_margin_required`: Decimal | None (Total maintenance margin >= 0)
- `total_position_notional`: Decimal | None (Total position notional >= 0)
- `total_unrealized_pnl`: Decimal | None (Total unrealized PnL)

**Extension Slots:**
- `hl_details`: HyperliquidMarginDetails | None
- `bp_details`: BackpackMarginDetails | None

#### BackpackMarginDetails Extension
Contains comprehensive collateral-specific fields:
- `assets_value`: Total value of all assets
- `liabilities_value`: Total value of all liabilities
- `locked_equity`: Equity locked in orders/positions
- `borrow_liability`: Total borrowed amount liability
- `unsettled_equity`: Equity pending settlement
- `margin_fraction`: Current margin utilization fraction
- `net_exposure_futures`: Net futures/perp exposure notional
- `collateral_assets`: Detailed collateral breakdown by asset (internal use)
- `source_endpoint`: Source endpoint for debugging

### 2. Exchange-Specific Analysis

#### Backpack Exchange

**Primary Endpoint:** `GET /api/v1/capital/collateral`
- **Instruction:** `collateralQuery`
- **Authentication:** Required (signed request)
- **Response Model:** `MarginAccountSummary` with detailed collateral array

**Available Data:**
- Per-asset collateral breakdown with:
  - `symbol`: Asset symbol
  - `assetMarkPrice`: Current mark price of the asset
  - `totalQuantity`: Total quantity held
  - `balanceNotional`: Notional value (quantity × price)
  - `collateralWeight`: Risk weight factor (0-1) applied to asset
  - `collateralValue`: Effective collateral value (notional × weight)
  - `openOrderQuantity`: Quantity locked in open orders
  - `lendQuantity`: Quantity currently lent out
  - `availableQuantity`: Available for trading/withdrawal

**BackpackRawCollateralResponse Model** (already exists):
Located in `cyberdelta/apis/backpack/models/bp_raw_collateral.py`
- Comprehensive model matching OpenAPI MarginAccountSummary
- Includes array of `BackpackRawCollateralAsset` objects
- Full equity breakdown, margin fields, and risk metrics

#### Hyperliquid Exchange

**Primary Data Source:** `POST /info` with `type: "clearinghouseState"`
- **Authentication:** Not required for info endpoints
- **Response Model:** `ApiClearinghouseState`

**Available Data Structure:**
- `marginSummary`: Aggregate margin information
  - `accountValue`: Total account equity (total collateral value)
  - `totalMarginUsed`: Total margin currently used
  - `totalNtlPos`: Total notional position value
  - `totalRawUsd`: Total raw USD value
- `withdrawable`: Available collateral that can be withdrawn
- `crossMaintenanceMarginUsed`: Maintenance margin used in cross-margin
- `isolatedMaintenanceMarginUsed`: Maintenance margin used in isolated-margin
- `assetPositions`: Per-position data including:
  - `marginUsed`: Margin used for each position
  - `positionValue`: Notional value of each position
  - `leverage`: Leverage settings and type
  - `liquidationPx`: Liquidation price
  - `unrealizedPnl`: Unrealized PnL

**Key Differences:**
- No dedicated collateral endpoint
- No per-asset collateral weights or explicit breakdown
- Aggregate collateral metrics with per-position margin usage
- Cross vs isolated margin separation

### 3. Conceptual Approach: Unified vs Dedicated Return Types

After analyzing both exchange capabilities and existing models, there are two architectural approaches:

## Approach 1: Return Existing MarginAccountSummary (Recommended)

**Rationale:**
- `MarginAccountSummary` already contains all necessary collateral information
- Follows established architecture patterns with extension slots
- Avoids model duplication and maintains consistency
- Supports both aggregate collateral data and detailed breakdowns

**Implementation:**
```python
@abstractmethod
async def get_collateral() -> MarginAccountSummary:
    """Retrieve collateral information for the account.

    Returns comprehensive margin account summary including:
    - Total equity (acts as total collateral value)
    - Available equity (available collateral)
    - Margin requirements and usage
    - Exchange-specific collateral details in extension slots

    For Backpack: bp_details.collateral_assets contains per-asset breakdown
    For Hyperliquid: hl_details contains aggregate collateral metrics

    Returns:
        MarginAccountSummary: Complete collateral and margin state

    Raises:
        APIError: If collateral data cannot be retrieved
    """
    raise NotImplementedError
```

## Approach 2: Create Dedicated CollateralData Model

**Alternative approach** (not recommended due to duplication):
```python
class CollateralData(BaseModel):
    """Dedicated collateral data model (alternative approach)."""
    exchange: str
    timestamp: datetime
    total_collateral_value: Decimal
    available_collateral: Decimal
    # ... would duplicate many MarginAccountSummary fields
```

**Why not recommended:**
- Significant overlap with `MarginAccountSummary`
- Violates DRY principle
- Creates confusion about which model to use
- Both models would need to be maintained

## Implementation Architecture

Following the CyberDeltaEngine 6-layer architecture:

### Layer 1: Base Exchange API (`base/exchange_api.py`)

Add abstract method to ExchangeAPI:
```python
@abstractmethod
async def get_collateral(self) -> MarginAccountSummary:
    """Retrieve collateral information for the account.

    Returns comprehensive margin account summary including collateral data.
    For detailed per-asset breakdown, check exchange-specific extension slots.

    Returns:
        MarginAccountSummary: Complete collateral and margin state

    Raises:
        APIError: If collateral data cannot be retrieved
    """
    raise NotImplementedError
```

### Layer 2: Exchange-Specific Implementations

#### Backpack Implementation Path

**Service Method:** `BackpackAccountService.get_collateral()`

```python
async def get_collateral(self) -> MarginAccountSummary:
    """Retrieve collateral information via /api/v1/capital/collateral."""
    frame = inspect.currentframe()
    current_method = frame.f_code.co_name if frame is not None else "get_collateral"

    status_code: int = 0
    raw_response_content: str | None = None

    try:
        # Build request for collateral endpoint
        endpoint_path = "/api/v1/capital/collateral"

        # Execute signed request
        raw_data, status_code, _ = await self._http_client_requester(
            method="GET",
            endpoint=endpoint_path,
            is_signed=True,
        )

        # Handle response using existing collateral response handler
        raw_collateral_response = self._response_handler.handle_collateral_response(
            raw_data, status_code
        )

        # Transform to MarginAccountSummary using existing mapper
        return self._account_mapper.transform_raw_collateral_to_margin_summary(
            raw_collateral_response,
            exchange_name=self._exchange_name
        )

    except APIError:
        raise
    # ... standard error handling pattern
```

**Implementation Notes:**
- Leverages existing `BackpackRawCollateralResponse` model
- Uses existing `handle_collateral_response()` method (may need verification)
- Requires new mapper method or extension of existing one

#### Hyperliquid Implementation Path

**Service Method:** `HyperliquidAccountService.get_collateral()`

```python
async def get_collateral(self) -> MarginAccountSummary:
    """Retrieve collateral information derived from clearinghouse state."""
    frame = inspect.currentframe()
    current_method = frame.f_code.co_name if frame is not None else "get_collateral"

    try:
        # Leverage existing clearinghouse state fetching
        raw_clearinghouse_state = await self._get_raw_clearinghouse_state()

        # Use existing mapper that already creates MarginAccountSummary
        return self._account_mapper.transform_raw_clearinghouse_state_to_margin_summary(
            raw_clearinghouse_state,
        )

    except APIError:
        raise
    # ... standard error handling pattern
```

**Implementation Notes:**
- Reuses existing `_get_raw_clearinghouse_state()` method
- Leverages existing `transform_raw_clearinghouse_state_to_margin_summary()` mapper
- **Minimal implementation** - functionality already exists!

### Layer 3: Data Transformation (Mappers)

#### BackpackAccountDataMapper

```python
@staticmethod
def transform_raw_collateral_to_margin_summary(
    raw_collateral: BackpackRawCollateralResponse,
    exchange_name: str
) -> MarginAccountSummary:
    """Transform Backpack collateral response to margin summary."""

    # Core field mappings
    total_equity = parse_decimal_value(raw_collateral.net_equity, allow_none=False)
    available_equity = parse_decimal_value(raw_collateral.net_equity_available, allow_none=False)

    # Calculate maintenance margin from margin fraction if available
    total_maintenance_margin = None
    if raw_collateral.margin_fraction:
        margin_frac = parse_decimal_value(raw_collateral.margin_fraction, allow_none=True)
        if margin_frac and total_equity:
            total_maintenance_margin = total_equity * margin_frac

    # Build extension details with collateral assets
    collateral_assets_data = []
    for asset in raw_collateral.collateral:
        collateral_assets_data.append({
            "symbol": asset.symbol,
            "collateral_value": str(asset.collateral_value),
            "collateral_weight": str(asset.collateral_weight),
            "total_quantity": str(asset.total_quantity),
            "available_quantity": str(asset.available_quantity),
        })

    bp_details = BackpackMarginDetails(
        assets_value=parse_decimal_value(raw_collateral.assets_value),
        liabilities_value=parse_decimal_value(raw_collateral.liabilities_value),
        locked_equity=parse_decimal_value(raw_collateral.net_equity_locked),
        borrow_liability=parse_decimal_value(raw_collateral.borrow_liability),
        unsettled_equity=parse_decimal_value(raw_collateral.unsettled_equity),
        margin_fraction=parse_decimal_value(raw_collateral.margin_fraction),
        net_exposure_futures=parse_decimal_value(raw_collateral.net_exposure_futures),
        imf_raw=raw_collateral.imf,
        mmf_raw=raw_collateral.mmf,
        collateral_assets=collateral_assets_data,
        source_endpoint="/api/v1/capital/collateral",
    )

    return MarginAccountSummary(
        exchange=exchange_name,
        timestamp=datetime.now(UTC),
        total_equity=total_equity,
        available_equity=available_equity,
        total_initial_margin_required=None,  # Calculate if needed from IMF
        total_maintenance_margin_required=total_maintenance_margin,
        total_position_notional=parse_decimal_value(raw_collateral.net_exposure_futures),
        total_unrealized_pnl=parse_decimal_value(raw_collateral.pnl_unrealized),
        bp_details=bp_details
    )
```

#### HyperliquidAccountDataMapper

**No changes needed** - the existing `transform_raw_clearinghouse_state_to_margin_summary()` method already provides all necessary collateral information in the `MarginAccountSummary` format.

### Layer 4: Main API Classes

#### BackpackAPI
```python
async def get_collateral(self) -> MarginAccountSummary:
    """Retrieve collateral information for the account."""
    return await self._account_service.get_collateral()
```

#### HyperliquidAPI
```python
async def get_collateral(self) -> MarginAccountSummary:
    """Retrieve collateral information for the account."""
    return await self._account_service.get_collateral()
```

## Key Benefits of This Approach

### 1. Leverages Existing Infrastructure
- **Backpack:** Raw models and response handlers already exist
- **Hyperliquid:** Complete implementation already exists via `get_account_summary()`
- Minimal new code required

### 2. Maintains Architectural Consistency
- Follows "Core + Typed Extension Slots" pattern
- Uses existing error handling patterns
- Maintains immutable model design

### 3. Provides Rich Collateral Information
- **Total collateral:** Available via `total_equity`
- **Available collateral:** Available via `available_equity`
- **Used collateral:** Can be calculated from margin fields
- **Per-asset breakdown:** Available in Backpack via `bp_details.collateral_assets`
- **Risk metrics:** Margin fractions, liquidation data, etc.

### 4. Exchange-Specific Flexibility
- Backpack provides detailed per-asset collateral breakdown
- Hyperliquid provides aggregate metrics with position-level detail
- Both approaches captured in unified model

## Testing Strategy

### 1. Unit Tests
- Test mapper transformations for collateral responses
- Validate decimal parsing and field mapping
- Test error scenarios (empty collateral, invalid data)

### 2. Integration Tests
- Test actual API calls to collateral endpoints
- Verify response parsing and model validation
- Test authentication and rate limiting

### 3. Consistency Tests
- Verify `get_collateral()` returns equivalent data to `get_account_summary()`
- Test that collateral metrics align across different endpoints
- Validate extension slot population

### 4. Test Scripts
- `scripts/test_collateral_endpoints.py`
- Extend existing `scripts/test_balance_and_collateral_endpoints.py`

## Implementation Checklist

### Backpack Implementation
- [ ] Verify `BackpackRawCollateralResponse` model completeness
- [ ] Add/verify `handle_collateral_response()` method in response handler
- [ ] Implement `transform_raw_collateral_to_margin_summary()` mapper method
- [ ] Add `get_collateral()` method to `BackpackAccountService`
- [ ] Wire up method in `BackpackAPI` main class
- [ ] Add integration tests

### Hyperliquid Implementation
- [ ] Add `get_collateral()` method to `HyperliquidAccountService` (simple wrapper)
- [ ] Wire up method in `HyperliquidAPI` main class
- [ ] Verify existing mapper provides sufficient collateral data
- [ ] Add integration tests

### Base Layer
- [ ] Add `get_collateral()` abstract method to `ExchangeAPI`
- [ ] Update interface documentation
- [ ] Add method to any existing mock implementations

### Testing & Documentation
- [ ] Unit tests for new mapper methods
- [ ] Integration tests with VCR recordings
- [ ] Update API documentation
- [ ] Add usage examples showing collateral access patterns

## Collateral Data Access Patterns

### Example Usage

```python
# Get collateral information
collateral_summary = await exchange_api.get_collateral()

# Access total collateral value
total_collateral = collateral_summary.total_equity
available_collateral = collateral_summary.available_equity

# Calculate used collateral
used_collateral = total_collateral - available_collateral

# Exchange-specific details
if collateral_summary.bp_details and collateral_summary.bp_details.collateral_assets:
    # Backpack per-asset breakdown
    for asset_data in collateral_summary.bp_details.collateral_assets:
        symbol = asset_data["symbol"]
        collateral_value = Decimal(asset_data["collateral_value"])
        weight = Decimal(asset_data["collateral_weight"])
        print(f"{symbol}: ${collateral_value} (weight: {weight})")

elif collateral_summary.hl_details:
    # Hyperliquid aggregate data
    cross_margin = collateral_summary.hl_details.cross_maintenance_margin_used
    isolated_margin = collateral_summary.hl_details.isolated_maintenance_margin_used
    print(f"Cross margin: ${cross_margin}, Isolated: ${isolated_margin}")
```

## Alternative Implementation Notes

### Option: Alias Method
If preferred, `get_collateral()` could be implemented as an alias:

```python
async def get_collateral(self) -> MarginAccountSummary:
    """Retrieve collateral information (alias for get_account_summary)."""
    return await self.get_account_summary()
```

**Pros:**
- Zero implementation effort
- Guaranteed consistency

**Cons:**
- May not use most optimal endpoint (Backpack has dedicated collateral endpoint)
- Less semantic clarity

## Conclusion

The recommended approach leverages the existing `MarginAccountSummary` model as the return type for `get_collateral()`, providing a unified interface while maintaining exchange-specific flexibility through extension slots. This approach:

1. **Minimizes implementation effort** by reusing existing infrastructure
2. **Maintains architectural consistency** with established patterns
3. **Provides comprehensive collateral data** suitable for risk management and trading decisions
4. **Supports both exchanges** with their unique data structures and capabilities

The implementation primarily requires:
- Adding the abstract method to the base API class
- Simple wrapper methods in the service classes
- One new mapper method for Backpack (Hyperliquid already complete)
- Comprehensive testing

This approach successfully bridges the gap between Backpack's detailed per-asset collateral model and Hyperliquid's aggregate margin approach while providing a clean, unified interface for application-level collateral management.

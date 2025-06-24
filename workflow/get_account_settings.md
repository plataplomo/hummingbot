# Account Settings Implementation Research

## Overview

This document provides comprehensive research and implementation guidance for adding `get_account_settings()` functionality to the CyberDeltaEngine API layer, following the established architectural patterns.

## Current State Analysis

### 1. AccountSettings Model
The `AccountSettings` model is already defined in `cyberdelta/core/models/account_settings.py`:

**Core Fields (Common to all exchanges):**
- `exchange`: str (Required exchange name)
- `timestamp`: datetime (Last update timestamp UTC)
- `leverage_limit`: Decimal | None (Account leverage limit >= 1)
- `auto_borrow_settlements`: bool | None
- `auto_lend`: bool | None
- `auto_realize_pnl`: bool | None
- `auto_repay_borrows`: bool | None

**Extension Slots:**
- `hl_details`: HyperliquidAccountSettingsDetails | None
- `bp_details`: BackpackAccountSettingsDetails | None

### 2. Exchange-Specific Analysis

#### Backpack Exchange

**API Endpoint:** `GET /api/v1/account`
- **Instruction:** `accountQuery`
- **Response Model:** `AccountSummary`

**Available Settings Fields in AccountSummary:**
- `autoBorrowSettlements`: boolean
- `autoLend`: boolean
- `autoRealizePnl`: boolean
- `autoRepayBorrows`: boolean
- `leverageLimit`: string (decimal)
- `borrowLimit`: string (decimal)
- `positionLimit`: string (decimal)
- `liquidating`: boolean
- `futuresMakerFee`: string (decimal)
- `futuresTakerFee`: string (decimal)
- `spotMakerFee`: string (decimal)
- `spotTakerFee`: string (decimal)
- `limitOrders`: integer (uint64)
- `triggerOrders`: integer (uint64)

**BackpackAccountSettingsDetails Fields:**
- `leverage_limit_raw`: str | None (Raw API string for debugging)
- `subaccount_id`: int | None
- `auto_liquidation_enabled`: bool | None
- `auto_margin_call_enabled`: bool | None
- `source_endpoint`: str | None (excluded from serialization)

#### Hyperliquid Exchange

**API Characteristics:**
- No dedicated account settings endpoint
- Leverage is managed per-asset, not globally
- Settings are derived from:
  - User state (`clearinghouseState`)
  - Per-asset leverage configurations
  - Cross vs. isolated margin preferences

**HyperliquidAccountSettingsDetails Fields:**
- `asset_leverage_settings`: dict[int, int] | None (asset index → leverage)
- `cross_margin_enabled`: bool | None

**Key Differences:**
- Hyperliquid uses per-asset leverage (1-50x depending on asset)
- No global account leverage limit
- No auto-lend/auto-borrow settings
- Margin type (cross/isolated) is per-position

## Implementation Architecture

Following the CyberDeltaEngine API architecture patterns:

### 1. Layer 2: Base Exchange API (`base/exchange_api.py`)

Add abstract method to ExchangeAPI:
```python
@abstractmethod
async def get_account_settings(self) -> AccountSettings:
    """Retrieve current account configuration settings.

    Returns:
        AccountSettings: Unified account settings model with exchange-specific details

    Raises:
        APIError: If settings cannot be retrieved
    """
    raise NotImplementedError
```

### 2. Layer 3: Exchange-Specific Components

#### Backpack Implementation Path

**Request Flow:**
1. `BackpackAccountService.get_account_settings()`
2. Build request using existing `accountQuery` instruction
3. Execute HTTP GET to `/api/v1/account`
4. Handle response with `BackpackResponseHandler`
5. Transform `AccountSummary` → `AccountSettings` via mapper

**Response Handler:**
- Add `handle_account_query_response()` method
- Validate response structure
- Return `BackpackRawAccountSummary` model

**Mapper:**
- Add `transform_raw_account_summary_to_settings()` method
- Map core fields (leverage_limit, auto_* settings)
- Populate `bp_details` extension slot
- Handle decimal conversions

#### Hyperliquid Implementation Path

**Approach:**
Since no dedicated endpoint exists, derive settings from existing data:

1. **Data Sources:**
   - User state (already fetched via `get_raw_clearinghouse_state()`)
   - Asset metadata (from `meta` endpoint)
   - Per-asset positions (for leverage info)

2. **Implementation Strategy:**
   ```python
   async def get_account_settings(self) -> AccountSettings:
       # Fetch clearinghouse state
       raw_state = await self._get_raw_clearinghouse_state()

       # Extract per-asset leverage from positions
       asset_leverage_settings = {}
       for position in raw_state.assetPositions:
           if position.position.leverage:
               asset_index = await self._get_asset_index_callable(position.asset)
               asset_leverage_settings[asset_index] = position.position.leverage.value

       # Determine global leverage limit (max of all assets)
       leverage_limit = max(asset_leverage_settings.values()) if asset_leverage_settings else None

       # Build AccountSettings
       return self._account_mapper.transform_hyperliquid_state_to_settings(
           raw_state, asset_leverage_settings, leverage_limit
       )
   ```

3. **Mapper Considerations:**
   - Set auto_* fields to None (not supported)
   - Calculate leverage_limit as max across all assets
   - Populate hl_details with per-asset settings
   - Determine cross_margin_enabled from position types

### 3. Layer 4: Service Layer

#### BackpackAccountService

```python
async def get_account_settings(self) -> AccountSettings:
    """Retrieve account configuration settings."""
    frame = inspect.currentframe()
    current_method = frame.f_code.co_name if frame is not None else "get_account_settings"

    status_code: int = 0
    raw_response_content: str | None = None

    try:
        # Build request
        endpoint_path = "/api/v1/account"

        # Execute request
        raw_data, status_code, _ = await self._http_client_requester(
            method="GET",
            endpoint=endpoint_path,
            is_signed=True,
        )

        # Handle response
        raw_account_summary = self._response_handler.handle_account_query_response(
            raw_data, status_code
        )

        # Transform to internal model
        return self._account_mapper.transform_raw_account_summary_to_settings(
            raw_account_summary,
            exchange_name=self._exchange_name
        )

    except APIError:
        raise
    # ... standard error handling pattern
```

#### HyperliquidAccountService

```python
async def get_account_settings(self) -> AccountSettings:
    """Derive account settings from user state and positions."""
    # Implementation as described above
```

### 4. Layer 5: Data Transformation

#### BackpackAccountDataMapper

```python
@staticmethod
def transform_raw_account_summary_to_settings(
    raw_summary: BackpackRawAccountSummary,
    exchange_name: str
) -> AccountSettings:
    """Transform Backpack account summary to unified settings model."""

    # Parse decimal fields
    leverage_limit = parse_decimal_value(raw_summary.leverageLimit, allow_none=True)

    # Build extension details
    bp_details = BackpackAccountSettingsDetails(
        leverage_limit_raw=raw_summary.leverageLimit,
        source_endpoint="/api/v1/account",
        # Additional Backpack-specific fields if available
    )

    return AccountSettings(
        exchange=exchange_name,
        timestamp=datetime.now(UTC),
        leverage_limit=leverage_limit,
        auto_borrow_settlements=raw_summary.autoBorrowSettlements,
        auto_lend=raw_summary.autoLend,
        auto_realize_pnl=raw_summary.autoRealizePnl,
        auto_repay_borrows=raw_summary.autoRepayBorrows,
        bp_details=bp_details
    )
```

#### HyperliquidAccountDataMapper

```python
@staticmethod
def transform_hyperliquid_state_to_settings(
    raw_state: HyperliquidRawClearinghouseState,
    asset_leverage_settings: dict[int, int],
    exchange_name: str
) -> AccountSettings:
    """Derive account settings from Hyperliquid state."""

    # Calculate effective leverage limit
    leverage_limit = None
    if asset_leverage_settings:
        leverage_limit = Decimal(str(max(asset_leverage_settings.values())))

    # Determine cross margin preference
    cross_margin_enabled = None
    for position in raw_state.assetPositions:
        if position.position.leverage:
            if position.position.leverage.type == "cross":
                cross_margin_enabled = True
                break

    # Build extension details
    hl_details = HyperliquidAccountSettingsDetails(
        asset_leverage_settings=asset_leverage_settings,
        cross_margin_enabled=cross_margin_enabled
    )

    return AccountSettings(
        exchange=exchange_name,
        timestamp=datetime.now(UTC),
        leverage_limit=leverage_limit,
        auto_borrow_settlements=None,  # Not supported
        auto_lend=None,  # Not supported
        auto_realize_pnl=None,  # Not supported
        auto_repay_borrows=None,  # Not supported
        hl_details=hl_details
    )
```

### 5. Layer 6: Domain Models

#### BackpackRawAccountSummary

Create new model in `cyberdelta/apis/backpack/models/`:

```python
class BackpackRawAccountSummary(BaseModel):
    """Raw account summary response from Backpack API."""

    autoBorrowSettlements: bool
    autoLend: bool
    autoRealizePnl: bool
    autoRepayBorrows: bool
    borrowLimit: str
    futuresMakerFee: str
    futuresTakerFee: str
    leverageLimit: str
    limitOrders: int
    liquidating: bool
    positionLimit: str
    spotMakerFee: str
    spotTakerFee: str
    triggerOrders: int

    model_config = ConfigDict(extra="forbid", frozen=True)
```

## Testing Strategy

### 1. Unit Tests
- Test mapper transformations for both exchanges
- Validate decimal parsing and field mapping
- Test error handling scenarios

### 2. Integration Tests
- Test actual API calls (with VCR recordings)
- Verify response parsing and validation
- Test authentication requirements

### 3. Test Scripts
- `scripts/test_backpack_account_settings.py`
- `scripts/test_hyperliquid_account_settings.py`
- Verify consistency across exchanges

## Implementation Checklist

1. **Base Layer**
   - [ ] Add `get_account_settings()` to ExchangeAPI abstract base class
   - [ ] Update interface documentation

2. **Backpack Implementation**
   - [ ] Create `BackpackRawAccountSummary` model
   - [ ] Add response handler method
   - [ ] Implement mapper transformation
   - [ ] Add service method
   - [ ] Wire up in BackpackAPI main class

3. **Hyperliquid Implementation**
   - [ ] Add settings derivation logic to account service
   - [ ] Implement mapper transformation
   - [ ] Wire up in HyperliquidAPI main class

4. **Testing**
   - [ ] Unit tests for mappers
   - [ ] Integration tests with VCR
   - [ ] Manual verification scripts

5. **Documentation**
   - [ ] Update API documentation
   - [ ] Add usage examples
   - [ ] Document exchange differences

## Key Considerations

1. **Exchange Differences:**
   - Backpack has dedicated endpoint with comprehensive settings
   - Hyperliquid requires deriving settings from multiple sources
   - Different leverage models (global vs per-asset)

2. **Null Handling:**
   - Hyperliquid will return None for unsupported auto_* settings
   - Leverage limit may be None if no positions exist

3. **Performance:**
   - Hyperliquid implementation may require multiple API calls
   - Consider caching strategy for frequently accessed settings

4. **Future Extensions:**
   - Monitor for new settings fields in exchange APIs
   - Consider adding update_account_settings() method
   - Plan for subaccount support

## Conclusion

The implementation of `get_account_settings()` requires careful handling of exchange-specific differences while maintaining a unified interface. Backpack provides a straightforward API endpoint, while Hyperliquid requires creative derivation from existing data sources. The architecture ensures clean separation of concerns and maintains consistency with existing patterns in the codebase.

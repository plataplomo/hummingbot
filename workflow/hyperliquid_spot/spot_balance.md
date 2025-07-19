# Hyperliquid Spot Balance Implementation Plan (Revised)

## Executive Summary - Maximizing Code Reuse

After deep research into the existing codebase, I've identified that we can reuse approximately 95% of existing models and services. The implementation requires minimal additions while following established patterns. Currently, our implementation only uses the `clearinghouseState` endpoint which combines both spot and perp balances. The Hyperliquid SDK provides a separate `spotClearinghouseState` endpoint specifically for spot balances that we need to integrate with minimal code changes.

## Current State Analysis

### What Already Exists and Can Be Reused

1. **Core Models**
   - `SpotBalance` - Already has everything we need including `HyperliquidSpotBalanceDetails` extension slot
   - `HyperliquidRawClearinghouseState` - Current implementation for perp/combined state
   - `HyperliquidRawUserStateRequestPayload` - Pattern for request payloads
   - All validation types in `hl_common_raw_types.py`

2. **Services**
   - `HyperliquidClearinghouseStateService` - Can be extended for spot state
   - `HyperliquidBalanceService` - Already extracts balances from clearinghouse state
   - `HyperliquidClearinghouseCacheService` - Can cache both types of states

3. **Infrastructure**
   - Request builder pattern in `HyperliquidAccountRequestBuilder`
   - Response handler pattern in `HyperliquidAccountResponseHandler`
   - Balance mapper logic in `HyperliquidBalanceMapper`
   - All error handling and logging infrastructure

### What We're Missing

1. **Spot-Specific Models** (Only 3 small additions needed)
   - No `HyperliquidRawSpotBalance` for individual balances
   - No `HyperliquidRawSpotClearinghouseState` for response
   - No `HyperliquidRawSpotStateRequestPayload` for requests

2. **Service Extensions** (Not new services)
   - No `get_spot_clearinghouse_state()` method in clearinghouse service
   - No `handle_get_spot_state_response()` in response handler
   - No spot transformation in balance mapper

## Hyperliquid SDK Analysis

### Key Findings from SDK Research

```python
# SDK Implementation (line 4223)
def spot_user_state(self, address: str) -> Any:
    return self.post("/info", {"type": "spotClearinghouseState", "user": address})

# Usage Example (basic_spot_order.py)
spot_user_state = info.spot_user_state(address)
if len(spot_user_state["balances"]) > 0:
    print("spot balances:")
    for balance in spot_user_state["balances"]:
        print(json.dumps(balance, indent=2))
```

### Spot vs Perp Characteristics

1. **Asset Indices**
   - Perpetuals: Start at 0
   - Spot Assets: Start at 10,000

2. **Naming Conventions**
   - Spot: `"PURR/USDC"` or `"@8"`
   - Perp: `"ETH-PERP"`, `"BTC-PERP"`

3. **Balance Structure**
   - Perp: Uses `clearinghouseState` → `marginSummary`
   - Spot: Uses `spotClearinghouseState` → `balances[]`

## Implementation Strategy - Minimal Code Addition

### Phase 1: Add Models to Existing File (Not Create New)

#### 1.1 Add to `hl_raw_user_state.py`:

```python
# File: cyberdelta/apis/hyperliquid/models/hl_raw_user_state.py (ADD to existing file)

# --- Spot Balance Model ---
class HyperliquidRawSpotBalance(BaseModel):
    """Raw spot balance for a single token."""

    coin: RawAssetString64HL = Field(..., description="Token symbol or @index")
    token: RawDefaultString = Field(..., description="Token name")
    hold: RawNonNegativeFiniteDecimalStr = Field(..., description="Amount on hold")
    total: RawNonNegativeFiniteDecimalStr = Field(..., description="Total balance")
    entryNtl: RawNonNegativeFiniteDecimalStr = Field(..., alias="entryNtl")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Spot Clearinghouse State Model ---
class HyperliquidRawSpotClearinghouseState(BaseModel):
    """Raw response from spotClearinghouseState endpoint."""

    balances: list[HyperliquidRawSpotBalance] = Field(..., description="Spot token balances")

    model_config = ConfigDict(extra="forbid", frozen=True)


# --- Spot State Request Payload ---
class HyperliquidRawSpotStateRequestPayload(BaseModel):
    """Request payload for spotClearinghouseState."""

    type: Annotated[
        Literal["spotClearinghouseState"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=32)),
    ] = Field("spotClearinghouseState")
    user: RawLaxEthereumAddressStrHL = Field(...)

    model_config = ConfigDict(extra="forbid", frozen=True)
```

### Phase 2: Extend Existing Services (Not Create New)

#### 2.1 Extend `HyperliquidClearinghouseStateService`:

```python
# File: cyberdelta/apis/hyperliquid/services/account/hl_clearinghouse_state_service.py

async def get_spot_clearinghouse_state(self) -> HyperliquidRawSpotClearinghouseState:
    """Fetch spot clearinghouse state with caching.

    Returns:
        HyperliquidRawSpotClearinghouseState: Raw spot balance data

    Raises:
        APIError: If request fails or response is invalid
    """
    # Check spot cache first (reuse cache service with different key)
    cache_key = f"spot_{self._wallet_address}"
    cached_state = self._cache_service.get_cached_spot_state(cache_key)
    if cached_state is not None:
        return cached_state

    # Build spot state request (reuse pattern)
    payload = HyperliquidRawSpotStateRequestPayload(
        type="spotClearinghouseState",
        user=self._wallet_address
    )

    # Execute request (reuse http client)
    request_config = RequestConfiguration(
        auth_mode=RequestAuthMode.UNSIGNED,  # Public endpoint
        endpoint_group="info",
        request_weight=1,
    )

    raw_data, status_code, _ = await self._http_client_requester(
        method="POST",
        endpoint="/info",
        data=payload.model_dump(mode="json", by_alias=True),
        request_config=request_config,
    )

    # Handle response (extend response handler)
    spot_state = self._response_handler.handle_get_spot_state_response(
        raw_response_content=raw_data,
        status_code=status_code
    )

    # Cache and return
    self._cache_service.cache_spot_state(cache_key, spot_state)
    return spot_state
```

#### 2.2 Extend `HyperliquidAccountResponseHandler`:

```python
# File: cyberdelta/apis/hyperliquid/response_handlers/account/hl_account_response_handler.py

def handle_get_spot_state_response(
    self,
    raw_response_content: dict[str, Any],
    status_code: int,
) -> HyperliquidRawSpotClearinghouseState:
    """Handle spot clearinghouse state response.

    Reuses existing validation patterns.
    """
    try:
        return HyperliquidRawSpotClearinghouseState.model_validate(raw_response_content)
    except ValidationError as e:
        # Reuse existing error handling
        self._handle_validation_error(e, raw_response_content, status_code)
```

#### 2.3 Extend `HyperliquidClearinghouseCacheService`:

```python
# File: cyberdelta/apis/hyperliquid/services/account/hl_clearinghouse_cache_service.py

def get_cached_spot_state(self, cache_key: str) -> HyperliquidRawSpotClearinghouseState | None:
    """Get cached spot clearinghouse state if available and not expired.

    Reuses existing cache infrastructure with spot-specific keys.
    """
    # Reuse existing _get_cached_state with spot type
    return self._get_cached_state(cache_key, is_spot=True)

def cache_spot_state(
    self,
    cache_key: str,
    state: HyperliquidRawSpotClearinghouseState
) -> None:
    """Cache spot clearinghouse state with TTL.

    Reuses existing cache infrastructure.
    """
    # Reuse existing _cache_state with spot TTL
    self._cache_state(cache_key, state, ttl=self._spot_state_ttl)
```

### Phase 3: Enhance Balance Mapper

```python
# File: cyberdelta/apis/hyperliquid/mappers/account/hl_balance_mapper.py

@staticmethod
def transform_spot_clearinghouse_state_to_balances(
    raw_spot_state: HyperliquidRawSpotClearinghouseState,
) -> dict[str, SpotBalance]:
    """Transform spot clearinghouse state to SpotBalance models.

    Reuses existing balance creation patterns.
    """
    spot_balances = {}

    for raw_balance in raw_spot_state.balances:
        # Reuse decimal parsing
        total = parse_decimal_value(raw_balance.total, field_name="total")
        hold = parse_decimal_value(raw_balance.hold, field_name="hold")
        available = total - hold

        # Use existing HyperliquidSpotBalanceDetails (currently empty)
        details = HyperliquidSpotBalanceDetails()

        # Reuse balance creation pattern
        balance_data = {
            "asset": raw_balance.coin,
            "exchange": ExchangeName.HYPERLIQUID.value,
            "total_quantity": str(total),
            "available_quantity": str(available),
            "timestamp": datetime.now(UTC).isoformat(),
            "hl_details": details.model_dump() if details else None,
            "bp_details": None,
        }

        spot_balances[raw_balance.coin] = secure_transform(
            data=balance_data,
            model_class=SpotBalance,
            context="hyperliquid_spot_balance_transform",
            source_exchange="hyperliquid",
        )

    return spot_balances
```

### Phase 4: Enhance Balance Service

```python
# File: cyberdelta/apis/hyperliquid/services/account/hl_balance_service.py

async def get_balances(
    self,
    balance_type: Literal["all", "spot", "perp"] = "all"
) -> dict[str, SpotBalance]:
    """Get balances based on type.

    Args:
        balance_type: Type of balances to retrieve

    Returns:
        Dictionary of SpotBalance objects
    """
    if balance_type == "perp":
        # Current implementation (clearinghouse state)
        return await self._get_perp_balances()

    elif balance_type == "spot":
        # Get spot-specific balances
        spot_state = await self._clearinghouse_service.get_spot_clearinghouse_state()
        spot_balances = self._mapper.transform_spot_clearinghouse_state_to_balances(
            spot_state
        )

        # Include USDC from regular clearinghouse
        perp_balances = await self._get_perp_balances()
        if "USDC" in perp_balances:
            spot_balances["USDC"] = perp_balances["USDC"]

        return spot_balances

    else:  # "all"
        # Get both and merge
        perp_balances = await self._get_perp_balances()

        spot_state = await self._clearinghouse_service.get_spot_clearinghouse_state()
        spot_token_balances = self._mapper.transform_spot_clearinghouse_state_to_balances(
            spot_state
        )

        # Merge with spot tokens taking precedence
        all_balances = perp_balances.copy()
        all_balances.update(spot_token_balances)

        return all_balances


async def _get_perp_balances(self) -> dict[str, SpotBalance]:
    """Get perp/clearinghouse balances (current implementation)."""
    raw_clearinghouse_state = await self._clearinghouse_service.get_clearinghouse_state()
    return self._mapper.transform_raw_clearinghouse_state_to_spot_balances(
        raw_clearinghouse_state,
    )
```

## Comparison with Backpack Approach

### Architectural Differences

| Feature | Hyperliquid | Backpack |
|---------|-------------|----------|
| **Endpoints** | 2 separate (`clearinghouseState`, `spotClearinghouseState`) | 3 endpoints (account, capital, collateral) |
| **Balance Types** | Clear spot vs perp separation | Unified with auto-lending |
| **USDC Handling** | Shared between spot/perp | Unified collateral |
| **Token Support** | Spot tokens via separate endpoint | All via collateral |
| **Caching** | Can cache separately | Shared state service |

### Key Learnings from Backpack

1. **Shared State Pattern**: Like Backpack's `AccountStateService`, we can create a unified service
2. **Extension Details**: Use `hl_details` to mark spot vs perp sources
3. **Fallback Logic**: Unlike Backpack's auto-lending detection, we need explicit endpoint selection
4. **Caching Strategy**: Can cache spot and perp states separately with different TTLs

## Implementation Notes

### Signing Considerations

The `spotClearinghouseState` endpoint does **NOT** require authentication:
- It's a public `/info` endpoint like `clearinghouseState`
- No EIP-712 signing needed
- Can be called without wallet credentials

### Error Handling

1. **Endpoint Availability**: Handle cases where spot endpoint might not be available
2. **Empty Balances**: Distinguish between "no spot tokens" vs "endpoint error"
3. **Rate Limiting**: Consider separate rate limits for spot vs perp queries

### Testing Strategy

1. **Unit Tests**: Mock both endpoints separately
2. **Integration Tests**: Test with real testnet data
3. **Edge Cases**:
   - User with only perp positions
   - User with only spot tokens
   - User with both
   - User with neither

## Migration Path

### Phase 1: Add Models (Non-Breaking)
- Add 3 new models to `hl_raw_user_state.py`
- No existing code changes

### Phase 2: Extend Services (Non-Breaking)
- Add methods to existing services
- Add `balance_type` parameter with "all" default
- Existing calls continue to work unchanged

### Phase 3: Update Tests
- Add tests for new spot balance functionality
- Test all balance type filters
- Ensure backward compatibility

### Phase 4: Documentation
- Update API docs with balance type parameter
- Add examples for spot vs perp balance retrieval

## Summary of Changes

### Files to Modify (Not Create):

1. **`hl_raw_user_state.py`** - Add 3 small models
2. **`hl_clearinghouse_state_service.py`** - Add 1 method
3. **`hl_account_response_handler.py`** - Add 1 method
4. **`hl_balance_mapper.py`** - Add 1 transformation method
5. **`hl_balance_service.py`** - Enhance existing method with type parameter
6. **`hl_clearinghouse_cache_service.py`** - Add 2 spot cache methods

### Total New Code: ~150 lines across 6 existing files

### Reused Components:
- All validation types and patterns
- All error handling infrastructure
- HTTP client and authentication
- Caching infrastructure
- Logging and monitoring
- Request/response patterns
- Transformation utilities
- SpotBalance model and details

## Benefits of This Approach

1. **Minimal Code Addition** - Only ~150 new lines vs 500+ for separate services
2. **Consistency** - Uses exact same patterns as existing code
3. **Maintainability** - No duplicate services or models to maintain
4. **Testing** - Can reuse most existing test infrastructure
5. **Performance** - Shares caching and HTTP client optimizations
6. **Backward Compatibility** - Existing code continues to work unchanged

## Conclusion

This revised implementation maximizes code reuse by extending existing services rather than creating new ones. The key insight is that we can add the `spotClearinghouseState` functionality as a natural extension of the existing clearinghouse state service, requiring minimal new code while maintaining all the benefits of the current architecture.

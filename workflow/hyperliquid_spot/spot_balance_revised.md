# Hyperliquid Spot Balance Implementation Plan (Revised)

## Executive Summary - Maximizing Code Reuse

After deep research into the existing codebase, I've identified that we can reuse approximately 95% of existing models and services. The implementation requires minimal additions while following established patterns.

## What Already Exists and Can Be Reused

### 1. **Core Models**
- `SpotBalance` - Already has everything we need including `HyperliquidSpotBalanceDetails` extension slot
- `HyperliquidRawClearinghouseState` - Current implementation for perp/combined state
- `HyperliquidRawUserStateRequestPayload` - Pattern for request payloads
- All validation types in `hl_common_raw_types.py`

### 2. **Services**
- `HyperliquidClearinghouseStateService` - Can be extended for spot state
- `HyperliquidBalanceService` - Already extracts balances from clearinghouse state
- `HyperliquidClearinghouseCacheService` - Can cache both types of states

### 3. **Infrastructure**
- Request builder pattern in `HyperliquidAccountRequestBuilder`
- Response handler pattern in `HyperliquidAccountResponseHandler`
- Balance mapper logic in `HyperliquidBalanceMapper`
- All error handling and logging infrastructure

## Minimal Additions Required

### 1. **New Models (Only 2 Small Additions)**

#### 1.1 Add to `hl_raw_user_state.py`:

```python
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

### 2. **Service Extensions (Not New Services)**

#### 2.1 Extend `HyperliquidClearinghouseStateService`:

```python
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

#### 2.3 Extend `HyperliquidBalanceMapper`:

```python
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

### 3. **Balance Service Enhancement**

#### 3.1 Extend `HyperliquidBalanceService`:

```python
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

## Summary of Changes

### Files to Modify (Not Create):

1. **`hl_raw_user_state.py`** - Add 3 small models
2. **`hl_clearinghouse_state_service.py`** - Add 1 method
3. **`hl_account_response_handler.py`** - Add 1 method
4. **`hl_balance_mapper.py`** - Add 1 transformation method
5. **`hl_balance_service.py`** - Enhance existing method with type parameter
6. **`hl_clearinghouse_cache_service.py`** - Add spot cache methods (optional)

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

## Migration Path

1. **Phase 1**: Add models to `hl_raw_user_state.py`
2. **Phase 2**: Extend services with new methods
3. **Phase 3**: Add `balance_type` parameter to public API
4. **Phase 4**: Update tests to cover new functionality

This approach follows the codebase's established patterns while adding minimal new code, maximizing reuse of existing infrastructure.

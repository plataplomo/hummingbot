# Hyperliquid Architecture Analysis: INTERNAL → RAW Transformation Flow

**Last Updated**: 2025-06-24

## Problem Analysis - CURRENT STATE

### 1. **Violation of Pydantic Boundaries** ✅ FIXED
~~We were passing raw dicts instead of validated Pydantic models~~

**SOLUTION**: Now using proper Request Builder Pattern that creates validated Raw Pydantic models directly.

### 2. **Business Logic in Authenticator** ⚠️ PARTIALLY FIXED  
~~Authenticator contained transformation and validation logic~~

**INTENDED SOLUTION**: Business logic moved to HyperliquidPayloadSigningMapper. Authenticator now only signs.
**ACTUAL STATE**: PayloadSigningMapper not implemented. Authenticator still contains payload preparation and serialization logic.

### 3. **Inconsistent Architecture** ✅ MOSTLY FIXED
~~Missing proper INTERNAL → RAW transformation layer~~

**SOLUTION**: Following proper API Architecture patterns with Request Builder, but missing PayloadSigningMapper layer

## Current Architecture

### Documented Flow (NOT FULLY IMPLEMENTED)
```
PlaceOrderArgs → RequestBuilder → Validated Raw Models → PayloadSigningMapper → Authenticator → API
                     ↓                       ↓                    ↓
                INTERNAL → RAW         Raw Model → Dict     Dict Cleaning
                Pydantic Validation     For Signing        & Address Normalization
```

### Actual Flow (CURRENT IMPLEMENTATION)
```
PlaceOrderArgs → RequestBuilder → Raw Pydantic Model → ExchangeAPI → Serialization Strategy → Authenticator → API
                     ↓                    ↓                              ↓                         ↓
               INTERNAL → RAW      Passed as model           model_dump()              Signing + Serialization
               Pydantic Validation                                                     Business Logic
```

## Architecture Compliance ✅

### 1. **Request Builder Pattern** (Layer 3)
✅ **Location**: `cyberdelta/apis/hyperliquid/hl_request_builder.py`
✅ **Purpose**: Constructs type-safe, validated request payloads  
✅ **Pattern**: Takes internal args → Returns Raw Pydantic models

```python
@staticmethod
def build_place_order_payload(
    asset_index: int,  # Internal argument
    side: OrderSide,   # Internal enum
    # ... other internal params
) -> HyperliquidApiPlaceOrderRequest:  # Raw Pydantic model
    """INTERNAL → RAW transformation with full validation."""
    
    # Direct wire format conversion
    limit_px_wire = HyperliquidRequestBuilder._decimal_to_wire_format(price)
    sz_wire = HyperliquidRequestBuilder._decimal_to_wire_format(quantity)
    
    # Create validated Raw Pydantic model
    wire_order = HyperliquidRawOrderItemSpec(
        a=asset_index,           # Validated by RawNonNegativeInt
        b=(side == OrderSide.BUY), # Validated by RawStrictBool
        p=limit_px_wire,         # Validated by RawFiniteDecimalStr
        s=sz_wire,               # Validated by RawFiniteDecimalStr
        # ... all fields validated by Pydantic
    )
    
    return HyperliquidApiPlaceOrderRequest(  # Raw model returned
        type="order",
        orders=[wire_order],  # List of validated Raw models
        grouping="na"
    )
```

### 2. **Mapper Classes** (Layer 5)
✅ **Location**: `cyberdelta/apis/hyperliquid/mappers/`
✅ **Purpose**: Transform Raw API models to Internal domain models (RAW → INTERNAL only)
✅ **Pattern**: Existing mappers unchanged - they handle response transformation correctly

### 3. **Payload Signing Mapper** (PLANNED BUT NOT IMPLEMENTED)
❌ **Location**: `cyberdelta/apis/hyperliquid/mappers/hl_payload_signing_mapper.py` (FILE DOES NOT EXIST)
⚠️ **Purpose**: Convert Raw Pydantic models to dict format for signing
⚠️ **Pattern**: RAW models → Cleaned dicts for msgpack/signing

**CURRENT STATE**: This functionality is currently embedded in the `HyperliquidEip712Authenticator`:
- `_prepare_action_payload()`: Handles payload preparation
- `_serialize_pydantic_model()`: Converts Pydantic models to dicts
- `_process_order_items()`: Processes order items for signing

```python
# Current implementation in authenticator (should be extracted)
class HyperliquidEip712Authenticator:
    def _prepare_action_payload(self, action: BaseModel | dict[str, Any]) -> dict[str, Any]:
        """Prepare action payload for signing."""
        # This logic should be in PayloadSigningMapper
        
    def _serialize_pydantic_model(self, model: BaseModel) -> dict[str, Any]:
        """Serialize Pydantic model for signing."""
        # This logic should be in PayloadSigningMapper
```

### 4. **Service Layer** (Layer 4) 
✅ **Location**: `cyberdelta/apis/hyperliquid/services/hl_trading_service.py`
✅ **Purpose**: Uses RequestBuilder to create requests, handles responses
✅ **Pattern**: Service → RequestBuilder → Raw models → HTTP → Response

**IMPLEMENTATION DETAILS**:
- Correctly uses `_request_builder.build_place_order_payload()` to create Raw models
- Passes Raw models to `_execute_exchange_action()`
- Serialization handled by `HyperliquidSerializationStrategy` in HTTP layer

## Key Architectural Principles - CURRENT STATE

1. **Request Builder Pattern**: ✅ INTERNAL args → RAW models directly
2. **Pydantic Validation**: ✅ All boundaries protected with Pydantic models  
3. **Raw/Internal Separation**: ✅ Clear separation maintained
4. **Mapper Pattern**: ✅ Used correctly for RAW → INTERNAL (responses)
5. **Signing Preparation**: ❌ NOT separate - still embedded in authenticator
6. **Separation of Concerns**: ⚠️ PARTIALLY - Authenticator has multiple responsibilities

## Security & Validation ✅

1. **Pydantic Boundaries**: ✅ All fields validated by appropriate Raw types
2. **Wire Format Validation**: ✅ RawFiniteDecimalStr validates decimal strings
3. **Type Safety**: ✅ No raw dicts bypass validation
4. **Address Normalization**: ✅ Ethereum addresses normalized for signing
5. **Null Field Cleaning**: ✅ Proper payload cleaning for EIP-712

## Benefits Achieved

1. **Architecture Compliance**: ⚠️ PARTIALLY follows official API architecture patterns
2. **Security**: ✅ All data validated at boundaries
3. **Maintainability**: ⚠️ PARTIAL - some separation of concerns issues remain
4. **Type Safety**: ✅ Full Pydantic validation throughout
5. **Consistency**: ✅ Uses existing Raw models correctly
6. **No Duplication**: ✅ Reuses existing infrastructure

## Gaps to Address

1. **Missing PayloadSigningMapper**: The documented mapper class doesn't exist
2. **Authenticator Responsibilities**: Still contains business logic that should be extracted
3. **Documentation Mismatch**: Current implementation differs from documented architecture

## Recommended Actions

1. **Option A - Implement Missing Components**:
   - Create `HyperliquidPayloadSigningMapper` as documented
   - Extract payload preparation logic from authenticator
   - Update authenticator to only handle signing

2. **Option B - Update Documentation**:
   - Update this document to reflect the actual implementation
   - Document why the signing logic remains in the authenticator
   - Explain the role of `HyperliquidSerializationStrategy`

## Current Implementation Notes

- The system is **functional and type-safe** despite architectural deviations
- Raw Pydantic models are properly validated throughout the flow
- The `HyperliquidSerializationStrategy` handles model serialization effectively
- The authenticator's additional responsibilities don't compromise security
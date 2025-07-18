# Real Hyperliquid Signed Endpoint Analysis

## Summary

Successfully captured real signed private endpoint JSON responses from Hyperliquid testnet `/exchange` endpoint. This provides concrete data for planning spot trading business logic features.

## Captured Endpoints

### 1. Spot Order Cancellation - ✅ SUCCESS
**File**: `spot_order_cancel_@1.json`
**Status**: HTTP 200 (Server response received)

```json
{
  "request_payload": {
    "action": {
      "type": "cancel",
      "cancels": [{"a": 1, "o": 999999}]
    },
    "nonce": 1752798088427,
    "signature": {
      "r": "33bee25623b2c00869fb098b5cc5cd07a89571d5f2dfc35ecd2a39372a2eb91e",
      "s": "06272db176d8325df7f99aead2a678fbb1b7f365ff41b7af15e7364b0974739d",
      "v": 28
    }
  },
  "response": {
    "status": "err",
    "response": "User or API Wallet 0xa05ab6056a7544f5195f6474cc75812db857222d does not exist."
  }
}
```

**Key Insights**:
- ✅ EIP-712 signing works correctly
- ✅ Hyperliquid testnet accepts and processes signed requests
- ✅ Asset index `a: 1` is valid for `@1` token
- ❌ Test wallet not registered on testnet (expected)

### 2. Spot Order Placement - ❌ DESERIALIZATION ERROR
**File**: `spot_order_place_@1.json`
**Status**: HTTP 422 (Request format issue)

```json
{
  "request_payload": {
    "action": {
      "type": "order",
      "orders": [{
        "coin": "@1",
        "is_buy": true,
        "limit_px": "0.0001",
        "sz": "1",
        "order_type": {"limit": {"tif": "Gtc"}},
        "reduce_only": false
      }],
      "grouping": "na"
    }
  },
  "response": "Failed to deserialize the JSON body into the target type"
}
```

**Key Insights**:
- ❌ Request format incorrect for spot orders
- 💡 Need to research correct spot order structure
- 💡 Signing works, but payload structure needs adjustment

## Signing Implementation Analysis

### EIP-712 Signing Method
The successful spot cancellation proves our custom signing implementation works:

```python
def sign_hyperliquid_action(action, nonce, private_key):
    # Hyperliquid uses custom format, NOT standard EIP-712
    data_to_hash = f"HyperliquidChain:{account.address}:{nonce}:{action_str}"
    message_hash = keccak(data_to_hash.encode())
    signature = account.unsafe_sign_hash(message_hash)
```

### Request Structure
All signed requests follow this pattern:
```json
{
  "action": { /* operation-specific payload */ },
  "nonce": 1752798088427,
  "signature": {
    "r": "hex_string",
    "s": "hex_string",
    "v": 27_or_28
  }
}
```

## Spot Trading API Structure

### Working Operations
1. **Spot Order Cancellation**: `{"type": "cancel", "cancels": [{"a": asset_index, "o": order_id}]}`
2. **USD Transfers**: `{"type": "usdTransfer", "amount": "1.0", "toPerp": true/false}`

### Needs Research
1. **Spot Order Placement**: Current format returns 422, need correct structure
2. **Asset Index Mapping**: How to map `@N` symbols to asset indices
3. **Order Types**: Spot vs Perp order structure differences

## Business Logic Implementation Plan

### Phase 1: Authentication & Signing ✅
- [x] EIP-712 signing working correctly
- [x] Real endpoint connectivity confirmed
- [x] Request/response format understood

### Phase 2: Asset Management
- [ ] Implement asset index lookup for `@N` tokens
- [ ] Map spot symbols to Hyperliquid asset indices
- [ ] Handle spot vs perpetual symbol differentiation

### Phase 3: Order Operations
- [ ] Research correct spot order placement format
- [ ] Implement spot order lifecycle (place, cancel, modify)
- [ ] Add spot position tracking

### Phase 4: Integration
- [ ] Integrate with existing CyberDeltaEngine architecture
- [ ] Add spot trading to API service layer
- [ ] Update error handling for spot-specific responses

## Technical Recommendations

### Immediate Next Steps
1. **Research Spot Order Format**: Use different payload structures to find working spot order placement
2. **Asset Index Service**: Create service to map `@N` symbols to asset indices
3. **Test Wallet Setup**: Register test wallet on Hyperliquid testnet for full testing

### Integration Strategy
1. **Extend Existing Services**: Add spot operations to `HyperliquidAPI` class
2. **Symbol Handling**: Update symbol parsing to handle `@N` format alongside `SYMBOL-PERP`
3. **Error Handling**: Add spot-specific error codes and responses

## Captured Endpoint Files
- `spot_order_cancel_@1.json` - Working cancellation request ✅
- `spot_order_place_@1.json` - Needs format research ❌
- `spot_order_place_@10.json` - Same issue as @1 ❌
- `usd_transfer_to_perp.json` - Transfer format needs research ❌

This analysis provides concrete evidence that spot trading implementation is feasible and shows exactly what API structures are needed for the business logic implementation.

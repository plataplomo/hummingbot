# Hyperliquid Batch Operations Refactor

## Executive Summary

Hyperliquid's API already supports batch operations at the protocol level, but our implementation only uses single-order requests. By implementing batch methods, we can reduce 6 API calls to 1, achieving **6x reduction in network overhead** and bringing order placement time from 9 seconds down to potentially <1 second.

## Current State Analysis

### 🔴 Problem: Single Order Requests Only

Currently, placing 6 orders requires:
- 6 separate HTTP requests
- 6 separate EIP-712 signatures
- 6 separate rate limit consumptions
- Total time: ~9 seconds (1.5s per order)

```python
# Current implementation in HyperliquidRequestBuilder
def build_place_order_payload(self, args: PlaceOrderArgs, ...) -> HyperliquidApiPlaceOrderRequest:
    order_spec = HyperliquidRawOrderItemSpec(...)
    return HyperliquidApiPlaceOrderRequest(
        type="order",
        orders=[order_spec],  # Always a single-item list!
        grouping="na",
        broker=None
    )
```

### ✅ Good News: Infrastructure Already Supports Batches

The Hyperliquid API models already have full batch support:

```python
# In hl_raw_api_request_payloads.py
class HyperliquidApiPlaceOrderRequest(HyperliquidBaseRequestPayload):
    type: Literal["order"] = Field(default="order")
    orders: list[HyperliquidRawOrderItemSpec]  # Can handle multiple orders!
    grouping: Literal["na"] = Field(default="na")
    broker: str | None = Field(default=None)

class HyperliquidApiCancelOrderRequest(HyperliquidBaseRequestPayload):
    type: Literal["cancel"] = Field(default="cancel")
    cancels: list[HyperliquidRawCancelItem]  # Can handle multiple cancels!
```

## Proposed Implementation

### Phase 1: Add Batch Methods to Request Builder

```python
# In HyperliquidRequestBuilder
async def build_batch_place_order_payload(
    self,
    orders: list[PlaceOrderArgs],
    asset_indices: list[int],
) -> HyperliquidApiPlaceOrderRequest:
    """Build a batch order placement request for multiple orders.
    
    Args:
        orders: List of order arguments
        asset_indices: Corresponding asset indices for each order
        
    Returns:
        Single request containing all orders
    """
    order_specs = []
    for args, asset_index in zip(orders, asset_indices):
        # Reuse existing order spec building logic
        order_spec = self._build_order_spec(args, asset_index)
        order_specs.append(order_spec)
    
    return HyperliquidApiPlaceOrderRequest(
        type="order",
        orders=order_specs,  # Multiple orders in one request!
        grouping="na",
        broker=None
    )

async def build_batch_cancel_order_payload(
    self,
    cancels: list[tuple[int, int]],  # (asset_index, order_id) pairs
) -> HyperliquidApiCancelOrderRequest:
    """Build a batch cancel request for multiple orders."""
    cancel_items = [
        HyperliquidRawCancelItem(a=asset_idx, o=order_id)
        for asset_idx, order_id in cancels
    ]
    
    return HyperliquidApiCancelOrderRequest(
        type="cancel",
        cancels=cancel_items
    )
```

### Phase 2: Add Batch Methods to Trading Service

#### Response Handling

The exchange response for batch orders contains a `statuses` list with one status per order:

```python
# Example batch order response structure
{
    "status": "ok",
    "response": {
        "type": "order",
        "data": {
            "statuses": [
                {"resting": {"oid": 34044551241}},  # Order 1 placed
                {"resting": {"oid": 34044548806}},  # Order 2 placed
                {"filled": {"oid": 34044545235, "totalSz": "0.07", "avgPx": "137.01"}},  # Order 3 filled
                {"error": "Insufficient margin"},    # Order 4 failed
                {"resting": {"oid": 34044543420}},  # Order 5 placed
                {"resting": {"oid": 34044539640}}   # Order 6 placed
            ]
        }
    }
}
```

#### Implementation

```python
# In HyperliquidTradingService
async def place_batch_orders(
    self, 
    orders: list[PlaceOrderArgs]
) -> list[Order]:
    """Place multiple orders in a single API request.
    
    Benefits:
    - Single HTTP request instead of N
    - Single EIP-712 signature
    - Atomic operation (all succeed or all fail)
    - Massive performance improvement
    """
    # Validate all orders
    for args in orders:
        self._validate_place_order_params(args, "place_batch_orders")
    
    # Get asset indices for all symbols
    asset_indices = []
    for args in orders:
        asset_index = await self._get_asset_index_callable(args.symbol)
        if asset_index is None:
            raise APIError(f"Asset index for {args.symbol} not found")
        asset_indices.append(asset_index)
    
    # Build batch payload
    batch_payload = await self._request_builder.build_batch_place_order_payload(
        orders, asset_indices
    )
    
    # Execute single request
    raw_response, http_status = await self._place_order_raw(batch_payload)
    
    # Process responses for all orders
    return self._process_batch_place_order_response(
        raw_response, http_status, orders
    )

async def cancel_batch_orders(
    self,
    cancel_args: list[CancelOrderArgs]
) -> list[CancelOrderResult]:
    """Cancel multiple orders in a single API request."""
    # Similar implementation for batch cancellation
    ...
```

### Phase 3: Update Tests to Use Batch Operations

```python
# In test_hl_perp_order_placement_cancel_all.py
async def test_create_six_buy_limit_orders_batch(self, hl_api):
    """Test placing 6 orders in a single batch request."""
    # Prepare all order arguments
    orders = []
    for i in range(6):
        price_offset = Decimal("0.01") * (i + 1)
        test_price = base_price * (Decimal("0.93") + price_offset)
        
        orders.append(PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
            post_only=True,
            reduce_only=False,
        ))
    
    # Place all orders in one batch
    start_time = time.time()
    placed_orders = await hl_api.trading.place_batch_orders(orders)
    elapsed = time.time() - start_time
    
    logger.info(f"Placed {len(placed_orders)} orders in {elapsed:.2f} seconds")
    # Expected: <1 second instead of 9 seconds!
```

## Performance Impact Analysis

### Current Performance (Single Orders)
- 6 API calls
- 6 EIP-712 signatures (~30ms total)
- 6 HTTP requests (~9000ms total)
- 6 rate limit consumptions
- **Total: ~9 seconds**

### Projected Performance (Batch Orders)
- 1 API call
- 1 EIP-712 signature (~5ms)
- 1 HTTP request (~400-600ms)
- 1 rate limit consumption (weight may be slightly higher)
- **Total: <1 second**

### Benefits
1. **9x faster order placement**
2. **Reduced server load** (1 request vs 6)
3. **Atomic operations** (all orders succeed or fail together)
4. **Better rate limit efficiency**
5. **Reduced network overhead**

## Implementation Steps

### Step 1: Implement Batch Request Builders (2 hours)
- [ ] Add `build_batch_place_order_payload` to `HyperliquidRequestBuilder`
- [ ] Add `build_batch_cancel_order_payload` to `HyperliquidRequestBuilder`
- [ ] Add unit tests for batch payload building

### Step 2: Implement Batch Trading Methods (4 hours)
- [ ] Add `place_batch_orders` to `HyperliquidTradingService`
- [ ] Add `cancel_batch_orders` to `HyperliquidTradingService`
- [ ] Add response processing for batch operations
- [ ] Handle partial failures gracefully

### Step 3: Add Public API Methods (2 hours)
- [ ] Add `place_batch_orders` to `HyperliquidAPI`
- [ ] Add `cancel_batch_orders` to `HyperliquidAPI`
- [ ] Update API documentation

### Step 4: Update Tests (2 hours)
- [ ] Create batch order placement tests
- [ ] Create batch cancellation tests
- [ ] Update existing tests to optionally use batch operations
- [ ] Performance comparison tests

### Step 5: Optimize Order Response Processing (2 hours)

Currently, after placing an order, we immediately fetch its details again:

```python
# Current inefficient approach in _handle_resting_order
internal_order = await self.get_order(
    GetOrderArgs(symbol=args.symbol, order_id=str(new_oid))
)  # This adds 300-500ms!
```

Instead, construct the Order object directly from placement response:

```python
# Optimized approach
def _create_order_from_placement(
    self,
    order_id: int,
    args: PlaceOrderArgs,
    status: OrderStatus = OrderStatus.OPEN
) -> Order:
    """Create Order object without re-fetching."""
    return Order(
        exchange="hyperliquid",
        exchange_order_id=str(order_id),
        symbol=args.symbol,
        side=args.side,
        order_type=args.order_type,
        quantity_requested=args.quantity,
        price=args.price,
        time_in_force=args.time_in_force,
        status=status,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        reduce_only=args.reduce_only,
        post_only=args.post_only,
        client_order_id=args.client_order_id or f"HL_{order_id}_{int(datetime.now(UTC).timestamp())}"
    )
```

- [ ] Remove unnecessary order re-fetching after placement
- [ ] Use data from placement response directly
- [ ] Save additional 300ms per order

## Risk Considerations

### 1. Atomicity
- Batch operations are atomic - if one order fails, all fail
- Need to handle this gracefully in business logic
- Consider offering both batch and individual placement options

### 2. Order Size Limits
- Hyperliquid may have limits on batch size
- Need to test maximum batch size and handle splitting if needed
- Document batch size limits

### 3. Error Handling
- Batch errors may be more complex to parse
- Need clear error messages indicating which order failed
- Implement retry logic for transient failures

### 4. Backwards Compatibility
- Keep existing single-order methods
- Batch methods should be additions, not replacements
- Allow gradual migration

## Success Metrics

1. **Performance**: Order placement time reduced from 9s to <1s
2. **Reliability**: No increase in error rates
3. **Maintainability**: Clean, well-tested implementation
4. **Documentation**: Clear examples and migration guide

## Conclusion

The infrastructure for batch operations already exists in the Hyperliquid API models. We just need to implement methods that utilize these capabilities. This refactor will bring our implementation in line with Hyperliquid's high-performance architecture and provide a much better developer experience.

### Next Steps
1. Review this proposal with the team
2. Create feature branch `feature/hl-batch-operations`
3. Implement in phases as outlined above
4. Benchmark performance improvements
5. Deploy to production with feature flag
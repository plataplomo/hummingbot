# Hyperliquid Batch Operations Refactor - Updated Plan

## Executive Summary

Based on comprehensive code analysis, Hyperliquid's API models **already fully support** batch operations. The infrastructure is in place but unused. By implementing batch methods in the service layer, we can reduce 6 API calls to 1, achieving **6x reduction in network overhead** and reducing order placement time from 9 seconds to potentially <1 second.

## Current State Analysis (Updated)

### ✅ Infrastructure Already Supports Batches

1. **Request Models Ready**:
   ```python
   # In hl_raw_api_request_payloads.py
   class HyperliquidApiPlaceOrderRequest(HyperliquidBaseRequestPayload):
       orders: list[HyperliquidRawOrderItemSpec]  # Already accepts multiple orders!

   class HyperliquidApiCancelOrderRequest(HyperliquidBaseRequestPayload):
       cancels: list[HyperliquidRawCancelItem]  # Already accepts multiple cancels!
   ```

2. **Response Models Ready**:
   ```python
   # In hl_raw_exchange_response.py
   class HyperliquidRawExchangeResponseData(BaseModel):
       statuses: list[str | HyperliquidRawExchangeStatusObject]  # Handles multiple statuses!
   ```

3. **Service Layer Limitation**:
   - `HyperliquidRequestBuilder.build_place_order_payload()` creates a single-item list
   - `HyperliquidTradingService._process_place_order_response()` only processes `statuses[0]`
   - No batch-specific methods exist in the public API

### 🔴 Current Performance Issue

The test `test_create_six_buy_limit_orders_with_random_symbols` shows:
- 6 separate API calls with individual timing logs
- ~1.5 seconds per order placement
- Total time: ~9 seconds for 6 orders
- Each call creates a new EIP-712 signature

## Architecture-Compliant Implementation Plan

### Phase 1: Add Batch Methods to Trading Service (4 hours)

**File**: `cyberdelta/apis/hyperliquid/services/hl_trading_service.py`

```python
async def place_orders(self, orders: list[PlaceOrderArgs]) -> list[Order]:
    """Place multiple orders in a single batch request.

    Benefits:
    - Single HTTP request for all orders
    - Single EIP-712 signature
    - Atomic operation per exchange rules
    - Massive performance improvement

    Args:
        orders: List of order placement arguments

    Returns:
        List of placed Order objects

    Raises:
        APIError: If validation fails or API request fails
    """
    # Validate all orders first
    for args in orders:
        self._validate_place_order_params(args, "place_orders")

    # Get asset indices for all symbols
    asset_indices = []
    for args in orders:
        asset_index = await self._get_asset_index_callable(args.symbol)
        if asset_index is None:
            raise APIError(
                f"Asset index for {args.symbol} not found",
                code=APIErrorCode.VALIDATION_ERROR.value
            )
        asset_indices.append(asset_index)

    # Build batch payload using existing infrastructure
    order_specs = []
    for args, asset_index in zip(orders, asset_indices):
        order_spec = self._request_builder._build_order_item_spec(args, asset_index)
        order_specs.append(order_spec)

    batch_payload = HyperliquidApiPlaceOrderRequest(
        type="order",
        orders=order_specs,  # Multiple orders!
        grouping="na",
        broker=None
    )

    # Execute single request
    raw_response, http_status = await self._place_order_raw(batch_payload)

    # Process all statuses
    return self._process_batch_place_order_response(
        raw_response, http_status, orders
    )

def _process_batch_place_order_response(
    self,
    raw_response: HyperliquidRawExchangeResponse,
    http_status: int,
    original_orders: list[PlaceOrderArgs]
) -> list[Order]:
    """Process batch order placement response."""
    if not raw_response.response or not raw_response.response.data:
        raise APIError("Invalid response structure")

    response_data = raw_response.response.data
    if len(response_data.statuses) != len(original_orders):
        raise APIError(
            f"Response status count ({len(response_data.statuses)}) "
            f"doesn't match request count ({len(original_orders)})"
        )

    placed_orders = []
    for i, (status, args) in enumerate(zip(response_data.statuses, original_orders)):
        try:
            order = self._process_single_order_status(status, args, i)
            placed_orders.append(order)
        except APIError as e:
            # Re-raise with context about which order failed
            raise APIError(
                f"Order {i+1}/{len(original_orders)} failed: {e}",
                code=e.code,
                original_exception=e
            ) from e

    return placed_orders
```

### Phase 2: Add Batch Cancellation Methods (2 hours)

```python
async def cancel_orders(
    self,
    cancel_args: list[CancelOrderArgs]
) -> list[CancelOrderResult]:
    """Cancel multiple orders in a single batch request."""
    # Validate arguments
    for args in cancel_args:
        self._validate_cancel_order_params(args, "cancel_orders")

    # Build batch cancel payload
    cancel_items = []
    for args in cancel_args:
        asset_index = await self._get_asset_index_callable(args.symbol)
        if asset_index is None:
            raise APIError(f"Asset index for {args.symbol} not found")

        cancel_items.append(
            HyperliquidRawCancelItem(
                a=asset_index,
                o=int(args.order_id)
            )
        )

    batch_payload = HyperliquidApiCancelOrderRequest(
        type="cancel",
        cancels=cancel_items
    )

    # Execute request
    raw_response, http_status = await self._cancel_order_raw(batch_payload)

    # Process responses
    return self._process_batch_cancel_response(
        raw_response, http_status, cancel_args
    )
```

### Phase 3: Add Public API Methods (2 hours)

**File**: `cyberdelta/apis/hyperliquid/hl_api.py`

```python
async def place_orders(self, orders: list[PlaceOrderArgs]) -> list[Order]:
    """Place multiple orders in a single batch request.

    This method provides significant performance improvements over placing
    orders individually by batching them into a single API request.

    Args:
        orders: List of order placement arguments

    Returns:
        List of placed Order objects

    Raises:
        APIError: If any order fails validation or the API request fails
    """
    return await self.trading_service.place_orders(orders)

async def cancel_orders(
    self,
    cancel_args: list[CancelOrderArgs]
) -> list[CancelOrderResult]:
    """Cancel multiple orders in a single batch request.

    Args:
        cancel_args: List of cancellation arguments

    Returns:
        List of CancelOrderResult objects indicating success/failure
    """
    return await self.trading_service.cancel_orders(cancel_args)
```

### Phase 4: Update Base Class Interface (1 hour)

**File**: `cyberdelta/apis/base/exchange_api.py`

Add these abstract methods to support batch operations across all exchanges:

```python
@abstractmethod
async def place_orders(self, orders: list[PlaceOrderArgs]) -> list[Order]:
    """Place multiple orders in a single batch request.

    Args:
        orders: List of order placement arguments

    Returns:
        List of placed Order objects

    Note:
        Not all exchanges support batch operations. Implementations
        should fall back to sequential placement if needed.
    """
    raise NotImplementedError

@abstractmethod
async def cancel_orders(
    self,
    cancel_args: list[CancelOrderArgs]
) -> list[CancelOrderResult]:
    """Cancel multiple orders in a single batch request."""
    raise NotImplementedError
```

### Phase 5: Implement Comprehensive Tests (3 hours)

**File**: `tests/integration/apis/hyperliquid/perp/orders/test_hl_perp_batch_operations.py`

```python
class TestHyperliquidBatchOperations:
    """Test batch order placement and cancellation operations."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_batch_orders_success(self, hl_api):
        """Test successful batch order placement."""
        # Prepare 6 orders with different prices
        orders = []
        for i in range(6):
            orders.append(PlaceOrderArgs(
                symbol="ETH-USD",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.01"),
                price=Decimal("3000") - (Decimal("10") * i),
                time_in_force=TimeInForce.GTC,
                post_only=True
            ))

        # Place all orders in one batch
        start_time = time.time()
        placed_orders = await hl_api.place_orders(orders)
        elapsed = time.time() - start_time

        # Validate results
        assert len(placed_orders) == 6
        assert elapsed < 1.0  # Should be much faster than 9 seconds

        # Validate each order
        for order in placed_orders:
            assert isinstance(order, Order)
            assert order.exchange_order_id is not None
            assert order.status in [OrderStatus.OPEN, OrderStatus.NEW]

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_batch_orders_partial_failure(self, hl_api):
        """Test batch placement with some orders failing."""
        # Mix valid and invalid orders
        orders = [
            PlaceOrderArgs(...),  # Valid order
            PlaceOrderArgs(quantity=Decimal("999999")),  # Too large
            PlaceOrderArgs(...),  # Valid order
        ]

        with pytest.raises(APIError) as exc_info:
            await hl_api.place_orders(orders)

        assert "Order 2/3 failed" in str(exc_info.value)
```

### Phase 6: Performance Benchmark Tests (2 hours)

```python
@pytest.mark.benchmark
async def test_batch_vs_sequential_performance(self, hl_api):
    """Compare batch vs sequential order placement performance."""
    orders = [create_test_order(i) for i in range(6)]

    # Sequential placement
    sequential_start = time.time()
    sequential_results = []
    for order in orders:
        result = await hl_api.place_order(order)
        sequential_results.append(result)
    sequential_time = time.time() - sequential_start

    # Cancel all for clean state
    await hl_api.cancel_all_orders()

    # Batch placement
    batch_start = time.time()
    batch_results = await hl_api.place_orders(orders)
    batch_time = time.time() - batch_start

    # Validate improvement
    assert batch_time < sequential_time / 3  # At least 3x faster
    logger.info(
        f"Performance improvement: {sequential_time:.2f}s -> {batch_time:.2f}s "
        f"({sequential_time/batch_time:.1f}x faster)"
    )
```

## Implementation Guidelines

### 1. Follow Project Rules
- Use `.venv/bin/` for all tools
- Run static analysis after each change
- No `# type: ignore` or `# noqa`
- Use `Decimal` for all financial values
- Comprehensive error handling

### 2. Maintain Architecture Boundaries
- Raw models stay in `models/` directory
- Service logic in `services/` directory
- Public API methods in `hl_api.py`
- No cross-boundary imports

### 3. Error Handling Strategy
```python
# Handle partial batch failures gracefully
try:
    results = await place_orders(orders)
except APIError as e:
    if "partial" in str(e):
        # Extract successful orders from error context
        successful = e.context.get("successful_orders", [])
        failed = e.context.get("failed_orders", [])
        # Handle appropriately
```

### 4. Backwards Compatibility
- Keep existing single-order methods
- Batch methods are additions, not replacements
- Document migration path for users

## Risk Mitigation

### 1. Batch Size Limits
- Test maximum batch size (likely 10-50 orders)
- Implement automatic chunking if needed
- Document limits clearly

### 2. Atomicity Considerations
- Hyperliquid may process orders independently
- Document partial success scenarios
- Provide clear error messages

### 3. Rate Limiting
- Batch operations may have different weights
- Update rate limit strategy if needed
- Monitor for new rate limit headers

## Success Metrics

1. **Performance**: 6 orders placed in <1 second (vs 9 seconds)
2. **Reliability**: No increase in error rates
3. **Code Quality**: All static analysis passing
4. **Test Coverage**: 100% coverage of new methods
5. **Documentation**: Clear migration examples

## Next Steps

1. Create feature branch: `feature/hl-batch-operations`
2. Implement Phase 1 (Trading Service methods)
3. Add unit tests for new methods
4. Implement Phase 2-3 (Public API)
5. Add integration tests
6. Performance benchmarking
7. Documentation updates
8. Code review and merge

## Conclusion

The Hyperliquid API infrastructure is already designed for batch operations. We just need to implement the service layer methods to utilize this capability. This refactor will bring massive performance improvements with minimal risk, as we're using existing, tested infrastructure in the way it was designed to be used.

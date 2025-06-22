# Hyperliquid Batch Operations Implementation Todo List

Based on the updated refactor plan, here's a comprehensive todo list for implementing Hyperliquid batch operations with clear batch-named methods:

## 📋 Hyperliquid Batch Operations Refactor Todo List

### Phase 1: Trading Service Implementation (4 hours)
- [ ] In `hl_trading_service.py`, add `place_batch_orders()` method for batch order placement
- [ ] Add `_process_batch_place_order_response()` to handle multiple order statuses
- [ ] Add `_process_single_order_status_from_batch()` helper method
- [ ] Add `cancel_batch_orders()` method for batch cancellation
- [ ] Add `_process_batch_cancel_response()` method
- [ ] Run static analysis: `.venv/bin/ruff`, `.venv/bin/mypy`, `.venv/bin/pyright` on modified files
- [ ] Write unit tests for new batch service methods

### Phase 2: Request Builder Updates (2 hours)
- [ ] Extract `_build_order_item_spec()` as a reusable method in `hl_request_builder.py`
- [ ] Add `build_batch_place_order_payload()` method
- [ ] Add `build_batch_cancel_order_payload()` method
- [ ] Ensure decimal-to-string conversions work for batch operations
- [ ] Run static analysis on changes

### Phase 3: Public API Methods (2 hours)
- [ ] Add `place_batch_orders()` method to `HyperliquidAPI` class
- [ ] Add `cancel_batch_orders()` method to `HyperliquidAPI` class
- [ ] Update method signatures with proper type hints
- [ ] Add comprehensive docstrings explaining batch behavior
- [ ] Run static analysis on API changes

### Phase 4: Base Class Updates (1 hour)
- [ ] Add abstract `place_batch_orders()` method to `ExchangeAPI` base class
- [ ] Add abstract `cancel_batch_orders()` method to `ExchangeAPI` base class
- [ ] Document that implementations can fall back to sequential if batch not supported
- [ ] Update Backpack API with stub batch implementations (raise NotImplementedError)

### Phase 5: Integration Tests (3 hours)
- [ ] Create `test_hl_perp_batch_operations.py` test file
- [ ] Write `test_place_batch_orders_success()` - happy path with 6 orders
- [ ] Write `test_place_batch_orders_partial_failure()` - mixed success/failure
- [ ] Write `test_cancel_batch_orders_success()`
- [ ] Write `test_cancel_batch_orders_partial_failure()`
- [ ] Write `test_batch_size_limits()` - test maximum batch size
- [ ] Add VCR cassettes for all new batch tests

### Phase 6: Performance Tests (2 hours)
- [ ] Write `test_batch_vs_sequential_performance()` benchmark
- [ ] Add timing assertions (batch should be <1s vs 9s sequential)
- [ ] Create performance comparison report
- [ ] Document batch performance improvements in code

### Phase 7: Error Handling & Edge Cases (2 hours)
- [ ] Implement batch size validation (discover and enforce limits)
- [ ] Handle partial batch success scenarios gracefully
- [ ] Add proper error context for failed orders in batch
- [ ] Test empty batch handling
- [ ] Test duplicate orders in batch
- [ ] Ensure client_order_id uniqueness across batch

### Phase 8: Documentation (1 hour)
- [ ] Update API documentation with batch methods
- [ ] Add migration guide: "Moving from single orders to batch operations"
- [ ] Document batch size limits and constraints
- [ ] Add batch code examples for common use cases
- [ ] Update relevant markdown files with batch performance benefits

### Phase 9: Quality Assurance (1 hour)
- [ ] Run full test suite: `.venv/bin/pytest tests/`
- [ ] Ensure all static analysis passes: `.venv/bin/ruff`, `.venv/bin/mypy`, `.venv/bin/pyright`
- [ ] Check test coverage for new batch methods (aim for 100%)
- [ ] Review changes for architecture boundary compliance
- [ ] Verify no cross-boundary imports between apis/ and core/

### Phase 10: Optimization (Optional, 2 hours)
- [ ] Remove unnecessary order re-fetching after batch placement
- [ ] Optimize batch response processing to avoid redundant operations
- [ ] Consider implementing automatic batch chunking for large requests
- [ ] Add batch operation metrics/logging
- [ ] Create `_construct_order_from_batch_placement()` to avoid re-fetching

### Success Criteria Checklist
- [ ] ✅ 6 orders placed in <1 second using batch (vs current 9 seconds)
- [ ] ✅ All static analysis passing (ruff, mypy, pyright)
- [ ] ✅ 100% test coverage on new batch methods
- [ ] ✅ No architecture boundary violations
- [ ] ✅ Comprehensive batch error handling with clear messages
- [ ] ✅ Backwards compatibility maintained (single order methods still work)
- [ ] ✅ Batch documentation complete and clear

### Implementation Notes:
- Always use `.venv/bin/` prefix for all tools
- No `# type: ignore` or `# noqa` comments allowed
- Use `Decimal` for all financial values
- Follow existing code patterns and conventions
- Test batch operations with real market data using test helpers
- Maintain strict separation between raw API models and internal models
- Batch methods should be clearly named with "batch" in the method name

### Time Estimates:
- Phase 1: 4 hours
- Phase 2: 2 hours
- Phase 3: 2 hours
- Phase 4: 1 hour
- Phase 5: 3 hours
- Phase 6: 2 hours
- Phase 7: 2 hours
- Phase 8: 1 hour
- Phase 9: 1 hour
- Phase 10: 2 hours (optional)

**Total estimated time: ~20 hours**

### Priority Order:
1. Start with Phase 1 (Trading Service) as it's the core implementation
2. Then Phase 3 (Public API) to expose the functionality
3. Follow with Phase 5 (Integration Tests) to validate
4. Complete remaining phases based on project needs
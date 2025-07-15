# Disabled Tests

## test_synchronized_order_submission.py.disabled

**Reason:** This test file contains multiple tests that mock private methods (_compensate_verification_failure, _execute_sequential_with_verification), which violates the requirement that tests can only use public APIs.

**Required Action:** The following tests need to be rewritten to test through public behavior only:

1. `test_submit_orders_pre_execution_failure` - mocks `_compensate_verification_failure`
2. `test_submit_orders_post_execution_failure` - mocks `_compensate_verification_failure`
3. Several other tests that mock `_execute_sequential_with_verification`

**Refactoring Approach:**
- Remove all `@patch.object` decorators that target private methods (those starting with underscore)
- Rewrite tests to only call public methods of `SynchronizedOrderSubmissionService`
- Test the behavior through the public `submit_orders()` method with different scenarios
- If private method testing is absolutely necessary, consider making those methods public or testing them indirectly through their effects on public behavior

**Public API Methods Available for Testing:**
- `submit_orders()`
- `verify_pre_execution()`
- `verify_post_execution()`
- `reconcile_positions()`
- `handle_position_discrepancy()`

The tests should be rewritten to simulate failure scenarios through configuration or mock external dependencies rather than mocking internal implementation methods.

# Breaking Changes - Test Migration Notes

## Removed Test Files

The following test files have been removed or modified due to the breaking changes approach:

### Performance Comparison Tests
- `test_performance_benchmarks.py` - Legacy comparison tests removed
- `test_simple_performance.py` - Legacy wrapper tests removed  
- `test_validation_comparison.py` - Legacy vs unified comparison tests removed

These tests were designed to compare legacy validators with the unified validation service. Since we've adopted a breaking changes approach and completely removed legacy validators, these comparison tests are no longer applicable.

## Remaining Tests

The following tests remain valid and continue to provide coverage:

- `test_unified_validation.py` - Integration tests for unified validation service
- `test_property_based_validation.py` - Property-based tests using Hypothesis
- `rules/test_*.py` - Unit tests for individual validation rules

## Testing Strategy

With the breaking changes approach, testing focuses on:

1. **Unit Testing**: Individual validation rules are thoroughly tested
2. **Integration Testing**: Unified validation service integration tests
3. **Property-Based Testing**: Hypothesis-generated test cases for edge conditions
4. **Performance Testing**: Unified service performance benchmarks (without legacy comparison)

## Future Test Development

New tests should focus on:

- Testing new validation rules as they are added
- Performance regression testing for the unified service
- Integration testing with the new API parameters
- Error handling and edge case validation
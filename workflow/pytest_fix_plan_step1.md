# Step 1: Standardize Mock Assertions & Error Message Checks

## Goal
Ensure mock call assertions are accurate and error message checks are robust to detailed Pydantic validation outputs.

## Specific Actions

### 1.1 Mock Call Assertion Fixes
**Problem**: Tests use `assert_called_with` expecting positional args when code uses keyword args.

**Files to Review**:
- `tests/unit/apis/backpack/test_bp_api.py`
- `tests/unit/apis/backpack/test_bp_response_handler.py` 
- `tests/unit/apis/hyperliquid/test_hl_api.py`
- `tests/unit/apis/hyperliquid/services/test_hl_market_data_service.py`

**Fix Pattern**:
```python
# BEFORE (Incorrect)
mock_method.assert_called_with("value1", "value2")

# AFTER (Correct)
mock_method.assert_called_with(param1="value1", param2="value2")
```

### 1.2 Error Message Assertion Refinement
**Problem**: Tests expect exact error message matches but get detailed Pydantic ValidationError messages.

**Fix Pattern**:
```python
# BEFORE (Brittle)
assert exc_info.value.message == "Invalid ticker response"

# AFTER (Robust)
assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
assert "ticker" in exc_info.value.message.lower()
assert isinstance(exc_info.value.original_exception, ValidationError)
```

### 1.3 APIError Content Verification
**Enhancement**: Check both APIError structure and wrapped exception details.

**Pattern**:
```python
# Comprehensive APIError checking
with pytest.raises(APIError) as exc_info:
    method_under_test()

error = exc_info.value
assert error.code == APIErrorCode.INVALID_RESPONSE.value
assert "expected context" in error.message
assert isinstance(error.original_exception, (ValidationError, ValueError))
assert "specific field" in str(error.original_exception)
```

## Implementation Priority
1. Fix mock assertion mismatches (high impact, low risk)
2. Refine error message checks (medium impact, low risk)
3. Enhance APIError verification patterns (low impact, high value)

## Validation
- All mock-related test failures should resolve
- Error message assertion failures should resolve
- No new test failures introduced 
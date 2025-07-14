# Symbol Not Found Error Handling Analysis

## Issue Summary

When sending invalid symbols (e.g., `INVALID_SYMBOL_XYZ`) to Hyperliquid API, our system returns error code 6 (`INVALID_RESPONSE`) instead of the expected error codes 105 (`INVALID_SYMBOL`) or 106 (`SYMBOL_NOT_FOUND`). This indicates a design issue in our error handling pipeline.

## Root Cause Analysis

### What Should Happen
1. **Invalid Symbol Request**: Test sends `INVALID_SYMBOL_XYZ` to Hyperliquid
2. **Symbol Lookup**: System checks if symbol exists in available symbols
3. **Symbol Not Found**: Raises `SymbolNotFoundError` with code 106 (`SYMBOL_NOT_FOUND`)

### What Actually Happens
1. **Invalid Symbol Request**: Test sends `INVALID_SYMBOL_XYZ` to Hyperliquid
2. **Hyperliquid Response**: Returns HTTP 200 OK with response (likely empty or malformed)
3. **Pydantic Validation**: Response handler tries to validate against `HyperliquidRawMetaAndAssetCtxsResponse` model
4. **Validation Fails**: Pydantic raises `ValidationError` because response doesn't match expected structure
5. **Error Mapping**: Asset indexer catches `ValidationError` and maps to `INVALID_RESPONSE` (code 6)
6. **Never Reaches Symbol Lookup**: Since validation fails, we never get to proper symbol analysis

## Technical Details

### Error Code Mapping
- `INVALID_RESPONSE` code: **6** (what we're getting)
- `SYMBOL_NOT_FOUND` code: **106** (what we should get)
- `INVALID_SYMBOL` code: **105** (alternative expected code)

### Code Locations

**Asset Indexer Validation Error Handling** (`cyberdelta/apis/hyperliquid/hl_asset_indexer.py:218-233`):
```python
except ValidationError as e_val:
    # This is where INVALID_RESPONSE gets set
    raise APIError(
        parse_failed_msg,
        code=APIErrorCode.INVALID_RESPONSE.value,  # Code 6
        original_exception=e_val,
        http_status=status_code,
    ) from e_val
```

**Correct Symbol Not Found Error** (`cyberdelta/apis/exceptions/market_data_service.py:242`):
```python
class SymbolNotFoundError(MarketDataServiceError):
    def __init__(self, symbol: str, ...):
        super().__init__(
            message=message,
            code=APIErrorCode.SYMBOL_NOT_FOUND.value,  # Code 106 - this should be used
            # ...
        )
```

**Order Placement Symbol Check** (`cyberdelta/apis/hyperliquid/services/trading/hl_order_placement_service.py:230-232`):
```python
asset_index = await self._get_asset_index_callable(order_args.symbol)
if asset_index is None:
    raise SymbolNotFoundError(symbol=order_args.symbol, exchange="Hyperliquid")
```

## The Problem

Our error handling is **too early in the pipeline**. We're catching Pydantic validation errors before we can properly analyze whether the issue is:
- An invalid symbol (should be `SYMBOL_NOT_FOUND` or `INVALID_SYMBOL`) 
- A truly malformed response (should be `INVALID_RESPONSE`)

## Impact

1. **Incorrect Error Classification**: Invalid symbols are misclassified as response validation errors
2. **Poor User Experience**: Generic "invalid response" error instead of clear "symbol not found" message
3. **Broken Error Handling Logic**: Consumer code expecting symbol-specific errors gets generic errors
4. **Test Confusion**: Tests need to accept wrong error codes, masking the real issue

## Proposed Solution

Fix the asset indexer to handle symbol validation more gracefully:

### Option 1: Two-Stage Validation
1. **Allow validation to succeed** even with empty/missing symbols in response
2. **Check the validated response** to see if requested symbol is present
3. **Raise `SymbolNotFoundError`** (code 106) when symbol is not found  
4. **Only use `INVALID_RESPONSE`** for actual response structure validation failures

### Option 2: Pre-validate Symbols
1. **Fetch all available symbols** before making specific symbol requests
2. **Check if requested symbol exists** in available symbols list
3. **Raise `SymbolNotFoundError`** immediately if symbol not in list
4. **Proceed with request** only for valid symbols

## Files That Need Changes

1. **`cyberdelta/apis/hyperliquid/hl_asset_indexer.py`** - Primary fix location
2. **Response handler models** - May need to allow more flexible validation
3. **Tests** - Remove workarounds that accept `INVALID_RESPONSE` for invalid symbols

## Current Workaround in Tests

Tests have been updated to accept `INVALID_RESPONSE` for invalid symbols:
```python
# Should be removed once proper fix is implemented
APIErrorCode.INVALID_RESPONSE.value,  # Empty response for invalid symbol
```

## Conclusion

**This IS a bug in our error handling logic**. The error code 6 (`INVALID_RESPONSE`) is masking the real issue (symbol not found) with a generic validation error. We should fix the asset indexer logic rather than updating tests to accept incorrect error codes.

The current behavior makes it impossible for consumer code to distinguish between:
- "Symbol doesn't exist" (should be 106)
- "API response is malformed" (should be 6)

Both scenarios currently return code 6, which is incorrect.
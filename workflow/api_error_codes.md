# API Error Code Handling in CyberDeltaEngine

## Overview

This document captures the rationale, design, and best practices for handling API error codes in CyberDeltaEngine, focusing on a superset abstraction (`APIErrorCode`) that unifies error handling across multiple exchanges (e.g., Backpack, Hyperliquid). It is intended as a lasting reference for maintainers and contributors.

---

## Why a Superset Error Code Enum?

- **Consistency:** Enables uniform error handling in business logic, regardless of the source exchange.
- **Extensibility:** New exchanges or error types can be integrated by extending mapping logic, not core business logic.
- **Validation:** Supports Pydantic-first validation and transformation of error responses into a type-safe, internal model.
- **Testing:** Allows comprehensive, cross-exchange error handling tests at the abstraction layer.

---

## Design Principles

- **APIErrorCode** is the canonical set of error codes for the engine. It should be:
  - **Comprehensive** for all actionable error states encountered across supported exchanges.
  - **Stable**: Only add new codes for distinct, actionable error states.
  - **Well-documented**: Each code should have a clear, unambiguous meaning.
- **Exchange-Specific Mapping**: Each exchange (e.g., Backpack, Hyperliquid) has a dedicated error mapping function/class that:
  - Validates the raw error response using a Pydantic model.
  - Maps the exchange-specific error code/message to an `APIErrorCode`.
  - Attaches the original error message/code for diagnostics.
- **Fallbacks**: Always provide a fallback (`UNKNOWN`, `EXCHANGE_SPECIFIC`, or `ERROR`) for unmapped or ambiguous errors. Log the original error for diagnostics.

---

## Example: Error Mapping and Validation Flow

```python
# Pydantic validation
try:
    if exchange == "backpack":
        error_obj = BackpackRawApiError.model_validate(error_data)
        code = map_backpack_error_code(error_obj)
    elif exchange == "hyperliquid":
        error_obj = HyperliquidRawApiError.model_validate(error_data)
        code = map_hyperliquid_error_code(error_obj)
    else:
        code = APIErrorCode.UNKNOWN
except ValidationError:
    code = APIErrorCode.INVALID_REQUEST

# Centralized error object
api_error = APIError(
    code=code,
    message=error_obj.msg if hasattr(error_obj, 'msg') else error_obj.error,
    exchange_code=getattr(error_obj, 'code', None),
    exchange_message=getattr(error_obj, 'msg', None),
    http_status=status_code,
)
```

---

## Pros & Cons

### Pros
- **Uniformity:** Business logic and user-facing error handling are consistent and type-safe.
- **Maintainability:** Adding new exchanges or error types is straightforward.
- **Validation:** Strict Pydantic models ensure only well-formed errors are processed.
- **Testing:** Centralized error handling is easier to test and audit.

### Cons
- **Loss of Specificity:** Some exchange-specific errors may not map cleanly to a generic code.
- **Maintenance Overhead:** Mapping logic must be kept up to date as exchanges evolve.
- **Ambiguity:** Free-form error messages (e.g., Hyperliquid) require substring or regex matching, which is less robust than code-based mapping.

---

## Best Practices

- **Extend `APIErrorCode` only for actionable, cross-exchange error states.**
- **Document all mapping logic and update as new error types are discovered.**
- **Use Pydantic models for strict validation of all error responses.**
- **Log all unmapped or ambiguous errors for future refinement.**
- **Write tests for all known error mappings and fallback scenarios.**

---

## References
- [Hyperliquid Exchange Endpoint Docs](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint)
- [Hyperliquid Error Responses](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses)
- [Hyperliquid Python SDK](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/tree/master)
- [CCXT Hyperliquid Integration](https://docs.ccxt.com/#/exchanges/hyperliquid)
- [CyberDeltaEngine Pydantic Refactor Plan](model_alignment_and_pydantic_refactor_plan.md)

---

## Summary

A superset error code abstraction, validated and mapped via Pydantic models, is the recommended approach for robust, maintainable, and extensible error handling in CyberDeltaEngine. This enables consistent business logic, simplifies testing, and supports future growth as new exchanges and error types are integrated. 
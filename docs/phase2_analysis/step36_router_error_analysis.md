# Step 36: Router Error Analysis - Current Error Handling Patterns

## Overview

This document analyzes all current error handling patterns in `ws_router.py` to identify areas requiring migration to the new typed WebSocket error system. The analysis focuses on identifying `dict[str, Any]` conversions and APIError dependencies that need to be replaced with typed error handling.

## Current Router Architecture

The `BaseWebSocketRouter` is a generic abstract base class that provides:
- Type-safe message processing with `TypeSafeWebSocketProcessor`
- Centralized error handling with `BaseErrorHandler`
- Exchange-agnostic routing abstractions
- Memory optimization for high-frequency scenarios
- Error recovery system integration

## Error Handling Patterns Found

### 1. **Dict-Based Error Context Creation** ❌

**Location**: `_handle_missing_processor` method (lines 344-359)

```python
async def _handle_missing_processor(
    self,
    routing_key: str,
    payload: dict[str, Any] | list[Any],
    context: WebSocketContextProtocol,
) -> None:
    # Ensure payload is dict for error handler
    payload_dict = payload if isinstance(payload, dict) else {"data": payload}
    # Convert typed context to dict for error handler (temporary until error handler is updated)
    context_dict = context.model_dump(mode="python")  # ❌ DICT CONVERSION!
    await self.error_handler.handle_processing_error(
        error=ValueError(f"No processor found for routing key: {routing_key}"),
        payload=payload_dict,
        context=context_dict,  # ❌ PASSING DICT TO ERROR HANDLER
    )
```

**Issues**:
- Converts typed `WebSocketContextProtocol` to `dict[str, Any]`
- Uses legacy error handler interface expecting dict context
- Loses type safety in error context

### 2. **Generic Error Context Building** ❌

**Location**: `_handle_envelope_validation_error` method (lines 298-313)

```python
async def _handle_envelope_validation_error(
    self,
    error: Exception,
    message: dict[str, Any],
) -> None:
    await self.error_handler.handle_unroutable_message(
        message=message,
        reason=f"Invalid message envelope format: {error}",
        context={  # ❌ MANUAL DICT CONSTRUCTION
            "exchange": self.exchange_name,
            "validation_error": str(error),
            "error_type": type(error).__name__,
            "message_keys": list(message.keys()),
        },
    )
```

**Issues**:
- Manually constructs dict-based error context
- No standardized context creation pattern
- Missing routing key, connection ID, and other context data

### 3. **Missing Routing Key Error Handling** ❌

**Location**: `_handle_missing_routing_key` method (lines 315-328)

```python
async def _handle_missing_routing_key(
    self,
    message: dict[str, Any],
    envelope: EnvelopeType,
) -> None:
    await self.error_handler.handle_unroutable_message(
        message=message,
        reason="Unable to extract routing key from validated envelope",
        context={  # ❌ MANUAL DICT CONSTRUCTION
            "exchange": self.exchange_name,
            "envelope_type": type(envelope).__name__,
        },
    )
```

**Issues**:
- Limited context information in manually built dict
- No connection ID, sequence numbers, or routing details
- Inconsistent error context structure

### 4. **APIError Dependency in Error Recovery** ❌

**Location**: `route_message` method (lines 383-398)

```python
# Create structured WebSocketError for better error handling
websocket_error = WebSocketErrorRecovery.create_websocket_error(
    message=f"WebSocket routing error: {e!s}",
    error_code=APIErrorCode.NETWORK_ISSUE,  # ❌ APIError CODE DEPENDENCY
    original_exception=e,
    metadata={
        "exchange": self.exchange_name,
        "message_keys": list(message.keys()),
        "error_type": type(e).__name__,
    },
)
```

**Issues**:
- Uses `APIErrorCode.NETWORK_ISSUE` instead of WebSocket error codes
- Recovery system still depends on APIError architecture
- Metadata is dict-based rather than typed

### 5. **Missing Handler Warning (Low Priority)** ⚠️

**Location**: `_handle_missing_handler` method (lines 330-342)

```python
async def _handle_missing_handler(
    self,
    message: dict[str, Any],
    routing_key: str,
    handlers: dict[str, MessageHandler],
) -> None:
    self.logger.warning(
        "no_handler_for_routing_key",
        exchange=self.exchange_name,
        routing_key=routing_key,
        available_handlers=list(handlers.keys()),
    )
```

**Issues**:
- Only logs warning, doesn't integrate with error handling system
- Could benefit from typed error reporting for monitoring

## Migration Requirements

### High Priority Issues (Must Fix)

1. **Replace `context.model_dump()` with Typed Context Passing**
   - Update `_handle_missing_processor` to use typed error handler interface
   - Eliminate dict conversion of WebSocketContextProtocol

2. **Create Router Error Context Builder**
   - Standardized way to create `StreamErrorContext` from router state
   - Include routing key, exchange, connection ID, envelope type
   - Replace manual dict construction in error methods

3. **Update Error Recovery Integration**
   - Replace `APIErrorCode` with `WebSocketErrorCode`
   - Use typed `WebSocketStreamError` instead of generic WebSocketError
   - Update recovery system to use typed error context

4. **Create Router Error Bridge**
   - Bridge between router and typed error system
   - Similar to ProcessorErrorBridge pattern
   - Handle routing errors, missing processors, validation failures

### Medium Priority Issues

5. **Enhance Error Context Information**
   - Include more contextual information in error contexts
   - Add sequence numbers, timestamps, message IDs
   - Standardize error context structure across all error types

6. **Add Missing Handler Error Integration**
   - Integrate missing handler warnings with error system
   - Create proper error events for monitoring

## Dependencies

### Required Components (From Phase 1)
- ✅ `WebSocketErrorCode` enum with routing-specific codes
- ✅ `StreamErrorContext` model for typed error context
- ✅ `WebSocketStreamError` base class
- ✅ `WebSocketStreamErrorHandler` for typed error handling

### Components to Create (This Phase)
- 🆕 Router Error Context Builder
- 🆕 Router Error Bridge (similar to ProcessorErrorBridge)
- 🆕 Router-specific WebSocket error codes if needed

## Proposed Migration Strategy

### Step 1: Create Router Error Context Builder
- `RouterErrorContextBuilder.from_routing_error()`
- `RouterErrorContextBuilder.from_envelope_validation_error()`
- `RouterErrorContextBuilder.from_missing_processor()`

### Step 2: Create Router Error Bridge
- Bridge router to `WebSocketStreamErrorHandler`
- Handle all router error scenarios with typed system
- Maintain fallback to legacy error handler

### Step 3: Update Error Methods
- Replace dict-based context with typed context
- Use router error bridge for all error scenarios
- Update error recovery integration

### Step 4: Update Configuration
- Add router-specific error handler configuration
- Configure typed error handler for router

### Step 5: Testing
- Unit tests for router error handling
- Integration tests with typed error system
- Performance validation

## Error Code Mapping

Current APIError codes → WebSocket error codes:
- `APIErrorCode.NETWORK_ISSUE` → `WebSocketErrorCode.ROUTING_FAILED`
- Validation errors → `WebSocketErrorCode.MESSAGE_VALIDATION_FAILED`
- Missing processor → `WebSocketErrorCode.PROCESSOR_NOT_FOUND`
- Missing routing key → `WebSocketErrorCode.ROUTING_KEY_MISSING`

## Conclusion

The router has **4 critical dict-based error handling patterns** that need migration to the typed error system. The migration follows a similar pattern to the processor integration:

1. Create router-specific error context builder
2. Create error bridge to integrate with typed error system
3. Update all error handling methods to use typed contexts
4. Test and validate performance

The router integration will eliminate the remaining `dict[str, Any]` patterns in WebSocket error handling and complete the type safety goals for the routing layer.
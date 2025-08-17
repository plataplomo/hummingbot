# Backpack WebSocket Router Architecture

## Overview

This document describes the architecture of the Backpack WebSocket router (`bp_ws_router.py`) and captures architectural ideas explored in previous iterations for future reference.

## Current Implementation: bp_ws_router.py

### Purpose
The BackpackWebSocketRouter provides complete WebSocket message routing and processing for the Backpack exchange, handling all message types with type safety and comprehensive error handling.

### Key Features
- **Complete message type support**: depth, ticker, trades, orders, positions, fills, subscription responses
- **Type-safe processing**: Validated models with Pydantic
- **Rich error contexts**: ProcessorErrorContextBuilder and RouterErrorContextBuilder integration
- **Stateful transformations**: Depth updates use stateful transformer for incremental updates
- **Production tested**: Currently in active use

### Architecture Components

```
┌─────────────────────────────────────────────────────────┐
│                   BackpackWebSocketRouter                │
├─────────────────────────────────────────────────────────┤
│ Dependencies:                                            │
│ - 8 Mappers (order_book, ticker, trade, balance, etc.)  │
│ - WebSocketErrorHandler                                  │
│ - WebSocketContextFactory                                │
│ - Memory optimization settings                           │
├─────────────────────────────────────────────────────────┤
│ Processors:                                              │
│ - depth → BackpackDepthStateTransformer                  │
│ - ticker → WebSocketMapperAdapter                        │
│ - trades → WebSocketMapperAdapter                        │
│ - orders → WebSocketMapperAdapter                        │
│ - positions → WebSocketMapperAdapter                     │
│ - fills → WebSocketMapperAdapter                         │
│ - subscriptionResponse → WebSocketControlMessageAdapter  │
└─────────────────────────────────────────────────────────┘
```

### Message Flow

1. **Message Reception**: Raw WebSocket message arrives
2. **Envelope Validation**: `validate_backpack_envelope()` ensures structure
3. **Routing Key Extraction**: Topic parsed (e.g., "ticker.SOL_USDC" → "ticker")
4. **Context Creation**: Typed context with metadata
5. **Processor Selection**: Based on message type
6. **Transformation**: Via mapper adapters to domain models
7. **Handler Invocation**: Application handlers receive typed models

## Architectural Ideas from V2 Exploration

A proof-of-concept router (V2) was explored to test alternative architectural patterns. While not adopted, these ideas remain valuable for future improvements:

### 1. Specialized Transformer Pattern

**Concept**: Instead of generic adapters, use dedicated transformer classes per data type.

```python
# V2 Exploration: Specialized transformers
class BackpackDepthTransformer:
    """Dedicated transformer for depth updates."""
    def __init__(self, order_book_mapper: BackpackOrderBookMapper):
        self.mapper = order_book_mapper

    def transform(self, validated: BackpackRawDepthUpdateEvent,
                 context: WebSocketContextProtocol) -> OrderBook:
        symbol = self._extract_symbol_from_context(validated, context)
        return self.mapper.transform_ws_depth_event_to_internal(symbol, validated)

class BackpackTickerTransformer:
    """Dedicated transformer for ticker events."""
    def __init__(self, ticker_mapper: BackpackTickerMapper):
        self.ticker_mapper = ticker_mapper

    def transform(self, validated: BackpackRawTickerEvent,
                 context: WebSocketContextProtocol) -> Ticker:
        return self.ticker_mapper.transform_ws_ticker_event_to_internal(validated)
```

**Benefits Explored**:
- More explicit transformation logic
- Better encapsulation of symbol extraction
- Type safety at the class level

**Why Not Adopted**:
- Adds boilerplate without clear benefit
- Generic adapters achieve same type safety with less code
- Would require transformer class for each message type

### 2. Simplified Dependency Injection

**Concept**: Reduce constructor parameters by taking only essential dependencies.

```python
# V2 Exploration: Minimal constructor
def __init__(self,
    stream_error_handler: WebSocketErrorHandler,
    memory_settings: MemorySettings,
    # Only 3 essential mappers instead of 8
    order_book_mapper: BackpackOrderBookMapper,
    ticker_mapper: BackpackTickerMapper,
    trade_mapper: BackpackFillMapper):
```

**Benefits Explored**:
- Cleaner constructor signature
- Appears simpler to initialize

**Why Not Adopted**:
- Hides dependencies rather than eliminating them
- Makes testing harder (can't mock unused mappers)
- All 8 mappers are actually needed for complete functionality

### 3. ProcessorFactory Pattern

**Concept**: Use factory methods for simple processors without transformations.

```python
# V2 Exploration: Factory for simple processors
self.processors["order"] = ProcessorFactory.create_simple_processor(
    raw_model=BackpackRawOrderUpdate,
    stream_error_handler=self.stream_error_handler,
    processor_name="backpack_order",
)
```

**Benefits Explored**:
- Reduces boilerplate for simple cases
- Standardizes processor creation

**Why Not Adopted**:
- Most processors need custom transformations
- Factory pattern adds abstraction without much value
- Current explicit setup is clearer

### 4. Internal Context Factory Creation

**Concept**: Router creates its own context factory internally.

```python
# V2 Exploration: Internal factory creation
builder = BackpackRegistryBuilder()
registry = builder.build_registry()
context_factory = WebSocketContextFactory(registry)
```

**Benefits Explored**:
- Encapsulates registry building
- Reduces external dependencies

**Why Not Adopted**:
- Makes testing harder
- Reduces flexibility
- Context factory often needs external configuration

## Design Decisions and Rationale

### Why We Use WebSocketMapperAdapter

The current implementation uses `WebSocketMapperAdapter` as a generic wrapper around mapper methods:

```python
transformer=WebSocketMapperAdapter[BackpackRawTickerEvent, Ticker](
    mapper_method=self.ticker_mapper.transform_ws_ticker_event_to_internal,
)
```

**Rationale**:
- **Type Safety**: Full generic type checking
- **Reusability**: One adapter class for all mappers
- **Simplicity**: No need for transformer class per type
- **Flexibility**: Easy to swap mapper implementations

### Why We Take All Dependencies Explicitly

The router takes 8 mapper dependencies even though not all are used immediately:

**Rationale**:
- **Testability**: Can mock/stub any mapper for testing
- **Clarity**: All dependencies visible at construction
- **Flexibility**: Easy to add new message types
- **Type Safety**: Compiler validates all dependencies

### Why We Override route_message()

The router overrides the base `route_message()` method for Backpack-specific routing:

**Rationale**:
- **Topic Patterns**: Backpack uses "type.symbol" format
- **Handler Registration**: Supports full topic names
- **Subscription Responses**: Special handling for confirmations
- **Compatibility**: Handles both "trade" and "trades"

## Future Improvement Opportunities

Based on the V2 exploration and current implementation analysis:

### 1. Processor Creation Standardization
Consider a builder pattern for processor setup to reduce repetition while maintaining explicitness.

### 2. Symbol Extraction Enhancement
The symbol extraction logic could be centralized in a dedicated component rather than scattered across transformers.

### 3. Mapper Method References
Instead of string method names, consider using method references or protocols for stronger type checking.

### 4. Configuration Injection
Consider injecting specific configuration objects rather than full AppSettings to follow Interface Segregation Principle.

## Performance Considerations

### Current Optimizations
- **Stateful Transformers**: Depth updates maintain state for efficiency
- **Memory Pools**: Configurable memory optimization
- **Processor Caching**: Processors created once and reused

### Areas for Investigation
- **Lazy Processor Creation**: Create processors only when first needed
- **Message Batching**: Process multiple messages in single handler call
- **Async Transformations**: Parallelize independent transformations

## Testing Strategy

### Unit Testing
- Test each processor independently
- Mock mappers to test transformation logic
- Verify error context creation

### Integration Testing
- Use recorded VCR cassettes for reproducibility
- Test full message flow from raw to domain model
- Verify handler invocation with correct data

### Key Test Scenarios
1. Valid message routing
2. Invalid envelope handling
3. Missing processor handling
4. Transformation errors
5. Handler errors (expected vs unexpected)
6. Subscription response processing

## Conclusion

The current `bp_ws_router.py` implementation represents a mature, production-tested solution that balances:
- **Completeness**: All message types supported
- **Type Safety**: Full typing with Pydantic models
- **Maintainability**: Clear structure and dependencies
- **Performance**: Optimized for production use

While alternative architectural patterns were explored (specialized transformers, simplified DI, factory patterns), the current implementation's explicit, adapter-based approach provides the best balance of clarity, testability, and maintainability for production use.

The ideas explored in V2 remain valuable references for future improvements but don't justify the complexity of a major refactor at this time.

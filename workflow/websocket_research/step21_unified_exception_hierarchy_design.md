# Step 21: Unified Exception Hierarchy Design

## Current State Analysis

### Exception Distribution
- **ws_exceptions.py**: 15 exception classes (main file)
- **ws_envelope.py**: 6 exception classes (envelope validation)
- **ws_validators.py**: 10 exception classes (payload validation)
- **ws_security.py**: 7 exception classes (security validation)
- **Total**: 38 exception classes across 4 files

### Current Base Classes
1. **WebSocketStreamError** - Main base class for stream errors
2. **Exception** - Generic Python exception base
3. **ValueError** - For value-related errors
4. **TypeError** - For type-related errors

## Proposed Unified Hierarchy

### 1. Root Exception Class
```python
WebSocketException
├── Base class for all WebSocket-related exceptions
├── Inherits from: Exception
├── Provides: error_id, timestamp, correlation_id
└── Common interface for all WebSocket errors
```

### 2. Major Category Classes
```python
WebSocketException
├── WebSocketStreamError (existing - for runtime stream errors)
│   ├── Inherits error codes, recovery strategies, severity
│   └── Used for operational streaming errors
│
├── WebSocketValidationError (new - for validation errors)
│   ├── Consolidates all validation-related exceptions
│   ├── Includes: payload, envelope, format, type validation
│   └── Provides validation context and details
│
├── WebSocketConnectionError (existing - refactored)
│   ├── Connection establishment and maintenance
│   └── Network-related issues
│
├── WebSocketSecurityError (existing - enhanced)
│   ├── Authentication, authorization, security violations
│   └── Blocked patterns, size limits, validation
│
└── WebSocketConfigurationError (existing - enhanced)
    ├── Configuration and setup errors
    └── Context creation, transformer setup
```

### 3. Detailed Hierarchy Structure

#### A. WebSocketValidationError Branch
```python
WebSocketValidationError
├── PayloadValidationError
│   ├── InvalidPayloadTypeError (from ws_validators.py)
│   ├── PayloadSizeError (from ws_validators.py)
│   ├── PayloadTooLargeError (from ws_envelope.py)
│   ├── PayloadNoneError (from ws_envelope.py)
│   └── MissingRequiredFieldsError (from ws_validators.py)
│
├── EnvelopeValidationError
│   ├── EnvelopeValidationFailedError (from ws_envelope.py)
│   ├── EmptyRoutingKeyError (from ws_envelope.py)
│   ├── InvalidRoutingKeyFormatError (from ws_envelope.py)
│   └── WebSocketMessageFormatError (from ws_exceptions.py)
│
├── FieldValidationError
│   ├── InvalidFieldTypeError (from ws_validators.py)
│   ├── InvalidItemTypeError (from ws_validators.py)
│   ├── UnexpectedFieldsError (from ws_validators.py)
│   ├── WebSocketFieldValidationError (from ws_exceptions.py)
│   └── InvalidFormatError (from ws_validators.py)
│
└── ValueValidationError
    ├── InvalidNumericValueError (from ws_validators.py)
    ├── NumericRangeError (from ws_validators.py)
    ├── InvalidTimestampError (from ws_validators.py)
    ├── WebSocketSequenceValidationError (from ws_exceptions.py)
    └── WebSocketSequenceError (from ws_exceptions.py)
```

#### B. WebSocketSecurityError Branch
```python
WebSocketSecurityError
├── SecurityValidationError (from ws_security.py)
├── BlockedPatternFoundError (from ws_security.py)
├── SizeSecurityError
│   ├── MessageSizeExceedsLimitError (from ws_security.py)
│   ├── MessageSizeValidationFailedError (from ws_security.py)
│   ├── NestingDepthExceedsLimitError (from ws_security.py)
│   ├── ObjectKeysExceedLimitError (from ws_security.py)
│   ├── ArrayLengthExceedsLimitError (from ws_security.py)
│   └── StringLengthExceedsLimitError (from ws_security.py)
└── WebSocketAuthenticationError (from ws_exceptions.py)
```

#### C. WebSocketStreamError Branch (existing, organized)
```python
WebSocketStreamError
├── WebSocketConnectionError (connection issues)
├── WebSocketSubscriptionError
│   ├── WebSocketSubscriptionLimitError
│   └── WebSocketInvalidChannelError
└── WebSocketStreamInterruptedError
```

#### D. WebSocketConfigurationError Branch
```python
WebSocketConfigurationError
├── WebSocketContextCreationError (from ws_exceptions.py)
└── WebSocketTransformerError (from ws_exceptions.py)
```

## Design Principles

### 1. Inheritance Strategy
- **Single inheritance**: Each exception has one clear parent
- **Logical grouping**: Related errors grouped under common ancestors
- **Backward compatibility**: Existing exception names preserved where possible
- **Type safety**: Clear type hierarchy for isinstance() checks

### 2. Common Interface Design
```python
class WebSocketException(Exception):
    """Base class for all WebSocket exceptions."""

    def __init__(
        self,
        message: str,
        error_id: str | None = None,
        correlation_id: str | None = None,
        cause: Exception | None = None
    ):
        # Common error tracking and correlation
        pass

    @property
    def category(self) -> str:
        """Get error category for classification."""

    def to_dict(self) -> dict[str, Any]:
        """Serialize exception for logging/monitoring."""

    def get_troubleshooting_guide(self) -> str:
        """Get user-friendly troubleshooting information."""
```

### 3. Enhanced Features
- **Error correlation**: Common correlation_id across related errors
- **Categorization**: Automatic categorization for monitoring
- **Serialization**: Standard to_dict() for structured logging
- **User guidance**: Built-in troubleshooting information
- **Migration helpers**: Factory methods for backward compatibility

## Migration Strategy

### Phase 1: Create New Base Classes (Step 21)
1. Define WebSocketException base class
2. Create category classes (ValidationError, SecurityError, etc.)
3. Design common interfaces and protocols

### Phase 2: Merge Validation Exceptions (Step 22-24)
1. Consolidate payload validation exceptions
2. Merge envelope validation exceptions
3. Unify field and value validation exceptions

### Phase 3: Create Exception Factory (Step 25)
1. Build factory methods for creating exceptions
2. Add backward compatibility helpers
3. Implement error code integration

### Phase 4: Migrate Scattered Exceptions (Steps 27-29)
1. Move envelope exceptions to main hierarchy
2. Move validator exceptions to main hierarchy
3. Move security exceptions to main hierarchy

### Phase 5: Clean Up (Step 30)
1. Remove old exception definitions
2. Update all imports throughout codebase
3. Run comprehensive tests

## Benefits

### 1. Consistency
- Single source of truth for all WebSocket exceptions
- Consistent error handling patterns
- Unified logging and monitoring

### 2. Maintainability
- Clear hierarchy makes adding new exceptions straightforward
- Reduced code duplication
- Better type safety with isinstance() checks

### 3. Debugging
- Better error categorization for monitoring
- Correlation IDs for tracking related errors
- Built-in troubleshooting guidance

### 4. Backward Compatibility
- Existing exception names preserved
- Factory methods for easy migration
- Gradual migration path

## Implementation Notes

### Type Safety
- All exceptions will maintain strict typing
- Generic constraints where appropriate
- Protocol-based interfaces for flexibility

### Performance
- Minimal overhead for exception creation
- Lazy evaluation of troubleshooting information
- Efficient serialization for high-frequency logging

### Testing
- Comprehensive test coverage for all exception paths
- Backward compatibility tests
- Integration tests for error flows

This unified hierarchy will significantly improve the maintainability and consistency of WebSocket error handling while preserving all existing functionality.

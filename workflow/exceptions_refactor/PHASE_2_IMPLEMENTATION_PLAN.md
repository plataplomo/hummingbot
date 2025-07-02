# Phase 2 Implementation Plan: Advanced Exception Consolidation

## Executive Summary

**Objective**: Reduce exceptions from 82 to ~50 (39% additional reduction) using context-rich consolidation patterns while enhancing semantic richness and maintaining 100% TRY compliance.

**Total Project Impact**: 112 → 50 exceptions (**55% overall reduction**)

## Implementation Phases

### Phase 2A: Foundation & Quick Wins (Week 1)
**Target**: 82 → 68 exceptions (-14)
**Risk Level**: Low
**Effort**: 2-3 days

#### Tasks
1. **Remove Unused Base Classes** (8 → 0)
   ```bash
   # Remove these unused exceptions:
   - WebSocketError (base class, 0 uses)
   - FieldError (base class, 0 uses)
   - MarketDataServiceError (base class, 0 uses)
   - ServiceParameterError (created but unused)
   - ResponseParsingError (0 uses)
   - HttpTimeoutError (0 uses)
   - MsgpackSerializationError (0 uses)
   - ActionHashError (0 uses)
   ```

2. **Eliminate Core/API Layer Duplicates** (8 → 4)
   ```python
   # Remove from cyberdelta/apis/exceptions/:
   - DecimalFiniteError      # Use cyberdelta/exceptions/parsing.py
   - TypeFieldError          # Use cyberdelta/exceptions/parsing.py
   - FieldError              # Use cyberdelta/exceptions/parsing.py
   - ListFieldError          # Use cyberdelta/exceptions/parsing.py

   # Update imports in affected files
   ```

3. **Consolidate Low-Usage WebSocket Errors** (4 → 2)
   ```python
   # Before: 4 separate WebSocket exceptions
   class UserEventsSubscriptionError(...)      # 1 use
   class UnsupportedWebSocketTopicError(...)   # 1 use
   class WebSocketSubscriptionError(...)       # 1 use
   class InvalidWebSocketDataError(...)        # 5 uses (keep)

   # After: 2 consolidated exceptions
   class WebSocketSubscriptionError(APIError):  # Enhanced for all subscription issues
   # Keep: InvalidWebSocketDataError (distinct purpose, 5 uses)
   ```

#### Deliverables
- [ ] 8 unused base classes removed
- [ ] 4 core/API duplicates eliminated
- [ ] WebSocket errors consolidated
- [ ] All imports updated
- [ ] Tests verified passing
- [ ] TRY compliance maintained

### Phase 2B: Semantic Consolidation (Weeks 2-3)
**Target**: 68 → 50 exceptions (-18)
**Risk Level**: Medium
**Effort**: 1.5 weeks

#### Task 1: Market Data Service Consolidation (8 → 3)
```python
# Current fragmented approach (8 exceptions):
class EmptySymbolError(...)           # 5 uses
class InvalidLimitError(...)          # 2 uses
class InvalidTimeRangeError(...)      # 2 uses
class EmptySymbolListError(...)       # 1 use
class EmptySymbolInListError(...)     # 1 use
class NullSymbolsError(...)           # 1 use
class UnsupportedIntervalError(...)   # 1 use
class NoFundingDataError(...)         # 2 uses

# New consolidated approach (3 exceptions):
class ServiceParameterError(MarketDataServiceError):
    """Consolidated parameter validation for all service methods."""
    def __init__(
        self,
        parameter: str,
        issue: str,
        service_method: str,
        value: object = None,
        expected_type: str | None = None,
        supported_values: list[str] | None = None,
        exchange: str | None = None,
        suggestion: str | None = None,
    ):
        # Handles: EmptySymbol, InvalidLimit, InvalidTimeRange,
        #         EmptySymbolList, EmptySymbolInList, NullSymbols,
        #         UnsupportedInterval

class ServiceDataError(MarketDataServiceError):
    """Data availability and format issues."""
    # Handles: NoFundingDataError + future data availability issues

# Keep: SymbolNotFoundError (4 uses, semantically distinct)
```

#### Task 2: Authentication Streamlining (4 → 2)
```python
# Current scattered approach (4 exceptions):
class InvalidAPIKeyError(...)              # 1 use
class AuthenticationPreparationError(...)  # 1 use
class WebSocketSignatureError(...)         # 1 use
class AuthenticatorNotConfiguredError(...) # 2 uses

# New consolidated approach (2 exceptions):
class AuthenticationError(APIError, ValueError):
    """All credential and signature-related failures."""
    def __init__(
        self,
        reason: str,
        credential_type: str,  # "api_key", "private_key", "signature"
        operation: str | None = None,
        exchange: str | None = None,
        hint: str | None = None,
    ):
        # Handles: InvalidAPIKey, AuthenticationPreparation, WebSocketSignature

class AuthenticationConfigError(APIError):
    """Setup and configuration issues."""
    # Handles: AuthenticatorNotConfigured, endpoint configuration
```

#### Task 3: Request/Response Validation Merge (6 → 3)
```python
# Current fragmented validation (6 exceptions):
class DecimalFormatError(...)            # 3 uses (if not deduplicated)
class DecimalRangeError(...)             # 4 uses
class PrecisionLossError(...)            # 1 use
class MissingRequiredParameterError(...) # 4 uses
class InvalidParameterTypeError(...)     # 1 use
class InvalidEnumValueError(...)         # 1 use

# New consolidated validation (3 exceptions):
class RequestParameterError(APIError):
    """All request parameter validation issues."""
    def __init__(
        self,
        parameter: str,
        issue: str,
        value: object = None,
        constraint: str | None = None,
        expected_type: str | None = None,
        exchange: str | None = None,
        suggestion: str | None = None,
    ):
        # Handles: DecimalFormat, DecimalRange, PrecisionLoss,
        #         MissingRequiredParameter, InvalidParameterType, InvalidEnumValue

class ResponseValidationError(APIError):
    """Response format and content validation."""
    # Handles: InvalidLeverageError, NotImplementedOperationError

# Keep: EmptyResponseError, UnreachableCodeError (higher usage, distinct purposes)
```

#### Deliverables
- [ ] Market data service errors consolidated (8 → 3)
- [ ] Authentication errors streamlined (4 → 2)
- [ ] Request/response validation merged (6 → 3)
- [ ] All affected raise sites updated with enhanced context
- [ ] Comprehensive test coverage for new consolidated exceptions
- [ ] Documentation updated

### Phase 2C: Transformation Revolution (Weeks 4-5)
**Target**: 50 → 42 exceptions (-8)
**Risk Level**: High
**Effort**: 1.5 weeks

#### The Big Transformation: Domain Explosion → Context Rich (8 → 1)

**Current Domain-Specific Pattern**:
```python
# 8 separate transformation exceptions
class OrderTransformationError(...)      # 12 uses
class TradeTransformationError(...)      # 8 uses
class TickerTransformationError(...)     # 3 uses
class MarketTransformationError(...)     # 4 uses
class OrderBookTransformationError(...)  # 5 uses
class FundingRateTransformationError(...) # 3 uses
class CandleTransformationError(...)     # 2 uses
class CollateralTransformationError(...) # 1 use
```

**New Context-Rich Pattern**:
```python
class TransformationError(APIError):
    """Universal transformation error with rich domain context."""

    def __init__(
        self,
        message: str,
        domain: str,           # "order", "trade", "ticker", "market", etc.
        operation: str,        # "parse", "convert", "validate", "serialize"
        field_name: str | None = None,
        source_value: object = None,
        target_type: str | None = None,
        source_format: str | None = None,
        exchange: str | None = None,
        validation_errors: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize transformation error with comprehensive context.

        Args:
            message: Human-readable description of the transformation failure
            domain: Data domain being transformed (order, trade, ticker, etc.)
            operation: Specific transformation operation (parse, convert, etc.)
            field_name: Specific field that failed transformation
            source_value: Original value that couldn't be transformed
            target_type: Expected target type/format
            source_format: Source data format (json, xml, raw, etc.)
            exchange: Exchange where transformation failed
            validation_errors: List of specific validation failures
            metadata: Additional context for debugging and monitoring
        """
        # Build rich contextual message
        context_prefix = f"[{domain.upper()}]"
        if exchange:
            context_prefix = f"[{exchange}] {context_prefix}"

        enhanced_message = f"{context_prefix} {message}"
        if operation:
            enhanced_message += f" during {operation}"
        if field_name:
            enhanced_message += f" (field: {field_name})"
        if source_value is not None:
            enhanced_message += f" (value: {source_value})"
        if target_type:
            enhanced_message += f" (expected: {target_type})"

        super().__init__(
            message=enhanced_message,
            code=APIErrorCode.TRANSFORMATION_FAILED.value,
            metadata={
                "transformation_domain": domain,
                "operation": operation,
                "field_name": field_name,
                "source_value": str(source_value) if source_value is not None else None,
                "target_type": target_type,
                "source_format": source_format,
                "exchange": exchange,
                "validation_errors": validation_errors,
                "timestamp": datetime.now(tz=UTC).isoformat(),
                **(metadata or {})
            }
        )

        # Store context as attributes for programmatic access
        self.domain = domain
        self.operation = operation
        self.field_name = field_name
        self.source_value = source_value
        self.target_type = target_type
        self.exchange = exchange
```

**Usage Examples**:
```python
# Before: Domain-specific exceptions
raise OrderTransformationError("Invalid order format")
raise TradeTransformationError("Invalid trade format")
raise TickerTransformationError("Invalid ticker format")

# After: Single exception with rich context
raise TransformationError(
    message="Invalid format",
    domain="order",
    operation="parse_from_raw",
    field_name="order_type",
    source_value="INVALID_TYPE",
    target_type="OrderType",
    exchange="hyperliquid",
    validation_errors=["order_type must be LIMIT or MARKET"]
)

raise TransformationError(
    message="Failed to parse timestamp",
    domain="trade",
    operation="convert_timestamp",
    field_name="execution_time",
    source_value="invalid-timestamp",
    target_type="datetime",
    exchange="backpack"
)
```

#### Migration Strategy
1. **Create new TransformationError** with comprehensive context support
2. **Update all raise sites** to use new exception with full context
3. **Create backward compatibility aliases** for heavily used exceptions
4. **Gradual deprecation** of old domain-specific exceptions
5. **Enhanced testing** to ensure no information loss

#### Deliverables
- [ ] Single powerful TransformationError implemented
- [ ] All 35+ raise sites updated with rich context
- [ ] Backward compatibility aliases created
- [ ] Comprehensive test suite for new pattern
- [ ] Performance validation (ensure context doesn't impact performance)
- [ ] Documentation with usage examples

## Risk Mitigation Strategy

### Backward Compatibility Plan
```python
# Maintain aliases during transition period
OrderTransformationError = TransformationError  # Deprecated alias
TradeTransformationError = TransformationError  # Deprecated alias
# ... etc for other heavily used exceptions

# With custom __new__ for automatic context injection if needed
class OrderTransformationError(TransformationError):
    def __new__(cls, message: str, **kwargs):
        return TransformationError(
            message=message,
            domain="order",
            operation="transform",
            **kwargs
        )
```

### Testing Strategy
1. **Comprehensive unit tests** for all new consolidated exceptions
2. **Integration tests** to ensure raise sites work correctly
3. **Backward compatibility tests** for alias support
4. **Performance tests** to ensure metadata doesn't impact performance
5. **End-to-end tests** to validate complete error handling flows

### Rollback Plan
- **Phase-by-phase implementation** allows selective rollback
- **Git feature branches** for each phase enable easy reversion
- **Comprehensive test coverage** catches issues early
- **Gradual migration** minimizes impact of any problems

## Success Metrics

### Quantitative Goals
- [ ] Exception count: 82 → 50 (39% reduction)
- [ ] Total project reduction: 112 → 50 (55% reduction)
- [ ] Zero unused exceptions
- [ ] 100% TRY compliance maintained
- [ ] All tests passing

### Qualitative Goals
- [ ] Enhanced debugging through rich context metadata
- [ ] Improved maintainability through generic patterns
- [ ] Better production monitoring capabilities
- [ ] Future-proof architecture (new domains don't need new exceptions)
- [ ] Preserved semantic richness through context parameters

### Performance Goals
- [ ] No performance regression in exception creation/handling
- [ ] Metadata addition doesn't impact hot paths
- [ ] Memory usage remains constant or improves

## Timeline Summary

| Phase | Duration | Target Reduction | Risk Level |
|-------|----------|------------------|------------|
| **2A: Foundation** | Week 1 | 82 → 68 (-14) | Low |
| **2B: Consolidation** | Weeks 2-3 | 68 → 50 (-18) | Medium |
| **2C: Revolution** | Weeks 4-5 | 50 → 42 (-8) | High |
| **Total** | **5 weeks** | **82 → 42 (-40)** | **Managed** |

**Final State**: 112 → 42 exceptions (**63% total reduction**)

This implementation plan provides a structured path to achieving exceptional exception handling - fewer exceptions, richer context, better maintainability, and enhanced debugging capabilities.

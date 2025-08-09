# WebSocket Type Safety Fix: Decoupled Architecture Implementation Plan

## Overview
This document tracks the implementation of the **DECOUPLED WEBSOCKET ERROR ARCHITECTURE** - a complete separation from the REST API error system to achieve 100% type safety in WebSocket error handling.

**🎯 DECIDED APPROACH**: Full decoupled architecture with `WebSocketStreamError` independent of `APIError`, using compatibility adapter for legacy systems.

**Status Legend**: ⬜ Not Started | 🟨 In Progress | ✅ Complete | ❌ Blocked

---

## 🎯 **FINAL ARCHITECTURE DECISION**

### **Option B: Full Decoupled Architecture (CHOSEN)**

After comprehensive analysis in `05_comprehensive_architecture_research.md`, the decision is made:

**✅ IMPLEMENT**: Complete WebSocket error system separation:
- `WebSocketStreamError` does **NOT** inherit from `APIError`
- `StreamErrorContext` provides **fully typed** WebSocket context
- `WebSocketRecoveryStrategy` enum for **typed recovery** strategies
- Temporary `WebSocketErrorAdapter` for legacy compatibility

**❌ REJECTED**: Constrained fixes within existing APIError inheritance
**❌ REJECTED**: Working around the semantic mismatch

### **Key Benefits of Chosen Approach**:

1. **100% Type Safety**: Zero `dict[str, Any]` in WebSocket error handling
2. **Semantic Clarity**: WebSocket streaming concepts vs HTTP concepts
3. **Rich Recovery Logic**: `WebSocketRecoveryStrategy` vs boolean `is_retryable`
4. **Future-Proof**: Clean foundation for msgspec integration
5. **Backward Compatible**: Temporary adapter maintains existing integrations

### **Implementation Timeline**: 4 weeks (detailed in comprehensive research)

---

## Phase 1: Critical Trading Safety (P0) - Week 1
*These issues pose immediate risk to trading operations*

### Error Handler Type Safety (Steps 1-8)

**🚨 ARCHITECTURAL CONSTRAINT**: WebSocket errors inherit from `APIError` which expects HTTP concepts (status codes, retry_after). This forces dict conversions. We'll add type safety WITHIN this constraint.

1. ⬜ **Audit all error handlers for dict conversions**
   - File: `cyberdelta/apis/websocket/ws_error_handler.py`
   - Find all `dict[str, Any]` parameters in error handlers
   - Document each conversion point
   - **KEY FINDING**: `convert_validation_error_to_api_error()` forces WebSocket errors into REST model

2. ⬜ **Create typed error context models**
   ```python
   class TypedErrorContext(BaseModel):
       error: Exception
       context: WebSocketContextProtocol
       payload: BaseModel | None
       # NOTE: Must still convert to APIError for compatibility
       # Future work: Separate WebSocket error system
   ```

3. ⬜ **Update BaseErrorHandler interface**
   - Replace `dict[str, Any]` with typed protocols
   - Maintain backward compatibility with adapter pattern
   - **CONSTRAINT**: Must still produce `APIError` instances
   - Add adapter to bridge typed context to `APIError` requirements

4. ⬜ **Fix ws_processor.py error handling**
   - Line 264: Remove `context.model_dump(mode="python")`
   - Pass typed context directly to error handlers

5. ⬜ **Fix ws_router.py error handling**
   - Line 354: Remove context dict conversion
   - Update `_handle_missing_processor` to use typed context

6. ⬜ **Create error handler migration tests**
   - Test both old dict-based and new typed interfaces
   - Ensure no regression in error reporting

7. ⬜ **Deploy error handler fixes to staging**
   - Monitor for any error handling issues
   - Validate error messages still propagate correctly

8. ⬜ **Production deployment of error handler fixes**
   - Gradual rollout with feature flags
   - Monitor error rates and message quality

### Exchange Coupling Removal (Steps 9-15)

9. ⬜ **Create ExchangeProtocol interface**
   ```python
   class ExchangeEnvelopeProtocol(Protocol):
       def get_routing_key(self) -> str: ...
       def get_topic(self) -> str | None: ...
   ```

10. ⬜ **Update BackpackRawWebSocketEnvelope**
    - Implement `get_routing_key()` method
    - Implement `get_topic()` method returning `stream`

11. ⬜ **Update HyperliquidRawWebSocketEnvelope**
    - Implement `get_routing_key()` method
    - Implement `get_topic()` method returning `channel`

12. ⬜ **Remove exchange-specific logic from ws_context.py**
    - Lines 72-75: Replace conditional with protocol method call
    - Use `self.validated_envelope.get_topic()`

13. ⬜ **Fix router routing key extraction**
    - Remove Backpack-specific split logic
    - Use exchange-agnostic routing key extraction

14. ⬜ **Update discriminated unions for plugin pattern**
    - Create exchange detector registry
    - Remove hardcoded exchange detection

15. ⬜ **Test with new exchange simulator**
    - Create mock exchange with different format
    - Ensure no hardcoded assumptions break

### Type Guard Integration (Steps 16-20)

16. ⬜ **Replace isinstance with type guards in ws_security.py**
    - Line 338: Use `is_secure_dict()` instead of `isinstance(obj, dict)`
    - Update all security validation points

17. ⬜ **Create comprehensive type guard suite**
    ```python
    is_secure_dict()
    is_secure_list()
    is_valid_payload()
    is_exchange_envelope()
    ```

18. ⬜ **Integrate type guards with security validators**
    - Update WebSocketSecurityValidator to use type guards
    - Add proper type narrowing at each validation point

19. ⬜ **Add type guard performance benchmarks**
    - Measure overhead of type guards vs isinstance
    - Optimize hot paths if needed

20. ⬜ **Deploy type guard integration**
    - Test in staging environment
    - Monitor for any validation failures

---

## Phase 2: Architecture Integrity (P1) - Week 2
*These issues affect system maintainability and correctness*

### Missing Pydantic Models (Steps 21-28)

21. ⬜ **Create SecurityValidationError model**
    ```python
    class SecurityValidationError(BaseModel):
        violation_type: SecurityViolationType
        message_data: ValidationErrorContext
        security_context: SecurityContext
        # NOTE: Will need conversion to APIError for compatibility
    ```

22. ⬜ **Create ProcessingMetrics model**
    ```python
    class ProcessingMetrics(BaseModel):
        total_processed: int
        validation_errors: int
        average_processing_time_ms: float
    ```

23. ⬜ **Create ErrorContext hierarchy**
    - Base ErrorContext model
    - Specific error context types for each error category

24. ⬜ **Create LogContext models**
    ```python
    class StructuredLogContext(BaseModel):
        correlation_id: str
        exchange: str
        operation: str
        metadata: dict[str, str]
    ```

25. ⬜ **Update ws_processor.py stats method**
    - Return ProcessingMetrics instead of dict
    - Add computed fields for derived metrics

26. ⬜ **Update metrics collector**
    - Use typed metrics models throughout
    - Remove dict-based metric collection

27. ⬜ **Create model validation tests**
    - Test all new Pydantic models
    - Ensure proper validation and serialization

28. ⬜ **Migrate existing dict usages to models**
    - Find and replace all metric/error dicts
    - Update consumers of these APIs

### Internal API Type Safety (Steps 29-35)

29. ⬜ **Audit all internal APIs for dict[str, Any]**
    - Create comprehensive list of all dict-based APIs
    - Prioritize by usage frequency

30. ⬜ **Create typed interfaces for processor communication**
    ```python
    class ProcessorMessage(BaseModel):
        routing_key: str
        payload: BaseModel
        context: WebSocketContextProtocol
    ```

31. ⬜ **Update transformer interfaces**
    - Remove dict parameters
    - Use typed models for all transformations

32. ⬜ **Fix WebSocket message factory**
    - Type all factory methods properly
    - Remove Any from return types

33. ⬜ **Update router internal methods**
    - Type all private methods properly
    - Remove dict conversions in internal APIs

34. ⬜ **Create type-safe message bus**
    - Replace dict-based message passing
    - Use typed protocols for all messages

35. ⬜ **Deploy internal API updates**
    - Test thoroughly in staging
    - Monitor for any message passing issues

---

## Phase 3: Performance Optimization (P2) - Week 3
*Optimize without losing type safety*

### Validation Consolidation (Steps 36-42)

36. ⬜ **Profile current validation overhead**
    - Measure time spent in each validation layer
    - Identify redundant validations

37. ⬜ **Create unified validation pipeline**
    ```python
    class UnifiedValidator:
        def validate_once(self, raw: bytes) -> ValidatedMessage:
            # Single validation pass
    ```

38. ⬜ **Remove redundant security checks**
    - Consolidate security validation with type validation
    - Single pass for both concerns

39. ⬜ **Optimize Pydantic validation**
    - Use validate_call where appropriate
    - Cache validation schemas

40. ⬜ **Implement validation result caching**
    - Cache validation results for identical messages
    - Use LRU cache with appropriate size

41. ⬜ **Add validation performance metrics**
    - Track validation time percentiles
    - Alert on performance degradation

42. ⬜ **Deploy optimized validation**
    - A/B test performance improvements
    - Ensure no validation gaps

### Serialization Optimization (Steps 43-46)

43. ⬜ **Implement model dump caching**
    - Cache frequently dumped models
    - Use proper cache invalidation

44. ⬜ **Switch to binary WebSocket protocol**
    - Use send_bytes instead of send_str
    - Remove unnecessary UTF-8 decode

45. ⬜ **Optimize orjson usage**
    - Use orjson.OPT_PASSTHROUGH_DATETIME
    - Configure optimal orjson options

46. ⬜ **Add serialization benchmarks**
    - Track serialization performance
    - Compare different serialization strategies

---

## Phase 4: Testing and Documentation (Week 4)

### Comprehensive Testing (Steps 47-50)

47. ⬜ **Create type safety regression tests**
    - Test suite to prevent dict[str, Any] reintroduction
    - AST-based checks for type safety violations

48. ⬜ **Add integration tests for all fixes**
    - End-to-end WebSocket message flow tests
    - Test with multiple exchange types

49. ⬜ **Performance regression tests**
    - Ensure fixes don't degrade performance
    - Benchmark against baseline metrics

50. ⬜ **Documentation and knowledge transfer**
    - Document all architectural changes
    - Create type safety guidelines
    - Update developer onboarding docs
    - **CRITICAL**: Document WebSocket-APIError coupling as technical debt
    - Create migration plan for future error system separation

---

## Success Metrics

### Must Achieve (Week 1-2)
- [ ] Zero `dict[str, Any]` in critical error paths (within APIError constraint)
- [ ] No exchange-specific code in core components
- [ ] All type guards properly integrated
- [ ] All P0 issues resolved
- [ ] Document WebSocket-APIError coupling for future separation

### Should Achieve (Week 3)
- [ ] All internal APIs typed
- [ ] 50% reduction in validation overhead
- [ ] All missing Pydantic models created

### Nice to Have (Week 4)
- [ ] 100% type coverage in WebSocket layer
- [ ] Comprehensive test coverage
- [ ] Performance improvements documented

---

## Risk Mitigation

### Architectural Debt
- **WebSocket-APIError Coupling**: Creates type safety issues
- **Mitigation**: Work within constraint for now, document for future separation
- **Long-term**: Plan WebSocket-specific error system (see `03_error_system_analysis.md`)

### Rollback Strategy
- Each phase can be rolled back independently
- Feature flags for all major changes
- Gradual rollout to production

### Monitoring
- Real-time type error tracking
- Performance metrics dashboard
- Error rate monitoring

### Testing Strategy
- Unit tests for each change
- Integration tests for phase completion
- Load tests before production deployment

---

## Dependencies

### Team Resources
- 2 senior engineers for implementation
- 1 QA engineer for testing
- DevOps support for deployment

### External Dependencies
- No new libraries required
- Existing monitoring infrastructure sufficient
- Current CI/CD pipeline adequate

---

## Daily Checklist

### Before Starting Any Fix
- [ ] Create feature branch
- [ ] Write tests first (TDD)
- [ ] Document the change

### After Completing Each Step
- [ ] Run type checkers (mypy, pyright)
- [ ] Run linters (ruff)
- [ ] Run tests
- [ ] Update this document

### End of Day
- [ ] Commit all changes
- [ ] Update progress percentages
- [ ] Note any blockers

---

## Progress Tracking

### Week 1 Progress: 0% (0/20 steps)
- Phase 1 Steps 1-20: ⬜⬜⬜⬜⬜ ⬜⬜⬜⬜⬜ ⬜⬜⬜⬜⬜ ⬜⬜⬜⬜⬜

### Week 2 Progress: 0% (0/15 steps)
- Phase 2 Steps 21-35: ⬜⬜⬜⬜⬜ ⬜⬜⬜⬜⬜ ⬜⬜⬜⬜⬜

### Week 3 Progress: 0% (0/11 steps)
- Phase 3 Steps 36-46: ⬜⬜⬜⬜⬜ ⬜⬜⬜⬜⬜ ⬜

### Week 4 Progress: 0% (0/4 steps)
- Phase 4 Steps 47-50: ⬜⬜⬜⬜

### Overall Progress: 0% (0/50 steps)

---

## Notes and Blockers

### Current Blockers
- None yet

### Decisions Needed
- Backward compatibility strategy for error handlers
- Performance vs type safety trade-offs
- Deployment timeline coordination
- **CRITICAL**: Whether to separate WebSocket/API error systems (future work)

### Lessons Learned
- WebSocketError inherits from APIError causing semantic mismatch
- Same error codes mean different things in REST vs WebSocket context
- WebSocket errors forced to include `http_status` (always None)
- Dict conversions exist to bridge WebSocket context to APIError model
- Future work: Separate error systems with shared protocol interface

---

*Last Updated: [Current Date]*
*Next Review: [Weekly]*

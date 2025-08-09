# HTTP/REST API Error Domain Type Safety Improvement

## Executive Summary

This workflow documents a comprehensive enhancement plan for the HTTP/REST API error domain in `cyberdelta/apis/common/` to bring it up to the same level of type safety and semantic clarity achieved by the WebSocket error architecture. **This is FUTURE work to be considered only AFTER the WebSocket error migration is complete.**

## Context

The WebSocket error architecture (documented in `../websocket_type_safety/05_comprehensive_architecture_research.md`) establishes new patterns for type-safe error handling:

1. **Type-Safe Context**: `StreamErrorContext` (Pydantic) vs `dict[str, Any]`
2. **Typed Recovery Strategies**: `WebSocketRecoveryStrategy` enum vs `bool is_retryable`
3. **Domain-Specific Error Codes**: WebSocket streaming concepts vs generic HTTP codes
4. **Validation Integration**: `ErrorContextValidator` with reusable validation logic

## Current State Analysis

After deep research of `cyberdelta/apis/common/`, the current HTTP API error system has:

### ✅ Strong Foundation
- **Exception-wraps-Pydantic-model Pattern**: Clean architecture with `APIError` → `APIErrorResponse`
- **Comprehensive Error Codes**: Well-organized `APIErrorCode` enum with logical ranges
- **Rich HTTP Context**: HTTP status, retry-after, exchange codes properly captured
- **Original Exception Chaining**: Good error traceability

### ❌ Critical Type Safety Issues
- **Type Erasure**: `metadata: dict[str, Any]` destroys all type information
- **Interface Propagation**: `IErrorMapper` forces `dict[str, Any]` throughout system
- **Validation Gaps**: No structure validation for error context
- **Recovery Limitations**: Boolean `is_retryable` vs rich recovery strategies

## Enhancement Opportunity

The HTTP API error domain can be enhanced using the same patterns proven successful in the WebSocket architecture:

| Aspect | Current HTTP API | WebSocket Architecture | Enhancement Opportunity |
|--------|------------------|----------------------|------------------------|
| **Error Context** | `metadata: dict[str, Any]` | `StreamErrorContext: BaseModel` | → `HTTPErrorContext: BaseModel` |
| **Recovery Logic** | `is_retryable: bool` | `WebSocketRecoveryStrategy` enum | → `HTTPRecoveryStrategy` enum |
| **Type Safety** | Type erasure at boundaries | Full type preservation | → Discriminated union contexts |
| **Validation** | Manual dict validation | `ErrorContextValidator` | → `HTTPErrorContextValidator` |

## Scope and Timing

**⚠️ IMPORTANT**: This enhancement work should ONLY begin after:
1. ✅ WebSocket error migration is complete and stable
2. ✅ WebSocket architecture has proven its value in production
3. ✅ Team has gained experience with the new error patterns

**Estimated Timeline**: 4-6 weeks after WebSocket completion
**Risk Level**: Medium (well-understood patterns, proven architecture)

## Documents in This Workflow

1. **00_executive_summary.md** - This overview document
2. **01_http_api_error_enhancement_ideas.md** - Detailed technical enhancement proposals
3. **02_current_architecture_analysis.md** - Deep analysis of existing system issues
4. **03_type_safety_enhancement_plan.md** - Specific plans for eliminating type erasure
5. **04_recovery_strategy_enhancement.md** - Enhanced recovery logic design
6. **05_migration_strategy.md** - Implementation phases and timeline
7. **06_integration_testing_plan.md** - Testing strategy for seamless migration

## Success Criteria

After implementation, the HTTP API error domain should achieve:

1. **100% Type Safety**: Zero `dict[str, Any]` usage in error handling
2. **Rich Recovery Context**: Typed strategies for different error scenarios
3. **Consistent Architecture**: Same patterns as WebSocket error system
4. **Backward Compatibility**: Seamless migration for existing consumers
5. **Enhanced Monitoring**: Structured, typed error context for observability

## Value Proposition

This enhancement will provide:
- **Better Developer Experience**: Full IDE support and compile-time error detection
- **Improved Monitoring**: Rich, structured error context for better observability
- **Enhanced Recovery**: Sophisticated recovery strategies beyond simple retry logic
- **Architectural Consistency**: Unified error handling patterns across domains
- **Future-Proof Foundation**: Type-safe extension points for additional error contexts

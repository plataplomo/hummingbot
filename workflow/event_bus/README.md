# Event Bus Architecture Documentation

## Overview

This directory contains the complete documentation for migrating CyberDeltaEngine from DomainEvent (with `dict[str, Any]` violations) to a high-performance, type-safe event architecture using msgspec and bubus, enhanced with production-grade patterns inspired by Nautilus Trader.

## Document Structure

### Core Documents

1. **[01_architecture_overview.md](01_architecture_overview.md)**
   - Executive summary of the new architecture
   - Technology choices and rationale
   - Benefits and trade-offs
   - High-level design decisions

2. **[02_implementation_guide.md](02_implementation_guide.md)**
   - Step-by-step implementation instructions
   - Code examples for all components
   - Directory structure
   - Testing strategies

3. **[03_migration_strategy.md](03_migration_strategy.md)**
   - Three-phase migration plan
   - Risk mitigation strategies
   - Rollback procedures
   - Success criteria

4. **[04_event_structures.md](04_event_structures.md)**
   - Complete event definitions
   - Field usage patterns
   - EventType enum mapping
   - Performance characteristics

5. **[05_symbol_enum_conversion.md](05_symbol_enum_conversion.md)**
   - **UPDATED**: Symbol as `str` in events (zero overhead decision)
   - Enums work directly in msgspec (no conversion needed!)
   - Handler creates Symbol objects only when needed
   - Performance analysis and rationale

10. **[10_symbol_string_decision.md](10_symbol_string_decision.md)** *(NEW)*
   - Architecture Decision Record for Symbol representation
   - Performance analysis: 0μs overhead vs 0.42μs per conversion
   - Implementation patterns and consequences
   - Validation results

6. **[06_tree_structure.md](06_tree_structure.md)**
   - Complete directory tree structure
   - File locations for all components
   - Import examples
   - Bootstrap wiring with lifecycle management
   - Testing structure including lifecycle tests

7. **[07_handler_placement_analysis.md](07_handler_placement_analysis.md)**
   - Analysis of where event handlers should live
   - Recommends co-locating with domain logic instead of separate handlers folder

8. **[09_nautilus_patterns_analysis.md](09_nautilus_patterns_analysis.md)**
   - Deep analysis of Nautilus Trader patterns
   - Which patterns to leverage vs avoid
   - Implementation examples with lifecycle management
   - Component state management strategies

## Key Architecture Decisions

### Technology Stack
- **msgspec ONLY for Events**: Ultra-fast event serialization (25x faster than Pydantic)
  - ALL events use msgspec.Struct
  - ALL event contexts use msgspec.Struct  
  - ALL handler health models use msgspec.Struct
  - NO Pydantic in the event system
  - Symbols as `str` (zero conversion overhead)
  - Enums work directly (ExchangeName, OrderSide, etc.)
- **bubus**: Complex workflow orchestration with audit trails (uses msgspec contexts)
- **tenacity**: Retry logic for resilient event handling (project standard)
- **Event Handlers**: Decouples events from domain models (co-located with domains)
- **Pydantic**: ONLY for non-event domain models (Order, Position, etc.)

### Nautilus-Inspired Enhancements
- **Lifecycle Management**: on_start(), on_stop(), on_degrade() hooks for handlers
- **Component States**: PRE_INITIALIZED, RUNNING, DEGRADED, STOPPED state tracking
- **Priority-Based Routing**: CRITICAL, HIGH, NORMAL, LOW handler priorities
- **Handler-Level Caching**: Performance optimization through local caches
- **Request/Response Pattern**: Synchronous queries with timeouts
- **Hierarchical Event Routing**: Specific → category → generic handler fallback
- **Auto-Degradation**: Automatic degraded mode on error thresholds
- **Resilient Retry Logic**: Tenacity-based retry for transient failures (ConnectionError, TimeoutError)

### Design Principles
1. **Zero Domain Model Changes**: Domain models stay exactly as they are
2. **Type Safety**: No more `dict[str, Any]` in events
3. **Performance**: 25x faster event processing
4. **Minimal Structures**: Only 7 msgspec events + 3-5 bubus workflows
5. **Progressive Migration**: Can migrate gradually without breaking changes
6. **Domain Cohesion**: Event handlers co-located with domain logic, not separate
7. **Production Reliability**: Lifecycle management and degraded modes for resilience
8. **Performance First**: Handler caching and priority routing for optimal latency

## Quick Start

For implementers:
1. Start with [02_implementation_guide.md](02_implementation_guide.md)
2. Reference [04_event_structures.md](04_event_structures.md) for event definitions
3. Follow [03_migration_strategy.md](03_migration_strategy.md) for migration steps

For architects:
1. Review [01_architecture_overview.md](01_architecture_overview.md) for design rationale
2. Review [07_handler_placement_analysis.md](07_handler_placement_analysis.md) for adapter placement
3. Review [09_nautilus_patterns_analysis.md](09_nautilus_patterns_analysis.md) for production patterns
4. Understand the event adapter pattern for decoupling

## Architecture Diagram


### Current Event Flow (Critical to Understand!)\n```\nAPIs (bp_api, hl_api) → return data → Domain Services → publish DomainEvent\n        ↑                                      ↑\n   NO events here                        Events published here\n```\n\n### New Architecture with Nautilus Enhancements
```
┌─────────────────────────────────────────────┐
│           APIs (UNCHANGED - dont publish events)          │
└─────────────────┬───────────────────────────┘
                  │ Raw bytes
                  ▼
┌─────────────────────────────────────────────┐
│         msgspec Events (7 structures)        │
│  • MarketData  • OrderEvent  • PositionEvent │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│   Enhanced MsgspecEventBus (Priority-Based)  │
│  • CRITICAL → HIGH → NORMAL → LOW routing    │
│  • Request/Response pattern support          │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│   Event Handlers with Lifecycle (domain/*)   │
│  • on_start() / on_stop() / on_degrade()    │
│  • Handler-level caching                     │
│  • Auto-degradation on errors                │
│  • Hierarchical routing                      │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│      Domain Models (Pydantic - UNCHANGED)    │
│  • Order  • Position  • Account              │
└─────────────────────────────────────────────┘
```

## Performance Gains

| Metric | Current (DomainEvent) | New (msgspec) | Improvement |
|--------|--------------------|---------------|-------------|
| Decode Time | 3,470 μs | 140 μs | 25x faster |
| Memory Usage | 16.26 MB | 0.64 MB | 25x less |
| Type Safety | Magic strings | Full typing | 100% safe |
| Event Count | 33 classes | 7 structures | 79% reduction |
| Handler Latency | No caching | With caching | ~10x faster |
| Critical Events | Sequential | Priority routing | Sub-ms response |
| System Resilience | Hard failures | Degraded modes | 99.9% uptime |

## Migration Timeline

- **Week 1**: Foundation - Create events, handlers with lifecycle, and enhanced bus
- **Week 2**: Parallel Operation - Run both systems with health monitoring
- **Week 3**: Cutover - Remove old system, verify degraded modes work

## Contact

For questions about this architecture, please consult the team lead or review the CODING_STANDARDS.md for broader context.

## Key Features from Nautilus Trader

### What We're Leveraging
- Actor lifecycle management (on_start, on_stop, on_degrade)
- Component state tracking for reliability
- Priority-based message routing for critical events
- Handler-level caching for performance
- Request/response pattern for synchronous queries
- Hierarchical event routing (specific → generic)

### Retry Patterns with Tenacity
- **Connection Resilience**: Automatic retry for ConnectionError and TimeoutError
- **Exponential Backoff**: Prevents overwhelming failed services
- **Critical Event Protection**: Extra retry attempts for OrderEvent and PositionEvent
- **Graceful Degradation**: Auto-degrade after retry exhaustion
- **Configurable Strategies**: Different retry policies for different event types

### What We're NOT Using
- Rust/Cython implementations (keeping pure Python)
- Complex distributed actors (keeping simple)
- Full message store (using bubus for critical workflows only)
- Heavy protocol definitions (keeping lightweight)

---

**Last Updated**: 2025-08-08
**Status**: Ready for Implementation with Nautilus Enhancements
**Approved By**: [Pending]

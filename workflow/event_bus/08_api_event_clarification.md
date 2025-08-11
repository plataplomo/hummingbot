# Critical Clarification: APIs Do Not Publish Events

## Architecture Understanding

### Current Event Publishing in CyberDeltaEngine

**APIs are NOT event publishers.** This is a critical architectural distinction that simplifies migration:

```python
# How it actually works:

# 1. APIs fetch/send data to exchanges
result = await bp_api.place_order(order_params)  # Returns OrderResult data

# 2. Domain services use API data and publish events
async def execute_order(self, signal: TradeSignal):
    # Use API to place order
    result = await self.bp_api.place_order(...)

    # Domain service publishes the event
    event = DomainEvent(
        event_type=EventType.ORDER_PLACED,
        payload={"order_id": result.order_id}
    )
    await self.event_bus.publish(event)
```

### Event Flow Diagram

```
Current Architecture:
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Exchange   │────▶│     API      │────▶│   Domain     │
│  (Backpack)  │     │  (bp_api.py) │     │   Service    │
└──────────────┘     └──────────────┘     └──────┬───────┘
                           │                      │
                      Returns data           Publishes event
                           │                      │
                           ▼                      ▼
                     ┌──────────────┐      ┌──────────────┐
                     │    Result    │      │ DomainEvent  │
                     │    (data)    │      │   (event)    │
                     └──────────────┘      └──────────────┘
```

## Impact on Migration Strategy

### What This Means

1. **Zero API Changes Required**
   - APIs continue returning data as they do now
   - No modifications to `bp_api.py` or `hl_api.py`
   - No risk to exchange integrations

2. **Only Domain Services Change**
   - Replace DomainEvent with msgspec events in services
   - Add event handlers to consume new events
   - Gradual migration possible

3. **Simpler Migration Path**
   - No coordination with API team needed
   - No risk of breaking exchange connections
   - Can test with domain logic only

### Migration Phases

#### Phase 1: Add Infrastructure
- Create msgspec event structures
- Add MsgspecEventBus
- Add event handlers in domains
- **APIs: No changes**

#### Phase 2: Dual Publishing
```python
# In TradingService (domain layer)
async def execute_order(self, signal: TradeSignal):
    result = await self.api.place_order(...)  # API unchanged

    # Old event (for compatibility)
    old_event = DomainEvent(...)
    await self.old_bus.publish(old_event)

    # New msgspec event
    new_event = OrderEvent(...)
    await self.new_bus.publish(new_event)
```

#### Phase 3: Remove Old System
- Stop publishing DomainEvent
- Remove old EventBus
- Delete DomainEvent class
- **APIs: Still no changes**

## Common Misconceptions

### Misconception 1: "APIs publish events"
**Reality:** APIs are pure data providers. They fetch/send data to exchanges and return results.

### Misconception 2: "WebSocket handlers are in APIs"
**Reality:** WebSocket handlers might emit events, but they're separate from the REST API layer.

### Misconception 3: "Migration requires API changes"
**Reality:** Zero API changes needed. Migration only affects domain services.

## Verification

You can verify this by searching the codebase:

```bash
# Check if APIs import EventBus or DomainEvent
grep -r "EventBus\|DomainEvent" cyberdelta/apis/

# Result: No matches in API layer

# Check where events are published
grep -r "event_bus.publish" cyberdelta/

# Result: Only in domain services like TradingService
```

## Benefits of This Architecture

1. **Clean Separation of Concerns**
   - APIs: Handle exchange communication
   - Domain Services: Handle business logic and events
   - Event Bus: Distributes events to consumers

2. **Easier Testing**
   - Can test APIs without event infrastructure
   - Can test domain logic with mock APIs
   - Can test event flow independently

3. **Flexible Migration**
   - Change event system without touching APIs
   - Roll back if needed without API impact
   - Test new events in isolation

## Summary

The fact that APIs don't publish events is a **major advantage** for migration:
- ✅ No API modifications needed
- ✅ No risk to exchange integrations
- ✅ Simpler migration path
- ✅ Can focus solely on domain layer
- ✅ Rollback doesn't affect APIs

This architectural decision makes the migration from DomainEvent to msgspec significantly safer and easier to implement.

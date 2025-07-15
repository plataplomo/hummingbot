# WebSocket Memory Optimization Guide

This document explains how to use memory optimization features in the CyberDelta WebSocket infrastructure for high-frequency trading scenarios.

## Overview

The WebSocket memory optimization system provides significant performance improvements for high-volume trading applications through:

- **Memory Pool Allocation**: Reduces garbage collection pressure
- **Optimized Pydantic Models**: Minimal validation overhead
- **Performance Mode Presets**: Pre-configured optimization profiles
- **Runtime Controls**: Dynamic optimization enabling/disabling

## Performance Modes

### 1. STANDARD Mode (Default)
**Best for**: Development, testing, low-volume trading

```python
from cyberdelta.apis.base.ws_router_factory import create_standard_router

config = create_standard_router(
    exchange_name="backpack",
    exchange_type=ExchangeType.BACKPACK,
    error_handler=error_handler,
)
```

**Characteristics**:
- Memory usage: Low
- Pool size: 500 (disabled)
- GC pressure: Low
- Latency: Standard
- Memory monitoring: 50MB warning / 100MB critical

### 2. HIGH_FREQUENCY Mode
**Best for**: Algorithmic trading, 1000+ messages/sec

```python
from cyberdelta.apis.base.ws_router_factory import create_high_frequency_router

config = create_high_frequency_router(
    exchange_name="hyperliquid",
    exchange_type=ExchangeType.HYPERLIQUID,
    error_handler=error_handler,
    message_rate_per_second=2000,  # Optional: scales pool size
)
```

**Characteristics**:
- Memory usage: Medium-high (+50-100%)
- Pool size: 2000 (enabled)
- GC pressure: -40% reduction
- Latency: -20-30% improvement
- Throughput: +100-200% improvement
- Memory monitoring: 200MB warning / 500MB critical

### 3. ULTRA_LOW_LATENCY Mode
**Best for**: Market making, sub-millisecond requirements

```python
from cyberdelta.apis.base.ws_router_factory import create_ultra_low_latency_router

config = create_ultra_low_latency_router(
    exchange_name="backpack",
    exchange_type=ExchangeType.BACKPACK,
    error_handler=error_handler,
)
```

**Characteristics**:
- Memory usage: High (+100-200%)
- Pool size: 5000 (enabled)
- GC pressure: -60-80% reduction
- Latency: -50-70% improvement
- Throughput: +300-500% improvement
- Memory monitoring: 500MB warning / 1000MB critical

### 4. MEMORY_OPTIMIZED Mode
**Best for**: Memory-constrained environments, embedded systems

```python
from cyberdelta.apis.base.ws_router_factory import create_memory_optimized_router

config = create_memory_optimized_router(
    exchange_name="hyperliquid",
    exchange_type=ExchangeType.HYPERLIQUID,
    error_handler=error_handler,
    memory_limit_mb=64,  # Optional: adjusts pool size
)
```

**Characteristics**:
- Memory usage: Very low (-30-50%)
- Pool size: 200 (enabled)
- GC pressure: -20-40% reduction
- Latency: Slight increase (+5-10%)
- Memory monitoring: 25MB warning / 50MB critical

## Auto-Configuration

Let the system choose the optimal mode based on your requirements:

```python
from cyberdelta.apis.base.ws_router_factory import auto_configure_router

config = auto_configure_router(
    exchange_name="backpack",
    exchange_type=ExchangeType.BACKPACK,
    error_handler=error_handler,
    message_rate_per_second=3000,    # High-frequency scenario
    memory_limit_mb=None,            # No memory constraints
    latency_requirement_ms=0.5,      # Sub-millisecond requirement
)
# Returns: ULTRA_LOW_LATENCY configuration
```

**Auto-selection rules**:
- Memory < 100MB → `MEMORY_OPTIMIZED`
- Latency < 1ms → `ULTRA_LOW_LATENCY`
- Rate > 1000 msg/sec → `HIGH_FREQUENCY`
- Otherwise → `STANDARD`

## Creating Router Instances

### Using Factory Functions

```python
from cyberdelta.apis.base.ws_router_factory import create_high_frequency_router
from cyberdelta.apis.backpack.bp_ws_router import BackpackWebSocketRouter

# Create configuration
config = create_high_frequency_router(
    exchange_name="backpack",
    exchange_type=ExchangeType.BACKPACK,
    error_handler=error_handler,
    envelope_validator=BackpackRawWebSocketEnvelope.model_validate,
)

# Get router initialization arguments
router_kwargs = config.get_router_kwargs()

# Create router with optimized configuration
router = BackpackWebSocketRouter(**router_kwargs)
```

### Manual Configuration

```python
from cyberdelta.apis.base.ws_router import BaseWebSocketRouter

router = BackpackWebSocketRouter(
    exchange_name="backpack",
    exchange_type=ExchangeType.BACKPACK,
    error_handler=error_handler,
    envelope_validator=BackpackRawWebSocketEnvelope.model_validate,
    enable_memory_optimization=True,
    memory_pool_size=2000,
)
```

## Runtime Optimization Controls

### Enable High-Frequency Mode at Runtime

```python
# Enable memory optimization during runtime
success = router.enable_high_frequency_mode()
if success:
    print("High-frequency mode enabled")
else:
    print("Already enabled")
```

### Disable Memory Optimization

```python
# Disable and clear memory pools
success = router.disable_memory_optimization()
if success:
    print("Memory optimization disabled")
```

### Check Current Status

```python
# Get comprehensive statistics
stats = router.get_comprehensive_stats()
print(f"Memory optimization: {stats.get('memory_optimization', 'disabled')}")

# Get memory-specific statistics
memory_stats = router.get_memory_stats()
if memory_stats:
    print(f"Pool hit rate: {memory_stats['pool_hit_rate']:.1f}%")
    print(f"Objects allocated: {memory_stats['allocated']}")
```

## Memory Pool Usage

### Direct Pool Access

```python
from cyberdelta.apis.base.ws_memory_optimized import (
    MemoryPool,
    create_memory_optimized_envelope,
    get_memory_usage_stats,
)

# Create memory pool
pool = MemoryPool(pool_size=1000)

# Create optimized envelopes
backpack_data = {
    "stream": "ticker.BTC_USDC",
    "data": {"price": "65000.50", "volume": "1.234"},
}

envelope = create_memory_optimized_envelope(backpack_data, "backpack")
print(f"Routing key: {envelope.routing_key}")
print(f"Symbol: {envelope.symbol}")

# Get system statistics
stats = get_memory_usage_stats()
print(f"Memory pool stats: {stats['memory_pool_stats']}")
```

### Context Creation

```python
from cyberdelta.apis.base.ws_memory_optimized import MemoryOptimizedMessageContext

# Create memory-optimized context
context = MemoryOptimizedMessageContext(
    envelope_type="backpack_optimized",
    routing_key="ticker",
    message_id="msg_123",
    connection_id="conn_456",
    symbol="BTC_USDC",
)

print(f"Is private: {context.is_private}")
print(f"Priority: {context.priority}")
```

## Performance Monitoring

### Memory Statistics

```python
# Get comprehensive memory statistics
stats = get_memory_usage_stats()

print("Memory Pool Performance:")
print(f"  Pool hit rate: {stats['memory_pool_stats']['pool_hit_rate']:.1f}%")
print(f"  Objects allocated: {stats['memory_pool_stats']['allocated']}")
print(f"  Objects reused: {stats['memory_pool_stats']['reused']}")

print("System Information:")
print(f"  Python version: {stats['system_info']['python_version']}")
print(f"  Memory pool enabled: {stats['system_info']['memory_pool_enabled']}")

print("Optimization Features:")
for feature, enabled in stats['optimization_features'].items():
    print(f"  {feature}: {enabled}")
```

### Router Health Monitoring

```python
# Monitor router performance
health_stats = router.get_connection_health()
if health_stats:
    print(f"Connection state: {health_stats['state']}")
    print(f"Last successful operation: {health_stats['last_successful_operation']}")

# Monitor error recovery
recovery_stats = router.get_recovery_stats()
if recovery_stats:
    print(f"Reconnection attempts: {recovery_stats['reconnection_attempts']}")
    print(f"Messages replayed: {recovery_stats['messages_replayed']}")
```

## Best Practices

### 1. Choose the Right Mode

```python
# For development and testing
config = create_standard_router(...)

# For production with moderate load
config = create_high_frequency_router(..., message_rate_per_second=1500)

# For ultra-fast trading
config = create_ultra_low_latency_router(...)

# For resource-constrained environments
config = create_memory_optimized_router(..., memory_limit_mb=128)
```

### 2. Monitor Performance

```python
import time

# Benchmark envelope creation
start_time = time.perf_counter()
for i in range(10000):
    envelope = create_memory_optimized_envelope(data, "backpack")
end_time = time.perf_counter()

avg_time_us = ((end_time - start_time) * 1000000) / 10000
print(f"Average creation time: {avg_time_us:.2f} microseconds")
```

### 3. Handle Memory Warnings

```python
# Monitor memory usage
def check_memory_usage(router):
    stats = router.get_memory_stats()
    if stats:
        # Implement your memory monitoring logic
        if stats.get('memory_warning_triggered'):
            logger.warning("Memory usage approaching threshold")
```

### 4. Production Deployment

```python
# Recommended production configuration
config = create_high_frequency_router(
    exchange_name="production_exchange",
    exchange_type=ExchangeType.BACKPACK,
    error_handler=production_error_handler,
    envelope_validator=BackpackRawWebSocketEnvelope.model_validate,
    message_rate_per_second=2000,
)

router_kwargs = config.get_router_kwargs()
router = BackpackWebSocketRouter(**router_kwargs)

# Enable error recovery with connection adapter
await router.start_error_recovery(connection_adapter)
```

## Troubleshooting

### Common Issues

1. **Memory pool disabled by default**
   ```python
   # Solution: Use factory functions or enable explicitly
   router.enable_high_frequency_mode()
   ```

2. **Pool size too small for high-frequency scenarios**
   ```python
   # Solution: Use message rate for auto-sizing
   config = create_high_frequency_router(..., message_rate_per_second=5000)
   ```

3. **Memory usage too high**
   ```python
   # Solution: Use memory-optimized mode
   config = create_memory_optimized_router(..., memory_limit_mb=64)
   ```

### Performance Debugging

```python
# Enable detailed logging for memory optimization
import logging
logging.getLogger("cyberdelta.apis.base.ws_memory_optimized").setLevel(logging.DEBUG)

# Run benchmarks
if __name__ == "__main__":
    from cyberdelta.apis.base.ws_memory_optimized import benchmark_memory_optimization
    results = benchmark_memory_optimization(iterations=10000)
    print(f"Performance improvement: {results['improvement_percentage']:.1f}%")
```

## Integration Examples

See the complete example in `examples/memory_optimization_example.py` for a full demonstration of all memory optimization features.
# WebSocket Troubleshooting Guide

## Overview

This guide helps diagnose and resolve common issues with the CyberDeltaEngine WebSocket system. It covers connection problems, message processing errors, performance issues, and monitoring setup.

## Quick Diagnosis Checklist

Before diving into specific troubleshooting steps, run through this quick checklist:

- [ ] Check WebSocket connection status
- [ ] Verify message format against exchange documentation
- [ ] Review error logs for validation failures
- [ ] Check rate limiting violations
- [ ] Verify handler registration
- [ ] Monitor processing metrics

## Connection Issues

### Connection Fails to Establish

**Symptoms:**
```
ERROR: WebSocket connection failed
ERROR: Connection refused to wss://api.exchange.com/ws
```

**Diagnostic Steps:**

1. **Test Network Connectivity:**
```bash
# Test basic connectivity
curl -I https://api.exchange.com
ping api.exchange.com

# Test WebSocket endpoint
wscat -c wss://api.exchange.com/ws
```

2. **Check SSL/TLS Issues:**
```python
import ssl
import websockets

# Test with different SSL contexts
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

async with websockets.connect(uri, ssl=ssl_context) as ws:
    print("Connected with relaxed SSL")
```

3. **Verify Authentication:**
```python
# Check if authentication is required
headers = {
    "Authorization": f"Bearer {api_key}",
    "X-API-Key": api_key
}

await websockets.connect(uri, extra_headers=headers)
```

**Common Solutions:**
- Verify API credentials and permissions
- Check firewall/proxy settings
- Validate WebSocket URL format
- Ensure SSL certificates are valid
- Check for IP restrictions on exchange side

### Connection Drops Frequently

**Symptoms:**
```
INFO: WebSocket connection lost
INFO: Reconnecting in 2.0 seconds (attempt 3/10)
```

**Diagnostic Steps:**

1. **Check Connection Health:**
```python
# Enable detailed connection logging
import logging
logging.getLogger("websockets").setLevel(logging.DEBUG)
logging.getLogger("cyberdelta.apis.base").setLevel(logging.DEBUG)
```

2. **Monitor Connection Metrics:**
```python
# Check connection duration and failure patterns
health = error_recovery.get_health_status()
print(f"Total reconnections: {health.total_reconnections}")
print(f"Consecutive failures: {health.consecutive_failures}")
print(f"Uptime: {health.uptime_seconds}s")
```

3. **Review Error Recovery Settings:**
```python
# Adjust backoff configuration
config = ErrorRecoveryConfig(
    backoff=BackoffConfig(
        initial_delay=1.0,      # Start with 1 second delay
        max_delay=60.0,         # Max 1 minute delay
        multiplier=1.5,         # Slower exponential growth
        max_retries=20          # More retry attempts
    )
)
```

**Common Solutions:**
- Increase connection timeout settings
- Implement proper heartbeat/ping mechanism
- Check network stability
- Review exchange connection limits
- Adjust error recovery parameters

### Circuit Breaker Opens Frequently

**Symptoms:**
```
WARNING: Circuit breaker opened (failures: 5)
INFO: Circuit breaker state: OPEN
```

**Diagnostic Steps:**

1. **Review Circuit Breaker Configuration:**
```python
# Check circuit breaker settings
cb_config = CircuitBreakerConfig(
    failure_threshold=10,       # Increase threshold
    success_threshold=3,        # Require more successes
    timeout_seconds=120.0       # Longer recovery time
)
```

2. **Analyze Failure Patterns:**
```python
# Get recovery statistics
stats = error_recovery.get_recovery_stats()
print("Recent events:")
for event in stats["recent_events"]:
    print(f"  {event['event_type']}: {event['success']} - {event['error_details']}")
```

**Common Solutions:**
- Adjust circuit breaker thresholds
- Fix underlying connection issues
- Implement proper error handling
- Add retry logic for transient errors

## Message Processing Issues

### ValidationError: Pydantic Model Validation Failed

**Symptoms:**
```
ERROR: ValidationError: 2 validation errors for DepthUpdate
price -> field required
quantity -> ensure this value is greater than 0
```

**Diagnostic Steps:**

1. **Enable Raw Message Logging:**
```python
# Log raw messages before validation
class DebugProcessor(PydanticWebSocketProcessor):
    async def process(self, payload, handler, context=None):
        self.logger.info("raw_payload", payload=payload)
        return await super().process(payload, handler, context)
```

2. **Compare Against Exchange Documentation:**
```python
# Print message structure for comparison
def debug_message_structure(message):
    def print_structure(obj, indent=0):
        if isinstance(obj, dict):
            for key, value in obj.items():
                print("  " * indent + f"{key}: {type(value).__name__}")
                if isinstance(value, (dict, list)) and indent < 3:
                    print_structure(value, indent + 1)
        elif isinstance(obj, list) and obj:
            print("  " * indent + f"[{len(obj)} items of {type(obj[0]).__name__}]")
    
    print_structure(message)
```

3. **Test Model Validation Manually:**
```python
# Test validation with actual message data
from pydantic import ValidationError

try:
    model = YourMessageModel.model_validate(raw_message)
    print("Validation successful!")
except ValidationError as e:
    print("Validation errors:")
    for error in e.errors():
        print(f"  {error['loc']}: {error['msg']}")
```

**Common Solutions:**
- Update model fields to match exchange format
- Add field validators for complex validation
- Handle optional fields with default values
- Use field aliases for naming mismatches

### UnroutableMessage: No Processor Found

**Symptoms:**
```
WARNING: No processor found for routing key: 'unknown_channel'
WARNING: Unroutable message received
```

**Diagnostic Steps:**

1. **Debug Routing Key Extraction:**
```python
class DebugRouter(BaseWebSocketRouter):
    def _extract_routing_key(self, message):
        key = super()._extract_routing_key(message)
        self.logger.debug("routing_debug", 
                         extracted_key=key, 
                         available_processors=list(self.processors.keys()),
                         message_keys=list(message.keys()))
        return key
```

2. **Check Processor Registration:**
```python
# List registered processors
router_info = router.get_processor_info()
print("Registered processors:")
for key, info in router_info["processors"].items():
    print(f"  {key}: {info['type']}")
```

3. **Analyze Message Format:**
```python
# Check message routing fields
def analyze_routing(message):
    potential_keys = []
    for key in ["channel", "topic", "type", "stream", "method"]:
        if key in message:
            potential_keys.append((key, message[key]))
    print(f"Potential routing keys: {potential_keys}")
```

**Common Solutions:**
- Fix routing key extraction logic
- Register missing processors
- Handle unknown message types gracefully
- Add fallback processors for unknown channels

### TransformationError: Domain Model Creation Failed

**Symptoms:**
```
ERROR: Transformation failed: KeyError: 'required_field'
ERROR: Processing error in stage: transformation
```

**Diagnostic Steps:**

1. **Add Transformation Debugging:**
```python
class DebugTransformer:
    def transform(self, validated):
        try:
            result = self._actual_transform(validated)
            logger.debug("transformation_success", 
                        input_type=type(validated).__name__,
                        output_type=type(result).__name__)
            return result
        except Exception as e:
            logger.error("transformation_error",
                        error=str(e),
                        validated_data=validated.model_dump())
            raise
```

2. **Test Transformation Independently:**
```python
# Test transformer with known good data
transformer = YourTransformer()
test_model = YourValidatedModel(**test_data)
try:
    domain_model = transformer.transform(test_model)
    print("Transformation successful!")
except Exception as e:
    print(f"Transformation failed: {e}")
```

**Common Solutions:**
- Handle missing fields in transformer logic
- Add null checks for optional fields
- Validate domain model after transformation
- Use defensive programming techniques

### HandlerError: Message Handler Failed

**Symptoms:**
```
ERROR: Handler error: DatabaseError: Connection lost
ERROR: Processing error in stage: handler_invocation
```

**Diagnostic Steps:**

1. **Add Handler Error Logging:**
```python
async def robust_handler(domain_model, original_message):
    try:
        await your_actual_handler(domain_model, original_message)
    except Exception as e:
        logger.exception("handler_error", 
                        error=str(e),
                        domain_model=domain_model,
                        message_type=original_message.get("channel"))
        # Don't re-raise to prevent processor failure
```

2. **Test Handler Independently:**
```python
# Test handler with mock data
test_domain_model = {"symbol": "BTCUSD", "price": 50000}
test_original = {"channel": "depth", "timestamp": 1640995200}

await your_handler(test_domain_model, test_original)
```

**Common Solutions:**
- Add proper error handling in handlers
- Implement retry logic for transient errors
- Use database connection pooling
- Handle handler failures gracefully

## Performance Issues

### High Processing Latency

**Symptoms:**
```
WARN: High processing latency detected: 150ms average
INFO: Processing time histogram: p95=200ms, p99=500ms
```

**Diagnostic Steps:**

1. **Profile Processing Pipeline:**
```python
import time

class ProfilingProcessor(PydanticWebSocketProcessor):
    async def process(self, payload, handler, context=None):
        start_time = time.perf_counter()
        
        # Validation timing
        validation_start = time.perf_counter()
        validated = self.raw_model.model_validate(payload)
        validation_time = time.perf_counter() - validation_start
        
        # Transformation timing
        transform_start = time.perf_counter()
        domain_model = self.transformer.transform(validated)
        transform_time = time.perf_counter() - transform_start
        
        # Handler timing
        handler_start = time.perf_counter()
        await handler(domain_model.model_dump(), context.get("original_message", {}))
        handler_time = time.perf_counter() - handler_start
        
        total_time = time.perf_counter() - start_time
        
        self.logger.info("processing_profile",
                        total_ms=total_time * 1000,
                        validation_ms=validation_time * 1000,
                        transform_ms=transform_time * 1000,
                        handler_ms=handler_time * 1000)
```

2. **Check Message Size Distribution:**
```python
# Monitor message sizes
def analyze_message_sizes(messages):
    sizes = [len(json.dumps(msg)) for msg in messages]
    print(f"Average size: {sum(sizes) / len(sizes):.0f} bytes")
    print(f"Max size: {max(sizes)} bytes")
    print(f"Large messages (>10KB): {sum(1 for s in sizes if s > 10000)}")
```

**Common Solutions:**
- Optimize database queries in handlers
- Use connection pooling
- Implement message batching
- Profile and optimize hot code paths
- Consider async optimizations

### Low Message Throughput

**Symptoms:**
```
INFO: Current throughput: 500 msg/sec (target: 5000 msg/sec)
WARN: Message queue backing up
```

**Diagnostic Steps:**

1. **Identify Bottlenecks:**
```python
# Monitor queue depths and processing rates
import asyncio

class ThroughputMonitor:
    def __init__(self):
        self.processed_count = 0
        self.start_time = time.time()
        
    async def monitor_throughput(self):
        while True:
            await asyncio.sleep(10)  # Report every 10 seconds
            elapsed = time.time() - self.start_time
            rate = self.processed_count / elapsed
            print(f"Current rate: {rate:.1f} msg/sec")
```

2. **Test with Synthetic Load:**
```python
async def throughput_test():
    """Test processing throughput with synthetic messages."""
    router = create_test_router()
    handler = lambda d, o: None  # No-op handler
    handlers = {"test": handler}
    
    message_count = 10000
    start_time = time.time()
    
    tasks = []
    for i in range(message_count):
        message = {"channel": "test", "data": f"message_{i}"}
        task = router.route_message(message, handlers)
        tasks.append(task)
    
    await asyncio.gather(*tasks)
    
    duration = time.time() - start_time
    throughput = message_count / duration
    print(f"Throughput: {throughput:.1f} msg/sec")
```

**Common Solutions:**
- Use async/await properly
- Implement connection pooling
- Optimize JSON parsing (orjson)
- Use batching for database operations
- Scale horizontally if needed

### Memory Usage Growing

**Symptoms:**
```
WARN: Memory usage increasing: 1.2GB -> 1.8GB
ERROR: Out of memory error
```

**Diagnostic Steps:**

1. **Monitor Memory Usage:**
```python
import psutil
import gc

def monitor_memory():
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    print(f"Memory usage: {memory_mb:.1f} MB")
    
    # Force garbage collection
    collected = gc.collect()
    print(f"Garbage collected: {collected} objects")
```

2. **Check Buffer Sizes:**
```python
# Monitor message buffer sizes
def check_buffer_sizes(error_recovery):
    stats = error_recovery.message_buffer.get_stats()
    print(f"Pending replay: {stats['pending_replay']}")
    print(f"Sent messages: {stats['sent_messages']}")
    print(f"Capacity: {stats['max_capacity']}")
```

**Common Solutions:**
- Implement buffer size limits
- Add periodic cleanup tasks
- Fix memory leaks in handlers
- Use streaming processing for large datasets
- Implement back-pressure mechanisms

## Rate Limiting Issues

### Rate Limit Violations

**Symptoms:**
```
WARN: Rate limit violation: global limit exceeded
WARN: Request denied: retry after 5.2 seconds
```

**Diagnostic Steps:**

1. **Check Rate Limiter Configuration:**
```python
# Review rate limiting settings
rate_limiter_stats = rate_limiter.get_stats()
print("Rate limiter configuration:")
for limiter_key, details in rate_limiter_stats["limiter_details"].items():
    print(f"  {limiter_key}: {details}")
```

2. **Monitor Request Patterns:**
```python
# Track request rates by connection/type
class RequestTracker:
    def __init__(self):
        self.request_counts = defaultdict(int)
        self.start_time = time.time()
    
    def track_request(self, connection_id, message_type):
        key = f"{connection_id}:{message_type}"
        self.request_counts[key] += 1
        
    def get_rates(self):
        elapsed = time.time() - self.start_time
        return {key: count/elapsed for key, count in self.request_counts.items()}
```

**Common Solutions:**
- Adjust rate limiting thresholds
- Implement request queuing
- Add retry logic with exponential backoff
- Distribute requests across connections
- Implement client-side rate limiting

## Monitoring and Alerting Setup

### Setting Up Comprehensive Monitoring

1. **Enable Telemetry:**
```python
from cyberdelta.apis.base.ws_telemetry import TelemetryConfig, get_telemetry_manager

config = TelemetryConfig(
    service_name="cyberdelta-websocket",
    trace_enabled=True,
    metrics_enabled=True,
    export_endpoint="http://jaeger:14268/api/traces"
)

telemetry_manager = get_telemetry_manager(config)
```

2. **Configure Prometheus Metrics:**
```python
# Export metrics endpoint
from prometheus_client import start_http_server, generate_latest

def setup_metrics_endpoint():
    start_http_server(8000)  # Metrics available at :8000/metrics
    
# Custom metrics dashboard
def export_custom_metrics():
    metrics_text = metrics_collector.export_prometheus()
    return metrics_text
```

3. **Set Up Alerting Rules:**
```yaml
# prometheus.yml alerting rules
groups:
- name: websocket_alerts
  rules:
  - alert: HighErrorRate
    expr: websocket_errors_total / websocket_messages_total > 0.01
    for: 5m
    annotations:
      summary: "High WebSocket error rate detected"
      
  - alert: ConnectionFailures
    expr: increase(websocket_connection_failures_total[5m]) > 10
    for: 1m
    annotations:
      summary: "Multiple WebSocket connection failures"
```

### Health Check Endpoints

```python
from fastapi import FastAPI
from cyberdelta.apis.base.ws_error_recovery import ConnectionState

app = FastAPI()

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    health_status = {}
    
    for exchange, recovery in recovery_systems.items():
        health = recovery.get_health_status()
        health_status[exchange] = {
            "status": health.state,
            "healthy": health.state == ConnectionState.CONNECTED,
            "uptime": health.uptime_seconds,
            "reconnections": health.total_reconnections
        }
    
    overall_healthy = all(status["healthy"] for status in health_status.values())
    
    return {
        "status": "healthy" if overall_healthy else "degraded",
        "exchanges": health_status,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/metrics")
async def metrics_endpoint():
    """Prometheus metrics endpoint."""
    metrics_text = ""
    for collector in metrics_collectors.values():
        metrics_text += collector.export_prometheus() + "\n"
    return metrics_text
```

## Log Analysis

### Important Log Patterns

1. **Connection Issues:**
```
# Search for connection problems
grep -E "(connection_error|reconnection_|circuit_breaker)" websocket.log

# Connection success rate
grep "connection_" websocket.log | grep -c "success"
grep "connection_" websocket.log | grep -c "failed"
```

2. **Message Processing Errors:**
```
# Find validation errors
grep "ValidationError" websocket.log | head -10

# Processing performance
grep "processing_time" websocket.log | awk '{print $NF}' | sort -n
```

3. **Rate Limiting:**
```
# Rate limit violations
grep "rate_limit" websocket.log | grep "violation"

# Circuit breaker events
grep "circuit_breaker" websocket.log
```

### Log Aggregation with ELK Stack

```yaml
# logstash.conf
input {
  file {
    path => "/var/log/cyberdelta/websocket.log"
    type => "websocket"
    codec => "json"
  }
}

filter {
  if [type] == "websocket" {
    mutate {
      add_field => { "component" => "websocket" }
    }
    
    if [level] == "ERROR" {
      mutate {
        add_tag => [ "error" ]
      }
    }
  }
}

output {
  elasticsearch {
    hosts => ["elasticsearch:9200"]
    index => "cyberdelta-websocket-%{+YYYY.MM.dd}"
  }
}
```

This troubleshooting guide provides comprehensive diagnostic procedures and solutions for common WebSocket issues. Regular monitoring and proactive maintenance using these techniques will help maintain system reliability and performance.
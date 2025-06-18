# How to Verify Connection Pooling in Production

## 1. Add Connection Monitoring to HttpClient

First, let's add logging to track connection pool usage:

```python
# In http_client.py - Add this method to HttpClient class
async def get_connection_stats(self) -> dict[str, Any]:
    """Get current connection pool statistics."""
    session = await self._get_session()
    if session and session.connector:
        connector = session.connector
        stats = {
            "total_connections": len(connector._conns) if hasattr(connector, '_conns') else 0,
            "available_connections": len(connector._available_connections) if hasattr(connector, '_available_connections') else 0,
            "acquired_connections": len(connector._acquired) if hasattr(connector, '_acquired') else 0,
            "acquired_per_host": {str(key): len(conns) for key, conns in connector._acquired_per_host.items()} if hasattr(connector, '_acquired_per_host') else {},
            "limit": connector.limit,
            "limit_per_host": connector.limit_per_host,
            "keepalive_timeout": connector._keepalive_timeout,
        }
        return stats
    return {"error": "No active session or connector"}

# Add logging to _execute_single_request
async def _execute_single_request(self, ...):
    session = await self._get_session()
    
    # Log connection stats before request
    if logger.isEnabledFor(logging.DEBUG):
        stats = await self.get_connection_stats()
        logger.debug(f"[{self.exchange_name}] Connection pool stats BEFORE request: {stats}")
    
    # ... existing request code ...
    
    # Log connection stats after request
    if logger.isEnabledFor(logging.DEBUG):
        stats = await self.get_connection_stats()
        logger.debug(f"[{self.exchange_name}] Connection pool stats AFTER request: {stats}")
```

## 2. Production Verification Script

Create a simple script to verify connection pooling:

```python
# verify_connection_pooling.py
import asyncio
import time
import logging
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config.config_loader import load_config

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def verify_connection_pooling():
    """Verify that connections are being reused."""
    # Load production config
    config = load_config("production")
    
    # Create single API instance
    api = HyperliquidAPI(
        exchange_config=config.exchanges.hyperliquid,
        exchange_secrets=config.secrets.hyperliquid
    )
    
    try:
        # Make multiple requests and time them
        timings = []
        
        for i in range(5):
            start = time.time()
            
            # Make a simple API call (e.g., get account info)
            await api.account.get_account_summary()
            
            elapsed = time.time() - start
            timings.append(elapsed)
            
            logger.info(f"Request {i+1}: {elapsed:.3f}s")
            
            # Get connection stats
            if hasattr(api._http_client, 'get_connection_stats'):
                stats = await api._http_client.get_connection_stats()
                logger.info(f"Connection stats: {stats}")
            
            # Small delay between requests
            await asyncio.sleep(0.1)
        
        # Analyze results
        logger.info("\n=== Connection Pooling Analysis ===")
        logger.info(f"First request: {timings[0]:.3f}s (includes connection setup)")
        logger.info(f"Subsequent avg: {sum(timings[1:])/len(timings[1:]):.3f}s")
        
        if timings[0] > timings[1] * 1.5:
            logger.info("✅ Connection pooling is working! First request slower than subsequent.")
        else:
            logger.warning("⚠️ Connection pooling may not be working effectively.")
            
    finally:
        # Clean up
        if hasattr(api._http_client, 'close_session'):
            await api._http_client.close_session()

if __name__ == "__main__":
    asyncio.run(verify_connection_pooling())
```

## 3. Network-Level Verification

Use `tcpdump` or `wireshark` to verify connection reuse:

```bash
# Monitor HTTPS connections to Hyperliquid
sudo tcpdump -i any -n host api.hyperliquid.xyz and port 443 -w hl_connections.pcap

# In another terminal, run your application
python your_app.py

# Analyze the capture
tcpdump -r hl_connections.pcap | grep "SYN"
# Should see only 1-2 SYN packets (new connections) even for many requests
```

## 4. Application Metrics

Add metrics to your production app:

```python
# In your main application
class ConnectionMetrics:
    def __init__(self):
        self.request_count = 0
        self.connection_created_count = 0
        self.request_timings = []
    
    async def track_request(self, api_client, operation):
        start = time.time()
        
        # Check if new connection was created
        initial_stats = await api_client._http_client.get_connection_stats()
        
        # Perform operation
        result = await operation()
        
        # Check stats after
        final_stats = await api_client._http_client.get_connection_stats()
        
        elapsed = time.time() - start
        self.request_count += 1
        self.request_timings.append(elapsed)
        
        if final_stats['total_connections'] > initial_stats['total_connections']:
            self.connection_created_count += 1
        
        # Log metrics every 100 requests
        if self.request_count % 100 == 0:
            avg_time = sum(self.request_timings[-100:]) / 100
            logger.info(f"""
            === Connection Pool Metrics ===
            Total Requests: {self.request_count}
            New Connections: {self.connection_created_count}
            Connection Reuse Rate: {(1 - self.connection_created_count/self.request_count)*100:.1f}%
            Avg Request Time (last 100): {avg_time:.3f}s
            """)
        
        return result
```

## 5. Signs Connection Pooling is Working

### ✅ Good Signs:
1. **First request slower** (500-1000ms) than subsequent requests (100-300ms)
2. **Connection reuse rate > 95%** after warmup
3. **Consistent low latency** after first few requests
4. **Few TCP SYN packets** in network capture
5. **`total_connections` stays stable** in stats

### ❌ Bad Signs:
1. **All requests take similar time** (400-600ms each)
2. **New connection for each request** (connection count keeps increasing)
3. **High variation in request times**
4. **Many TCP SYN packets** for each request

## 6. Production Best Practices

### 1. Single Long-Lived API Instance
```python
# Good - Application level
class TradingApp:
    def __init__(self):
        self.api = HyperliquidAPI(config)  # Create once
    
    async def place_order(self, ...):
        return await self.api.trading.place_order(...)  # Reuse

# Bad - Creating new instance per request
async def handle_request():
    api = HyperliquidAPI(config)  # New instance each time!
    return await api.trading.place_order(...)
```

### 2. Graceful Shutdown
```python
# Ensure connections are closed properly
async def shutdown():
    await api._http_client.close_session()
```

### 3. Monitor Connection Health
```python
# Periodic health check
async def health_check():
    stats = await api._http_client.get_connection_stats()
    if stats['total_connections'] > 50:
        logger.warning("High connection count, possible leak")
    return stats
```

## 7. Quick Verification Commands

```bash
# Check established connections to Hyperliquid
netstat -an | grep "api.hyperliquid" | grep ESTABLISHED | wc -l

# Monitor connection creation in real-time
watch -n 1 'netstat -an | grep "api.hyperliquid" | grep -E "ESTABLISHED|TIME_WAIT" | wc -l'

# Check if keep-alive is working (should see keep-alive packets)
sudo tcpdump -i any -n host api.hyperliquid.xyz -A | grep -i keep-alive
```

## Summary

To ensure connection pooling works in production:

1. **Add monitoring code** to track connection statistics
2. **Use a single API instance** throughout your application lifecycle
3. **Monitor metrics** - first request should be slower, subsequent faster
4. **Check network level** - should see connection reuse, not new connections
5. **Set up alerts** for high connection counts or slow requests

The key indicator: **After the first request, subsequent requests should be 3-5x faster** if connection pooling is working correctly.
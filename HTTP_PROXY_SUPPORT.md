# HTTP Proxy Support for Hummingbot

## Overview

This implementation adds HTTP proxy support to Hummingbot, allowing all API requests to be routed through a proxy server. This is useful for:

- **Rate limit management** - Share API quota across multiple bot instances via a caching proxy
- **Network restrictions** - Access exchanges through corporate proxies
- **Monitoring & debugging** - Inspect API traffic using tools like mitmproxy
- **Caching** - Reduce API calls using a caching proxy layer

## How It Works

The implementation modifies three key components:

1. **`hummingbot/client/config/client_config_map.py`** - Adds proxy configuration to the client config
2. **`hummingbot/core/web_assistant/connections/connections_factory.py`** - Main connection factory used by most connectors
3. **`hummingbot/data_feed/data_feed_base.py`** - Data feed connections

When proxy is enabled in the client configuration, the code creates `ClientSession` objects with `trust_env=True`, which makes aiohttp respect standard proxy environment variables (`HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`).

## Configuration

### Method 1: Using Hummingbot Configuration (Recommended)

Configure proxy settings through the Hummingbot config command:

```
>>> config http_proxy_enabled
Would you like to enable HTTP proxy support? (True/False) >>> True

>>> config http_proxy_url
Enter HTTP proxy URL (e.g., http://localhost:8080) >>> http://localhost:8080

>>> config https_proxy_url
Enter HTTPS proxy URL (e.g., http://localhost:8080) >>> http://localhost:8080

>>> config no_proxy_hosts
Enter hosts to bypass proxy (comma-separated, e.g., localhost,127.0.0.1) >>> localhost,127.0.0.1
```

Or edit `conf/conf_client.yml` directly:

```yaml
http_proxy_enabled: true
http_proxy_url: "http://localhost:8080"
https_proxy_url: "http://localhost:8080"
no_proxy_hosts: "localhost,127.0.0.1"
```

### Method 2: Using Environment Variables

Set these environment variables before starting Hummingbot:

```bash
# Enable proxy support
export HUMMINGBOT_USE_PROXY=true

# Configure proxy server
export HTTP_PROXY=http://localhost:8080
export HTTPS_PROXY=http://localhost:8080

# Optional: Bypass proxy for specific hosts
export NO_PROXY=localhost,127.0.0.1,10.0.0.0/8

# Start Hummingbot
./start
```

### Docker Configuration

When using Docker, pass the environment variables:

```bash
docker run -it \
  -e HUMMINGBOT_USE_PROXY=true \
  -e HTTP_PROXY=http://host.docker.internal:8080 \
  -e HTTPS_PROXY=http://host.docker.internal:8080 \
  -e NO_PROXY=localhost,127.0.0.1 \
  hummingbot/hummingbot:latest
```

Or in `docker-compose.yml`:

```yaml
services:
  hummingbot:
    image: hummingbot/hummingbot:latest
    environment:
      - HUMMINGBOT_USE_PROXY=true
      - HTTP_PROXY=http://proxy:8080
      - HTTPS_PROXY=http://proxy:8080
      - NO_PROXY=localhost,127.0.0.1
```

## Testing

### 1. Test Script

Run the provided test script to verify proxy support:

```bash
# Without proxy
python test_proxy_support.py

# With proxy
HUMMINGBOT_USE_PROXY=true HTTP_PROXY=http://localhost:8080 python test_proxy_support.py
```

### 2. Using mitmproxy

Test with mitmproxy to inspect traffic:

```bash
# Terminal 1: Start mitmproxy
mitmproxy --listen-port 8080

# Terminal 2: Run Hummingbot with proxy
export HUMMINGBOT_USE_PROXY=true
export HTTP_PROXY=http://localhost:8080
export HTTPS_PROXY=http://localhost:8080
./start
```

You should see all API requests in the mitmproxy interface.

### 3. Using a Caching Proxy

For rate limit management, use a caching proxy like Squid:

```bash
# Install Squid
sudo apt-get install squid

# Configure Squid for caching (edit /etc/squid/squid.conf)
# Add these lines for API caching:
refresh_pattern ^https?://api\. 1 20% 60 override-expire ignore-reload ignore-no-store

# Start Squid
sudo systemctl start squid

# Run Hummingbot with Squid proxy
export HUMMINGBOT_USE_PROXY=true
export HTTP_PROXY=http://localhost:3128
export HTTPS_PROXY=http://localhost:3128
./start
```

## Verification

To verify proxy is working:

1. **Check proxy logs** - Your proxy server should show incoming requests from Hummingbot
2. **Monitor network traffic** - Use `tcpdump` or `wireshark` to confirm traffic goes to proxy, not directly to exchanges
3. **Test with httpbin** - The test script uses httpbin.org/ip to show your external IP (should be proxy's IP when enabled)

## Implementation Details

### Modified Files

1. **`client_config_map.py`** (lines 694-721):
   - Added `http_proxy_enabled` boolean field to enable/disable proxy
   - Added `http_proxy_url` and `https_proxy_url` fields for proxy URLs
   - Added `no_proxy_hosts` field for proxy bypass list
   - All fields include proper prompts for configuration UI

2. **`connections_factory.py`** (lines 50-91):
   ```python
   async def _get_shared_client(self) -> aiohttp.ClientSession:
       if self._shared_client is None:
           # Try to get proxy settings from client config first
           use_proxy = False
           proxy_from_config = False

           try:
               from hummingbot.client.config.config_helpers import load_client_config_map_from_file
               client_config = load_client_config_map_from_file()

               if hasattr(client_config, 'http_proxy_enabled') and client_config.http_proxy_enabled:
                   use_proxy = True
                   proxy_from_config = True
                   # Set environment variables from config
                   if client_config.http_proxy_url:
                       os.environ["HTTP_PROXY"] = client_config.http_proxy_url
                   # ... (similar for HTTPS_PROXY and NO_PROXY)
           except Exception:
               pass

           # Fall back to environment variable if config doesn't enable proxy
           if not proxy_from_config:
               use_proxy = os.getenv("HUMMINGBOT_USE_PROXY", "").lower() in ("true", "1", "yes")

           if use_proxy:
               self._shared_client = aiohttp.ClientSession(trust_env=True)
           else:
               self._shared_client = aiohttp.ClientSession()
       return self._shared_client
   ```

3. **`data_feed_base.py`** (lines 46-82, 100-136):
   - Similar modification to `_http_client()` method
   - Also updated `check_network()` method for consistency

### Why These Changes Work

- **Configuration-first approach** - Follows Hummingbot's configuration pattern using `ClientConfigMap`
- **Centralized connection creation** - Most Hummingbot connectors use `ConnectionsFactory` for creating HTTP sessions
- **`trust_env=True`** - This aiohttp parameter enables reading proxy settings from environment variables
- **Backward compatible** - Without enabling proxy in config, behavior is unchanged
- **Dual configuration** - Supports both config file and environment variables for flexibility
- **Standard proxy variables** - Uses industry-standard `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`

## Use Cases

### 1. Rate Limit Management with Caching Proxy

Deploy a caching proxy to share API quota across multiple bot instances:

```yaml
# docker-compose.yml
services:
  cache-proxy:
    image: cyberDeltaEngine/cache-proxy
    ports:
      - "8080:8080"
    environment:
      - CACHE_TTL_ORDERBOOK=500ms
      - CACHE_TTL_FUNDING=60s

  hummingbot-1:
    image: hummingbot/hummingbot
    environment:
      - HUMMINGBOT_USE_PROXY=true
      - HTTP_PROXY=http://cache-proxy:8080

  hummingbot-2:
    image: hummingbot/hummingbot
    environment:
      - HUMMINGBOT_USE_PROXY=true
      - HTTP_PROXY=http://cache-proxy:8080
```

### 2. Corporate Network Access

Access exchanges through corporate proxy:

```bash
export HUMMINGBOT_USE_PROXY=true
export HTTP_PROXY=http://corporate-proxy.company.com:8080
export HTTPS_PROXY=http://corporate-proxy.company.com:8080
export NO_PROXY=*.company.com,10.0.0.0/8
```

### 3. Development & Debugging

Use mitmproxy for API debugging:

```bash
# Start mitmproxy with API response modification
mitmdump -s modify_responses.py --listen-port 8080

# Run Hummingbot through proxy
export HUMMINGBOT_USE_PROXY=true
export HTTP_PROXY=http://localhost:8080
```

## Troubleshooting

### Proxy Not Working

1. **Check environment variables**:
   ```bash
   echo $HUMMINGBOT_USE_PROXY  # Should be "true"
   echo $HTTP_PROXY             # Should be your proxy URL
   ```

2. **Verify proxy is running**:
   ```bash
   curl -x http://localhost:8080 https://httpbin.org/ip
   ```

3. **Check proxy logs** for incoming connections

### SSL Certificate Errors

If using HTTPS proxy with self-signed certificates:

```bash
# Disable SSL verification (development only!)
export PYTHONWARNINGS="ignore:Unverified HTTPS request"
```

### Connection Timeouts

Increase timeout if proxy is slow:

```python
# In connections_factory.py, modify ClientSession creation:
timeout = aiohttp.ClientTimeout(total=60)
self._shared_client = aiohttp.ClientSession(trust_env=True, timeout=timeout)
```

## Security Considerations

1. **Proxy authentication** - If your proxy requires authentication, use:
   ```bash
   export HTTP_PROXY=http://username:password@proxy:8080
   ```

2. **SSL/TLS** - Proxy can decrypt HTTPS traffic. Only use trusted proxies.

3. **Sensitive data** - API keys and trading data pass through proxy. Ensure proxy is secure.

4. **Production use** - In production, use a properly configured, secure proxy with appropriate access controls.

## Future Enhancements

Potential improvements to consider:

1. **Per-connector proxy settings** - Allow different proxies for different exchanges
2. **Dynamic proxy switching** - Change proxy without restarting
3. **Proxy authentication UI** - Configure proxy credentials in Hummingbot UI
4. **Built-in caching** - Add Redis-based caching without external proxy
5. **Proxy health monitoring** - Automatic fallback if proxy fails

## Conclusion

This HTTP proxy support implementation provides a simple, effective way to:
- Manage rate limits across multiple bot instances
- Debug and monitor API traffic
- Work within network restrictions
- Implement caching layers for better performance

The implementation is minimal, backward-compatible, and follows standard proxy conventions, making it easy to integrate with existing proxy infrastructure.

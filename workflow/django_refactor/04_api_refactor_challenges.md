# Django Refactor: API Gateway via FastAPI (Wrapper Pattern)

> **🚨 CURRENT STATUS: PROPOSAL ONLY - NOT IMPLEMENTED**
>
> This document describes a **proposed FastAPI gateway** that does not currently exist in the CyberDeltaEngine codebase.
>
> **Actual Current State:**
> - No FastAPI implementation exists
> - No REST API gateway exists
> - No external API endpoints exist
> - No API key management system exists
> - No Redis pub/sub bridge exists for external API communication
> - CyberDeltaEngine operates as a command-line application only

## Overview

This document outlines how to create a FastAPI gateway that provides external REST API access to CyberDeltaEngine without modifying the core async trading engine. The gateway acts as a translation layer between REST requests and core engine commands.

**⚠️ IMPLEMENTATION STATUS: This is a design proposal. No external API exists.**

## Current API Architecture Analysis

### Current Exchange API Structure
```
cyberdelta/apis/
├── base/
│   ├── exchange_api.py           # Abstract base class
│   ├── authenticator_interface.py
│   ├── rate_limit_strategy_interface.py
│   └── simple_rate_limit_strategy.py
├── hyperliquid/
│   ├── hl_api.py                 # Main API class
│   ├── hl_auth.py                # Authentication
│   ├── hl_request_builder.py     # Request construction
│   ├── hl_response_handler.py    # Response processing
│   ├── hl_ws_message_router.py   # WebSocket routing
│   ├── services/                 # Business logic services
│   ├── models/                   # Pydantic models
│   └── mappers/                  # Data transformation
├── backpack/
│   └── [similar structure]
└── connectivity/
    ├── http_client.py            # Async HTTP client
    └── ws_manager.py             # WebSocket manager
```

### Current Design Patterns (Preserved in Core)

#### 1. Async-First Architecture
The core engine maintains its high-performance async architecture:
- All exchange APIs remain async
- WebSocket connections unchanged
- Strategy execution stays async
- No performance degradation

#### 2. Service Layer Pattern
Core services continue operating exactly as designed:
- Trading services handle execution
- Account services manage balances
- Market data services process updates
- Risk management enforces limits

#### 3. Real-time WebSocket Integration
WebSocket connections remain in the core:
- Direct exchange connections
- Real-time data processing
- Low-latency order updates
- High-frequency data handling

## FastAPI Gateway Architecture

### Why FastAPI for the API Gateway?
- **Native async support**: Works seamlessly with core engine
- **High performance**: Can handle trading-level throughput
- **Auto documentation**: OpenAPI/Swagger built-in
- **Type safety**: Pydantic models match core exactly
- **Easy integration**: Can embed in Django or run separately

### Gateway Pattern Implementation

```python
# api_gateway/main.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import asyncio
from typing import Dict, List, Optional
import redis
import json

app = FastAPI(title="CyberDelta API Gateway", version="1.0.0")
security = HTTPBearer()

# Redis connection for core communication
redis_client = redis.Redis(decode_responses=True)

class CoreBridge:
    """Bridge between FastAPI and core engine"""

    async def send_command(self, command: Dict) -> Dict:
        """Send command to core and await response"""
        command_id = str(uuid4())
        command['id'] = command_id

        # Send command via Redis
        redis_client.publish('api_commands', json.dumps(command))

        # Wait for response with timeout
        response = await self.wait_for_response(command_id, timeout=30)
        if not response:
            raise HTTPException(status_code=408, detail="Core engine timeout")

        return response

    async def wait_for_response(self, command_id: str, timeout: int) -> Optional[Dict]:
        """Wait for core engine response"""
        pubsub = redis_client.pubsub()
        pubsub.subscribe(f'api_response:{command_id}')

        end_time = asyncio.get_event_loop().time() + timeout

        while asyncio.get_event_loop().time() < end_time:
            message = pubsub.get_message(timeout=0.1)
            if message and message['type'] == 'message':
                return json.loads(message['data'])
            await asyncio.sleep(0.1)

        return None

bridge = CoreBridge()

# Authentication dependency
async def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify API key against Django database"""
    # Query Django database for API key validity
    # This is the only direct Django interaction
    api_key = credentials.credentials

    # Verify via Django ORM or REST call
    if not await is_valid_api_key(api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    return api_key

# API Endpoints
@app.get("/api/v1/ticker/{exchange}/{symbol}")
async def get_ticker(
    exchange: str,
    symbol: str,
    api_key: str = Depends(verify_api_key)
):
    """Get current ticker data"""

    # First try cache for recent data
    cache_key = f"ticker:{exchange}:{symbol}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    # Otherwise query core engine
    command = {
        'type': 'get_ticker',
        'exchange': exchange,
        'symbol': symbol
    }

    response = await bridge.send_command(command)
    return response

@app.post("/api/v1/orders")
async def place_order(
    order: OrderRequest,
    api_key: str = Depends(verify_api_key)
):
    """Place a new order"""

    # Validate order against risk limits
    if not await validate_order_limits(order):
        raise HTTPException(status_code=400, detail="Order exceeds risk limits")

    command = {
        'type': 'place_order',
        'order': order.dict()
    }

    response = await bridge.send_command(command)
    return response

@app.get("/api/v1/portfolio")
async def get_portfolio(
    api_key: str = Depends(verify_api_key)
):
    """Get current portfolio state"""

    # Read from database for point-in-time consistency
    from django_wrapper.apps.persistence.models import Balance, Position

    balances = await fetch_latest_balances()
    positions = await fetch_latest_positions()

    return {
        'balances': balances,
        'positions': positions,
        'timestamp': datetime.utcnow()
    }

@app.websocket("/ws/market-data")
async def market_data_websocket(websocket: WebSocket):
    """WebSocket endpoint for real-time market data"""
    await websocket.accept()

    # Verify authentication
    auth_message = await websocket.receive_json()
    if not await verify_websocket_auth(auth_message):
        await websocket.close(code=1008)
        return

    # Subscribe to market data updates
    pubsub = redis_client.pubsub()
    pubsub.subscribe('market_data_updates')

    try:
        while True:
            # Forward updates from core to client
            message = pubsub.get_message(timeout=1.0)
            if message and message['type'] == 'message':
                await websocket.send_text(message['data'])
    except Exception:
        await websocket.close()
```

### Integration with Django

```python
# Django integration for API keys and users
# django_wrapper/apps/api_gateway/models.py

from django.db import models
from django.contrib.auth.models import User

class APIKey(models.Model):
    """API keys for external access"""
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    key = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=100)
    is_active = models.BooleanField(default=True)
    permissions = models.JSONField(default=list)  # ['read', 'trade', 'admin']
    rate_limit_per_minute = models.IntegerField(default=100)
    created_at = models.DateTimeField(auto_now_add=True)
    last_used_at = models.DateTimeField(null=True)

    class Meta:
        ordering = ['-created_at']

# FastAPI authentication integration
async def is_valid_api_key(api_key: str) -> bool:
    """Verify API key via Django ORM"""
    from django_wrapper.apps.api_gateway.models import APIKey
    from django.utils import timezone

    try:
        key = await sync_to_async(APIKey.objects.get)(
            key=api_key,
            is_active=True
        )

        # Update last used timestamp
        key.last_used_at = timezone.now()
        await sync_to_async(key.save)()

        return True
    except APIKey.DoesNotExist:
        return False
```

### Rate Limiting Strategy

```python
# api_gateway/rate_limiting.py
from datetime import datetime, timedelta
import redis
from typing import Tuple

class APIRateLimiter:
    """Distributed rate limiting for API gateway"""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def check_rate_limit(
        self,
        api_key: str,
        limit: int = 100
    ) -> Tuple[bool, int]:
        """Check if request is within rate limits"""

        # Use sliding window counter
        now = datetime.utcnow()
        window_start = now - timedelta(minutes=1)

        # Redis sorted set key
        key = f"rate_limit:{api_key}"

        # Remove old entries
        self.redis.zremrangebyscore(
            key,
            0,
            window_start.timestamp()
        )

        # Count requests in window
        current_count = self.redis.zcard(key)

        if current_count < limit:
            # Add current request
            self.redis.zadd(key, {str(now): now.timestamp()})
            self.redis.expire(key, 120)  # Expire after 2 minutes
            return True, limit - current_count - 1
        else:
            # Rate limit exceeded
            return False, 0

    async def get_reset_time(self, api_key: str) -> datetime:
        """Get when rate limit resets"""
        key = f"rate_limit:{api_key}"

        # Get oldest entry
        oldest = self.redis.zrange(key, 0, 0, withscores=True)
        if oldest:
            oldest_timestamp = oldest[0][1]
            return datetime.fromtimestamp(oldest_timestamp) + timedelta(minutes=1)

        return datetime.utcnow()

# FastAPI middleware
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Apply rate limiting to all API requests"""

    # Extract API key from header
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return await call_next(request)

    api_key = auth_header.split(" ")[1]

    # Check rate limit
    limiter = APIRateLimiter(redis_client)
    allowed, remaining = await limiter.check_rate_limit(api_key)

    if not allowed:
        reset_time = await limiter.get_reset_time(api_key)
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded"},
            headers={
                "X-RateLimit-Limit": "100",
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(int(reset_time.timestamp()))
            }
        )

    # Process request
    response = await call_next(request)

    # Add rate limit headers
    response.headers["X-RateLimit-Limit"] = "100"
    response.headers["X-RateLimit-Remaining"] = str(remaining)

    return response
```

### Error Handling & Monitoring

```python
# api_gateway/monitoring.py
from prometheus_client import Counter, Histogram, Gauge
import logging

# Metrics
api_requests = Counter(
    'api_requests_total',
    'Total API requests',
    ['method', 'endpoint', 'status']
)
api_latency = Histogram(
    'api_request_latency_seconds',
    'API request latency',
    ['method', 'endpoint']
)
active_connections = Gauge(
    'api_active_connections',
    'Active WebSocket connections'
)

# Error handling
class APIErrorHandler:
    """Centralized error handling for API gateway"""

    @staticmethod
    async def handle_core_error(error: Dict) -> JSONResponse:
        """Handle errors from core engine"""

        error_type = error.get('type', 'unknown')
        message = error.get('message', 'Internal error')

        if error_type == 'validation_error':
            return JSONResponse(
                status_code=400,
                content={'detail': message}
            )
        elif error_type == 'risk_limit_exceeded':
            return JSONResponse(
                status_code=403,
                content={'detail': f'Risk limit exceeded: {message}'}
            )
        elif error_type == 'not_found':
            return JSONResponse(
                status_code=404,
                content={'detail': message}
            )
        else:
            # Log unknown errors
            logger.error(f"Unknown core error: {error}")
            return JSONResponse(
                status_code=500,
                content={'detail': 'Internal server error'}
            )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""

    logger.exception(f"Unhandled exception: {exc}")

    # Don't expose internal errors
    return JSONResponse(
        status_code=500,
        content={'detail': 'Internal server error'}
    )
```

## API Gateway Deployment

### Deployment Architecture
```
┌─────────────────────────────────────────────────────────┐
│                    Load Balancer                        │
└─────────────────────────────────────────────────────────┘
                           │
         ┌─────────────────┴─────────────────┐
         │                                   │
┌─────────────────┐                 ┌─────────────────┐
│  FastAPI API    │                 │  Django Admin   │
│  Gateway        │                 │  + Dashboard    │
│  (Port 8001)    │                 │  (Port 8000)    │
└─────────────────┘                 └─────────────────┘
         │                                   │
         └─────────────────┬─────────────────┘
                           │
                    ┌──────────────┐
                    │    Redis     │
                    │  Pub/Sub     │
                    └──────────────┘
                           │
                ┌──────────────────┐
                │  Core Engine     │
                │  (Subprocess)    │
                └──────────────────┘
```

### FastAPI Startup Script
```python
# api_gateway/run.py
import uvicorn
from multiprocessing import Process
import os
import signal

def run_fastapi():
    """Run FastAPI server"""
    uvicorn.run(
        "api_gateway.main:app",
        host="0.0.0.0",
        port=8001,
        workers=4,
        loop="uvloop",
        log_config={
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
        }
    )

def run_core_engine():
    """Run core CyberDeltaEngine"""
    from cyberdelta.main import main
    import asyncio

    # Run core engine with command listener
    asyncio.run(main(enable_api_bridge=True))

if __name__ == "__main__":
    # Start core engine process
    core_process = Process(target=run_core_engine)
    core_process.start()

    try:
        # Run FastAPI in main process
        run_fastapi()
    except KeyboardInterrupt:
        # Graceful shutdown
        os.kill(core_process.pid, signal.SIGTERM)
        core_process.join()
```

### Core Engine API Bridge
```python
# Add to core engine (minimal modification)
# cyberdelta/api_bridge.py

class APIBridge:
    """Bridge for external API commands"""

    def __init__(self, engine):
        self.engine = engine
        self.redis_client = redis.Redis()

    async def start(self):
        """Start listening for API commands"""
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe('api_commands')

        asyncio.create_task(self._command_listener(pubsub))

    async def _command_listener(self, pubsub):
        """Listen for commands from API gateway"""
        for message in pubsub.listen():
            if message['type'] == 'message':
                try:
                    command = json.loads(message['data'])
                    response = await self._handle_command(command)

                    # Send response back
                    self.redis_client.publish(
                        f"api_response:{command['id']}",
                        json.dumps(response)
                    )
                except Exception as e:
                    logger.error(f"API bridge error: {e}")

    async def _handle_command(self, command: Dict) -> Dict:
        """Route commands to appropriate handlers"""
        cmd_type = command.get('type')

        if cmd_type == 'get_ticker':
            return await self._get_ticker(command)
        elif cmd_type == 'place_order':
            return await self._place_order(command)
        elif cmd_type == 'get_portfolio':
            return await self._get_portfolio(command)
        else:
            return {'error': f'Unknown command: {cmd_type}'}

    async def _get_ticker(self, command: Dict) -> Dict:
        """Get ticker from engine"""
        exchange = command['exchange']
        symbol = command['symbol']

        try:
            ticker = await self.engine.get_ticker(exchange, symbol)
            return {
                'success': True,
                'data': ticker.dict()
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
```

## Performance & Security Considerations

### Performance Optimizations

```python
# Connection pooling for Redis
redis_pool = redis.ConnectionPool(
    host='localhost',
    port=6379,
    max_connections=50,
    socket_keepalive=True,
    socket_keepalive_options={
        1: 1,  # TCP_KEEPIDLE
        2: 3,  # TCP_KEEPINTVL
        3: 5   # TCP_KEEPCNT
    }
)

# Response caching
from fastapi_cache import FastAPICache
from fastapi_cache.decorator import cache
from fastapi_cache.backends.redis import RedisBackend

@app.on_event("startup")
async def startup():
    redis = aioredis.from_url("redis://localhost", encoding="utf8")
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")

@app.get("/api/v1/ticker/{exchange}/{symbol}")
@cache(expire=5)  # Cache for 5 seconds
async def get_ticker(exchange: str, symbol: str):
    # Implementation
    pass
```

### Security Best Practices

```python
# API key hashing and validation
import hashlib
import secrets

def generate_api_key() -> tuple[str, str]:
    """Generate API key and hash"""
    raw_key = secrets.token_urlsafe(32)
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    return raw_key, key_hash

# Request validation
from pydantic import BaseModel, validator

class OrderRequest(BaseModel):
    symbol: str
    side: Literal["buy", "sell"]
    quantity: Decimal
    price: Optional[Decimal]
    order_type: Literal["market", "limit"]

    @validator('quantity')
    def validate_quantity(cls, v):
        if v <= 0:
            raise ValueError('Quantity must be positive')
        if v > Decimal('1000000'):
            raise ValueError('Quantity exceeds maximum')
        return v

    @validator('price')
    def validate_price(cls, v, values):
        if values.get('order_type') == 'limit' and v is None:
            raise ValueError('Limit orders require price')
        if v is not None and v <= 0:
            raise ValueError('Price must be positive')
        return v

# CORS configuration
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://app.cyberdelta.com"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)
```

## Implementation Benefits

### Wrapper Pattern Advantages
1. **Zero core modifications**: Trading engine remains untouched
2. **Independent scaling**: API gateway scales separately
3. **Technology flexibility**: FastAPI for API, Django for admin
4. **Gradual rollout**: Can enable features incrementally

### Developer Experience
1. **Auto-generated docs**: Swagger UI at `/docs`
2. **Type safety**: Pydantic models throughout
3. **Async support**: Native async for high performance
4. **Easy testing**: Built-in test client

### Operational Benefits
1. **Monitoring**: Prometheus metrics built-in
2. **Rate limiting**: Per-API-key limits
3. **Audit trail**: All API calls logged
4. **High availability**: Stateless gateway design

## Summary

The FastAPI gateway provides a modern REST API interface to CyberDeltaEngine without modifying the core trading engine. This wrapper approach ensures:

- **Production stability**: Core engine continues running unchanged
- **API accessibility**: External systems can integrate easily
- **Performance**: Async FastAPI matches core engine speed
- **Security**: Proper authentication and rate limiting
- **Maintainability**: Clean separation of concerns

Total implementation time: 1-2 weeks as part of the overall 8-week project.

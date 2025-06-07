# Django Refactor: API Layer Challenges & Solutions

## Overview

The current CyberDeltaEngine API layer is heavily async-based, designed for high-frequency trading operations. Migrating to Django requires careful consideration of performance, real-time requirements, and architectural patterns.

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

### Current Design Patterns

#### 1. Async-First Architecture
```python
# Current pattern
class HyperliquidAPI(ExchangeAPI):
    async def get_ticker(self, symbol: str) -> Ticker:
        async with self.http_client.session() as session:
            response = await session.get(f"/ticker/{symbol}")
            return self.response_handler.parse_ticker(response)
    
    async def place_order(self, order: OrderRequest) -> Order:
        signed_request = await self.auth.sign_request(order)
        response = await self.http_client.post("/order", signed_request)
        return self.response_handler.parse_order(response)
```

#### 2. Service Layer Pattern
```python
# Current service pattern
class HLTradingService:
    def __init__(self, api: HyperliquidAPI):
        self.api = api
    
    async def execute_strategy_signal(self, signal: TradeSignal) -> Order:
        # Validate signal
        # Check risk limits
        # Place order
        # Track execution
        return await self.api.place_order(order_request)
```

#### 3. Real-time WebSocket Integration
```python
# Current WebSocket pattern
class HLWebSocketManager:
    async def connect(self):
        self.ws = await websockets.connect(self.ws_url)
        asyncio.create_task(self.message_handler())
    
    async def message_handler(self):
        async for message in self.ws:
            parsed = self.message_router.route(message)
            await self.data_handler.process(parsed)
```

## Django Migration Challenges

### Challenge 1: Async to Sync Conversion

#### Current Async Pattern
```python
async def get_all_tickers():
    tasks = []
    for exchange in exchanges:
        tasks.append(exchange.get_ticker(symbol))
    
    results = await asyncio.gather(*tasks)
    return results
```

#### Django Sync Solution Options

**Option A: Synchronous with Threading**
```python
# Django views.py
import concurrent.futures
from django.http import JsonResponse

def get_all_tickers(request):
    def fetch_ticker(exchange, symbol):
        # Use requests instead of aiohttp
        return exchange.get_ticker_sync(symbol)
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for exchange in exchanges:
            future = executor.submit(fetch_ticker, exchange, symbol)
            futures.append(future)
        
        results = [future.result() for future in futures]
    
    return JsonResponse({'tickers': results})
```

**Option B: Celery Background Tasks**
```python
# tasks.py
from celery import group

@shared_task
def fetch_single_ticker(exchange_id, symbol):
    exchange = get_exchange(exchange_id)
    return exchange.get_ticker_sync(symbol)

# views.py
def get_all_tickers(request):
    symbol = request.GET.get('symbol')
    
    # Create group of parallel tasks
    job = group(
        fetch_single_ticker.s(exchange.id, symbol) 
        for exchange in exchanges
    )
    
    result = job.apply_async()
    tickers = result.get(timeout=10)  # Wait max 10 seconds
    
    return JsonResponse({'tickers': tickers})
```

**Option C: Django Async Views (Django 4.1+)**
```python
# views.py (async Django view)
import asyncio
import aiohttp

async def get_all_tickers(request):
    async def fetch_ticker(session, exchange, symbol):
        async with session.get(f"{exchange.api_url}/ticker/{symbol}") as response:
            return await response.json()
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for exchange in exchanges:
            task = fetch_ticker(session, exchange, symbol)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
    
    return JsonResponse({'tickers': results})
```

### Challenge 2: Real-time Data Streams

#### Current WebSocket Architecture
```python
# Current: Direct async WebSocket handling
class MarketDataEngine:
    async def start(self):
        for exchange in self.exchanges:
            asyncio.create_task(exchange.start_websocket())
    
    async def handle_market_data(self, data):
        # Process immediately in async loop
        await self.strategy_engine.process_data(data)
```

#### Django Solution: Celery + Django Channels
```python
# consumers.py (Django Channels)
class MarketDataConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add("market_data", self.channel_name)
        await self.accept()
    
    async def market_data_update(self, event):
        await self.send(text_data=json.dumps(event['data']))

# tasks.py (Celery workers)
@shared_task
def process_market_data_stream():
    """Background task to handle market data streams"""
    
    def handle_message(message):
        # Process market data
        processed_data = process_ticker_update(message)
        
        # Store in database
        Ticker.objects.create(**processed_data)
        
        # Broadcast to WebSocket clients
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)("market_data", {
            "type": "market_data_update",
            "data": processed_data
        })
    
    # Connect to exchange WebSocket in background
    with websocket_connection(exchange_url) as ws:
        for message in ws:
            handle_message(message)

# management/commands/start_market_data.py
class Command(BaseCommand):
    def handle(self, *args, **options):
        # Start market data streams in background
        for exchange in Exchange.objects.filter(is_active=True):
            process_market_data_stream.delay(exchange.id)
```

### Challenge 3: Rate Limiting & Authentication

#### Current Rate Limiting
```python
# Current: Per-instance rate limiting
class RateLimitStrategy:
    def __init__(self, requests_per_minute: int):
        self.semaphore = asyncio.Semaphore(requests_per_minute)
        self.request_times = deque()
    
    async def acquire(self):
        async with self.semaphore:
            now = time.time()
            # Rate limiting logic
            await asyncio.sleep(delay)
```

#### Django Solution: Distributed Rate Limiting
```python
# utils/rate_limiter.py
from django.core.cache import cache
import time

class DistributedRateLimiter:
    def __init__(self, exchange_name: str, requests_per_minute: int):
        self.exchange_name = exchange_name
        self.rpm = requests_per_minute
        self.window = 60  # seconds
    
    def can_proceed(self) -> tuple[bool, float]:
        """Check if request can proceed, return (allowed, wait_time)"""
        cache_key = f"rate_limit:{self.exchange_name}"
        
        with cache.lock(cache_key + ":lock", timeout=1):
            current_time = time.time()
            window_start = current_time - self.window
            
            # Get recent requests
            requests = cache.get(cache_key, [])
            
            # Remove old requests
            requests = [req_time for req_time in requests if req_time > window_start]
            
            if len(requests) < self.rpm:
                # Allow request
                requests.append(current_time)
                cache.set(cache_key, requests, self.window)
                return True, 0.0
            else:
                # Calculate wait time
                oldest_request = min(requests)
                wait_time = oldest_request + self.window - current_time
                return False, wait_time

# Decorator for API methods
def rate_limited(exchange_name: str, rpm: int):
    def decorator(func):
        def wrapper(*args, **kwargs):
            limiter = DistributedRateLimiter(exchange_name, rpm)
            can_proceed, wait_time = limiter.can_proceed()
            
            if not can_proceed:
                time.sleep(wait_time)
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Usage in API methods
@rate_limited("hyperliquid", 100)
def get_ticker(self, symbol: str) -> Ticker:
    response = requests.get(f"{self.base_url}/ticker/{symbol}")
    return self.parse_ticker_response(response)
```

### Challenge 4: Error Handling & Retry Logic

#### Current Async Error Handling
```python
# Current: Async retry with exponential backoff
async def api_request_with_retry(self, method, url, data=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            async with self.session.request(method, url, json=data) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Rate limited
                    wait_time = int(response.headers.get('Retry-After', 2 ** attempt))
                    await asyncio.sleep(wait_time)
                else:
                    response.raise_for_status()
        except aiohttp.ClientError as e:
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)
```

#### Django Solution: Celery Retry with Database Logging
```python
# tasks.py
from celery.exceptions import Retry

@shared_task(bind=True, max_retries=3)
def api_request_task(self, exchange_id, method, endpoint, data=None):
    """API request with automatic retry and error logging"""
    
    try:
        exchange = Exchange.objects.get(id=exchange_id)
        client = get_api_client(exchange)
        
        response = client.request(method, endpoint, data)
        
        # Log successful request
        APIRequestLog.objects.create(
            exchange=exchange,
            method=method,
            endpoint=endpoint,
            status='success',
            response_time=response.elapsed.total_seconds()
        )
        
        return response.json()
        
    except requests.exceptions.RequestException as exc:
        # Log failed request
        APIRequestLog.objects.create(
            exchange=exchange,
            method=method,
            endpoint=endpoint,
            status='error',
            error_message=str(exc),
            retry_count=self.request.retries
        )
        
        # Exponential backoff
        countdown = 2 ** self.request.retries
        
        # Special handling for rate limits
        if hasattr(exc, 'response') and exc.response.status_code == 429:
            retry_after = exc.response.headers.get('Retry-After', countdown)
            countdown = int(retry_after)
        
        raise self.retry(exc=exc, countdown=countdown)

# models.py
class APIRequestLog(models.Model):
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE)
    method = models.CharField(max_length=10)
    endpoint = models.CharField(max_length=200)
    status = models.CharField(max_length=20)
    response_time = models.FloatField(null=True)
    error_message = models.TextField(blank=True)
    retry_count = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-created_at']
```

## Proposed Django API Architecture

### 1. Service-Oriented Architecture
```python
# apps/exchanges/services.py

class ExchangeAPIService:
    """Base service for exchange API operations"""
    
    def __init__(self, exchange: Exchange):
        self.exchange = exchange
        self.client = self._get_http_client()
        self.rate_limiter = DistributedRateLimiter(
            exchange.name, 
            exchange.rate_limit_per_minute
        )
    
    def _get_http_client(self):
        """Get configured HTTP client with auth"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'CyberDeltaEngine/1.0',
            'Content-Type': 'application/json'
        })
        
        # Add authentication
        auth_service = AuthenticationService(self.exchange)
        session.auth = auth_service.get_auth_handler()
        
        return session

class HyperliquidService(ExchangeAPIService):
    """Hyperliquid-specific API service"""
    
    def get_ticker(self, symbol: str) -> Ticker:
        """Get ticker data for symbol"""
        endpoint = f"/info/ticker/{symbol}"
        response_data = self._make_request("GET", endpoint)
        
        # Transform to internal model
        ticker_data = HLTickerMapper.to_internal(response_data)
        
        # Save to database
        ticker = Ticker.objects.create(**ticker_data)
        
        return ticker
    
    def place_order(self, order_request: OrderRequest) -> Order:
        """Place order on exchange"""
        # Validate order
        self._validate_order(order_request)
        
        # Check risk limits
        risk_service = RiskManagementService(self.exchange)
        risk_service.validate_order(order_request)
        
        # Place order
        endpoint = "/exchange"
        signed_data = self._sign_order_request(order_request)
        response_data = self._make_request("POST", endpoint, signed_data)
        
        # Create order record
        order = Order.objects.create(
            exchange=self.exchange,
            **OrderMapper.to_internal(response_data)
        )
        
        # Send notification
        order_placed.send(sender=self.__class__, order=order)
        
        return order
    
    def _make_request(self, method: str, endpoint: str, data=None):
        """Make rate-limited API request"""
        
        # Rate limiting
        can_proceed, wait_time = self.rate_limiter.can_proceed()
        if not can_proceed:
            time.sleep(wait_time)
        
        # Make request with retry
        return api_request_task.delay(
            self.exchange.id, 
            method, 
            endpoint, 
            data
        ).get(timeout=30)
```

### 2. Django REST API Layer
```python
# apps/api/views.py
from rest_framework import generics, status
from rest_framework.response import Response

class TickerAPIView(generics.RetrieveAPIView):
    """Get ticker data for a trading pair"""
    
    def get(self, request, exchange_name, symbol):
        try:
            exchange = Exchange.objects.get(name=exchange_name)
            service = get_exchange_service(exchange)
            
            # Try to get from cache first
            cache_key = f"ticker:{exchange_name}:{symbol}"
            ticker = cache.get(cache_key)
            
            if not ticker:
                # Fetch from exchange
                ticker = service.get_ticker(symbol)
                cache.set(cache_key, ticker, 60)  # Cache for 1 minute
            
            serializer = TickerSerializer(ticker)
            return Response(serializer.data)
            
        except Exchange.DoesNotExist:
            return Response(
                {"error": "Exchange not found"}, 
                status=status.HTTP_404_NOT_FOUND
            )
        except APIException as e:
            return Response(
                {"error": str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class OrderAPIView(generics.CreateAPIView):
    """Place new order"""
    
    def post(self, request, exchange_name):
        try:
            exchange = Exchange.objects.get(name=exchange_name)
            service = get_exchange_service(exchange)
            
            # Validate request data
            serializer = OrderRequestSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            
            # Create order request
            order_request = OrderRequest(**serializer.validated_data)
            
            # Place order (async via Celery)
            task = place_order_task.delay(exchange.id, order_request.dict())
            
            return Response({
                "task_id": task.id,
                "status": "submitted"
            }, status=status.HTTP_202_ACCEPTED)
            
        except ValidationError as e:
            return Response(
                {"error": "Invalid order data", "details": e.detail},
                status=status.HTTP_400_BAD_REQUEST
            )
```

### 3. Background Task Architecture
```python
# apps/exchanges/tasks.py

@shared_task
def place_order_task(exchange_id: int, order_data: dict) -> dict:
    """Background task to place order"""
    try:
        exchange = Exchange.objects.get(id=exchange_id)
        service = get_exchange_service(exchange)
        
        order_request = OrderRequest(**order_data)
        order = service.place_order(order_request)
        
        return {
            "status": "success",
            "order_id": order.id,
            "exchange_order_id": order.order_id
        }
        
    except Exception as e:
        # Log error
        logger.error(f"Order placement failed: {e}")
        
        # Update order status
        Order.objects.filter(client_order_id=order_data.get('client_order_id'))\
            .update(status='failed', error_message=str(e))
        
        return {
            "status": "failed",
            "error": str(e)
        }

@shared_task
def sync_exchange_data(exchange_id: int):
    """Periodic task to sync exchange data"""
    exchange = Exchange.objects.get(id=exchange_id)
    service = get_exchange_service(exchange)
    
    # Sync balances
    balances = service.get_balances()
    for balance_data in balances:
        Balance.objects.update_or_create(
            account=balance_data['account'],
            asset=balance_data['asset'],
            defaults=balance_data
        )
    
    # Sync positions
    positions = service.get_positions()
    for position_data in positions:
        Position.objects.update_or_create(
            account=position_data['account'],
            trading_pair=position_data['trading_pair'],
            defaults=position_data
        )

# Periodic task scheduling
from celery.schedules import crontab

CELERY_BEAT_SCHEDULE = {
    'sync-exchange-data': {
        'task': 'apps.exchanges.tasks.sync_exchange_data',
        'schedule': crontab(minute='*/5'),  # Every 5 minutes
        'args': (1,)  # Exchange ID
    },
}
```

## Performance Considerations

### 1. Database Connection Pooling
```python
# settings.py
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'cyberdelta',
        'USER': 'cyberdelta',
        'PASSWORD': '***',
        'HOST': 'localhost',
        'PORT': '5432',
        'CONN_MAX_AGE': 600,  # Connection pooling
        'OPTIONS': {
            'MAX_CONNS': 20,
            'MIN_CONNS': 5,
        }
    }
}
```

### 2. Redis Caching Strategy
```python
# Cache configuration
CACHES = {
    'default': {
        'BACKEND': 'django_redis.cache.RedisCache',
        'LOCATION': 'redis://127.0.0.1:6379/1',
        'OPTIONS': {
            'CLIENT_CLASS': 'django_redis.client.DefaultClient',
            'CONNECTION_POOL_KWARGS': {
                'max_connections': 50,
                'retry_on_timeout': True,
            }
        }
    }
}

# Caching patterns
def get_ticker_with_cache(exchange_name: str, symbol: str) -> Ticker:
    cache_key = f"ticker:{exchange_name}:{symbol}"
    
    ticker = cache.get(cache_key)
    if ticker:
        return ticker
    
    # Fetch from database first
    try:
        ticker = Ticker.objects.filter(
            trading_pair__exchange__name=exchange_name,
            trading_pair__symbol=symbol
        ).latest('timestamp')
        
        if ticker.is_recent(seconds=60):
            cache.set(cache_key, ticker, 60)
            return ticker
    except Ticker.DoesNotExist:
        pass
    
    # Fetch from exchange API
    exchange = Exchange.objects.get(name=exchange_name)
    service = get_exchange_service(exchange)
    ticker = service.get_ticker(symbol)
    
    cache.set(cache_key, ticker, 60)
    return ticker
```

### 3. Bulk Operations
```python
def bulk_update_tickers(ticker_data_list: list[dict]):
    """Efficiently update multiple tickers"""
    
    ticker_objects = []
    for data in ticker_data_list:
        ticker = Ticker(
            trading_pair_id=data['trading_pair_id'],
            last_price=data['last_price'],
            timestamp=data['timestamp']
        )
        ticker_objects.append(ticker)
    
    # Bulk create with ignore conflicts
    Ticker.objects.bulk_create(
        ticker_objects, 
        batch_size=1000,
        ignore_conflicts=True
    )
    
    # Update cache in batch
    pipeline = cache._cache.get_client().pipeline()
    for ticker in ticker_objects:
        cache_key = f"ticker:{ticker.trading_pair.exchange.name}:{ticker.trading_pair.symbol}"
        pipeline.set(cache_key, pickle.dumps(ticker), ex=60)
    pipeline.execute()
```

## Migration Strategy

### Phase 1: API Client Migration
1. **Convert async HTTP clients to sync requests**
2. **Implement rate limiting with Redis**
3. **Add comprehensive error handling**
4. **Create service layer abstractions**

### Phase 2: Background Task Implementation
1. **Set up Celery with Redis broker**
2. **Migrate WebSocket connections to background tasks**
3. **Implement periodic data synchronization**
4. **Add task monitoring and error recovery**

### Phase 3: REST API Development
1. **Create Django REST Framework APIs**
2. **Add authentication and authorization**
3. **Implement API versioning**
4. **Add comprehensive API documentation**

### Phase 4: Performance Optimization
1. **Implement comprehensive caching**
2. **Optimize database queries**
3. **Add monitoring and alerting**
4. **Load testing and optimization**

This architecture maintains the functionality of the current async system while providing the benefits of Django's ecosystem and better maintainability.
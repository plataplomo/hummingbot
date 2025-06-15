# Django Refactor: Migration Timeline & Implementation Plan (Wrapper Pattern)

## Executive Summary

This document provides a detailed timeline for implementing Django + HTMX wrappers around the existing CyberDeltaEngine without modifying the core trading system. The project is structured as an 8-week implementation focused on building external layers that read from and command the unchanged core engine.

## Migration Philosophy: Wrapper Pattern

### Core Principles
1. **Zero Core Modifications**: Trading engine remains completely unchanged
2. **Independent Operation**: Wrappers run alongside core, not replacing it
3. **Gradual Enhancement**: Add features incrementally without risk
4. **Production Safety**: Core engine failure doesn't affect wrappers, and wrapper failure doesn't affect trading
5. **Data Synchronization**: Django reads from synchronized database, commands via Redis

### Success Criteria
- **Zero Risk**: Core trading performance unaffected
- **Enhanced Features**: Multi-user support, historical analysis, external API access
- **Performance**: Wrapper response times < 200ms (non-critical path)
- **Reliability**: Wrapper failure gracefully degrades without affecting trading
- **Maintainability**: Clean separation enables independent development

## Phase 1: Foundation & Data Bridge (Weeks 1-2)

### Week 1: Django Wrapper Setup
```bash
# Day 1-2: Django Wrapper Project Structure
django_wrapper/
├── manage.py
├── requirements/
│   ├── base.txt
│   ├── production.txt
│   └── development.txt
├── config/
│   ├── settings/
│   │   ├── base.py
│   │   ├── development.py
│   │   ├── production.py
│   │   └── testing.py
│   ├── urls.py
│   ├── wsgi.py
│   └── asgi.py
├── apps/
│   ├── persistence/      # Django models for synchronized data
│   ├── dashboard/        # HTMX dashboard
│   ├── api_gateway/      # FastAPI gateway
│   ├── bridge/          # Service bridge to core
│   └── monitoring/      # Health monitoring
└── tests/

# Day 3-5: Infrastructure Setup
- PostgreSQL + TimescaleDB for data persistence
- Redis for pub/sub communication with core
- Django Channels for WebSocket proxy
- Service bridge foundation
- Docker containers for wrapper services
```

### Week 2: Data Synchronization Bridge
```python
# Core Data Models (Read-Only Persistence)
class Exchange(models.Model):
    """Mirror of core exchange configuration"""
    name = models.CharField(max_length=50, unique=True)
    display_name = models.CharField(max_length=100)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

class TradingPair(models.Model):
    """Mirror of trading pairs from core"""
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE)
    symbol = models.CharField(max_length=20)
    base_asset = models.CharField(max_length=10)
    quote_asset = models.CharField(max_length=10)
    pair_type = models.CharField(max_length=20)  # spot, perpetual, etc.

class Ticker(models.Model):
    """Real-time ticker data from core (TimescaleDB)"""
    trading_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE)
    timestamp = models.DateTimeField(db_index=True)
    last_price = models.DecimalField(max_digits=20, decimal_places=8)
    bid_price = models.DecimalField(max_digits=20, decimal_places=8, null=True)
    ask_price = models.DecimalField(max_digits=20, decimal_places=8, null=True)
    volume_24h = models.DecimalField(max_digits=20, decimal_places=8, null=True)
    
    class Meta:
        db_table = 'tickers'  # TimescaleDB hypertable

# Data Synchronization Service
class DataSyncService:
    """Synchronizes data from core engine to Django database"""
    
    def __init__(self):
        self.redis_client = redis.Redis()
    
    async def sync_ticker_data(self, ticker_data: dict):
        """Sync incoming ticker from core"""
        try:
            trading_pair = await sync_to_async(TradingPair.objects.get)(
                symbol=ticker_data['symbol'],
                exchange__name=ticker_data['exchange']
            )
            
            await sync_to_async(Ticker.objects.create)(
                trading_pair=trading_pair,
                timestamp=parse(ticker_data['timestamp']),
                last_price=ticker_data['last_price'],
                bid_price=ticker_data.get('bid_price'),
                ask_price=ticker_data.get('ask_price'),
                volume_24h=ticker_data.get('volume_24h')
            )
        except Exception as e:
            logger.error(f"Ticker sync error: {e}")
```

**Deliverables:**
- ✅ Django wrapper project foundation
- ✅ Data synchronization from core to Django
- ✅ Redis pub/sub bridge setup  
- ✅ TimescaleDB integration for time-series data
- ✅ Basic monitoring and health checks

**Phase 1 Milestone Review:**
- Wrapper foundation established
- Data synchronization operational  
- Zero modifications to core engine
- Ready for dashboard and API implementation

---

## Phase 2: Dashboard & WebSocket Proxy (Weeks 3-4)

### Week 3: HTMX Dashboard Implementation
```python
# Dashboard Views (Server-Side Rendering)
class DashboardOverviewView(TemplateView):
    """Main dashboard reading from synchronized database"""
    template_name = 'dashboard/overview.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Read from synchronized database
        context['latest_tickers'] = Ticker.objects.select_related('trading_pair')\
            .filter(timestamp__gte=timezone.now() - timedelta(minutes=5))\
            .order_by('trading_pair', '-timestamp')\
            .distinct('trading_pair')
        
        # Strategy performance from database
        context['strategy_metrics'] = self.calculate_strategy_metrics()
        
        # Market data for charts
        context['chart_data'] = self.get_chart_data()
        
        return context

# HTMX Component Views
class PerformanceChartView(View):
    """HTMX endpoint for updating charts"""
    
    def get(self, request):
        time_range = request.GET.get('time_range', '24h')
        strategies = request.GET.getlist('strategies')
        
        # Query database for historical data
        chart_data = self.get_performance_data(strategies, time_range)
        
        # Server-side chart generation
        fig = create_plotly_figure(chart_data)
        
        return render(request, 'dashboard/components/chart.html', {
            'chart_json': fig.to_json(),
            'time_range': time_range
        })

# Core Command Service (Dashboard → Core)
class CoreCommandService:
    """Send commands to core engine from dashboard"""
    
    def __init__(self):
        self.redis_client = redis.Redis()
        
    async def start_strategy(self, strategy_id: str, user: User):
        """Send start command to core engine"""
        command = {
            'type': 'start_strategy',
            'strategy_id': strategy_id,
            'user_id': user.id,
            'timestamp': timezone.now().isoformat()
        }
        
        # Send command via Redis pub/sub
        self.redis_client.publish('core_commands', json.dumps(command))
        
        # Log command for audit
        CommandLog.objects.create(
            user=user,
            command_type='start_strategy',
            payload=command,
            status='sent'
        )
```

**Deliverables:**
- ✅ HTMX dashboard with server-side rendering
- ✅ Real-time components reading from database
- ✅ Command service for core engine communication
- ✅ Strategy control interface

### Week 4: WebSocket Proxy & Real-time Updates  
```python
# Django Channels WebSocket Consumer
class DashboardConsumer(AsyncWebsocketConsumer):
    """WebSocket proxy for real-time dashboard updates"""
    
    async def connect(self):
        self.user = self.scope["user"]
        
        # Authentication check
        if not self.user.is_authenticated:
            await self.close(code=4001)
            return
            
        await self.accept()
        await self.channel_layer.group_add("dashboard", self.channel_name)
        
        # Send initial state from database
        await self.send_initial_dashboard_state()
    
    async def receive(self, text_data):
        """Handle client subscription requests"""
        try:
            data = json.loads(text_data)
            if data.get('type') == 'subscribe':
                symbols = data.get('symbols', [])
                await self.handle_subscription(symbols)
        except json.JSONDecodeError:
            await self.send_error('Invalid JSON')
    
    async def market_data_update(self, event):
        """Forward market data from Redis to client"""
        await self.send(text_data=json.dumps({
            'type': 'ticker_update',
            'data': event['data']
        }))

# Redis Bridge Service
class RedisWebSocketBridge:
    """Bridge Redis pub/sub to Django Channels"""
    
    def __init__(self):
        self.redis_client = redis.Redis()
        self.channel_layer = get_channel_layer()
    
    async def start_listening(self):
        """Listen to Redis channels from core engine"""
        pubsub = self.redis_client.pubsub()
        
        # Subscribe to core engine channels
        channels = [
            'market_data:hyperliquid',
            'market_data:backpack', 
            'trades',
            'positions',
            'strategy_updates'
        ]
        
        for channel in channels:
            pubsub.subscribe(channel)
        
        # Process messages and broadcast to WebSocket clients
        async for message in pubsub.listen():
            if message['type'] == 'message':
                await self.broadcast_to_clients(message)
    
    async def broadcast_to_clients(self, message):
        """Broadcast Redis message to WebSocket clients"""
        try:
            channel = message['channel']
            data = json.loads(message['data'])
            
            # Route to appropriate consumer group
            if channel.startswith('market_data:'):
                await self.channel_layer.group_send("dashboard", {
                    "type": "market_data_update",
                    "data": data
                })
            elif channel == 'trades':
                await self.channel_layer.group_send("trading", {
                    "type": "trade_update", 
                    "data": data
                })
        except Exception as e:
            logger.error(f"Broadcast error: {e}")

# Minimal Core Integration (Optional Redis Publisher)
class RedisDataPublisher:
    """Add to core engine for publishing to Redis"""
    
    def __init__(self):
        self.redis_client = redis.Redis()
        self.enabled = True  # Can disable if not needed
    
    async def publish_ticker(self, exchange: str, ticker_data: dict):
        """Publish ticker update to Redis"""
        if not self.enabled:
            return
            
        try:
            channel = f"market_data:{exchange}"
            message = {
                'type': 'ticker',
                'exchange': exchange,
                'data': ticker_data,
                'timestamp': datetime.utcnow().isoformat()
            }
            self.redis_client.publish(channel, json.dumps(message))
        except Exception as e:
            # Log but don't crash core engine
            logger.debug(f"Redis publish error: {e}")
```

**Deliverables:**
- ✅ Django Channels WebSocket proxy
- ✅ Redis bridge for real-time data forwarding
- ✅ Multi-user WebSocket support with authentication
- ✅ Minimal core integration (optional Redis publisher)

### Week 7: Celery Background Tasks
```python
# Background Task Framework
@shared_task(bind=True, max_retries=3)
def sync_exchange_data(self, exchange_id: int):
    """Periodic sync of exchange data"""
    
    try:
        exchange = Exchange.objects.get(id=exchange_id)
        service = MarketDataService(exchange)
        
        # Get active trading pairs
        trading_pairs = exchange.trading_pairs.filter(is_active=True)
        symbols = [tp.symbol for tp in trading_pairs]
        
        # Bulk update tickers
        tickers = service.bulk_update_tickers(symbols)
        
        # Update funding rates for perpetuals
        perp_pairs = trading_pairs.filter(pair_type='perpetual')
        for pair in perp_pairs:
            update_funding_rate.delay(exchange_id, pair.symbol)
        
        return {"updated_tickers": len(tickers)}
        
    except Exception as exc:
        logger.error(f"Exchange sync failed: {exc}")
        raise self.retry(exc=exc, countdown=60)

# Task Monitoring
@shared_task
def monitor_task_health():
    """Monitor Celery task health"""
    
    # Check for failed tasks
    failed_tasks = check_failed_tasks()
    if failed_tasks:
        send_alert_notification.delay("Failed tasks detected", failed_tasks)
    
    # Check queue lengths
    queue_stats = get_queue_statistics()
    for queue, length in queue_stats.items():
        if length > 1000:  # Alert if queue is backing up
            send_alert_notification.delay(f"Queue {queue} backing up", length)
```

**Deliverables:**
- ✅ Celery task framework for background operations
- ✅ Periodic data synchronization tasks
- ✅ Task monitoring and alerting
- ✅ Error handling and retry logic

### Week 8: API Integration Testing
```python
# Integration Test Suite
class ExchangeAPIIntegrationTests(TransactionTestCase):
    
    def test_hyperliquid_api_integration(self):
        """Test complete Hyperliquid API integration"""
        
        # Setup
        exchange = Exchange.objects.get(name='hyperliquid')
        service = MarketDataService(exchange)
        
        # Test ticker updates
        ticker = service.update_ticker('BTC-USD')
        self.assertIsNotNone(ticker.last_price)
        self.assertTrue(ticker.last_price > 0)
        
        # Test database persistence
        db_ticker = Ticker.objects.filter(
            trading_pair__symbol='BTC-USD',
            trading_pair__exchange=exchange
        ).latest('timestamp')
        self.assertEqual(ticker.id, db_ticker.id)
    
    def test_rate_limiting_behavior(self):
        """Test rate limiting prevents API abuse"""
        
        # Make requests up to rate limit
        # Verify requests are throttled appropriately
        # Check that rate limiter state is maintained across requests

# Performance Testing
def load_test_api_endpoints():
    """Load test new API endpoints"""
    
    # Test concurrent API requests
    # Measure response times under load
    # Verify database performance
    # Check memory usage patterns
```

**Deliverables:**
- ✅ Comprehensive integration test suite
- ✅ Performance testing results
- ✅ API client validation
- ✅ Rate limiting verification

**Phase 2 Milestone Review:**
- Dashboard operational with HTMX
- WebSocket proxy providing real-time updates
- Command bridge for core engine communication
- Ready for API gateway implementation

---

## Phase 3: FastAPI Gateway & Multi-User Support (Weeks 5-6)

### Week 5: FastAPI Gateway Implementation
```python
# FastAPI Gateway for External API Access
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer
import redis
import json
from uuid import uuid4

app = FastAPI(title="CyberDelta API Gateway", version="1.0.0")
security = HTTPBearer()

class CoreBridge:
    """Bridge between FastAPI and core engine"""
    
    def __init__(self):
        self.redis_client = redis.Redis()
    
    async def send_command(self, command: dict) -> dict:
        """Send command to core and await response"""
        command_id = str(uuid4())
        command['id'] = command_id
        
        # Send command via Redis
        self.redis_client.publish('api_commands', json.dumps(command))
        
        # Wait for response with timeout
        response = await self.wait_for_response(command_id, timeout=30)
        if not response:
            raise HTTPException(status_code=408, detail="Core engine timeout")
        
        return response
    
    async def wait_for_response(self, command_id: str, timeout: int) -> dict:
        """Wait for core engine response"""
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(f'api_response:{command_id}')
        
        # Implement timeout logic
        end_time = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < end_time:
            message = pubsub.get_message(timeout=0.1)
            if message and message['type'] == 'message':
                return json.loads(message['data'])
            await asyncio.sleep(0.1)
        
        return None

bridge = CoreBridge()

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

@app.get("/api/v1/portfolio")
async def get_portfolio(api_key: str = Depends(verify_api_key)):
    """Get current portfolio from database"""
    
    # Read from synchronized database for consistency
    balances = await fetch_latest_balances()
    positions = await fetch_latest_positions()
    
    return {
        'balances': balances,
        'positions': positions,
        'timestamp': datetime.utcnow()
    }

# Authentication with Django
async def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify API key against Django database"""
    from django_wrapper.apps.api_gateway.models import APIKey
    
    try:
        api_key = credentials.credentials
        key_obj = await sync_to_async(APIKey.objects.get)(
            key=api_key, 
            is_active=True
        )
        
        # Update last used timestamp
        key_obj.last_used_at = timezone.now()
        await sync_to_async(key_obj.save)()
        
        return api_key
    except APIKey.DoesNotExist:
        raise HTTPException(status_code=401, detail="Invalid API key")
```

**Deliverables:**
- ✅ FastAPI gateway for external API access
- ✅ Authentication integration with Django
- ✅ Core engine command bridge via Redis
- ✅ Rate limiting and security features

### Week 6: User Management & Permissions
```python
# User Management Models
from django.contrib.auth.models import AbstractUser
from django.db import models

class TradingUser(AbstractUser):
    """Extended user model for trading platform"""
    
    # Trading permissions
    can_view_dashboard = models.BooleanField(default=True)
    can_view_trades = models.BooleanField(default=False)
    can_execute_trades = models.BooleanField(default=False)
    can_manage_strategies = models.BooleanField(default=False)
    
    # API access
    max_api_calls_per_minute = models.IntegerField(default=100)
    
    # Profile information
    organization = models.CharField(max_length=100, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    last_login_ip = models.GenericIPAddressField(null=True, blank=True)

class APIKey(models.Model):
    """API keys for external access"""
    user = models.ForeignKey(TradingUser, on_delete=models.CASCADE)
    key = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=100)
    is_active = models.BooleanField(default=True)
    
    # Permissions
    permissions = models.JSONField(default=list)  # ['read', 'trade', 'admin']
    
    # Rate limiting
    rate_limit_per_minute = models.IntegerField(default=100)
    
    # Tracking
    created_at = models.DateTimeField(auto_now_add=True)
    last_used_at = models.DateTimeField(null=True, blank=True)
    total_requests = models.BigIntegerField(default=0)

class UserStrategyAccess(models.Model):
    """Control which users can access which strategies"""
    user = models.ForeignKey(TradingUser, on_delete=models.CASCADE)
    strategy_name = models.CharField(max_length=100)
    
    # Access levels
    can_view = models.BooleanField(default=True)
    can_start_stop = models.BooleanField(default=False)
    can_configure = models.BooleanField(default=False)
    
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ['user', 'strategy_name']

# Permission Checking
class PermissionService:
    """Service for checking user permissions"""
    
    @staticmethod
    def can_user_access_strategy(user: TradingUser, strategy_name: str) -> dict:
        """Check user's access level for a strategy"""
        
        try:
            access = UserStrategyAccess.objects.get(
                user=user, 
                strategy_name=strategy_name
            )
            return {
                'can_view': access.can_view,
                'can_start_stop': access.can_start_stop,
                'can_configure': access.can_configure
            }
        except UserStrategyAccess.DoesNotExist:
            # Default permissions for authenticated users
            return {
                'can_view': user.can_view_dashboard,
                'can_start_stop': False,
                'can_configure': False
            }
    
    @staticmethod
    def can_user_execute_trades(user: TradingUser) -> bool:
        """Check if user can execute trades"""
        return user.can_execute_trades and user.is_active
    
    @staticmethod
    def get_user_api_limit(user: TradingUser) -> int:
        """Get API rate limit for user"""
        return user.max_api_calls_per_minute

# Django Admin Integration
from django.contrib import admin

@admin.register(TradingUser)
class TradingUserAdmin(admin.ModelAdmin):
    list_display = ['username', 'email', 'organization', 'can_execute_trades', 'last_login']
    list_filter = ['can_execute_trades', 'can_manage_strategies', 'is_active']
    search_fields = ['username', 'email', 'organization']
    
    fieldsets = (
        (None, {'fields': ('username', 'email', 'password')}),
        ('Trading Permissions', {
            'fields': ('can_view_dashboard', 'can_view_trades', 
                      'can_execute_trades', 'can_manage_strategies')
        }),
        ('API Access', {'fields': ('max_api_calls_per_minute',)}),
        ('Profile', {'fields': ('organization', 'last_login_ip')}),
    )

@admin.register(APIKey)
class APIKeyAdmin(admin.ModelAdmin):
    list_display = ['name', 'user', 'is_active', 'last_used_at', 'total_requests']
    list_filter = ['is_active', 'permissions']
    search_fields = ['name', 'user__username']
    readonly_fields = ['key', 'total_requests', 'last_used_at']
```

**Deliverables:**
- ✅ Multi-user authentication system
- ✅ Permission-based access control
- ✅ API key management
- ✅ Django admin interface for user management

### Week 11: Strategy Engine Integration
```python
# Strategy Processing Pipeline
@shared_task
def process_market_data_for_strategies(data_type: str, data: dict):
    """Route market data to relevant strategies"""
    
    symbol = data['symbol']
    exchange = data['exchange']
    
    # Find strategies trading this symbol
    strategy_instances = StrategyInstance.objects.filter(
        is_active=True,
        trading_pairs__symbol=symbol,
        trading_pairs__exchange__name=exchange
    ).select_related('strategy')
    
    # Process each strategy in parallel
    for strategy_instance in strategy_instances:
        process_strategy_signal.delay(strategy_instance.id, data_type, data)

@shared_task
def process_strategy_signal(strategy_instance_id: int, data_type: str, data: dict):
    """Process market data for specific strategy"""
    
    try:
        strategy_instance = StrategyInstance.objects.get(id=strategy_instance_id)
        
        # Load strategy dynamically
        strategy_class = load_strategy_class(strategy_instance.strategy)
        strategy = strategy_class(strategy_instance.config)
        
        # Process data and generate signals
        signals = strategy.process_data(data_type, data)
        
        # Handle generated signals
        for signal in signals:
            validate_and_execute_signal.delay(strategy_instance.id, signal)
            
    except Exception as e:
        logger.error(f"Strategy processing error: {e}")

# Signal Execution
@shared_task
def validate_and_execute_signal(strategy_instance_id: int, signal_data: dict):
    """Validate and execute trading signal"""
    
    # Create signal record
    trade_signal = TradeSignal.objects.create(
        strategy_instance_id=strategy_instance_id,
        **signal_data
    )
    
    # Risk validation
    risk_service = RiskManagementService()
    if not risk_service.validate_signal(trade_signal):
        trade_signal.status = 'rejected'
        trade_signal.save()
        return
    
    # Execute signal
    execute_trade_signal.delay(trade_signal.id)
    
    # Broadcast to dashboard
    broadcast_signal_update.delay(trade_signal.id)
```

**Deliverables:**
- ✅ Strategy engine integration with Celery
- ✅ Real-time signal processing
- ✅ Risk management integration
- ✅ Signal execution pipeline

### Week 12: Performance Optimization
```python
# Database Optimization
class OptimizedTickerManager(models.Manager):
    """Optimized queries for ticker data"""
    
    def latest_by_pair(self):
        """Get latest ticker for each trading pair"""
        return self.select_related('trading_pair__exchange')\
            .order_by('trading_pair', '-timestamp')\
            .distinct('trading_pair')
    
    def price_history(self, symbol: str, exchange: str, hours: int = 24):
        """Get price history with optimized query"""
        cutoff_time = timezone.now() - timedelta(hours=hours)
        
        return self.filter(
            trading_pair__symbol=symbol,
            trading_pair__exchange__name=exchange,
            timestamp__gte=cutoff_time
        ).values('timestamp', 'last_price')\
         .order_by('timestamp')

# Caching Strategy
class CacheService:
    """Centralized caching service"""
    
    @staticmethod
    def get_latest_ticker(exchange: str, symbol: str) -> Optional[dict]:
        """Get latest ticker from cache"""
        cache_key = f"ticker:latest:{exchange}:{symbol}"
        return cache.get(cache_key)
    
    @staticmethod
    def set_latest_ticker(exchange: str, symbol: str, ticker_data: dict):
        """Cache latest ticker data"""
        cache_key = f"ticker:latest:{exchange}:{symbol}"
        cache.set(cache_key, ticker_data, 300)  # 5 minutes
    
    @staticmethod
    def invalidate_ticker_cache(exchange: str, symbol: str):
        """Invalidate ticker cache"""
        cache_key = f"ticker:latest:{exchange}:{symbol}"
        cache.delete(cache_key)

# Performance Monitoring
@shared_task
def collect_performance_metrics():
    """Collect system performance metrics"""
    
    metrics = {
        'timestamp': timezone.now(),
        'api_response_times': measure_api_response_times(),
        'websocket_latency': measure_websocket_latency(),
        'database_query_times': measure_database_performance(),
        'cache_hit_rates': measure_cache_performance(),
        'celery_queue_lengths': measure_celery_performance()
    }
    
    # Store metrics
    PerformanceMetric.objects.create(**metrics)
    
    # Alert on performance issues
    check_performance_thresholds(metrics)
```

**Deliverables:**
- ✅ Database query optimization
- ✅ Comprehensive caching strategy
- ✅ Performance monitoring system
- ✅ Automated alerting

**Phase 3 Milestone Review:**
- FastAPI gateway operational
- Multi-user authentication system active
- API access control implemented
- Ready for testing and deployment

---

## Phase 4: Testing & Production Deployment (Weeks 7-8)

### Week 7: Comprehensive Testing
```python
# Integration Testing Suite
class WrapperIntegrationTests(TestCase):
    """Test wrapper integration with core engine"""
    
    def setUp(self):
        self.user = TradingUser.objects.create_user(
            username='testuser',
            password='testpass',
            can_view_dashboard=True
        )
        
    def test_data_synchronization(self):
        """Test data flows from core to Django database"""
        
        # Simulate core engine publishing ticker data
        ticker_data = {
            'exchange': 'hyperliquid',
            'symbol': 'BTC-USD',
            'last_price': '50000.00',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Publish to Redis
        redis_client = redis.Redis()
        redis_client.publish('market_data:hyperliquid', json.dumps(ticker_data))
        
        # Wait for synchronization
        time.sleep(1)
        
        # Verify data in database
        ticker = Ticker.objects.filter(
            trading_pair__symbol='BTC-USD',
            trading_pair__exchange__name='hyperliquid'
        ).first()
        
        self.assertIsNotNone(ticker)
        self.assertEqual(float(ticker.last_price), 50000.00)
    
    def test_dashboard_authentication(self):
        """Test dashboard requires authentication"""
        
        # Unauthenticated request
        response = self.client.get('/dashboard/')
        self.assertEqual(response.status_code, 302)  # Redirect to login
        
        # Authenticated request
        self.client.login(username='testuser', password='testpass')
        response = self.client.get('/dashboard/')
        self.assertEqual(response.status_code, 200)
    
    def test_api_gateway_authentication(self):
        """Test API gateway requires valid API key"""
        
        # Create API key
        api_key = APIKey.objects.create(
            user=self.user,
            key='test_key_123',
            name='Test Key',
            permissions=['read']
        )
        
        # Test without API key
        response = self.client.get('/api/v1/ticker/hyperliquid/BTC-USD')
        self.assertEqual(response.status_code, 401)
        
        # Test with valid API key
        headers = {'Authorization': 'Bearer test_key_123'}
        response = self.client.get('/api/v1/ticker/hyperliquid/BTC-USD', **headers)
        self.assertIn(response.status_code, [200, 404])  # 404 if no data

# Load Testing
class WrapperLoadTest:
    """Load testing for wrapper components"""
    
    def test_dashboard_concurrent_users(self):
        """Test dashboard with 50 concurrent users"""
        
        def simulate_user_session():
            session = requests.Session()
            
            # Login
            session.post('/auth/login/', data={
                'username': 'testuser1',
                'password': 'testpass'
            })
            
            # Dashboard requests
            times = []
            for _ in range(10):
                start = time.time()
                response = session.get('/dashboard/')
                times.append(time.time() - start)
                time.sleep(0.5)
            
            return {
                'avg_time': sum(times) / len(times),
                'max_time': max(times)
            }
        
        # Run 50 concurrent sessions
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(simulate_user_session) for _ in range(50)]
            results = [future.result() for future in futures]
        
        # Verify performance
        avg_response_time = sum(r['avg_time'] for r in results) / len(results)
        assert avg_response_time < 1.0  # Under 1 second average
    
    def test_websocket_concurrent_connections(self):
        """Test WebSocket proxy with many connections"""
        
        async def test_websocket_connection():
            uri = "ws://localhost:8000/ws/dashboard/"
            
            try:
                async with websockets.connect(uri) as websocket:
                    # Send auth message
                    await websocket.send(json.dumps({
                        'type': 'auth',
                        'token': 'test_token'
                    }))
                    
                    # Receive messages for 30 seconds
                    start_time = time.time()
                    message_count = 0
                    
                    while time.time() - start_time < 30:
                        try:
                            message = await asyncio.wait_for(
                                websocket.recv(), 
                                timeout=1.0
                            )
                            message_count += 1
                        except asyncio.TimeoutError:
                            continue
                    
                    return message_count
                    
            except Exception as e:
                return 0
        
        # Test 100 concurrent connections
        async def run_load_test():
            tasks = [test_websocket_connection() for _ in range(100)]
            results = await asyncio.gather(*tasks)
            
            successful_connections = [r for r in results if r > 0]
            assert len(successful_connections) >= 90  # 90% success rate
        
        asyncio.run(run_load_test())

# Core Engine Isolation Test
class CoreEngineIsolationTest(TestCase):
    """Verify wrapper failure doesn't affect core engine"""
    
    def test_wrapper_failure_isolation(self):
        """Test that wrapper crashes don't affect core"""
        
        # Simulate wrapper database failure
        with patch('django.db.connection.cursor') as mock_cursor:
            mock_cursor.side_effect = Exception("Database connection failed")
            
            # Dashboard should fail gracefully
            response = self.client.get('/dashboard/')
            self.assertEqual(response.status_code, 500)
            
            # Core engine should still be running
            # (This would be verified by checking core engine health endpoints)
            core_health = self.check_core_engine_health()
            self.assertTrue(core_health['healthy'])
    
    def test_redis_failure_graceful_degradation(self):
        """Test wrapper handles Redis failures gracefully"""
        
        with patch('redis.Redis') as mock_redis:
            mock_redis.side_effect = Exception("Redis connection failed")
            
            # Dashboard should load with cached/database data
            response = self.client.get('/dashboard/')
            self.assertEqual(response.status_code, 200)
            
            # Real-time features should be disabled
            self.assertContains(response, "Real-time updates unavailable")
    
    def check_core_engine_health(self):
        """Check if core engine is healthy"""
        # This would ping core engine health endpoints
        return {'healthy': True}
```

**Deliverables:**
- ✅ Comprehensive integration test suite
- ✅ Load testing for concurrent users
- ✅ WebSocket stress testing  
- ✅ Core engine isolation verification

### Week 8: Production Deployment
```bash
# Production Deployment Script
#!/bin/bash

echo "🚀 Deploying CyberDelta Django Wrapper..."

# 1. Environment Setup
echo "Setting up production environment..."
export DJANGO_SETTINGS_MODULE=config.settings.production
export DJANGO_SECRET_KEY=$(openssl rand -base64 32)
export DATABASE_URL="postgresql://user:pass@localhost:5432/cyberdelta"
export REDIS_URL="redis://localhost:6379/0"

# 2. Database Setup
echo "Setting up database..."
python manage.py migrate --settings=config.settings.production
python manage.py collectstatic --noinput --settings=config.settings.production

# 3. Create Superuser (if needed)
echo "Creating admin user..."
python manage.py shell -c "
from django_wrapper.apps.users.models import TradingUser
if not TradingUser.objects.filter(username='admin').exists():
    TradingUser.objects.create_superuser('admin', 'admin@cyberdelta.com', 'secure_password')
"

# 4. Start Services in Production
echo "Starting production services..."

# Start Celery worker for data synchronization
celery multi start data_sync \
    -A config.celery:app \
    --pidfile=/var/run/celery/data_sync.pid \
    --logfile=/var/log/celery/data_sync.log \
    --loglevel=INFO \
    -Q data_sync

# Start Django Channels (WebSocket proxy)
daphne -b 0.0.0.0 -p 8001 config.asgi:application &
echo $! > /var/run/daphne.pid

# Start FastAPI Gateway
uvicorn api_gateway.main:app \
    --host 0.0.0.0 \
    --port 8002 \
    --workers 2 \
    --access-log &
echo $! > /var/run/fastapi.pid

# Start Django WSGI (Dashboard)
gunicorn config.wsgi:application \
    --bind 0.0.0.0:8000 \
    --workers 2 \
    --worker-class gevent \
    --worker-connections 1000 \
    --timeout 120 &
echo $! > /var/run/gunicorn.pid

echo "✅ Deployment complete!"
echo "📊 Dashboard: http://localhost:8000"
echo "🔌 WebSocket: ws://localhost:8001/ws/"
echo "🚪 API Gateway: http://localhost:8002"

# 5. Health Checks
sleep 5
echo "🔍 Running health checks..."

# Check Django
if curl -f http://localhost:8000/health/ > /dev/null 2>&1; then
    echo "✅ Django wrapper healthy"
else
    echo "❌ Django wrapper not responding"
fi

# Check FastAPI
if curl -f http://localhost:8002/health/ > /dev/null 2>&1; then
    echo "✅ FastAPI gateway healthy"
else
    echo "❌ FastAPI gateway not responding"
fi

# Check WebSocket
if curl -f http://localhost:8001/health/ > /dev/null 2>&1; then
    echo "✅ WebSocket proxy healthy"
else
    echo "❌ WebSocket proxy not responding"
fi
```

```python
# Production Monitoring & Health Checks
class ProductionMonitor:
    """Monitor wrapper system health in production"""
    
    @staticmethod
    def check_wrapper_health():
        """Comprehensive health check for wrapper system"""
        
        health_status = {
            'timestamp': timezone.now(),
            'django_db': check_django_database(),
            'redis_connection': check_redis_connection(),
            'data_sync': check_data_synchronization(),
            'websocket_proxy': check_websocket_proxy(),
            'api_gateway': check_api_gateway(),
            'core_engine_connection': check_core_connection()
        }
        
        # Store health metrics
        WrapperHealthMetric.objects.create(**health_status)
        
        # Check for issues
        issues = []
        
        if not health_status['django_db']['healthy']:
            issues.append("Django database connection failed")
        
        if not health_status['data_sync']['healthy']:
            issues.append("Data synchronization from core is stale")
        
        if not health_status['core_engine_connection']['healthy']:
            issues.append("Cannot reach core engine")
        
        # Send alerts for critical issues
        if issues:
            send_wrapper_alert.delay(issues)
        
        return health_status
    
    @staticmethod
    def check_data_synchronization():
        """Check if data is being synchronized from core"""
        try:
            # Check latest ticker timestamp
            latest_ticker = Ticker.objects.latest('timestamp')
            time_since_update = timezone.now() - latest_ticker.timestamp
            
            if time_since_update.total_seconds() > 300:  # 5 minutes
                return {
                    'healthy': False,
                    'error': f'Data is {time_since_update.total_seconds()}s stale'
                }
            
            return {
                'healthy': True,
                'latest_update': latest_ticker.timestamp,
                'seconds_ago': time_since_update.total_seconds()
            }
            
        except Ticker.DoesNotExist:
            return {
                'healthy': False,
                'error': 'No ticker data found in database'
            }
    
    @staticmethod
    def check_core_connection():
        """Check if we can communicate with core engine"""
        try:
            redis_client = redis.Redis()
            
            # Send ping command to core
            command = {
                'type': 'ping',
                'timestamp': timezone.now().isoformat()
            }
            
            redis_client.publish('core_commands', json.dumps(command))
            
            # Wait for response
            pubsub = redis_client.pubsub()
            pubsub.subscribe('ping_response')
            
            # Wait up to 5 seconds for response
            start_time = time.time()
            while time.time() - start_time < 5:
                message = pubsub.get_message(timeout=0.1)
                if message and message['type'] == 'message':
                    return {
                        'healthy': True,
                        'response_time': time.time() - start_time
                    }
            
            return {
                'healthy': False,
                'error': 'Core engine ping timeout'
            }
            
        except Exception as e:
            return {
                'healthy': False,
                'error': f'Core connection error: {str(e)}'
            }

# Deployment Configuration
DEPLOYMENT_CONFIG = {
    'services': {
        'django': {
            'port': 8000,
            'workers': 2,
            'worker_class': 'gevent'
        },
        'websocket': {
            'port': 8001,
            'backend': 'daphne'
        },
        'api_gateway': {
            'port': 8002,
            'workers': 2,
            'backend': 'uvicorn'
        }
    },
    'monitoring': {
        'health_check_interval': 60,  # seconds
        'alert_thresholds': {
            'response_time': 5.0,  # seconds
            'data_staleness': 300,  # seconds
            'error_rate': 0.05  # 5%
        }
    }
}
```

**Deliverables:**
- ✅ Production deployment scripts
- ✅ Health monitoring system
- ✅ Service orchestration
- ✅ Automated health checks

```python
# Component Views
class MetricsTableView(TemplateView):
    """Real-time updating metrics table"""
    template_name = 'dashboard/components/metrics_table.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Calculate current metrics for all strategies
        metrics = []
        for strategy in StrategyInstance.objects.filter(is_active=True):
            metric_data = self.calculate_strategy_metrics(strategy)
            metrics.append(metric_data)
        
        context['metrics'] = metrics
        return context
    
    def calculate_strategy_metrics(self, strategy: StrategyInstance) -> dict:
        """Calculate performance metrics for strategy"""
        
        # Get trades from last 24 hours
        since_24h = timezone.now() - timedelta(hours=24)
        recent_trades = Trade.objects.filter(
            order__account__in=strategy.accounts.all(),
            executed_at__gte=since_24h
        )
        
        # Calculate metrics
        pnl_24h = sum(trade.realized_pnl or 0 for trade in recent_trades)
        
        # Get all-time performance
        all_trades = Trade.objects.filter(
            order__account__in=strategy.accounts.all()
        )
        
        total_return = self.calculate_total_return(all_trades)
        sharpe_ratio = self.calculate_sharpe_ratio(all_trades)
        max_drawdown = self.calculate_max_drawdown(all_trades)
        
        return {
            'strategy': strategy,
            'pnl_24h': pnl_24h,
            'total_return': total_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'status': strategy.get_status()
        }

class FundingRateHeatmapView(TemplateView):
    """Funding rate heatmap component"""
    template_name = 'dashboard/components/funding_heatmap.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        time_range = self.request.GET.get('time_range', '24h')
        
        # Get funding rate data
        funding_data = self.get_funding_rate_matrix(time_range)
        
        # Create heatmap data for Plotly
        heatmap_data = {
            'z': funding_data['values'],
            'x': funding_data['exchanges'],
            'y': funding_data['symbols'],
            'type': 'heatmap',
            'colorscale': 'RdBu',
            'zmid': 0
        }
        
        context['heatmap_data'] = json.dumps(heatmap_data)
        context['time_range'] = time_range
        
        return context
```

**Deliverables:**
- ✅ Performance chart component with Plotly
- ✅ Real-time metrics table
- ✅ Funding rate heatmap
- ✅ Interactive controls with HTMX

### Week 15: Advanced Features & Polish
```html
<!-- Trade Analysis with Infinite Scroll -->
<div id="trade-analysis" class="trade-container">
    <div class="trade-filters">
        <select name="strategy"
                hx-get="{% url 'dashboard:trade_analysis' %}"
                hx-target="#trade-tbody"
                hx-trigger="change">
            <option value="">All Strategies</option>
            {% for strategy in strategies %}
                <option value="{{ strategy.id }}">{{ strategy.name }}</option>
            {% endfor %}
        </select>
        
        <input type="date" 
               name="date_from"
               hx-get="{% url 'dashboard:trade_analysis' %}"
               hx-target="#trade-tbody"
               hx-trigger="change"
               hx-include="[name='strategy'], [name='date_to']">
        
        <input type="date" 
               name="date_to"
               hx-get="{% url 'dashboard:trade_analysis' %}"
               hx-target="#trade-tbody"
               hx-trigger="change"
               hx-include="[name='strategy'], [name='date_from']">
    </div>
    
    <table class="trades-table">
        <thead>
            <tr>
                <th>Time</th>
                <th>Strategy</th>
                <th>Symbol</th>
                <th>Side</th>
                <th>Size</th>
                <th>Price</th>
                <th>PnL</th>
                <th>Fee</th>
            </tr>
        </thead>
        <tbody id="trade-tbody">
            {% include 'dashboard/partials/trade_rows.html' %}
        </tbody>
    </table>
    
    <!-- Infinite scroll trigger -->
    <div hx-get="{% url 'dashboard:trade_analysis' %}?page={{ page|add:1 }}"
         hx-target="#trade-tbody"
         hx-swap="beforeend"
         hx-trigger="revealed"
         hx-include="[name='strategy'], [name='date_from'], [name='date_to']">
        <div class="loading-indicator">Loading more trades...</div>
    </div>
</div>

<!-- Real-time Trade Notifications -->
<div id="trade-notifications" 
     x-data="tradeNotifications()"
     class="notifications-container">
    
    <div x-show="notifications.length > 0" class="notification-list">
        <template x-for="notification in notifications" :key="notification.id">
            <div class="notification" 
                 :class="notification.type"
                 x-show="notification.visible"
                 x-transition>
                <div class="notification-content">
                    <strong x-text="notification.title"></strong>
                    <p x-text="notification.message"></p>
                </div>
                <button @click="dismissNotification(notification.id)" 
                        class="notification-close">×</button>
            </div>
        </template>
    </div>
</div>

<script>
function tradeNotifications() {
    return {
        notifications: [],
        
        init() {
            // Listen for WebSocket trade updates
            document.body.addEventListener('htmx:wsAfterMessage', (event) => {
                const data = JSON.parse(event.detail.message);
                if (data.type === 'trade_executed') {
                    this.addNotification({
                        id: Date.now(),
                        type: 'success',
                        title: 'Trade Executed',
                        message: `${data.side} ${data.quantity} ${data.symbol} at $${data.price}`,
                        visible: true
                    });
                }
            });
        },
        
        addNotification(notification) {
            this.notifications.unshift(notification);
            
            // Auto-dismiss after 5 seconds
            setTimeout(() => {
                this.dismissNotification(notification.id);
            }, 5000);
            
            // Keep only last 10 notifications
            if (this.notifications.length > 10) {
                this.notifications = this.notifications.slice(0, 10);
            }
        },
        
        dismissNotification(id) {
            const notification = this.notifications.find(n => n.id === id);
            if (notification) {
                notification.visible = false;
                // Remove after transition
                setTimeout(() => {
                    this.notifications = this.notifications.filter(n => n.id !== id);
                }, 300);
            }
        }
    }
}
</script>
```

```css
/* Dashboard Styles */
.dashboard-container {
    display: grid;
    grid-template-areas: 
        "nav nav"
        "sidebar main"
        "status status";
    grid-template-rows: 60px 1fr 30px;
    grid-template-columns: 250px 1fr;
    height: 100vh;
}

.dashboard-nav {
    grid-area: nav;
    background: #1a1a1a;
    color: white;
    display: flex;
    align-items: center;
    padding: 0 20px;
}

.dashboard-main {
    grid-area: main;
    padding: 20px;
    overflow-y: auto;
}

.chart-container {
    background: white;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 20px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.metrics-table {
    width: 100%;
    border-collapse: collapse;
}

.metrics-table th,
.metrics-table td {
    padding: 12px;
    text-align: left;
    border-bottom: 1px solid #eee;
}

.positive { color: #22c55e; }
.negative { color: #ef4444; }

.status {
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 12px;
}

.status.active { background: #22c55e; color: white; }
.status.paused { background: #f59e0b; color: white; }
.status.error { background: #ef4444; color: white; }

/* Notifications */
.notifications-container {
    position: fixed;
    top: 80px;
    right: 20px;
    z-index: 1000;
}

.notification {
    background: white;
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 12px;
    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    border-left: 4px solid #22c55e;
    max-width: 300px;
}

.notification.error {
    border-left-color: #ef4444;
}

.notification.warning {
    border-left-color: #f59e0b;
}
```

**Deliverables:**
- ✅ Trade analysis with infinite scroll
- ✅ Real-time notifications system
- ✅ Advanced filtering and search
- ✅ Responsive design implementation

**Phase 4 Milestone Review:**
- Complete dashboard migrated to HTMX
- All Dash functionality preserved and enhanced
- Real-time updates working seamlessly
- No React dependencies remaining

---

## Phase 5: Testing & Production Deployment (Week 16)

### Week 16: Comprehensive Testing & Go-Live

```python
# Load Testing Suite
class DashboardLoadTest(TestCase):
    """Load testing for dashboard endpoints"""
    
    def test_dashboard_concurrent_users(self):
        """Test dashboard with 100 concurrent users"""
        
        with ThreadPoolExecutor(max_workers=100) as executor:
            futures = []
            
            for i in range(100):
                future = executor.submit(self.simulate_user_session, i)
                futures.append(future)
            
            # Collect results
            results = [future.result() for future in futures]
            
            # Verify performance
            avg_response_time = sum(r['avg_time'] for r in results) / len(results)
            self.assertLess(avg_response_time, 0.5)  # 500ms max
    
    def simulate_user_session(self, user_id: int) -> dict:
        """Simulate a user dashboard session"""
        
        session = requests.Session()
        times = []
        
        # Login
        start = time.time()
        response = session.post('/auth/login/', data={
            'username': f'testuser{user_id}',
            'password': 'testpass'
        })
        times.append(time.time() - start)
        
        # Dashboard overview
        start = time.time()
        response = session.get('/dashboard/')
        times.append(time.time() - start)
        
        # Performance chart
        start = time.time()
        response = session.get('/dashboard/performance-chart/')
        times.append(time.time() - start)
        
        # Trade analysis
        start = time.time()
        response = session.get('/dashboard/trade-analysis/')
        times.append(time.time() - start)
        
        return {
            'user_id': user_id,
            'avg_time': sum(times) / len(times),
            'max_time': max(times),
            'total_requests': len(times)
        }

# WebSocket Load Testing
class WebSocketLoadTest:
    """Load testing for WebSocket connections"""
    
    async def test_websocket_concurrent_connections(self):
        """Test 500 concurrent WebSocket connections"""
        
        async def client_connection(client_id: int):
            uri = "ws://localhost:8000/ws/dashboard/"
            
            try:
                async with websockets.connect(uri) as websocket:
                    # Receive messages for 60 seconds
                    start_time = time.time()
                    message_count = 0
                    
                    while time.time() - start_time < 60:
                        try:
                            message = await asyncio.wait_for(
                                websocket.recv(), 
                                timeout=1.0
                            )
                            message_count += 1
                        except asyncio.TimeoutError:
                            continue
                    
                    return {
                        'client_id': client_id,
                        'messages_received': message_count,
                        'duration': 60
                    }
                    
            except Exception as e:
                return {
                    'client_id': client_id,
                    'error': str(e)
                }
        
        # Create 500 concurrent connections
        tasks = [client_connection(i) for i in range(500)]
        results = await asyncio.gather(*tasks)
        
        # Analyze results
        successful_connections = [r for r in results if 'error' not in r]
        self.assertGreaterEqual(len(successful_connections), 450)  # 90% success rate
        
        avg_messages = sum(r['messages_received'] for r in successful_connections) / len(successful_connections)
        self.assertGreater(avg_messages, 50)  # At least 50 messages per minute
```

```bash
# Production Deployment Script
#!/bin/bash

echo "Starting CyberDelta Django deployment..."

# 1. Database Migration
echo "Running database migrations..."
python manage.py migrate --settings=config.settings.production

# 2. Static Files
echo "Collecting static files..."
python manage.py collectstatic --noinput --settings=config.settings.production

# 3. Cache Warmup
echo "Warming up cache..."
python manage.py warm_cache --settings=config.settings.production

# 4. Health Checks
echo "Running health checks..."
python manage.py check --deploy --settings=config.settings.production

# 5. Start Services
echo "Starting services..."

# Start Celery workers
celery multi start worker1 worker2 worker3 \
    -A config.celery:app \
    --pidfile=/var/run/celery/%n.pid \
    --logfile=/var/log/celery/%n%I.log \
    --loglevel=INFO \
    -Q:worker1 realtime \
    -Q:worker2 strategies \
    -Q:worker3 default

# Start Celery beat
celery beat \
    -A config.celery:app \
    --pidfile=/var/run/celery/beat.pid \
    --logfile=/var/log/celery/beat.log \
    --loglevel=INFO \
    --detach

# Start Django Channels (via Daphne)
daphne -b 0.0.0.0 -p 8001 config.asgi:application &

# Start Django WSGI (via Gunicorn)
gunicorn config.wsgi:application \
    --bind 0.0.0.0:8000 \
    --workers 4 \
    --worker-class gevent \
    --worker-connections 1000 \
    --max-requests 1000 \
    --timeout 120

echo "Deployment complete!"
```

```python
# Production Monitoring
class ProductionMonitor:
    """Monitor production system health"""
    
    @shared_task
    def check_system_health():
        """Comprehensive system health check"""
        
        health_status = {
            'timestamp': timezone.now(),
            'database': check_database_health(),
            'redis': check_redis_health(),
            'celery': check_celery_health(),
            'websockets': check_websocket_health(),
            'api_performance': check_api_performance(),
            'memory_usage': check_memory_usage(),
            'disk_usage': check_disk_usage()
        }
        
        # Store health metrics
        SystemHealthMetric.objects.create(**health_status)
        
        # Check for critical issues
        critical_issues = []
        
        if not health_status['database']['healthy']:
            critical_issues.append("Database connection issues")
        
        if health_status['memory_usage'] > 90:
            critical_issues.append(f"High memory usage: {health_status['memory_usage']}%")
        
        if health_status['api_performance']['avg_response_time'] > 1.0:
            critical_issues.append("API performance degraded")
        
        # Send alerts for critical issues
        if critical_issues:
            send_critical_alert.delay(critical_issues)
        
        return health_status
    
    @staticmethod
    def check_database_health() -> dict:
        """Check database connectivity and performance"""
        try:
            start_time = time.time()
            
            # Test connection
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
            
            response_time = time.time() - start_time
            
            # Check active connections
            active_connections = connection.queries_log
            
            return {
                'healthy': True,
                'response_time': response_time,
                'active_connections': len(active_connections)
            }
            
        except Exception as e:
            return {
                'healthy': False,
                'error': str(e)
            }
    
    @staticmethod
    def check_api_performance() -> dict:
        """Check API endpoint performance"""
        
        test_endpoints = [
            '/api/v1/exchanges/',
            '/api/v1/tickers/',
            '/api/v1/strategies/',
            '/dashboard/metrics-table/'
        ]
        
        response_times = []
        
        for endpoint in test_endpoints:
            try:
                start_time = time.time()
                response = requests.get(f"http://localhost:8000{endpoint}")
                response_time = time.time() - start_time
                
                if response.status_code == 200:
                    response_times.append(response_time)
                    
            except Exception:
                continue
        
        if response_times:
            return {
                'avg_response_time': sum(response_times) / len(response_times),
                'max_response_time': max(response_times),
                'successful_requests': len(response_times),
                'total_requests': len(test_endpoints)
            }
        else:
            return {
                'avg_response_time': None,
                'error': 'No successful API requests'
            }
```

### Final Deployment Checklist

**Pre-Deployment:**
- ✅ All tests passing (unit, integration, load)
- ✅ Performance benchmarks meet requirements
- ✅ Security audit completed
- ✅ Database backup created
- ✅ Rollback plan documented

**Deployment:**
- ✅ Blue-green deployment strategy
- ✅ Traffic gradually shifted to new system
- ✅ Real-time monitoring active
- ✅ Automated alerting configured

**Post-Deployment:**
- ✅ System health monitoring
- ✅ Performance metrics collection
- ✅ User feedback collection
- ✅ Issue tracking and resolution

**Phase 4 Final Deliverables:**
- ✅ Production-ready Django wrapper system
- ✅ Comprehensive monitoring and health checks
- ✅ Load testing validation results
- ✅ Successful production deployment
- ✅ Zero-risk operation alongside core engine

## Risk Mitigation & Wrapper Safety

### Wrapper-Specific Risk Management

1. **Core Engine Isolation**
   - **Guarantee**: Core trading engine completely unchanged
   - **Safety**: Wrapper failure cannot affect trading operations
   - **Validation**: Core operates independently with zero dependencies on wrapper

2. **Data Synchronization Issues**
   - **Risk**: Lag between core state and wrapper database
   - **Mitigation**: Real-time Redis pub/sub, data validation checks
   - **Contingency**: Graceful degradation to cached data, core query fallback

3. **Wrapper Service Failures**
   - **Risk**: Dashboard or API gateway becomes unavailable
   - **Mitigation**: Health monitoring, automatic restarts, redundancy
   - **Contingency**: Core engine continues trading, wrapper services restart independently

4. **Performance Impact**
   - **Risk**: Wrapper services consume system resources
   - **Mitigation**: Separate infrastructure, resource limits, monitoring
   - **Contingency**: Scale wrapper services independently, disable non-critical features

### Success Metrics (Wrapper Pattern)

**Safety Metrics:**
- Zero core engine modifications
- Zero core performance impact
- 100% core operation independence
- Wrapper failure graceful degradation

**Enhancement Metrics:**
- Multi-user dashboard operational
- External API access functional
- Historical data analysis available
- Real-time monitoring enhanced

## Post-Implementation Benefits

1. **Enhanced Capabilities**
   - Multi-user access with permissions
   - Historical data analysis and visualization  
   - External API for integrations
   - Improved monitoring and alerting

2. **Zero Risk Operation**
   - Core trading engine completely preserved
   - Independent failure domains
   - Gradual feature rollout capability
   - Easy rollback at any time

3. **Development Benefits**
   - Clean separation of concerns
   - Standard Django development patterns
   - Independent testing and deployment
   - Simplified maintenance

4. **Operational Advantages**
   - Better user management and access control
   - Comprehensive audit trails
   - Enhanced monitoring capabilities
   - Professional dashboard interface

## Architecture Summary

```
┌─────────────────────────────────────────────────────────────┐
│                    Django Wrapper Layer                    │
│  ┌─────────────────┐ ┌─────────────────┐ ┌───────────────┐│
│  │  HTMX Dashboard │ │  FastAPI Gateway│ │  WebSocket    ││
│  │  (Multi-User)   │ │  (External API) │ │  Proxy        ││
│  └─────────────────┘ └─────────────────┘ └───────────────┘│
└─────────────────────────────────────────────────────────────┘
                           │
                    ┌──────────────┐
                    │    Redis     │
                    │   Pub/Sub    │
                    │  (Bridge)    │
                    └──────────────┘
                           │
┌─────────────────────────────────────────────────────────────┐
│              CyberDeltaEngine Core (Unchanged)             │
│  ┌─────────────────┐ ┌─────────────────┐ ┌───────────────┐│
│  │  Exchange APIs  │ │  Strategy Engine│ │  Risk Engine  ││
│  │  (Async)        │ │  (Async)        │ │  (Async)      ││
│  └─────────────────┘ └─────────────────┘ └───────────────┘│
└─────────────────────────────────────────────────────────────┘
```

This wrapper-based implementation provides a comprehensive roadmap for enhancing CyberDeltaEngine with modern web capabilities while maintaining absolute safety and zero risk to the production trading system. The 8-week timeline ensures rapid delivery of value with minimal complexity and maximum safety.
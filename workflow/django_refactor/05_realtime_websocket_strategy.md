# Django Refactor: Real-time WebSocket Strategy (Wrapper Pattern)

## Overview

This document outlines how to create a WebSocket proxy layer that forwards real-time data from the core CyberDeltaEngine to Django Channels clients, without modifying the core's existing WebSocket connections. The core maintains all exchange connections while Django provides multi-user WebSocket access.

## Current Real-time Architecture

### Current WebSocket Implementation
```python
# cyberdelta/apis/connectivity/ws_manager.py
class WebSocketManager:
    def __init__(self, url: str, message_handler):
        self.url = url
        self.message_handler = message_handler
        self.ws = None
        self.reconnect_attempts = 0

    async def connect(self):
        try:
            self.ws = await websockets.connect(self.url)
            asyncio.create_task(self._message_loop())
        except Exception as e:
            await self._handle_connection_error(e)

    async def _message_loop(self):
        try:
            async for message in self.ws:
                parsed_message = json.loads(message)
                await self.message_handler.handle(parsed_message)
        except websockets.exceptions.ConnectionClosed:
            await self._handle_disconnect()

# Usage in exchange APIs
class HyperliquidAPI:
    async def start_websocket(self):
        ws_manager = WebSocketManager(
            self.ws_url,
            HLWebSocketMessageHandler(self.data_processor)
        )
        await ws_manager.connect()
```

### Current Data Flow (Preserved)
```
Exchange WebSocket → Message Handler → Data Processor → Strategy Engine
                                    ↓
                                In-memory State
                                    ↓
                                Dash Dashboard
```

## Django Channels Wrapper Architecture

### New Architecture with Wrapper
```
                    ┌─────────────────────────────────┐
                    │     Core CyberDeltaEngine       │
                    │  (Unchanged - All WebSockets)   │
                    └──────────────┬──────────────────┘
                                   │
                           Publishes to Redis
                                   │
                    ┌──────────────┴──────────────────┐
                    │         Redis Pub/Sub           │
                    │    (market_data, trades, etc)   │
                    └──────────────┬──────────────────┘
                                   │
                    ┌──────────────┴──────────────────┐
                    │    Django Channels Consumer     │
                    │    (WebSocket Proxy Layer)      │
                    └──────────────┬──────────────────┘
                                   │
                         Broadcasts to Clients
                                   │
        ┌──────────────┬───────────┴────────┬──────────────┐
        │              │                    │              │
    Client 1       Client 2             Client 3       Client N
   (Trader A)     (Trader B)          (Analytics)    (Monitor)
```

### Core Components

#### 1. Core Engine Redis Publisher (Minimal Addition)
```python
# Add to core engine - cyberdelta/monitoring/redis_publisher.py
# This is the ONLY modification to core engine

import redis
import json
from typing import Dict, Any

class RedisDataPublisher:
    """Publishes real-time data to Redis for external consumers"""

    def __init__(self):
        self.redis_client = redis.Redis(decode_responses=True)
        self.enabled = True  # Can disable if not needed

    async def publish_ticker(self, exchange: str, ticker_data: Dict[str, Any]):
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

    async def publish_trade(self, trade_data: Dict[str, Any]):
        """Publish trade execution to Redis"""
        if not self.enabled:
            return

        try:
            message = {
                'type': 'trade',
                'data': trade_data,
                'timestamp': datetime.utcnow().isoformat()
            }
            self.redis_client.publish('trades', json.dumps(message))
        except Exception as e:
            logger.debug(f"Redis publish error: {e}")

    async def publish_position_update(self, position_data: Dict[str, Any]):
        """Publish position update to Redis"""
        if not self.enabled:
            return

        try:
            message = {
                'type': 'position',
                'data': position_data,
                'timestamp': datetime.utcnow().isoformat()
            }
            self.redis_client.publish('positions', json.dumps(message))
        except Exception as e:
            logger.debug(f"Redis publish error: {e}")

# Integration point in existing code (example)
# In cyberdelta/apis/hyperliquid/hl_ws_message_router.py
async def handle_ticker_message(self, message: Dict):
    """Existing ticker handler with Redis publish added"""
    # ... existing processing ...

    # Add this line to publish to Redis
    if hasattr(self, 'redis_publisher'):
        await self.redis_publisher.publish_ticker('hyperliquid', ticker_data)

    # ... rest of existing code ...
```

#### 2. Django Redis Listener Service
```python
# django_wrapper/apps/websocket/redis_listener.py

import asyncio
import redis
import json
from channels.layers import get_channel_layer
from django.core.management.base import BaseCommand

class RedisWebSocketBridge:
    """Bridges Redis pub/sub to Django Channels"""

    def __init__(self):
        self.redis_client = redis.Redis(decode_responses=True)
        self.channel_layer = get_channel_layer()
        self.subscriptions = [
            'market_data:hyperliquid',
            'market_data:backpack',
            'trades',
            'positions',
            'signals'
        ]

    async def start(self):
        """Start listening to Redis channels"""
        pubsub = self.redis_client.pubsub()

        # Subscribe to all channels
        for channel in self.subscriptions:
            pubsub.subscribe(channel)

        # Process messages
        while True:
            try:
                message = pubsub.get_message(timeout=1.0)
                if message and message['type'] == 'message':
                    await self.process_message(message)
            except Exception as e:
                logger.error(f"Redis listener error: {e}")
                await asyncio.sleep(1)

    async def process_message(self, message):
        """Process Redis message and broadcast to WebSocket clients"""
        try:
            channel = message['channel']
            data = json.loads(message['data'])

            # Route based on channel
            if channel.startswith('market_data:'):
                await self.broadcast_market_data(data)
            elif channel == 'trades':
                await self.broadcast_trade(data)
            elif channel == 'positions':
                await self.broadcast_position(data)
            elif channel == 'signals':
                await self.broadcast_signal(data)

        except Exception as e:
            logger.error(f"Message processing error: {e}")

    async def broadcast_market_data(self, data):
        """Broadcast market data to subscribed clients"""
        await self.channel_layer.group_send(
            "market_data",
            {
                "type": "market_data_update",
                "message": data
            }
        )

    async def broadcast_trade(self, data):
        """Broadcast trade to authorized clients"""
        await self.channel_layer.group_send(
            "trades",
            {
                "type": "trade_update",
                "message": data
            }
        )

# Management command to run the bridge
class Command(BaseCommand):
    help = 'Run Redis to WebSocket bridge'

    def handle(self, *args, **options):
        bridge = RedisWebSocketBridge()
        asyncio.run(bridge.start())
```

#### 3. Django Channels Consumers
```python
# django_wrapper/apps/websocket/consumers.py

import json
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async
from django.contrib.auth.models import AnonymousUser

class MarketDataConsumer(AsyncWebsocketConsumer):
    """WebSocket consumer for market data streaming"""

    async def connect(self):
        self.user = self.scope["user"]
        self.subscribed_symbols = set()

        # Accept connection
        await self.accept()

        # Join market data group
        await self.channel_layer.group_add("market_data", self.channel_name)

        # Send initial state from database
        await self.send_initial_state()

    async def disconnect(self, close_code):
        # Leave all groups
        await self.channel_layer.group_discard("market_data", self.channel_name)

    async def receive(self, text_data):
        """Handle client messages"""
        try:
            data = json.loads(text_data)
            command = data.get('command')

            if command == 'subscribe':
                symbols = data.get('symbols', [])
                self.subscribed_symbols.update(symbols)
                await self.send_confirmation('subscribed', symbols)

            elif command == 'unsubscribe':
                symbols = data.get('symbols', [])
                self.subscribed_symbols.difference_update(symbols)
                await self.send_confirmation('unsubscribed', symbols)

        except json.JSONDecodeError:
            await self.send_error('Invalid JSON')

    async def market_data_update(self, event):
        """Handle market data updates from Redis bridge"""
        message = event['message']

        # Filter based on client subscriptions
        if 'data' in message and 'symbol' in message['data']:
            symbol = message['data']['symbol']
            if not self.subscribed_symbols or symbol in self.subscribed_symbols:
                await self.send(text_data=json.dumps({
                    'type': 'market_data',
                    'data': message
                }))

    async def send_initial_state(self):
        """Send initial market state from database"""
        tickers = await self.get_latest_tickers()
        await self.send(text_data=json.dumps({
            'type': 'initial_state',
            'tickers': tickers
        }))

    @database_sync_to_async
    def get_latest_tickers(self):
        """Get latest tickers from database"""
        from django_wrapper.apps.persistence.models import Ticker

        tickers = {}
        for ticker in Ticker.objects.select_related('trading_pair').filter(
            timestamp__gte=timezone.now() - timedelta(minutes=5)
        ).order_by('trading_pair', '-timestamp').distinct('trading_pair'):
            tickers[ticker.trading_pair.symbol] = {
                'exchange': ticker.trading_pair.exchange.name,
                'symbol': ticker.trading_pair.symbol,
                'last_price': float(ticker.last_price),
                'bid': float(ticker.bid_price) if ticker.bid_price else None,
                'ask': float(ticker.ask_price) if ticker.ask_price else None,
                'timestamp': ticker.timestamp.isoformat()
            }

        return tickers

class TradingConsumer(AsyncWebsocketConsumer):
    """WebSocket consumer for trading operations"""

    async def connect(self):
        self.user = self.scope["user"]

        # Require authentication
        if not self.user.is_authenticated:
            await self.close(code=4001)
            return

        # Check trading permission
        if not await self.has_trading_permission():
            await self.close(code=4003)
            return

        await self.accept()

        # Join user-specific groups
        await self.channel_layer.group_add(f"user_{self.user.id}", self.channel_name)
        await self.channel_layer.group_add("trades", self.channel_name)

    async def disconnect(self, close_code):
        if hasattr(self, 'user') and self.user.is_authenticated:
            await self.channel_layer.group_discard(f"user_{self.user.id}", self.channel_name)
            await self.channel_layer.group_discard("trades", self.channel_name)

    async def receive(self, text_data):
        """Handle trading commands"""
        try:
            data = json.loads(text_data)
            command = data.get('command')

            if command == 'get_portfolio':
                await self.send_portfolio()
            elif command == 'get_positions':
                await self.send_positions()
            elif command == 'get_orders':
                await self.send_orders()

        except Exception as e:
            await self.send_error(str(e))

    async def trade_update(self, event):
        """Handle trade updates from Redis bridge"""
        # Only send trades for this user
        trade_data = event['message']['data']
        if trade_data.get('user_id') == self.user.id:
            await self.send(text_data=json.dumps({
                'type': 'trade',
                'data': trade_data
            }))

    @database_sync_to_async
    def has_trading_permission(self):
        """Check if user can view trading data"""
        return self.user.has_perm('trading.view_trades')
```

#### 3. WebSocket Routing Configuration
```python
# django_wrapper/config/routing.py

from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
from django.urls import re_path
from apps.websocket import consumers

websocket_urlpatterns = [
    re_path(r'ws/market-data/$', consumers.MarketDataConsumer.as_asgi()),
    re_path(r'ws/trading/$', consumers.TradingConsumer.as_asgi()),
]

application = ProtocolTypeRouter({
    'websocket': AuthMiddlewareStack(
        URLRouter(websocket_urlpatterns)
    ),
})

# Channel layers configuration
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [("127.0.0.1", 6379)],
            "capacity": 1500,
            "expiry": 60,
        },
    },
}
```

## Performance Optimization

### 1. WebSocket Connection Pooling
```python
# django_wrapper/apps/websocket/connection_pool.py

class WebSocketConnectionPool:
    """Manage WebSocket connections efficiently"""

    def __init__(self, max_connections: int = 100):
        self.max_connections = max_connections
        self.connections = {}
        self.connection_count = defaultdict(int)

    async def add_connection(self, user_id: int, channel_name: str):
        """Add connection to pool"""
        if user_id not in self.connections:
            self.connections[user_id] = set()

        self.connections[user_id].add(channel_name)
        self.connection_count[user_id] += 1

        # Limit connections per user
        if self.connection_count[user_id] > 5:
            oldest = list(self.connections[user_id])[0]
            await self.remove_connection(user_id, oldest)

    async def broadcast_to_user(self, user_id: int, message: dict):
        """Send message to all user connections"""
        if user_id in self.connections:
            channel_layer = get_channel_layer()
            for channel_name in self.connections[user_id]:
                await channel_layer.send(channel_name, message)

    async def remove_connection(self, user_id: int, channel_name: str):
        """Remove connection from pool"""
        if user_id in self.connections:
            self.connections[user_id].discard(channel_name)
            self.connection_count[user_id] -= 1

            if not self.connections[user_id]:
                del self.connections[user_id]
                del self.connection_count[user_id]
```

### 2. Message Filtering and Throttling
```python
# django_wrapper/apps/websocket/throttling.py

from datetime import datetime, timedelta
from collections import defaultdict
import asyncio

class MessageThrottler:
    """Throttle WebSocket messages per user/symbol"""

    def __init__(self):
        self.last_sent = defaultdict(lambda: defaultdict(datetime))
        self.min_intervals = {
            'ticker': timedelta(milliseconds=100),  # Max 10/second
            'trade': timedelta(milliseconds=50),    # Max 20/second
            'position': timedelta(seconds=1),       # Max 1/second
        }

    def should_send(self, user_id: int, message_type: str, symbol: str) -> bool:
        """Check if message should be sent based on throttling"""
        now = datetime.now()
        key = f"{message_type}:{symbol}"
        last = self.last_sent[user_id][key]

        min_interval = self.min_intervals.get(message_type, timedelta(seconds=0))

        if now - last >= min_interval:
            self.last_sent[user_id][key] = now
            return True

        return False

    def cleanup_old_entries(self):
        """Remove old throttling entries"""
        cutoff = datetime.now() - timedelta(minutes=5)

        for user_id in list(self.last_sent.keys()):
            user_data = self.last_sent[user_id]

            # Remove old entries
            for key in list(user_data.keys()):
                if user_data[key] < cutoff:
                    del user_data[key]

            # Remove empty user entries
            if not user_data:
                del self.last_sent[user_id]

# Integration in consumer
class OptimizedMarketDataConsumer(AsyncWebsocketConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.throttler = MessageThrottler()

    async def market_data_update(self, event):
        """Handle market data with throttling"""
        message = event['message']

        if 'data' in message and 'symbol' in message['data']:
            symbol = message['data']['symbol']
            message_type = message.get('type', 'ticker')

            # Apply throttling
            if self.throttler.should_send(self.user.id, message_type, symbol):
                await self.send(text_data=json.dumps({
                    'type': 'market_data',
                    'data': message
                }))
```

## Monitoring and Deployment

### 1. WebSocket Health Monitoring
```python
# django_wrapper/apps/monitoring/websocket_health.py

from django.core.management.commands.runserver import Command as RunServerCommand
import prometheus_client

# Prometheus metrics
websocket_connections = prometheus_client.Gauge(
    'websocket_active_connections',
    'Number of active WebSocket connections',
    ['consumer_type']
)
websocket_messages = prometheus_client.Counter(
    'websocket_messages_total',
    'Total WebSocket messages processed',
    ['direction', 'message_type']
)
websocket_errors = prometheus_client.Counter(
    'websocket_errors_total',
    'Total WebSocket errors',
    ['error_type']
)

class WebSocketHealthMonitor:
    """Monitor WebSocket health metrics"""

    @staticmethod
    def record_connection(consumer_type: str, delta: int):
        """Record connection change"""
        websocket_connections.labels(consumer_type=consumer_type).inc(delta)

    @staticmethod
    def record_message(direction: str, message_type: str):
        """Record message processed"""
        websocket_messages.labels(
            direction=direction,
            message_type=message_type
        ).inc()

    @staticmethod
    def record_error(error_type: str):
        """Record error occurrence"""
        websocket_errors.labels(error_type=error_type).inc()

    @staticmethod
    def check_redis_bridge_health() -> bool:
        """Check if Redis bridge is running"""
        redis_client = redis.Redis()

        # Check if we're receiving messages
        last_message_key = "redis_bridge:last_message"
        last_message_time = redis_client.get(last_message_key)

        if last_message_time:
            time_since_last = time.time() - float(last_message_time)
            if time_since_last > 60:  # No messages for 1 minute
                logger.warning(f"Redis bridge stale: {time_since_last}s since last message")
                return False

        return True
```

### 2. Deployment Configuration
```python
# django_wrapper/config/asgi.py

import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

application = ProtocolTypeRouter({
    "http": get_asgi_application(),
    "websocket": AuthMiddlewareStack(
        URLRouter(websocket_urlpatterns)
    ),
})

# Run with Daphne
# daphne -b 0.0.0.0 -p 8001 config.asgi:application

# Supervisor configuration
"""
[program:django_websocket]
command=/path/to/venv/bin/daphne -b 0.0.0.0 -p 8001 config.asgi:application
directory=/path/to/django_wrapper
autostart=true
autorestart=true
stderr_logfile=/var/log/django_websocket.err.log
stdout_logfile=/var/log/django_websocket.out.log

[program:redis_bridge]
command=/path/to/venv/bin/python manage.py run_redis_bridge
directory=/path/to/django_wrapper
autostart=true
autorestart=true
stderr_logfile=/var/log/redis_bridge.err.log
stdout_logfile=/var/log/redis_bridge.out.log
"""
```

## Benefits of Wrapper Approach

### Zero Impact on Core
1. **Unchanged WebSockets**: Core maintains all exchange connections
2. **No performance loss**: Core runs at full async speed
3. **Graceful degradation**: Django failure doesn't affect trading
4. **Easy rollback**: Can disable wrapper anytime

### Enhanced Capabilities
1. **Multi-user support**: Each user gets filtered data
2. **Authentication**: Proper user/permission management
3. **Scalability**: Django Channels scales horizontally
4. **Monitoring**: Better metrics and health checks

### Simplified Architecture
1. **Clear separation**: Core trading vs user interface
2. **Standard patterns**: Django Channels best practices
3. **Easy testing**: Mock Redis pub/sub for tests
4. **Gradual migration**: Add features incrementally

## Summary

The WebSocket wrapper strategy provides real-time data access to multiple users without modifying the core CyberDeltaEngine. Key points:

- **Minimal core changes**: Only add Redis publisher (can disable)
- **Full isolation**: Core and Django run independently
- **Performance maintained**: No impact on trading latency
- **Enhanced features**: Multi-user, auth, monitoring
- **Production ready**: Battle-tested Django Channels

Total implementation: 1 week as part of the 8-week project.

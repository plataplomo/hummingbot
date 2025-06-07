# Django Refactor: Real-time WebSocket Strategy

## Overview

One of the most critical aspects of CyberDeltaEngine is real-time data handling. The current system processes market data, order updates, and funding rates in real-time using async WebSockets. This document outlines the strategy for migrating to Django Channels while maintaining performance and reliability.

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

### Current Data Flow
```
Exchange WebSocket → Message Handler → Data Processor → Strategy Engine
                                    ↓
                                Database (in-memory)
                                    ↓
                                Dashboard Updates
```

## Django Channels Architecture

### Proposed Architecture Overview
```
Exchange WebSocket → Celery Worker → Database → Django Channels → Frontend
                         ↓
                   Signal Processing → Strategy Engine (Celery)
                         ↓
                   Order Placement (Celery)
```

### Core Components

#### 1. Exchange WebSocket Consumers (Celery Workers)
```python
# apps/exchanges/websocket_workers.py

import asyncio
import websockets
import json
from celery import shared_task
from django.core.cache import cache
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

@shared_task(bind=True)
def hyperliquid_websocket_worker(self):
    """Celery worker to handle Hyperliquid WebSocket connection"""
    
    async def websocket_handler():
        uri = "wss://api.hyperliquid.xyz/ws"
        
        while True:
            try:
                async with websockets.connect(uri) as websocket:
                    # Subscribe to channels
                    subscribe_msg = {
                        "method": "subscribe",
                        "subscription": {
                            "type": "allMids"
                        }
                    }
                    await websocket.send(json.dumps(subscribe_msg))
                    
                    async for message in websocket:
                        await process_hyperliquid_message(message)
                        
            except websockets.exceptions.ConnectionClosed:
                logger.warning("Hyperliquid WebSocket connection closed, reconnecting...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Hyperliquid WebSocket error: {e}")
                await asyncio.sleep(10)
    
    # Run the async WebSocket handler
    asyncio.run(websocket_handler())

async def process_hyperliquid_message(message: str):
    """Process incoming WebSocket message"""
    try:
        data = json.loads(message)
        
        if data.get('channel') == 'allMids':
            await process_ticker_update(data)
        elif data.get('channel') == 'trades':
            await process_trade_update(data)
        elif data.get('channel') == 'user':
            await process_user_update(data)
            
    except Exception as e:
        logger.error(f"Error processing message: {e}")

async def process_ticker_update(data: dict):
    """Process ticker update and broadcast to clients"""
    
    # Transform to internal format
    ticker_data = HLTickerMapper.to_internal(data)
    
    # Store in database (sync operation in async context)
    from asgiref.sync import sync_to_async
    
    @sync_to_async
    def save_ticker():
        trading_pair = TradingPair.objects.get(
            exchange__name='hyperliquid',
            symbol=ticker_data['symbol']
        )
        
        ticker = Ticker.objects.create(
            trading_pair=trading_pair,
            last_price=ticker_data['last_price'],
            bid_price=ticker_data['bid_price'],
            ask_price=ticker_data['ask_price'],
            timestamp=ticker_data['timestamp']
        )
        return ticker
    
    ticker = await save_ticker()
    
    # Update cache for fast access
    cache_key = f"ticker:hyperliquid:{ticker_data['symbol']}"
    cache.set(cache_key, ticker_data, 300)  # 5 minutes
    
    # Broadcast to WebSocket clients
    channel_layer = get_channel_layer()
    await channel_layer.group_send("market_data", {
        "type": "ticker_update",
        "ticker": ticker_data
    })
    
    # Trigger strategy processing
    strategy_data_update.delay('ticker', ticker_data)
```

#### 2. Django Channels Consumers
```python
# apps/dashboard/consumers.py

import json
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async
from django.contrib.auth.models import AnonymousUser

class DashboardConsumer(AsyncWebsocketConsumer):
    """WebSocket consumer for dashboard real-time updates"""
    
    async def connect(self):
        self.user = self.scope["user"]
        
        # Add to groups based on user permissions
        await self.channel_layer.group_add("dashboard", self.channel_name)
        await self.channel_layer.group_add("market_data", self.channel_name)
        
        if await self.has_trading_permission():
            await self.channel_layer.group_add("trading_updates", self.channel_name)
        
        await self.accept()
        
        # Send initial data
        await self.send_initial_data()
    
    async def disconnect(self, close_code):
        # Remove from all groups
        await self.channel_layer.group_discard("dashboard", self.channel_name)
        await self.channel_layer.group_discard("market_data", self.channel_name)
        await self.channel_layer.group_discard("trading_updates", self.channel_name)
    
    async def receive(self, text_data):
        """Handle messages from WebSocket"""
        try:
            data = json.loads(text_data)
            message_type = data.get('type')
            
            if message_type == 'subscribe_symbol':
                await self.subscribe_to_symbol(data.get('symbol'))
            elif message_type == 'unsubscribe_symbol':
                await self.unsubscribe_from_symbol(data.get('symbol'))
            elif message_type == 'get_chart_data':
                await self.send_chart_data(data.get('symbol'), data.get('timeframe'))
                
        except json.JSONDecodeError:
            await self.send(text_data=json.dumps({
                'error': 'Invalid JSON'
            }))
    
    # Group message handlers
    async def ticker_update(self, event):
        """Handle ticker update from exchange WebSocket worker"""
        await self.send(text_data=json.dumps({
            'type': 'ticker_update',
            'data': event['ticker']
        }))
    
    async def trade_update(self, event):
        """Handle trade execution update"""
        await self.send(text_data=json.dumps({
            'type': 'trade_update',
            'data': event['trade']
        }))
    
    async def strategy_update(self, event):
        """Handle strategy performance update"""
        await self.send(text_data=json.dumps({
            'type': 'strategy_update',
            'data': event['strategy_data']
        }))
    
    async def funding_rate_update(self, event):
        """Handle funding rate update"""
        await self.send(text_data=json.dumps({
            'type': 'funding_rate_update',
            'data': event['funding_data']
        }))
    
    # Helper methods
    @database_sync_to_async
    def has_trading_permission(self):
        """Check if user has trading permissions"""
        if isinstance(self.user, AnonymousUser):
            return False
        return self.user.has_perm('trading.can_trade')
    
    async def send_initial_data(self):
        """Send initial dashboard data on connection"""
        
        # Get latest tickers
        latest_tickers = await self.get_latest_tickers()
        await self.send(text_data=json.dumps({
            'type': 'initial_data',
            'tickers': latest_tickers
        }))
        
        # Get strategy performance
        strategy_performance = await self.get_strategy_performance()
        await self.send(text_data=json.dumps({
            'type': 'initial_data',
            'strategy_performance': strategy_performance
        }))
    
    @database_sync_to_async
    def get_latest_tickers(self):
        """Get latest ticker data for all symbols"""
        from apps.market_data.models import Ticker
        
        latest_tickers = []
        for ticker in Ticker.objects.select_related('trading_pair').order_by(
            'trading_pair', '-timestamp'
        ).distinct('trading_pair'):
            latest_tickers.append({
                'symbol': ticker.trading_pair.symbol,
                'exchange': ticker.trading_pair.exchange.name,
                'last_price': float(ticker.last_price),
                'timestamp': ticker.timestamp.isoformat()
            })
        
        return latest_tickers

class TradingConsumer(AsyncWebsocketConsumer):
    """Dedicated consumer for trading operations"""
    
    async def connect(self):
        self.user = self.scope["user"]
        
        # Only allow authenticated users with trading permissions
        if not await self.can_trade():
            await self.close(code=4003)  # Forbidden
            return
        
        await self.channel_layer.group_add("trading_updates", self.channel_name)
        await self.accept()
    
    async def receive(self, text_data):
        """Handle trading commands"""
        try:
            data = json.loads(text_data)
            command = data.get('command')
            
            if command == 'place_order':
                await self.place_order(data)
            elif command == 'cancel_order':
                await self.cancel_order(data)
            elif command == 'get_positions':
                await self.send_positions()
                
        except Exception as e:
            await self.send(text_data=json.dumps({
                'error': str(e)
            }))
    
    async def place_order(self, order_data):
        """Place order via WebSocket"""
        # Validate order data
        # Submit to Celery task
        # Send confirmation
        
        task_id = await self.submit_order_task(order_data)
        await self.send(text_data=json.dumps({
            'type': 'order_submitted',
            'task_id': task_id
        }))
    
    @database_sync_to_async
    def can_trade(self):
        """Check if user can trade"""
        return (
            self.user.is_authenticated and 
            self.user.has_perm('trading.can_trade')
        )
```

#### 3. Celery-based Strategy Processing
```python
# apps/strategies/tasks.py

@shared_task
def strategy_data_update(data_type: str, data: dict):
    """Process new market data for strategies"""
    
    if data_type == 'ticker':
        process_ticker_for_strategies(data)
    elif data_type == 'funding_rate':
        process_funding_rate_for_strategies(data)
    elif data_type == 'trade':
        process_trade_for_strategies(data)

def process_ticker_for_strategies(ticker_data: dict):
    """Process ticker update for all active strategies"""
    
    symbol = ticker_data['symbol']
    exchange = ticker_data['exchange']
    
    # Get strategies that trade this symbol
    strategies = StrategyInstance.objects.filter(
        is_active=True,
        trading_pairs__symbol=symbol,
        trading_pairs__exchange__name=exchange
    ).select_related('strategy')
    
    for strategy_instance in strategies:
        # Process in separate task to avoid blocking
        process_strategy_signal.delay(strategy_instance.id, ticker_data)

@shared_task
def process_strategy_signal(strategy_instance_id: int, market_data: dict):
    """Process market data for specific strategy"""
    
    try:
        strategy_instance = StrategyInstance.objects.get(id=strategy_instance_id)
        
        # Load strategy module dynamically
        strategy_module = importlib.import_module(strategy_instance.strategy.module_path)
        strategy_class = getattr(strategy_module, strategy_instance.strategy.name)
        
        # Initialize strategy with current state
        strategy = strategy_class(
            config=strategy_instance.config,
            data_source=DatabaseDataSource()
        )
        
        # Process market data
        signals = strategy.process_market_data(market_data)
        
        # Handle generated signals
        for signal in signals:
            handle_trade_signal.delay(strategy_instance.id, signal)
            
    except Exception as e:
        logger.error(f"Error processing strategy signal: {e}")

@shared_task
def handle_trade_signal(strategy_instance_id: int, signal_data: dict):
    """Handle trade signal from strategy"""
    
    try:
        strategy_instance = StrategyInstance.objects.get(id=strategy_instance_id)
        
        # Create signal record
        trade_signal = TradeSignal.objects.create(
            strategy_instance=strategy_instance,
            signal_id=signal_data['signal_id'],
            trading_pair_id=signal_data['trading_pair_id'],
            signal_type=signal_data['signal_type'],
            side=signal_data['side'],
            quantity=signal_data['quantity'],
            price=signal_data.get('price'),
            confidence=signal_data.get('confidence'),
            metadata=signal_data.get('metadata', {})
        )
        
        # Risk validation
        risk_service = RiskManagementService()
        if not risk_service.validate_signal(trade_signal):
            trade_signal.status = 'rejected'
            trade_signal.save()
            return
        
        # Execute signal
        execute_trade_signal.delay(trade_signal.id)
        
        # Broadcast signal to dashboard
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)("dashboard", {
            "type": "trade_signal_update",
            "signal": {
                "id": trade_signal.id,
                "strategy": strategy_instance.name,
                "symbol": trade_signal.trading_pair.symbol,
                "side": trade_signal.side,
                "quantity": float(trade_signal.quantity),
                "price": float(trade_signal.price) if trade_signal.price else None
            }
        })
        
    except Exception as e:
        logger.error(f"Error handling trade signal: {e}")
```

## Performance Optimization

### 1. Message Queuing and Batching
```python
# apps/exchanges/message_processing.py

class MessageBatcher:
    """Batch WebSocket messages for efficient processing"""
    
    def __init__(self, batch_size: int = 100, batch_timeout: int = 1):
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.ticker_batch = []
        self.trade_batch = []
        self.last_flush = time.time()
    
    async def add_ticker(self, ticker_data: dict):
        """Add ticker to batch"""
        self.ticker_batch.append(ticker_data)
        
        if len(self.ticker_batch) >= self.batch_size:
            await self.flush_tickers()
    
    async def add_trade(self, trade_data: dict):
        """Add trade to batch"""
        self.trade_batch.append(trade_data)
        
        if len(self.trade_batch) >= self.batch_size:
            await self.flush_trades()
    
    async def flush_tickers(self):
        """Flush ticker batch to database"""
        if not self.ticker_batch:
            return
        
        # Bulk create tickers
        bulk_create_tickers.delay(self.ticker_batch.copy())
        
        # Broadcast to WebSocket clients
        channel_layer = get_channel_layer()
        await channel_layer.group_send("market_data", {
            "type": "ticker_batch_update",
            "tickers": self.ticker_batch.copy()
        })
        
        self.ticker_batch.clear()
        self.last_flush = time.time()

@shared_task
def bulk_create_tickers(ticker_data_list: list[dict]):
    """Efficiently create multiple tickers"""
    
    ticker_objects = []
    for data in ticker_data_list:
        try:
            trading_pair = TradingPair.objects.get(
                exchange__name=data['exchange'],
                symbol=data['symbol']
            )
            
            ticker = Ticker(
                trading_pair=trading_pair,
                last_price=data['last_price'],
                bid_price=data.get('bid_price'),
                ask_price=data.get('ask_price'),
                timestamp=datetime.fromisoformat(data['timestamp'])
            )
            ticker_objects.append(ticker)
            
        except TradingPair.DoesNotExist:
            logger.warning(f"Trading pair not found: {data['exchange']}:{data['symbol']}")
    
    # Bulk create with conflict handling
    if ticker_objects:
        Ticker.objects.bulk_create(
            ticker_objects,
            batch_size=1000,
            ignore_conflicts=True
        )
```

### 2. Redis-based Channel Layer Configuration
```python
# settings.py

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [("127.0.0.1", 6379)],
            "symmetric_encryption_keys": [SECRET_KEY],
            "capacity": 1500,  # Maximum messages to store
            "expiry": 60,      # Message expiry time
            "group_expiry": 86400,  # Group expiry time
            "prefix": "cyberdelta:",
        },
    },
}

# Celery configuration for real-time tasks
CELERY_TASK_ROUTES = {
    'apps.exchanges.tasks.process_websocket_message': {'queue': 'realtime'},
    'apps.strategies.tasks.process_strategy_signal': {'queue': 'strategies'},
    'apps.trading.tasks.execute_trade_signal': {'queue': 'trading'},
}

CELERY_WORKER_POOL_RESTARTS = True
CELERY_WORKER_MAX_TASKS_PER_CHILD = 1000
```

### 3. Connection Management
```python
# apps/exchanges/connection_manager.py

class WebSocketConnectionManager:
    """Manage WebSocket connections with automatic reconnection"""
    
    def __init__(self):
        self.connections = {}
        self.reconnect_delays = {}
        self.max_reconnect_delay = 300  # 5 minutes
    
    async def maintain_connection(self, exchange_name: str, ws_url: str):
        """Maintain WebSocket connection with exponential backoff"""
        
        while True:
            try:
                logger.info(f"Connecting to {exchange_name} WebSocket...")
                
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10
                ) as websocket:
                    
                    self.connections[exchange_name] = websocket
                    self.reconnect_delays[exchange_name] = 1  # Reset delay
                    
                    # Handle messages
                    await self.handle_messages(exchange_name, websocket)
                    
            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"{exchange_name} WebSocket connection closed")
                await self.handle_reconnect(exchange_name)
                
            except Exception as e:
                logger.error(f"{exchange_name} WebSocket error: {e}")
                await self.handle_reconnect(exchange_name)
    
    async def handle_reconnect(self, exchange_name: str):
        """Handle reconnection with exponential backoff"""
        
        delay = self.reconnect_delays.get(exchange_name, 1)
        delay = min(delay * 2, self.max_reconnect_delay)
        self.reconnect_delays[exchange_name] = delay
        
        logger.info(f"Reconnecting to {exchange_name} in {delay} seconds...")
        await asyncio.sleep(delay)
    
    async def handle_messages(self, exchange_name: str, websocket):
        """Handle incoming messages for an exchange"""
        
        batcher = MessageBatcher()
        
        async for message in websocket:
            try:
                data = json.loads(message)
                
                # Route message based on type
                if self.is_ticker_message(data):
                    await batcher.add_ticker(self.parse_ticker(exchange_name, data))
                elif self.is_trade_message(data):
                    await batcher.add_trade(self.parse_trade(exchange_name, data))
                
                # Periodic flush
                if time.time() - batcher.last_flush > batcher.batch_timeout:
                    await batcher.flush_all()
                    
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON from {exchange_name}: {message}")
            except Exception as e:
                logger.error(f"Error processing message from {exchange_name}: {e}")
```

## Monitoring and Health Checks

### 1. WebSocket Health Monitoring
```python
# apps/monitoring/websocket_monitor.py

@shared_task
def monitor_websocket_health():
    """Monitor WebSocket connection health"""
    
    for exchange in Exchange.objects.filter(is_active=True):
        # Check last message timestamp
        last_message_key = f"ws_last_message:{exchange.name}"
        last_message_time = cache.get(last_message_key)
        
        if last_message_time:
            time_since_last = time.time() - last_message_time
            
            if time_since_last > 300:  # 5 minutes
                logger.warning(f"No messages from {exchange.name} for {time_since_last} seconds")
                
                # Trigger alert
                send_websocket_alert.delay(exchange.name, time_since_last)
        
        # Check connection status
        connection_key = f"ws_connection:{exchange.name}"
        is_connected = cache.get(connection_key, False)
        
        if not is_connected:
            logger.error(f"WebSocket for {exchange.name} is disconnected")
            restart_websocket_worker.delay(exchange.name)

@shared_task
def restart_websocket_worker(exchange_name: str):
    """Restart WebSocket worker for exchange"""
    
    # Revoke existing worker
    app.control.revoke(f"websocket_worker_{exchange_name}", terminate=True)
    
    # Start new worker
    if exchange_name == "hyperliquid":
        hyperliquid_websocket_worker.delay()
    elif exchange_name == "backpack":
        backpack_websocket_worker.delay()
```

### 2. Performance Metrics
```python
# apps/monitoring/performance_metrics.py

class WebSocketMetrics:
    """Track WebSocket performance metrics"""
    
    @staticmethod
    def record_message_received(exchange_name: str, message_type: str):
        """Record message received"""
        timestamp = time.time()
        
        # Update message count
        count_key = f"ws_msg_count:{exchange_name}:{message_type}"
        cache.set(count_key, cache.get(count_key, 0) + 1, 3600)
        
        # Update last message time
        last_msg_key = f"ws_last_message:{exchange_name}"
        cache.set(last_msg_key, timestamp, 3600)
    
    @staticmethod
    def record_processing_time(exchange_name: str, message_type: str, duration: float):
        """Record message processing time"""
        
        # Store in time series for analysis
        metric_key = f"ws_processing_time:{exchange_name}:{message_type}"
        
        # Keep last 100 measurements
        measurements = cache.get(metric_key, [])
        measurements.append({
            'timestamp': time.time(),
            'duration': duration
        })
        
        # Keep only recent measurements
        cutoff_time = time.time() - 3600  # 1 hour
        measurements = [m for m in measurements if m['timestamp'] > cutoff_time]
        
        cache.set(metric_key, measurements[-100:], 3600)
    
    @staticmethod
    def get_performance_stats(exchange_name: str) -> dict:
        """Get performance statistics"""
        
        stats = {}
        
        # Message counts
        for msg_type in ['ticker', 'trade', 'funding']:
            count_key = f"ws_msg_count:{exchange_name}:{msg_type}"
            stats[f"{msg_type}_count"] = cache.get(count_key, 0)
        
        # Processing times
        for msg_type in ['ticker', 'trade', 'funding']:
            metric_key = f"ws_processing_time:{exchange_name}:{msg_type}"
            measurements = cache.get(metric_key, [])
            
            if measurements:
                durations = [m['duration'] for m in measurements]
                stats[f"{msg_type}_avg_processing"] = sum(durations) / len(durations)
                stats[f"{msg_type}_max_processing"] = max(durations)
        
        return stats
```

## Testing Strategy

### 1. WebSocket Testing
```python
# tests/test_websocket_consumers.py

import pytest
from channels.testing import WebsocketCommunicator
from django.test import TransactionTestCase
from apps.dashboard.consumers import DashboardConsumer

class TestDashboardConsumer(TransactionTestCase):
    
    async def test_dashboard_consumer_connection(self):
        """Test dashboard WebSocket connection"""
        
        communicator = WebsocketCommunicator(DashboardConsumer.as_asgi(), "/ws/dashboard/")
        connected, subprotocol = await communicator.connect()
        
        assert connected
        
        # Test initial data
        response = await communicator.receive_json_from()
        assert response['type'] == 'initial_data'
        
        await communicator.disconnect()
    
    async def test_ticker_update_broadcast(self):
        """Test ticker update broadcasting"""
        
        communicator = WebsocketCommunicator(DashboardConsumer.as_asgi(), "/ws/dashboard/")
        await communicator.connect()
        
        # Simulate ticker update
        from channels.layers import get_channel_layer
        channel_layer = get_channel_layer()
        
        await channel_layer.group_send("market_data", {
            "type": "ticker_update",
            "ticker": {
                "symbol": "BTC-USDC",
                "exchange": "hyperliquid",
                "last_price": 50000.0
            }
        })
        
        # Verify message received
        response = await communicator.receive_json_from()
        assert response['type'] == 'ticker_update'
        assert response['data']['symbol'] == 'BTC-USDC'
        
        await communicator.disconnect()
```

### 2. Load Testing
```python
# tests/load_test_websockets.py

import asyncio
import websockets
import json
import time
from concurrent.futures import ThreadPoolExecutor

async def test_websocket_load():
    """Load test WebSocket consumers"""
    
    async def client_connection(client_id: int):
        uri = "ws://localhost:8000/ws/dashboard/"
        
        try:
            async with websockets.connect(uri) as websocket:
                # Send subscription message
                await websocket.send(json.dumps({
                    "type": "subscribe_symbol",
                    "symbol": "BTC-USDC"
                }))
                
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
                
                print(f"Client {client_id}: received {message_count} messages")
                
        except Exception as e:
            print(f"Client {client_id} error: {e}")
    
    # Create 100 concurrent connections
    tasks = []
    for i in range(100):
        tasks.append(client_connection(i))
    
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(test_websocket_load())
```

This real-time WebSocket strategy provides a robust foundation for maintaining the high-performance requirements of CyberDeltaEngine while leveraging Django's ecosystem benefits.
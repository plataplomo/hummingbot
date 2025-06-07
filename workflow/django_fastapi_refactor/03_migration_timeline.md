# Django + FastAPI Refactor: Migration Timeline & Implementation Strategy

## Executive Summary

This document outlines a comprehensive 20-week migration plan to transform CyberDeltaEngine from its current async Python + Dash architecture to a hybrid Django + FastAPI + HTMX system. The migration is structured to minimize trading downtime while providing incremental value and maintaining system reliability.

## Migration Principles

### Core Philosophy
1. **Zero Trading Downtime**: Current trading operations continue throughout migration
2. **Incremental Value Delivery**: Each phase provides tangible business benefits
3. **Risk Mitigation**: Comprehensive testing and rollback capabilities at every stage
4. **Performance Parity**: New system matches or exceeds current performance
5. **Parallel Development**: Build new system alongside existing one
6. **Data Integrity**: Ensure no loss of historical data or configurations

### Success Criteria
- **Performance**: API response times < 50ms, WebSocket latency < 25ms
- **Reliability**: 99.95% uptime for trading operations
- **Functionality**: 100% feature parity plus new capabilities
- **Scalability**: Support for 10x current trading volume
- **Maintainability**: Reduced development time for new features

## Phase 1: Foundation & Database Infrastructure (Weeks 1-4)

### Week 1: Project Setup & Architecture
```bash
# Day 1-2: Repository Structure
django_fastapi_cyberdelta/
├── django_web/                    # Django web application
│   ├── manage.py
│   ├── config/
│   │   ├── settings/
│   │   │   ├── base.py
│   │   │   ├── development.py
│   │   │   ├── production.py
│   │   │   └── testing.py
│   │   ├── urls.py
│   │   ├── wsgi.py
│   │   └── asgi.py
│   ├── apps/
│   │   ├── accounts/
│   │   ├── dashboard/
│   │   ├── configuration/
│   │   ├── analytics/
│   │   └── api/
│   └── requirements/
├── fastapi_engine/                # FastAPI trading engine
│   ├── main.py
│   ├── routers/
│   ├── services/
│   ├── models/
│   ├── dependencies/
│   └── requirements.txt
├── fastapi_market_data/           # FastAPI market data service
│   ├── main.py
│   ├── routers/
│   ├── services/
│   └── websocket/
├── shared/                        # Shared utilities and models
│   ├── database/
│   ├── messaging/
│   ├── monitoring/
│   └── testing/
├── docker/                        # Docker configurations
│   ├── django/
│   ├── fastapi/
│   ├── postgres/
│   ├── redis/
│   └── nginx/
├── scripts/                       # Migration and deployment scripts
├── tests/                         # Integration tests
└── docs/                          # Architecture documentation

# Day 3-5: Infrastructure Setup
# PostgreSQL + TimescaleDB configuration
# Redis configuration for caching and messaging
# Docker development environment
# CI/CD pipeline setup
# Monitoring infrastructure (Prometheus + Grafana)
```

**Key Deliverables:**
- ✅ Complete project structure
- ✅ Docker development environment
- ✅ Database setup with TimescaleDB
- ✅ Redis messaging infrastructure
- ✅ CI/CD pipeline configuration
- ✅ Monitoring setup

### Week 2: Shared Database Models

```python
# shared/database/models.py - SQLAlchemy models shared between services
from sqlalchemy import Column, Integer, String, DateTime, Boolean, JSON, Numeric, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Exchange(Base):
    __tablename__ = 'exchanges_exchange'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    display_name = Column(String(100), nullable=False)
    is_active = Column(Boolean, default=True)
    api_base_url = Column(String(255))
    websocket_url = Column(String(255))
    rate_limit_per_minute = Column(Integer, default=600)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

class TradingPair(Base):
    __tablename__ = 'exchanges_tradingpair'
    
    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey('exchanges_exchange.id'))
    symbol = Column(String(20), nullable=False)
    base_asset = Column(String(10), nullable=False)
    quote_asset = Column(String(10), nullable=False)
    pair_type = Column(String(20), default='spot')  # spot, perpetual, future
    is_active = Column(Boolean, default=True)
    min_order_size = Column(Numeric(20, 8))
    max_order_size = Column(Numeric(20, 8))
    price_precision = Column(Integer, default=8)
    quantity_precision = Column(Integer, default=8)
    
    exchange = relationship("Exchange", back_populates="trading_pairs")

class Strategy(Base):
    __tablename__ = 'strategies_strategy'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    strategy_type = Column(String(50), nullable=False)
    is_active = Column(Boolean, default=False)
    config = Column(JSON)
    long_exchange_id = Column(Integer, ForeignKey('exchanges_exchange.id'))
    short_exchange_id = Column(Integer, ForeignKey('exchanges_exchange.id'))
    long_symbol = Column(String(20))
    short_symbol = Column(String(20))
    max_position_size = Column(Numeric(20, 8))
    risk_limit = Column(Numeric(10, 4))
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

# TimescaleDB hypertables for time-series data
class Ticker(Base):
    __tablename__ = 'market_data_ticker'
    
    id = Column(Integer, primary_key=True)
    trading_pair_id = Column(Integer, ForeignKey('exchanges_tradingpair.id'))
    timestamp = Column(DateTime, nullable=False, index=True)
    last_price = Column(Numeric(20, 8), nullable=False)
    bid_price = Column(Numeric(20, 8))
    ask_price = Column(Numeric(20, 8))
    volume_24h = Column(Numeric(20, 8))
    price_change_24h = Column(Numeric(10, 4))
    
    trading_pair = relationship("TradingPair")

class Candle(Base):
    __tablename__ = 'market_data_candle'
    
    id = Column(Integer, primary_key=True)
    trading_pair_id = Column(Integer, ForeignKey('exchanges_tradingpair.id'))
    timestamp = Column(DateTime, nullable=False, index=True)
    interval = Column(String(10), nullable=False)
    open_price = Column(Numeric(20, 8), nullable=False)
    high_price = Column(Numeric(20, 8), nullable=False)
    low_price = Column(Numeric(20, 8), nullable=False)
    close_price = Column(Numeric(20, 8), nullable=False)
    volume = Column(Numeric(20, 8), nullable=False)
    
    trading_pair = relationship("TradingPair")

class FundingRate(Base):
    __tablename__ = 'market_data_fundingrate'
    
    id = Column(Integer, primary_key=True)
    trading_pair_id = Column(Integer, ForeignKey('exchanges_tradingpair.id'))
    timestamp = Column(DateTime, nullable=False, index=True)
    funding_rate = Column(Numeric(10, 8), nullable=False)
    predicted_rate = Column(Numeric(10, 8))
    next_funding_time = Column(DateTime)
    
    trading_pair = relationship("TradingPair")
```

```python
# Django models that map to the same tables
# django_web/apps/exchanges/models.py
from django.db import models
from django.utils import timezone

class Exchange(models.Model):
    name = models.CharField(max_length=50, unique=True)
    display_name = models.CharField(max_length=100)
    is_active = models.BooleanField(default=True)
    api_base_url = models.URLField(blank=True)
    websocket_url = models.URLField(blank=True)
    rate_limit_per_minute = models.IntegerField(default=600)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'exchanges_exchange'
    
    def __str__(self):
        return self.display_name

class TradingPair(models.Model):
    PAIR_TYPES = [
        ('spot', 'Spot'),
        ('perpetual', 'Perpetual'),
        ('future', 'Future'),
    ]
    
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE, related_name='trading_pairs')
    symbol = models.CharField(max_length=20)
    base_asset = models.CharField(max_length=10)
    quote_asset = models.CharField(max_length=10)
    pair_type = models.CharField(max_length=20, choices=PAIR_TYPES, default='spot')
    is_active = models.BooleanField(default=True)
    min_order_size = models.DecimalField(max_digits=20, decimal_places=8, null=True, blank=True)
    max_order_size = models.DecimalField(max_digits=20, decimal_places=8, null=True, blank=True)
    price_precision = models.IntegerField(default=8)
    quantity_precision = models.IntegerField(default=8)
    
    class Meta:
        db_table = 'exchanges_tradingpair'
        unique_together = ['exchange', 'symbol']
    
    def __str__(self):
        return f"{self.exchange.name}:{self.symbol}"
```

**Key Deliverables:**
- ✅ Complete database schema design
- ✅ SQLAlchemy models for FastAPI services
- ✅ Django models mapping to same tables
- ✅ TimescaleDB hypertable setup
- ✅ Database migrations for both Django and Alembic

### Week 3: Message Queue & Communication Infrastructure

```python
# shared/messaging/broker.py
import redis
import json
import asyncio
from typing import Dict, Any, Callable, Optional
from datetime import datetime

class MessageBroker:
    """Redis-based message broker for inter-service communication"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        self.subscribers: Dict[str, Callable] = {}
    
    async def publish(self, channel: str, message: Dict[str, Any]) -> None:
        """Publish message to channel"""
        message_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'data': message
        }
        await self.redis_client.publish(channel, json.dumps(message_data))
    
    async def subscribe(self, channel: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Subscribe to channel with message handler"""
        self.subscribers[channel] = handler
        await self.pubsub.subscribe(channel)
    
    async def listen(self) -> None:
        """Listen for messages on subscribed channels"""
        async for message in self.pubsub.listen():
            if message['type'] == 'message':
                channel = message['channel']
                if channel in self.subscribers:
                    try:
                        data = json.loads(message['data'])
                        await self.subscribers[channel](data)
                    except Exception as e:
                        print(f"Error processing message on {channel}: {e}")

# Message schemas using Pydantic
from pydantic import BaseModel

class StrategySignalMessage(BaseModel):
    strategy_id: int
    signal_type: str
    symbol: str
    side: str
    quantity: float
    price: Optional[float] = None
    timestamp: datetime

class RiskAlertMessage(BaseModel):
    alert_type: str
    severity: str
    message: str
    strategy_id: Optional[int] = None
    timestamp: datetime

class MarketDataUpdateMessage(BaseModel):
    exchange: str
    symbol: str
    data_type: str  # ticker, candle, funding_rate
    data: Dict[str, Any]
    timestamp: datetime

class PortfolioUpdateMessage(BaseModel):
    account_id: str
    updates: Dict[str, Any]
    timestamp: datetime
```

```python
# Django Channels integration
# django_web/apps/dashboard/consumers.py
import json
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async
from shared.messaging.broker import MessageBroker

class DashboardConsumer(AsyncWebsocketConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message_broker = MessageBroker()
        self.user_group = None
    
    async def connect(self):
        # Authentication check
        user = self.scope["user"]
        if not user.is_authenticated:
            await self.close(code=4003)
            return
        
        self.user_group = f"user_{user.id}"
        
        # Join user-specific group
        await self.channel_layer.group_add(self.user_group, self.channel_name)
        
        # Subscribe to Redis channels
        await self.message_broker.subscribe("strategy_signals", self.handle_strategy_signal)
        await self.message_broker.subscribe("risk_alerts", self.handle_risk_alert)
        await self.message_broker.subscribe("market_data_updates", self.handle_market_data)
        
        await self.accept()
        
        # Start listening to Redis messages
        asyncio.create_task(self.message_broker.listen())
    
    async def disconnect(self, close_code):
        if self.user_group:
            await self.channel_layer.group_discard(self.user_group, self.channel_name)
    
    async def handle_strategy_signal(self, message):
        """Handle strategy signal from FastAPI engine"""
        await self.send(text_data=json.dumps({
            'type': 'strategy_signal',
            'data': message['data']
        }))
    
    async def handle_risk_alert(self, message):
        """Handle risk alert from FastAPI engine"""
        await self.send(text_data=json.dumps({
            'type': 'risk_alert',
            'data': message['data']
        }))
    
    async def handle_market_data(self, message):
        """Handle market data update from FastAPI market data service"""
        await self.send(text_data=json.dumps({
            'type': 'market_data_update',
            'data': message['data']
        }))
```

**Key Deliverables:**
- ✅ Redis-based message broker implementation
- ✅ Pydantic message schemas
- ✅ Django Channels WebSocket consumers
- ✅ Inter-service communication framework
- ✅ Message routing and error handling

### Week 4: Development Environment & Testing Framework

```yaml
# docker-compose.yml
version: '3.8'

services:
  postgres:
    image: timescale/timescaledb:latest-pg14
    environment:
      POSTGRES_DB: cyberdelta
      POSTGRES_USER: cyberdelta
      POSTGRES_PASSWORD: cyberdelta_dev
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts/init_timescaledb.sql:/docker-entrypoint-initdb.d/init_timescaledb.sql

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes
    volumes:
      - redis_data:/data

  django_web:
    build:
      context: .
      dockerfile: docker/django/Dockerfile
    ports:
      - "8000:8000"
    volumes:
      - ./django_web:/app
    environment:
      - DATABASE_URL=postgresql://cyberdelta:cyberdelta_dev@postgres:5432/cyberdelta
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - postgres
      - redis
    command: python manage.py runserver 0.0.0.0:8000

  fastapi_engine:
    build:
      context: .
      dockerfile: docker/fastapi/Dockerfile
    ports:
      - "8001:8001"
    volumes:
      - ./fastapi_engine:/app
    environment:
      - DATABASE_URL=postgresql://cyberdelta:cyberdelta_dev@postgres:5432/cyberdelta
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - postgres
      - redis
    command: uvicorn main:app --host 0.0.0.0 --port 8001 --reload

  fastapi_market_data:
    build:
      context: .
      dockerfile: docker/fastapi/Dockerfile
    ports:
      - "8002:8002"
    volumes:
      - ./fastapi_market_data:/app
    environment:
      - DATABASE_URL=postgresql://cyberdelta:cyberdelta_dev@postgres:5432/cyberdelta
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - postgres
      - redis
    command: uvicorn main:app --host 0.0.0.0 --port 8002 --reload

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
    volumes:
      - ./docker/nginx/nginx.conf:/etc/nginx/nginx.conf
    depends_on:
      - django_web
      - fastapi_engine
      - fastapi_market_data

volumes:
  postgres_data:
  redis_data:
```

```python
# tests/integration/test_inter_service_communication.py
import pytest
import asyncio
from shared.messaging.broker import MessageBroker, StrategySignalMessage

@pytest.mark.asyncio
async def test_message_broker_communication():
    """Test message passing between services"""
    broker = MessageBroker("redis://localhost:6379/1")  # Test DB
    
    received_messages = []
    
    async def message_handler(message):
        received_messages.append(message)
    
    # Subscribe to test channel
    await broker.subscribe("test_channel", message_handler)
    
    # Start listening in background
    listen_task = asyncio.create_task(broker.listen())
    
    # Send test message
    test_message = {
        "strategy_id": 1,
        "signal_type": "buy",
        "symbol": "BTC-USD",
        "side": "long",
        "quantity": 0.1
    }
    
    await broker.publish("test_channel", test_message)
    
    # Wait for message to be received
    await asyncio.sleep(0.1)
    
    # Clean up
    listen_task.cancel()
    
    assert len(received_messages) == 1
    assert received_messages[0]["data"]["strategy_id"] == 1

@pytest.mark.django_db
@pytest.mark.asyncio
async def test_django_fastapi_database_consistency():
    """Test that Django and FastAPI services can read/write to same database"""
    from django_web.apps.exchanges.models import Exchange as DjangoExchange
    from shared.database.models import Exchange as SQLAlchemyExchange
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    # Create exchange via Django ORM
    django_exchange = DjangoExchange.objects.create(
        name="test_exchange",
        display_name="Test Exchange",
        is_active=True
    )
    
    # Read via SQLAlchemy
    engine = create_engine("postgresql://cyberdelta:cyberdelta_dev@localhost:5432/cyberdelta")
    Session = sessionmaker(bind=engine)
    session = Session()
    
    sqlalchemy_exchange = session.query(SQLAlchemyExchange).filter_by(
        name="test_exchange"
    ).first()
    
    assert sqlalchemy_exchange is not None
    assert sqlalchemy_exchange.display_name == "Test Exchange"
    assert sqlalchemy_exchange.id == django_exchange.id
    
    session.close()
```

**Key Deliverables:**
- ✅ Complete Docker development environment
- ✅ Integration testing framework
- ✅ Database consistency testing
- ✅ Message broker testing
- ✅ CI/CD pipeline implementation

**Phase 1 Milestone Review:**
- Infrastructure fully operational
- Database schema implemented and tested
- Inter-service communication working
- Development environment stable
- Ready for service implementation

---

## Phase 2: FastAPI Market Data Service (Weeks 5-8)

### Week 5: Market Data Service Core

```python
# fastapi_market_data/main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import logging
from routers import market_data, exchanges, websocket
from services.connection_manager import ConnectionManager
from services.data_collector import DataCollectionService

app = FastAPI(
    title="CyberDelta Market Data Service",
    description="High-performance market data collection and distribution",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(market_data.router, prefix="/api/v1", tags=["market_data"])
app.include_router(exchanges.router, prefix="/api/v1", tags=["exchanges"])
app.include_router(websocket.router, prefix="/ws", tags=["websocket"])

# Global services
connection_manager = ConnectionManager()
data_collector = DataCollectionService()

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    await data_collector.start()
    logging.info("Market Data Service started")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    await data_collector.stop()
    logging.info("Market Data Service stopped")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "market_data",
        "active_connections": connection_manager.get_connection_count(),
        "data_streams": data_collector.get_active_streams()
    }
```

```python
# fastapi_market_data/services/hyperliquid_service.py
import asyncio
import websockets
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from shared.messaging.broker import MessageBroker, MarketDataUpdateMessage
from shared.database.crud import MarketDataCRUD

class HyperliquidService:
    """Hyperliquid WebSocket and REST API service"""
    
    def __init__(self):
        self.websocket_url = "wss://api.hyperliquid.xyz/ws"
        self.rest_base_url = "https://api.hyperliquid.xyz"
        self.message_broker = MessageBroker()
        self.market_data_crud = MarketDataCRUD()
        self.websocket = None
        self.is_running = False
        
    async def start(self):
        """Start WebSocket connection and data collection"""
        self.is_running = True
        await self._connect_websocket()
        
    async def stop(self):
        """Stop data collection and close connections"""
        self.is_running = False
        if self.websocket:
            await self.websocket.close()
            
    async def _connect_websocket(self):
        """Establish WebSocket connection with auto-reconnect"""
        while self.is_running:
            try:
                async with websockets.connect(self.websocket_url) as websocket:
                    self.websocket = websocket
                    
                    # Subscribe to all required channels
                    await self._subscribe_to_channels()
                    
                    # Message processing loop
                    async for message in websocket:
                        await self._process_message(json.loads(message))
                        
            except Exception as e:
                logging.error(f"Hyperliquid WebSocket error: {e}")
                if self.is_running:
                    await asyncio.sleep(5)  # Reconnect delay
                    
    async def _subscribe_to_channels(self):
        """Subscribe to required data channels"""
        subscriptions = [
            {"method": "subscribe", "subscription": {"type": "allMids"}},
            {"method": "subscribe", "subscription": {"type": "trades", "coin": "BTC"}},
            {"method": "subscribe", "subscription": {"type": "trades", "coin": "ETH"}},
            {"method": "subscribe", "subscription": {"type": "trades", "coin": "SOL"}},
        ]
        
        for subscription in subscriptions:
            await self.websocket.send(json.dumps(subscription))
            
    async def _process_message(self, message: Dict[str, Any]):
        """Process incoming WebSocket message"""
        try:
            channel = message.get("channel", "")
            data = message.get("data", {})
            
            if channel == "allMids":
                await self._process_ticker_data(data)
            elif channel.startswith("trades"):
                await self._process_trade_data(data)
            elif channel == "funding":
                await self._process_funding_data(data)
                
        except Exception as e:
            logging.error(f"Error processing Hyperliquid message: {e}")
            
    async def _process_ticker_data(self, data: Dict[str, Any]):
        """Process ticker/mid price data"""
        for symbol, price_info in data.get("mids", {}).items():
            ticker_data = {
                "symbol": symbol,
                "last_price": float(price_info),
                "timestamp": datetime.utcnow(),
                "exchange": "hyperliquid"
            }
            
            # Save to database
            await self.market_data_crud.create_ticker(ticker_data)
            
            # Broadcast via message broker
            await self.message_broker.publish("market_data_updates", {
                "exchange": "hyperliquid",
                "symbol": symbol,
                "data_type": "ticker",
                "data": ticker_data
            })
            
    async def _process_trade_data(self, data: Dict[str, Any]):
        """Process trade data"""
        for trade in data:
            trade_data = {
                "symbol": trade["coin"],
                "price": float(trade["px"]),
                "quantity": float(trade["sz"]),
                "side": trade["side"],
                "timestamp": datetime.fromtimestamp(trade["time"] / 1000),
                "exchange": "hyperliquid"
            }
            
            # Save to database
            await self.market_data_crud.create_trade(trade_data)
            
            # Broadcast via message broker
            await self.message_broker.publish("market_data_updates", {
                "exchange": "hyperliquid",
                "symbol": trade["coin"],
                "data_type": "trade",
                "data": trade_data
            })
```

**Key Deliverables:**
- ✅ FastAPI market data service framework
- ✅ Hyperliquid WebSocket integration
- ✅ Backpack WebSocket integration
- ✅ Real-time data processing pipeline
- ✅ Database persistence layer

### Week 6: Data Normalization & Caching

```python
# fastapi_market_data/services/data_normalizer.py
from typing import Dict, Any, List
from datetime import datetime
from decimal import Decimal
from shared.database.models import TradingPair

class DataNormalizer:
    """Normalize data from different exchanges to common format"""
    
    def __init__(self):
        self.exchange_mappings = {
            "hyperliquid": self._normalize_hyperliquid,
            "backpack": self._normalize_backpack
        }
        
    async def normalize_ticker(self, exchange: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize ticker data from any exchange"""
        normalizer = self.exchange_mappings.get(exchange)
        if not normalizer:
            raise ValueError(f"Unsupported exchange: {exchange}")
            
        return await normalizer("ticker", raw_data)
        
    async def normalize_trade(self, exchange: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize trade data from any exchange"""
        normalizer = self.exchange_mappings.get(exchange)
        if not normalizer:
            raise ValueError(f"Unsupported exchange: {exchange}")
            
        return await normalizer("trade", raw_data)
        
    async def _normalize_hyperliquid(self, data_type: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Hyperliquid data"""
        if data_type == "ticker":
            return {
                "exchange": "hyperliquid",
                "symbol": raw_data["symbol"],
                "last_price": Decimal(str(raw_data["price"])),
                "bid_price": Decimal(str(raw_data.get("bid", 0))),
                "ask_price": Decimal(str(raw_data.get("ask", 0))),
                "volume_24h": Decimal(str(raw_data.get("volume", 0))),
                "timestamp": datetime.utcnow()
            }
        elif data_type == "trade":
            return {
                "exchange": "hyperliquid",
                "symbol": raw_data["coin"],
                "price": Decimal(str(raw_data["px"])),
                "quantity": Decimal(str(raw_data["sz"])),
                "side": raw_data["side"],
                "trade_id": raw_data.get("tid"),
                "timestamp": datetime.fromtimestamp(raw_data["time"] / 1000)
            }
            
    async def _normalize_backpack(self, data_type: str, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Backpack data"""
        if data_type == "ticker":
            return {
                "exchange": "backpack",
                "symbol": raw_data["symbol"],
                "last_price": Decimal(str(raw_data["lastPrice"])),
                "bid_price": Decimal(str(raw_data["bidPrice"])),
                "ask_price": Decimal(str(raw_data["askPrice"])),
                "volume_24h": Decimal(str(raw_data["volume"])),
                "timestamp": datetime.fromtimestamp(raw_data["timestamp"] / 1000)
            }
```

```python
# fastapi_market_data/services/cache_service.py
import redis
import json
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

class CacheService:
    """Redis-based caching service for market data"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/1"):
        self.redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
        
    async def set_latest_ticker(self, exchange: str, symbol: str, ticker_data: Dict[str, Any]) -> None:
        """Cache latest ticker data"""
        cache_key = f"ticker:latest:{exchange}:{symbol}"
        await self.redis_client.setex(
            cache_key, 
            300,  # 5 minutes TTL
            json.dumps(ticker_data, default=str)
        )
        
    async def get_latest_ticker(self, exchange: str, symbol: str) -> Optional[Dict[str, Any]]:
        """Get latest ticker from cache"""
        cache_key = f"ticker:latest:{exchange}:{symbol}"
        cached_data = await self.redis_client.get(cache_key)
        
        if cached_data:
            return json.loads(cached_data)
        return None
        
    async def set_price_history(self, exchange: str, symbol: str, prices: List[Dict[str, Any]]) -> None:
        """Cache price history for charts"""
        cache_key = f"prices:history:{exchange}:{symbol}"
        await self.redis_client.setex(
            cache_key,
            3600,  # 1 hour TTL
            json.dumps(prices, default=str)
        )
        
    async def get_price_history(self, exchange: str, symbol: str) -> Optional[List[Dict[str, Any]]]:
        """Get price history from cache"""
        cache_key = f"prices:history:{exchange}:{symbol}"
        cached_data = await self.redis_client.get(cache_key)
        
        if cached_data:
            return json.loads(cached_data)
        return None
        
    async def invalidate_symbol_cache(self, exchange: str, symbol: str) -> None:
        """Invalidate all cache entries for a symbol"""
        pattern = f"*:{exchange}:{symbol}"
        keys = await self.redis_client.keys(pattern)
        
        if keys:
            await self.redis_client.delete(*keys)
```

**Key Deliverables:**
- ✅ Data normalization across exchanges
- ✅ Redis caching implementation
- ✅ Performance optimization
- ✅ Error handling and retry logic

### Week 7: WebSocket Hub & Rate Limiting

```python
# fastapi_market_data/services/websocket_hub.py
from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict, List, Set
import json
import asyncio
from shared.messaging.broker import MessageBroker

class WebSocketConnectionManager:
    """Manage WebSocket connections and data distribution"""
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.user_subscriptions: Dict[str, Set[str]] = {}
        self.message_broker = MessageBroker()
        
    async def connect(self, websocket: WebSocket, client_id: str):
        """Accept new WebSocket connection"""
        await websocket.accept()
        self.active_connections[client_id] = websocket
        self.user_subscriptions[client_id] = set()
        
        # Subscribe to market data updates
        await self.message_broker.subscribe("market_data_updates", self._broadcast_market_data)
        
    def disconnect(self, client_id: str):
        """Remove WebSocket connection"""
        if client_id in self.active_connections:
            del self.active_connections[client_id]
        if client_id in self.user_subscriptions:
            del self.user_subscriptions[client_id]
            
    async def subscribe_to_symbol(self, client_id: str, exchange: str, symbol: str):
        """Subscribe client to specific symbol updates"""
        subscription_key = f"{exchange}:{symbol}"
        if client_id in self.user_subscriptions:
            self.user_subscriptions[client_id].add(subscription_key)
            
    async def unsubscribe_from_symbol(self, client_id: str, exchange: str, symbol: str):
        """Unsubscribe client from symbol updates"""
        subscription_key = f"{exchange}:{symbol}"
        if client_id in self.user_subscriptions:
            self.user_subscriptions[client_id].discard(subscription_key)
            
    async def _broadcast_market_data(self, message: Dict):
        """Broadcast market data to subscribed clients"""
        data = message["data"]
        subscription_key = f"{data['exchange']}:{data['symbol']}"
        
        disconnected_clients = []
        
        for client_id, subscriptions in self.user_subscriptions.items():
            if subscription_key in subscriptions:
                websocket = self.active_connections.get(client_id)
                if websocket:
                    try:
                        await websocket.send_text(json.dumps({
                            "type": "market_data",
                            "data": data
                        }))
                    except Exception:
                        disconnected_clients.append(client_id)
                        
        # Clean up disconnected clients
        for client_id in disconnected_clients:
            self.disconnect(client_id)

# WebSocket router
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import uuid

router = APIRouter()
connection_manager = WebSocketConnectionManager()

@router.websocket("/market-data")
async def websocket_endpoint(websocket: WebSocket):
    client_id = str(uuid.uuid4())
    await connection_manager.connect(websocket, client_id)
    
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message["type"] == "subscribe":
                await connection_manager.subscribe_to_symbol(
                    client_id,
                    message["exchange"],
                    message["symbol"]
                )
            elif message["type"] == "unsubscribe":
                await connection_manager.unsubscribe_from_symbol(
                    client_id,
                    message["exchange"],
                    message["symbol"]
                )
                
    except WebSocketDisconnect:
        connection_manager.disconnect(client_id)
```

```python
# fastapi_market_data/middleware/rate_limiting.py
import time
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
import redis

class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limiting middleware using sliding window"""
    
    def __init__(self, app, redis_url: str = "redis://localhost:6379/2"):
        super().__init__(app)
        self.redis_client = redis.Redis.from_url(redis_url)
        self.window_size = 60  # 1 minute window
        self.max_requests = 1000  # Max requests per window
        
    async def dispatch(self, request: Request, call_next):
        # Get client identifier (IP or API key)
        client_id = self._get_client_id(request)
        
        # Check rate limit
        if not await self._check_rate_limit(client_id):
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded. Please slow down your requests."
            )
        
        response = await call_next(request)
        return response
        
    def _get_client_id(self, request: Request) -> str:
        """Get client identifier for rate limiting"""
        # Check for API key first
        api_key = request.headers.get("X-API-Key")
        if api_key:
            return f"api_key:{api_key}"
        
        # Fall back to IP address
        client_ip = request.client.host
        return f"ip:{client_ip}"
        
    async def _check_rate_limit(self, client_id: str) -> bool:
        """Check if client is within rate limit using sliding window"""
        now = time.time()
        window_start = now - self.window_size
        
        # Redis key for this client
        key = f"rate_limit:{client_id}"
        
        # Remove old entries and count current requests
        pipe = self.redis_client.pipeline()
        pipe.zremrangebyscore(key, 0, window_start)
        pipe.zcard(key)
        pipe.zadd(key, {str(now): now})
        pipe.expire(key, self.window_size)
        
        results = pipe.execute()
        current_requests = results[1]
        
        return current_requests < self.max_requests
```

**Key Deliverables:**
- ✅ WebSocket connection management
- ✅ Real-time data distribution
- ✅ Rate limiting middleware
- ✅ Subscription management

### Week 8: Performance Optimization & Testing

```python
# fastapi_market_data/services/performance_monitor.py
import time
import asyncio
from typing import Dict, List
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class PerformanceMetric:
    timestamp: datetime
    metric_name: str
    value: float
    tags: Dict[str, str]

class PerformanceMonitor:
    """Monitor service performance metrics"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetric] = []
        self.start_time = time.time()
        
    async def record_api_latency(self, endpoint: str, duration: float):
        """Record API endpoint latency"""
        self.metrics.append(PerformanceMetric(
            timestamp=datetime.utcnow(),
            metric_name="api_latency",
            value=duration,
            tags={"endpoint": endpoint}
        ))
        
    async def record_websocket_message_rate(self, exchange: str, messages_per_second: float):
        """Record WebSocket message processing rate"""
        self.metrics.append(PerformanceMetric(
            timestamp=datetime.utcnow(),
            metric_name="websocket_message_rate",
            value=messages_per_second,
            tags={"exchange": exchange}
        ))
        
    async def record_database_query_time(self, query_type: str, duration: float):
        """Record database query performance"""
        self.metrics.append(PerformanceMetric(
            timestamp=datetime.utcnow(),
            metric_name="database_query_time",
            value=duration,
            tags={"query_type": query_type}
        ))
        
    def get_metrics_summary(self) -> Dict[str, float]:
        """Get performance metrics summary"""
        now = datetime.utcnow()
        last_hour = now - timedelta(hours=1)
        
        recent_metrics = [m for m in self.metrics if m.timestamp >= last_hour]
        
        summary = {
            "uptime_seconds": time.time() - self.start_time,
            "total_requests": len([m for m in recent_metrics if m.metric_name == "api_latency"]),
            "avg_api_latency": self._calculate_average("api_latency", recent_metrics),
            "avg_db_query_time": self._calculate_average("database_query_time", recent_metrics),
            "websocket_connections": len(self._get_active_connections()),
        }
        
        return summary
        
    def _calculate_average(self, metric_name: str, metrics: List[PerformanceMetric]) -> float:
        """Calculate average value for a metric"""
        values = [m.value for m in metrics if m.metric_name == metric_name]
        return sum(values) / len(values) if values else 0.0
```

```python
# tests/performance/test_market_data_performance.py
import pytest
import asyncio
import time
from fastapi.testclient import TestClient
from fastapi_market_data.main import app

class TestMarketDataPerformance:
    
    @pytest.fixture
    def client(self):
        return TestClient(app)
    
    def test_api_response_time(self, client):
        """Test API response times are under 50ms"""
        endpoints = [
            "/api/v1/exchanges",
            "/api/v1/tickers/hyperliquid/BTC-USD",
            "/api/v1/trading-pairs",
        ]
        
        for endpoint in endpoints:
            start_time = time.time()
            response = client.get(endpoint)
            duration = time.time() - start_time
            
            assert response.status_code == 200
            assert duration < 0.05, f"Endpoint {endpoint} took {duration:.3f}s (>50ms)"
    
    @pytest.mark.asyncio
    async def test_websocket_message_throughput(self):
        """Test WebSocket can handle high message throughput"""
        from fastapi_market_data.services.websocket_hub import WebSocketConnectionManager
        
        manager = WebSocketConnectionManager()
        
        # Simulate high-frequency market data
        messages_sent = 0
        start_time = time.time()
        
        for i in range(1000):
            await manager._broadcast_market_data({
                "data": {
                    "exchange": "hyperliquid",
                    "symbol": "BTC-USD",
                    "data_type": "ticker",
                    "price": 50000 + i,
                    "timestamp": time.time()
                }
            })
            messages_sent += 1
            
        duration = time.time() - start_time
        messages_per_second = messages_sent / duration
        
        assert messages_per_second > 500, f"Only {messages_per_second:.0f} msg/s (expected >500)"
    
    def test_concurrent_api_requests(self, client):
        """Test handling of concurrent API requests"""
        import concurrent.futures
        
        def make_request():
            return client.get("/api/v1/exchanges")
        
        # Make 100 concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            start_time = time.time()
            futures = [executor.submit(make_request) for _ in range(100)]
            
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
            duration = time.time() - start_time
        
        # All requests should succeed
        assert all(r.status_code == 200 for r in results)
        
        # Should complete within reasonable time
        assert duration < 5.0, f"100 concurrent requests took {duration:.2f}s"
        
        # Should maintain good throughput
        requests_per_second = 100 / duration
        assert requests_per_second > 50, f"Only {requests_per_second:.0f} req/s"
```

**Key Deliverables:**
- ✅ Performance monitoring system
- ✅ Load testing framework
- ✅ Performance optimization
- ✅ Metrics collection and reporting

**Phase 2 Milestone Review:**
- FastAPI market data service fully operational
- Real-time data collection from all exchanges
- WebSocket distribution working efficiently
- Performance targets met (>500 msg/s, <50ms API latency)
- Ready for trading engine integration

---

## Phase 3: FastAPI Trading Engine (Weeks 9-12)

### Week 9: Core Trading Engine

```python
# fastapi_engine/main.py
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from routers import strategies, orders, portfolio, risk, websocket
from services.strategy_executor import StrategyExecutionService
from services.order_router import OrderRoutingService
from services.portfolio_tracker import PortfolioTrackingService
from services.risk_validator import RiskValidationService
from shared.messaging.broker import MessageBroker

app = FastAPI(
    title="CyberDelta Trading Engine",
    description="High-performance trading strategy execution engine",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global services
strategy_executor = StrategyExecutionService()
order_router = OrderRoutingService()
portfolio_tracker = PortfolioTrackingService()
risk_validator = RiskValidationService()
message_broker = MessageBroker()

# Include routers
app.include_router(strategies.router, prefix="/api/v1", tags=["strategies"])
app.include_router(orders.router, prefix="/api/v1", tags=["orders"])
app.include_router(portfolio.router, prefix="/api/v1", tags=["portfolio"])
app.include_router(risk.router, prefix="/api/v1", tags=["risk"])
app.include_router(websocket.router, prefix="/ws", tags=["websocket"])

@app.on_event("startup")
async def startup_event():
    """Initialize trading engine services"""
    await strategy_executor.start()
    await portfolio_tracker.start()
    
    # Subscribe to market data updates
    await message_broker.subscribe("market_data_updates", strategy_executor.process_market_data)
    
    logging.info("Trading Engine started")

@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown trading engine services"""
    await strategy_executor.stop()
    await portfolio_tracker.stop()
    logging.info("Trading Engine stopped")

@app.get("/health")
async def health_check():
    """Health check with detailed status"""
    return {
        "status": "healthy",
        "service": "trading_engine",
        "active_strategies": strategy_executor.get_active_strategy_count(),
        "portfolio_value": await portfolio_tracker.get_total_portfolio_value(),
        "risk_status": await risk_validator.get_risk_status()
    }
```

```python
# fastapi_engine/services/strategy_executor.py
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
import logging
from shared.database.crud import StrategyCRUD, PortfolioCRUD
from shared.messaging.broker import MessageBroker, StrategySignalMessage
from .order_router import OrderRoutingService
from .risk_validator import RiskValidationService

class StrategyExecutionService:
    """Execute trading strategies based on market data"""
    
    def __init__(self):
        self.active_strategies: Dict[int, 'StrategyInstance'] = {}
        self.strategy_crud = StrategyCRUD()
        self.message_broker = MessageBroker()
        self.order_router = OrderRoutingService()
        self.risk_validator = RiskValidationService()
        self.is_running = False
        
    async def start(self):
        """Start strategy execution service"""
        self.is_running = True
        
        # Load active strategies from database
        active_strategies = await self.strategy_crud.get_active_strategies()
        
        for strategy_config in active_strategies:
            await self.start_strategy(strategy_config.id)
            
        logging.info(f"Started {len(self.active_strategies)} strategies")
        
    async def stop(self):
        """Stop all running strategies"""
        self.is_running = False
        
        for strategy_id in list(self.active_strategies.keys()):
            await self.stop_strategy(strategy_id)
            
        logging.info("All strategies stopped")
        
    async def start_strategy(self, strategy_id: int) -> bool:
        """Start a specific strategy"""
        try:
            strategy_config = await self.strategy_crud.get_strategy_by_id(strategy_id)
            if not strategy_config:
                raise ValueError(f"Strategy {strategy_id} not found")
                
            # Create strategy instance
            strategy_instance = await self._create_strategy_instance(strategy_config)
            
            # Validate strategy configuration
            if not await self.risk_validator.validate_strategy_config(strategy_config):
                raise ValueError(f"Strategy {strategy_id} failed risk validation")
                
            # Start strategy
            await strategy_instance.start()
            self.active_strategies[strategy_id] = strategy_instance
            
            # Broadcast strategy start
            await self.message_broker.publish("strategy_updates", {
                "strategy_id": strategy_id,
                "status": "started",
                "timestamp": datetime.utcnow()
            })
            
            logging.info(f"Started strategy {strategy_id}: {strategy_config.name}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to start strategy {strategy_id}: {e}")
            return False
            
    async def stop_strategy(self, strategy_id: int) -> bool:
        """Stop a specific strategy"""
        try:
            if strategy_id in self.active_strategies:
                strategy_instance = self.active_strategies[strategy_id]
                await strategy_instance.stop()
                del self.active_strategies[strategy_id]
                
                # Broadcast strategy stop
                await self.message_broker.publish("strategy_updates", {
                    "strategy_id": strategy_id,
                    "status": "stopped",
                    "timestamp": datetime.utcnow()
                })
                
                logging.info(f"Stopped strategy {strategy_id}")
                return True
            else:
                logging.warning(f"Strategy {strategy_id} not running")
                return False
                
        except Exception as e:
            logging.error(f"Failed to stop strategy {strategy_id}: {e}")
            return False
            
    async def process_market_data(self, message: Dict):
        """Process market data and route to relevant strategies"""
        try:
            market_data = message["data"]
            exchange = market_data["exchange"]
            symbol = market_data["symbol"]
            
            # Find strategies interested in this symbol
            relevant_strategies = [
                strategy for strategy in self.active_strategies.values()
                if strategy.is_interested_in_symbol(exchange, symbol)
            ]
            
            # Process data for each relevant strategy
            for strategy in relevant_strategies:
                asyncio.create_task(strategy.process_market_data(market_data))
                
        except Exception as e:
            logging.error(f"Error processing market data: {e}")
            
    async def _create_strategy_instance(self, strategy_config) -> 'StrategyInstance':
        """Create strategy instance based on configuration"""
        strategy_type = strategy_config.strategy_type
        
        if strategy_type == "funding_rate_arbitrage":
            from .strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
            return FundingRateArbitrageStrategy(
                config=strategy_config,
                order_router=self.order_router,
                risk_validator=self.risk_validator,
                message_broker=self.message_broker
            )
        else:
            raise ValueError(f"Unknown strategy type: {strategy_type}")
            
    def get_active_strategy_count(self) -> int:
        """Get number of active strategies"""
        return len(self.active_strategies)
        
    def get_strategy_status(self, strategy_id: int) -> Optional[Dict]:
        """Get status of a specific strategy"""
        if strategy_id in self.active_strategies:
            return self.active_strategies[strategy_id].get_status()
        return None
```

```python
# fastapi_engine/services/strategies/funding_rate_arbitrage.py
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from decimal import Decimal
import logging

class FundingRateArbitrageStrategy:
    """Funding rate arbitrage strategy implementation"""
    
    def __init__(self, config, order_router, risk_validator, message_broker):
        self.config = config
        self.order_router = order_router
        self.risk_validator = risk_validator
        self.message_broker = message_broker
        
        # Strategy parameters
        self.long_exchange = config.long_exchange_id
        self.short_exchange = config.short_exchange_id
        self.long_symbol = config.long_symbol
        self.short_symbol = config.short_symbol
        self.funding_threshold = Decimal(str(config.config["funding_threshold"]))
        self.max_price_spread = Decimal(str(config.config["max_price_spread_pct"]))
        self.min_profit_usd = Decimal(str(config.config["min_profit_usd"]))
        
        # State tracking
        self.is_running = False
        self.current_position = None
        self.last_funding_rates = {}
        self.last_prices = {}
        
    async def start(self):
        """Start the strategy"""
        self.is_running = True
        logging.info(f"Started funding arbitrage strategy: {self.config.name}")
        
    async def stop(self):
        """Stop the strategy and close positions"""
        self.is_running = False
        
        # Close any open positions
        if self.current_position:
            await self._close_position()
            
        logging.info(f"Stopped funding arbitrage strategy: {self.config.name}")
        
    def is_interested_in_symbol(self, exchange: str, symbol: str) -> bool:
        """Check if strategy is interested in this market data"""
        return (
            (exchange == self.long_exchange and symbol == self.long_symbol) or
            (exchange == self.short_exchange and symbol == self.short_symbol)
        )
        
    async def process_market_data(self, market_data: Dict[str, Any]):
        """Process incoming market data"""
        if not self.is_running:
            return
            
        try:
            exchange = market_data["exchange"]
            symbol = market_data["symbol"]
            data_type = market_data["data_type"]
            
            if data_type == "ticker":
                await self._process_price_update(exchange, symbol, market_data["data"])
            elif data_type == "funding_rate":
                await self._process_funding_rate_update(exchange, symbol, market_data["data"])
                
        except Exception as e:
            logging.error(f"Error processing market data in strategy {self.config.id}: {e}")
            
    async def _process_price_update(self, exchange: str, symbol: str, price_data: Dict):
        """Process price update"""
        key = f"{exchange}:{symbol}"
        self.last_prices[key] = {
            "price": Decimal(str(price_data["last_price"])),
            "timestamp": price_data["timestamp"]
        }
        
        # Check for arbitrage opportunity
        await self._check_arbitrage_opportunity()
        
    async def _process_funding_rate_update(self, exchange: str, symbol: str, funding_data: Dict):
        """Process funding rate update"""
        key = f"{exchange}:{symbol}"
        self.last_funding_rates[key] = {
            "rate": Decimal(str(funding_data["funding_rate"])),
            "timestamp": funding_data["timestamp"]
        }
        
        # Check for arbitrage opportunity
        await self._check_arbitrage_opportunity()
        
    async def _check_arbitrage_opportunity(self):
        """Check if there's a profitable arbitrage opportunity"""
        # Get current funding rates
        long_key = f"{self.long_exchange}:{self.long_symbol}"
        short_key = f"{self.short_exchange}:{self.short_symbol}"
        
        if long_key not in self.last_funding_rates or short_key not in self.last_funding_rates:
            return  # Not enough data
            
        long_funding = self.last_funding_rates[long_key]["rate"]
        short_funding = self.last_funding_rates[short_key]["rate"]
        
        # Calculate funding rate spread
        funding_spread = abs(long_funding - short_funding)
        
        if funding_spread < self.funding_threshold:
            return  # Spread too small
            
        # Check price spread
        if long_key not in self.last_prices or short_key not in self.last_prices:
            return  # No price data
            
        long_price = self.last_prices[long_key]["price"]
        short_price = self.last_prices[short_key]["price"]
        
        price_spread_pct = abs(long_price - short_price) / ((long_price + short_price) / 2) * 100
        
        if price_spread_pct > self.max_price_spread:
            return  # Price spread too large
            
        # Calculate potential profit
        position_size = self._calculate_optimal_position_size()
        estimated_profit = funding_spread * position_size
        
        if estimated_profit < self.min_profit_usd:
            return  # Profit too small
            
        # Execute arbitrage
        await self._execute_arbitrage(long_funding, short_funding, position_size)
        
    async def _execute_arbitrage(self, long_funding: Decimal, short_funding: Decimal, position_size: Decimal):
        """Execute arbitrage trade"""
        try:
            # Determine which side to long/short based on funding rates
            if long_funding > short_funding:
                # Long on short exchange, short on long exchange
                long_exchange = self.short_exchange
                long_symbol = self.short_symbol
                short_exchange = self.long_exchange
                short_symbol = self.long_symbol
            else:
                # Long on long exchange, short on short exchange
                long_exchange = self.long_exchange
                long_symbol = self.long_symbol
                short_exchange = self.short_exchange
                short_symbol = self.short_symbol
                
            # Risk validation
            trade_request = {
                "strategy_id": self.config.id,
                "long_exchange": long_exchange,
                "long_symbol": long_symbol,
                "short_exchange": short_exchange,
                "short_symbol": short_symbol,
                "position_size": position_size
            }
            
            if not await self.risk_validator.validate_trade_request(trade_request):
                logging.warning(f"Trade request failed risk validation: {trade_request}")
                return
                
            # Execute long position
            long_order = await self.order_router.place_order(
                exchange=long_exchange,
                symbol=long_symbol,
                side="buy",
                order_type="market",
                quantity=position_size
            )
            
            # Execute short position
            short_order = await self.order_router.place_order(
                exchange=short_exchange,
                symbol=short_symbol,
                side="sell",
                order_type="market",
                quantity=position_size
            )
            
            # Track position
            self.current_position = {
                "long_order": long_order,
                "short_order": short_order,
                "entry_time": datetime.utcnow(),
                "expected_profit": abs(long_funding - short_funding) * position_size
            }
            
            # Broadcast signal
            await self.message_broker.publish("strategy_signals", {
                "strategy_id": self.config.id,
                "signal_type": "arbitrage_entry",
                "long_exchange": long_exchange,
                "long_symbol": long_symbol,
                "short_exchange": short_exchange,
                "short_symbol": short_symbol,
                "position_size": float(position_size),
                "expected_profit": float(self.current_position["expected_profit"]),
                "timestamp": datetime.utcnow()
            })
            
            logging.info(f"Executed arbitrage: {trade_request}")
            
        except Exception as e:
            logging.error(f"Failed to execute arbitrage: {e}")
            
    def _calculate_optimal_position_size(self) -> Decimal:
        """Calculate optimal position size based on risk parameters"""
        # Simple implementation - use fixed percentage of max position size
        return self.config.max_position_size * Decimal("0.1")  # 10% of max
        
    async def _close_position(self):
        """Close current arbitrage position"""
        if not self.current_position:
            return
            
        try:
            # Close long position
            long_order = self.current_position["long_order"]
            await self.order_router.place_order(
                exchange=long_order["exchange"],
                symbol=long_order["symbol"],
                side="sell",
                order_type="market",
                quantity=long_order["quantity"]
            )
            
            # Close short position
            short_order = self.current_position["short_order"]
            await self.order_router.place_order(
                exchange=short_order["exchange"],
                symbol=short_order["symbol"],
                side="buy",
                order_type="market",
                quantity=short_order["quantity"]
            )
            
            # Broadcast signal
            await self.message_broker.publish("strategy_signals", {
                "strategy_id": self.config.id,
                "signal_type": "arbitrage_exit",
                "timestamp": datetime.utcnow()
            })
            
            self.current_position = None
            logging.info(f"Closed arbitrage position for strategy {self.config.id}")
            
        except Exception as e:
            logging.error(f"Failed to close position: {e}")
            
    def get_status(self) -> Dict[str, Any]:
        """Get current strategy status"""
        return {
            "strategy_id": self.config.id,
            "name": self.config.name,
            "is_running": self.is_running,
            "has_position": self.current_position is not None,
            "last_check": datetime.utcnow(),
            "funding_rates": self.last_funding_rates,
            "prices": self.last_prices
        }
```

**Key Deliverables:**
- ✅ FastAPI trading engine framework
- ✅ Strategy execution service
- ✅ Funding rate arbitrage strategy implementation
- ✅ Market data processing pipeline
- ✅ Strategy lifecycle management

This migration timeline provides a comprehensive roadmap for transitioning to Django + FastAPI + HTMX while maintaining operational excellence throughout the process. The next sections will detail the remaining phases including order routing, risk management, Django web application, and final integration.
# Minimal Architecture Design: Adapter Pattern Strategy

## Overview

This document outlines the minimal architecture that preserves all existing CyberDeltaEngine components while adding modern interfaces through the adapter pattern. The core principle is **wrapping, not replacing** your proven trading infrastructure.

## Architecture Philosophy

### Core Principle: Adapter Pattern
Instead of rewriting components, we create thin adapters that expose existing functionality through modern interfaces:

```
┌─────────────────────────────────────────────────────────────┐
│                    New Modern Interfaces                    │
├─────────────────────────────────────────────────────────────┤
│  Django Admin  │  HTMX Dashboard  │  FastAPI REST  │  Auth  │
└─────────────────────────────────────────────────────────────┘
                                 │
┌─────────────────────────────────────────────────────────────┐
│                    Adapter Layer (New)                     │
├─────────────────────────────────────────────────────────────┤
│  Django Models  │  Service Adapters  │  WebSocket Bridge   │
└─────────────────────────────────────────────────────────────┘
                                 │
┌─────────────────────────────────────────────────────────────┐
│               Existing CyberDelta Components               │
│                        (Unchanged)                         │
├─────────────────────────────────────────────────────────────┤
│  cyberdelta.apis  │  cyberdelta.core  │  cyberdelta.strategies │
│  cyberdelta.validation  │  cyberdelta.config  │  cyberdelta.utils │
└─────────────────────────────────────────────────────────────┘
```

## System Architecture

### High-Level Service Architecture
```
                                Users
                                  │
                    ┌─────────────────────────┐
                    │      Load Balancer      │
                    │       (Nginx)          │
                    └─────────────────────────┘
                                  │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Django Web     │    │  FastAPI Market │    │  FastAPI Trading│
│  Application    │    │  Data Service   │    │  Engine Service │
│                 │    │                 │    │                 │
│ • HTMX Dashboard│    │ • Data Endpoints│    │ • Strategy APIs │
│ • User Auth     │    │ • WebSocket Hub │    │ • Order APIs    │
│ • Admin Panel   │    │ • Rate Limiting │    │ • Portfolio APIs│
│ • Config Mgmt   │    │                 │    │ • Risk APIs     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────────────┐
                    │    Message Broker       │
                    │      (Redis)           │
                    └─────────────────────────┘
                                 │
                    ┌─────────────────────────┐
                    │      Database          │
                    │   (PostgreSQL)         │
                    └─────────────────────────┘
                                 │
        ┌───────────────────────────────────────────────────┐
        │            Existing CyberDelta Core               │
        │              (Unchanged)                          │
        │  • cyberdelta.apis   • cyberdelta.core           │
        │  • cyberdelta.strategies  • cyberdelta.validation │
        └───────────────────────────────────────────────────┘
```

## Detailed Component Design

### 1. Django Web Application

#### Purpose
- Modern web interface replacing Dash dashboard
- User authentication and multi-user support
- Configuration management through web UI
- Historical analytics and reporting

#### Key Components
```python
# Django project structure
django_web/
├── apps/
│   ├── dashboard/           # HTMX dashboard replacing Dash
│   ├── accounts/           # User management
│   ├── configuration/      # Web-based config management
│   ├── analytics/          # Historical data analysis
│   └── monitoring/         # System health monitoring
├── adapters/              # Adapters to existing cyberdelta
│   ├── trading_adapter.py  # Wraps cyberdelta.core
│   ├── market_adapter.py   # Wraps cyberdelta.apis
│   └── config_adapter.py   # Wraps cyberdelta.config
└── services/              # Business logic services
    ├── dashboard_service.py
    ├── user_service.py
    └── notification_service.py
```

#### Adapter Example: Trading Integration
```python
# django_web/adapters/trading_adapter.py
import sys
import os

# Add existing cyberdelta to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../cyberdelta'))

from cyberdelta.core.engine import Engine
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy

class TradingAdapter:
    """Adapter to expose existing trading engine through Django"""
    
    def __init__(self):
        # Use existing components exactly as-is
        self.engine = Engine(name="CyberDelta_Web")
        self.portfolio_tracker = PortfolioTracker(...)
        self.strategies = {}
        
    def get_strategy_performance(self, strategy_name: str, days: int = 30):
        """Get strategy performance using existing portfolio tracker"""
        # Direct call to existing method - no changes to core logic
        return self.portfolio_tracker.get_strategy_performance(strategy_name, days)
    
    def get_active_strategies(self):
        """Get list of active strategies from existing engine"""
        return [
            {
                'name': name,
                'enabled': strategy.enabled,
                'symbol': strategy.symbol,
                'performance': self.get_strategy_performance(name)
            }
            for name, strategy in self.engine.strategies.items()
        ]
    
    def start_strategy(self, strategy_name: str) -> bool:
        """Start strategy using existing engine"""
        self.engine.enable_strategy(strategy_name)
        return True
    
    def stop_strategy(self, strategy_name: str) -> bool:
        """Stop strategy using existing engine"""
        self.engine.disable_strategy(strategy_name)
        return True
```

### 2. FastAPI Market Data Service

#### Purpose
- High-performance API endpoints for market data
- WebSocket hub for real-time data distribution
- Rate limiting and caching
- External API access to market data

#### Key Components
```python
# fastapi_market_data/
├── main.py                # FastAPI application
├── routers/
│   ├── tickers.py         # Ticker endpoints
│   ├── candles.py         # OHLCV data endpoints
│   ├── funding_rates.py   # Funding rate endpoints
│   └── websocket.py       # WebSocket endpoints
├── adapters/              # Adapters to existing APIs
│   ├── hyperliquid_adapter.py
│   ├── backpack_adapter.py
│   └── base_adapter.py
├── services/
│   ├── market_data_service.py
│   ├── websocket_manager.py
│   └── cache_service.py
└── middleware/
    ├── rate_limiting.py
    └── authentication.py
```

#### Adapter Example: Hyperliquid Integration
```python
# fastapi_market_data/adapters/hyperliquid_adapter.py
import sys
import os

# Add existing cyberdelta to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../cyberdelta'))

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config.config_manager import get_app_settings
from cyberdelta.config.secrets_manager import get_secrets_config

class HyperliquidAdapter:
    """Adapter to expose existing Hyperliquid API through FastAPI"""
    
    def __init__(self):
        # Use existing configuration system
        self.config = get_app_settings()
        self.secrets = get_secrets_config()
        
        # Use existing API client - ZERO changes to existing code
        self.hl_api = HyperliquidAPI(
            self.config.exchanges.hyperliquid,
            self.secrets.exchanges.hyperliquid
        )
        
    async def get_ticker(self, symbol: str) -> dict:
        """Get ticker data using existing API client"""
        # Direct call to existing method
        ticker_data = await self.hl_api.get_ticker(symbol)
        
        # Optional: Transform for external API if needed
        return {
            'symbol': symbol,
            'price': ticker_data.last_price,
            'bid': ticker_data.bid_price,
            'ask': ticker_data.ask_price,
            'volume': ticker_data.volume_24h,
            'timestamp': ticker_data.timestamp.isoformat()
        }
    
    async def get_candles(self, symbol: str, interval: str, limit: int = 100) -> list:
        """Get candle data using existing API client"""
        # Direct call to existing method
        candles = await self.hl_api.get_candles(symbol, interval, limit)
        
        # Transform to standard format
        return [
            {
                'timestamp': candle.timestamp.isoformat(),
                'open': float(candle.open_price),
                'high': float(candle.high_price),
                'low': float(candle.low_price),
                'close': float(candle.close_price),
                'volume': float(candle.volume)
            }
            for candle in candles
        ]
    
    async def start_websocket_stream(self, symbols: list[str]):
        """Start WebSocket stream using existing WebSocket manager"""
        # Use existing WebSocket implementation
        await self.hl_api.connect_websocket()
        
        # Subscribe to symbols using existing subscription logic
        for symbol in symbols:
            await self.hl_api.subscribe_to_ticker(symbol)
```

### 3. FastAPI Trading Engine Service

#### Purpose
- RESTful APIs for strategy management
- Order management and execution endpoints
- Portfolio and risk management APIs
- Real-time trading status and metrics

#### Key Components
```python
# fastapi_trading_engine/
├── main.py                # FastAPI application
├── routers/
│   ├── strategies.py      # Strategy management endpoints
│   ├── orders.py          # Order management endpoints
│   ├── portfolio.py       # Portfolio status endpoints
│   ├── risk.py           # Risk management endpoints
│   └── websocket.py       # Real-time trading updates
├── adapters/              # Adapters to existing core
│   ├── engine_adapter.py
│   ├── strategy_adapter.py
│   ├── risk_adapter.py
│   └── portfolio_adapter.py
├── services/
│   ├── trading_service.py
│   ├── order_service.py
│   └── risk_service.py
└── middleware/
    ├── authentication.py
    └── risk_validation.py
```

#### Adapter Example: Strategy Management
```python
# fastapi_trading_engine/adapters/strategy_adapter.py
import sys
import os

# Add existing cyberdelta to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../cyberdelta'))

from cyberdelta.core.engine import Engine
from cyberdelta.core.strategy_manager import StrategyManager
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy

class StrategyAdapter:
    """Adapter to expose existing strategy system through FastAPI"""
    
    def __init__(self):
        # Use existing strategy components
        self.engine = Engine(name="CyberDelta_API")
        self.strategy_manager = StrategyManager(...)
        
        # Load existing strategies
        self._load_strategies()
    
    def _load_strategies(self):
        """Load strategies using existing strategy system"""
        # Use existing strategy loading logic
        funding_strategy = FundingRateArbitrageStrategy(
            name="HL-BP-Funding",
            symbol="BTC-USD",
            data_handler=self.strategy_manager.data_handler,
            portfolio_tracker=self.strategy_manager.portfolio_tracker,
            risk_manager=self.strategy_manager.risk_manager,
            params={
                "funding_threshold": 0.01,
                "max_price_spread_pct": 0.5,
                "min_profit_usd": 10.0
            }
        )
        
        self.engine.add_strategy(funding_strategy)
    
    async def list_strategies(self) -> list[dict]:
        """List all strategies using existing engine"""
        strategies = []
        for name, strategy in self.engine.strategies.items():
            strategies.append({
                'name': name,
                'type': strategy.__class__.__name__,
                'symbol': strategy.symbol,
                'enabled': strategy.enabled,
                'status': 'active' if strategy.enabled else 'inactive'
            })
        return strategies
    
    async def start_strategy(self, strategy_name: str) -> dict:
        """Start strategy using existing engine"""
        if strategy_name not in self.engine.strategies:
            raise ValueError(f"Strategy {strategy_name} not found")
        
        # Use existing engine method
        self.engine.enable_strategy(strategy_name)
        
        return {
            'strategy_name': strategy_name,
            'status': 'started',
            'message': f'Strategy {strategy_name} started successfully'
        }
    
    async def stop_strategy(self, strategy_name: str) -> dict:
        """Stop strategy using existing engine"""
        if strategy_name not in self.engine.strategies:
            raise ValueError(f"Strategy {strategy_name} not found")
        
        # Use existing engine method
        self.engine.disable_strategy(strategy_name)
        
        return {
            'strategy_name': strategy_name,
            'status': 'stopped',
            'message': f'Strategy {strategy_name} stopped successfully'
        }
    
    async def get_strategy_performance(self, strategy_name: str) -> dict:
        """Get strategy performance using existing portfolio tracker"""
        if strategy_name not in self.engine.strategies:
            raise ValueError(f"Strategy {strategy_name} not found")
        
        strategy = self.engine.strategies[strategy_name]
        
        # Use existing performance calculation logic
        performance = self.strategy_manager.portfolio_tracker.get_strategy_performance(
            strategy_name
        )
        
        return {
            'strategy_name': strategy_name,
            'total_pnl': float(performance.total_pnl),
            'daily_pnl': float(performance.daily_pnl),
            'win_rate': float(performance.win_rate),
            'total_trades': performance.total_trades,
            'sharpe_ratio': float(performance.sharpe_ratio) if performance.sharpe_ratio else None
        }
```

### 4. Database Integration Layer

#### Purpose
- Persist configuration data (replacing YAML files)
- Store historical performance metrics
- User authentication and authorization data
- Audit trails and logging

#### Database Schema
```python
# shared/database/models.py (Django models)
from django.db import models
from django.contrib.auth.models import User

class Exchange(models.Model):
    """Exchange configuration - complements existing cyberdelta.config"""
    name = models.CharField(max_length=50, unique=True)
    display_name = models.CharField(max_length=100)
    is_active = models.BooleanField(default=True)
    api_config = models.JSONField(default=dict)  # Store existing config format
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class Strategy(models.Model):
    """Strategy configuration - complements existing strategy system"""
    name = models.CharField(max_length=100, unique=True)
    strategy_type = models.CharField(max_length=50)
    is_active = models.BooleanField(default=False)
    config = models.JSONField(default=dict)  # Store existing strategy params
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class PerformanceSnapshot(models.Model):
    """Historical performance data - complements existing portfolio tracker"""
    strategy = models.ForeignKey(Strategy, on_delete=models.CASCADE)
    timestamp = models.DateTimeField(auto_now_add=True)
    total_pnl = models.DecimalField(max_digits=20, decimal_places=8)
    daily_pnl = models.DecimalField(max_digits=20, decimal_places=8)
    win_rate = models.FloatField()
    total_trades = models.IntegerField()
    metadata = models.JSONField(default=dict)  # Additional metrics
```

#### Configuration Adapter
```python
# shared/adapters/config_adapter.py
import sys
import os

# Add existing cyberdelta to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../cyberdelta'))

from cyberdelta.config.config_manager import get_app_settings, AppSettings
from cyberdelta.config.secrets_manager import get_secrets_config
from shared.database.models import Exchange, Strategy

class ConfigurationAdapter:
    """Adapter between database config and existing cyberdelta config system"""
    
    def __init__(self):
        # Use existing config system as fallback
        self.file_config = get_app_settings()
        self.secrets_config = get_secrets_config()
    
    async def get_app_settings(self) -> AppSettings:
        """Get app settings combining database and file config"""
        # Start with existing file-based config
        config = self.file_config
        
        # Override with database settings if available
        db_exchanges = Exchange.objects.filter(is_active=True)
        if db_exchanges.exists():
            # Update exchange configs from database
            for db_exchange in db_exchanges:
                if hasattr(config.exchanges, db_exchange.name):
                    # Merge database config with file config
                    existing_config = getattr(config.exchanges, db_exchange.name)
                    existing_config.update(db_exchange.api_config)
        
        return config
    
    async def save_strategy_config(self, strategy_name: str, config_dict: dict):
        """Save strategy configuration to database"""
        strategy, created = Strategy.objects.get_or_create(
            name=strategy_name,
            defaults={
                'strategy_type': config_dict.get('type', 'unknown'),
                'config': config_dict,
                'is_active': False
            }
        )
        
        if not created:
            strategy.config = config_dict
            strategy.save()
        
        return strategy
    
    async def load_strategy_configs(self) -> dict:
        """Load strategy configurations from database"""
        strategies = {}
        for db_strategy in Strategy.objects.filter(is_active=True):
            strategies[db_strategy.name] = db_strategy.config
        
        return strategies
```

## Inter-Service Communication

### Message Broker Pattern (Redis)
```python
# shared/messaging/broker.py
import redis
import json
import asyncio
from typing import Callable, Dict, Any

class MessageBroker:
    """Redis-based message broker for service communication"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
        self.subscribers: Dict[str, Callable] = {}
    
    async def publish(self, channel: str, message: Dict[str, Any]):
        """Publish message to channel"""
        await self.redis_client.publish(channel, json.dumps(message))
    
    async def subscribe(self, channel: str, handler: Callable):
        """Subscribe to channel with message handler"""
        self.subscribers[channel] = handler
        # Start listening in background
        asyncio.create_task(self._listen(channel))
    
    async def _listen(self, channel: str):
        """Listen for messages on channel"""
        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe(channel)
        
        async for message in pubsub.listen():
            if message['type'] == 'message':
                data = json.loads(message['data'])
                handler = self.subscribers.get(channel)
                if handler:
                    await handler(data)
```

### Example: Real-time Data Flow
```python
# Example of how existing WebSocket data flows to new interfaces
# In existing cyberdelta WebSocket handler:

class ExistingWebSocketHandler:
    def __init__(self):
        # Existing initialization
        self.message_broker = MessageBroker()
    
    async def on_ticker_update(self, ticker_data):
        """Existing method - just add message publishing"""
        # Existing ticker processing logic (unchanged)
        processed_ticker = self.process_ticker_data(ticker_data)
        
        # NEW: Publish to message broker for new services
        await self.message_broker.publish('ticker_updates', {
            'exchange': 'hyperliquid',
            'symbol': processed_ticker.symbol,
            'price': float(processed_ticker.last_price),
            'timestamp': processed_ticker.timestamp.isoformat()
        })
        
        # Existing signal generation logic (unchanged)
        await self.process_signals(processed_ticker)
```

## Benefits of This Architecture

### 1. Risk Mitigation
- **Zero changes** to proven trading logic
- **Easy rollback**: Original system stays intact
- **Gradual migration**: Services can be added one at a time
- **Parallel operation**: Old and new systems can run together

### 2. Development Efficiency
- **Faster timeline**: 12 weeks vs 20+ weeks for rewrite
- **Lower cost**: Minimal new development required
- **Immediate value**: Modern interfaces without core changes
- **Preserved investment**: All existing work remains valuable

### 3. Technical Benefits
- **Service isolation**: Failures in UI don't affect trading
- **Independent scaling**: Scale services based on load
- **Modern interfaces**: REST APIs and HTMX dashboard
- **Database persistence**: Configuration and historical data

### 4. Future Flexibility
- **Gradual modernization**: Can migrate components over time
- **Technology evolution**: Can upgrade individual services
- **External integration**: APIs enable third-party tools
- **Multi-user support**: Authentication layer for team access

## Implementation Priority

### Phase 1: Foundation (Weeks 1-3)
1. **Setup project structure** with existing cyberdelta preserved
2. **Create basic adapters** for core components
3. **Setup database models** complementing existing config
4. **Implement message broker** for service communication

### Phase 2: Market Data Service (Weeks 4-6)
1. **FastAPI market data service** wrapping existing APIs
2. **WebSocket hub** distributing real-time data
3. **Rate limiting and caching** for external access
4. **API documentation** with OpenAPI/Swagger

### Phase 3: Django Dashboard (Weeks 7-9)
1. **HTMX dashboard** replacing Dash interface
2. **User authentication** and multi-user support
3. **Configuration management** through web interface
4. **Real-time updates** via WebSocket

### Phase 4: Trading Service (Weeks 10-12)
1. **FastAPI trading service** wrapping existing engine
2. **Strategy management APIs** for external control
3. **Portfolio and risk APIs** for monitoring
4. **Integration testing** and production deployment

This architecture ensures that your valuable trading infrastructure is preserved while gaining the benefits of modern web technologies and service-oriented design.
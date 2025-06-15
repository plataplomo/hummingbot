# Database Integration: PostgreSQL + TimescaleDB for Persistence

## Overview (Updated June 2025)

This document outlines the database integration strategy that adds persistent storage to CyberDeltaEngine while preserving all existing in-memory state management and business logic within the 8-9 week timeline. The current system uses file-based persistence for some components but lacks comprehensive historical data storage. The approach uses PostgreSQL with TimescaleDB extension for efficient time-series data storage while maintaining compatibility with existing state management and supporting the reduced team structure.

## Current State Management Analysis

### Existing Architecture (Preserved)
```python
# Current: cyberdelta/utils/state_manager.py - KEEP EXACTLY AS-IS
class StateManager:
    def __init__(self):
        self.state = {}  # In-memory state storage
        self.lock = asyncio.Lock()
        
    async def save_state(self, key: str, value: Any):
        # Existing in-memory logic - unchanged
        async with self.lock:
            self.state[key] = value
    
    async def load_state(self, key: str) -> Any:
        # Existing retrieval logic - unchanged
        return self.state.get(key)
```

### What We're Adding (Not Replacing)
- **Persistent storage layer** that complements existing state management
- **Historical data preservation** for analytics and reporting (currently limited)
- **Configuration database** as alternative to YAML files (optional migration)
- **Audit trails** for compliance and debugging
- **Time-series optimization** for performance metrics and market data
- **Multi-user data isolation** for authentication support

## Database Architecture

### Technology Stack
```
Database Layer:
├── PostgreSQL 15+        # Primary database engine
├── TimescaleDB 2.10+     # Time-series data extension
├── Redis 7+              # Message broker and cache
└── pgBackRest            # Backup and recovery
```

### Database Structure
```
cyberdelta_db
├── Configuration Schema   # Replace YAML files (optional)
├── Historical Schema     # Time-series market data
├── Audit Schema         # Trading events and logs
├── User Schema          # Authentication and permissions
└── Analytics Schema     # Pre-computed metrics
```

## Database Schema Design

### 1. Configuration Tables (Optional YAML Replacement)

```sql
-- Exchange configuration
CREATE TABLE exchanges (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    api_config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Strategy configuration
CREATE TABLE strategies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    strategy_type VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    is_active BOOLEAN DEFAULT false,
    parameters JSONB NOT NULL DEFAULT '{}',
    user_id INTEGER REFERENCES users(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Risk management configuration
CREATE TABLE risk_config (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    max_position_size DECIMAL(20,8),
    max_daily_loss DECIMAL(20,8),
    stop_loss_pct DECIMAL(5,4),
    config JSONB NOT NULL DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### 2. Time-Series Data Tables (TimescaleDB)

```sql
-- Market data storage
CREATE TABLE market_data (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    data_type VARCHAR(20) NOT NULL, -- 'ticker', 'candle', 'funding_rate'
    data JSONB NOT NULL,
    PRIMARY KEY (time, exchange, symbol, data_type)
);

-- Convert to TimescaleDB hypertable
SELECT create_hypertable('market_data', 'time', chunk_time_interval => INTERVAL '1 hour');

-- Performance metrics snapshots
CREATE TABLE performance_snapshots (
    time TIMESTAMPTZ NOT NULL,
    strategy_name VARCHAR(100) NOT NULL,
    total_pnl DECIMAL(20,8),
    daily_pnl DECIMAL(20,8),
    win_rate DECIMAL(5,4),
    total_trades INTEGER,
    sharpe_ratio DECIMAL(10,6),
    max_drawdown DECIMAL(5,4),
    metadata JSONB DEFAULT '{}',
    PRIMARY KEY (time, strategy_name)
);

SELECT create_hypertable('performance_snapshots', 'time', chunk_time_interval => INTERVAL '1 day');

-- Trade history
CREATE TABLE trades (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    strategy_name VARCHAR(100) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL, -- 'buy', 'sell'
    order_type VARCHAR(20) NOT NULL,
    quantity DECIMAL(20,8) NOT NULL,
    price DECIMAL(20,8) NOT NULL,
    fee DECIMAL(20,8) DEFAULT 0,
    realized_pnl DECIMAL(20,8) DEFAULT 0,
    order_id VARCHAR(100),
    fill_id VARCHAR(100),
    metadata JSONB DEFAULT '{}'
);

-- Create index for time-based queries
CREATE INDEX idx_trades_timestamp ON trades (timestamp DESC);
CREATE INDEX idx_trades_strategy ON trades (strategy_name, timestamp DESC);
```

### 3. User Management Tables

```sql
-- Users (extends Django auth)
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(150) UNIQUE NOT NULL,
    email VARCHAR(254) UNIQUE NOT NULL,
    is_active BOOLEAN DEFAULT true,
    is_staff BOOLEAN DEFAULT false,
    date_joined TIMESTAMPTZ DEFAULT NOW(),
    last_login TIMESTAMPTZ,
    preferences JSONB DEFAULT '{}'
);

-- User strategy permissions
CREATE TABLE user_strategy_permissions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    strategy_name VARCHAR(100) NOT NULL,
    can_view BOOLEAN DEFAULT true,
    can_start_stop BOOLEAN DEFAULT false,
    can_configure BOOLEAN DEFAULT false,
    granted_at TIMESTAMPTZ DEFAULT NOW(),
    granted_by INTEGER REFERENCES users(id)
);
```

### 4. Audit and Logging Tables

```sql
-- System events audit trail
CREATE TABLE audit_events (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type VARCHAR(50) NOT NULL,
    user_id INTEGER REFERENCES users(id),
    strategy_name VARCHAR(100),
    event_data JSONB NOT NULL,
    ip_address INET,
    user_agent TEXT
);

-- Error and exception logging
CREATE TABLE error_logs (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    level VARCHAR(20) NOT NULL, -- 'ERROR', 'WARNING', 'CRITICAL'
    component VARCHAR(100) NOT NULL,
    message TEXT NOT NULL,
    exception_type VARCHAR(100),
    stack_trace TEXT,
    metadata JSONB DEFAULT '{}'
);

CREATE INDEX idx_audit_events_timestamp ON audit_events (timestamp DESC);
CREATE INDEX idx_error_logs_timestamp ON error_logs (timestamp DESC);
```

## Database Adapter Implementation

### 1. Configuration Adapter (Preserves Existing Config System)

```python
# shared/adapters/database_config_adapter.py
import sys
import os
from typing import Optional, Dict, Any
from pathlib import Path

# Add existing cyberdelta to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../cyberdelta'))

from cyberdelta.config.config_manager import get_app_settings, AppSettings
from cyberdelta.config.secrets_manager import get_secrets_config
from cyberdelta.config.config_models import ExchangeConfig, RiskConfig

from shared.database.models import Exchange, Strategy, RiskConfig as DBRiskConfig

class DatabaseConfigAdapter:
    """Adapter between database config and existing cyberdelta config system
    
    This preserves all existing YAML-based configuration while optionally
    adding database overrides and persistence. The existing config system
    with its sophisticated validation remains the primary source.
    """
    
    def __init__(self):
        # Use existing config system as primary source
        self.file_config = get_app_settings()
        self.secrets_config = get_secrets_config()
        
        # Database provides optional overrides and persistence
        self.use_database = False
        
    async def get_app_settings(self) -> AppSettings:
        """Get app settings with database overrides"""
        # Start with existing file-based configuration (unchanged)
        config = self.file_config
        
        # Optionally override with database settings
        if self.use_database:
            config = await self._merge_database_config(config)
        
        return config
    
    async def _merge_database_config(self, file_config: AppSettings) -> AppSettings:
        """Merge database configuration with file configuration"""
        # Load database overrides
        db_exchanges = await self._load_exchange_configs()
        db_risk_config = await self._load_risk_config()
        
        # Create new config with database overrides
        merged_config = file_config.copy(deep=True)
        
        # Override exchange configurations
        for exchange_name, db_config in db_exchanges.items():
            if hasattr(merged_config.exchanges, exchange_name):
                existing_config = getattr(merged_config.exchanges, exchange_name)
                # Merge database config into existing config
                for key, value in db_config.items():
                    setattr(existing_config, key, value)
        
        # Override risk configuration
        if db_risk_config:
            merged_config.risk_management.update(db_risk_config)
        
        return merged_config
    
    async def _load_exchange_configs(self) -> Dict[str, Dict[str, Any]]:
        """Load exchange configurations from database"""
        configs = {}
        
        # Use async database queries (example with async ORM)
        exchanges = await Exchange.objects.filter(is_active=True).all()
        
        for exchange in exchanges:
            configs[exchange.name] = exchange.api_config
        
        return configs
    
    async def _load_risk_config(self) -> Optional[Dict[str, Any]]:
        """Load risk configuration from database"""
        try:
            risk_config = await DBRiskConfig.objects.filter(is_active=True).first()
            if risk_config:
                return {
                    'max_position_size': float(risk_config.max_position_size),
                    'max_daily_loss': float(risk_config.max_daily_loss),
                    'stop_loss_pct': float(risk_config.stop_loss_pct),
                    **risk_config.config
                }
        except Exception:
            # Fall back to file configuration
            pass
        
        return None
    
    async def save_strategy_config(self, strategy_name: str, config: Dict[str, Any]):
        """Save strategy configuration to database"""
        strategy, created = await Strategy.objects.get_or_create(
            name=strategy_name,
            defaults={
                'strategy_type': config.get('type', 'unknown'),
                'symbol': config.get('symbol', ''),
                'parameters': config,
                'is_active': config.get('enabled', False)
            }
        )
        
        if not created:
            strategy.parameters = config
            strategy.is_active = config.get('enabled', False)
            await strategy.save()
        
        return strategy
    
    async def load_strategy_configs(self) -> Dict[str, Dict[str, Any]]:
        """Load strategy configurations from database"""
        configs = {}
        
        strategies = await Strategy.objects.filter(is_active=True).all()
        
        for strategy in strategies:
            configs[strategy.name] = {
                'type': strategy.strategy_type,
                'symbol': strategy.symbol,
                'enabled': strategy.is_active,
                **strategy.parameters
            }
        
        return configs
```

### 2. Market Data Storage Adapter

```python
# shared/adapters/market_data_storage_adapter.py
import sys
import os
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import asyncio
import asyncpg
import json

# Add existing cyberdelta to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../cyberdelta'))

from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HLRawCandle
from cyberdelta.apis.backpack.models.bp_raw_kline import BPRawKline

class MarketDataStorageAdapter:
    """Adapter to store market data from existing APIs to database
    
    This captures and stores data from the production-ready API clients
    including all enhancements like auto-lending balances and margin data.
    The existing APIs continue to work unchanged.
    """
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: Optional[asyncpg.Pool] = None
    
    async def start(self):
        """Initialize database connection pool"""
        self.pool = await asyncpg.create_pool(self.database_url)
    
    async def stop(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
    
    async def store_ticker_data(self, exchange: str, symbol: str, ticker_data: Dict[str, Any]):
        """Store ticker data from existing API responses"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO market_data (time, exchange, symbol, data_type, data)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (time, exchange, symbol, data_type) 
                DO UPDATE SET data = EXCLUDED.data
            """, 
            datetime.utcnow(),
            exchange,
            symbol,
            'ticker',
            json.dumps(ticker_data)
            )
    
    async def store_candle_data(self, exchange: str, symbol: str, candles: List[Any]):
        """Store candle data from existing API responses"""
        async with self.pool.acquire() as conn:
            for candle in candles:
                # Handle both Hyperliquid and Backpack candle formats
                if isinstance(candle, HLRawCandle):
                    candle_data = {
                        'timestamp': candle.timestamp.isoformat(),
                        'open': float(candle.open_price),
                        'high': float(candle.high_price),
                        'low': float(candle.low_price),
                        'close': float(candle.close_price),
                        'volume': float(candle.volume)
                    }
                elif isinstance(candle, BPRawKline):
                    candle_data = {
                        'timestamp': candle.timestamp.isoformat(),
                        'open': float(candle.open_price),
                        'high': float(candle.high_price),
                        'low': float(candle.low_price),
                        'close': float(candle.close_price),
                        'volume': float(candle.volume)
                    }
                else:
                    # Generic candle data
                    candle_data = dict(candle)
                
                await conn.execute("""
                    INSERT INTO market_data (time, exchange, symbol, data_type, data)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (time, exchange, symbol, data_type) 
                    DO UPDATE SET data = EXCLUDED.data
                """,
                candle.timestamp if hasattr(candle, 'timestamp') else datetime.utcnow(),
                exchange,
                symbol,
                'candle',
                json.dumps(candle_data)
                )
    
    async def store_funding_rate(self, exchange: str, symbol: str, funding_data: Dict[str, Any]):
        """Store funding rate data from existing API responses"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO market_data (time, exchange, symbol, data_type, data)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (time, exchange, symbol, data_type) 
                DO UPDATE SET data = EXCLUDED.data
            """,
            datetime.utcnow(),
            exchange,
            symbol,
            'funding_rate',
            json.dumps(funding_data)
            )
    
    async def get_historical_candles(
        self, 
        exchange: str, 
        symbol: str, 
        start_time: datetime, 
        end_time: datetime,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Retrieve historical candle data for analysis"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT time, data
                FROM market_data
                WHERE exchange = $1 
                    AND symbol = $2 
                    AND data_type = 'candle'
                    AND time BETWEEN $3 AND $4
                ORDER BY time ASC
                LIMIT $5
            """, exchange, symbol, start_time, end_time, limit)
            
            return [
                {
                    'timestamp': row['time'],
                    **json.loads(row['data'])
                }
                for row in rows
            ]
    
    async def get_funding_rate_history(
        self, 
        exchange: str, 
        symbol: str, 
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """Get funding rate history for analysis"""
        start_time = datetime.utcnow() - timedelta(days=days)
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT time, data
                FROM market_data
                WHERE exchange = $1 
                    AND symbol = $2 
                    AND data_type = 'funding_rate'
                    AND time >= $3
                ORDER BY time ASC
            """, exchange, symbol, start_time)
            
            return [
                {
                    'timestamp': row['time'],
                    **json.loads(row['data'])
                }
                for row in rows
            ]
```

### 3. Performance Data Storage Adapter

```python
# shared/adapters/performance_storage_adapter.py
import sys
import os
from typing import Dict, Any, List
from datetime import datetime, timedelta
import asyncpg
import json

# Add existing cyberdelta to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../cyberdelta'))

from cyberdelta.monitoring.performance_metrics import PerformanceMetrics

class PerformanceStorageAdapter:
    """Adapter to store performance data from existing portfolio tracker
    
    This preserves and extends the sophisticated performance tracking
    already built into the system, adding persistent storage for
    historical analysis while maintaining all existing calculations.
    """
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: Optional[asyncpg.Pool] = None
    
    async def start(self):
        """Initialize database connection pool"""
        self.pool = await asyncpg.create_pool(self.database_url)
    
    async def stop(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
    
    async def store_performance_snapshot(
        self, 
        strategy_name: str, 
        performance_data: Dict[str, Any]
    ):
        """Store performance snapshot from existing portfolio tracker"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO performance_snapshots 
                (time, strategy_name, total_pnl, daily_pnl, win_rate, total_trades, sharpe_ratio, max_drawdown, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            datetime.utcnow(),
            strategy_name,
            performance_data.get('total_pnl', 0),
            performance_data.get('daily_pnl', 0),
            performance_data.get('win_rate', 0),
            performance_data.get('total_trades', 0),
            performance_data.get('sharpe_ratio'),
            performance_data.get('max_drawdown'),
            json.dumps(performance_data.get('metadata', {}))
            )
    
    async def store_trade_record(self, trade_data: Dict[str, Any]):
        """Store individual trade record"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO trades 
                (timestamp, strategy_name, exchange, symbol, side, order_type, quantity, price, fee, realized_pnl, order_id, fill_id, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            """,
            trade_data['timestamp'],
            trade_data['strategy_name'],
            trade_data['exchange'],
            trade_data['symbol'],
            trade_data['side'],
            trade_data.get('order_type', 'market'),
            trade_data['quantity'],
            trade_data['price'],
            trade_data.get('fee', 0),
            trade_data.get('realized_pnl', 0),
            trade_data.get('order_id'),
            trade_data.get('fill_id'),
            json.dumps(trade_data.get('metadata', {}))
            )
    
    async def get_strategy_performance_history(
        self, 
        strategy_name: str, 
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """Get historical performance data for charts"""
        start_time = datetime.utcnow() - timedelta(days=days)
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT time, total_pnl, daily_pnl, win_rate, total_trades, sharpe_ratio, max_drawdown
                FROM performance_snapshots
                WHERE strategy_name = $1 AND time >= $2
                ORDER BY time ASC
            """, strategy_name, start_time)
            
            return [
                {
                    'timestamp': row['time'],
                    'total_pnl': float(row['total_pnl']),
                    'daily_pnl': float(row['daily_pnl']),
                    'win_rate': float(row['win_rate']),
                    'total_trades': row['total_trades'],
                    'sharpe_ratio': float(row['sharpe_ratio']) if row['sharpe_ratio'] else None,
                    'max_drawdown': float(row['max_drawdown']) if row['max_drawdown'] else None
                }
                for row in rows
            ]
    
    async def get_recent_trades(
        self, 
        strategy_name: str = None, 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get recent trades for display"""
        async with self.pool.acquire() as conn:
            if strategy_name:
                rows = await conn.fetch("""
                    SELECT * FROM trades
                    WHERE strategy_name = $1
                    ORDER BY timestamp DESC
                    LIMIT $2
                """, strategy_name, limit)
            else:
                rows = await conn.fetch("""
                    SELECT * FROM trades
                    ORDER BY timestamp DESC
                    LIMIT $1
                """, limit)
            
            return [
                {
                    'id': row['id'],
                    'timestamp': row['timestamp'],
                    'strategy_name': row['strategy_name'],
                    'exchange': row['exchange'],
                    'symbol': row['symbol'],
                    'side': row['side'],
                    'order_type': row['order_type'],
                    'quantity': float(row['quantity']),
                    'price': float(row['price']),
                    'fee': float(row['fee']),
                    'realized_pnl': float(row['realized_pnl']),
                    'order_id': row['order_id'],
                    'fill_id': row['fill_id']
                }
                for row in rows
            ]
```

## Integration with Existing Components

### 1. Enhanced State Manager (Preserves Existing Logic)

```python
# shared/adapters/enhanced_state_manager.py
import sys
import os
from typing import Any, Optional
import asyncio

# Add existing cyberdelta to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../cyberdelta'))

from cyberdelta.utils.state_manager import StateManager

class EnhancedStateManager(StateManager):
    """Enhanced state manager that adds database persistence to existing logic
    
    This extends the existing state management system without breaking
    any current functionality. All existing code continues to work
    exactly as before, with optional database persistence added.
    """
    
    def __init__(self, storage_adapter: Optional[Any] = None):
        # Initialize existing state manager (unchanged)
        super().__init__()
        
        # Add optional database storage
        self.storage_adapter = storage_adapter
        self.enable_persistence = storage_adapter is not None
    
    async def save_state(self, key: str, value: Any):
        """Save state with optional database persistence"""
        # Use existing in-memory logic (unchanged)
        await super().save_state(key, value)
        
        # Optionally persist to database
        if self.enable_persistence:
            try:
                await self.storage_adapter.store_state(key, value)
            except Exception as e:
                # Log error but don't fail the operation
                print(f"Failed to persist state {key}: {e}")
    
    async def load_state(self, key: str) -> Any:
        """Load state with database fallback"""
        # Try existing in-memory storage first (unchanged)
        value = await super().load_state(key)
        
        # If not found and database enabled, try database
        if value is None and self.enable_persistence:
            try:
                value = await self.storage_adapter.load_state(key)
                if value is not None:
                    # Cache in memory for future access
                    await super().save_state(key, value)
            except Exception as e:
                print(f"Failed to load state {key} from database: {e}")
        
        return value
    
    async def enable_database_persistence(self, storage_adapter: Any):
        """Enable database persistence without changing existing behavior"""
        self.storage_adapter = storage_adapter
        self.enable_persistence = True
    
    async def disable_database_persistence(self):
        """Disable database persistence, fall back to memory only"""
        self.storage_adapter = None
        self.enable_persistence = False
```

### 2. Real-time Data Pipeline Integration

```python
# shared/adapters/realtime_data_pipeline.py
import sys
import os
import asyncio
from typing import Any, Dict

# Add existing cyberdelta to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../cyberdelta'))

from cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler import HLWebSocketRawMessageHandler

class EnhancedWebSocketHandler(HLWebSocketRawMessageHandler):
    """Enhanced WebSocket handler that adds database storage to existing logic"""
    
    def __init__(self, *args, **kwargs):
        # Initialize existing handler (unchanged)
        super().__init__(*args, **kwargs)
        
        # Add database storage adapters
        self.market_data_storage = None
        self.performance_storage = None
    
    def set_storage_adapters(self, market_data_storage, performance_storage):
        """Set database storage adapters"""
        self.market_data_storage = market_data_storage
        self.performance_storage = performance_storage
    
    async def on_ticker_update(self, ticker_data: Dict[str, Any]):
        """Enhanced ticker update with database storage"""
        # Use existing ticker processing logic (unchanged)
        processed_ticker = await super().on_ticker_update(ticker_data)
        
        # Add database storage if enabled
        if self.market_data_storage:
            try:
                await self.market_data_storage.store_ticker_data(
                    exchange='hyperliquid',
                    symbol=processed_ticker.symbol,
                    ticker_data={
                        'last_price': float(processed_ticker.last_price),
                        'bid_price': float(processed_ticker.bid_price) if processed_ticker.bid_price else None,
                        'ask_price': float(processed_ticker.ask_price) if processed_ticker.ask_price else None,
                        'volume_24h': float(processed_ticker.volume_24h) if processed_ticker.volume_24h else None,
                        'timestamp': processed_ticker.timestamp.isoformat()
                    }
                )
            except Exception as e:
                # Log error but don't interrupt existing flow
                print(f"Failed to store ticker data: {e}")
        
        return processed_ticker
    
    async def on_trade_execution(self, trade_data: Dict[str, Any]):
        """Enhanced trade execution with database storage"""
        # Use existing trade processing logic (unchanged)
        processed_trade = await super().on_trade_execution(trade_data)
        
        # Add database storage if enabled
        if self.performance_storage:
            try:
                await self.performance_storage.store_trade_record({
                    'timestamp': processed_trade.timestamp,
                    'strategy_name': processed_trade.strategy_name,
                    'exchange': 'hyperliquid',
                    'symbol': processed_trade.symbol,
                    'side': processed_trade.side,
                    'quantity': processed_trade.quantity,
                    'price': processed_trade.price,
                    'fee': processed_trade.fee,
                    'realized_pnl': processed_trade.realized_pnl,
                    'order_id': processed_trade.order_id,
                    'fill_id': processed_trade.fill_id
                })
            except Exception as e:
                # Log error but don't interrupt existing flow
                print(f"Failed to store trade record: {e}")
        
        return processed_trade
```

## Database Operations and Maintenance

### 1. TimescaleDB Setup and Configuration

```sql
-- Database initialization script
-- Run after PostgreSQL + TimescaleDB installation

-- Create database
CREATE DATABASE cyberdelta_db;

-- Connect to cyberdelta_db
\c cyberdelta_db;

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS config;
CREATE SCHEMA IF NOT EXISTS historical;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Set search path
SET search_path TO public, config, historical, audit, analytics;

-- Create hypertables for time-series data
SELECT create_hypertable('market_data', 'time', chunk_time_interval => INTERVAL '1 hour');
SELECT create_hypertable('performance_snapshots', 'time', chunk_time_interval => INTERVAL '1 day');
SELECT create_hypertable('audit_events', 'timestamp', chunk_time_interval => INTERVAL '1 day');
SELECT create_hypertable('error_logs', 'timestamp', chunk_time_interval => INTERVAL '1 day');

-- Create indexes for performance
CREATE INDEX CONCURRENTLY idx_market_data_exchange_symbol ON market_data (exchange, symbol, time DESC);
CREATE INDEX CONCURRENTLY idx_performance_strategy ON performance_snapshots (strategy_name, time DESC);
CREATE INDEX CONCURRENTLY idx_trades_strategy_time ON trades (strategy_name, timestamp DESC);

-- Set up data retention policies
-- Keep market data for 1 year
SELECT add_retention_policy('market_data', INTERVAL '1 year');

-- Keep performance snapshots for 2 years
SELECT add_retention_policy('performance_snapshots', INTERVAL '2 years');

-- Keep audit events for 5 years
SELECT add_retention_policy('audit_events', INTERVAL '5 years');

-- Keep error logs for 1 year
SELECT add_retention_policy('error_logs', INTERVAL '1 year');
```

### 2. Backup and Recovery Strategy

```bash
#!/bin/bash
# backup_cyberdelta.sh - Database backup script

# Configuration
DB_NAME="cyberdelta_db"
BACKUP_DIR="/var/backups/cyberdelta"
RETENTION_DAYS=30

# Create backup directory
mkdir -p $BACKUP_DIR

# Full database backup
pg_dump -h localhost -U postgres -d $DB_NAME \
    --format=custom \
    --compress=9 \
    --file="$BACKUP_DIR/cyberdelta_full_$(date +%Y%m%d_%H%M%S).backup"

# Configuration-only backup (lightweight)
pg_dump -h localhost -U postgres -d $DB_NAME \
    --schema=config \
    --format=custom \
    --file="$BACKUP_DIR/cyberdelta_config_$(date +%Y%m%d_%H%M%S).backup"

# Clean old backups
find $BACKUP_DIR -name "*.backup" -mtime +$RETENTION_DAYS -delete

echo "Backup completed: $(date)"
```

### 3. Performance Monitoring Queries

```sql
-- Monitor database performance
SELECT 
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation,
    most_common_vals
FROM pg_stats 
WHERE schemaname IN ('public', 'config', 'historical')
ORDER BY schemaname, tablename;

-- Check hypertable compression
SELECT 
    hypertable_schema,
    hypertable_name,
    chunk_schema,
    chunk_name,
    compression_status,
    uncompressed_total_bytes,
    compressed_total_bytes
FROM timescaledb_information.chunks
WHERE compression_status = 'Compressed'
ORDER BY hypertable_name, chunk_name;

-- Monitor query performance
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    rows
FROM pg_stat_statements
WHERE query LIKE '%market_data%' OR query LIKE '%performance_snapshots%'
ORDER BY total_time DESC
LIMIT 10;
```

## Benefits of This Database Integration

### 1. Preserves Existing Architecture
- **Zero changes** to existing state management logic
- **Zero changes** to existing business logic
- **Zero changes** to existing API integrations
- **Zero risk** to proven trading algorithms

### 2. Adds Powerful Capabilities
- **Historical analytics**: Complete trade and performance history
- **Configuration persistence**: Database-backed configuration management
- **Audit trails**: Complete system event logging
- **Scalable storage**: TimescaleDB for efficient time-series operations

### 3. Operational Benefits
- **Better monitoring**: Historical performance analysis
- **Compliance support**: Complete audit trails
- **Disaster recovery**: Database backups and replication
- **Multi-user support**: User management and permissions

### 4. Future Flexibility
- **Gradual migration**: Can move from YAML to database over time
- **Analytics platform**: Rich data for business intelligence
- **API foundation**: Database enables REST API functionality
- **Scaling path**: Supports horizontal scaling with read replicas

This database integration approach gives you the benefits of persistent storage while maintaining the reliability and performance of your existing in-memory architecture.

## Key Database Integration Principles

### 1. Additive, Not Replacement
- Existing state management continues unchanged
- Database storage is optional layer on top
- All current performance characteristics preserved
- Easy rollback to memory-only operation

### 2. TimescaleDB for Trading Data
- Optimized for time-series market data storage
- Efficient compression for historical data
- Fast queries for analytics and charting
- Automatic data retention policies

### 3. Preserve Existing Patterns
- Use existing configuration system as primary
- Maintain existing error handling and validation
- Keep all sophisticated features (auto-lending, margin calculations)
- Support existing test infrastructure

### 4. Multi-User Foundation
- User-based data isolation
- Permission-based access control
- Audit trails for compliance
- Session management support

The database layer enables the dashboard to provide historical analysis and multi-user support while ensuring zero impact on the core trading functionality that already works excellently.
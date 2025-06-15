# Django Refactor: Database Models Design (Wrapper Pattern)

## Overview

This document outlines the Django model design for the **wrapper layer** that provides persistence without modifying the core CyberDeltaEngine. These models mirror the existing Pydantic models and are populated via background synchronization from the running core engine.

## Core Model Categories

### 1. Exchange & Asset Management

```python
# apps/exchanges/models.py

class Exchange(models.Model):
    """Supported cryptocurrency exchanges"""
    name = models.CharField(max_length=50, unique=True)
    display_name = models.CharField(max_length=100)
    api_base_url = models.URLField()
    websocket_url = models.URLField()
    is_active = models.BooleanField(default=True)
    supported_features = models.JSONField(default=dict)  # spot, futures, margin, etc.
    rate_limit_per_minute = models.PositiveIntegerField(default=1000)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['name']

class Asset(models.Model):
    """Cryptocurrency assets"""
    symbol = models.CharField(max_length=20, unique=True)  # BTC, ETH, USDC
    name = models.CharField(max_length=100)  # Bitcoin, Ethereum
    decimals = models.PositiveSmallIntegerField(default=8)
    is_stablecoin = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    
    class Meta:
        ordering = ['symbol']

class TradingPair(models.Model):
    """Trading pairs available on exchanges"""
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE)
    base_asset = models.ForeignKey(Asset, on_delete=models.CASCADE, related_name='base_pairs')
    quote_asset = models.ForeignKey(Asset, on_delete=models.CASCADE, related_name='quote_pairs')
    symbol = models.CharField(max_length=50)  # BTC-USDC, ETH-PERP
    pair_type = models.CharField(max_length=20, choices=[
        ('spot', 'Spot'),
        ('perpetual', 'Perpetual'),
        ('future', 'Future'),
        ('option', 'Option')
    ])
    is_active = models.BooleanField(default=True)
    min_order_size = models.DecimalField(max_digits=20, decimal_places=10)
    max_order_size = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    tick_size = models.DecimalField(max_digits=20, decimal_places=10)
    
    class Meta:
        unique_together = ['exchange', 'symbol']
        ordering = ['exchange', 'symbol']
```

### 2. Market Data Models

```python
# apps/market_data/models.py

class Ticker(models.Model):
    """Real-time ticker data"""
    trading_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE)
    last_price = models.DecimalField(max_digits=20, decimal_places=10)
    bid_price = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    ask_price = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    volume_24h = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    price_change_24h = models.DecimalField(max_digits=10, decimal_places=4, null=True)
    timestamp = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['trading_pair', '-timestamp']),
        ]

class Candle(models.Model):
    """OHLCV candlestick data - optimized for TimescaleDB"""
    trading_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE)
    open_time = models.DateTimeField()
    close_time = models.DateTimeField()
    interval = models.CharField(max_length=10)  # 1m, 5m, 1h, 1d
    open_price = models.DecimalField(max_digits=20, decimal_places=10)
    high_price = models.DecimalField(max_digits=20, decimal_places=10)
    low_price = models.DecimalField(max_digits=20, decimal_places=10)
    close_price = models.DecimalField(max_digits=20, decimal_places=10)
    volume = models.DecimalField(max_digits=20, decimal_places=10)
    quote_volume = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    trade_count = models.PositiveIntegerField(null=True)
    
    class Meta:
        unique_together = ['trading_pair', 'interval', 'open_time']
        ordering = ['-open_time']
        indexes = [
            models.Index(fields=['trading_pair', 'interval', '-open_time']),
        ]

class FundingRate(models.Model):
    """Perpetual contract funding rates"""
    trading_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE)
    funding_rate = models.DecimalField(max_digits=10, decimal_places=8)
    predicted_rate = models.DecimalField(max_digits=10, decimal_places=8, null=True)
    next_funding_time = models.DateTimeField()
    timestamp = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ['trading_pair', 'timestamp']
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['trading_pair', '-timestamp']),
            models.Index(fields=['-timestamp']),  # For cross-pair analysis
        ]

class OrderBook(models.Model):
    """Order book snapshots"""
    trading_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE)
    bids = models.JSONField()  # [["price", "quantity"], ...]
    asks = models.JSONField()  # [["price", "quantity"], ...]
    timestamp = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['trading_pair', '-timestamp']),
        ]
```

### 3. Portfolio & Trading Models

```python
# apps/portfolio/models.py

class Account(models.Model):
    """Trading accounts across exchanges"""
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE)
    account_id = models.CharField(max_length=100)  # Exchange-specific account ID
    account_type = models.CharField(max_length=20, choices=[
        ('spot', 'Spot'),
        ('margin', 'Margin'),
        ('futures', 'Futures'),
        ('unified', 'Unified')
    ])
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ['exchange', 'account_id', 'account_type']

class Balance(models.Model):
    """Asset balances per account"""
    account = models.ForeignKey(Account, on_delete=models.CASCADE)
    asset = models.ForeignKey(Asset, on_delete=models.CASCADE)
    total_balance = models.DecimalField(max_digits=20, decimal_places=10, default=0)
    available_balance = models.DecimalField(max_digits=20, decimal_places=10, default=0)
    locked_balance = models.DecimalField(max_digits=20, decimal_places=10, default=0)
    timestamp = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ['account', 'asset', 'timestamp']
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['account', 'asset', '-timestamp']),
        ]

class Position(models.Model):
    """Derivative positions (futures/perpetuals)"""
    account = models.ForeignKey(Account, on_delete=models.CASCADE)
    trading_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE)
    side = models.CharField(max_length=10, choices=[('long', 'Long'), ('short', 'Short')])
    size = models.DecimalField(max_digits=20, decimal_places=10)
    entry_price = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    mark_price = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    liquidation_price = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    unrealized_pnl = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    margin_requirement = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    timestamp = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ['account', 'trading_pair', 'timestamp']
        ordering = ['-timestamp']

class Order(models.Model):
    """Order tracking"""
    account = models.ForeignKey(Account, on_delete=models.CASCADE)
    trading_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE)
    order_id = models.CharField(max_length=100)
    client_order_id = models.CharField(max_length=100, null=True)
    side = models.CharField(max_length=10, choices=[('buy', 'Buy'), ('sell', 'Sell')])
    order_type = models.CharField(max_length=20, choices=[
        ('market', 'Market'),
        ('limit', 'Limit'),
        ('stop', 'Stop'),
        ('stop_limit', 'Stop Limit')
    ])
    quantity = models.DecimalField(max_digits=20, decimal_places=10)
    price = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    filled_quantity = models.DecimalField(max_digits=20, decimal_places=10, default=0)
    average_fill_price = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    status = models.CharField(max_length=20, choices=[
        ('pending', 'Pending'),
        ('open', 'Open'),
        ('filled', 'Filled'),
        ('partially_filled', 'Partially Filled'),
        ('cancelled', 'Cancelled'),
        ('rejected', 'Rejected')
    ])
    time_in_force = models.CharField(max_length=10, default='GTC')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = ['account', 'order_id']
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['account', 'status', '-created_at']),
            models.Index(fields=['trading_pair', '-created_at']),
        ]

class Trade(models.Model):
    """Executed trades"""
    order = models.ForeignKey(Order, on_delete=models.CASCADE, null=True)
    account = models.ForeignKey(Account, on_delete=models.CASCADE)
    trading_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE)
    trade_id = models.CharField(max_length=100)
    side = models.CharField(max_length=10, choices=[('buy', 'Buy'), ('sell', 'Sell')])
    quantity = models.DecimalField(max_digits=20, decimal_places=10)
    price = models.DecimalField(max_digits=20, decimal_places=10)
    fee = models.DecimalField(max_digits=20, decimal_places=10, default=0)
    fee_asset = models.ForeignKey(Asset, on_delete=models.CASCADE, null=True)
    is_maker = models.BooleanField(default=False)
    executed_at = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ['account', 'trade_id']
        ordering = ['-executed_at']
        indexes = [
            models.Index(fields=['account', '-executed_at']),
            models.Index(fields=['trading_pair', '-executed_at']),
        ]
```

### 4. Strategy & Signal Models

```python
# apps/strategies/models.py

class Strategy(models.Model):
    """Strategy definitions"""
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField()
    strategy_type = models.CharField(max_length=50)  # funding_arbitrage, market_making
    module_path = models.CharField(max_length=200)  # Python import path
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class StrategyInstance(models.Model):
    """Running strategy instances"""
    strategy = models.ForeignKey(Strategy, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)
    config = models.JSONField()  # Strategy-specific configuration
    accounts = models.ManyToManyField(Account)  # Accounts this strategy can trade on
    trading_pairs = models.ManyToManyField(TradingPair)  # Pairs this strategy trades
    is_active = models.BooleanField(default=True)
    max_position_size = models.DecimalField(max_digits=20, decimal_places=10)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = ['strategy', 'name']

class TradeSignal(models.Model):
    """Trading signals generated by strategies"""
    strategy_instance = models.ForeignKey(StrategyInstance, on_delete=models.CASCADE)
    signal_id = models.CharField(max_length=100, unique=True)
    trading_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE)
    signal_type = models.CharField(max_length=20, choices=[
        ('enter_long', 'Enter Long'),
        ('enter_short', 'Enter Short'),
        ('exit_long', 'Exit Long'),
        ('exit_short', 'Exit Short'),
        ('adjust', 'Adjust Position')
    ])
    side = models.CharField(max_length=10, choices=[('buy', 'Buy'), ('sell', 'Sell')])
    quantity = models.DecimalField(max_digits=20, decimal_places=10)
    price = models.DecimalField(max_digits=20, decimal_places=10, null=True)
    confidence = models.DecimalField(max_digits=5, decimal_places=4, null=True)
    metadata = models.JSONField(default=dict)
    status = models.CharField(max_length=20, choices=[
        ('pending', 'Pending'),
        ('executed', 'Executed'),
        ('rejected', 'Rejected'),
        ('expired', 'Expired')
    ], default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    executed_at = models.DateTimeField(null=True)
    
    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['strategy_instance', 'status', '-created_at']),
            models.Index(fields=['trading_pair', '-created_at']),
        ]

class ArbitrageOpportunity(models.Model):
    """Detected arbitrage opportunities"""
    strategy_instance = models.ForeignKey(StrategyInstance, on_delete=models.CASCADE)
    long_exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE, related_name='arb_long')
    short_exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE, related_name='arb_short')
    long_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE, related_name='arb_long')
    short_pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE, related_name='arb_short')
    price_spread = models.DecimalField(max_digits=10, decimal_places=6)
    funding_rate_spread = models.DecimalField(max_digits=10, decimal_places=6, null=True)
    estimated_profit = models.DecimalField(max_digits=20, decimal_places=10)
    opportunity_type = models.CharField(max_length=30, choices=[
        ('funding_rate', 'Funding Rate'),
        ('price_arbitrage', 'Price Arbitrage'),
        ('triangular', 'Triangular Arbitrage')
    ])
    is_executed = models.BooleanField(default=False)
    expires_at = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['strategy_instance', 'is_executed', '-created_at']),
            models.Index(fields=['-estimated_profit']),
        ]
```

### 5. Configuration Models

```python
# apps/config/models.py

class SystemConfig(models.Model):
    """System-wide configuration"""
    key = models.CharField(max_length=100, unique=True)
    value = models.JSONField()
    description = models.TextField(blank=True)
    is_secret = models.BooleanField(default=False)  # For sensitive config
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class ExchangeConfig(models.Model):
    """Exchange-specific configuration"""
    exchange = models.OneToOneField(Exchange, on_delete=models.CASCADE)
    api_key_hash = models.CharField(max_length=64)  # Hashed for security
    api_secret_hash = models.CharField(max_length=64)
    testnet_mode = models.BooleanField(default=True)
    max_requests_per_minute = models.PositiveIntegerField(default=1000)
    timeout_seconds = models.PositiveIntegerField(default=30)
    retry_attempts = models.PositiveSmallIntegerField(default=3)
    additional_config = models.JSONField(default=dict)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class RiskConfig(models.Model):
    """Risk management configuration"""
    name = models.CharField(max_length=100, unique=True)
    max_position_usd = models.DecimalField(max_digits=20, decimal_places=2)
    max_total_exposure_usd = models.DecimalField(max_digits=20, decimal_places=2)
    max_drawdown_percent = models.DecimalField(max_digits=5, decimal_places=2)
    position_size_limits = models.JSONField(default=dict)  # Per-asset limits
    blacklisted_assets = models.JSONField(default=list)
    circuit_breaker_config = models.JSONField(default=dict)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

## Database Optimization Considerations

### TimescaleDB Integration
```sql
-- Convert time-series tables to hypertables
SELECT create_hypertable('market_data_candle', 'open_time');
SELECT create_hypertable('market_data_ticker', 'timestamp');
SELECT create_hypertable('market_data_fundingrate', 'timestamp');
SELECT create_hypertable('portfolio_balance', 'timestamp');
SELECT create_hypertable('portfolio_position', 'timestamp');

-- Create continuous aggregates for common queries
CREATE MATERIALIZED VIEW daily_funding_rates AS
SELECT trading_pair_id,
       time_bucket('1 day', timestamp) AS day,
       avg(funding_rate) as avg_funding_rate,
       max(funding_rate) as max_funding_rate,
       min(funding_rate) as min_funding_rate
FROM market_data_fundingrate
GROUP BY trading_pair_id, day;
```

### Indexing Strategy
```sql
-- Performance-critical indexes
CREATE INDEX CONCURRENTLY idx_candle_pair_interval_time 
ON market_data_candle (trading_pair_id, interval, open_time DESC);

CREATE INDEX CONCURRENTLY idx_ticker_pair_time 
ON market_data_ticker (trading_pair_id, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_trade_account_time 
ON portfolio_trade (account_id, executed_at DESC);

-- Composite indexes for dashboard queries
CREATE INDEX CONCURRENTLY idx_signal_strategy_status_time 
ON strategies_tradesignal (strategy_instance_id, status, created_at DESC);
```

## Data Synchronization Strategy

### 1. Core → Django Model Mapping
- **core.models.Candle** → **market_data.Candle** (via background sync)
- **core.models.Trade** → **portfolio.Trade** (real-time sync)
- **core.models.SpotBalance** → **portfolio.Balance** (periodic sync)
- **core.models.DerivativePosition** → **portfolio.Position** (periodic sync)
- **core.models.TradeSignal** → **strategies.TradeSignal** (real-time sync)

### 2. Synchronization Architecture
```python
# apps/bridge/sync_service.py
from django.core.management.base import BaseCommand
import asyncio
from typing import Dict, Any
from cyberdelta.core import CyberDeltaEngine

class DataSyncService:
    """Synchronizes data from core engine to Django database"""
    
    def __init__(self):
        self.engine = None  # Reference to running core engine
        self.sync_interval = 5  # seconds
        
    async def connect_to_core(self):
        """Establish connection to core engine process"""
        # Connect via IPC, Redis, or ZeroMQ
        pass
        
    async def sync_market_data(self):
        """Sync market data from core to database"""
        while True:
            try:
                # Get latest data from core
                tickers = await self.engine.get_latest_tickers()
                
                # Bulk create in database
                ticker_objects = []
                for ticker_data in tickers:
                    ticker_objects.append(
                        Ticker(
                            trading_pair_id=self.get_pair_id(ticker_data),
                            last_price=ticker_data.last_price,
                            bid_price=ticker_data.bid_price,
                            ask_price=ticker_data.ask_price,
                            timestamp=ticker_data.timestamp
                        )
                    )
                
                # Efficient bulk insert
                Ticker.objects.bulk_create(
                    ticker_objects,
                    batch_size=1000,
                    ignore_conflicts=True
                )
                
                await asyncio.sleep(self.sync_interval)
                
            except Exception as e:
                logger.error(f"Market data sync error: {e}")
                await asyncio.sleep(self.sync_interval * 2)
    
    async def sync_portfolio_data(self):
        """Sync portfolio data from core to database"""
        while True:
            try:
                # Get portfolio state from core
                portfolio_state = await self.engine.get_portfolio_state()
                
                # Update balances
                for balance_data in portfolio_state.balances:
                    Balance.objects.update_or_create(
                        account_id=balance_data.account_id,
                        asset_id=balance_data.asset_id,
                        defaults={
                            'total_balance': balance_data.total,
                            'available_balance': balance_data.available,
                            'locked_balance': balance_data.locked,
                            'timestamp': timezone.now()
                        }
                    )
                
                # Update positions
                for position_data in portfolio_state.positions:
                    Position.objects.update_or_create(
                        account_id=position_data.account_id,
                        trading_pair_id=position_data.pair_id,
                        defaults={
                            'side': position_data.side,
                            'size': position_data.size,
                            'entry_price': position_data.entry_price,
                            'mark_price': position_data.mark_price,
                            'unrealized_pnl': position_data.unrealized_pnl,
                            'timestamp': timezone.now()
                        }
                    )
                
                await asyncio.sleep(self.sync_interval * 2)  # Less frequent
                
            except Exception as e:
                logger.error(f"Portfolio sync error: {e}")
                await asyncio.sleep(self.sync_interval * 4)

# Management command to run sync service
class Command(BaseCommand):
    def handle(self, *args, **options):
        sync_service = DataSyncService()
        asyncio.run(sync_service.start())
```

### 3. Real-time Event Streaming
```python
# apps/bridge/event_bridge.py
class EventBridge:
    """Bridges events from core engine to Django"""
    
    def __init__(self):
        self.redis_client = redis.Redis()
        self.channel_layer = get_channel_layer()
        
    async def listen_for_events(self):
        """Listen for events from core engine"""
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe('core_events')
        
        for message in pubsub.listen():
            if message['type'] == 'message':
                event = json.loads(message['data'])
                await self.handle_core_event(event)
    
    async def handle_core_event(self, event: Dict[str, Any]):
        """Process events from core engine"""
        event_type = event['type']
        
        if event_type == 'trade_executed':
            # Save to database
            Trade.objects.create(
                account_id=event['account_id'],
                trading_pair_id=event['pair_id'],
                side=event['side'],
                quantity=event['quantity'],
                price=event['price'],
                fee=event['fee'],
                executed_at=event['timestamp']
            )
            
            # Broadcast to WebSocket clients
            await self.channel_layer.group_send("trades", {
                "type": "trade_update",
                "trade": event
            })
            
        elif event_type == 'signal_generated':
            # Save trading signal
            TradeSignal.objects.create(
                strategy_instance_id=event['strategy_id'],
                signal_type=event['signal_type'],
                trading_pair_id=event['pair_id'],
                side=event['side'],
                quantity=event['quantity'],
                created_at=event['timestamp']
            )
```

## Performance Considerations

### Read Optimization
- **Dashboard queries**: Read from local database, not core
- **Historical analysis**: TimescaleDB continuous aggregates
- **Real-time display**: Redis cache for latest values
- **Bulk reads**: Django select_related and prefetch_related

### Write Optimization
- **Bulk sync**: Batch inserts every 5 seconds
- **Event streaming**: Real-time for critical events only
- **Data retention**: Automatic old data cleanup
- **Connection pooling**: Persistent connections to core

### Data Consistency
- **Eventually consistent**: Database lags core by sync interval
- **Critical data**: Real-time sync for trades and signals
- **Reconciliation**: Periodic full sync to catch any gaps
- **Monitoring**: Track sync lag and alert on issues

This wrapper approach ensures the core engine remains unchanged while providing all the benefits of persistent storage for historical analysis and multi-user access.
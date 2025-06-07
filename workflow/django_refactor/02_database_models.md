# Django Refactor: Database Models Design

## Overview

This document outlines the Django model design to replace the current Pydantic-based in-memory models. The goal is to provide persistent storage while maintaining the same data integrity and relationships.

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

## Migration Strategy from Current Models

### 1. Pydantic → Django Model Mapping
- **Current Candle** → **market_data.Candle**
- **Current Trade** → **portfolio.Trade** 
- **Current SpotBalance** → **portfolio.Balance**
- **Current DerivativePosition** → **portfolio.Position**
- **Current TradeSignal** → **strategies.TradeSignal**

### 2. Data Migration Scripts
```python
# Example migration script
def migrate_pydantic_to_django():
    """Migrate existing Pydantic data to Django models"""
    
    # Create exchanges and assets
    for exchange_name in current_exchanges:
        exchange, created = Exchange.objects.get_or_create(
            name=exchange_name,
            defaults={...}
        )
    
    # Migrate trading pairs
    for pair_data in current_trading_pairs:
        trading_pair, created = TradingPair.objects.get_or_create(
            exchange=exchange,
            symbol=pair_data.symbol,
            defaults={...}
        )
    
    # Migrate historical data if needed
    for candle_data in historical_candles:
        Candle.objects.create(
            trading_pair=trading_pair,
            open_time=candle_data.timestamp,
            # ... other fields
        )
```

## Performance Considerations

### Read Optimization
- **Time-series queries**: Use TimescaleDB features
- **Dashboard aggregations**: Pre-computed materialized views
- **Real-time data**: Redis caching for latest values
- **Bulk operations**: Use Django's bulk_create and bulk_update

### Write Optimization
- **Batch inserts**: Collect market data and insert in batches
- **Connection pooling**: Use pgbouncer for database connections
- **Async tasks**: Celery for non-critical data writes
- **Partitioning**: Automatic partitioning for large time-series tables

This model design provides a solid foundation for the Django refactor while maintaining data integrity and supporting the high-performance requirements of a trading system.
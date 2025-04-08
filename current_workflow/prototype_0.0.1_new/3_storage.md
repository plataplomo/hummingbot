# Storage and Data Management

## Overview

The storage subsystem handles persistent and in-memory data storage for the CyberDelta Engine. This document outlines the key storage components, data structures, and interactions.

## Data Categories

### Configuration
- Environment-specific settings
- API keys (encrypted)
- Connection parameters
- Strategy parameters

### Market Data
- Order book snapshots
- Real-time trade data
- Funding rate history
- Price candles
- Exchange-specific metrics

### Account Data
- Wallet balances
- Open positions
- Order history
- Execution performance metrics
- Collateral allocation

### Strategy Data
- Signal generation parameters
- Position sizing rules
- Risk parameters
- Opportunity detection thresholds
- Historical performance metrics

## Storage Components

### In-Memory Cache
A high-performance in-memory cache stores frequently accessed data, particularly real-time market data and active positions.

```python
class DataCache:
    """High-performance in-memory cache for market and account data."""
    
    def __init__(self, max_order_book_depth=10, expiration_time=300):
        self.order_books = {}  # Exchange -> Market -> OrderBook
        self.recent_trades = {}  # Exchange -> Market -> List[Trade]
        self.active_positions = {}  # Exchange -> Market -> Position
        self.active_orders = {}  # Exchange -> List[Order]
        self.funding_rates = {}  # Exchange -> Market -> List[FundingRate]
        self.max_order_book_depth = max_order_book_depth
        self.expiration_time = expiration_time
        self._last_updated = {}  # Key -> timestamp
        
    def update_order_book(self, exchange, market, order_book):
        """Update order book for specific exchange and market."""
        if exchange not in self.order_books:
            self.order_books[exchange] = {}
        self.order_books[exchange][market] = order_book
        self._set_updated(f"order_book:{exchange}:{market}")
    
    def get_order_book(self, exchange, market):
        """Get order book for specific exchange and market."""
        if not self._is_valid(f"order_book:{exchange}:{market}"):
            return None
        if exchange in self.order_books and market in self.order_books[exchange]:
            return self.order_books[exchange][market]
        return None
    
    def update_active_position(self, exchange, market, position):
        """Update active position for specific exchange and market."""
        if exchange not in self.active_positions:
            self.active_positions[exchange] = {}
        self.active_positions[exchange][market] = position
        self._set_updated(f"position:{exchange}:{market}")
    
    def _set_updated(self, key):
        """Mark a key as recently updated."""
        self._last_updated[key] = time.time()
    
    def _is_valid(self, key):
        """Check if a key's data is still valid based on expiration time."""
        if key not in self._last_updated:
            return False
        return (time.time() - self._last_updated[key]) < self.expiration_time
```

### Persistent Storage
SQLite is used for persistent storage of historical data, configuration, and performance metrics.

```python
class SQLiteStorage:
    """Persistent storage using SQLite for historical data."""
    
    def __init__(self, db_path="data/cyberdelta.db"):
        self.db_path = db_path
        self._ensure_tables()
    
    def _ensure_tables(self):
        """Create tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Order history table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS order_history (
            id TEXT PRIMARY KEY,
            exchange TEXT,
            market TEXT,
            side TEXT,
            size REAL,
            price REAL,
            order_type TEXT,
            status TEXT,
            created_at INTEGER,
            updated_at INTEGER
        )
        ''')
        
        # Position history table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS position_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT,
            market TEXT,
            side TEXT,
            size REAL,
            entry_price REAL,
            exit_price REAL,
            pnl REAL,
            opened_at INTEGER,
            closed_at INTEGER
        )
        ''')
        
        # Funding rates table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS funding_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT,
            market TEXT,
            rate REAL,
            timestamp INTEGER
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def save_order(self, order):
        """Save order to persistent storage."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT OR REPLACE INTO order_history
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            order.id,
            order.exchange,
            order.market,
            order.side,
            order.size,
            order.price,
            order.order_type,
            order.status,
            order.created_at,
            int(time.time())
        ))
        
        conn.commit()
        conn.close()
    
    def save_funding_rate(self, exchange, market, rate, timestamp):
        """Save funding rate data."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO funding_rates (exchange, market, rate, timestamp)
        VALUES (?, ?, ?, ?)
        ''', (exchange, market, rate, timestamp))
        
        conn.commit()
        conn.close()
```

## Exchange-Specific Data Structures

### HyperLiquid Data Structures

HyperLiquid's data model has specific requirements that our storage system must accommodate:

```python
class HyperliquidOrderBook:
    """HyperLiquid-specific order book implementation."""
    
    def __init__(self, asset, bids=None, asks=None):
        self.asset = asset
        self.bids = bids or []  # List of [price, size] pairs
        self.asks = asks or []  # List of [price, size] pairs
        self.last_updated = time.time()
    
    def update(self, data):
        """Update order book with data from HyperLiquid WebSocket."""
        if 'bids' in data:
            self.bids = [[float(b[0]), float(b[1])] for b in data['bids']]
        if 'asks' in data:
            self.asks = [[float(a[0]), float(a[1])] for a in data['asks']]
        self.last_updated = time.time()
    
    def mid_price(self):
        """Calculate mid price from order book."""
        if not self.bids or not self.asks:
            return None
        return (self.bids[0][0] + self.asks[0][0]) / 2

class HyperliquidPosition:
    """HyperLiquid-specific position data structure."""
    
    def __init__(self, asset, size=0, entry_price=0, unrealized_pnl=0, leverage=1):
        self.asset = asset
        self.size = size  # Positive for long, negative for short
        self.entry_price = entry_price
        self.unrealized_pnl = unrealized_pnl
        self.leverage = leverage
        self.last_updated = time.time()
    
    def update_from_api(self, position_data):
        """Update position with data from HyperLiquid API."""
        if 'size' in position_data:
            self.size = float(position_data['size'])
        if 'entryPrice' in position_data:
            self.entry_price = float(position_data['entryPrice'])
        if 'unrealizedPnl' in position_data:
            self.unrealized_pnl = float(position_data['unrealizedPnl'])
        if 'leverage' in position_data:
            self.leverage = float(position_data['leverage'])
        self.last_updated = time.time()
```

### Backpack Data Structures

Backpack has its own unique data structures that we need to handle:

```python
class BackpackOrderBook:
    """Backpack-specific order book implementation."""
    
    def __init__(self, market, bids=None, asks=None):
        self.market = market
        self.bids = bids or []  # List of [price, size, order_id] tuples
        self.asks = asks or []  # List of [price, size, order_id] tuples
        self.last_updated = time.time()
    
    def update(self, data):
        """Update order book with data from Backpack WebSocket."""
        if 'bids' in data:
            self.bids = [[float(b[0]), float(b[1]), b[2]] for b in data['bids']]
        if 'asks' in data:
            self.asks = [[float(a[0]), float(a[1]), a[2]] for a in data['asks']]
        self.last_updated = time.time()
    
    def best_bid(self):
        """Get best bid price and size."""
        return self.bids[0] if self.bids else None
    
    def best_ask(self):
        """Get best ask price and size."""
        return self.asks[0] if self.asks else None

class BackpackPosition:
    """Backpack-specific position data structure."""
    
    def __init__(self, market, base_size=0, quote_size=0, entry_price=0, pnl=0):
        self.market = market
        self.base_size = base_size  # Amount of base asset
        self.quote_size = quote_size  # Amount of quote asset
        self.entry_price = entry_price
        self.pnl = pnl
        self.last_updated = time.time()
    
    def update_from_api(self, position_data):
        """Update position with data from Backpack API."""
        if 'baseSize' in position_data:
            self.base_size = float(position_data['baseSize'])
        if 'quoteSize' in position_data:
            self.quote_size = float(position_data['quoteSize'])
        if 'entryPrice' in position_data:
            self.entry_price = float(position_data['entryPrice'])
        if 'unrealizedPnl' in position_data:
            self.pnl = float(position_data['unrealizedPnl'])
        self.last_updated = time.time()
```

## Data Access Patterns

### Data Retrieval
The system provides efficient methods for retrieving data:

```python
def get_latest_funding_rates(exchange, market, limit=10):
    """Get the most recent funding rates for a market."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT rate, timestamp FROM funding_rates
    WHERE exchange = ? AND market = ?
    ORDER BY timestamp DESC
    LIMIT ?
    ''', (exchange, market, limit))
    
    results = cursor.fetchall()
    conn.close()
    
    return [(rate, timestamp) for rate, timestamp in results]

def get_position_history(exchange, market, start_time, end_time):
    """Get position history for a specific market and time range."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT * FROM position_history
    WHERE exchange = ? AND market = ? AND opened_at >= ? AND closed_at <= ?
    ORDER BY opened_at ASC
    ''', (exchange, market, start_time, end_time))
    
    results = cursor.fetchall()
    conn.close()
    
    return results
```

### Data Synchronization
The system maintains consistency between in-memory and persistent storage:

```python
def sync_market_data(exchange, market):
    """Synchronize market data from in-memory cache to persistent storage."""
    # Get order book data from cache
    order_book = data_cache.get_order_book(exchange, market)
    
    # Get recent trades from cache
    recent_trades = data_cache.get_recent_trades(exchange, market)
    
    # Get funding rates from cache
    funding_rates = data_cache.get_funding_rates(exchange, market)
    
    # Persist to SQLite
    for rate, timestamp in funding_rates:
        storage.save_funding_rate(exchange, market, rate, timestamp)
    
    # Update last sync timestamp
    return True
```

## Exchange-Specific Storage Methods

### HyperLiquid Methods

```python
def store_hyperliquid_funding_info(asset, funding_rate, funding_time):
    """Store HyperLiquid funding information."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO funding_rates (exchange, market, rate, timestamp)
    VALUES (?, ?, ?, ?)
    ''', ('hyperliquid', asset, funding_rate, funding_time))
    
    conn.commit()
    conn.close()

def store_hyperliquid_hyperps_data(asset, hyperps_data):
    """Store HyperLiquid hyperps data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS hyperliquid_hyperps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        asset TEXT,
        time INTEGER,
        avg_price REAL,
        agg_pos REAL,
        funding_rate REAL
    )
    ''')
    
    cursor.execute('''
    INSERT INTO hyperliquid_hyperps (asset, time, avg_price, agg_pos, funding_rate)
    VALUES (?, ?, ?, ?, ?)
    ''', (
        asset,
        int(time.time()),
        hyperps_data.get('avgPrice', 0),
        hyperps_data.get('aggPos', 0),
        hyperps_data.get('fundingRate', 0)
    ))
    
    conn.commit()
    conn.close()
```

### Backpack Methods

```python
def store_backpack_order(order_data):
    """Store Backpack order data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS backpack_orders (
        order_id TEXT PRIMARY KEY,
        market TEXT,
        side TEXT,
        price REAL,
        size REAL,
        status TEXT,
        created_at INTEGER
    )
    ''')
    
    cursor.execute('''
    INSERT OR REPLACE INTO backpack_orders
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        order_data.get('orderId', ''),
        order_data.get('market', ''),
        order_data.get('side', ''),
        float(order_data.get('price', 0)),
        float(order_data.get('size', 0)),
        order_data.get('status', ''),
        int(time.time())
    ))
    
    conn.commit()
    conn.close()

def store_backpack_market_metadata(market_metadata):
    """Store Backpack market metadata."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS backpack_markets (
        market TEXT PRIMARY KEY,
        base_currency TEXT,
        quote_currency TEXT,
        min_order_size REAL,
        price_increment REAL,
        size_increment REAL,
        updated_at INTEGER
    )
    ''')
    
    for market, data in market_metadata.items():
        cursor.execute('''
        INSERT OR REPLACE INTO backpack_markets
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            market,
            data.get('baseCurrency', ''),
            data.get('quoteCurrency', ''),
            float(data.get('minOrderSize', 0)),
            float(data.get('priceIncrement', 0)),
            float(data.get('sizeIncrement', 0)),
            int(time.time())
        ))
    
    conn.commit()
    conn.close()
```

## Data Schema Migration

As the system evolves, the storage schema needs to adapt. Migration scripts handle schema upgrades:

```python
def migrate_database(from_version, to_version):
    """Migrate database schema from one version to another."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check current version
    cursor.execute('PRAGMA user_version')
    current_version = cursor.fetchone()[0]
    
    if current_version != from_version:
        raise ValueError(f"Expected database version {from_version}, found {current_version}")
    
    # Apply migrations
    if from_version == 1 and to_version >= 2:
        # Add HyperLiquid specific tables
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS hyperliquid_hyperps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT,
            time INTEGER,
            avg_price REAL,
            agg_pos REAL,
            funding_rate REAL
        )
        ''')
    
    if from_version <= 2 and to_version >= 3:
        # Add Backpack specific tables
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS backpack_markets (
            market TEXT PRIMARY KEY,
            base_currency TEXT,
            quote_currency TEXT,
            min_order_size REAL,
            price_increment REAL,
            size_increment REAL,
            updated_at INTEGER
        )
        ''')
    
    # Update version
    cursor.execute(f'PRAGMA user_version = {to_version}')
    
    conn.commit()
    conn.close()
```

## Conclusion

The storage subsystem provides robust data management for the CyberDelta Engine, ensuring efficient access to both real-time and historical data across different exchanges. The design accommodates exchange-specific data structures while maintaining a consistent interface for the rest of the system. 
# Database Implementation Plan - Prototype 0.0.1

This document outlines the specific implementation details for integrating databases into the CyberDeltaEngine Prototype 0.0.1. It builds upon the general database strategy and provides concrete implementation plans, schemas, and code examples.

## Implementation Overview

For Prototype 0.0.1, we will follow a phased approach to database integration:

1. **Phase 0 (Initial)**: In-memory storage with file-based persistence
2. **Phase 1 (Core)**: Redis integration for state persistence and caching
3. **Phase 2 (Planning)**: Preparation for time-series database integration
4. **Phase 3 (Future)**: SQL database for structured logging (post-prototype)

## Phase 0: In-Memory with File-Based Persistence

For the initial implementation, we'll use in-memory storage with periodic serialization to files for basic persistence. This approach minimizes dependencies while establishing the foundation for more robust database integration later.

### Implementation Details

```python
# cyberdelta/utils/file_persistence.py

import json
import os
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

class FilePersistenceManager:
    """Simple file-based persistence manager for saving and loading state."""
    
    def __init__(self, data_dir: str = "./data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True, parents=True)
        self.checkpoint_dir = self.data_dir / "checkpoints"
        self.checkpoint_dir.mkdir(exist_ok=True)
        
    async def save_state(self, name: str, data: Dict[str, Any]) -> bool:
        """Save state to a JSON file.
        
        Args:
            name: Identifier for the state (e.g., 'portfolio', 'orders')
            data: Dictionary containing the state to save
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        try:
            # Create a timestamp for the filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{name}_{timestamp}.json"
            filepath = self.checkpoint_dir / filename
            
            # Write data to file
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
                
            # Create/update a symlink to the latest file
            latest_link = self.data_dir / f"{name}_latest.json"
            if latest_link.exists():
                latest_link.unlink()
            os.symlink(filepath, latest_link)
            
            return True
        except Exception as e:
            print(f"Error saving state: {e}")
            return False
            
    async def load_latest_state(self, name: str) -> Optional[Dict[str, Any]]:
        """Load the latest state for a given name.
        
        Args:
            name: Identifier for the state to load
            
        Returns:
            Dict or None: The loaded state or None if not found
        """
        try:
            latest_file = self.data_dir / f"{name}_latest.json"
            if not latest_file.exists():
                return None
                
            with open(latest_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading state: {e}")
            return None
            
    async def start_periodic_save(self, name: str, data_getter, interval_seconds: int = 300):
        """Start periodic saving of state.
        
        Args:
            name: Identifier for the state
            data_getter: Callable that returns the current state
            interval_seconds: How often to save the state (default: 5min)
        """
        while True:
            await asyncio.sleep(interval_seconds)
            data = data_getter()
            await self.save_state(name, data)
```

### Integration with Portfolio Tracker

```python
# cyberdelta/core/portfolio_tracker.py

from cyberdelta.utils.file_persistence import FilePersistenceManager

class PortfolioTracker:
    def __init__(self, api_clients, persistence_manager=None):
        self.api_clients = api_clients
        self.positions = {}  # exchange -> symbol -> Position
        self.balances = {}   # exchange -> asset -> Balance
        self.orders = {}     # exchange -> order_id -> Order
        
        # Initialize persistence manager if provided
        self.persistence_manager = persistence_manager
        
    async def initialize(self):
        """Initialize the portfolio tracker with latest state."""
        if self.persistence_manager:
            # Try to load state from files
            portfolio_state = await self.persistence_manager.load_latest_state("portfolio")
            if portfolio_state:
                self._restore_from_state(portfolio_state)
            
            # Start periodic saving
            asyncio.create_task(
                self.persistence_manager.start_periodic_save(
                    "portfolio", 
                    self.get_serializable_state,
                    interval_seconds=300  # 5 minutes
                )
            )
        
        # Always load current state from APIs as the authoritative source
        await self.load_initial_state()
        
    def _restore_from_state(self, state):
        """Restore tracker state from saved data."""
        if "positions" in state:
            self.positions = {
                exchange: {
                    symbol: Position(**pos_data)
                    for symbol, pos_data in positions.items()
                }
                for exchange, positions in state["positions"].items()
            }
            
        if "balances" in state:
            self.balances = {
                exchange: {
                    asset: Balance(**bal_data)
                    for asset, bal_data in balances.items()
                }
                for exchange, balances in state["balances"].items()
            }
            
        if "orders" in state:
            self.orders = {
                exchange: {
                    order_id: Order(**order_data)
                    for order_id, order_data in orders.items()
                }
                for exchange, orders in state["orders"].items()
            }
    
    def get_serializable_state(self):
        """Get a serializable representation of the current state."""
        return {
            "positions": {
                exchange: {
                    symbol: pos.to_dict()
                    for symbol, pos in positions.items()
                }
                for exchange, positions in self.positions.items()
            },
            "balances": {
                exchange: {
                    asset: bal.to_dict()
                    for asset, bal in balances.items()
                }
                for exchange, balances in self.balances.items()
            },
            "orders": {
                exchange: {
                    order_id: order.to_dict()
                    for order_id, order in orders.items()
                }
                for exchange, orders in self.orders.items()
            },
            "timestamp": datetime.now().isoformat()
        }
```

### Integration with Main Application

```python
# cyberdelta/main.py

from cyberdelta.utils.file_persistence import FilePersistenceManager

async def main():
    # Initialize file persistence
    persistence_manager = FilePersistenceManager(data_dir="./data")
    
    # Initialize components with persistence
    portfolio_tracker = PortfolioTracker(api_clients, persistence_manager)
    
    # Other initialization...
    
    # Start the components
    await portfolio_tracker.initialize()
    # ...
```

## Phase 1: Redis Integration

For more robust state persistence and potential performance improvements, we'll integrate Redis as a follow-on to Phase 0. The Redis implementation will provide:

1. Fast state persistence and recovery
2. Performance optimization through caching
3. Inter-component communication via Pub/Sub
4. Potential backend API integration points

### Implementation Details

```python
# cyberdelta/utils/redis_manager.py

import json
import redis.asyncio as redis
from typing import Dict, Any, Optional, List

class RedisManager:
    """Redis integration for state persistence and caching."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        """Initialize Redis manager.
        
        Args:
            redis_url: Redis connection string
        """
        self.redis_url = redis_url
        self.redis_client = None
        
    async def connect(self):
        """Connect to Redis server."""
        self.redis_client = await redis.from_url(self.redis_url)
        pong = await self.redis_client.ping()
        return pong
        
    async def disconnect(self):
        """Disconnect from Redis server."""
        if self.redis_client:
            await self.redis_client.close()
            
    async def save_state(self, key: str, data: Dict[str, Any]) -> bool:
        """Save state to Redis.
        
        Args:
            key: Redis key
            data: Dictionary to save
            
        Returns:
            bool: True if saved successfully
        """
        if not self.redis_client:
            return False
            
        try:
            serialized = json.dumps(data)
            await self.redis_client.set(key, serialized)
            return True
        except Exception as e:
            print(f"Error saving to Redis: {e}")
            return False
            
    async def load_state(self, key: str) -> Optional[Dict[str, Any]]:
        """Load state from Redis.
        
        Args:
            key: Redis key
            
        Returns:
            Dict or None: Loaded data or None if not found
        """
        if not self.redis_client:
            return None
            
        try:
            data = await self.redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            print(f"Error loading from Redis: {e}")
            return None
            
    async def publish_message(self, channel: str, message: Dict[str, Any]) -> int:
        """Publish message to Redis channel.
        
        Args:
            channel: Redis channel name
            message: Message to publish
            
        Returns:
            int: Number of clients that received the message
        """
        if not self.redis_client:
            return 0
            
        try:
            serialized = json.dumps(message)
            return await self.redis_client.publish(channel, serialized)
        except Exception as e:
            print(f"Error publishing to Redis: {e}")
            return 0
            
    async def subscribe(self, channels: List[str], callback):
        """Subscribe to Redis channels.
        
        Args:
            channels: List of channel names
            callback: Async function to call with received messages
        """
        if not self.redis_client:
            return
            
        try:
            pubsub = self.redis_client.pubsub()
            await pubsub.subscribe(*channels)
            
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True)
                if message:
                    try:
                        channel = message['channel'].decode('utf-8')
                        data = json.loads(message['data'].decode('utf-8'))
                        await callback(channel, data)
                    except json.JSONDecodeError:
                        print(f"Invalid JSON in Redis message: {message['data']}")
        except Exception as e:
            print(f"Error in Redis subscription: {e}")
```

### Redis-Enhanced Portfolio Tracker

```python
# cyberdelta/core/portfolio_tracker.py

from cyberdelta.utils.redis_manager import RedisManager

class PortfolioTracker:
    def __init__(self, api_clients, redis_manager=None):
        self.api_clients = api_clients
        self.positions = {}
        self.balances = {}
        self.orders = {}
        
        # Redis integration
        self.redis_manager = redis_manager
        
    async def initialize(self):
        """Initialize portfolio tracker with Redis support."""
        if self.redis_manager:
            # Try to load state from Redis
            portfolio_state = await self.redis_manager.load_state("portfolio:state")
            if portfolio_state:
                self._restore_from_state(portfolio_state)
                
            # Subscribe to updates from other components
            asyncio.create_task(
                self.redis_manager.subscribe(
                    ["portfolio:updates", "orders:updates"],
                    self._handle_redis_message
                )
            )
            
            # Schedule periodic state saves to Redis
            self._start_periodic_save()
            
        # Always load current state from APIs as the source of truth
        await self.load_initial_state()
        
    async def _handle_redis_message(self, channel, data):
        """Process messages from Redis subscription."""
        if channel == "portfolio:updates":
            # Handle updates from other components/services
            pass
        elif channel == "orders:updates":
            # Handle order updates
            if "exchange" in data and "order_id" in data:
                exchange = data["exchange"]
                order_id = data["order_id"]
                # Update local state based on message
                # ...
                
    def _start_periodic_save(self):
        """Start periodic state saving to Redis."""
        async def save_loop():
            while True:
                await asyncio.sleep(60)  # Save every minute
                state = self.get_serializable_state()
                await self.redis_manager.save_state("portfolio:state", state)
                
        asyncio.create_task(save_loop())
        
    async def update_position(self, exchange, symbol, position):
        """Update position and publish the change."""
        # Standard position update logic
        if exchange not in self.positions:
            self.positions[exchange] = {}
        self.positions[exchange][symbol] = position
        
        # Publish update to Redis if available
        if self.redis_manager:
            await self.redis_manager.publish_message(
                "portfolio:updates",
                {
                    "type": "position_update",
                    "exchange": exchange,
                    "symbol": symbol,
                    "position": position.to_dict()
                }
            )
```

### Performance Enhancement: Caching Market Data

```python
# cyberdelta/core/data_handler.py

from cyberdelta.utils.redis_manager import RedisManager

class DataHandler:
    def __init__(self, api_clients, redis_manager=None):
        self.api_clients = api_clients
        self.tickers = {}
        self.orderbooks = {}
        self.funding_rates = {}
        
        # Redis for caching
        self.redis_manager = redis_manager
        
    async def get_funding_rate(self, exchange, symbol):
        """Get funding rate with Redis caching."""
        # Try to get from in-memory cache first
        if exchange in self.funding_rates and symbol in self.funding_rates[exchange]:
            return self.funding_rates[exchange][symbol]
            
        # Try Redis cache if available
        if self.redis_manager:
            cache_key = f"funding_rate:{exchange}:{symbol}"
            cached_data = await self.redis_manager.load_state(cache_key)
            if cached_data and (time.time() - cached_data["timestamp"]) < 300:  # 5min TTL
                # Store in local cache and return
                if exchange not in self.funding_rates:
                    self.funding_rates[exchange] = {}
                self.funding_rates[exchange][symbol] = cached_data["rate"]
                return cached_data["rate"]
                
        # Fetch from API if no valid cache
        rate = await self.api_clients[exchange].fetch_funding_rate(symbol)
        
        # Update caches
        if exchange not in self.funding_rates:
            self.funding_rates[exchange] = {}
        self.funding_rates[exchange][symbol] = rate
        
        # Store in Redis if available
        if self.redis_manager:
            await self.redis_manager.save_state(
                f"funding_rate:{exchange}:{symbol}",
                {"rate": rate, "timestamp": time.time()}
            )
            
        return rate
```

## Phase 2: Time-Series Database Integration (Planning)

For Prototype 0.0.1, we will primarily focus on planning the time-series database integration. The following outlines the schema design and initial implementation that will be developed further in subsequent versions.

### Schema Design for InfluxDB

InfluxDB organizes data into measurements, tags (indexed), and fields (not indexed):

1. **Funding Rates Measurement**
   - Tags: exchange, symbol, market_type
   - Fields: rate, predicted_rate, next_payment_timestamp
   - Retention: 1 year

2. **Order Book Snapshots Measurement**
   - Tags: exchange, symbol, depth
   - Fields: bids_json, asks_json, spread, midprice
   - Retention: 7 days

3. **Trades Measurement**
   - Tags: exchange, symbol, side
   - Fields: price, size, liquidation
   - Retention: 30 days

4. **Portfolio State Measurement**
   - Tags: exchange, asset_type
   - Fields: total_value, margin_used, free_collateral
   - Retention: 1 year

5. **Bot Performance Measurement**
   - Tags: strategy, timeframe
   - Fields: pnl, sharpe, trades_count, win_rate
   - Retention: Infinite

### Sample Code for Future Implementation

```python
# FUTURE IMPLEMENTATION - For planning purposes only
# cyberdelta/utils/tsdb_manager.py

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from datetime import datetime

class TSDBManager:
    """Time-Series Database Manager for historical data."""
    
    def __init__(self, url, token, org, bucket="cyberdelta"):
        """Initialize TSDB connection."""
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self.bucket = bucket
        
    async def store_funding_rate(self, exchange, symbol, rate, timestamp=None):
        """Store funding rate data point."""
        if timestamp is None:
            timestamp = datetime.utcnow()
            
        point = Point("funding_rates") \
            .tag("exchange", exchange) \
            .tag("symbol", symbol) \
            .field("rate", float(rate)) \
            .time(timestamp, WritePrecision.NS)
            
        self.write_api.write(bucket=self.bucket, record=point)
        
    async def store_orderbook_snapshot(self, exchange, symbol, bids, asks, timestamp=None):
        """Store order book snapshot."""
        if timestamp is None:
            timestamp = datetime.utcnow()
            
        # Calculate derived values
        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None
        spread = best_ask - best_bid if (best_bid and best_ask) else None
        midprice = (best_bid + best_ask) / 2 if (best_bid and best_ask) else None
        
        # Store just top levels to save space
        top_bids = bids[:5]
        top_asks = asks[:5]
        
        point = Point("orderbook_snapshots") \
            .tag("exchange", exchange) \
            .tag("symbol", symbol) \
            .tag("depth", "5") \
            .field("bids_json", json.dumps(top_bids)) \
            .field("asks_json", json.dumps(top_asks)) \
            .field("spread", spread) \
            .field("midprice", midprice) \
            .time(timestamp, WritePrecision.NS)
            
        self.write_api.write(bucket=self.bucket, record=point)
        
    async def store_portfolio_state(self, portfolio_state, timestamp=None):
        """Store portfolio state snapshot."""
        if timestamp is None:
            timestamp = datetime.utcnow()
            
        # Create points for overall portfolio value
        total_value = portfolio_state.get("total_value", 0)
        
        portfolio_point = Point("portfolio_state") \
            .tag("metric", "total_value") \
            .field("value", float(total_value)) \
            .time(timestamp, WritePrecision.NS)
            
        self.write_api.write(bucket=self.bucket, record=portfolio_point)
        
        # Create points for each position
        for exchange, positions in portfolio_state.get("positions", {}).items():
            for symbol, position in positions.items():
                position_point = Point("positions") \
                    .tag("exchange", exchange) \
                    .tag("symbol", symbol) \
                    .field("size", float(position.get("size", 0))) \
                    .field("entry_price", float(position.get("entry_price", 0))) \
                    .field("unrealized_pnl", float(position.get("unrealized_pnl", 0))) \
                    .time(timestamp, WritePrecision.NS)
                    
                self.write_api.write(bucket=self.bucket, record=position_point)
                
    async def query_historical_funding_rates(self, exchange, symbol, start_time, end_time):
        """Query historical funding rates."""
        query = f'''
        from(bucket: "{self.bucket}")
            |> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()})
            |> filter(fn: (r) => r._measurement == "funding_rates")
            |> filter(fn: (r) => r.exchange == "{exchange}")
            |> filter(fn: (r) => r.symbol == "{symbol}")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        
        result = self.query_api.query(org=self.org, query=query)
        
        return [
            {
                "timestamp": record.get_time(),
                "rate": record.get_value("rate")
            }
            for table in result
            for record in table.records
        ]
```

## Deployment & Operations Plan

### Local Development Environment

For local development, we'll use Docker Compose to set up Redis and InfluxDB instances:

```yaml
# docker-compose.yml
version: '3'
services:
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes

  influxdb:
    image: influxdb:2.7
    ports:
      - "8086:8086"
    volumes:
      - influxdb-data:/var/lib/influxdb2
    environment:
      - DOCKER_INFLUXDB_INIT_MODE=setup
      - DOCKER_INFLUXDB_INIT_USERNAME=admin
      - DOCKER_INFLUXDB_INIT_PASSWORD=password123
      - DOCKER_INFLUXDB_INIT_ORG=cyberdelta
      - DOCKER_INFLUXDB_INIT_BUCKET=market_data
      - DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=my-super-secret-token

volumes:
  redis-data:
  influxdb-data:
```

### Connection Configuration

Database connection details will be stored in the configuration file:

```yaml
# config.yaml additions
databases:
  file_persistence:
    enabled: true
    data_dir: "./data"
    
  redis:
    enabled: false  # Default to disabled for Prototype 0.0.1
    url: "redis://localhost:6379/0"
    
  influxdb:
    enabled: false  # Default to disabled for Prototype 0.0.1
    url: "http://localhost:8086"
    token: "my-super-secret-token"
    org: "cyberdelta"
    bucket: "market_data"
```

## Implementation Timeline for Prototype 0.0.1

1. **Week 1**: Implement File-based Persistence
   - Create `FilePersistenceManager` class
   - Integrate with `PortfolioTracker` and `DataHandler`
   - Add serialization methods to data models

2. **Week 2-3**: Implement Redis Manager (Optional)
   - Create `RedisManager` class
   - Setup Docker Compose for local testing
   - Integrate with `PortfolioTracker` for state persistence
   - Implement caching in `DataHandler`

3. **Week 4-5**: Plan Time-Series Database Integration
   - Design schema for historical data
   - Create initial TSDB manager interface
   - Document integration points for future implementation

## Conclusion

The database implementation plan for Prototype 0.0.1 focuses on building a solid foundation while minimizing initial dependencies. We'll start with simple file-based persistence, add Redis integration for improved state management, and plan for future time-series database integration.

This approach allows us to focus on core trading functionality while establishing the infrastructure needed for robust data management in future iterations. The phased approach also provides flexibility to adjust based on performance requirements and operational needs as the system evolves. 
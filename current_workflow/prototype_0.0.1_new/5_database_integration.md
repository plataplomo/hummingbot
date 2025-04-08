# Database Integration Strategy - Prototype 0.0.1

This document outlines the database integration strategy for CyberDeltaEngine, detailing the requirements, technology options, implementation approach, and timeline for incorporating database functionality into the system.

## Integration Goals

1. **State Persistence**: Maintain system state across restarts to enable recovery from failures without data loss
2. **Configuration Management**: Centralize configuration storage and enable dynamic updates without code changes
3. **Historical Data Storage**: Store market data, positions, and performance metrics for analysis and strategy refinement
4. **Logging and Auditing**: Record system activities, errors, and trading decisions for compliance and debugging
5. **Caching**: Improve performance by caching frequently accessed data

## Technology Options

### In-Memory Database: Redis

**Use Cases:**
- State persistence
- Configuration management
- Caching
- Pub/sub for component communication

**Advantages:**
- Exceptional performance (100K+ operations/second)
- Built-in data structures (hashes, lists, sets)
- Pub/sub messaging system
- Simple integration
- Persistence options (RDB snapshots, AOF logs)

**Considerations:**
- Limited complex query capabilities
- Data size limited by available memory
- Additional infrastructure requirements

### Time-Series Database: InfluxDB/TimescaleDB

**Use Cases:**
- Market data storage
- Performance metrics
- Position history

**Advantages:**
- Optimized for time-series data
- Efficient data compression
- Fast time-range queries
- Built-in data retention policies
- Downsampling capabilities

**Considerations:**
- Specialized use case
- Learning curve for query language
- Resource intensive for high-frequency data

### SQL Database: PostgreSQL

**Use Cases:**
- Configuration management
- Detailed logging
- Financial records
- Account management

**Advantages:**
- ACID compliance
- Rich query capabilities
- Extensive tooling ecosystem
- JSON support for flexible schemas
- Strong reliability

**Considerations:**
- Performance overhead for high-frequency operations
- Requires schema design and management
- More complex to set up and maintain

### Document Database: MongoDB

**Use Cases:**
- Configuration management
- Order/trade history
- System state snapshots

**Advantages:**
- Schema flexibility
- JSON-based document storage
- Good performance for read-heavy workloads
- Simpler setup than relational databases

**Considerations:**
- Less suitable for complex relationships
- Transaction support limitations
- Consistency trade-offs

## Phased Integration Approach

### Phase 0: File-Based Storage (Current State)
- Use simple file-based storage for configuration
- Store logs in text files
- No persistent state between restarts
- No historical data retention

### Phase 1: Redis Integration
- **Timeline**: Week 2-3
- **Components**:
  - State Manager: For persisting critical state
  - Configuration Manager: For storing and updating configuration
  - Pub/Sub System: For component communication

```python
# Example Redis integration for state persistence
import redis
import json

class StateManager:
    def __init__(self, redis_url="redis://localhost:6379/0"):
        self.redis = redis.Redis.from_url(redis_url)
        
    async def save_portfolio_state(self, portfolio_state):
        """Save current portfolio state to Redis."""
        state_json = json.dumps(portfolio_state)
        self.redis.set("portfolio_state", state_json)
        
    async def load_portfolio_state(self):
        """Load portfolio state from Redis."""
        state_json = self.redis.get("portfolio_state")
        if state_json:
            return json.loads(state_json)
        return None
        
    async def save_active_orders(self, active_orders):
        """Save active orders to Redis."""
        orders_json = json.dumps(active_orders)
        self.redis.set("active_orders", orders_json)
        
    async def load_active_orders(self):
        """Load active orders from Redis."""
        orders_json = self.redis.get("active_orders")
        if orders_json:
            return json.loads(orders_json)
        return []
```

### Phase 2: Time-Series Database Integration
- **Timeline**: Week 4-5
- **Components**:
  - Historical Data Manager: For storing and retrieving market data
  - Performance Tracker: For recording system performance metrics
  - Analysis Tools: For analyzing historical data patterns

```python
# Example InfluxDB integration for market data storage
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import time

class MarketDataStore:
    def __init__(self, url="http://localhost:8086", token="my-token", org="my-org", bucket="market_data"):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self.bucket = bucket
        
    async def store_funding_rate(self, symbol, exchange, rate, timestamp=None):
        """Store funding rate data point."""
        if timestamp is None:
            timestamp = int(time.time() * 1_000_000_000)  # nanoseconds
            
        point = Point("funding_rate") \
            .tag("symbol", symbol) \
            .tag("exchange", exchange) \
            .field("rate", rate) \
            .time(timestamp)
            
        self.write_api.write(bucket=self.bucket, record=point)
        
    async def get_historical_funding_rates(self, symbol, exchange, start_time, end_time):
        """Query historical funding rates for a symbol."""
        query = f'''
        from(bucket: "{self.bucket}")
            |> range(start: {start_time}, stop: {end_time})
            |> filter(fn: (r) => r._measurement == "funding_rate")
            |> filter(fn: (r) => r.symbol == "{symbol}")
            |> filter(fn: (r) => r.exchange == "{exchange}")
        '''
        
        result = self.query_api.query(query=query)
        return [
            {"time": record.get_time(), "rate": record.get_value()} 
            for table in result 
            for record in table.records
        ]
```

### Phase 3: SQL Database for Logging and Auditing
- **Timeline**: Future Phase (Post-Prototype)
- **Components**:
  - Structured Logger: For detailed system activity logging
  - Audit Trail: For trading decisions and regulatory compliance
  - Configuration Repository: For comprehensive configuration management

## Implementation Priorities

For Prototype 0.0.1, the database integration will focus on:

1. **Redis Integration** for:
   - Critical state persistence (portfolio state, active orders)
   - Basic configuration storage
   - Component communication via pub/sub

2. **Initial Time-Series Database Planning** for:
   - Defining schema and retention policies
   - Creating data access layer interfaces
   - Setting up basic market data storage

## Integration Points

The database integration will connect with the following system components:

1. **Portfolio Tracker**: State persistence via Redis
2. **Configuration Manager**: Configuration storage in Redis
3. **Data Handler**: Historical data storage in Time-Series DB
4. **Execution Handler**: Order state persistence in Redis
5. **Main Application**: System state management and recovery

## Recovery Mechanisms

The database integration will support the following recovery scenarios:

1. **Restart Recovery**: Restore active positions and orders after system restart
2. **Crash Recovery**: Recover from unexpected termination by rebuilding state
3. **Connection Loss**: Handle temporary database connection failures gracefully
4. **Data Inconsistency**: Detect and resolve inconsistencies between in-memory and persisted state

## Deployment Considerations

### Local Development
- Use Docker Compose for local database instances
- Include setup scripts in the repository
- Document connection parameters and configuration

### Production
- Use managed database services when possible
- Implement connection pooling and retries
- Configure proper backup and replication
- Implement monitoring for database health

## Testing Strategy

1. **Unit Tests**: Test database access layer in isolation
2. **Integration Tests**: Verify database interactions with live instances
3. **Failure Tests**: Simulate database failures and verify recovery
4. **Performance Tests**: Measure throughput and latency under load

## Conclusion and Recommendations

For Prototype 0.0.1, we recommend starting with **Phase 0** (file-based storage) and implementing **Phase 1** (Redis) only if there are specific recovery requirements that cannot be met with simpler solutions.

The initial implementation should focus on the core trading functionality, with database integration as a secondary priority. This approach allows for faster development of the prototype while establishing the foundations for more robust data management in future iterations. 
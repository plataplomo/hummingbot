# Binance Rate Limiting Analysis for Funding Arbitrage Strategy

## Problem Summary

When running a funding arbitrage strategy with 24 tokens across 3 exchanges (Binance, Hyperliquid, Backpack), Binance hits rate limits during initialization, preventing the bot from starting properly.

### Error Messages
```
OSError: Error executing request GET https://fapi.binance.com/fapi/v1/income. HTTP status is 429.
Error: {"code":-1003,"msg":"Too many requests; current limit of IP(38.25.70.6) is 2400 requests per minute.
Please use the websocket for live updates to avoid polling the API."}

OSError: Error executing request GET https://fapi.binance.com/fapi/v1/depth. HTTP status is 429.
```

## Root Cause Analysis

### 1. API Weight Limits
- **Binance Rate Limit**: 2400 weight units per minute
- **Critical Endpoint**: `/fapi/v1/income` has weight of **30** (vs 1 for most endpoints)
- **Impact**: 24 tokens × 30 weight = **720 weight units** just for initialization (30% of limit instantly)

### 2. Framework Behavior
Located in `hummingbot/connector/perpetual_derivative_py_base.py`:

```python
async def _funding_payment_polling_loop(self):
    """
    Periodically calls _update_funding_payment(), responsible for handling all funding payments.
    """
    await self._update_all_funding_payments(fire_event_on_new=False)  # initialization of the timestamps
```

During startup:
1. `_update_all_funding_payments()` is called for initialization
2. This calls `_fetch_last_fee_payment()` for EACH trading pair
3. Each call makes 2 API requests:
   - `/fapi/v1/income` (weight 30) - to get funding payment history
   - `/fapi/v1/premiumIndex` (weight 1) - to get current funding rate

### 3. Configuration Impact
With the test configuration:
- **3 exchanges**: Backpack, Binance, Hyperliquid
- **24 tokens**: SOL, BTC, ETH, HYPE, SEI, SUI, IP, ENA, DOGE, XRP, LINK, FARTCOIN, AAVE, UNI, WLFI, PUMP, APT, PENGU, BNB, JUP, TRUMP, ADA, WIF, KAITO, TON
- **Result**: 72 total trading pairs (24 tokens × 3 exchanges)
- **Binance alone**: 24 trading pairs = 48+ API calls on startup

### 4. Why Websockets Don't Help
Binance provides websocket streams for:
- Order book updates (`@depth`)
- Trade updates (`@aggTrade`)
- Funding rate updates (`@markPrice`)

However, the `_fetch_last_fee_payment()` method needs **historical funding payment data** from `/fapi/v1/income`, which is NOT available via websocket.

## Optimization Strategies

### Without Code Changes (Configuration Only)

#### 1. **Reduce Binance Token Count**
```yaml
# Limit Binance to major tokens only (8-10 tokens)
binance_tokens:
- BTC
- ETH
- SOL
- BNB
- XRP
- DOGE
- ADA
- LINK
```
- Keep all 24 tokens on Hyperliquid and Backpack
- Reduces Binance initialization to 240 weight units (10% of limit)

#### 2. **Increase Refresh Intervals**
```yaml
opportunity_refresh_interval: 300  # 5 minutes instead of 2
reconciliation_interval: 30  # 30 seconds instead of 5
```

#### 3. **Multiple API Keys Strategy**
- Use 3 different Binance accounts/API keys
- Run 3 bot instances, each with 8 tokens
- Each gets separate rate limit (2400 per key)

#### 4. **Time-Based Rotation**
Create multiple configs that rotate:
- Morning: First 12 tokens
- Evening: Second 12 tokens
- Rotate every few hours to cover all tokens

### With Code Changes (Framework Modifications Needed)

#### 1. **Staggered Initialization**
```python
async def _update_all_funding_payments(self, fire_event_on_new: bool):
    batch_size = 5  # Process 5 pairs at a time
    delay_between_batches = 15.0  # 15 seconds between batches

    for i in range(0, len(trading_pairs), batch_size):
        batch = trading_pairs[i:i + batch_size]
        # Process batch
        await safe_gather(*batch_tasks)
        if i + batch_size < len(trading_pairs):
            await asyncio.sleep(delay_between_batches)
```

#### 2. **Batch API Calls**
Instead of calling `/fapi/v1/income` for each symbol:
```python
# Single call without symbol filter
response = await self._api_get(
    path_url=CONSTANTS.GET_INCOME_HISTORY_URL,
    params={"incomeType": "FUNDING_FEE"},  # No symbol filter
)
# Then filter locally for each symbol
```

#### 3. **Skip Historical on Startup**
```python
async def _funding_payment_polling_loop(self):
    # Skip initialization call
    # await self._update_all_funding_payments(fire_event_on_new=False)

    # Start with empty history, build it over time
    while True:
        await self._funding_fee_poll_notifier.wait()
        await self._update_all_funding_payments(fire_event_on_new=True)
```

#### 4. **Cache Funding Payment History**
- Store funding payment history to disk
- On restart, load from cache instead of API
- Only fetch new payments since last cached timestamp

## Practical Recommendations

### For 24 Tokens Without Framework Changes:

1. **Best Option**: Use multiple Binance API keys (3 accounts × 8 tokens each)

2. **Second Option**: Create rotating configs:
   - `config_batch1.yml`: Tokens 1-8 on all exchanges
   - `config_batch2.yml`: Tokens 9-16 on all exchanges
   - `config_batch3.yml`: Tokens 17-24 on all exchanges
   - Run different configs at different times

3. **Third Option**: Skip Binance temporarily when rate limited:
   - Have a `config_no_binance.yml` ready
   - Use only Hyperliquid + Backpack (they handle 24 tokens fine)
   - Switch back to Binance after rate limit resets

4. **Configuration Optimizations**:
   ```yaml
   opportunity_refresh_interval: 300  # 5 minutes
   reconciliation_interval: 30  # 30 seconds
   enable_reconciliation: false  # Temporarily if needed
   ```

### Long-term Solution
The framework needs modification to handle large token sets better:
- Implement proper request batching
- Add configurable delays between market initializations
- Cache historical data to avoid re-fetching
- Use more efficient API endpoints where possible

## Testing Recommendations

1. Start with smaller token sets to verify functionality
2. Gradually increase token count while monitoring rate limits
3. Use Binance Testnet for development (different rate limits)
4. Monitor the weight consumption in response headers

## Key Files for Reference

- **Connector Base**: `hummingbot/connector/perpetual_derivative_py_base.py`
- **Binance Connector**: `hummingbot/connector/derivative/binance_perpetual/binance_perpetual_derivative.py`
- **Binance Constants**: `hummingbot/connector/derivative/binance_perpetual/binance_perpetual_constants.py`
- **Controller**: `controllers/arbitrage/funding_arbitrage_controller.py`
- **Strategy**: `scripts/v2_funding_arbitrage_with_controller.py`

## Rate Limit Details

From `binance_perpetual_constants.py`:
```python
RATE_LIMITS = [
    # Pool Limits
    RateLimit(limit_id=REQUEST_WEIGHT, limit=2400, time_interval=ONE_MINUTE),

    # Heavy endpoints
    RateLimit(limit_id=GET_INCOME_HISTORY_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=30)]),  # ← The culprit!

    # Light endpoints
    RateLimit(limit_id=MARK_PRICE_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE, weight=1,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
]
```

## Optimal Solution: Data Collection Bot + Database Architecture

### Overview
A separate data collection service with database caching is the best long-term solution. This architecture completely isolates rate limit concerns from the trading bot.

### 1. **Separate Data Collector Service**
```python
# funding_data_collector.py
# Runs independently, 24/7
# Slowly and carefully collects funding data without hitting rate limits
# Stores everything in a database
```

**Benefits:**
- Runs at its own pace, respecting rate limits
- Can spread API calls over time (e.g., update each token every 5 minutes)
- Builds historical database over time
- Handles reconnections and failures gracefully

### 2. **Database Schema**
```sql
-- Funding rates table
CREATE TABLE funding_rates (
    id SERIAL PRIMARY KEY,
    exchange VARCHAR(50),
    symbol VARCHAR(20),
    funding_rate DECIMAL(18,8),
    mark_price DECIMAL(18,8),
    index_price DECIMAL(18,8),
    next_funding_time TIMESTAMP,
    timestamp TIMESTAMP,
    UNIQUE(exchange, symbol, timestamp)
);

-- Funding payments table
CREATE TABLE funding_payments (
    id SERIAL PRIMARY KEY,
    exchange VARCHAR(50),
    symbol VARCHAR(20),
    payment_amount DECIMAL(18,8),
    funding_rate DECIMAL(18,8),
    position_size DECIMAL(18,8),
    payment_time TIMESTAMP,
    fetched_at TIMESTAMP,
    UNIQUE(exchange, symbol, payment_time)
);

-- Latest state cache
CREATE TABLE latest_funding_state (
    exchange VARCHAR(50),
    symbol VARCHAR(20),
    last_payment_time TIMESTAMP,
    last_payment_amount DECIMAL(18,8),
    current_funding_rate DECIMAL(18,8),
    updated_at TIMESTAMP,
    PRIMARY KEY(exchange, symbol)
);
```

### 3. **Trading Bot Integration**
The trading bot would:
1. Connect to database instead of calling APIs directly
2. Read latest funding data from `latest_funding_state` table
3. Subscribe to websockets for real-time updates only
4. Never call the heavy `/fapi/v1/income` endpoint

### 4. **Implementation Approach**

**Data Collector (runs separately):**
```python
class FundingDataCollector:
    def __init__(self, db_connection, exchanges):
        self.db = db_connection
        self.exchanges = exchanges
        self.tokens = load_tokens_from_config()

    async def collect_loop(self):
        while True:
            for exchange in self.exchanges:
                for token in self.tokens:
                    try:
                        # Fetch with proper rate limiting
                        funding_data = await self.fetch_funding_data_with_backoff(
                            exchange, token
                        )
                        await self.store_in_db(funding_data)
                    except RateLimitError:
                        await asyncio.sleep(60)  # Back off

                    # Space out requests
                    await asyncio.sleep(5)  # 5 seconds between each token

            # Full cycle every 30 minutes
            await asyncio.sleep(1800)
```

**Trading Bot (modified to use DB):**
```python
async def _fetch_last_fee_payment(self, trading_pair: str):
    # Instead of API call, query database
    query = """
        SELECT last_payment_time, last_payment_amount, current_funding_rate
        FROM latest_funding_state
        WHERE exchange = %s AND symbol = %s
    """
    result = await self.db.fetch_one(query, [self.exchange_name, trading_pair])

    if result:
        return result.last_payment_time, result.current_funding_rate, result.last_payment_amount
    else:
        # Fallback or initialize
        return 0, Decimal("-1"), Decimal("-1")
```

### 5. **Advantages of This Architecture**

1. **Complete rate limit isolation** - Collector manages its own rate limits
2. **Historical data persistence** - Never lose funding history
3. **Fast bot startup** - Read from DB instead of 48+ API calls
4. **Multiple bot instances** - All can share the same data source
5. **Failure resilience** - If collector fails, bots continue with cached data
6. **Analytics capability** - Query historical funding patterns
7. **Backtesting support** - Historical data readily available

### 6. **Deployment Strategy**

```yaml
# docker-compose.yml
version: '3.8'
services:
  postgres:
    image: postgres:14
    volumes:
      - funding_data:/var/lib/postgresql/data
    environment:
      POSTGRES_DB: funding_arbitrage

  data_collector:
    build: ./data_collector
    depends_on:
      - postgres
    environment:
      DB_URL: postgresql://postgres@postgres/funding_arbitrage
      BINANCE_API_KEY: ${BINANCE_COLLECTOR_KEY}
      # Separate read-only API key for collection

  trading_bot:
    build: ./hummingbot
    depends_on:
      - postgres
    environment:
      DB_URL: postgresql://postgres@postgres/funding_arbitrage
      BINANCE_API_KEY: ${BINANCE_TRADING_KEY}
      # Different API key for trading
```

### 7. **Migration Path**

1. Start data collector first, let it build history
2. Once sufficient data collected (few hours), start trading bot
3. Trading bot uses DB for initialization, websockets for real-time
4. Gradually phase out direct API calls from trading bot

### 8. **Rate Limit Calculations for Collector**

With proper spacing:
- 24 tokens × 30 weight = 720 weight per full cycle
- Spread over 2 minutes (5 seconds per token) = 360 weight/minute
- Well under 2400 limit with room for other operations

### 9. **Additional Optimizations**

- Use read-only API keys for collector (may have different limits)
- Implement exponential backoff on rate limit errors
- Cache funding rate schedules (most are every 8 hours)
- Only fetch payment history when funding time passes
- Use multiple collector instances with different API keys for redundancy

## Conclusion

The funding arbitrage strategy requires historical funding payment data for proper operation, which creates a conflict with Binance's rate limits when using many tokens.

**Short-term solutions** (without framework modifications):
1. Use multiple API keys to distribute the load
2. Reduce the number of tokens on Binance specifically
3. Accept temporary rate limiting during startup and wait for recovery

**Optimal solution**: Implement a separate data collection service with database caching. This architecture completely solves the rate limit problem while providing additional benefits like historical data analysis, fast startup, and multi-instance support.

The framework ideally needs to be enhanced to handle this use case better through batching, caching, or staggered initialization, but the database architecture provides a robust solution that works with the existing framework.

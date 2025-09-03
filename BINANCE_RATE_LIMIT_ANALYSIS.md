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

## Conclusion

The funding arbitrage strategy requires historical funding payment data for proper operation, which creates a conflict with Binance's rate limits when using many tokens. Without framework modifications, the only viable solutions are:

1. Use multiple API keys to distribute the load
2. Reduce the number of tokens on Binance specifically
3. Accept temporary rate limiting during startup and wait for recovery

The framework ideally needs to be enhanced to handle this use case better through batching, caching, or staggered initialization.

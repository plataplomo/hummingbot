# Funding Rate Arbitrage Fix Proposal

## Executive Summary

This document provides a comprehensive implementation plan to fix the funding rate arbitrage system in CyberDeltaEngine. The primary issue is that Hyperliquid funding rates are not being fetched or cached, preventing the arbitrage strategy from identifying opportunities.

## Current State Analysis

### What's Working
- ✅ Backpack WebSocket funding subscriptions (`funding.{symbol}`)
- ✅ Hyperliquid REST API has `get_funding_rates()` method implemented
- ✅ Strategy logic for calculating arbitrage opportunities
- ✅ Data structures for storing funding rates in DataHandler

### What's Broken
- ❌ No Hyperliquid funding rate subscriptions in DataHandler
- ❌ No periodic REST API polling for Hyperliquid funding rates
- ❌ Strategy fails silently when funding data unavailable
- ❌ No retry logic or fallback mechanisms

## Technical Discovery

### Hyperliquid Funding Rate Architecture

1. **No WebSocket Funding Stream**: Hyperliquid doesn't provide a dedicated WebSocket stream for funding rates
2. **REST API Available**: Funding rates are available via `/info` endpoint with `metaAndAssetCtxs` request
3. **Data Structure**: `HyperliquidRawAssetCtx.funding` field contains hourly funding rate as decimal string
4. **Existing Implementation**: `HyperliquidAPI.get_funding_rates()` method already exists and works

### Implementation Plan

## Phase 1: Quick Fix (1-2 days)

### 1.1 Add Periodic Hyperliquid Funding Rate Polling

**File**: `cyberdelta/core/data_handler.py`

```python
class DataHandler:
    def __init__(self, ...):
        # Add new attributes
        self._funding_refresh_tasks: dict[str, asyncio.Task] = {}
        self._funding_refresh_intervals = {
            "hyperliquid": 300,  # 5 minutes
            "backpack": None,    # Uses WebSocket
        }

    async def start_connections(self):
        """Start WebSocket connections and periodic tasks."""
        # Existing WebSocket connection code...

        # NEW: Start periodic funding refresh for exchanges without WebSocket funding
        for exchange_id, interval in self._funding_refresh_intervals.items():
            if interval and exchange_id in self.api_clients:
                self.logger.info(
                    f"Starting periodic funding rate refresh for {exchange_id} "
                    f"with interval {interval}s"
                )
                task = asyncio.create_task(
                    self._periodic_funding_refresh(exchange_id, interval)
                )
                self._funding_refresh_tasks[exchange_id] = task

    async def _periodic_funding_refresh(self, exchange_id: str, interval: int):
        """Periodically refresh funding rates via REST API."""
        await asyncio.sleep(5)  # Initial delay to let WebSocket connections establish

        while self._running:
            try:
                start_time = asyncio.get_event_loop().time()

                # Get all tracked symbols for this exchange
                symbols = list(self.symbol_maps[exchange_id].keys())

                if not symbols:
                    logger.debug(f"No symbols to refresh funding for {exchange_id}")
                    await asyncio.sleep(interval)
                    continue

                # Fetch funding rates via REST API
                logger.debug(f"Fetching funding rates for {exchange_id}: {symbols}")
                rates = await self.api_clients[exchange_id].get_funding_rates(
                    GetFundingRatesArgs(symbols=symbols)
                )

                # Update cache with fresh data
                for rate in rates:
                    if rate.symbol in self.symbol_maps[exchange_id]:
                        self._update_funding_rate(
                            exchange_id,
                            rate.symbol,
                            rate,
                            datetime.now(UTC)
                        )
                        logger.debug(
                            f"Updated funding rate for {exchange_id}:{rate.symbol} = {rate.funding_rate}"
                        )

                # Log performance metrics
                elapsed = asyncio.get_event_loop().time() - start_time
                logger.info(
                    "funding_refresh_completed",
                    exchange_id=exchange_id,
                    symbols_updated=len(rates),
                    elapsed_ms=elapsed * 1000,
                    interval_seconds=interval
                )

            except Exception as e:
                logger.error(
                    "funding_refresh_failed",
                    exchange_id=exchange_id,
                    error=str(e),
                    action="will_retry_next_interval"
                )

            await asyncio.sleep(interval)

    async def stop(self):
        """Stop all connections and tasks."""
        self._running = False

        # Cancel funding refresh tasks
        for exchange_id, task in self._funding_refresh_tasks.items():
            if not task.done():
                logger.info(f"Cancelling funding refresh task for {exchange_id}")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Existing stop logic...
```

### 1.2 Fix get_latest_funding_rate to Not Return None for Stale Data

**File**: `cyberdelta/core/data_handler.py`

```python
def get_latest_funding_rate(self, exchange_id: str, symbol: str) -> FundingRate | None:
    """Get the latest funding rate for a symbol on an exchange.

    Returns the funding rate even if stale, with a staleness warning logged.
    """
    # Check if we have the symbol mapping
    if exchange_id not in self.symbol_maps or symbol not in self.symbol_maps[exchange_id]:
        logger.warning(
            f"Symbol {symbol} not found in symbol map for {exchange_id}. "
            f"Available: {list(self.symbol_maps.get(exchange_id, {}).keys())}"
        )
        return None

    # Get funding rate object
    funding_rate_obj = self.funding_rates.get(exchange_id, {}).get(symbol)
    if not funding_rate_obj:
        logger.warning(
            f"No funding rate data for {exchange_id} - {symbol}. "
            f"Available rates: {list(self.funding_rates.get(exchange_id, {}).keys())}"
        )
        return None

    # Check staleness but still return the data
    timestamp = self.last_update_time.get(exchange_id, {}).get(f"{symbol}_funding")
    if timestamp:
        staleness_threshold = self.staleness_thresholds.get(
            f"{exchange_id}_funding",
            self.default_staleness_threshold,
        )
        age = datetime.now(UTC) - timestamp

        if age > staleness_threshold:
            logger.warning(
                "funding_rate_stale_but_returning",
                exchange_id=exchange_id,
                symbol=symbol,
                last_update=timestamp.isoformat(),
                age_seconds=age.total_seconds(),
                staleness_threshold_seconds=staleness_threshold.total_seconds(),
                funding_rate=float(funding_rate_obj.funding_rate)
            )

    return funding_rate_obj
```

### 1.3 Add Retry Logic to Strategy

**File**: `cyberdelta/strategies/funding_rate_arbitrage.py`

```python
async def _check_opportunity(self) -> ArbitrageOpportunity | None:
    """Check for arbitrage opportunities with retry logic."""
    # Get funding rate with retries
    funding_rate = await self._get_funding_rate_with_retry(
        self.perp_exchange,
        self.symbol,
        max_retries=3
    )

    if funding_rate is None:
        self._consecutive_failures += 1

        # Log with context
        perp_ticker = self.data_handler.get_latest_ticker(self.perp_exchange, self.symbol)
        spot_symbol = self._get_spot_symbol(self.symbol)
        spot_ticker = self.data_handler.get_latest_ticker(self.spot_exchange, spot_symbol)

        logger.error(
            "funding_rate_unavailable_with_context",
            strategy=self.name,
            symbol=self.symbol,
            perp_exchange=self.perp_exchange,
            consecutive_failures=self._consecutive_failures,
            perp_price=float(perp_ticker.price) if perp_ticker and perp_ticker.price else None,
            spot_price=float(spot_ticker.price) if spot_ticker and spot_ticker.price else None,
            has_perp_connection=self.perp_exchange in self.data_handler.api_clients,
            action="skipping_opportunity_check"
        )

        # Trigger alert if too many consecutive failures
        if self._consecutive_failures >= 10:
            logger.critical(
                "funding_rate_critical_failure",
                strategy=self.name,
                consecutive_failures=self._consecutive_failures,
                message="Funding rate data unavailable for extended period"
            )

        return None

    # Reset failure counter on success
    self._consecutive_failures = 0

    # Continue with existing opportunity check logic...

async def _get_funding_rate_with_retry(
    self,
    exchange_id: str,
    symbol: str,
    max_retries: int = 3
) -> FundingRate | None:
    """Get funding rate with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            rate = self.data_handler.get_latest_funding_rate(exchange_id, symbol)
            if rate is not None:
                return rate

            # If no data and not last attempt, wait before retry
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 0.5  # 0.5s, 1s, 2s
                logger.debug(
                    f"Retrying funding rate fetch for {exchange_id}:{symbol} "
                    f"(attempt {attempt + 1}/{max_retries}) after {wait_time}s"
                )
                await asyncio.sleep(wait_time)

        except Exception as e:
            logger.error(
                f"Error fetching funding rate for {exchange_id}:{symbol}: {e}",
                exc_info=True
            )

    return None
```

## Phase 2: Robust Solution (3-5 days)

### 2.1 Configuration-Driven Subscriptions

**File**: `cyberdelta/config/models/exchange_configs.py` (new file)

```python
from pydantic import BaseModel
from typing import Dict, List, Optional

class SubscriptionConfig(BaseModel):
    """Configuration for a single subscription type."""
    topic_format: str
    handler: str
    shared: bool = False
    rest_fallback: bool = False
    refresh_interval: Optional[int] = None

class ExchangeSubscriptionConfig(BaseModel):
    """Subscription configuration for an exchange."""
    subscriptions: Dict[str, SubscriptionConfig]

EXCHANGE_SUBSCRIPTION_CONFIGS = {
    "hyperliquid": ExchangeSubscriptionConfig(
        subscriptions={
            "orderbook": SubscriptionConfig(
                topic_format="l2Book:{symbol}",
                handler="orderbook"
            ),
            "ticker": SubscriptionConfig(
                topic_format="trades:{symbol}",
                handler="ticker"
            ),
            "funding": SubscriptionConfig(
                topic_format="",  # No WebSocket topic
                handler="funding",
                rest_fallback=True,
                refresh_interval=300  # 5 minutes
            )
        }
    ),
    "backpack": ExchangeSubscriptionConfig(
        subscriptions={
            "ticker": SubscriptionConfig(
                topic_format="ticker.{symbol}",
                handler="ticker"
            ),
            "orderbook": SubscriptionConfig(
                topic_format="orderbook.{symbol}",
                handler="orderbook"
            ),
            "funding": SubscriptionConfig(
                topic_format="funding.{symbol}",
                handler="funding"
            )
        }
    )
}
```

### 2.2 Hybrid WebSocket/REST Data Manager

**File**: `cyberdelta/core/hybrid_data_manager.py` (new file)

```python
class HybridDataManager:
    """Manages both WebSocket and REST data sources with intelligent fallback."""

    def __init__(self, data_handler: DataHandler):
        self.data_handler = data_handler
        self._rest_cache: Dict[str, Dict[str, Tuple[Any, datetime]]] = {}
        self._cache_ttl = timedelta(minutes=5)

    async def get_funding_rate(
        self,
        exchange_id: str,
        symbol: str,
        force_fresh: bool = False
    ) -> FundingRate | None:
        """Get funding rate from WebSocket or REST with caching."""
        # Try WebSocket data first
        ws_data = self.data_handler.get_latest_funding_rate(exchange_id, symbol)

        # If data is fresh enough, return it
        if ws_data and not force_fresh:
            staleness = self._check_data_staleness(exchange_id, symbol, "funding")
            if staleness < timedelta(minutes=10):
                return ws_data

        # Check REST cache
        cache_key = f"{exchange_id}:{symbol}:funding"
        if cache_key in self._rest_cache:
            cached_data, cached_time = self._rest_cache[cache_key]
            if datetime.now(UTC) - cached_time < self._cache_ttl:
                return cached_data

        # Fetch fresh data via REST
        try:
            api_client = self.data_handler.api_clients.get(exchange_id)
            if not api_client:
                return ws_data  # Return stale data if no API client

            rates = await api_client.get_funding_rates(
                GetFundingRatesArgs(symbols=[symbol])
            )

            if rates:
                fresh_rate = rates[0]
                # Update cache
                self._rest_cache[cache_key] = (fresh_rate, datetime.now(UTC))
                # Update DataHandler storage
                self.data_handler._update_funding_rate(
                    exchange_id, symbol, fresh_rate, datetime.now(UTC)
                )
                return fresh_rate

        except Exception as e:
            logger.error(
                f"REST fallback failed for {exchange_id}:{symbol} funding: {e}"
            )

        return ws_data  # Return stale data as last resort
```

## Testing Plan

### Unit Tests
- [ ] Test periodic funding refresh task
- [ ] Test funding rate caching with TTL
- [ ] Test retry logic with exponential backoff
- [ ] Test configuration-driven subscriptions

### Integration Tests
- [ ] Test full arbitrage workflow with REST-only funding
- [ ] Test failover from WebSocket to REST
- [ ] Test handling of stale data
- [ ] Test with multiple symbols

### Performance Tests
- [ ] Measure REST API call frequency
- [ ] Monitor memory usage with caching
- [ ] Test system under API rate limits

## Rollout Plan

### Day 1-2: Phase 1 Implementation
1. Implement periodic funding refresh in DataHandler
2. Fix stale data handling
3. Add retry logic to strategy
4. Deploy to test environment

### Day 3-4: Testing & Monitoring
1. Run integration tests
2. Monitor funding data availability
3. Check arbitrage opportunity detection
4. Tune refresh intervals

### Day 5-7: Phase 2 Implementation
1. Implement configuration-driven subscriptions
2. Create hybrid data manager
3. Add comprehensive error handling
4. Performance optimization

## Monitoring & Alerting

### Key Metrics
```python
# Metrics to track
metrics = {
    "funding_data_availability": {
        "description": "Percentage of time funding data is available",
        "alert_threshold": 0.95,  # Alert if < 95% availability
    },
    "funding_data_staleness": {
        "description": "Age of funding data in seconds",
        "alert_threshold": 600,  # Alert if > 10 minutes stale
    },
    "rest_api_calls_per_minute": {
        "description": "Number of REST API calls for funding",
        "alert_threshold": 10,  # Alert if > 10 calls/min
    },
    "arbitrage_opportunities_per_hour": {
        "description": "Number of opportunities detected",
        "alert_threshold": 0,  # Alert if 0 for 1 hour
    }
}
```

## Risk Mitigation

1. **API Rate Limits**:
   - Implement adaptive polling intervals
   - Use exponential backoff on 429 errors
   - Monitor API usage metrics

2. **Data Consistency**:
   - Timestamp all data updates
   - Validate funding rate ranges
   - Log all data source switches

3. **System Stability**:
   - Graceful degradation on failures
   - Circuit breakers for repeated errors
   - Comprehensive error logging

## Success Criteria

1. ✅ Funding rate data available for Hyperliquid >95% of the time
2. ✅ Arbitrage opportunities detected when funding differential exists
3. ✅ System handles API failures gracefully
4. ✅ Performance metrics within acceptable ranges

## Conclusion

This fix proposal addresses the critical funding rate data gap while building a robust, production-ready solution. The phased approach allows for quick wins while working toward a comprehensive fix.

The implementation leverages existing Hyperliquid REST API capabilities and creates a resilient system that can handle various failure modes while maintaining high availability of funding rate data for the arbitrage strategy.

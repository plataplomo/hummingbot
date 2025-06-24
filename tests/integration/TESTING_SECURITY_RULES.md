# TESTING SECURITY RULES

> **🚨 CRITICAL: This is a TRADING ENGINE - Financial Safety is PARAMOUNT**
>
> These rules are **MANDATORY** for all integration tests. Violations can lead to catastrophic financial losses in production.

## 🚫 **ABSOLUTELY FORBIDDEN PATTERNS**

### 1. **NO HARDCODED FINANCIAL VALUES**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Hardcoded prices/quantities
except Exception as e:
    logger.warning(f"Failed to get price, using fallback: {e}")
    return Decimal("150.00")  # 💀 DEATH SENTENCE

# DANGEROUS - Hardcoded order sizes
if quantity_calculation_fails:
    return Decimal("0.01")  # 💀 ARBITRARY FALLBACK

# DANGEROUS - Hardcoded market constraints
fallback_constraints = {
    "tick_size": Decimal("0.01"),     # 💀 MADE UP
    "step_size": Decimal("0.01"),     # 💀 MADE UP
    "min_quantity": Decimal("0.01"),  # 💀 MADE UP
}
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Fail fast when real data unavailable
except Exception as e:
    raise RuntimeError(
        f"Failed to get market price for {symbol}: {e}. "
        "Trading tests require real market data and cannot use fallback values."
    ) from e
```

**WHY CRITICAL:** Hardcoded values in tests can mask bugs that would cause massive losses with real market data.

---

### 2. **NO GRACEFUL ERROR HANDLING**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Hiding real problems
except Exception as e:
    logger.info(f"Order failed (may be expected): {e}")
    # Test continues as if nothing happened 💀

# DANGEROUS - Sweeping cancellation failures under the rug
try:
    await api.cancel_order(order_id)
except Exception as e:
    logger.warning(f"Could not cancel order: {e}")
    # Continues without canceling 💀

# DANGEROUS - Marking real failures as "expected"
except Exception as e:
    pytest.xfail(f"Cannot create position due to: {e}")  # 💀
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Fail fast and clearly
except Exception as e:
    pytest.fail(
        f"Failed to place order for {symbol}: {e}. "
        "Order placement is a critical operation that must work reliably."
    )

# SAFE - Distinguish expected vs unexpected errors
except Exception as e:
    if "insufficient_balance" in str(e).lower():
        logger.info(f"Order correctly rejected due to balance: {e}")
    else:
        pytest.fail(f"Unexpected error in order placement: {e}")
```

**WHY CRITICAL:** Graceful handling hides real bugs that could cause trading failures in production.

---

### 3. **NO ARBITRARY TOLERANCES**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Made up tolerances
price_tolerance = Decimal("5.0")  # 💀 $5 tolerance? Why?
assert abs(actual_price - expected_price) < price_tolerance

# DANGEROUS - Percentage tolerances without justification
assert abs(pnl_diff) < portfolio_value * Decimal("0.1")  # 💀 10%?!
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Use exchange-specific precision
tick_size = await get_market_constraints(api, symbol)["tick_size"]
assert actual_price % tick_size == Decimal("0"), "Price must respect tick size"

# SAFE - Use documented exchange tolerances
exchange_precision = await get_exchange_precision_limits(api)
assert quantity_matches_within_exchange_limits(actual, expected, exchange_precision)
```

---

### 4. **NO TIME-DEPENDENT TEST ASSUMPTIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Market hours assumptions
if datetime.now().hour < 9:  # 💀 Markets closed?
    pytest.skip("Market closed")  # Wrong timezone? Different exchange hours?

# DANGEROUS - Weekend assumptions
if datetime.now().weekday() > 4:  # 💀 Crypto trades 24/7!
    pytest.skip("Weekend")

# DANGEROUS - Fixed delays
time.sleep(5)  # 💀 Order should be filled by now, right?
assert order.status == OrderStatus.FILLED  # Network lag? High volatility?
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Check actual market status from exchange
market_status = await api.get_market_status(symbol)
if not market_status.is_trading:
    pytest.skip(f"Market {symbol} not trading: {market_status.reason}")

# SAFE - Wait with proper timeout and validation
await wait_for_order_status(order_id, OrderStatus.FILLED, timeout=30)
```

---

### 5. **NO FLOATING POINT ARITHMETIC FOR MONEY**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Float precision errors
price = 123.456  # 💀 float
quantity = 0.1   # 💀 Cannot represent 0.1 exactly in binary!
notional = price * quantity  # 💀 12.345600000000001

# DANGEROUS - Float comparisons
if pnl == 0.0:  # 💀 Never true due to floating point errors
    assert "No PnL"

# DANGEROUS - Currency calculations with floats
fee = notional * 0.001  # 💀 Compound rounding errors
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Always use Decimal for money
from decimal import Decimal
price = Decimal("123.456")
quantity = Decimal("0.1")  # Exact representation
notional = price * quantity  # Exact: 12.3456

# SAFE - Proper money comparisons
if pnl.quantize(Decimal("0.01")) == Decimal("0"):  # Round to cents
    assert "No significant PnL"
```

---

### 6. **NO MOCKING OF CRITICAL FINANCIAL OPERATIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Mocking order placement
@mock.patch('api.place_order')
def test_trading_strategy(mock_place_order):
    mock_place_order.return_value = fake_order  # 💀 Not testing real API!

# DANGEROUS - Mocking balance checks
@mock.patch('api.get_balances')
def test_position_sizing(mock_balances):
    mock_balances.return_value = {"USDC": Decimal("1000")}  # 💀 Fake money!

# DANGEROUS - Mocking price feeds
@mock.patch('api.get_ticker')
def test_arbitrage(mock_ticker):
    mock_ticker.return_value = Ticker(price=Decimal("100"))  # 💀 Fake prices!
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Integration tests with real APIs (using VCR for reproducibility)
@pytest.mark.vcr()
async def test_trading_strategy_integration():
    # Uses real API calls, recorded for reproducibility
    real_order = await api.place_order(args)  # Real exchange response

# SAFE - Unit tests only for non-financial logic
def test_strategy_signal_calculation():
    # Test pure calculation logic, not API interactions
    signal = calculate_signal(price_data, volume_data)
    assert signal in [-1, 0, 1]
```

---

### 7. **NO CURRENCY/SYMBOL HARDCODING**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Hardcoded currency assumptions
usdc_balance = balances["USDC"]  # 💀 What if it's "USD" or "USDC.e"?
btc_price = tickers["BTC"]       # 💀 "BTC-USD"? "BTCUSD"? "BTC_USDC"?

# DANGEROUS - Hardcoded symbol formats
symbol = "BTC-USDC"  # 💀 Binance uses "BTCUSDC", others use "BTC_USDC"
perp_symbol = symbol + "_PERP"  # 💀 Format varies by exchange

# DANGEROUS - Hardcoded decimal places
btc_quantity = round(quantity, 8)    # 💀 What if exchange uses 6 decimals?
usdc_amount = round(amount, 2)       # 💀 Some exchanges use 6 decimals for USDC
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Get formats from exchange specifications
market_info = await api.get_market_info(symbol)
precision = market_info.quantity_precision
formatted_quantity = quantity.quantize(Decimal(10) ** -precision)

# SAFE - Use exchange-specific symbol mapping
normalized_symbol = normalize_symbol_for_exchange(symbol, exchange_name)
balance_key = get_balance_key_for_exchange(asset, exchange_name)
```

---

### 8. **NO TIMEZONE NAIVE DATETIME OPERATIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Naive datetime comparisons
order_time = datetime.now()  # 💀 Local timezone!
if order_time > market_close:  # 💀 Which timezone? Daylight saving?

# DANGEROUS - Hardcoded timezone assumptions
utc_time = datetime.utcnow()  # 💀 UTC is not always exchange timezone
ny_time = utc_time - timedelta(hours=5)  # 💀 EST vs EDT?

# DANGEROUS - String datetime parsing without timezone
timestamp = "2024-01-15 14:30:00"  # 💀 Which timezone?
order_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Always use timezone-aware datetimes
from datetime import datetime, timezone
order_time = datetime.now(timezone.utc)  # Explicit UTC

# SAFE - Get exchange timezone from API
exchange_tz = await api.get_exchange_timezone()
market_time = datetime.now(exchange_tz)

# SAFE - Parse with explicit timezone
from zoneinfo import ZoneInfo
timestamp_with_tz = "2024-01-15T14:30:00+00:00"
order_time = datetime.fromisoformat(timestamp_with_tz)
```

---

### 9. **NO RACE CONDITION ASSUMPTIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Assuming order sequence
order1 = await api.place_order(args1)
order2 = await api.place_order(args2)
assert order1.timestamp < order2.timestamp  # 💀 Race condition!

# DANGEROUS - Assuming immediate updates
await api.place_order(args)
positions = await api.get_positions()  # 💀 May not be updated yet!
assert len(positions) > 0

# DANGEROUS - Assuming atomic operations
balance_before = await api.get_balance("USDC")
await api.place_order(buy_order)  # 💀 Another process might trade!
balance_after = await api.get_balance("USDC")
assert balance_before > balance_after  # 💀 Race condition!
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Wait for updates with polling
order = await api.place_order(args)
await wait_for_position_update(expected_size, timeout=30)

# SAFE - Use order IDs for tracking
order_id = order.exchange_order_id
await wait_for_order_status(order_id, OrderStatus.FILLED, timeout=30)

# SAFE - Expect eventual consistency
await eventually_assert(
    lambda: len(await api.get_positions()) > 0,
    timeout=30,
    message="Position should appear after order fill"
)
```

---

### 10. **NO NETWORK/CONNECTIVITY ASSUMPTIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Assuming network reliability
response = await api.get_ticker(symbol)  # 💀 Network failure? API down?
assert response.price > 0  # Test fails in network outage

# DANGEROUS - No retry logic in tests
try:
    order = await api.place_order(args)
except Exception:
    pytest.fail("Order failed")  # 💀 Maybe just network blip?

# DANGEROUS - Assuming API rate limits won't be hit
for i in range(1000):  # 💀 Will hit rate limits!
    await api.get_ticker(f"SYMBOL_{i}")
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Retry with exponential backoff for transient failures
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(ConnectionError)
)
async def reliable_api_call():
    return await api.get_ticker(symbol)

# SAFE - Distinguish network errors from API errors
try:
    order = await api.place_order(args)
except ConnectionError as e:
    pytest.skip(f"Network connectivity issue: {e}")
except APIError as e:
    # This is a real API error that should fail the test
    pytest.fail(f"API rejected order: {e}")
```

---

## ✅ **MANDATORY PRACTICES**

### 1. **Always Use Real Market Data**

```python
# REQUIRED - Use shared helpers that fail fast
from tests.integration.apis.backpack.shared.test_helpers import (
    get_dynamic_test_price,        # Real market prices
    get_market_constraints,        # Real exchange constraints
    get_minimal_order_size,        # Real calculated sizes
    get_current_market_price,      # Real current prices
)

# REQUIRED - Never create local helpers with fallbacks
```

### 2. **Fail Fast on Critical Operations**

```python
# REQUIRED - Order operations must succeed or fail clearly
placed_order = await api.place_order(args)
assert placed_order.exchange_order_id, "Order must have valid exchange ID"

# REQUIRED - Cancellation must work
cancellation_result = await api.cancel_order(cancel_args)
if not cancellation_result:
    pytest.fail("Order cancellation is critical and must succeed")
```

### 3. **Validate Error Types**

```python
# REQUIRED - Distinguish business logic errors from system errors
except APIError as e:
    if "insufficient_funds" in str(e).lower():
        # Expected business logic error
        logger.info(f"Order correctly rejected: {e}")
    else:
        # Unexpected system error
        pytest.fail(f"Unexpected API error: {e}")
except Exception as e:
    # All other exceptions are test failures
    pytest.fail(f"System error in trading operation: {e}")
```

---

### 11. **NO IMPLICIT ROUNDING OR TRUNCATION**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Implicit rounding
quantity = Decimal("1.123456789")
rounded = round(quantity, 2)  # 💀 Python's round() uses banker's rounding!

# DANGEROUS - Silent truncation
price_str = f"{price:.2f}"  # 💀 Truncates, doesn't round!
truncated_price = Decimal(price_str)

# DANGEROUS - Floor division for fees
fee = notional // 1000  # 💀 Integer division loses precision!
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Explicit rounding with proper mode
from decimal import ROUND_HALF_UP, ROUND_DOWN
quantity = quantity.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

# SAFE - Use exchange-specific rounding rules
rounding_mode = get_exchange_rounding_mode(exchange, operation_type)
final_amount = amount.quantize(precision, rounding=rounding_mode)
```

---

### 12. **NO SILENT OVERFLOW/UNDERFLOW**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Unchecked multiplication
huge_price = Decimal("1000000")
huge_quantity = Decimal("1000000")
notional = huge_price * huge_quantity  # 💀 Could overflow system limits

# DANGEROUS - Division by very small numbers
tiny_price = Decimal("0.00000001")
position_value = balance / tiny_price  # 💀 Could create massive position

# DANGEROUS - Unchecked leverage calculation
leverage = position_size / margin  # 💀 What if margin is near zero?
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Check for reasonable bounds
MAX_NOTIONAL = Decimal("100000000")  # $100M limit
if huge_price * huge_quantity > MAX_NOTIONAL:
    pytest.fail(f"Notional {notional} exceeds safety limit {MAX_NOTIONAL}")

# SAFE - Minimum value checks
MIN_PRICE = Decimal("0.000001")
if price < MIN_PRICE:
    pytest.fail(f"Price {price} below minimum {MIN_PRICE}")
```

---

### 13. **NO STALE DATA ASSUMPTIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Using cached/stale prices
cached_price = price_cache.get(symbol)  # 💀 Could be hours old!
if cached_price:
    order_size = calculate_size(cached_price)

# DANGEROUS - Assuming balance is current
balance = await api.get_balance("USDC")
time.sleep(60)  # 💀 Other processes might have traded!
order = await api.place_order(args_based_on_old_balance)

# DANGEROUS - Using old market data
yesterday_ticker = historical_data[-1]  # 💀 Using yesterday's prices!
arbitrage_opportunity = compare_prices(today_price, yesterday_ticker.price)
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Always get fresh data before trading decisions
current_price = await api.get_ticker(symbol)
assert current_price.timestamp > datetime.now() - timedelta(seconds=30)

# SAFE - Validate data freshness
data_age = datetime.now() - ticker.timestamp
if data_age > timedelta(seconds=10):
    pytest.fail(f"Price data too stale: {data_age}")
```

---

### 14. **NO ORDER OF OPERATIONS ASSUMPTIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Assuming operation order in async
async def dangerous_concurrent():
    balance_task = asyncio.create_task(api.get_balance("USDC"))
    price_task = asyncio.create_task(api.get_ticker(symbol))

    balance = await balance_task  # 💀 Which completes first?
    price = await price_task      # 💀 Order not guaranteed!

    # Using potentially inconsistent data
    order_size = calculate_size(balance, price)

# DANGEROUS - Assuming sequential fills
order1 = await api.place_order(args1)  # 💀 Market order
order2 = await api.place_order(args2)  # 💀 Market order
# Assuming order1 filled before order2 placed - NOT GUARANTEED!
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Get consistent snapshot
async def safe_data_gathering():
    # Get data as close together as possible
    start_time = datetime.now()
    balance, ticker = await asyncio.gather(
        api.get_balance("USDC"),
        api.get_ticker(symbol)
    )
    end_time = datetime.now()

    # Verify data consistency
    if end_time - start_time > timedelta(seconds=1):
        pytest.fail("Data gathering took too long, may be inconsistent")
```

---

### 15. **NO IMPLICIT CURRENCY CONVERSIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Assuming USD = USDC = USDT
if balance["USD"] > required_amount:  # 💀 USD ≠ USDC!
    await place_usdc_order()

# DANGEROUS - Mixing stablecoin types
total_stable = balance["USDC"] + balance["USDT"]  # 💀 Different assets!
total_stable += balance["BUSD"]  # 💀 De-pegging risk!

# DANGEROUS - Implicit forex assumptions
btc_eur_price = btc_usd_price  # 💀 Missing EUR/USD conversion!
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Explicit currency handling
def get_normalized_balance(balances: dict, target_currency: str):
    if target_currency not in balances:
        raise ValueError(f"No {target_currency} balance available")
    return balances[target_currency]

# SAFE - Explicit conversion when needed
usd_equivalent = await convert_currency(
    amount=balance_usdc,
    from_currency="USDC",
    to_currency="USD",
    exchange=api
)
```

---

### 16. **NO BATCH OPERATION PARTIAL FAILURE IGNORANCE**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Ignoring partial batch failures
orders = []
for symbol in symbols:
    try:
        order = await api.place_order(get_args(symbol))
        orders.append(order)
    except Exception:
        continue  # 💀 Silent failure! Portfolio now unbalanced!

# DANGEROUS - Assuming all-or-nothing semantics
batch_result = await api.place_batch_orders(all_orders)
assert len(batch_result) == len(all_orders)  # 💀 Some might have failed!
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Track and handle partial failures
successful_orders = []
failed_orders = []

for symbol in symbols:
    try:
        order = await api.place_order(get_args(symbol))
        successful_orders.append(order)
    except Exception as e:
        failed_orders.append((symbol, str(e)))

# Fail if critical orders failed
if len(failed_orders) > len(symbols) * 0.2:  # More than 20% failed
    pytest.fail(f"Too many order failures: {failed_orders}")
```

---

### 17. **NO PRECISION LOSS IN CALCULATIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Multiple division operations
fee_rate = Decimal("0.001")
amount_after_fees = principal
for i in range(100):  # 💀 Compound rounding errors!
    amount_after_fees = amount_after_fees * (1 - fee_rate)

# DANGEROUS - Intermediate float conversion
decimal_amount = Decimal("123.456789")
percentage = float(decimal_amount) * 0.1  # 💀 Lost precision!
final_amount = Decimal(str(percentage))   # 💀 Cannot recover precision!
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Minimize operations, maintain precision
fee_rate = Decimal("0.001")
compound_rate = (Decimal("1") - fee_rate) ** 100
final_amount = principal * compound_rate

# SAFE - Keep everything in Decimal
percentage_rate = Decimal("0.1")
final_amount = decimal_amount * percentage_rate
```

---

### 18. **NO UNCHECKED EXTERNAL DATA**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Trusting external price feeds
external_price = await third_party_api.get_price(symbol)
arbitrage_opportunity = our_price - external_price  # 💀 Could be manipulated!

# DANGEROUS - No sanity checks on API responses
market_data = await api.get_market_data(symbol)
# Using market_data.price without validation 💀

# DANGEROUS - Trusting websocket data without validation
@websocket.on_message
def handle_price_update(data):
    new_price = Decimal(data["price"])  # 💀 No validation!
    trigger_trading_signal(new_price)
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Validate external data against multiple sources
prices = await asyncio.gather(
    api1.get_price(symbol),
    api2.get_price(symbol),
    api3.get_price(symbol)
)

# Check for outliers
median_price = statistics.median(prices)
for price in prices:
    if abs(price - median_price) > median_price * Decimal("0.05"):  # 5% deviation
        pytest.fail(f"Price outlier detected: {price} vs median {median_price}")

# SAFE - Sanity check all market data
if not (Decimal("0") < market_data.price < Decimal("1000000")):
    pytest.fail(f"Price {market_data.price} outside reasonable bounds")
```

---

## 🔍 **SECURITY CHECKLIST**

Before committing any integration test, verify:

### **Financial Data Integrity:**
- [ ] **No hardcoded prices, quantities, or financial values**
- [ ] **No fallback mechanisms that use arbitrary values**
- [ ] **All Decimal types for money calculations (no floats)**
- [ ] **Explicit rounding with proper modes (no implicit truncation)**
- [ ] **Exchange-specific precision and constraints**
- [ ] **Currency symbols explicitly specified (no assumptions)**

### **Error Handling:**
- [ ] **No `logger.info/warning("failed")` patterns without `pytest.fail()`**
- [ ] **No `pytest.xfail()` for real system failures**
- [ ] **No `except: pass` or silent exception handling**
- [ ] **Clear distinction between business errors vs system errors**
- [ ] **Network errors handled separately from API errors**

### **Trading Operations:**
- [ ] **Order operations fail tests when they fail in reality**
- [ ] **Cancellation failures cause test failures**
- [ ] **All market data comes from real exchange APIs**
- [ ] **Position calculations use real position data**
- [ ] **No mocking of critical financial operations**

### **Data Freshness & Consistency:**
- [ ] **No stale price data (validate timestamps)**
- [ ] **No timezone-naive datetime operations**
- [ ] **Race conditions properly handled**
- [ ] **Batch operation failures tracked**
- [ ] **External data validated against multiple sources**

### **Precision & Bounds:**
- [ ] **No precision loss in calculations**
- [ ] **Overflow/underflow checks for large calculations**
- [ ] **Reasonable bounds validation for all financial values**
- [ ] **Step size and tick size constraints respected**

### **Time & Market Assumptions:**
- [ ] **No hardcoded market hours or timezone assumptions**
- [ ] **No fixed delays or sleep() calls for order fills**
- [ ] **Market status checked from exchange (not assumed)**

---

## 🛡️ **ENFORCEMENT**

### Code Review Requirements

1. **Every PR touching integration tests must be reviewed by a senior developer**
2. **Any hardcoded financial value automatically fails review**
3. **Any graceful error handling must be explicitly justified**
4. **All test failures must be investigated, never ignored**

### Automated Checks

```bash
# These patterns automatically fail CI:
grep -r "Decimal.*[0-9]" tests/integration/           # Hardcoded decimals
grep -r "logger.*failed.*expected" tests/            # Graceful logging
grep -r "pytest.xfail" tests/integration/            # Hidden failures
grep -r "float.*price\|price.*float" tests/          # Float prices
grep -r "time\.sleep" tests/integration/             # Fixed delays
grep -r "round(" tests/integration/                  # Implicit rounding
grep -r "datetime\.now()" tests/integration/         # Timezone naive
grep -r "except.*pass" tests/integration/            # Silent failures
grep -r "USD.*USDC\|USDC.*USD" tests/                # Currency confusion
grep -r "mock.*place_order\|mock.*get_balance" tests/ # Mocked financials
```

### Static Analysis Rules

```python
# Add to pre-commit hooks:
# 1. Pylint rules for trading safety
# 2. mypy strict mode for Decimal types
# 3. bandit security scanning
# 4. Custom AST analyzer for financial patterns

# Custom rule examples:
def check_decimal_usage(node):
    """Ensure all financial calculations use Decimal"""
    if isinstance(node, ast.BinOp) and has_financial_context(node):
        if not all_operands_are_decimal(node):
            raise ViolationError("Financial calculation must use Decimal")

def check_hardcoded_values(node):
    """Detect hardcoded financial values"""
    if isinstance(node, ast.Num) and in_financial_context(node):
        raise ViolationError("No hardcoded financial values allowed")
```

---

## 💰 **FINANCIAL IMPACT**

**Why these rules exist - Real scenarios:**

### **Precision Errors:**
- **Float arithmetic**: `0.1 + 0.2 = 0.30000000000000004` → Wrong trade sizes
- **Implicit rounding**: Round $1.125 → $1.12 vs $1.13 → Different fills
- **Currency mix-up**: Sell BTC for "USD" but get USDC → Wrong accounting

### **Timing Disasters:**
- **Stale price data**: Use 1-hour old price → Trade at terrible rate
- **Race conditions**: Check balance, other process trades, place order → Overdraft
- **Time zone error**: Think market closed, it's actually open → Missed opportunities

### **Scale Disasters:**
- **Hardcoded $150** → Real market price $15,000 → **100x loss**
- **Wrong decimal places**: Order 1000 BTC instead of 1.000 BTC → **1000x loss**
- **Overflow**: Large leverage calculation → Position bigger than intended

### **Error Handling Disasters:**
- **Hidden order failure** → Position not hedged → **Unlimited loss**
- **Failed cancellation** → Stuck in bad trade → **Cannot exit**
- **Silent batch failure** → Portfolio unbalanced → **Risk exposure**

### **Real Numbers:**
- **$1M portfolio × 10% "tolerance"** → **$100K acceptable error?!**
- **0.001% precision error × High frequency** → **Millions in accumulated losses**
- **1 missed cancellation × Volatile market** → **Account liquidation**

---

## 🆘 **EXAMPLES OF CATASTROPHIC BUGS**

### Real-World Example 1: Knight Capital (2012)
- **Bug**: Test code accidentally deployed to production
- **Result**: $440 million loss in 45 minutes
- **Cause**: Tests didn't catch the deployment configuration error

### Real-World Example 2: Fat Finger Orders
- **Bug**: Hardcoded test quantities used in production
- **Result**: Massive unintended orders
- **Cause**: Tests used arbitrary sizes that seemed "safe"

### Real-World Example 3: Floating Point Precision (ARIANE 5)
- **Bug**: Float overflow in guidance system
- **Result**: $500 million rocket destroyed
- **Cause**: Converted 64-bit float to 16-bit integer, caused overflow

### Real-World Example 4: Timezone Bug (Energy Trading)
- **Bug**: Daylight saving time not handled properly
- **Result**: Millions in losses from incorrect position timing
- **Cause**: Hardcoded UTC-5 instead of using proper timezone libraries

### Real-World Example 5: Currency Confusion
- **Bug**: System confused JPY and USD (¥100 vs $100)
- **Result**: Massive over-leveraging (100x size error)
- **Cause**: Implicit currency assumptions in tests

### Real-World Example 6: Stale Price Data
- **Bug**: Used cached prices during market volatility
- **Result**: Executed trades at 20% worse rates
- **Cause**: Tests didn't validate data freshness requirements

---

## 📞 **WHEN IN DOUBT**

1. **Make the test fail rather than hide the problem**
2. **Use real market data rather than fake values**
3. **Ask a senior developer before adding any fallback mechanism**
4. **Remember: This is real money, not a game**

---

> **⚠️ REMEMBER: In trading, being approximately right is often worse than being precisely wrong. Tests that hide problems are more dangerous than no tests at all.**

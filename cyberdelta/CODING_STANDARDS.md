# CODING STANDARDS

> **🚨 CRITICAL: This is a TRADING ENGINE - Assumptions and Hardcoding are FORBIDDEN**
>
> These standards are **MANDATORY** for all code. Violations lead to unpredictable behavior, financial losses, and system failures.

## 🚫 **ABSOLUTELY FORBIDDEN PATTERNS**

### 1. **NO ASSUMPTIONS - EVER**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Assuming units
min_value = self._sizing_config.min_position_size * Decimal("100")  # 💀 Assuming $100 per unit

# DANGEROUS - Assuming formats
symbol = "BTC-USDC"  # 💀 What if exchange uses "BTCUSDC" or "BTC_USDC"?

# DANGEROUS - Assuming timezone
market_close = datetime(2024, 1, 15, 16, 0)  # 💀 Which timezone? DST?

# DANGEROUS - Assuming currency equivalence  
total_balance = balances["USD"] + balances["USDC"]  # 💀 USD ≠ USDC!

# DANGEROUS - Assuming market behavior
if datetime.now().weekday() > 4:  # 💀 Crypto trades 24/7!
    skip_trading()

# DANGEROUS - Assuming data structure
price = market_data["price"]  # 💀 What if key doesn't exist?
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Use values as they are from config
min_value = self._sizing_config.min_position_size  # Already in correct units

# SAFE - Get format from exchange/config
symbol = self.config.symbols.get_exchange_format(base, quote, exchange)

# SAFE - Use timezone-aware operations
market_close = exchange_info.market_close_time  # From exchange API

# SAFE - Handle each currency explicitly
usd_balance = balances.get("USD", Decimal(0))
usdc_balance = balances.get("USDC", Decimal(0))
# Never add different currencies!

# SAFE - Check actual market status
if not await exchange.is_market_open(symbol):
    logger.info(f"Market closed for {symbol}")
```

**WHY CRITICAL:** Assumptions create hidden dependencies that break when reality differs from your mental model.

---

### 2. **NO HARDCODED VALUES - NONE**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Hardcoded limits
self.max_position = 200.0  # 💀 WRONG!
self.max_exposure = 1000.0  # 💀 WRONG!

# DANGEROUS - Hardcoded timeouts
await asyncio.sleep(300)  # 💀 5 minutes? Says who?
timeout = 30.0  # 💀 Arbitrary timeout

# DANGEROUS - Hardcoded retries
for attempt in range(3):  # 💀 Why 3?
    try_operation()

# DANGEROUS - Hardcoded precision
rounded = round(value, 2)  # 💀 Why 2 decimals?

# DANGEROUS - Hardcoded fallbacks
price = get_price() or Decimal("100")  # 💀 NEVER!

# DANGEROUS - Hardcoded factors
fee = amount * 0.001  # 💀 What fee rate?
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Everything from config
self.max_position = config.risk.global_risk.max_position_usd
self.max_exposure = config.risk.global_risk.max_total_exposure_usd

# SAFE - Timeouts from config
await asyncio.sleep(config.general.state_save_interval)
timeout = config.exchanges[exchange].request_timeout_seconds

# SAFE - Retries from config
for attempt in range(config.execution.max_retries):
    try_operation()

# SAFE - Precision from exchange/config
precision = market_info.price_precision
rounded = value.quantize(Decimal(10) ** -precision)

# SAFE - No fallbacks, fail fast
price = get_price()
if price is None:
    raise ValueError("Price unavailable - cannot proceed")

# SAFE - Fees from config/exchange
fee_rate = config.exchanges[exchange].fee_rate
fee = amount * fee_rate
```

**WHY CRITICAL:** Hardcoded values create invisible configuration that cannot be changed without code deployment.

---

### 3. **NO DEFAULT VALUES FOR CRITICAL OPERATIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Default exchange
def place_order(symbol, exchange="hyperliquid"):  # 💀 Why this default?
    pass

# DANGEROUS - Default timeout with fallback
timeout = config.get("timeout", 30)  # 💀 Arbitrary default

# DANGEROUS - Optional with assumption
def calculate_risk(max_loss=None):
    if max_loss is None:
        max_loss = 1000  # 💀 Assumed default!

# DANGEROUS - Environment fallback
api_url = os.getenv("API_URL", "https://api.example.com")  # 💀 

# DANGEROUS - Missing config handling
try:
    rate_limit = config.exchanges[exchange].rate_limit
except KeyError:
    rate_limit = 100  # 💀 Made-up limit
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Explicit parameters required
def place_order(symbol: Symbol, exchange: ExchangeName):  # No defaults
    pass

# SAFE - Config must exist
timeout = config.exchanges[exchange].request_timeout_seconds
# Fails fast if not configured

# SAFE - Required parameters
def calculate_risk(max_loss: Decimal):  # Required, no default
    pass

# SAFE - Environment variable required
api_url = os.environ["API_URL"]  # Fails if not set

# SAFE - Config validation
if exchange not in config.exchanges:
    raise ConfigurationError(f"Exchange {exchange} not configured")
rate_limit = config.exchanges[exchange].rate_limit
```

**WHY CRITICAL:** Defaults hide missing configuration and create different behavior in different environments.

---

### 4. **NO IMPLICIT TYPE CONVERSIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - String to number assumptions
price = float("123.45")  # 💀 Precision loss
quantity = int(user_input)  # 💀 Truncation

# DANGEROUS - Number to string assumptions
price_str = f"{price:.2f}"  # 💀 Assumes 2 decimals

# DANGEROUS - Boolean assumptions
if config.get("enabled"):  # 💀 What if "false" string?
    trade()

# DANGEROUS - Implicit None handling
balance = api.get_balance() or 0  # 💀 None means 0?

# DANGEROUS - Enum assumptions
side = "buy" if signal > 0 else "sell"  # 💀 String not enum
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Explicit Decimal parsing
price = Decimal(price_str)  # Preserves precision

# SAFE - Validated parsing
from cyberdelta.utils.parsing import parse_decimal_value
quantity = parse_decimal_value(user_input, field_name="quantity")

# SAFE - Exchange-specific formatting
formatter = ExchangeFormatter(exchange_config)
price_str = formatter.format_price(price)

# SAFE - Explicit boolean validation
if config.general.safe_mode is True:  # Explicit check
    logger.info("Safe mode enabled")

# SAFE - Handle None explicitly
balance = api.get_balance()
if balance is None:
    raise ValueError("Balance unavailable")

# SAFE - Use proper enums
from cyberdelta.enums import OrderSide
side = OrderSide.BUY if signal > 0 else OrderSide.SELL
```

**WHY CRITICAL:** Implicit conversions lose information and create unexpected behavior.

---

### 5. **NO MAGIC NUMBERS OR STRINGS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Magic numbers everywhere
if position_size > 1000000:  # 💀 What's this limit?
    reject_order()

if error_count > 5:  # 💀 Why 5?
    circuit_break()

sleep_time = 2.5  # 💀 Random delay

# DANGEROUS - Magic strings
if status == "filled":  # 💀 Should be enum
    process_fill()

if exchange == "hl":  # 💀 Abbreviation assumption
    use_hyperliquid()
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Named configuration values
if position_size > config.risk.global_risk.max_position_usd:
    reject_order()

if error_count > config.safety_systems.circuit_breakers.global_consecutive_failures:
    circuit_break()

sleep_time = config.execution.retry_delay_base_sec

# SAFE - Use enums
from cyberdelta.enums import OrderStatus
if status == OrderStatus.FILLED:
    process_fill()

from cyberdelta.enums import ExchangeName  
if exchange == ExchangeName.HYPERLIQUID:
    use_hyperliquid()
```

**WHY CRITICAL:** Magic values have no context and cannot be configured or understood.

---

### 6. **NO PATH OR FILE ASSUMPTIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Hardcoded paths
log_file = "/var/log/trading.log"  # 💀 Unix only
state_file = "./data/state.json"  # 💀 Relative to what?

# DANGEROUS - Path separators
config_path = "config\\settings.yaml"  # 💀 Windows only

# DANGEROUS - File existence assumptions
with open("secrets.json") as f:  # 💀 What if doesn't exist?
    secrets = json.load(f)

# DANGEROUS - Directory assumptions
os.makedirs("logs")  # 💀 What if exists? Permissions?
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Paths from config
from pathlib import Path
log_file = Path(config.general.log_file)
state_file = Path(config.general.state_file)

# SAFE - Platform-independent paths
config_path = Path("config") / "settings.yaml"

# SAFE - Check existence
secrets_path = Path(config.general.secrets_file)
if not secrets_path.exists():
    raise ConfigurationError(f"Secrets file not found: {secrets_path}")

# SAFE - Create with exist_ok
log_dir = Path(config.general.log_dir)
log_dir.mkdir(parents=True, exist_ok=True)
```

**WHY CRITICAL:** Path assumptions break across platforms and deployments.

---

### 7. **NO NETWORK OR CONNECTIVITY ASSUMPTIONS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Assuming success
response = api.get_ticker(symbol)  # 💀 What if fails?
price = response["price"]

# DANGEROUS - Assuming availability  
while True:
    data = await websocket.recv()  # 💀 What if disconnects?
    process(data)

# DANGEROUS - Retry without limits
while not success:  # 💀 Infinite loop possible
    success = try_connect()

# DANGEROUS - Fixed delays
await asyncio.sleep(1)  # 💀 Network ready in 1 second?
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Handle failures explicitly
try:
    response = await api.get_ticker(symbol)
    if not response:
        raise ValueError("Empty response from API")
    price = response.get("price")
    if price is None:
        raise ValueError("No price in response")
except Exception as e:
    logger.error(f"Failed to get ticker: {e}")
    raise

# SAFE - Handle disconnections
try:
    async with timeout(config.websocket.receive_timeout):
        data = await websocket.recv()
except (asyncio.TimeoutError, ConnectionError) as e:
    await handle_reconnection()

# SAFE - Bounded retries from config
for attempt in range(config.connection.max_retries):
    if try_connect():
        break
    await asyncio.sleep(config.connection.retry_delay * (attempt + 1))
else:
    raise ConnectionError("Failed to connect after all retries")
```

**WHY CRITICAL:** Network assumptions create failures in production when connectivity isn't perfect.

---

### 8. **NO CALCULATION SHORTCUTS**

**❌ NEVER DO THIS:**
```python
# DANGEROUS - Percentage as float
fee = amount * 0.001  # 💀 Is this 0.1%? From where?

# DANGEROUS - Direct arithmetic
total = price * quantity  # 💀 Decimal? Float? Precision?

# DANGEROUS - Rounding assumptions
display_value = round(value, 2)  # 💀 Currency decimals?

# DANGEROUS - Unit conversions
btc_in_sats = btc * 100000000  # 💀 Hardcoded conversion
```

**✅ CORRECT APPROACH:**
```python
# SAFE - Fee from config/exchange
fee_rate = config.exchanges[exchange].maker_fee_rate
fee = amount * fee_rate

# SAFE - Explicit types
from decimal import Decimal
total = Decimal(price) * Decimal(quantity)

# SAFE - Format per exchange rules
decimals = exchange_info.quote_precision
display_value = value.quantize(Decimal(10) ** -decimals)

# SAFE - Named constants from config/enum
from cyberdelta.constants import SATOSHIS_PER_BTC
btc_in_sats = btc * SATOSHIS_PER_BTC
```

**WHY CRITICAL:** Calculation shortcuts compound errors and hide business logic.

---

## ✅ **MANDATORY PRACTICES**

### 1. **Configuration-First Development**

```python
# REQUIRED - All services receive AppSettings
class AnyService:
    def __init__(self, config: AppSettings):
        self.config = config
        # Cache frequently used values
        self._max_retries = config.execution.max_retries
        
    # NEVER hardcode in methods
    async def execute(self):
        timeout = self.config.execution.request_timeout
        # NOT timeout = 30
```

### 2. **Explicit Over Implicit**

```python
# REQUIRED - Be explicit about everything
symbol = Symbol("BTC")  # NOT "BTC" string
exchange = ExchangeName.HYPERLIQUID  # NOT "hyperliquid" string
side = OrderSide.BUY  # NOT "buy" string

# REQUIRED - Explicit error handling
result = await api.call()
if result is None:
    raise ValueError("API returned None")
# NOT: result = await api.call() or default_value
```

### 3. **Fail Fast Philosophy**

```python
# REQUIRED - Never hide failures
if not config.validate():
    raise ConfigurationError("Invalid configuration")
# NOT: use defaults when config invalid

# REQUIRED - Validate early
def process_order(order: Order):
    if order.quantity <= 0:
        raise ValueError(f"Invalid quantity: {order.quantity}")
    # NOT: silently skip invalid orders
```

### 4. **Type Safety Throughout**

```python
# REQUIRED - Use domain types
from cyberdelta.core.symbols import Symbol
from cyberdelta.enums import ExchangeName, OrderSide
from decimal import Decimal

# NOT raw strings and floats
async def place_order(
    symbol: Symbol,  # NOT str
    exchange: ExchangeName,  # NOT str  
    quantity: Decimal,  # NOT float
    price: Decimal  # NOT float
) -> Order:
    pass
```

---

## 🔍 **CODE REVIEW CHECKLIST**

Before committing any code, verify:

### **No Assumptions:**
- [ ] **No unit assumptions** (dollars, seconds, percentages)
- [ ] **No format assumptions** (symbol formats, date formats)
- [ ] **No timezone assumptions** (always timezone-aware)
- [ ] **No platform assumptions** (paths, line endings)
- [ ] **No market behavior assumptions** (hours, holidays)
- [ ] **No data structure assumptions** (keys exist, types)

### **No Hardcoding:**
- [ ] **No hardcoded numbers** (use config for everything)
- [ ] **No hardcoded strings** (use enums and config)
- [ ] **No hardcoded paths** (use Path and config)
- [ ] **No hardcoded timeouts** (use config)
- [ ] **No hardcoded retries** (use config)
- [ ] **No hardcoded limits** (use config)

### **Explicit Handling:**
- [ ] **No default parameters for critical functions**
- [ ] **No implicit type conversions**
- [ ] **No silent error handling**
- [ ] **No graceful degradation with fallbacks**
- [ ] **All None values handled explicitly**
- [ ] **All exceptions handled explicitly**

### **Configuration Usage:**
- [ ] **AppSettings injected via constructor**
- [ ] **All values come from configuration**
- [ ] **No magic numbers or strings**
- [ ] **Config validation at startup**

---

## 🛡️ **ENFORCEMENT**

### Automated Checks

```bash
# These patterns automatically fail CI:
grep -r "Decimal(\"[0-9]" cyberdelta/           # Hardcoded decimals
grep -r "sleep([0-9]" cyberdelta/              # Hardcoded delays  
grep -r "= [0-9]\\+\\.[0-9]" cyberdelta/       # Hardcoded floats
grep -r "timeout.*=" cyberdelta/ | grep -v config  # Hardcoded timeouts
grep -r "or [0-9]\\|or \"" cyberdelta/         # Fallback values
grep -r "except.*pass" cyberdelta/             # Silent exceptions
grep -r "datetime.now()" cyberdelta/           # Timezone naive
grep -r "round(" cyberdelta/                   # Implicit rounding
```

### Pre-commit Hooks

```yaml
# .pre-commit-config.yaml additions
- repo: local
  hooks:
    - id: no-hardcoding
      name: Check for hardcoded values
      entry: ./scripts/check_no_hardcoding.py
      language: python
      files: \.py$
      
    - id: no-assumptions  
      name: Check for assumptions
      entry: ./scripts/check_no_assumptions.py
      language: python
      files: \.py$
```

### Code Review Standards

1. **Any PR with hardcoded values is automatically rejected**
2. **Any PR with implicit assumptions must be rewritten**
3. **Any PR with default fallbacks for critical operations fails review**
4. **All configuration must come from AppSettings**

---

## 💀 **CONSEQUENCES OF VIOLATIONS**

### Real Examples of What Happens:

**Hardcoded Timeout:**
- Dev: "30 seconds should be enough"
- Prod: API takes 31 seconds during high load
- Result: All orders fail during busy periods

**Assumed Units:**
- Dev: "Position sizes are obviously in USD"
- Prod: Exchange returns position in base currency
- Result: 100 BTC position instead of $100

**Magic Number:**
- Dev: "5 retries is reasonable"  
- Prod: Network issues need 6 retries
- Result: Trades fail that could succeed

**Default Exchange:**
- Dev: "Most trades are on Hyperliquid"
- Prod: User trades on Backpack
- Result: Orders sent to wrong exchange

**Implicit Conversion:**
- Dev: `float(price_str)`
- Prod: "123.456789" becomes 123.45678900000001
- Result: Price precision errors

**Path Assumption:**
- Dev: "/tmp/cache" on Linux
- Prod: Deployed on Windows
- Result: Application crashes on startup

---

## 🆘 **WHEN IN DOUBT**

1. **If you're about to type a number, stop and add it to config**
2. **If you're about to use a string literal, stop and use an enum**
3. **If you're about to add a default parameter, stop and make it required**
4. **If you're about to handle an error gracefully, stop and fail fast**
5. **If you're about to assume something, stop and get it from config/API**

---

## 📢 **REMEMBER**

> **In trading systems, every assumption is a future bug, every hardcoded value is a future crisis, and every implicit behavior is a future investigation.**
>
> **Write code as if the person maintaining it is a violent psychopath who knows where you live. That person might be you in 6 months.**
>
> **The market doesn't care about your assumptions. Reality doesn't respect your defaults. Configuration is not optional.**

---

### The Golden Rule:

# **If it's not from config or an API response, it doesn't belong in the code.**
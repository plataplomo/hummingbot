# Hyperliquid Integration Tests

## API Endpoint Architecture

Hyperliquid has a unique API architecture that differs from typical exchange patterns:

### `/info` Endpoint (Read Operations)
- **Purpose**: All read operations, including private account data
- **Authentication**: NOT required for public data, but required for private data
- **Signing**: NEVER signed (uses public address in request body for private data)
- **Examples**:
  - `clearinghouseState` - Get account balances, positions, margins (private)
  - `openOrders` - Get open orders (private)
  - `userFills` - Get trade history (private)
  - `orderStatus` - Get order details (private)
  - `meta` - Get exchange metadata (public)
  - `allMids` - Get market prices (public)

### `/exchange` Endpoint (Write Operations)
- **Purpose**: All state-changing operations
- **Authentication**: ALWAYS required
- **Signing**: ALWAYS signed with EIP-712
- **Examples**:
  - Place order
  - Cancel order
  - Update leverage
  - Transfer funds
  - Withdraw

## Test Organization

Our tests are organized by **business model** rather than endpoint type:

### Account Data Tests
- `test_hl_account_summary.py` - Tests for MarginAccountSummary model (uses `/info`)
- `test_hl_balances.py` - Tests for SpotBalance model (uses `/info`)
- `test_hl_positions.py` - Tests for DerivativePosition model (uses `/info`)

### Trading Tests
- `test_hl_orders.py` - Tests for Order model read operations (uses `/info`)
  - Read operations: get_order, get_order_history, get_open_orders (uses `/info`)
- `test_hl_orders_private.py` - Tests for Order model write operations (uses `/exchange`)
  - Write operations: place_order, cancel_order (uses `/exchange`)

## Important Notes

1. **File naming convention**:
   - Files without "_private" suffix test `/info` endpoints (read operations, unsigned)
   - Files with "_private" suffix test `/exchange` endpoints (write operations, EIP-712 signed)
2. **All `/info` requests are unsigned** - they use the public wallet address in the request body
3. **All `/exchange` requests are signed** - they use EIP-712 cryptographic signatures
4. **VCR cassettes** should scrub sensitive data like wallet addresses and signatures
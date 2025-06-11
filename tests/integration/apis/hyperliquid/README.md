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
  - Write operations: place_order, cancel_order, cancel_all_orders (uses `/exchange`)

### Private Model Tests
- `test_hl_positions_private.py` - Tests for DerivativePosition model write operations (uses `/exchange`)
  - Position-affecting operations: position opening/closing orders, leverage management
- `test_hl_balances_private.py` - Tests for SpotBalance model write operations (uses `/exchange`)
  - Balance-affecting operations: L2 USD transfers, token withdrawals, ETH withdrawals
- `test_hl_account_summary_private.py` - Tests for MarginAccountSummary model write operations (uses `/exchange`)
  - Account-affecting operations: margin-impacting orders, account configuration changes

## Important Notes

1. **File naming convention**:
   - Files without "_private" suffix test `/info` endpoints (read operations, unsigned)
   - Files with "_private" suffix test `/exchange` endpoints (write operations, EIP-712 signed)
2. **All `/info` requests are unsigned** - they use the public wallet address in the request body
3. **All `/exchange` requests are signed** - they use EIP-712 cryptographic signatures
4. **VCR cassettes** should scrub sensitive data like wallet addresses and signatures

## VCR Cassette Organization

### Public Endpoint Cassettes (`/info`)
- `apis/hyperliquid/account_summary` - Account summary read operations
- `apis/hyperliquid/balances` - Balance read operations
- `apis/hyperliquid/positions` - Position read operations
- `apis/hyperliquid/orders` - Order read operations

### Private Endpoint Cassettes (`/exchange`)
- `apis/hyperliquid/orders_private` - Order write operations (place, cancel, cancel_all)
- `apis/hyperliquid/positions_private` - Position-affecting operations
- `apis/hyperliquid/balances_private` - Balance-affecting operations (transfers, withdrawals)
- `apis/hyperliquid/account_summary_private` - Account-affecting operations

## Test Coverage Summary

This comprehensive test suite covers:

### Happy Path Scenarios ✅
- Successful operations for all models with full validation
- Complete model field validation with proper Decimal precision
- End-to-end operation workflows with cleanup

### Edge Case Scenarios ✅
- Very small quantity/amount precision handling
- Boundary condition testing
- Multiple concurrent operations
- Symbol filtering and operation targeting

### Failure Scenarios ✅
- Authentication failures (invalid EIP-712 signatures)
- Insufficient balance/margin errors
- Invalid parameter validation
- Non-existent resource errors (orders, symbols)
- Network and timeout error handling

### Current Implementation Status
- **Fully Implemented**: Order operations (place, cancel, cancel_all)
- **Pending Implementation**: Transfer operations (L2 USD, withdrawals) - methods exist but raise NotImplementedError
- **Future Implementation**: Leverage management operations (not yet in API)
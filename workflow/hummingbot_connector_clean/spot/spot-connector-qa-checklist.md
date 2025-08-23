# Spot Connector QA Testing Checklist

## Testing Environment Setup

### Prerequisites
- Hummingbot installed from source
- Valid API keys for the exchange
- Test funds in the exchange account
- Access to exchange testnet (if available)

## Testing Steps

### 1. Connection Testing

#### Connect API Key
**Command**: `connect connector-name`

**Expected Results**:
1. ✅ Connects successfully with valid API key
2. ✅ Throws error/warning for invalid API key
3. ✅ Throws error/warning for expired API key
4. ✅ Same API key can be used on multiple bot instances (unless restricted)

**Test Cases**:
```
# Valid credentials
connect binance
Enter API key: [valid_key]
Enter secret: [valid_secret]
> You are now connected to binance

# Invalid credentials
connect binance
Enter API key: [invalid_key]
Enter secret: [invalid_secret]
> Error: Invalid API credentials

# Expired key
connect binance
Enter API key: [expired_key]
Enter secret: [expired_secret]
> Error: API key has expired
```

### 2. Balance Testing

#### Check Balances
**Command**: `balance`

**Expected Results**:
1. ✅ Shows all non-zero balances
2. ✅ Updates after trades
3. ✅ Matches exchange UI
4. ✅ Includes locked/reserved amounts

**Validation**:
- Compare with exchange website
- Verify after placing limit orders (balance should be reserved)
- Check after trades complete

### 3. Market Data Testing

#### Order Book Display
**Command**: `order_book --live`

**Expected Results**:
1. ✅ Shows real-time bid/ask prices
2. ✅ Updates continuously
3. ✅ Depth matches exchange
4. ✅ Handles market with low liquidity

#### Ticker Information
**Command**: `ticker --live`

**Expected Results**:
1. ✅ Shows current price
2. ✅ Updates in real-time
3. ✅ Includes 24h volume and price change

### 4. Trading Rules Validation

#### Get Trading Rules
**Test**: Create orders with various sizes and prices

**Expected Results**:
1. ✅ Respects minimum order size
2. ✅ Respects minimum notional value
3. ✅ Uses correct price precision (tick_size)
4. ✅ Uses correct quantity precision (step_size)
5. ✅ Rejects orders violating rules before sending to exchange

**Test Cases**:
```python
# Too small order
> Order amount 0.00001 BTC is below minimum 0.0001 BTC

# Price precision violation
> Price 100.123456 adjusted to 100.12 based on tick size

# Below minimum notional
> Order value $5 is below minimum notional $10
```

### 5. Order Management Testing

#### Place Buy Order
**Test Types**:
- LIMIT order
- LIMIT_MAKER order (if supported)
- MARKET order (if supported)

**Expected Results**:
1. ✅ Order placed successfully
2. ✅ Correct order ID returned
3. ✅ Order appears in `status`
4. ✅ Balance updated (reserved for limit orders)
5. ✅ Order visible on exchange

#### Place Sell Order
**Test Types**:
- LIMIT order
- LIMIT_MAKER order (if supported)
- MARKET order (if supported)

**Expected Results**:
1. ✅ Order placed successfully
2. ✅ Correct order ID returned
3. ✅ Order appears in `status`
4. ✅ Balance updated
5. ✅ Order visible on exchange

#### Cancel Order
**Command**: `cancel --order-id [order_id]`

**Expected Results**:
1. ✅ Order cancelled successfully
2. ✅ Order removed from `status`
3. ✅ Balance returned (for limit orders)
4. ✅ Cancellation reflected on exchange

#### Cancel All Orders
**Command**: `cancel --all`

**Expected Results**:
1. ✅ All orders cancelled
2. ✅ Status shows no open orders
3. ✅ All balances returned

### 6. Order Fill Testing

#### Partial Fill
**Setup**: Place limit order that partially fills

**Expected Results**:
1. ✅ Status shows partial fill amount
2. ✅ Balance updated correctly
3. ✅ Remaining order stays open
4. ✅ Fill history recorded

#### Complete Fill
**Setup**: Place order that completely fills

**Expected Results**:
1. ✅ Order marked as completed
2. ✅ Balance updated fully
3. ✅ Trade recorded in history
4. ✅ Fee deducted correctly

### 7. Fee Testing

#### Trading Fees
**Test**: Execute trades and verify fees

**Expected Results**:
1. ✅ Maker fee applied correctly
2. ✅ Taker fee applied correctly
3. ✅ Fees match exchange fee schedule
4. ✅ Fee currency handled properly

### 8. Error Handling

#### Insufficient Balance
**Test**: Try to place order exceeding balance

**Expected Results**:
1. ✅ Clear error message
2. ✅ Order not submitted to exchange
3. ✅ No balance changes

#### Network Disconnection
**Test**: Disconnect network during operation

**Expected Results**:
1. ✅ Reconnection attempted
2. ✅ Orders tracked during disconnect
3. ✅ State synchronized after reconnection

#### Rate Limiting
**Test**: Trigger rate limits

**Expected Results**:
1. ✅ Rate limit respected
2. ✅ Clear warning/error message
3. ✅ Automatic retry with backoff

### 9. Strategy Compatibility

#### Test with Different Strategies
**Strategies to Test**:
- Pure Market Making (PMM)
- Cross Exchange Market Making (XEMM)
- AMM Arbitrage
- PMM Simple

**Expected Results**:
1. ✅ Connector works with all compatible strategies
2. ✅ Functions as both maker and taker exchange
3. ✅ No strategy-specific errors

### 10. WebSocket Stability

#### Long Running Test
**Duration**: Run for 24+ hours

**Expected Results**:
1. ✅ Maintains connection
2. ✅ Handles reconnections gracefully
3. ✅ No memory leaks
4. ✅ No missed order updates

### 11. History and Logging

#### Trade History
**Command**: `history`

**Expected Results**:
1. ✅ Shows all completed trades
2. ✅ Includes fees
3. ✅ Timestamps accurate
4. ✅ Matches exchange history

#### Export Trades
**Command**: `export_trades`

**Expected Results**:
1. ✅ CSV export works
2. ✅ All fields populated
3. ✅ Data accurate

### 12. Edge Cases

#### Market Closure (if applicable)
**Test**: Behavior during market closure

**Expected Results**:
1. ✅ Clear status indication
2. ✅ Orders queued or rejected appropriately
3. ✅ Graceful handling

#### Symbol Delisting
**Test**: Handle delisted trading pairs

**Expected Results**:
1. ✅ Warning about delisted pair
2. ✅ Cannot create new orders
3. ✅ Can cancel existing orders

#### Maintenance Mode
**Test**: Exchange maintenance handling

**Expected Results**:
1. ✅ Detects maintenance mode
2. ✅ Appropriate error messages
3. ✅ Resumes after maintenance

## Performance Benchmarks

### Latency Requirements
- Order placement: < 500ms (99th percentile)
- Order cancellation: < 500ms (99th percentile)
- Balance update: < 1s after trade
- Order book update: < 100ms

### Reliability Metrics
- Uptime: > 99.9% over 24 hours
- Successful reconnection rate: 100%
- Order tracking accuracy: 100%
- Balance accuracy: 100%

## Test Report Template

```markdown
# Spot Connector QA Report

**Connector**: [connector_name]
**Version**: [version]
**Tester**: [name]
**Date**: [date]
**Test Environment**: [Mainnet/Testnet]

## Summary
- Total Tests: [number]
- Passed: [number]
- Failed: [number]
- Blocked: [number]

## Test Results

### Connection Tests
| Test Case | Result | Notes |
|-----------|--------|-------|
| Valid API Key | ✅/❌ | |
| Invalid API Key | ✅/❌ | |
| Expired API Key | ✅/❌ | |

### Trading Tests
| Test Case | Result | Notes |
|-----------|--------|-------|
| Place Buy Limit | ✅/❌ | |
| Place Sell Limit | ✅/❌ | |
| Cancel Order | ✅/❌ | |
| Partial Fill | ✅/❌ | |
| Complete Fill | ✅/❌ | |

### Stability Tests
| Test Case | Result | Notes |
|-----------|--------|-------|
| 24hr Run | ✅/❌ | |
| Reconnection | ✅/❌ | |
| Rate Limiting | ✅/❌ | |

## Issues Found
1. [Issue description]
2. [Issue description]

## Recommendations
- [Recommendation 1]
- [Recommendation 2]
```

## Automated Testing

### Unit Test Coverage
Minimum required coverage: 80%

Run tests:
```bash
coverage run -m pytest test/hummingbot/connector/exchange/[connector_name]/
coverage report
```

### Integration Tests
```bash
pytest test/hummingbot/connector/exchange/[connector_name]/ -m integration
```

## Sign-off Criteria

Before approving connector for production:

- [ ] All connection tests pass
- [ ] All trading operations work correctly
- [ ] Fees calculated accurately
- [ ] 24-hour stability test passed
- [ ] Rate limits handled properly
- [ ] Error messages are clear and actionable
- [ ] Works with major strategies
- [ ] Unit test coverage > 80%
- [ ] Documentation complete
- [ ] No critical issues outstanding

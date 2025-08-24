# Perpetual Connector QA Testing Checklist

## Overview
This QA checklist extends the spot connector testing with additional requirements specific to perpetual futures trading, including leverage, positions, funding rates, and liquidations.

## Additional Test Requirements for Perpetuals

### 1. Position Management Testing

#### Open Position - Long
**Test Steps**:
1. Set leverage (e.g., 5x)
2. Place buy order
3. Wait for fill

**Expected Results**:
1. ✅ Position opened with correct size
2. ✅ Leverage applied correctly
3. ✅ Margin reserved properly
4. ✅ Position appears in `status`
5. ✅ Unrealized PNL updates with price changes

#### Open Position - Short
**Test Steps**:
1. Set leverage
2. Place sell order (without existing position)
3. Wait for fill

**Expected Results**:
1. ✅ Short position opened
2. ✅ Negative position size shown
3. ✅ Margin calculated correctly
4. ✅ Unrealized PNL updates inversely with price

#### Close Position
**Test Steps**:
1. Open a position
2. Place opposite order to close

**Expected Results**:
1. ✅ Position fully closed
2. ✅ Realized PNL recorded
3. ✅ Margin returned
4. ✅ Position removed from status

#### Reduce Position
**Test Steps**:
1. Open position (e.g., 1 BTC long)
2. Place smaller opposite order (e.g., sell 0.5 BTC)

**Expected Results**:
1. ✅ Position reduced correctly
2. ✅ Partial PNL realized
3. ✅ Remaining position tracked
4. ✅ Margin adjusted proportionally

### 2. Leverage Testing

#### Set Leverage
**Command**: Via configuration or API

**Test Cases**:
```python
# Test different leverage levels
leverage_levels = [1, 5, 10, 20, 50, 100]  # Based on exchange limits
```

**Expected Results**:
1. ✅ Leverage set successfully
2. ✅ Applied to new positions
3. ✅ Margin requirements updated
4. ✅ Maximum position size adjusted

#### Leverage Limits
**Test**: Try to set leverage beyond limits

**Expected Results**:
1. ✅ Error for leverage too high
2. ✅ Error for leverage too low (if applicable)
3. ✅ Clear error messages

### 3. Position Mode Testing

#### One-Way Mode
**Configuration**: Set position mode to one-way

**Test Cases**:
1. Open long position
2. Try to open short position

**Expected Results**:
1. ✅ Can only have one direction position
2. ✅ Opposite order closes/reduces position
3. ✅ Cannot have both long and short

#### Hedge Mode (if supported)
**Configuration**: Set position mode to hedge

**Test Cases**:
1. Open long position
2. Open short position simultaneously

**Expected Results**:
1. ✅ Both positions tracked separately
2. ✅ Independent margin for each
3. ✅ Separate PNL calculations
4. ✅ Can close independently

### 4. Funding Rate Testing

#### Funding Info Display
**Command**: Check funding information

**Expected Results**:
1. ✅ Current funding rate displayed
2. ✅ Next funding time shown
3. ✅ Funding interval correct
4. ✅ Updates periodically

#### Funding Payment
**Test**: Hold position through funding time

**Expected Results**:
1. ✅ Funding fee charged/received
2. ✅ Balance updated
3. ✅ Funding history recorded
4. ✅ Event logged properly

**Scenarios**:
- Long position with positive funding (pay fee)
- Long position with negative funding (receive fee)
- Short position with positive funding (receive fee)
- Short position with negative funding (pay fee)

### 5. Margin and Collateral Testing

#### Initial Margin
**Test**: Open positions with different sizes

**Expected Results**:
1. ✅ Initial margin calculated correctly
2. ✅ Includes fees
3. ✅ Respects leverage
4. ✅ Cannot exceed available balance

#### Maintenance Margin
**Test**: Monitor margin ratio

**Expected Results**:
1. ✅ Margin ratio displayed
2. ✅ Warning when approaching liquidation
3. ✅ Updates with price changes

#### Add/Remove Margin
**Test**: Adjust position margin (if supported)

**Expected Results**:
1. ✅ Can add margin to position
2. ✅ Can remove excess margin
3. ✅ Liquidation price adjusts

### 6. Order Types - Perpetual Specific

#### Reduce-Only Orders
**Test**: Place reduce-only orders

**Expected Results**:
1. ✅ Only reduces position
2. ✅ Cannot increase position
3. ✅ Cancelled if no position

#### Post-Only Orders
**Test**: Place post-only limit orders

**Expected Results**:
1. ✅ Order rejected if would take
2. ✅ Added to book as maker
3. ✅ Maker fees applied

#### Stop Loss Orders (if supported)
**Test**: Set stop loss on position

**Expected Results**:
1. ✅ Stop triggered at price
2. ✅ Position closed/reduced
3. ✅ Can modify stop price

#### Take Profit Orders (if supported)
**Test**: Set take profit on position

**Expected Results**:
1. ✅ Triggered at profit target
2. ✅ Position closed at limit price
3. ✅ Can modify target

### 7. Liquidation Testing (Testnet Only)

#### Approach Liquidation
**Test**: Use high leverage and adverse price movement

**Expected Results**:
1. ✅ Warning messages appear
2. ✅ Margin ratio updates
3. ✅ Liquidation price shown

#### Forced Liquidation
**Test**: Allow position to be liquidated

**Expected Results**:
1. ✅ Position closed automatically
2. ✅ Liquidation fee charged
3. ✅ Remaining collateral returned (if any)
4. ✅ Event logged clearly

### 8. Risk Management Testing

#### Position Limits
**Test**: Try to exceed position limits

**Expected Results**:
1. ✅ Maximum position size enforced
2. ✅ Clear error messages
3. ✅ Limits based on leverage

#### Maximum Orders
**Test**: Place many open orders

**Expected Results**:
1. ✅ Order limit enforced
2. ✅ Old orders cancelled if needed
3. ✅ Performance maintained

### 9. Mark Price vs Last Price

#### Price Tracking
**Test**: Monitor both prices

**Expected Results**:
1. ✅ Mark price displayed
2. ✅ Last price displayed
3. ✅ PNL uses mark price
4. ✅ Liquidation uses mark price

### 10. Cross vs Isolated Margin (if applicable)

#### Isolated Margin
**Configuration**: Set to isolated margin mode

**Expected Results**:
1. ✅ Margin isolated per position
2. ✅ Losses limited to position margin
3. ✅ Other positions unaffected

#### Cross Margin
**Configuration**: Set to cross margin mode

**Expected Results**:
1. ✅ All balance available as margin
2. ✅ Positions share collateral
3. ✅ Higher effective leverage possible

## Perpetual-Specific Performance Tests

### 11. WebSocket Data Streams

#### Position Updates
**Test**: Stream position changes

**Expected Results**:
1. ✅ Real-time position updates
2. ✅ PNL updates with price
3. ✅ Margin ratio updates
4. ✅ No missed updates

#### Funding Rate Updates
**Test**: Monitor funding changes

**Expected Results**:
1. ✅ Funding rate updates
2. ✅ Next funding time updates
3. ✅ Historical funding available

### 12. Strategy Compatibility - Perpetual

#### Test Perpetual Strategies
**Strategies**:
- Perpetual Market Making
- Spot-Futures Arbitrage
- Funding Rate Arbitrage

**Expected Results**:
1. ✅ Strategies handle positions correctly
2. ✅ Leverage utilized properly
3. ✅ Funding payments tracked

## Extended Test Report Template

```markdown
# Perpetual Connector QA Report

**Connector**: [connector_name_perpetual]
**Version**: [version]
**Tester**: [name]
**Date**: [date]
**Test Environment**: [Mainnet/Testnet]

## Summary
- Total Tests: [number]
- Passed: [number]
- Failed: [number]
- Blocked: [number]

## Perpetual-Specific Test Results

### Position Management
| Test Case | Result | Notes |
|-----------|--------|-------|
| Open Long Position | ✅/❌ | |
| Open Short Position | ✅/❌ | |
| Close Position | ✅/❌ | |
| Reduce Position | ✅/❌ | |
| Position Tracking | ✅/❌ | |

### Leverage & Margin
| Test Case | Result | Notes |
|-----------|--------|-------|
| Set Leverage | ✅/❌ | |
| Margin Calculation | ✅/❌ | |
| Margin Warnings | ✅/❌ | |
| Add/Remove Margin | ✅/❌ | |

### Funding
| Test Case | Result | Notes |
|-----------|--------|-------|
| Funding Rate Display | ✅/❌ | |
| Funding Payment (Long) | ✅/❌ | |
| Funding Payment (Short) | ✅/❌ | |
| Funding History | ✅/❌ | |

### Risk Management
| Test Case | Result | Notes |
|-----------|--------|-------|
| Position Limits | ✅/❌ | |
| Liquidation Warning | ✅/❌ | |
| Stop Loss | ✅/❌ | |
| Take Profit | ✅/❌ | |

### Position Modes
| Test Case | Result | Notes |
|-----------|--------|-------|
| One-Way Mode | ✅/❌ | |
| Hedge Mode | ✅/❌ | |
| Mode Switching | ✅/❌ | |

## Critical Issues
1. [Issue description]
2. [Issue description]

## Performance Metrics
- Position Update Latency: [ms]
- Funding Update Frequency: [seconds]
- Maximum Positions Handled: [number]
- WebSocket Stability (24hr): [uptime %]

## Recommendations
- [Recommendation 1]
- [Recommendation 2]
```

## Testing Best Practices

### Use Testnet First
1. Most perpetual exchanges provide testnet
2. Test liquidations safely
3. Test with high leverage
4. No real funds at risk

### Test Data Points

#### Critical Metrics to Monitor
- Position size and direction
- Unrealized PNL
- Realized PNL
- Margin ratio
- Liquidation price
- Funding rate
- Mark price vs Last price
- Available balance
- Position margin
- Order margin

### Common Issues to Check

1. **Position Direction Confusion**
   - Ensure long/short clearly indicated
   - Check sign conventions (+ or -)

2. **Leverage Not Applied**
   - Verify leverage affects position size
   - Check margin requirements

3. **Funding Miscalculation**
   - Verify payment direction
   - Check timestamp accuracy

4. **Liquidation Price Errors**
   - Compare with exchange UI
   - Test calculation formula

5. **PNL Calculation Issues**
   - Use mark price not last price
   - Include fees and funding

## Sign-off Criteria - Perpetual Specific

Additional requirements beyond spot connector:

- [ ] Position management fully functional
- [ ] Leverage system working correctly
- [ ] Funding rates accurate and timely
- [ ] Margin calculations correct
- [ ] Liquidation warnings functional
- [ ] Position modes working (one-way/hedge)
- [ ] PNL calculations accurate
- [ ] Reduce-only orders working
- [ ] Mark price tracked correctly
- [ ] All perpetual-specific events logged
- [ ] 24-hour position tracking test passed
- [ ] Funding payment test passed
- [ ] High leverage test completed (testnet)
- [ ] Documentation includes perpetual features

## Safety Recommendations

### Production Deployment
1. Start with low leverage (1-3x)
2. Use small position sizes initially
3. Monitor closely for first 24-48 hours
4. Have manual backup access ready
5. Set conservative stop losses
6. Monitor funding payments closely
7. Watch for liquidation warnings
8. Keep extra margin available

### Risk Parameters
- Maximum leverage: Start at 5x
- Position size limit: Start small
- Stop loss: Always use
- Margin buffer: Keep 2x maintenance margin
- Monitor frequency: Every 5 minutes initially

# Hyperliquid Fee Structure Update - January 2025

## Summary of Changes

The Hyperliquid fee configuration in the Hummingbot codebase was outdated and has been updated to reflect the current fee structure as documented in the [official Hyperliquid documentation](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees).

## Previous (Incorrect) Fee Configuration

The codebase previously had:
- **Maker Fee**: 0% (incorrectly assumed -0.02% rebate based on old documentation)
- **Taker Fee**: 0.025%

This caused the funding arbitrage controller to incorrectly calculate fees as 0% for opening positions when using limit orders.

## Current Fee Structure (As of January 2025)

### Perpetuals (Perps) Fee Tiers

| Volume Tier | 30-Day Volume | Taker Fee | Maker Fee |
|------------|---------------|-----------|-----------|
| Base | < $5M | 0.045% | 0.015% |
| Tier 1 | > $5M | 0.040% | 0.012% |
| Tier 2 | > $25M | 0.035% | 0.008% |
| Tier 3 | > $100M | 0.030% | 0.004% |
| Tier 4 | > $500M | 0.028% | 0.000% |
| Tier 5 | > $2B | 0.026% | 0.000% |
| Tier 6 | > $7B | 0.024% | 0.000% |

### Spot Fee Tiers

| Volume Tier | 30-Day Volume | Taker Fee | Maker Fee |
|------------|---------------|-----------|-----------|
| Base | < $5M | 0.070% | 0.040% |
| Tier 1 | > $5M | 0.060% | 0.030% |
| Tier 2 | > $25M | 0.050% | 0.020% |
| Tier 3 | > $100M | 0.040% | 0.010% |
| Tier 4 | > $500M | 0.035% | 0.000% |
| Tier 5 | > $2B | 0.030% | 0.000% |
| Tier 6 | > $7B | 0.025% | 0.000% |

**Note**: Spot volume counts double toward fee tier calculation.

### Maker Rebates

Additional maker rebates are available based on maker volume percentage:

| Rebate Tier | Maker Volume % | Rebate |
|-------------|----------------|--------|
| Tier 1 | > 0.5% | -0.001% |
| Tier 2 | > 1.5% | -0.002% |
| Tier 3 | > 3.0% | -0.003% |

## Files Updated

### 1. `/hummingbot/connector/derivative/hyperliquid_perpetual/hyperliquid_perpetual_utils.py`

**Changes made:**
```python
# Before:
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0.00025"),
    buy_percent_fee_deducted_from_returns=True
)

# After:
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.00015"),  # 0.015% maker fee (base tier)
    taker_percent_fee_decimal=Decimal("0.00045"),  # 0.045% taker fee (base tier)
    buy_percent_fee_deducted_from_returns=True
)

# Also updated testnet fees:
OTHER_DOMAINS_DEFAULT_FEES = {"hyperliquid_perpetual_testnet": [0.015, 0.045]}
```

### 2. `/hummingbot/connector/exchange/hyperliquid/hyperliquid_utils.py`

**Changes made:**
```python
# Before:
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0.00025"),
    buy_percent_fee_deducted_from_returns=True
)

# After:
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0004"),  # 0.040% maker fee (base tier)
    taker_percent_fee_decimal=Decimal("0.0007"),  # 0.070% taker fee (base tier)
    buy_percent_fee_deducted_from_returns=True
)

# Also updated testnet fees:
OTHER_DOMAINS_DEFAULT_FEES = {"hyperliquid_testnet": [0.040, 0.070]}
```

## Impact on Funding Arbitrage Strategy

The funding arbitrage controller calculates expected trading fees when evaluating opportunities. With the corrected fees:

1. **For Limit Orders (Maker)**:
   - Opening positions will now correctly show 0.015% fee instead of 0%
   - This more accurately reflects the true cost of entering positions

2. **For Market Orders (Taker)**:
   - Opening positions will show 0.045% fee (increased from 0.025%)
   - This ensures proper cost accounting for urgent fills

3. **Net Spread Calculations**:
   - The strategy will now properly account for the higher fees
   - Opportunities that appeared profitable with 0% maker fees may no longer be viable
   - More accurate profitability calculations will prevent unprofitable trades

## Configuration Notes

- The base tier fees are used as defaults in the configuration
- Users with higher trading volumes may want to override these fees in `/conf/conf_fee_overrides.yml`
- Maker rebates are not configured by default but can be added for high-volume traders

## Verification

To verify the changes are working:
1. Restart the bot after the code changes
2. Check the logs for fee estimates - they should show:
   - For hyperliquid_perpetual with LIMIT orders: `open=0.015%`
   - For hyperliquid_perpetual with MARKET orders: `open=0.045%`
3. The close fee will be a weighted average based on the configured close order type and reconciliation probability

## References

- [Hyperliquid Official Fee Documentation](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees)
- Issue discovered: January 3, 2025
- Fix implemented: January 3, 2025

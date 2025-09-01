# 🧪 Testing Guide: Controller + Executor Setup

## Prerequisites
1. Make sure you have API keys configured for:
   - Backpack Perpetual
   - Binance Perpetual
2. Have at least $50 in your account for testing

## Step 1: Start Hummingbot
```bash
cd /workspaces/CyberDeltaEngine/worktrees/hummingbot-funding-arb
./start
```

## Step 2: Connect to Exchanges (if not already connected)
```
connect backpack_perpetual
connect binance_perpetual
```

## Step 3: Check Balances
```
balance
```
Make sure you have at least $50 USDC on Backpack and $50 USDT on Binance.

## Step 4: Start the TEST Strategy
```
start --script v2_funding_arbitrage_with_controller.py --conf conf_v2_funding_rate_arb_with_controller_test.yml
```

## Step 5: Monitor the Strategy

### Check Status
```
status --live
```

This will show:
- Opportunity tiers (Premium 💎, Standard 🔶, Marginal ⚪)
- Top opportunities by priority score
- Active positions with PnL
- Any warnings (unhedged exposure, etc.)

### What to Look For:
1. **Opportunity Discovery**: Within 30 seconds, you should see opportunities listed
2. **Tier Classification**: Opportunities should be marked with tier indicators
3. **Position Creation**: If spread > 0.1%, positions should start opening
4. **Reconciliation**: Watch for reconciliation messages every 5 seconds

## Step 6: Test Scenarios

### A. Normal Operation (5-10 minutes)
- Let it run and watch for:
  - Opportunity scanning every 30 seconds
  - Position opening when good spreads found
  - Proper hedging (both legs filled)

### B. Test Stop Command
```
stop
```
- All positions should close automatically
- Watch for proper cleanup

### C. Test Emergency Scenarios (Optional)
1. **Create Unhedged Exposure**: Cancel one order manually on exchange
2. **Watch Reconciliation**: Should warn after 15 seconds, emergency action after 60 seconds

## Step 7: Review Logs
```bash
# Check latest log
tail -f logs/logs_conf_v2_funding_rate_arb_with_controller_test.log
```

Look for:
- `💎 PREMIUM opportunity discovered` - Premium opportunities found
- `⚠️ Unhedged exposure detected` - Reconciliation working
- `✅ Unhedged exposure resolved` - Successful hedging
- Performance reports every 30 seconds

## Moving to Production

Once testing is successful:

### 1. Gradually Increase Position Size
Edit `funding_arbitrage_controller_config_test.yml`:
- Week 1: `position_size_quote: 10` ✓
- Week 2: `position_size_quote: 25`
- Week 3: `position_size_quote: 50`

### 2. Add More Tokens
- Start: SOL only ✓
- Then add: BTC, ETH
- Finally: SUI, DOGE

### 3. Use Production Config
```
start --script v2_funding_arbitrage_with_controller.py --conf conf_v2_funding_rate_arb_with_controller.yml
```

## Troubleshooting

### No Opportunities Found
- Check funding rates manually on exchanges
- Lower `min_funding_rate_profitability` to 0.0005 (0.05%)
- Ensure markets are open and liquid

### Orders Not Filling
- Check order books for liquidity
- Consider using MARKET orders for testing
- Verify API permissions include trading

### Unhedged Exposure Warnings
- Normal during volatile markets
- Increase `exposure_warning_time` if too frequent
- Check network latency to exchanges

## Key Metrics to Track

1. **Opportunity Quality**
   - How many Premium vs Standard vs Marginal?
   - Average spread of opportunities

2. **Execution Quality**
   - Fill rate of orders
   - Time to hedge both legs
   - Slippage from expected prices

3. **Performance**
   - PnL per position
   - Win rate
   - Average holding time

## Safety Checklist

- [ ] Started with TEST config (small positions)
- [ ] Monitoring shows opportunities
- [ ] Positions open and close properly
- [ ] Reconciliation warnings work
- [ ] Stop command closes all positions
- [ ] No unhedged exposure > 60 seconds
- [ ] Logs show expected behavior

## Next Steps

After successful testing:
1. Review performance metrics
2. Adjust parameters based on results
3. Gradually scale up position sizes
4. Add more tokens
5. Consider adding third exchange (Hyperliquid)

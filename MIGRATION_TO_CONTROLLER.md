# Migration Guide: From Direct Strategy to Controller-Based Architecture

## Current Testing Phase
Using `custom_bp_bin_hl_v2_funding_rate_arb.py` for testing with simplified config.

## Why Migrate to Controller?
- **Portfolio Management**: Handle 5+ tokens with intelligent capital allocation
- **Risk Management**: Global exposure limits across all positions
- **Opportunity Ranking**: Trade only the best opportunities within capital limits
- **Better Architecture**: Separation of concerns (Controller = brain, Executor = muscle)

## Migration Steps

### Phase 1: Testing (Current)
```bash
# Use simplified test config
start --script custom_bp_bin_hl_v2_funding_rate_arb.py \
      --conf conf_v2_funding_rate_arb_test_simple.yml
```
- Test with 1-2 tokens only
- Small position sizes ($10-50)
- Verify executor behavior
- Check reconciliation logic

### Phase 2: Parallel Testing
Run both strategies in parallel (different accounts/tokens):
```bash
# Terminal 1: Direct strategy (BTC only)
start --script custom_bp_bin_hl_v2_funding_rate_arb.py \
      --conf conf_v2_funding_rate_arb_test_simple.yml

# Terminal 2: Controller strategy (ETH, SOL, etc.)
start --script v2_funding_arbitrage_with_controller.py \
      --conf conf_v2_funding_rate_arb_with_controller.yml
```

### Phase 3: Full Migration
Once testing complete, switch entirely to controller:
```bash
# Production with all tokens
start --script v2_funding_arbitrage_with_controller.py \
      --conf conf_v2_funding_rate_arb_with_controller.yml
```

### Phase 4: Cleanup
After successful migration:
1. Delete `scripts/custom_bp_bin_hl_v2_funding_rate_arb.py`
2. Delete test configs
3. Use only controller-based architecture

## Config Comparison

### Test Config (Simplified)
```yaml
tokens: [BTC]  # Just 1 token
position_size_quote: 10  # Small
leverage: 5  # Conservative
```

### Production Config (Controller)
```yaml
tokens: [BTC, ETH, SOL, SUI, DOGE]  # Multiple
position_size_quote: 50
max_total_exposure: 500  # Portfolio limit
max_positions: 5  # Concurrent limit
leverage: 10
```

## Key Differences

| Feature | Direct Strategy | Controller Strategy |
|---------|----------------|-------------------|
| Token Management | All tokens get positions | Best opportunities only |
| Capital Allocation | Fixed per token | Dynamic based on profitability |
| Risk Management | Per-position only | Portfolio-wide limits |
| Complexity | Simple | Advanced |
| Use Case | Testing, 1-3 tokens | Production, 3+ tokens |

## Timeline
1. **Week 1-2**: Test direct strategy with BTC only
2. **Week 3**: Parallel testing with controller
3. **Week 4**: Full migration to controller
4. **Week 5**: Delete direct strategy code

## Monitoring During Migration
- Compare PnL between strategies
- Check execution quality
- Monitor reconciliation events
- Track capital efficiency

## Rollback Plan
If issues with controller:
1. Stop controller strategy
2. Revert to direct strategy (keep code until Phase 4)
3. Debug controller issues
4. Retry migration

## Success Criteria
- [ ] Controller handles 5+ tokens efficiently
- [ ] Capital allocation working correctly
- [ ] No unhedged exposure issues
- [ ] Better PnL than direct strategy
- [ ] Stable for 1 week minimum

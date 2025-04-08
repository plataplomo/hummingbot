# Cross-Exchange Funding Rate Arbitrage Optimization

## Abstract

This document focuses on optimizing the **Cross-Exchange Funding Rate Arbitrage** strategy within the CyberDeltaEngine, specifically for trading across **Backpack (CEX), Hyperliquid (DEX), and Paradex (DEX)**. We explore techniques for optimizing trade execution (minimizing legging risk and slippage), managing collateral efficiently across exchanges, and refining position sizing considering the nuances of multi-exchange operations.

## Introduction

Cross-exchange funding rate arbitrage presents unique challenges beyond single-exchange strategies, primarily related to execution risk (legging) and collateral management. This document outlines optimization techniques to address these challenges when trading between Backpack, Hyperliquid, and Paradex.

## Execution Optimization

Minimizing the risk associated with executing two legs on different exchanges is paramount.

### Legging Risk Mitigation

**Legging Risk:** The risk that the price moves adversely between the execution of the first leg and the second leg of a cross-exchange arbitrage trade.

**Techniques:**
- **Simultaneous Order Placement:** Use APIs to send orders for both legs close in time (low latency needed).
- **Conditional Orders (If Supported):** Explore exchange-specific conditional orders (rare across different exchanges).
- **Aggressive Order Types:** Use IOC/FOK for the second leg (trade-off: slippage vs. execution guarantee).
- **Market-Making Logic (Second Leg):** Place second leg passively inside BBO for faster fill.
- **Partial Fill Handling:** Define logic for first-leg partial fills (cancel/resize second leg, retry first leg).

### Slippage Reduction

- **Liquidity-Aware Sizing:** Size constrained by the less liquid leg.
- **Order Splitting:** Break large orders into smaller ones.
- **Passive Execution (First Leg):** Use limit order for first leg (increases legging risk).
- **Dynamic NFD Threshold:** Require higher **cost-adjusted NFD** ($\mathbb{E}_t[\pi^{AB}_t]$ including $C^{AB}_t$) in volatile/illiquid conditions.

### Latency Minimization

- **Co-location/Proximity Hosting:** Place engine near CEX servers (Backpack).
- **Efficient API Usage:** Optimize calls, use WebSockets.
- **Latency Modeling:** Model $t_{exec} \approx t_{API} + t_{network} + t_{exchange}$. Use to estimate legging risk ($LC^{AB}$) based on short-term volatility and $t_{exec}$ difference.

## Collateral Management Optimization

Efficiently managing collateral across BP, HL, PX maximizes capital use and enables trades.

### Cross-Margining (If Available)

Check platform features (unlikely between separate CEX/DEXs).

### Dynamic Collateral Allocation

- Algorithm to calculate target collateral $C_{target}^X = M_{req}^X \times (1 + \text{Buffer})$ per exchange.
- Reduce target positions if $\sum C_{target}^X > C_{total}$.
- Calculate and execute optimal transfers $\Delta C^X$ considering costs ($\gamma_{transfer}$) and delays ($\tau_{transfer}$) for CEX<->DEX moves.

### Automated Collateral Transfer Logic

Automating transfers can optimize capital but needs care:

- **Triggers:** Initiate when $C_{current}^X$ significantly deviates from $C_{target}^X$ (outside buffer zones) or a trade requires capital.
- **Cost Estimation ($\gamma_{transfer}$):** Sum CEX withdrawal fees, network gas (Arbitrum/StarkNet), and potential bridge fees.
    - *Assumption:* Direct BP <-> HL/PX transfers feasible (e.g., USDC). API confirmation needed.
- **Delay Estimation ($\tau_{transfer}$):** Sum CEX approval time, network confirmation times (Arbitrum/StarkNet), and potential bridge delays.
- **Decision Logic:** Choose optimal path (direct vs. bridge, asset choice) minimizing cost and delay.
- **Security:** Secure API/private key management (e.g., multi-sig, custody).
- **Execution:** Requires interfacing with exchange APIs and wallet libraries/contracts.

### Yield Generation on Idle Collateral

- Explore yield options on exchanges (CEX savings) or DeFi integration (DEX, high risk).

## Position Sizing Refinements

Based on Kelly criterion from the main model.

- **Incorporate Execution Uncertainty:** Adjust $\mathbb{E}[NFD]$ down by expected costs, adjust $\sigma_{B^{AB}}$ up based on latency/volatility.
- **Correlated Risks:** Use portfolio covariance matrix $\Sigma$ covering all basis risks ($B^{AB}, B^{AC}, B^{BC}, B^X$).
- **Capital Constraints per Exchange:** Max size often limited by capital on one exchange, despite Kelly fraction. Dynamic allocation helps but isn't instant.

## Risk Management Enhancements

- **Portfolio Level Risk:** Monitor $\sigma_P = \sqrt{w^T \Sigma w}$.
- **VaR / CVaR:** Monitor standard risk metrics.
- **Dynamic Risk Limits (Future Enhancement):** Consider $\sigma_{max, t} = f(\sigma_{mkt, t})$ to adapt to market conditions.
- **Data Validation Checks:** Implement checks like $|P_{BP} - P_{HL}| < \text{Threshold}$. Define fallback logic for stale/bad data.

## Exchange-Specific Considerations

- **Backpack (CEX):** Standard CEX API behavior, latency, withdrawal process.
- **Hyperliquid (DEX):** Arbitrum L1 specifics, hourly funding, oracle risks.
- **Paradex (DEX - StarkNet L2):** StarkNet L2 specifics, settlement, API details.

## Conclusion

Optimizing cross-exchange funding rate arbitrage involves minimizing execution risks (legging, slippage) via smart logic, latency modeling, and liquidity awareness. Efficient dynamic, potentially automated, collateral management across BP, HL, and PX is key. Sizing must consider execution uncertainty and capital limits. Robust risk management, including data validation and potentially dynamic limits, is essential. 
# Funding Rate Arbitrage: Mathematical Framework

## Abstract

This document formalizes the mathematical framework for funding rate arbitrage strategies across perpetual futures markets. This revision incorporates advanced concepts based on critique: refined no-arbitrage bounds including execution costs, stochastic and adaptive modeling for funding rates and volatility, refined Kelly criterion, explicit modeling of basis risk and optimal dynamic hedging, and enhanced risk management considerations (e.g., CVaR).

## Introduction

Perpetual futures contracts use a funding rate mechanism to align prices with the underlying spot asset. This document develops a rigorous mathematical framework for exploiting funding rate inefficiencies, focusing on cross-exchange arbitrage and incorporating advanced modeling techniques.

## Perpetual Futures Pricing Model

### Notation and Assumptions

**Market Setup:**
Market includes: Spot price $S_t$, Futures price $F_t$, Risk-free rate $r^f$, Spot borrow/lend rate $r^a$.

**Market Frictions:**
Incorporated: Proportional fees $c_s, c_f$; Slippage $s(v,L)$; Hedge holding costs $r^a$; Execution latency $\tau_{exec}$.

### Funding Rate Mechanics

Payments occur at $t_k = k \Delta T$ based on the average premium/discount over the interval.

**Funding Rate (Generalized):**
Let $I_t$ be the index/oracle price. Funding rate at $t_{k+1}$ is:

$$FR_{k+1} = \text{Clamp}_t\left( \text{Avg}_{t \in [t_k, t_{k+1}]} \left( \frac{F_t - I_t}{I_t} \right) \cdot D + R \right)$$

where $D$ is dampening factor, $R$ interest component.

*Critique Suggestion:* The clamping function $\text{Clamp}_t(\cdot)$ should ideally be dynamic, adapting to market volatility $\sigma_{premium}$: $\text{Clamp Range} = \pm k \cdot \sigma_{\text{premium}}$.

**Funding Payment:**
Payment $FP_{k+1}$ for position size $q$ at $t_{k+1}$:

$$FP_{k+1} = q \cdot V_{k+1} \cdot FR_{k+1}$$

where $V_{k+1}$ is valuation price (mark/oracle).

*Critique Suggestion:* Use multi-oracle average or TWAP for $V_{k+1}$ (if based on oracle) to mitigate manipulation risk.

### No-Arbitrage Bounds with Costs

**Refined No-Arbitrage Bounds with Execution Costs:**
Cash-and-carry (buy spot, short future), incorporating execution costs (latency $\tau_{exec}$, slippage $s$) and spot costs $r^a$:

$$(F_t - s_{sell}) e^{-\tau_{exec} r^f} (1-c_f) \le (S_t + s_{buy}) (1+c_s) e^{(r^a - \mathbb{E}_t[\text{Avg}(FR_{pos})])\tau}$$

Reverse cash-and-carry (short spot, long future):

$$(F_t + s_{buy}) e^{-\tau_{exec} r^f} (1+c_f) \ge (S_t - s_{sell}) (1-c_s) e^{(r^a - \mathbb{E}_t[\text{Avg}(FR_{neg})])\tau}$$

where $\mathbb{E}_t[FR]$ represents the expected funding rate.

*Critique Suggestion:* Estimating $\mathbb{E}_t[FR]$ accurately is key; consider ML models (e.g., LSTM) for prediction.

## Single-Exchange Funding Rate Arbitrage

Capture funding rate $FR^X$ on a single exchange $X$ by holding a delta-neutral position (e.g., short Perp, long Spot).

**Basis Risk:** $B^X_t = F^X_t - S_t$. Risk is basis volatility $\sigma_{B^X,t}$.

**Expected Profit (Single Exchange):**

Over the next funding period:

$$\mathbb{E}_t[\pi^X_t] \approx \mathbb{E}_t[FR^X_{t+1}] \cdot \text{Size} - C^X_t$$

Where $C^X_t$ = Total Costs:
- Exchange Fees ($c^X$)
- Slippage ($s^X \approx \beta \cdot \text{Size} / \text{Liquidity}$)
- Hedge Costs (e.g., spot fees, borrowing $r^a$ if applicable)

**Optimal Position Sizing (Kelly):**

$$f^X_t \approx \frac{\mathbb{E}_t[\pi^X_t]}{\text{Var}_t(\pi^X_t)} \approx \frac{(\mathbb{E}_t[FR^X_{t+1}] - C^X_t / \text{Size}) \cdot S_t}{(\sigma_{B^X, t})^2}$$

Use fractional Kelly: $f^*_{actual} = \alpha \cdot f^X_t$.

## Cross-Exchange Funding Rate Arbitrage

Capture the difference in funding rates between two exchanges (A and B) by taking opposing perpetual positions.

**Net Funding Differential (NFD):**

$$NFD^{AB}_t = FR^A_t - FR^B_t$$

**Expected Profit (Cross-Exchange):**

Over the next funding period:

$$\mathbb{E}_t[\pi^{AB}_t] \approx \mathbb{E}_t[NFD^{AB}_{t+1}] \cdot \text{Size} - C^{AB}_t$$

Where $C^{AB}_t$ = Total Costs (Cost-Adjusted NFD Components):
- Exchange Fees ($c^A + c^B$)
- Combined Slippage ($s^A + s^B$)
- Latency Cost / Legging Risk ($LC^{AB}$) - function of delay & volatility
- Collateral Transfer Costs ($\gamma_{transfer}$) - if immediate rebalancing needed (usually separate)

**Cross-Exchange Basis Risk:** $B^{AB}_t = F^A_t - F^B_t$. Risk is basis volatility $\sigma_{B^{AB}, t}$.

**Optimal Position Sizing (Kelly):**

$$f^{AB}_t \approx \frac{\mathbb{E}_t[\pi^{AB}_t]}{\text{Var}_t(\pi^{AB}_t)} \approx \frac{(\mathbb{E}_t[NFD^{AB}_{t+1}] - C^{AB}_t / \text{Size}) \cdot S_t}{(\sigma_{B^{AB}, t})^2}$$

Use fractional Kelly: $f^*_{actual} = \alpha \cdot f^{AB}_t$.

## Hyperliquid Funding Rate Mechanisms

### Standard Perpetuals

Index price $I_t$ = Oracle Price (CEX median). Valuation $V_{k+1}$ likely Mark Price.

### Hyperps (Hyperliquid-only Perps)

Index price $I^{EMA}_t$ = 8-hour EMA of Hyperliquid mark prices.

$$FR_{k+1} = \text{Clamp}_t\left( \text{Avg}_{t \in [t_k, t_{k+1}]} \left( \frac{F_t - I^{EMA}_t}{I^{EMA}_t} \right) \cdot D + R \right)$$

EMA calculation (corrected interpretation):

$$I^{EMA}_t = \frac{\sum_{i=0}^{N-1} M_{t-i} w_i}{\sum_{i=0}^{N-1} w_i}, \quad w_i = e^{-i/\tau_{EMA}(t)}$$ 
*Equation: hyperp_ema_adaptive*

where $\tau_{EMA}(t)$ could be adaptive based on volatility $\sigma_t$. Safeguard cap $\le M_{initial} \times 4$ might be replaced by dynamic threshold (e.g., $\pm 3\sigma$). Dampening factor $D$ might be smaller.

## Modeling Considerations & Advanced Concepts

- **Stochastic/Adaptive Models:** Use OU, HMM, or ML (e.g., LSTM) for adaptive $\mathbb{E}_t[FR]$. Use GARCH/Stochastic Vol models for adaptive $\sigma_{B,t}$.
- **Optimal Dynamic Hedging:** Employ time-varying $\delta_t^*$.
- **Execution Costs:** Explicitly model variable fees, slippage $s(q,L)$, and latency costs in profit/NFD.
- **Risk Management:** Use CVaR alongside VaR. Consider Sortino/Omega ratios. Base position limits on portfolio risk ($\\sqrt{w^T \\Sigma w}$) and use fractional Kelly.
- **Parameter Uncertainty:** Use Bayesian estimation, shrinkage, robust optimization.

## Conclusion

This enhanced framework incorporates critique suggestions: adaptive parameters (funding rate expectations, volatility, EMA decay, clamping), explicit execution costs, advanced risk metrics (CVaR), and robust estimation notes. These refinements aim for a more realistic and robust representation of funding rate arbitrage, especially for dynamic crypto markets. 
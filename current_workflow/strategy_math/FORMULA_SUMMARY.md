# Mathematical Formulas in CyberDeltaEngine

This document provides a comprehensive reference for the key mathematical formulas implemented in the CyberDeltaEngine project. It includes explanations, LaTeX implementations, and reflects recent enhancements based on internal critique. \textbf{Focus is on the core rule-based implementation; ML formulas are detailed separately.}

## Hyperliquid-Specific Formulas

### Funding Rate Calculation

**Formula Explanation:**
Hyperliquid's 8-hour funding rate incorporates a premium index ($P$) and an interest component ($I$), clamped to prevent excessive rates. This is paid hourly (1/8th of the rate), capped at $\pm 4\%$. The critique suggested making the clamp range dynamic.

**LaTeX Implementation (with Dynamic Clamp):**
```latex
\begin{equation}
FR_{HL,t} = P_t + \text{clamp}(I - P_t, -k \sigma_{\text{premium}, t}, +k \sigma_{\text{premium}, t})
\end{equation}
```
where $\sigma_{\text{premium}, t}$ is the rolling volatility of the premium index $P$, and $k$ is a constant. The hourly rate is $FR_{HL,t}/8$, capped at $\pm 0.04$.

### Premium Calculation

**Formula Explanation:**
The premium index $P$ is based on the difference between impact bid/ask prices relative to the oracle price. Critique suggested non-linear transformation or weighting.

**LaTeX Implementation (Original):**
```latex
\begin{equation}
P = \frac{\text{impact\_price}}{\text{oracle\_price}} - 1
\end{equation}
\text{impact\_price} = \max(\text{impact\_bid\_px} - \text{oracle\_px}, 0) - \max(\text{oracle\_px} - \text{impact\_ask\_px}, 0)
```
*Note: Implementing non-linear transformations or weighting requires further research and backtesting.* 

### Funding Payment

**Formula Explanation:**
Funding payment uses position size ($q$), a valuation price ($V_t$, typically oracle or mark price depending on exchange), and the funding rate ($FR_t$). Critique suggested using multi-oracle averages or TWAP for the valuation price if based on oracle.

**LaTeX Implementation:**
```latex
\begin{equation}
FP_t = q \cdot V_t \cdot FR_t 
\end{equation}
```
*Note: Use $V_t = \frac{1}{n} \sum \text{OraclePrice}_i$ or $V_t = \text{TWAP}(OraclePrice)$ for robustness against single oracle manipulation.* 

### Oracle Price for Hyperps

**Formula Explanation:**
Hyperps use an 8-hour EMA of Hyperliquid mark prices as their oracle ($I^{EMA}_t$). Critique suggested adaptive decay and dynamic threshold instead of a hard cap.

**LaTeX Implementation (Adaptive EMA):**
```latex
\begin{equation}
I^{EMA}_t = \frac{\sum_{i=0}^{N-1} M_{t-i} w_i}{\sum_{i=0}^{N-1} w_i}, \quad w_i = e^{-i/\tau_{EMA}(t)}
\end{equation}
```
where $\tau_{EMA}(t)$ is adaptive (e.g., based on volatility $\sigma_t$). The cap $\le M_{initial} \times 4$ could be replaced by a dynamic threshold like $M_{avg} \pm 3 \sigma_{M}$.

## Collateral Management and Transfer Optimization

### Target Collateral Prediction (ML-Based)

**Formula Explanation:**
Predictive target collateral levels based on expected needs and volatility to pre-position funds. `k` is a risk tolerance factor (e.g., 1.5-2).

**LaTeX Implementation:**
```latex
\begin{equation}
C_{\text{target}} = \mathbb{E}[C_{\text{needed}}] + k \cdot \sigma_{C}
\end{equation}
```
*Note: $\mathbb{E}[C_{\text{needed}}]$ and $\sigma_{C}$ are derived from historical data or ML forecasts.*

### Transfer Decision Cost Function (Dynamic Path Selection)

**Formula Explanation:**
Optimizes transfer path selection by considering multiple factors: fees, execution/confirmation time, and risk (e.g., bridge liquidity, reliability). Weights $\alpha, \beta, \gamma$ represent priorities.

**LaTeX Implementation:**
```latex
\begin{equation}
\text{Cost} = \alpha \cdot \text{Fees} + \beta \cdot \text{Time} + \gamma \cdot \text{Risk}
\end{equation}
```
*Note: Fees = API_Fee + Gas; Time = $t_{exec} + t_{confirm}$; Risk = $f(\text{Liquidity}, \text{Reliability})$.*

### Transfer Urgency Prioritization

**Formula Explanation:**
Assigns priority to transfers based on deadlines, particularly relevant for time-sensitive opportunities like funding rate payments.

**LaTeX Implementation:**
```latex
\begin{equation}
\text{Priority} = w_{\text{urgency}} \cdot t_{\text{deadline}}
\end{equation}
```
*Note: $w_{\text{urgency}}$ can be a binary flag or weight based on how close the deadline is.*

### Automated Transfer Retry Logic (Size Reduction)

**Formula Explanation:**
If a transfer fails, automatically retry with progressively smaller sizes to increase the chance of success (e.g., due to liquidity constraints). $n$ is the retry attempt number.

**LaTeX Implementation:**
```latex
\begin{equation}
\text{Retry\_Size}_n = \frac{\Delta C}{2^n}
\end{equation}
```
*Note: Maximum retry attempts should be capped (e.g., $n_{max}=3$).*

### Interim Position Scaling (During Transfers)

**Formula Explanation:**
Adjust active position weights ($w_t$) proportionally to the current available collateral ($C_{\text{current}}$) relative to the target ($C_{\text{target}}$) while a transfer is in progress to manage risk during delays.

**LaTeX Implementation:**
```latex
\begin{equation}
w_t = w_{\text{target}} \cdot \left( \frac{C_{\text{current}}}{C_{\text{target}}} \right)
\end{equation}
```

## Statistical Arbitrage Models

### Hidden Markov Model (HMM) Dynamics

**Formula Explanation:**
The spread $X_t$ follows a regime-switching OU process. Critique suggested incorporating non-Gaussian noise.

**LaTeX Implementation (OU Process):**
```latex
\begin{equation}
dX_t = \kappa(Z_t)(\theta(Z_t) - X_t)dt + \sigma(Z_t)dW_t
\end{equation}
```
*Note: For heavy tails, replace $dW_t$ with Levy increment $dL_t$ or use Student-t innovations in the discretized version.* 

**Discretized Implementation (Student-t noise):**
```latex
\begin{equation}
X_{t+\Delta t} = X_t + \kappa(Z_t)(\theta(Z_t) - X_t)\Delta t + \sigma(Z_t)\sqrt{\Delta t}\, \nu_t
\end{equation}
```
where $\nu_t \sim t(\text{df})$ (Student's t-distribution with df degrees of freedom).

### EM Algorithm for HMM Parameter Estimation

**Formula Explanation:**
Iterative E-step and M-step to estimate $\Theta = \{\kappa_i, \theta_i, \sigma_i, \mathbf{Q}\}$. Critique suggested better initialization and exploring alternatives.

**LaTeX Implementation (M-Step Updates - Simplified):**
```latex
\begin{align}
\kappa_i^{(n+1)} &= \dots \\
(\sigma_i^{(n+1)})^2 &= \dots \\
q_{ij}^{(n+1)} &= \dots
\end{align}
```
*Note: Use k-means for initialization. Consider Variational Inference for scalability. Bayesian MCMC provides parameter uncertainty.* 

### Optimal Trading Thresholds (Adaptive)

**Formula Explanation:**
Thresholds $b_i, a_i$ depend on mean $\theta_i$, volatility $\sigma_i$, and multiplier $\lambda_i$. Critique suggested optimizing $\lambda_i$ considering variable costs.

**LaTeX Implementation (Thresholds):**
```latex
\begin{equation}
b_{i,t} = \theta_i - \lambda_{i,t}^* \sigma_{i,t} \quad \text{and} \quad a_{i,t} = \theta_i + \lambda_{i,t}^* \sigma_{i,t}
\end{equation}
```
where $\sigma_{i,t}$ can be a dynamic estimate (e.g., EWMA) and $\lambda_{i,t}^*$ is optimized via simulation incorporating variable costs $c_{total} = c_{prop} + s(q,L)$.

### Expected Profit (Single Exchange)

**Formula Explanation:**
Expected profit over next funding period includes expected funding rate $\mathbb{E}_t[FR^X_{t+1}]$ minus total costs $C^X_t$ (fees, slippage, hedge costs).

**LaTeX Implementation:**
```latex
\begin{equation}
\mathbb{E}_t[\pi^X_t] \approx \mathbb{E}_t[FR^X_{t+1}] \cdot \text{Size} - C^X_t
\end{equation}
```

### Net Funding Differential (NFD) & Expected Cross-Exchange Profit

**Formula Explanation:**
NFD is the raw difference in funding rates. Expected profit subtracts total costs $C^{AB}_t$ (fees, slippage $s^{AB}$, latency $LC^{AB}$, potential transfer costs $\gamma_{transfer}$).

**LaTeX Implementation (NFD Definition):**
```latex
\begin{equation}
NFD^{AB}_t = FR^A_t - FR^B_t
\end{equation}
```
**LaTeX Implementation (Expected Cross-Exchange Profit):**
```latex
\begin{equation}
\mathbb{E}_t[\pi^{AB}_t] \approx \mathbb{E}_t[NFD^{AB}_{t+1}] \cdot \text{Size} - C^{AB}_t
\end{equation}
```

**Actual Allocation:** $f_{actual} = \alpha \cdot f_t^*$ (e.g., $\alpha = 0.5$).

### Transfer-Constrained Kelly Sizing

**Formula Explanation:**
Further constrains the Kelly fraction based on the *actually available* collateral on the target exchange ($C_{\text{available}}^X$) and ensuring minimum collateral levels ($C_{\text{min}}^Y$) are maintained elsewhere, accounting for transfer delays.

**LaTeX Implementation:**
```latex
\begin{equation}
f_{\text{constrained}} = \min \left( f_{\text{actual}}, \frac{C_{\text{available}}^X}{S_t \cdot \text{Size}}, \frac{C_{\text{total}} - \sum_{Y \ne X} C_{\text{min}}^Y}{S_t \cdot \text{Size}} \right)
\end{equation}
```
*Note: This ensures trades don't exceed available capital or jeopardize minimum balances on other exchanges due to slow transfers.*

## Funding Rate Arbitrage Models

### Kelly Criterion (Adaptive & Fractional, Per Signal)

**Formula Explanation:**
Further constrains the Kelly fraction based on the *actually available* collateral on the target exchange ($C_{\text{available}}^X$) and ensuring minimum collateral levels ($C_{\text{min}}^Y$) are maintained elsewhere, accounting for transfer delays.

**LaTeX Implementation:**
```latex
\begin{equation}
f_{\text{constrained}} = \min \left( f_{\text{actual}}, \frac{C_{\text{available}}^X}{S_t \cdot \text{Size}}, \frac{C_{\text{total}} - \sum_{Y \ne X} C_{\text{min}}^Y}{S_t \cdot \text{Size}} \right)
\end{equation}
```
*Note: This ensures trades don't exceed available capital or jeopardize minimum balances on other exchanges due to slow transfers.*

### Opportunity Ranking (Utility Function)

**Formula Explanation:**
Rank potential arbitrage opportunities based on a utility function that balances expected net profit ($\pi$, after costs including slippage) against the risk, represented by basis volatility ($\sigma_B$). $\lambda$ is a risk aversion parameter (e.g., $\lambda=1$).

**LaTeX Implementation:**
```latex
\begin{equation}
U = \pi_{adj} - \lambda \cdot \sigma_{B,t}^2
\end{equation}
```
*Note: $\pi_{adj}$ is the expected profit after accounting for estimated execution costs like slippage.*

### Estimated Slippage (Simple Model)

**Formula Explanation:**
A simple model to estimate slippage cost based on order size relative to available order book depth. $\beta$ is a scaling factor (e.g., 0.1 based on critique example).

**LaTeX Implementation:**
```latex
\begin{equation}
\text{Slippage}^X \approx \beta \cdot \frac{\text{Order\_Size}}{\text{Depth}^X}
\end{equation}
```
*Note: This is a basic estimate; more complex models considering book shape may be used.*

## Risk Management

### Value-at-Risk (VaR) and CVaR

**Formula Explanation:**
VaR estimates potential loss at confidence level $\alpha$. Critique suggests using historical/Monte Carlo VaR and Conditional VaR (CVaR) for tail risk.

**LaTeX Implementation (Parametric VaR):**
```latex
\begin{equation}
\text{VaR}_\alpha = -\mu \Delta t + \sigma \sqrt{\Delta t} \Phi^{-1}(\alpha)
\end{equation}
```
**CVaR (Expected Shortfall):**
```latex
\begin{equation}
\text{CVaR}_\alpha = \mathbb{E}[-X | -X \ge \text{VaR}_\alpha]
\end{equation}
```
*Note: Prefer Historical Simulation VaR/CVaR or Filtered Historical Simulation for non-normal returns.* 

### Dynamic VaR Adjustment

**Formula Explanation:**
Adjust the baseline Value-at-Risk ($VaR_0$) based on the ratio of current market volatility ($\sigma_{mkt,t}$, e.g., 1h rolling window) to historical baseline volatility ($\sigma_{mkt,0}$), making risk limits adaptive.

**LaTeX Implementation:**
```latex
\begin{equation}
VaR_t = VaR_0 \times \left( \frac{\sigma_{mkt,t}}{\sigma_{mkt,0}} \right)
\end{equation}
```
*Note: $VaR_0$ could be set based on historical PnL percentile or parametric calculation.*

### Position Limits (Portfolio Risk-Based)

**Formula Explanation:**
Critique suggests basing limits on overall portfolio risk rather than static leverage caps.

**LaTeX Implementation (Portfolio Volatility):**
```latex
\begin{equation}
\sigma_P = \sqrt{w^T \Sigma w}
\end{equation}
```
Position sizes $w$ are constrained such that $\sigma_P \le \text{MaxPortfolioRisk}$.

### Risk-Adjusted Return Ratios

**Formula Explanation:**
Sharpe ratio is standard but assumes normality. Critique suggests Sortino or Omega ratio for skewed/heavy-tailed returns.

**LaTeX Implementation (Sharpe):**
```latex
\begin{equation}
SR = \frac{\mathbb{E}[R_p] - r_f}{\sigma_p}
\end{equation}
```
**Sortino Ratio:**
```latex
\begin{equation}
Sortino = \frac{\mathbb{E}[R_p] - r_f}{\sigma_{downside}}
\end{equation}
```

## Monitoring & Adaptation

### Funding Yield (FY)

**Formula Explanation:**
Measures the collective funding earned across all exchanges relative to the total capital allocated.

**LaTeX Implementation:**
```latex
\begin{equation}
FY_t = \frac{\sum_{X} \text{Funding}^X_t}{\sum_{X} \text{Capital}^X_t}
\end{equation}
```
*Note: Provides a normalized measure of funding capture efficiency.*

### Performance-Based Adaptation Trigger (Example)

**Formula Explanation:**
An example trigger for adapting strategy parameters (e.g., Kelly fraction $\alpha$). If the recent Sharpe Ratio falls below a threshold (e.g., 1), reduce aggressiveness.

**Logic:**
```
IF SharpeRatio_t < Threshold THEN
  alpha_new = alpha_old * AdjustmentFactor // e.g., 0.8
ENDIF
```
*Note: Specific adaptation rules require backtesting and calibration.*

---

This summary reflects the integration of critique points, aiming for more robust and adaptive mathematical models within the CyberDeltaEngine, focusing on the core rule-based implementation. 
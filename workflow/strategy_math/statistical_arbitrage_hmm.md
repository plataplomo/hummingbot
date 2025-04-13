# Statistical Arbitrage with Hidden Markov Models

## Abstract

This document formalizes the statistical arbitrage approach using Hidden Markov Models (HMMs) for cryptocurrency markets. We develop a mathematical foundation for identifying cointegrated cryptocurrency pairs, modeling their spread dynamics using regime-switching processes, and generating optimal trading signals. This framework is integrated with funding rate arbitrage to create sophisticated multi-asset strategies for the CyberDeltaEngine system. This revision incorporates refinements based on critique: fractional cointegration, non-Gaussian HMM dynamics, Bayesian estimation, dynamic/adaptive thresholding considering variable costs, advanced portfolio optimization, and robust links between spread regimes and funding rates.

## Introduction

Statistical arbitrage exploits temporary pricing inefficiencies between related assets. In cryptocurrency markets, these inefficiencies can be particularly pronounced due to market fragmentation, varying liquidity, and diverse trader behaviors. This document develops a rigorous mathematical framework for identifying and exploiting statistical arbitrage opportunities in cryptocurrency markets, with a particular focus on using Hidden Markov Models (HMMs) to capture regime-switching behavior.

## Cointegration Analysis for Cryptocurrency Pairs

### Notation and Assumptions

**Market Setup:**
We consider a market with:
- A collection of cryptocurrency price series $\mathbf{P}_t = (P_{1,t}, P_{2,t}, \ldots, P_{n,t})$
- Log prices $\mathbf{p}_t = (\log P_{1,t}, \log P_{2,t}, \ldots, \log P_{n,t})$
- A sampling frequency of $\Delta t$ (e.g., hourly, daily)

**Integration Order and Fractional Cointegration:**
Standard cointegration tests assume $I(1)$ series. However, crypto prices may exhibit long memory and be better described as fractionally integrated processes, $I(d)$ with $0 < d < 1$. If two $I(d)$ series, $p_{1,t}$ and $p_{2,t}$, form a linear combination $X_t = p_{1,t} - \beta p_{2,t}$ which is $I(d-b)$ with $b > 0$, they are fractionally cointegrated. This implies the spread $X_t$ is stationary or mean-reverting, but potentially with slower reversion than an $I(0)$ process. Tests for fractional cointegration (e.g., based on Geweke-Porter-Hudak estimator or wavelet analysis) should be considered alongside standard tests.

### Cointegration Testing Framework

**Cointegration:**
Two or more non-stationary time series $\mathbf{p}_t$ are cointegrated if there exists a linear combination (the cointegrating relationship) $\beta' \mathbf{p}_t$ that is stationary (or has a lower order of integration than the original series, in the fractional case).

The Johansen procedure (VECM) remains a standard tool for testing integer cointegration ($I(1) \rightarrow I(0)$).

*Note:* Cointegrating relationships in crypto markets might be unstable or exhibit non-linearities. Methods like Threshold Cointegration or time-varying parameter models (TVP-VECM) could potentially capture these dynamics better, although they increase complexity significantly.

### Cryptocurrency-Specific Cointegration

For cryptocurrencies, we particularly focus on:
- BTC and ETH as potential cointegrating pairs
- Layer-1 blockchains with similar technological properties
- Tokens within the same ecosystem or service category
- Stablecoins and their pegged values

## Hidden Markov Models for Spread Dynamics

### Regime-Switching Ornstein-Uhlenbeck Model (with extensions)

Let $X_t = \beta' \mathbf{p}_t$ be the (mean-centered) stationary spread. We model its dynamics using a regime-switching Ornstein-Uhlenbeck (OU) process:

$$dX_t = \kappa(Z_t)(\theta(Z_t) - X_t)dt + \sigma(Z_t)dW_t$$ 
*Equation: hmm_sde*

where:
- $Z_t$ is an unobserved continuous-time Markov chain governing the regime, with finite state space $\mathcal{S} = \{1, 2, \ldots, K\}$.
- $\kappa(Z_t) > 0$ is the mean-reversion speed in regime $Z_t$.
- $\theta(Z_t)$ is the mean level (equilibrium) in regime $Z_t$. (Often assumed $\theta(Z_t) = 0$ if $X_t$ is pre-centered).
- $\sigma(Z_t) > 0$ is the volatility in regime $Z_t$.
- $W_t$ is a standard Brownian motion.

*Advanced Modeling Considerations:*
- **Heavy Tails/Jumps (Critique Suggestion):** Use non-Gaussian innovations (e.g., Student's t-distribution for \(\varepsilon_t\) in discretization) or Levy processes ($dL_t$ instead of $dW_t$) for HMM-Jump-Diffusion models.
- **Volatility Clustering (Intra-Regime):** HMM-GARCH models.

### Transition Probability Matrix

The Markov chain $Z_t$ is characterized by its generator matrix $\mathbf{Q}$:

$$
\mathbf{Q} = 
\begin{pmatrix}
-q_{1} & q_{12} & \cdots & q_{1K} \\
q_{21} & -q_{2} & \cdots & q_{2K} \\
\vdots & \vdots & \ddots & \vdots \\
q_{K1} & q_{K2} & \cdots & -q_{K}
\end{pmatrix}
$$

where $q_{ij} \ge 0$ for $i \neq j$ is the instantaneous transition rate from state $i$ to state $j$, and $q_i = \sum_{j \neq i} q_{ij}$ is the rate of leaving state $i$.

### Discretized Model

For practical implementation with data sampled at intervals $\Delta t$, we use the Euler-Maruyama discretization of Eq. hmm_sde:

$$X_{t+\Delta t} \approx X_t + \kappa(Z_t)(\theta(Z_t) - X_t)\Delta t + \sigma(Z_t)\sqrt{\Delta t}\, \varepsilon_t$$ 
*Equation: hmm_discrete*

where $\varepsilon_t \sim \mathcal{N}(0, 1)$. The transition probability matrix for the discrete-time Markov chain over interval $\Delta t$ is $\mathbf{P}(\Delta t) = e^{\mathbf{Q} \Delta t}$.

## Parameter Estimation

### EM Algorithm (Baseline)

Given observations $\mathcal{X}_T = \{X_0, X_{\Delta t}, \dots, X_{T\Delta t}\}$, the goal is to estimate the parameters $\Theta = \{\kappa_i, \theta_i, \sigma_i, \mathbf{Q}\}_{i=1}^K$. The EM algorithm iteratively performs:

- **E-step**: Compute filtered probabilities $\pi_t^i = \mathbb{P}(Z_t = i \mid \mathcal{X}_t)$ and smoothed probabilities $\gamma_t^i = \mathbb{P}(Z_t = i \mid \mathcal{X}_T)$. Also compute joint smoothed probabilities $\xi_{t,t+\Delta t}^{ij} = \mathbb{P}(Z_t = i, Z_{t+\Delta t} = j \mid \mathcal{X}_T)$. This typically involves forward-backward algorithms (like Baum-Welch).
- **M-step**: Update parameters $\Theta^{(n+1)}$ to maximize the expected complete-data log-likelihood given $\Theta^{(n)}$ and the computed probabilities.

### EM Algorithm M-Step Updates

The M-step updates for the HMM-OU model (assuming $\theta_i=0$) are approximately:

$$\kappa_i^{(n+1)} = \frac{\sum_{t=0}^{T-1} \gamma_t^i (X_{t+\Delta t} - X_t) X_t}{\sum_{t=0}^{T-1} \gamma_t^i X_t^2 (-\Delta t)}$$
$$(\sigma_i^{(n+1)})^2 = \frac{\sum_{t=0}^{T-1} \gamma_t^i (X_{t+\Delta t} - X_t - \kappa_i^{(n+1)}(- X_t)\Delta t)^2}{\sum_{t=0}^{T-1} \gamma_t^i \Delta t}$$
$$q_{ij}^{(n+1)} = \frac{\sum_{t=0}^{T-1} \xi_{t,t+\Delta t}^{ij}}{\sum_{t=0}^{T-1} \gamma_t^i \Delta t} \quad (i \neq j)$$

*Note:* These are approximations based on the discretization (Eq. hmm_discrete). More complex update formulas exist for exact likelihood maximization of the continuous-time process. Ensure the implementation uses validated EM update steps for the chosen model specification.

*Advanced Estimation Techniques:*
- **Bayesian Methods (MCMC):** Use Markov Chain Monte Carlo (MCMC) methods (e.g., Gibbs sampling) to estimate parameters and obtain full posterior distributions. This naturally incorporates parameter uncertainty and allows for more complex model specifications (like HMM-GARCH or models with jumps).
- **Particle Filtering/SMC:** For highly non-linear or non-Gaussian state-space models (if moving beyond HMM-OU), Sequential Monte Carlo (particle filtering) methods might be necessary for online state estimation and parameter learning.

## Trading Signal Generation

### Dynamic and Adaptive Trading Thresholds

Static thresholds based on regime parameters ($b_i = \theta_i \pm \lambda_i \sigma_i$) might be suboptimal. Consider dynamic thresholds that adapt to recent volatility or spread behavior.

**Dynamic Threshold Example: Bollinger Bands within Regimes**
Instead of a fixed $\lambda_i \sigma_i$, use a moving standard deviation of the spread *within the periods identified as regime i*, or use a regime-specific EWMA volatility $\sigma_{i,t}$:

$$b_{i,t} = \theta_i - \lambda_i \sigma_{i,t} \quad \text{and} \quad a_{i,t} = \theta_i + \lambda_i \sigma_{i,t}$$

where $\sigma_{i,t}$ is an estimate of volatility conditional on being in regime $i$ at time $t$. The probability-weighted threshold $b_t = \sum \pi_t^i b_{i,t}$ would then adapt more quickly to changing market conditions within regimes.

*Optimal Stopping Framework:* Rigorous threshold determination involves solving an optimal stopping problem, maximizing expected profit considering costs and the stochastic evolution of the spread and regimes. This is mathematically complex but provides a theoretical benchmark. Research often uses numerical methods (e.g., dynamic programming, finite difference methods for the associated Hamilton-Jacobi-Bellman equation) to approximate solutions.

### Trading Strategy Algorithm

**Algorithm: HMM-Based Statistical Arbitrage Strategy**
1. Initialize parameters: HMM with $K$ states, transaction costs $c_{\text{entry}}, c_{\text{exit}}$, threshold multipliers $\lambda_i$ (or single $\lambda$)
2. Estimate HMM parameters $\hat{\Theta}$ using historical data
3. LOOP:
    1. Observe spread $X_t = \beta' \mathbf{p}_t$
    2. Update filtered state probabilities $\pi_t^i = \mathbb{P}(Z_t = i \mid \mathcal{F}_t; \hat{\Theta})$
    3. Compute regime-dependent parameters $\hat{\theta}_i, \hat{\sigma}_i, \hat{\kappa}_i$
    4. Calculate regime-dependent thresholds $b_{i,t} = \hat{\theta}_i - \lambda_i \hat{\sigma}_i$ and $a_{i,t} = \hat{\theta}_i + \lambda_i \hat{\sigma}_i$
    5. Calculate probability-weighted thresholds: $b_t = \sum_{i=1}^K \pi_t^i b_{i,t}$ and $a_t = \sum_{i=1}^K \pi_t^i a_{i,t}$
    6. Calculate probability-weighted mean: $\theta_t = \sum_{i=1}^K \pi_t^i \hat{\theta}_i$
    7. IF position = 0 and $X_t < b_t$:
        - Enter long position in spread
    8. ELSIF position = 0 and $X_t > a_t$:
        - Enter short position in spread
    9. ELSIF position = 1 and $X_t > \theta_t$ *Exit rule: Reversion to probability-weighted mean*:
        - Exit long position
    10. ELSIF position = -1 and $X_t < \theta_t$ *Exit rule: Reversion to probability-weighted mean*:
        - Exit short position

## Multi-Asset Integration

### Combined Strategy Framework

Integrating statistical arbitrage (StatArb) and funding rate arbitrage (FundArb) requires a method to combine their signals or expected returns.

**Strategy Score:**
For each potential trade $(i,j)$ (e.g., pair spread, single asset funding), define a score:

$$S_{i,j} = w_1 \cdot S_{i,j}^{\text{StatArb}} + w_2 \cdot S_{i,j}^{\text{FundArb}}$$

where $S^{\text{StatArb}}$ could be related to the spread's deviation from its expected value normalized by volatility $(X_t - \theta_t) / \sigma_t$, and $S^{\text{FundArb}}$ could be the expected net funding rate. Weights $w_1, w_2$ require careful calibration.

### Advanced Portfolio Optimization

Beyond Mean-CVaR and Robust Optimization, consider:
- **Factor Models:** Model strategy returns based on underlying risk factors (e.g., market volatility, funding rate levels, liquidity). Portfolio construction then focuses on optimizing exposure to desirable factors while hedging unwanted ones.
- **Hierarchical Risk Parity (HRP):** A machine learning-based approach that uses graph theory and clustering to allocate capital, potentially more robust to estimation errors in the covariance matrix than traditional MVO.
- **Reinforcement Learning (RL):** Train an RL agent to directly optimize portfolio allocation decisions based on market state (including HMM probabilities, funding rates, volatility measures) to maximize a reward function (e.g., risk-adjusted return). This can capture complex, non-linear relationships.

## Regime-Adaptive Funding Rate Arbitrage

### Regime-Dependent Funding Rate Expectations

Modeling funding rates $FR_{i,t}$ conditioned on the spread regime $Z_t$ of a *specific pair*:

$$FR_{i,t} = \mu_{FR}(Z_t) + \epsilon_{i,t}$$

*Refined View:* As noted previously, directly linking a specific pair's spread regime ($Z_t$) to funding rates ($FR_{i,t}$) is questionable. A more advanced approach involves multi-variate models:

- **Multivariate HMM:** Model the joint dynamics of the spread $X_t$ and relevant funding rates $FR_{i,t}$ within a single HMM framework. The hidden state $Z_t$ would then capture regimes affecting *both* spread behavior and funding levels/differentials. This explicitly models the potential correlation between market regimes influencing spreads and funding.
- **Factor-Based Regime Model:** Define regimes based on broader market factors (e.g., VIX, aggregated funding levels, liquidity indicators). Both spread parameters ($\kappa, \theta, \sigma$) and funding rate expectations ($\mu_{FR}$) could then be conditioned on this common market regime factor.

## Model Calibration and Validation

### Model Selection Framework

The selection of the optimal number of regimes and other model parameters is critical for the performance of the HMM-based trading strategy. We implement the following procedure:

**Algorithm: Model Selection Framework**
1. Define candidate models $\mathcal{M} = \{M_1, M_2, \ldots, M_m\}$ with varying numbers of regimes (typically 2-5)
2. Partition historical data into training set $\mathcal{D}_{train}$ and validation set $\mathcal{D}_{val}$
3. For each model $M_k \in \mathcal{M}$:
    - Train $M_k$ on $\mathcal{D}_{train}$ using EM algorithm
    - Evaluate performance on $\mathcal{D}_{val}$ using metrics like log-likelihood, AIC, BIC, and backtested PnL
4. Select the best model based on validation performance

### Out-of-Sample Testing

- Use a separate out-of-sample test set $\mathcal{D}_{test}$ for final evaluation
- Employ walk-forward validation to simulate live trading conditions

## Advanced Extensions (Summary)

- Cointegration: Fractional, Threshold, Time-Varying Parameter models.
- Spread Dynamics: HMM-Jump-Diffusion, HMM-GARCH.
- Estimation: Bayesian MCMC, Particle Filters.
- Trading Signals: Dynamic Thresholds (e.g., regime-conditional volatility), Optimal Stopping frameworks.
- Portfolio Optimization: Mean-CVaR, Robust Opt., Factor Models, HRP, Reinforcement Learning.
- Integrated Modeling: Multivariate HMMs for spreads and funding, Factor-based regime models.

## Conclusion

This document has refined the mathematical framework for HMM-based statistical arbitrage, incorporating advanced concepts potentially found in recent academic literature. Key enhancements include considering fractional cointegration, non-Gaussian/GARCH/jump dynamics for spreads, Bayesian estimation, dynamic thresholding, advanced portfolio optimization techniques (Mean-CVaR, RL), and more sophisticated ways to model the interaction between spreads and funding rates (Multivariate HMMs). While increasing complexity, these extensions aim to better capture the intricate dynamics of cryptocurrency markets. 
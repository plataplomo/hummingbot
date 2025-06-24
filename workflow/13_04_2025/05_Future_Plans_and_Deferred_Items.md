# CyberDeltaEngine - Future Plans and Deferred Items (as of 2025-04-13 ~21:13 UTC-5)

## Planned Upcoming Phases/Features

*   **Complete Foundational Stability & Testing:** Finish resolving `mypy` and `ruff` errors in `cyberdelta/core/`, `tests/core/`, and `tests/unit/`. Ensure core logic is robust and well-typed.
*   **Integration Testing:** Develop and run integration tests covering interactions between core components (DataHandler, PortfolioTracker, ExecutionHandler, StrategyManager).
*   **API Adapter Refinement:** Review and potentially refactor `cyberdelta/apis/` modules (`backpack.py`, `hyperliquid.py`) for consistency, error handling, and alignment with base class definitions (e.g., ensuring `get_order_status` is handled correctly).
*   **Strategy Implementation (Funding Rate Arb):** Complete and test the initial funding rate arbitrage strategy (`cyberdelta/strategies/funding_rate_arbitrage.py`).
*   **End-to-End Testing (Prototype):** Conduct basic end-to-end tests for the v0.0.1 prototype (Hyperliquid/Backpack funding rate arbitrage).

## Explicitly Deferred Items

*   **Advanced Risk Management:** Implementation of more sophisticated risk controls beyond basic checks (e.g., Value-at-Risk (VaR), advanced Kelly criterion sizing, correlation limits across multiple strategies).
*   **Comprehensive CI/CD Pipeline:** Setting up a full continuous integration and deployment pipeline with automated testing, linting, building, and deployment steps.
*   **Database Integration:** Implementing robust database persistence for trades, portfolio snapshots, performance metrics, etc. (beyond simple file-based state).
*   **Advanced Monitoring/Dashboard:** Building a more comprehensive real-time monitoring dashboard beyond basic logging/metrics.
*   **Multi-Strategy Support:** Full implementation and testing framework for running multiple, potentially conflicting, strategies concurrently.
*   **Additional Strategies:** Development of strategies beyond the initial funding rate arbitrage (e.g., statistical arbitrage, ML-based signals).
*   **Additional Exchange Integrations:** Adding support for exchanges beyond Hyperliquid and Backpack.
*   **Refining `Any` Types:** Replacing placeholder `Any` types (e.g., in `handle_position_discrepancy`) with more specific types once requirements are clearer.

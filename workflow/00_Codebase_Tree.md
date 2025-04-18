# CyberDeltaEngine Codebase Tree (Analyzed Scope)

```text
.
├── main.py
├── cyberdelta/
│   ├── __init__.py
│   ├── apis/
│   │   ├── __init__.py
│   │   ├── backpack_old.py  # Note: Likely deprecated, will assess relevance
│   │   ├── backpack.py
│   │   ├── base.py
│   │   ├── errors.py
│   │   ├── hyperliquid.py
│   │   ├── rate_limiter.py
│   │   └── models/
│   │       ├── __init__.py
│   │       ├── api.py
│   │       └── enums.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── config_manager.py
│   │   ├── secrets_manager.py
│   │   └── settings.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── backtesting.py
│   │   ├── balance_monitor.py
│   │   ├── data_handler.py
│   │   ├── data_manager.py
│   │   ├── engine.py
│   │   ├── execution_handler.py
│   │   ├── portfolio_tracker.py
│   │   ├── results.py
│   │   ├── risk_manager.py
│   │   ├── signal_generator.py
│   │   ├── signal_queue.py
│   │   ├── strategy_manager.py
│   │   ├── strategy.py
│   │   ├── symbol_mapper.py
│   │   ├── trade_executor.py
│   │   ├── backtesting/
│   │   │   └── results.py
│   │   ├── execution/
│   │   │   ├── __init__.py
│   │   │   └── synchronized_order_submission.py
│   │   └── models/
│   │       ├── __init__.py
│   │       ├── enums.py
│   │       ├── market.py
│   │       ├── portfolio.py
│   │       └── strategy.py
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── dashboard_integration.py
│   │   ├── performance_metrics.py
│   │   ├── performance_tracker.py
│   │   ├── persistence.py
│   │   ├── real_time_dashboard.py
│   │   └── simplified_performance_tracker.py
│   ├── strategies/
│   │   └── funding_rate_arbitrage.py
│   ├── testing/
│   │   └── data_generation.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── constants.py
│   │   ├── logging_config.py
│   │   ├── parsing.py
│   │   ├── serialization.py
│   │   └── state_manager.py
│   ├── validation/
│   │   ├── __init__.py
│   │   ├── circuit_breaker.py
│   │   ├── funding_data.py
│   │   ├── funding_rate_validator.py
│   │   ├── multi_tier_funding_provider.py
│   │   └── position_reconciliation.py
│   └── visualization/
│       ├── __init__.py
│       ├── performance_visualizer.py
│       └── simplified_visualizer.py
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_config.py
    ├── config/
    │   ├── __init__.py
    │   └── test_config_consistency.py
    ├── core/
    │   ├── __init__.py
    │   ├── test_data_manager.py
    │   ├── test_execution_handler.py
    │   ├── test_portfolio_tracker.py
    │   ├── test_signal_generator.py
    │   ├── test_signal_queue.py
    │   └── execution/
    │       ├── __init__.py
    │       └── test_synchronized_order_submission.py
    ├── failure/  # Note: Directory exists, but no .py files listed
    ├── integration/
    │   ├── __init__.py
    │   ├── conftest.py
    │   ├── test_backtesting.py
    │   ├── test_core_workflow.py
    │   ├── test_failure_scenarios.py
    │   ├── test_safety_systems.py
    │   └── mocks/
    │       └── mock_exchange.py
    ├── strategies/
    │   ├── __init__.py
    │   └── test_funding_rate_arbitrage.py
    ├── unit/
    │   ├── __init__.py
    │   ├── conftest.py
    │   ├── test_backpack_api.py
    │   ├── test_backtest_engine.py
    │   ├── test_config_example.py
    │   ├── test_config_security.py
    │   ├── test_data_handler.py
    │   ├── test_execution_handler.py
    │   ├── test_hyperliquid_api.py
    │   ├── test_performance_visualizer.py
    │   ├── test_portfolio_tracker.py
    │   ├── test_position_sizing_integration.py
    │   ├── test_signal_queue.py
    │   ├── test_simplified_visualizer.py
    │   ├── test_strategy_manager.py
    │   ├── core/
    │   │   └── test_symbol_mapper.py
    │   └── risk/
    │       ├── __init__.py
    │       ├── conftest.py
    │       ├── test_risk_manager_init.py
    │       ├── test_rm_constraints.py
    │       ├── test_rm_controls.py
    │       ├── test_rm_dependencies.py
    │       ├── test_rm_sizing_simple.py
    │       ├── test_rm_sizing_standard.py
    │       └── test_rm_validation.py
    └── validation/
        ├── __init__.py
        ├── test_circuit_breaker.py
        ├── test_funding_rate_validator.py
        ├── test_multi_tier_funding_provider.py
        └── test_position_reconciliation.py

```
This tree represents the Python files within `main.py`, `cyberdelta/`, and `tests/` based on the `list_files` results. `.yaml`, `.sh`, and other non-Python files within these directories are excluded from this specific tree view but exist in the project. `__init__.py` files are included as they are Python files, but detailed analysis in Part 2 will focus on those with significant logic.
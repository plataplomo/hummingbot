# CyberDeltaEngine Codebase Tree (v0.0.1)

This tree includes all Python files in `main.py`, `cyberdelta/`, and `tests/` as required for the comprehensive audit.

```
.
├── main.py
├── cyberdelta/
│   ├── __init__.py
│   ├── apis/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── backpack.py
│   │   ├── backpack_old.py
│   │   ├── errors.py
│   │   ├── hyperliquid.py
│   │   ├── models/
│   │   └── rate_limiter.py
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
│   │   ├── strategy.py
│   │   ├── strategy_manager.py
│   │   ├── symbol_mapper.py
│   │   ├── trade_executor.py
│   │   └── models/
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── dashboard_integration.py
│   │   ├── performance_metrics.py
│   │   ├── performance_tracker.py
│   │   ├── persistence.py
│   │   ├── real_time_dashboard.py
│   │   └── simplified_performance_tracker.py
│   ├── strategies/
│   │   ├── funding_rate_arbitrage.py
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
    ├── failure/
    ├── integration/
    │   ├── __init__.py
    │   ├── conftest.py
    │   ├── test_backtesting.py
    │   ├── test_core_workflow.py
    │   ├── test_failure_scenarios.py
    │   └── test_safety_systems.py
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
    │   ├── test_signal_queue.py
    │   ├── test_simplified_visualizer.py
    │   ├── test_strategy_manager.py
    │   ├── test_position_sizing_integration.py
    │   └── core/
    │       └── test_symbol_mapper.py
    │   └── risk/
    │       ├── __init__.py
    │       ├── conftest.py
    │       ├── test_rm_constraints.py
    │       ├── test_rm_controls.py
    │       ├── test_rm_dependencies.py
    │       ├── test_rm_sizing_simple.py
    │       ├── test_rm_sizing_standard.py
    │       ├── test_rm_validation.py
    │       └── test_risk_manager_init.py
    └── validation/
        ├── __init__.py
        ├── test_circuit_breaker.py
        ├── test_funding_rate_validator.py
        ├── test_multi_tier_funding_provider.py
        ├── test_position_reconciliation.py
```
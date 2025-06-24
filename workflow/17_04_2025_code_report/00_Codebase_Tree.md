# CyberDeltaEngine Codebase Tree (v0.0.1 - Updated December 2025)

This tree includes all Python files in `main.py`, `cyberdelta/`, and `tests/` with recent architectural improvements and security enhancements.

```
.
├── main.py
├── cyberdelta/
│   ├── __init__.py
│   ├── apis/
│   │   ├── __init__.py
│   │   ├── base/
│   │   │   ├── __init__.py
│   │   │   ├── authenticator.py
│   │   │   ├── client_factory.py
│   │   │   ├── client_handler.py
│   │   │   ├── components.py
│   │   │   ├── errors.py
│   │   │   ├── exchange_api.py
│   │   │   ├── mapper.py
│   │   │   ├── rate_limiter.py
│   │   │   ├── request_handler.py
│   │   │   ├── response_handler.py
│   │   │   └── transformer.py
│   │   ├── backpack/
│   │   │   ├── __init__.py
│   │   │   ├── bp_api.py
│   │   │   ├── bp_api_components_factory.py
│   │   │   ├── bp_authenticator.py
│   │   │   ├── bp_config.py
│   │   │   ├── bp_data_transformer.py
│   │   │   ├── bp_mapper.py
│   │   │   ├── bp_market_service.py
│   │   │   ├── bp_trading_service.py
│   │   │   └── bp_websocket_client.py
│   │   ├── connectivity/
│   │   │   ├── __init__.py
│   │   │   ├── http_client.py
│   │   │   └── websocket_client.py
│   │   ├── decorators/
│   │   │   ├── __init__.py
│   │   │   ├── rate_limiting.py
│   │   │   └── security.py
│   │   ├── hyperliquid/
│   │   │   ├── __init__.py
│   │   │   ├── hl_api.py
│   │   │   ├── hl_api_components_factory.py
│   │   │   ├── hl_asset_indexer.py
│   │   │   ├── hl_authenticator.py
│   │   │   ├── hl_config.py
│   │   │   ├── hl_data_transformer.py
│   │   │   ├── hl_mapper.py
│   │   │   ├── hl_market_service.py
│   │   │   ├── hl_request_weighter.py
│   │   │   ├── hl_trading_service.py
│   │   │   └── hl_websocket_client.py
│   │   ├── models/
│   │   │   ├── __init__.py
│   │   │   ├── bp_raw_models.py
│   │   │   ├── hl_raw_models.py
│   │   │   └── response_base.py
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
│   │   ├── execution/
│   │   │   ├── __init__.py
│   │   │   ├── execution_handler.py
│   │   │   ├── order_manager.py
│   │   │   └── orders/
│   │   │       ├── __init__.py
│   │   │       ├── limit_order.py
│   │   │       └── market_order.py
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
│   │       ├── __init__.py
│   │       ├── account.py
│   │       ├── asset.py
│   │       ├── constants.py
│   │       ├── enums.py
│   │       ├── market/
│   │       │   ├── __init__.py
│   │       │   ├── candle.py
│   │       │   ├── funding_rate.py
│   │       │   ├── order_book.py
│   │       │   ├── ticker.py
│   │       │   └── trade.py
│   │       ├── order.py
│   │       ├── position.py
│   │       └── signal.py
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── dashboard_integration.py
│   │   ├── performance_metrics.py
│   │   ├── performance_tracker.py
│   │   ├── persistence.py
│   │   ├── real_time_dashboard.py
│   │   └── simplified_performance_tracker.py
│   ├── strategies/
│   │   ├── __init__.py
│   │   └── funding_rate_arbitrage.py
│   ├── testing/
│   │   ├── __init__.py
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
    ├── fixtures/
    │   ├── __init__.py
    │   ├── config_fixtures.py
    │   ├── market_data_fixtures.py
    │   ├── raw_api_data/
    │   │   ├── __init__.py
    │   │   ├── backpack_raw_data.py
    │   │   └── hyperliquid_raw_data.py
    │   └── time_fixtures.py
    ├── integration/
    │   ├── __init__.py
    │   ├── apis/
    │   │   ├── __init__.py
    │   │   ├── backpack/
    │   │   │   ├── __init__.py
    │   │   │   ├── conftest.py
    │   │   │   ├── test_bp_account_perp_positive_balance.py
    │   │   │   ├── test_bp_account_perp_zero_balance.py
    │   │   │   ├── test_bp_account_spot_positive_balance.py
    │   │   │   ├── test_bp_account_spot_zero_balance.py
    │   │   │   ├── test_bp_market_perp.py
    │   │   │   ├── test_bp_market_spot.py
    │   │   │   ├── test_bp_trading_perp.py
    │   │   │   └── test_bp_trading_spot.py
    │   │   └── hyperliquid/
    │   │       ├── __init__.py
    │   │       ├── conftest.py
    │   │       ├── test_hl_account_perp_positive_balance.py
    │   │       ├── test_hl_account_perp_zero_balance.py
    │   │       ├── test_hl_account_spot_positive_balance.py
    │   │       ├── test_hl_account_spot_zero_balance.py
    │   │       ├── test_hl_market_perp.py
    │   │       ├── test_hl_market_spot.py
    │   │       ├── test_hl_trading_perp.py
    │   │       └── test_hl_trading_spot.py
    │   ├── conftest.py
    │   ├── core/
    │   │   ├── __init__.py
    │   │   ├── test_backtesting.py
    │   │   ├── test_core_workflow.py
    │   │   └── test_market_orders.py
    │   ├── test_failure_scenarios.py
    │   └── test_safety_systems.py
    ├── unit/
    │   ├── __init__.py
    │   ├── apis/
    │   │   ├── __init__.py
    │   │   ├── test_backpack_components.py
    │   │   ├── test_hyperliquid_components.py
    │   │   └── test_rate_limiter.py
    │   ├── conftest.py
    │   ├── config/
    │   │   ├── __init__.py
    │   │   ├── test_config_consistency.py
    │   │   ├── test_config_example.py
    │   │   └── test_config_security.py
    │   ├── core/
    │   │   ├── __init__.py
    │   │   ├── test_data_manager.py
    │   │   ├── test_execution_handler.py
    │   │   ├── test_portfolio_tracker.py
    │   │   ├── test_signal_generator.py
    │   │   ├── test_signal_queue.py
    │   │   ├── test_symbol_mapper.py
    │   │   └── execution/
    │   │       ├── __init__.py
    │   │       └── test_synchronized_order_submission.py
    │   ├── models/
    │   │   ├── __init__.py
    │   │   ├── test_bp_raw_models.py
    │   │   └── test_hl_raw_models.py
    │   ├── monitoring/
    │   │   ├── __init__.py
    │   │   ├── test_performance_visualizer.py
    │   │   └── test_simplified_visualizer.py
    │   ├── risk/
    │   │   ├── __init__.py
    │   │   ├── conftest.py
    │   │   ├── test_rm_constraints.py
    │   │   ├── test_rm_controls.py
    │   │   ├── test_rm_dependencies.py
    │   │   ├── test_rm_sizing_simple.py
    │   │   ├── test_rm_sizing_standard.py
    │   │   ├── test_rm_validation.py
    │   │   └── test_risk_manager_init.py
    │   ├── strategies/
    │   │   ├── __init__.py
    │   │   └── test_funding_rate_arbitrage.py
    │   └── validation/
    │       ├── __init__.py
    │       ├── test_circuit_breaker.py
    │       ├── test_funding_rate_validator.py
    │       ├── test_multi_tier_funding_provider.py
    │       └── test_position_reconciliation.py
    └── vcr_cassettes/  # HTTP response recordings for deterministic testing
```

## Key Architectural Changes Since April 2025

### 1. **Enhanced API Architecture**
- Refactored from monolithic API files to component-based architecture
- Introduced factory pattern for API component creation
- Separated concerns: authenticators, mappers, transformers, service layers
- Added dedicated HTTP/WebSocket connectivity layer

### 2. **Security Improvements**
- New decorator-based security layer for input validation
- Fixed authentication issues (Hyperliquid EIP-712, Backpack ED25519)
- Enhanced rate limiting with request weighting

### 3. **Type Safety Enhancement**
- Complete migration to Pydantic V2
- Replaced generic `ParsedJsonResponse` with typed Raw models
- Added comprehensive validation for all API boundaries

### 4. **Market Order Support**
- New order execution system with limit and market order implementations
- Market orders implemented using aggressive IoC (Immediate-or-Cancel) orders

### 5. **Test Infrastructure**
- Comprehensive integration tests for all exchange operations
- VCR cassette recording for deterministic testing
- Separate test suites for different balance scenarios (positive/zero)
- Enhanced fixtures for time handling and configuration

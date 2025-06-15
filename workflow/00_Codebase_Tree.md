# CyberDeltaEngine Codebase Tree (Updated 15.06.2025)

```text
.
├── main.py
├── cyberdelta/
│   ├── __init__.py
│   ├── apis/
│   │   ├── __init__.py
│   │   ├── backpack/                      # Enhanced modular structure
│   │   │   ├── __init__.py
│   │   │   ├── BALANCE.md                 # Auto-lending documentation
│   │   │   ├── bp_api.py                  # Core API client
│   │   │   ├── bp_auth.py                 # Enhanced authentication
│   │   │   ├── bp_api_components_factory.py # Component factory
│   │   │   ├── bp_error_mapper.py         # Error mapping
│   │   │   ├── bp_rate_limit_strategy.py  # Rate limiting
│   │   │   ├── bp_request_builder.py      # Request construction
│   │   │   ├── bp_response_handler.py     # Response handling
│   │   │   ├── bp_ws_message_router.py    # WebSocket routing
│   │   │   ├── bp_ws_raw_message_handler.py # Message handling
│   │   │   ├── mappers/                   # Data transformation
│   │   │   │   ├── __init__.py
│   │   │   │   ├── bp_account_data_mapper.py # Enhanced with collateral
│   │   │   │   ├── bp_market_data_mapper.py
│   │   │   │   └── bp_trading_data_mapper.py # Enhanced with margin
│   │   │   ├── models/                    # 30+ Pydantic models
│   │   │   │   ├── __init__.py
│   │   │   │   ├── bp_raw_account.py
│   │   │   │   ├── bp_raw_collateral.py   # NEW: Collateral models
│   │   │   │   ├── bp_raw_margin_functions.py # NEW: Margin support
│   │   │   │   ├── bp_raw_market.py
│   │   │   │   ├── bp_raw_order.py
│   │   │   │   ├── bp_raw_trade.py
│   │   │   │   └── [25+ other model files]
│   │   │   └── services/                  # Business logic
│   │   │       ├── __init__.py
│   │   │       ├── bp_account_service.py  # Enhanced with auto-lending
│   │   │       ├── bp_market_data_service.py
│   │   │       └── bp_trading_service.py
│   │   ├── hyperliquid/                   # Comprehensive HL integration
│   │   │   ├── __init__.py
│   │   │   ├── hl_api.py                  # Core API client
│   │   │   ├── hl_auth.py                 # EIP-712 signatures
│   │   │   ├── hl_asset_indexer.py        # Asset management
│   │   │   ├── hl_config_loader.py        # Configuration
│   │   │   ├── hl_request_builder.py      # Request construction
│   │   │   ├── hl_response_handler.py     # Response handling
│   │   │   ├── hl_rate_limiter.py         # Weight-based limiting
│   │   │   ├── hl_ws_manager.py           # WebSocket management
│   │   │   ├── mappers/                   # Data transformation
│   │   │   │   ├── __init__.py
│   │   │   │   ├── hl_account_mapper.py
│   │   │   │   ├── hl_market_mapper.py
│   │   │   │   └── hl_trade_mapper.py
│   │   │   ├── models/                    # 40+ Pydantic models
│   │   │   │   ├── __init__.py
│   │   │   │   ├── hl_raw_responses.py
│   │   │   │   ├── hl_order_types.py
│   │   │   │   └── [35+ other model files]
│   │   │   └── services/                  # Service layer
│   │   │       ├── __init__.py
│   │   │       ├── hl_account_service.py
│   │   │       ├── hl_market_service.py
│   │   │       └── hl_trading_service.py
│   │   ├── base/                          # Abstract interfaces
│   │   │   ├── __init__.py
│   │   │   ├── authenticator_interface.py
│   │   │   ├── error_mapper_interface.py
│   │   │   ├── exchange_api.py
│   │   │   ├── rate_limit_strategy_interface.py
│   │   │   ├── request_builder_interface.py
│   │   │   └── response_handler_interface.py
│   │   ├── connectivity/                  # Connection management
│   │   │   ├── __init__.py
│   │   │   ├── http_client.py
│   │   │   └── ws_manager.py
│   │   ├── models/                        # Shared API models
│   │   │   ├── __init__.py
│   │   │   ├── api_config.py
│   │   │   ├── api_error.py
│   │   │   ├── api_request.py
│   │   │   └── exchange_api_config.py
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
│   ├── scripts/                           # Utility scripts
│   │   ├── test_autolending_status.py     # NEW: Auto-lending detection
│   │   ├── test_balance_and_collateral_endpoints.py # NEW: Balance tests
│   │   └── data_collection/               # Data gathering scripts
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
    │   ├── apis/                         # NEW: Exchange-specific tests
    │   │   └── backpack/
    │   │       ├── __init__.py
    │   │       ├── conftest.py           # Test configuration
    │   │       ├── shared/               # Shared test utilities
    │   │       │   └── test_helpers.py   # Dynamic test helpers
    │   │       ├── account/              # Account-related tests
    │   │       │   ├── balances/
    │   │       │   │   ├── positive/     # Tests with balances
    │   │       │   │   │   └── test_bp_balances_positive.py
    │   │       │   │   └── zero/         # Zero balance tests
    │   │       │   │       └── test_bp_balances_zero.py
    │   │       │   ├── margin_balances/  # NEW: Margin tests
    │   │       │   │   ├── positive/
    │   │       │   │   │   └── test_bp_margin_balances_positive.py
    │   │       │   │   ├── zero/
    │   │       │   │   │   └── test_bp_margin_balances_zero.py
    │   │       │   │   └── test_bp_margin_integration_flow.py
    │   │       │   ├── orders/
    │   │       │   │   ├── positive/
    │   │       │   │   │   └── test_bp_orders_positive.py
    │   │       │   │   └── zero/
    │   │       │   │       └── test_bp_orders_zero.py
    │   │       │   ├── positions/
    │   │       │   │   ├── positive/
    │   │       │   │   │   └── test_bp_positions_positive.py
    │   │       │   │   └── zero/
    │   │       │   │       └── test_bp_positions_zero.py
    │   │       │   └── positive/         # Enhanced account tests
    │   │       │       ├── test_bp_account_summary_enhanced.py
    │   │       │       └── test_bp_account_summary_private.py
    │   │       ├── perp/                 # Perpetual futures tests
    │   │       │   └── positions/
    │   │       │       ├── large/
    │   │       │       │   └── test_bp_perp_positions_large.py
    │   │       │       ├── positive/
    │   │       │       │   └── test_bp_perp_positions_private.py
    │   │       │       └── zero/
    │   │       └── spot/                 # Spot trading tests
    │   │           └── orders/
    │   │               └── positive/
    │   │                   └── test_bp_spot_orders_private.py
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
This updated tree represents the significant evolution of the codebase since the initial assessment:

## Key Structural Changes:

1. **Modular API Architecture**: Both Backpack and Hyperliquid APIs now follow a comprehensive service-oriented architecture with clear separation of concerns

2. **Enhanced Models**: 70+ Pydantic models across both exchanges providing strict type safety and validation

3. **Auto-Lending Support**: New models and services specifically for handling Backpack's auto-lending feature

4. **Comprehensive Testing**: Restructured integration tests with dynamic helpers and edge case coverage

5. **Service Layer Pattern**: Clear business logic separation in services/ directories

6. **Production Features**: Margin trading, collateral management, and advanced order types

The codebase has grown from ~150 files to 200+ files, with most additions being well-structured models, services, and tests that enhance rather than complicate the architecture.
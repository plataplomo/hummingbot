#!/bin/bash

# Script to move Backpack integration test candidates

# Base paths
UNIT_BASE="/home/demute/code/CyberDeltaEngine/tests/unit/apis/backpack"
INTEGRATION_BASE="/home/demute/code/CyberDeltaEngine/tests/integration/apis/backpack"

# Create integration test directories
mkdir -p "$INTEGRATION_BASE/services"
mkdir -p "$INTEGRATION_BASE/mappers"

# Move remaining service tests (account_info already moved)
mv "$UNIT_BASE/services/test_bp_account_service_balances.py" "$INTEGRATION_BASE/services/"
mv "$UNIT_BASE/services/test_bp_account_service_history_operations.py" "$INTEGRATION_BASE/services/"
mv "$UNIT_BASE/services/test_bp_account_service_positions.py" "$INTEGRATION_BASE/services/"
mv "$UNIT_BASE/services/test_bp_account_service_transfers.py" "$INTEGRATION_BASE/services/"
mv "$UNIT_BASE/services/test_bp_market_data_service_funding.py" "$INTEGRATION_BASE/services/"
mv "$UNIT_BASE/services/test_bp_market_data_service_klines_misc.py" "$INTEGRATION_BASE/services/"
mv "$UNIT_BASE/services/test_bp_market_data_service_public_data.py" "$INTEGRATION_BASE/services/"
mv "$UNIT_BASE/services/test_bp_trading_service_account_misc.py" "$INTEGRATION_BASE/services/"
mv "$UNIT_BASE/services/test_bp_trading_service_order_management.py" "$INTEGRATION_BASE/services/"
mv "$UNIT_BASE/services/test_bp_trading_service_query_status.py" "$INTEGRATION_BASE/services/"

# Move integration mapper test
mv "$UNIT_BASE/mappers/test_bp_trading_data_mapper_integration.py" "$INTEGRATION_BASE/mappers/"

# Move main API tests
mv "$UNIT_BASE/test_bp_api_ws.py" "$INTEGRATION_BASE/"
mv "$UNIT_BASE/test_bp_api_ws_subscriptions.py" "$INTEGRATION_BASE/"

echo "Integration test files moved successfully!"
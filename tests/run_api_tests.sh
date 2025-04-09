#!/bin/bash

# Run API client tests
echo "Running API client tests..."
.venv/bin/pytest tests/unit/test_hyperliquid_api.py tests/unit/test_backpack_api.py -v 
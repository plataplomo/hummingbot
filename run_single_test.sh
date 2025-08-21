#!/bin/bash

# Script to run a single test with proper Python 3.12 environment

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

CONDA_ENV_PATH="$SCRIPT_DIR/.conda/envs/hummingbot"
export PATH="$CONDA_ENV_PATH/bin:$PATH"
export PYTHONPATH="$SCRIPT_DIR:$PYTHONPATH"
PYTHON_BIN="$CONDA_ENV_PATH/bin/python"

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <test_name>"
    echo "Example: $0 test_backpack_perpetual_api_order_book_data_source::TestClass::test_method"
    exit 1
fi

echo "Running test: $1"
"$PYTHON_BIN" -m pytest test/hummingbot/connector/derivative/backpack_perpetual/$1 -xvs --tb=short
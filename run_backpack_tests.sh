#!/bin/bash

# Script to run Backpack perpetual connector tests with the correct local conda environment

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Running Backpack Perpetual Connector Tests${NC}"
echo "================================================"

# Change to the hummingbot-dev directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Check if .conda directory exists
if [ ! -d ".conda" ]; then
    echo -e "${RED}Error: .conda directory not found in $SCRIPT_DIR${NC}"
    exit 1
fi

# Use the hummingbot conda environment with Python 3.12
CONDA_ENV_PATH="$SCRIPT_DIR/.conda/envs/hummingbot"
if [ ! -d "$CONDA_ENV_PATH" ]; then
    echo -e "${RED}Error: hummingbot conda environment not found at $CONDA_ENV_PATH${NC}"
    exit 1
fi

export PATH="$CONDA_ENV_PATH/bin:$PATH"
export PYTHONPATH="$SCRIPT_DIR:$PYTHONPATH"
PYTHON_BIN="$CONDA_ENV_PATH/bin/python"

# Verify Python version
PYTHON_VERSION=$("$PYTHON_BIN" --version 2>&1 | cut -d' ' -f2)
echo -e "${YELLOW}Using Python: $PYTHON_VERSION${NC}"
echo -e "${YELLOW}Python path: $PYTHON_BIN${NC}"

# Check if pytest is installed
if ! "$PYTHON_BIN" -m pytest --version > /dev/null 2>&1; then
    echo -e "${RED}pytest not found, installing...${NC}"
    "$CONDA_ENV_PATH/bin/pip" install pytest pytest-asyncio --quiet
fi

# Run the Backpack perpetual connector tests
echo -e "\n${GREEN}Running Backpack Perpetual Connector Tests...${NC}\n"

# Check if specific test file is provided as argument
if [ "$#" -eq 1 ]; then
    TEST_TARGET="$1"
    echo -e "${YELLOW}Running specific test: $TEST_TARGET${NC}\n"
else
    TEST_TARGET="test/hummingbot/connector/derivative/backpack_perpetual/"
    echo -e "${YELLOW}Running all Backpack perpetual tests${NC}\n"
fi

# Run tests with proper environment
"$PYTHON_BIN" -m pytest "$TEST_TARGET" \
    --tb=short \
    --no-header \
    -rN \
    --disable-warnings \
    2>&1 | tee backpack_test_results.log

# Capture exit code
TEST_EXIT_CODE=${PIPESTATUS[0]}

# Summary
echo ""
echo "================================================"
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
else
    echo -e "${RED}✗ Some tests failed. Check backpack_test_results.log for details.${NC}"
    echo ""
    # Show failed test summary
    echo -e "${YELLOW}Failed tests summary:${NC}"
    grep -E "FAILED|ERROR" backpack_test_results.log | head -20 || true
fi
echo "================================================"

exit $TEST_EXIT_CODE
#!/bin/bash
# Test runner script for CyberDeltaEngine
# Provides convenient commands for running different test categories

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_colored() {
    printf "${1}${2}${NC}\n"
}

# Help message
show_help() {
    cat << EOF
Test Runner for CyberDeltaEngine

Usage: $0 [COMMAND] [OPTIONS]

Commands:
    unit            Run unit tests only (fast, for development)
    integration     Run integration tests only
    all             Run all tests with coverage
    quick           Run unit tests with fail-fast (development)
    coverage        Run tests with detailed coverage report
    markers         Show available pytest markers

Test Categories:
    unit-apis       Unit tests for API clients
    unit-core       Unit tests for core logic
    integration-config   Integration tests for configuration
    integration-viz      Integration tests for visualization

Examples:
    $0 unit                    # Fast unit tests
    $0 integration            # Integration tests only
    $0 quick                  # Fast development feedback
    $0 coverage               # Full coverage report
    $0 unit-core              # Only core unit tests

Options:
    -v, --verbose            Verbose output
    -x, --exitfirst         Exit on first failure
    --no-cov                Skip coverage reporting
    -k PATTERN              Run tests matching pattern
    -m MARKERS              Run tests with specific markers

EOF
}

# Default options
VERBOSE=""
EXITFIRST=""
NO_COV=""
PATTERN=""
MARKERS=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE="-v"
            shift
            ;;
        -x|--exitfirst)
            EXITFIRST="-x"
            shift
            ;;
        --no-cov)
            NO_COV="--no-cov"
            shift
            ;;
        -k)
            PATTERN="-k $2"
            shift 2
            ;;
        -m)
            MARKERS="-m $2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            COMMAND="$1"
            shift
            ;;
    esac
done

# Construct pytest command
PYTEST_CMD="python -m pytest"
if [[ -n "$VERBOSE" ]]; then
    PYTEST_CMD="$PYTEST_CMD $VERBOSE"
fi
if [[ -n "$EXITFIRST" ]]; then
    PYTEST_CMD="$PYTEST_CMD $EXITFIRST"
fi
if [[ -n "$PATTERN" ]]; then
    PYTEST_CMD="$PYTEST_CMD $PATTERN"
fi
if [[ -n "$MARKERS" ]]; then
    PYTEST_CMD="$PYTEST_CMD $MARKERS"
fi

# Execute based on command
case "${COMMAND:-all}" in
    unit)
        print_colored $BLUE "Running unit tests..."
        $PYTEST_CMD tests/unit/ $NO_COV
        ;;
    integration)
        print_colored $BLUE "Running integration tests..."
        $PYTEST_CMD tests/integration/ $NO_COV
        ;;
    quick)
        print_colored $YELLOW "Running quick unit tests (fail-fast)..."
        $PYTEST_CMD tests/unit/ -x --ff $NO_COV
        ;;
    coverage)
        print_colored $BLUE "Running all tests with coverage..."
        $PYTEST_CMD --cov=cyberdelta --cov-report=term-missing --cov-report=html
        print_colored $GREEN "Coverage report generated in htmlcov/"
        ;;
    all)
        print_colored $BLUE "Running all tests..."
        if [[ -z "$NO_COV" ]]; then
            $PYTEST_CMD --cov=cyberdelta --cov-report=term-missing
        else
            $PYTEST_CMD
        fi
        ;;
    unit-apis)
        print_colored $BLUE "Running API unit tests..."
        $PYTEST_CMD tests/unit/apis/ $NO_COV
        ;;
    unit-core)
        print_colored $BLUE "Running core unit tests..."
        $PYTEST_CMD tests/unit/core/ $NO_COV
        ;;
    integration-config)
        print_colored $BLUE "Running configuration integration tests..."
        $PYTEST_CMD tests/integration/config/ $NO_COV
        ;;
    integration-viz)
        print_colored $BLUE "Running visualization integration tests..."
        $PYTEST_CMD tests/integration/visualization/ $NO_COV
        ;;
    markers)
        print_colored $BLUE "Available pytest markers:"
        python -m pytest --markers | grep -E "^@pytest.mark\.|^    "
        ;;
    *)
        print_colored $RED "Unknown command: $COMMAND"
        show_help
        exit 1
        ;;
esac

# Print completion message
if [[ $? -eq 0 ]]; then
    print_colored $GREEN "✅ Tests completed successfully!"
else
    print_colored $RED "❌ Tests failed!"
    exit 1
fi

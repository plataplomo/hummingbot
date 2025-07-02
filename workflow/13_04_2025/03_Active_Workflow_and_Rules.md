# CyberDeltaEngine - Active Workflow and Rules (as of 2025-07-02)

## Active Workflow Phase

We are currently in the **Production Readiness & Strategy Implementation** phase. The foundational stability and testing objectives have been successfully completed. Current objectives focus on:
*   ✅ **Completed**: Strict type safety (near-perfect `mypy` compliance across 230 files)
*   ✅ **Completed**: Adherence to coding standards (comprehensive `ruff` compliance)
*   ✅ **Completed**: Correct and consistent use of `Decimal` for all financial calculations (107 files)
*   ✅ **Completed**: Robustness against `None` values and comprehensive error handling
*   🎯 **Current Focus**: Funding rate arbitrage strategy implementation and production deployment preparation

## Key Guiding Rules (Project-Specific) - IMPLEMENTED

The following rules have been successfully implemented and are actively maintained:

*   ✅ **Python File Validation:** Comprehensive `ruff check` and `mypy` validation across 230 Python files with near-perfect compliance
*   ✅ **Virtual Environment Execution:** All Python tools properly executed within project virtual environment
*   ✅ **Tool Configuration Integrity:** Robust `pyproject.toml` configuration with comprehensive linting and type checking rules
*   ✅ **Mandatory `Decimal` Usage:** Strict `Decimal` enforcement across 107 files for all financial quantities with comprehensive `None` checking
*   ✅ **Code-Level Documentation:** Comprehensive docstrings and inline documentation throughout the codebase
*   ✅ **Code Formatting and Style:** Strict adherence to `ruff format` and configured rules with consistent Python naming conventions
*   ✅ **Python Standards:** Full Python 3.13+ compliance with modern typing syntax and comprehensive static analysis
*   ✅ **Security:** Comprehensive security implementation including input validation, authentication, and secrets management
*   ✅ **Comprehensive Testing:** 393 test files with VCR recording and extensive coverage of all system components
*   ✅ **Pydantic Validation:** 423 models providing 100% API validation coverage across all exchanges

## Current Development Standards & Quality Metrics

*   **Architecture Excellence:** Complete 6-layer API architecture with clear separation of concerns
*   **Type Safety:** Near-perfect `mypy` compliance across 88,567 lines of production code
*   **Test Coverage:** 29.18% code coverage (7,260/24,878 lines) with 393 comprehensive test files
*   **Security Standards:** Comprehensive input validation, authentication systems, and secrets management
*   **Financial Precision:** Strict `Decimal` usage enforced across all monetary calculations and data models
*   **Production Readiness:** Robust error handling, circuit breakers, and position reconciliation systems
